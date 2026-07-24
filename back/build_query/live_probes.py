"""Phase 1.5b — construction des sondes de cardinalité `inspect --live` (pur, sans I/O).

Décompose chaque CTE en **préfixes de JOIN** (scan → ⋈ join1 → ⋈ join2 → …) et produit,
par frontière, la requête ``COUNT(*)`` du préfixe accumulé + (quand la clé est
extractible) un ``COUNT(*) / COUNT(DISTINCT clé_droite)`` sur la table jointe. Exécutées
sur l'ENTREPÔT RÉEL (via ``warehouse_gate``), elles donnent le waterfall « orders filtrés
= 90 → ⋈ order_items = 340 (×3,8) », qui localise le join coupable et le côté non-unique.

Le SQL produit reste dans le DIALECTE SOURCE (bigquery/snowflake) et référence les VRAIES
tables — aucun suffixe ni transpilation DuckDB (contrairement aux sondes synthétiques de
``examples_executor._run_join_count_probes``). Pur → testable hors-ligne ; l'exécution
gatée vit dans ``cli/test_runner.inspect_live``.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp

from utils.sqlglot_ast import get_from, quote_identifier


def _joined_alias(join: exp.Join) -> str:
    """Alias (sinon nom) de la table/CTE nouvellement jointe (côté droit)."""
    src = join.this
    if src is None:
        return ""
    return (getattr(src, "alias", "") or "") or (
        src.name if isinstance(src, exp.Table) else ""
    )


def _right_key(
    on: exp.Expression | None, joined_alias: str, dialect: str
) -> str | None:
    """Clé de jointure côté table jointe, extraite de la 1ʳᵉ égalité du ON dont une
    colonne référence *joined_alias*. ``None`` si non déterminable (ON complexe)."""
    if on is None or not joined_alias:
        return None
    ja = joined_alias.lower()
    for eq in on.find_all(exp.EQ):
        for side in (eq.left, eq.right):
            if isinstance(side, exp.Column) and (side.table or "").lower() == ja:
                return side.sql(dialect=dialect)
    return None


def _agg_distinct_key(tree: exp.Select, dialect: str) -> str | None:
    """Axe de comptage d'un agrégat COUNT — la clé sur laquelle un fan-out d'entrée
    gonfle un ``COUNT(...)`` non-distinct (cas COUNT vs COUNT DISTINCT, ex. sf_bq263).

    Priorité : ``COUNT(DISTINCT clé)`` (le SQL énonce déjà l'axe). Repli : l'argument
    colonne d'un ``COUNT(col)`` nu — c'est le cas du SQL **bugué** (DISTINCT manquant),
    précisément celui où ``inspect --live`` sert : mesurer n vs DISTINCT col y révèle le
    sur-comptage. ``COUNT(*)`` n'est pas un axe (pas de clé). ``None`` si aucun."""
    fallback: str | None = None
    for cnt in tree.find_all(exp.Count):
        this = cnt.this
        if isinstance(this, exp.Distinct):
            exprs = this.expressions or ([this.this] if this.this else [])
            if exprs and isinstance(exprs[0], exp.Column):
                return exprs[0].sql(dialect=dialect)
        elif fallback is None and isinstance(this, exp.Column):
            fallback = this.sql(dialect=dialect)
    return fallback


def _with_prefix(preceding: list[dict], dialect: str) -> str:
    """``WITH`` des CTE amont (hors ``final_query``) pour que le préfixe soit exécutable."""
    if not preceding:
        return ""
    parts = [
        f"{quote_identifier(c['name'], dialect)} AS ({c['code']})" for c in preceding
    ]
    return "WITH " + ",\n".join(parts) + "\n"


def build_live_probes(ctes: list, dialect: str, *, full: bool = False) -> list:
    """Sondes de cardinalité par préfixe, prêtes à tirer sur l'entrepôt.

    - ``full=False`` (défaut, **tiered/cheap**) : ne sonde que la DERNIÈRE CTE jointe
      (typiquement le pré-agrégat / la requête finale) — une passe courte suffit à révéler
      un fan-out à l'entrée du GROUP BY.
    - ``full=True`` : le waterfall complet sur toutes les CTE comportant des JOINs.

    Retour : liste de ``{"cte": nom, "probes": [...]}``. Chaque probe :
      ``{cte, boundary("scan"|"join"), [join_index, join_type, on, right_key,
         right_distinct_sql], label, count_sql}``.
    """
    joined: list = []
    for idx, cte in enumerate(ctes):
        try:
            tree = sqlglot.parse_one(cte["code"], read=dialect)
        except Exception:
            continue
        if not isinstance(tree, exp.Select):
            continue
        from_ = get_from(tree)
        joins = tree.args.get("joins") or []
        if from_ is None or not joins:
            continue
        joined.append((idx, cte, from_, joins))
    if not joined:
        return []

    targets = joined if full else [joined[-1]]
    out: list = []
    for idx, cte, from_, joins in targets:
        preceding = [c for c in ctes[:idx] if c["name"] != "final_query"]
        wp = _with_prefix(preceding, dialect)
        base_sql = from_.this.sql(dialect=dialect)
        running = base_sql
        probes: list = [
            {
                "cte": cte["name"],
                "boundary": "scan",
                "label": base_sql,
                "count_sql": f"{wp}SELECT COUNT(*) AS n FROM {running}",
            }
        ]
        for j_idx, join in enumerate(joins):
            join_sql = join.sql(dialect=dialect)
            right_sql = join.this.sql(dialect=dialect)
            on = join.args.get("on")
            alias = _joined_alias(join)
            rk = _right_key(on, alias, dialect)
            running = f"{running} {join_sql}"
            probe: dict = {
                "cte": cte["name"],
                "boundary": "join",
                "join_index": j_idx,
                "label": f"⋈ {right_sql}",
                "join_type": (
                    join.args.get("side") or join.args.get("kind") or "INNER"
                ).upper(),
                "on": on.sql(dialect=dialect) if on is not None else None,
                "count_sql": f"{wp}SELECT COUNT(*) AS n FROM {running}",
            }
            if rk:
                probe["right_key"] = rk
                probe["right_distinct_sql"] = (
                    f"{wp}SELECT COUNT(*) AS n, COUNT(DISTINCT {rk}) AS d FROM {right_sql}"
                )
            probes.append(probe)

        # Sonde pré-agrégat (la sonde CHEAP n°1 du plan) : COUNT(*) vs COUNT(DISTINCT clé)
        # à l'entrée du GROUP BY — préfixe complet + WHERE (l'entrée de l'agrégat est
        # post-filtre). 340 lignes / 90 clés = la signature COUNT vs COUNT DISTINCT
        # (sf_bq263) qu'aucune sonde d'unicité par-join ne capte quand le fan-out vit
        # dans la table de base.
        agg_key = _agg_distinct_key(tree, dialect)
        if agg_key:
            where = tree.args.get("where")
            where_sql = f" {where.sql(dialect=dialect)}" if where is not None else ""
            probes.append(
                {
                    "cte": cte["name"],
                    "boundary": "pre_agg",
                    "label": f"entrée GROUP BY (n vs DISTINCT {agg_key})",
                    "distinct_key": agg_key,
                    "count_sql": (
                        f"{wp}SELECT COUNT(*) AS n, COUNT(DISTINCT {agg_key}) AS d "
                        f"FROM {running}{where_sql}"
                    ),
                }
            )
        out.append({"cte": cte["name"], "probes": probes})
    return out


def classify_waterfall(probes: list) -> list:
    """Annote une liste de probes RÉSOLUES (chaque probe a reçu ``rows`` = COUNT(*), et
    optionnellement ``right_rows``/``right_distinct``) avec le ratio de fan-out et le
    verdict par frontière. Fait purement descriptif (comme ``_classify_join_cardinality``)
    — l'oracle du bug reste le contrat ``expect``, pas ce waterfall.
    """
    annotated: list = []
    prev_rows: int | None = None
    for p in probes:
        rows = p.get("rows")
        entry = dict(p)
        # Frontière pré-agrégat : le fan-out se lit sur l'AXE de l'agrégat (n vs clés
        # distinctes), pas contre le préfixe précédent (le WHERE fausserait la comparaison).
        if p.get("boundary") == "pre_agg":
            d = p.get("distinct_rows")
            if rows is not None and d is not None and d > 0:
                entry["agg_fanout_ratio"] = round(rows / d, 2)
                entry["verdict"] = "fan_out" if rows > d else "preserves"
            annotated.append(entry)
            continue
        if rows is not None and prev_rows is not None and prev_rows > 0:
            ratio = rows / prev_rows
            entry["fanout_ratio"] = round(ratio, 2)
            if rows == 0:
                entry["verdict"] = "empty"
            elif ratio > 1.0:
                entry["verdict"] = "fan_out"
            elif ratio < 1.0:
                entry["verdict"] = "shrinks"
            else:
                entry["verdict"] = "preserves"
        # Côté droit non-unique sur la clé = source mécanique du fan-out.
        rr, rd = p.get("right_rows"), p.get("right_distinct")
        if rr is not None and rd is not None:
            entry["right_non_unique"] = rd < rr
        if rows is not None:
            prev_rows = rows
        annotated.append(entry)
    return annotated
