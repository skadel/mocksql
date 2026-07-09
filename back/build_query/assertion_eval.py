"""Évaluation d'assertions dbt-style contre DuckDB — pur SQL, zéro dépendance LLM.

Isolé de ``examples_executor`` (qui tire langchain/langgraph au import) pour que le
replay CI (``mocksql test`` / ``cli/test_runner.py``) — qui ne fait AUCUN appel LLM —
ne paie pas ~3-4s de chargement du stack LLM. ``examples_executor`` ré-exporte
``_evaluate_assertions`` depuis ici pour ses appelants internes.
"""

from __future__ import annotations

from typing import Any, Dict, List

import sqlglot
from sqlglot import exp

# Fonctions de string-slicing dont le 1er argument DOIT être du texte en DuckDB. Appliquées
# à une colonne DATE/TIMESTAMP/numérique → `Binder Error` (ex. left(TIMESTAMP, INTEGER)).
_STRING_SLICE_FUNCS = (exp.Left, exp.Right, exp.Substring)
_STRING_SLICE_NAMES = {"LEFT", "RIGHT", "SUBSTR", "SUBSTRING"}


def _is_text_type(duckdb_type: str) -> bool:
    """Vrai si le type DuckDB est un type texte (sur lequel LEFT/SUBSTR est légitime)."""
    t = (duckdb_type or "").upper()
    return "CHAR" in t or "TEXT" in t or "STRING" in t


def _nontext_columns(view_name: str, con) -> set[str]:
    """Colonnes de la vue résultat dont le type DuckDB n'est PAS texte (lowercase).

    Ce sont exactement celles sur lesquelles un string-slicing (LEFT/SUBSTR…) échoue —
    on s'y limite pour ne jamais toucher un slicing légitime d'une vraie colonne string.
    Best-effort : DESCRIBE en échec → set vide (la garde devient un no-op).
    """
    try:
        rows = con.execute(f"DESCRIBE {view_name}").fetchall()
    except Exception:
        return set()
    return {r[0].lower() for r in rows if not _is_text_type(r[1])}


def _cast_nontext_string_slicing(
    sql: str, non_text_cols: set[str], dialect: str = "duckdb"
) -> str:
    """Enveloppe dans ``CAST(... AS TEXT)`` l'argument d'un LEFT/RIGHT/SUBSTR qui est une
    colonne NON-texte (présente dans ``non_text_cols``), pour rendre le string-slicing
    valide en DuckDB sans changer la sémantique voulue (slicer la représentation chaîne).

    Idempotent (saute un argument déjà casté) et best-effort (parsing en échec → SQL
    inchangé, jamais bloquant — même posture que ``_autoscope_failing_assertions``).
    """
    if not sql or not non_text_cols:
        return sql
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return sql
    if tree is None:
        return sql

    def _maybe_cast(arg):
        if not isinstance(arg, exp.Column) or isinstance(arg, exp.Cast):
            return None
        if arg.name.lower() not in non_text_cols:
            return None
        return exp.cast(arg.copy(), "TEXT")

    changed = False
    try:
        for node in tree.find_all(*_STRING_SLICE_FUNCS):
            casted = _maybe_cast(node.this)
            if casted is not None:
                node.set("this", casted)
                changed = True
        for node in tree.find_all(exp.Anonymous):
            if (node.name or "").upper() not in _STRING_SLICE_NAMES:
                continue
            args = node.expressions
            if not args:
                continue
            casted = _maybe_cast(args[0])
            if casted is not None:
                node.set("expressions", [casted, *args[1:]])
                changed = True
    except Exception:
        return sql

    return tree.sql(dialect=dialect) if changed else sql


def _is_empty_intent_sentinel(raw_sql: str) -> bool:
    """Vrai si l'assertion est la sentinelle « résultat vide intentionnel » : un
    ``SELECT * FROM __result__`` NU (sans ``WHERE``). Émise par ``test_evaluator`` quand
    le scénario attend explicitement 0 ligne (plage vide, anti-jointure ciblée…). Sa
    présence dans la suite signale que le vide est VOULU → la garde anti-vacuité sur
    résultat vide (ci-dessous) ne doit pas s'appliquer.
    """
    norm = " ".join((raw_sql or "").strip().rstrip(";").split()).lower()
    return norm == "select * from __result__"


_RESERVED_KEYWORDS: frozenset[str] | None = None


def _duckdb_reserved_keywords() -> frozenset[str]:
    """Mots réservés DuckDB (catégorie ``reserved``), lus depuis le moteur lui-même
    (``duckdb_keywords()``) — la liste embarquée de sqlglot est incomplète (elle quote
    ``offset`` mais pas ``end``). Cache module ; best-effort : échec → noyau connu.
    """
    global _RESERVED_KEYWORDS
    if _RESERVED_KEYWORDS is None:
        try:
            import duckdb

            rows = duckdb.execute(
                "SELECT keyword_name FROM duckdb_keywords() "
                "WHERE keyword_category = 'reserved'"
            ).fetchall()
            _RESERVED_KEYWORDS = frozenset(r[0].lower() for r in rows)
        except Exception:
            _RESERVED_KEYWORDS = frozenset({"offset", "end", "order", "group", "all"})
    return _RESERVED_KEYWORDS


def _quote_reserved_identifiers(sql: str, dialect: str = "duckdb") -> str:
    """Quote les identifiants mots réservés (``offset``, ``end``, …) — et eux seuls,
    sans sur-quoting — via l'AST sqlglot re-rendu.

    Un LLM les écrit nus (il ignore la liste des mots réservés DuckDB) → ``Parser
    Error`` sur TOUTE la suite d'assertions dès que le résultat expose une telle
    colonne (incident c6 : ``UNNEST(...) WITH OFFSET AS offset``), et les boucles de
    régénération/correction thrashent sans converger — aucun modèle ne peut réparer
    une erreur dont il ignore la cause. Accepte un fragment (condition, scope) comme
    une requête complète. Best-effort : parsing en échec → SQL inchangé, jamais
    bloquant (même posture que ``_cast_nontext_string_slicing``).
    """
    if not sql:
        return sql
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
        if tree is None:
            return sql
        reserved = _duckdb_reserved_keywords()
        changed = False
        for ident in tree.find_all(exp.Identifier):
            if not ident.args.get("quoted") and ident.name.lower() in reserved:
                ident.set("quoted", True)
                changed = True
        # Re-rendu seulement si un identifiant a été quoté : le SQL d'origine reste
        # byte-identique dans le cas courant (pas de reformatage cosmétique, ex.
        # `(c) IS NOT TRUE` → `NOT (c) IS TRUE`) — même posture que la garde sœur.
        return tree.sql(dialect=dialect) if changed else sql
    except Exception:
        return sql


def _evaluate_assertions(
    assertions: List[Dict[str, Any]], view_name: str, con
) -> List[Dict[str, Any]]:
    """
    Évalue chaque assertion dbt-style contre le DataFrame résultat enregistré sous view_name.

    Convention dbt-style : une assertion SQL doit retourner les lignes ÉCHOUANTES.
      - 0 ligne retournée → assertion passée (passed=True)
      - ≥1 ligne retournée → assertion échouée (passed=False), les lignes sont des contre-exemples

    Exemple : pour vérifier que `start_station_name` vaut toujours 'Central Park' :
      SELECT * FROM __result__ WHERE start_station_name != 'Central Park'
      → retourne les lignes où la station est incorrecte ; 0 ligne = OK.

    Ne pas confondre avec une assertion positive (WHERE col = 'X') qui retournerait
    des lignes quand la condition est vraie — ce serait l'inverse de la convention.
    """
    # Garde déterministe : une assertion qui string-slice (LEFT/SUBSTR…) une colonne
    # NON-texte est invalide en DuckDB (ex. left(TIMESTAMP, …)). On caste l'argument en
    # TEXT avant exécution — calcul des types de la vue une seule fois (cf. incident c3).
    non_text_cols = _nontext_columns(view_name, con)

    # Garde anti-vacuité sur RÉSULTAT VIDE (P0-1, incident c2) : une assertion `all` non
    # scopée (`SELECT * FROM __result__ WHERE (cond) IS NOT TRUE`) retourne 0 ligne
    # violante sur une vue VIDE → « passe » à tort. Une régression SQL qui vide la sortie
    # (la plus courante) shipperait alors au vert. On échoue ces assertions — SAUF si le
    # scénario attend explicitement un vide (sentinelle `SELECT * FROM __result__`), auquel
    # cas toute la suite est exemptée. Les assertions SCOPÉES sont déjà couvertes par la
    # garde de scope (périmètre vide → échec) ; les `exists` échouent déjà (aucun match) ;
    # les `aggregate` sont naturellement NON-vacuées sur vide (sous-requête scalaire :
    # SUM → NULL → IS NOT TRUE → violation, COUNT(*) = 0 → pass légitime) — on les laisse
    # s'exécuter pour un signal honnête plutôt qu'un force-fail.
    result_empty = False
    if assertions and not any(
        _is_empty_intent_sentinel(a.get("sql") or "") for a in assertions
    ):
        try:
            result_empty = (
                con.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()[0] == 0
            )
        except Exception:
            result_empty = False

    results = []
    for a in assertions:
        raw_sql = (a.get("sql") or "").strip()
        if not raw_sql:
            # SQL vide → con.execute("") renvoie None → ".fetchdf()" planterait avec un
            # message opaque ("'NoneType' object has no attribute 'fetchdf'"). On émet une
            # erreur explicite à la place (assertion malformée, ex. dérivation `sql` oubliée).
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    "sql": raw_sql,
                    "passed": False,
                    "error": "assertion SQL vide (aucune requête à exécuter)",
                }
            )
            continue
        raw_sql = _quote_reserved_identifiers(raw_sql)
        raw_sql = _cast_nontext_string_slicing(raw_sql, non_text_cols)
        sql = raw_sql.replace("__result__", view_name)
        scope = (a.get("scope") or "").strip().rstrip(";").strip()
        scope = _quote_reserved_identifiers(scope)
        scope = _cast_nontext_string_slicing(scope, non_text_cols)
        quantifier = (a.get("quantifier") or "all").strip() or "all"
        if result_empty and not scope and quantifier not in ("exists", "aggregate"):
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    "sql": raw_sql,
                    "passed": False,
                    "error": (
                        "résultat vide — l'assertion ne teste rien (vacante) : sur une "
                        "sortie vide, la condition n'a aucune ligne à contredire"
                    ),
                }
            )
            continue
        try:
            # Garde anti-vacuité du scope : une assertion scopée dont le périmètre ne
            # sélectionne AUCUNE ligne du résultat ne teste rien (0 ligne violante →
            # « passe » à tort). On l'échoue explicitement plutôt que de la laisser verte.
            # Inapplicable au mode `exists` : un scope (fondu dans l'EXISTS) qui ne couvre
            # aucune ligne fait DÉJÀ échouer l'assertion (rien à matcher) — pas de vacuité.
            # Applicable au mode `aggregate` : un agrégat sur un scope à 0 ligne est une
            # assertion d'absence déguisée (`COUNT(*) = 0` sur un label inexistant) —
            # interdite par la philosophie positive-only, comme en mode `all`.
            if scope and quantifier != "exists":
                scope_sql = scope.replace("__result__", view_name)
                covered = con.execute(
                    f"SELECT COUNT(*) FROM {view_name} WHERE ({scope_sql})"
                ).fetchone()[0]
                if covered == 0:
                    results.append(
                        {
                            "description": a.get("description", ""),
                            "expected_condition": a.get("expected_condition", ""),
                            "scope": a.get("scope", ""),
                            "sql": raw_sql,
                            "passed": False,
                            "error": (
                                "le périmètre (scope) ne sélectionne aucune ligne du "
                                "résultat — l'assertion ne teste rien (vacante)"
                            ),
                        }
                    )
                    continue
            fail_df = con.execute(sql).fetchdf()
            passed = len(fail_df) == 0
            # Modes `exists` / `aggregate` : l'échec renvoie une ligne sentinelle
            # (`_no_match` / `_agg_violation`) sans valeur métier — on n'expose pas de
            # contre-exemple (l'absence ou un agrégat faux n'est pas une ligne du résultat).
            failing_rows = (
                []
                if (passed or quantifier in ("exists", "aggregate"))
                else fail_df.to_dict(orient="records")
            )
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    **({"scope": a.get("scope", "")} if scope else {}),
                    **({"quantifier": quantifier} if quantifier != "all" else {}),
                    "sql": raw_sql,
                    "passed": passed,
                    "failing_rows": failing_rows,
                }
            )
        except Exception as e:
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    "sql": raw_sql,
                    "passed": False,
                    "error": str(e),
                }
            )
    return results
