"""Path-slicing des branches UNION ALL — génération focalisée par branche.

Une CTE (ou la requête finale) du type ``B1 UNION ALL B2 UNION ALL B3`` a des
branches **indépendantes par construction**. On peut réduire le SQL à une seule
branche sans changer son sens → générer un test *focalisé* (moins de colonnes,
contraintes/profil prunés, titre ``[Focus <branche>]``).

Fonctions **pures** : aucun effet de bord, aucun appel réseau, aucune dépendance au
state LangGraph. Elles opèrent sur ``query_decomposed`` (liste de ``{name, code, …}``
produite par ``validator.split_query``, dernier nœud nommé ``final_query``).

Réutilise les briques de graphe de [cte_graph.py](cte_graph.py) pour pruner les CTE
devenues orphelines après le slice. **Ne pas** utiliser ``build_isolated_sql`` ici : il
tronque en ``SELECT * FROM cte`` ; le path-slicing remplace l'opérande ``UNION`` *en
place* et garde tout l'aval de la CTE hôte intact.

Cadrage v1 (spec / plan tests-par-path) : seul un UNION ALL de **premier niveau**
(``distinct=False``) dans **une** CTE ou la requête finale. Imbriqués / multiples /
``UNION DISTINCT`` → ``list_union_paths`` renvoie ``[]`` (le système retombe sur le
comportement actuel : path ``all`` implicite).
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import sqlglot
from sqlglot import exp

from build_query.cte_graph import build_cte_dependency_graph, transitive_deps

ALL_PATH = "all"


@dataclass(frozen=True)
class PathSpec:
    """Une branche d'un UNION ALL de premier niveau.

    ``name`` est le **nom machine** (clé de ``target_path``), dérivé de la source
    primaire du FROM de la branche ; il est distinct du label humain produit par le
    LLM (``BranchPlan.branch``).
    """

    name: str
    branch_index: int
    host_cte: str


def _parse(code: str, dialect: str) -> exp.Expression | None:
    try:
        return sqlglot.parse_one(code, read=dialect)
    except Exception:
        return None


def _unwrap(node: exp.Expression) -> exp.Expression:
    """Retire les ``Paren`` et ``Subquery`` non-aliasés autour d'un nœud."""
    while True:
        if isinstance(node, exp.Paren):
            node = node.this
        elif isinstance(node, exp.Subquery) and not node.alias:
            node = node.this
        else:
            return node


def _strip_with(expr: exp.Expression) -> exp.Expression:
    """Copie de ``expr`` sans sa clause ``WITH`` (clé ``with`` ou ``with_``)."""
    body = expr.copy()
    body.args.pop("with", None)
    body.args.pop("with_", None)
    return body


def _top_union(expr: exp.Expression) -> exp.Union | None:
    """Renvoie le nœud ``UNION ALL`` si le corps de ``expr`` est un set-op de 1er niveau.

    Exclut ``UNION DISTINCT`` (``distinct=True``) et ``EXCEPT``/``INTERSECT``.
    """
    node = _unwrap(_strip_with(expr))
    if (
        isinstance(node, exp.Union)
        and not isinstance(node, (exp.Except, exp.Intersect))
        and not node.args.get("distinct")
    ):
        return node
    return None


def _flatten_union(union: exp.Union) -> list[exp.Expression]:
    """Aplatit un UNION ALL left-deep (``Union(Union(B1,B2),B3)`` → ``[B1,B2,B3]``)."""
    branches: list[exp.Expression] = []
    node: exp.Expression = union
    while (
        isinstance(node, exp.Union)
        and not isinstance(node, (exp.Except, exp.Intersect))
        and not node.args.get("distinct")
    ):
        branches.append(node.expression)
        node = node.this
    branches.append(node)  # opérande le plus à gauche
    branches.reverse()
    return branches


def _branch_name(branch: exp.Expression, index: int) -> str:
    """Nom machine d'une branche = source primaire de son FROM, sinon ``branch_N``."""
    sel = _unwrap(branch)
    if isinstance(sel, exp.Select):
        from_ = sel.args.get("from") or sel.args.get("from_")
        if from_ is not None and isinstance(from_.this, exp.Table):
            name = from_.this.name
            if name:
                return name
    return f"branch_{index + 1}"


def _find_host(query_decomposed: list[dict], dialect: str):
    """Le couple ``(host_name, union_node)`` s'il y a **exactement un** UNION ALL de
    1er niveau et aucune branche imbriquée ; sinon ``None``."""
    hosts: list[tuple[str, exp.Union]] = []
    for node in query_decomposed:
        parsed = _parse(node.get("code", ""), dialect)
        if parsed is None:
            continue
        union = _top_union(parsed)
        if union is not None:
            hosts.append((node["name"], union))
    if len(hosts) != 1:
        return None
    host_name, union = hosts[0]
    branches = _flatten_union(union)
    # Branche elle-même un UNION (imbriqué) → hors scope v1.
    if any(_top_union(b) is not None for b in branches):
        return None
    return host_name, union, branches


def list_union_paths(
    query_decomposed: list[dict], dialect: str = "bigquery"
) -> list[PathSpec]:
    """Liste les branches d'un UNION ALL de premier niveau (``[]`` si non applicable)."""
    found = _find_host(query_decomposed, dialect)
    if found is None:
        return []
    host_name, _union, branches = found

    specs: list[PathSpec] = []
    used: set[str] = set()
    for i, branch in enumerate(branches):
        name = _branch_name(branch, i)
        if name in used:  # collision de noms → désambiguïse en branch_N
            name = f"branch_{i + 1}"
        used.add(name)
        specs.append(PathSpec(name=name, branch_index=i, host_cte=host_name))
    return specs


def _prune_orphans(
    order: list[str],
    code_by_name: dict[str, str],
    main_code: str,
    dialect: str,
) -> list[str]:
    """Noms des CTE atteignables depuis ``final_query``, dans l'ordre d'origine."""
    nodes = [{"name": n, "code": code_by_name[n]} for n in order]
    nodes.append({"name": "final_query", "code": main_code})
    graph = build_cte_dependency_graph(nodes, dialect)
    reachable = transitive_deps(graph, "final_query")
    return [n for n in order if n in reachable]


def slice_to_path(
    sql: str,
    path_name: str | None,
    query_decomposed: list[dict],
    dialect: str = "bigquery",
) -> str:
    """Réécrit ``sql`` pour ne garder que la branche ``path_name`` du UNION ALL.

    ``path_name`` ∈ {nom de branche} → SQL slicé (branche seule + CTE orphelines
    prunées). ``path_name`` ∈ {``"all"``, ``None``} → ``sql`` inchangé (toutes les
    branches). Lève ``KeyError`` si ``path_name`` est un nom de branche inconnu.
    """
    if path_name in (ALL_PATH, None):
        return sql

    paths = list_union_paths(query_decomposed, dialect)
    match = next((p for p in paths if p.name == path_name), None)
    if match is None:
        raise KeyError(f"path inconnu : {path_name!r}")

    root = sqlglot.parse_one(sql, read=dialect)
    order = [c.alias_or_name for c in root.ctes]
    cte_inner = {c.alias_or_name: c.this for c in root.ctes}
    code_by_name = {n: cte_inner[n].sql(dialect=dialect, pretty=True) for n in order}
    main_body = _strip_with(root)

    if match.host_cte == "final_query":
        union = _top_union(main_body)
        branch = _unwrap(_flatten_union(union)[match.branch_index])
        main_body = branch
    else:
        union = _top_union(cte_inner[match.host_cte])
        branch = _unwrap(_flatten_union(union)[match.branch_index])
        code_by_name[match.host_cte] = branch.sql(dialect=dialect, pretty=True)

    main_code = main_body.sql(dialect=dialect, pretty=True)
    kept = _prune_orphans(order, code_by_name, main_code, dialect)

    if not kept:
        return main_code
    with_parts = [f"{n} AS (\n{code_by_name[n]}\n)" for n in kept]
    return "WITH " + ",\n".join(with_parts) + "\n" + main_code


def slice_used_columns(
    used_columns: list[dict],
    sliced_sql: str,
    dialect: str = "bigquery",
) -> list[dict]:
    """Restreint ``used_columns`` aux tables encore référencées dans ``sliced_sql``.

    Sous-ensemble du ``used_columns`` complet → prompt de génération plus court et
    focalisé sur les colonnes de la branche ciblée (le levier qualité du slicing).
    """
    parsed = _parse(sliced_sql, dialect)
    if parsed is None:
        return list(used_columns or [])
    referenced = {(t.name or "").lower() for t in parsed.find_all(exp.Table)}
    return [
        entry
        for entry in (used_columns or [])
        if (entry.get("table") or "").lower() in referenced
    ]


def build_path_plans(
    optimized_sql: str,
    query_decomposed: list[dict],
    used_columns: list[dict],
    dialect: str = "bigquery",
) -> dict | None:
    """Catalogue des paths d'un UNION ALL de 1er niveau, construit UNE fois à la validation.

    ``{path_name: {sliced_sql, used_columns, branch_index, host_cte}, "all": {…}}``.
    Renvoie ``None`` si pas d'UNION ALL de 1er niveau exploitable (l'appelant ne persiste
    alors rien → comportement actuel inchangé). AST pur : pas de dry-run, pas de
    ré-extraction de colonnes — on filtre le ``used_columns`` déjà calculé.
    """
    paths = list_union_paths(query_decomposed, dialect)
    if not paths:
        return None

    # used_columns peut arriver en dicts (validator) OU en strings JSON (CLI / state) :
    # on normalise en dicts pour que slice_used_columns puisse filtrer par table.
    norm_used = [
        json.loads(c) if isinstance(c, str) else c for c in (used_columns or [])
    ]

    plans: dict[str, dict] = {}
    for p in paths:
        sliced = slice_to_path(optimized_sql, p.name, query_decomposed, dialect)
        plans[p.name] = {
            "sliced_sql": sliced,
            "used_columns": slice_used_columns(norm_used, sliced, dialect),
            "branch_index": p.branch_index,
            "host_cte": p.host_cte,
        }
    plans[ALL_PATH] = {
        "sliced_sql": optimized_sql,
        "used_columns": norm_used,
        "branch_index": None,
        "host_cte": None,
    }
    return plans


_UNSET = object()


def resolve_active_sql(state: dict, target_path=_UNSET) -> tuple[str, list[dict]]:
    """``(sql, used_columns)`` à utiliser pour générer/exécuter selon le path ciblé.

    ``target_path`` : override explicite (ex. le path **du test** lors d'un re-run de
    suite mixte) ; non fourni → lu depuis ``state["target_path"]``.

    Path ciblé valide dans ``path_plans`` → branche slicée + ``used_columns`` réduits ;
    sinon (``None``/``"all"``/catalogue absent/path inconnu) → ``optimized_sql`` complet.
    **N'écrase jamais ``optimized_sql``** (décision produit) : le slicé n'existe que dans
    la valeur de retour. Défensif : tout élément manquant retombe sur le complet.
    """
    optimized = state.get("optimized_sql") or ""
    full_used = [
        json.loads(c) if isinstance(c, str) else c
        for c in (state.get("used_columns") or [])
    ]
    target = state.get("target_path") if target_path is _UNSET else target_path
    if not target or target == ALL_PATH:
        return optimized, full_used

    raw = state.get("path_plans")
    if not raw:
        return optimized, full_used
    try:
        plans = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return optimized, full_used
    plan = plans.get(target) if isinstance(plans, dict) else None
    if not plan:
        return optimized, full_used
    return plan.get("sliced_sql") or optimized, plan.get("used_columns") or full_used
