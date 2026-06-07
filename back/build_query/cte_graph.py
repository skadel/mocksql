"""Dépendances et classification des CTEs — fondations de la génération focalisée.

Toutes les fonctions de ce module sont **pures** : aucun effet de bord, aucun appel
réseau, aucune dépendance au state LangGraph. Elles opèrent sur `query_decomposed`
(liste de `{name, code, dependencies, sources}` produite par le validator / le
fallback `_lightweight_query_decomposed`) et, pour la classification, sur le
`cte_trace` produit par l'executor (`{cte_name: {"row_count": N, ...}}`).

Voir docs/spec-focused-cte-generation.md §6.1 (DAG) et §7 (classification bloquante).

Étape 0 du plan d'implémentation : zéro impact sur le chemin de prod tant que ces
fonctions ne sont pas câblées dans le graphe.
"""

from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

# Le `final_query` produit par `_lightweight_query_decomposed` conserve un `WITH`
# vide (les CTEs ont été détachées) — non re-parsable tel quel. On le retire.
_EMPTY_WITH = re.compile(r"^\s*WITH\s+(?=SELECT\b)", re.IGNORECASE)


def _parse(code: str, dialect: str) -> exp.Expression | None:
    try:
        return sqlglot.parse_one(code, read=dialect)
    except Exception:
        cleaned = _EMPTY_WITH.sub("", code)
        if cleaned != code:
            try:
                return sqlglot.parse_one(cleaned, read=dialect)
            except Exception:
                return None
        return None


def _select_from(select: exp.Select) -> exp.Expression | None:
    """Clause FROM d'un SELECT, tolérante aux variations de clé sqlglot (`from`/`from_`)."""
    return select.args.get("from") or select.args.get("from_")


def build_cte_dependency_graph(
    query_decomposed: list[dict], dialect: str = "bigquery"
) -> dict[str, set[str]]:
    """Map chaque CTE → l'ensemble des CTEs dont elle dépend **directement**.

    Pour chaque CTE on parse son `code` et on collecte les `exp.Table` dont le nom
    (insensible à la casse) correspond à un autre nom de CTE. Le nœud `final_query`,
    s'il est présent, est inclus comme n'importe quel autre nœud.

    `dependencies` de `query_decomposed` n'est jamais peuplé en amont (cf.
    query_chain.py / validator.py) — d'où le recalcul ici (spec §6.1).

    Limitation connue (MVP) : un `WITH` imbriqué dans le `code` d'une CTE dont un
    nom local entrerait en collision avec un nom de CTE de premier niveau pourrait
    produire un faux arc. Cas rare, accepté pour le MVP.
    """
    names = {c["name"] for c in query_decomposed}
    lower_to_canon = {n.lower(): n for n in names}

    graph: dict[str, set[str]] = {}
    for cte in query_decomposed:
        deps: set[str] = set()
        parsed = _parse(cte["code"], dialect)
        if parsed is not None:
            for tbl in parsed.find_all(exp.Table):
                canon = lower_to_canon.get((tbl.name or "").lower())
                if canon and canon != cte["name"]:
                    deps.add(canon)
        graph[cte["name"]] = deps
    return graph


def transitive_deps(graph: dict[str, set[str]], node: str) -> set[str]:
    """Fermeture transitive `deps*(node)` : toutes les CTEs atteignables depuis `node`.

    N'inclut pas `node` lui-même.
    """
    seen: set[str] = set()
    stack = list(graph.get(node, ()))
    while stack:
        n = stack.pop()
        if n in seen:
            continue
        seen.add(n)
        stack.extend(graph.get(n, ()))
    seen.discard(node)
    return seen


def topo_sort(graph: dict[str, set[str]]) -> list[str]:
    """Ordre topologique : les dépendances apparaissent **avant** leurs dépendants.

    Déterministe (parcours trié). Lève `ValueError` si le graphe contient un cycle.
    """
    DONE, IN_STACK = 2, 1
    visited: dict[str, int] = {}
    order: list[str] = []

    def visit(n: str) -> None:
        state = visited.get(n, 0)
        if state == DONE:
            return
        if state == IN_STACK:
            raise ValueError(f"cycle de CTEs détecté impliquant {n!r}")
        visited[n] = IN_STACK
        for dep in sorted(graph.get(n, ())):
            visit(dep)
        visited[n] = DONE
        order.append(n)

    for n in sorted(graph):
        visit(n)
    return order


def _anti_join_table_ids(parsed: exp.Expression) -> set[int]:
    """`id()` des `exp.Table` situées sous un anti-join ensembliste.

    Couvre : `NOT IN (subquery)`, `NOT EXISTS (...)`, `NOT (… IN/EXISTS …)`
    (parenthèses), et `x <> ALL (subquery)` / `x != ALL (subquery)`. Une source
    vide y *augmente* le résultat — elle ne doit jamais être ciblée ni comptée
    comme requise.

    Les anti-joins par `LEFT JOIN … WHERE alias.col IS NULL` ne sont PAS traités
    ici : ils relèvent de `_detect_anti_join_aliases` (cf. `_forced_outer_aliases`).
    """
    ids: set[int] = set()

    def add(node: exp.Expression) -> None:
        for tbl in node.find_all(exp.Table):
            ids.add(id(tbl))

    for not_node in parsed.find_all(exp.Not):
        inner = not_node.this
        while isinstance(inner, exp.Paren):
            inner = inner.this
        if isinstance(inner, (exp.In, exp.Exists)):
            add(inner)

    for neq in parsed.find_all(exp.NEQ):
        if isinstance(neq.expression, exp.All):
            add(neq.expression)

    return ids


def _forced_outer_aliases(select: exp.Select) -> set[str]:
    """Aliases de tables LEFT/RIGHT/FULL-jointes rendus **forçants** par le WHERE.

    Un OUTER JOIN se comporte comme un INNER JOIN dès qu'un prédicat non
    null-tolérant porte sur son alias (`x.col = 'v'`, `x.col IS NOT NULL`,
    `x.col IN (...)`). Les lignes non-matchées (colonnes NULL) sont alors filtrées.

    Forçant = (aliases OUTER référencés dans le WHERE) − (aliases anti-join
    `IS NULL`, via `_detect_anti_join_aliases`). Conservateur : un alias présent
    à la fois en `IS NULL` et ailleurs est considéré non forçant.
    """
    outer: set[str] = set()
    for join in select.args.get("joins") or []:
        if (join.args.get("side") or "").upper() in {"LEFT", "RIGHT", "FULL"}:
            src = join.this
            if isinstance(src, exp.Table):
                alias = (src.alias or src.name).lower()
                if alias:
                    outer.add(alias)
    if not outer:
        return set()

    where = select.args.get("where")
    referenced: set[str] = set()
    if where:
        for col in where.find_all(exp.Column):
            tbl = (col.table or "").lower()
            if tbl in outer:
                referenced.add(tbl)

    from build_query.constraint_simplifier import _detect_anti_join_aliases

    return referenced - _detect_anti_join_aliases(select)


def _required_table_names(parsed: exp.Expression) -> set[str]:
    """Noms (minuscules) des tables consommées sur un chemin **requis**.

    Requis = source directe d'un `FROM`, ou JOIN INNER/CROSS, ou OUTER JOIN
    forçant (`_forced_outer_aliases`). Exclut les anti-joins ensemblistes
    (`_anti_join_table_ids`). Inclut tables de base *et* CTEs — le filtrage
    base/CTE est fait par l'appelant.
    """
    anti_ids = _anti_join_table_ids(parsed)
    names: set[str] = set()

    def mark(tbl: exp.Table) -> None:
        if id(tbl) in anti_ids:
            return
        n = (tbl.name or "").lower()
        if n:
            names.add(n)

    for select in parsed.find_all(exp.Select):
        forced = _forced_outer_aliases(select)
        from_ = _select_from(select)
        # Source directe du FROM uniquement (les sous-requêtes en FROM sont
        # traitées par leur propre itération `find_all(exp.Select)`).
        if from_ is not None and isinstance(from_.this, exp.Table):
            mark(from_.this)
        for join in select.args.get("joins") or []:
            src = join.this
            if not isinstance(src, exp.Table):
                continue
            if (join.args.get("side") or "").upper() in {"LEFT", "RIGHT", "FULL"}:
                if (src.alias or src.name).lower() not in forced:
                    continue
            mark(src)

    return names


def build_required_dependency_graph(
    query_decomposed: list[dict], dialect: str = "bigquery"
) -> dict[str, set[str]]:
    """Comme `build_cte_dependency_graph`, mais ne garde que les arêtes **requises**.

    Une arête `parent → child` n'existe que si `child` est consommée par `parent`
    sur un chemin requis (FROM / INNER / OUTER forçant), pas via un LEFT JOIN
    optionnel ni un anti-join. Base de la classification bloquante (§7) et de la
    réduction du périmètre d'isolation (§6.3).
    """
    names = {c["name"] for c in query_decomposed}
    lower_to_canon = {n.lower(): n for n in names}

    graph: dict[str, set[str]] = {}
    for cte in query_decomposed:
        deps: set[str] = set()
        parsed = _parse(cte["code"], dialect)
        if parsed is not None:
            for tname in _required_table_names(parsed):
                canon = lower_to_canon.get(tname)
                if canon and canon != cte["name"]:
                    deps.add(canon)
        graph[cte["name"]] = deps
    return graph


def _sub_closure(failing_cte: str, graph: dict[str, set[str]]) -> list[str]:
    """Sous-graphe `deps*(failing_cte) ∪ {failing_cte}`, trié topologiquement."""
    needed = transitive_deps(graph, failing_cte) | {failing_cte}
    sub_graph = {n: graph.get(n, set()) & needed for n in needed}
    return topo_sort(sub_graph)


def build_isolated_sql(
    failing_cte: str,
    query_decomposed: list[dict],
    dialect: str = "bigquery",
) -> str:
    """Sous-requête tronquée : `WITH <deps* topo-triées + failing_cte> SELECT * FROM failing_cte`.

    Mirroir minimal de `_run_cte_trace` (examples_executor.py:654) mais restreint à
    la **fermeture transitive** de `failing_cte` (et non à tous les CTEs déclarés
    avant lui). Noms de CTE **nus** (valides DuckDB *et* BigQuery) ; le câblage prod
    passera le résultat par `run_query_on_test_dataset` pour le transpile dialecte.

    Lève `KeyError` si `failing_cte` est inconnu.
    """
    code_by_name = {c["name"]: c["code"] for c in query_decomposed}
    if failing_cte not in code_by_name:
        raise KeyError(f"CTE inconnue : {failing_cte!r}")

    graph = build_cte_dependency_graph(query_decomposed, dialect)
    ordered = _sub_closure(failing_cte, graph)

    with_parts = [f"{name} AS (\n{code_by_name[name]}\n)" for name in ordered]
    return "WITH " + ",\n".join(with_parts) + f"\nSELECT * FROM {failing_cte}"


def _required_source_tables(
    failing_cte: str,
    query_decomposed: list[dict],
    dialect: str = "bigquery",
) -> set[str]:
    """Noms (minuscules) des **tables de base** à générer pour `failing_cte`.

    Closure transitive sur le graphe **requis** (`build_required_dependency_graph`)
    puis, dans chaque CTE de cette closure, on ne retient que les tables de base
    consommées sur un chemin requis (`_required_table_names`).

    Conséquence voulue : une table (ou un sous-arbre) consommée uniquement via un
    LEFT JOIN optionnel ou un anti-join n'est **pas** générée — « pas d'intérêt à
    la générer dans le débogage ». Elle reste dans le `WITH` de `build_isolated_sql`
    (validité SQL) mais vide → le LEFT JOIN produit des NULLs.
    """
    cte_names = {c["name"].lower() for c in query_decomposed}
    code_by_name = {c["name"]: c["code"] for c in query_decomposed}
    req_graph = build_required_dependency_graph(query_decomposed, dialect)
    needed = transitive_deps(req_graph, failing_cte) | {failing_cte}

    sources: set[str] = set()
    for name in needed:
        parsed = _parse(code_by_name.get(name, ""), dialect)
        if parsed is None:
            continue
        for tname in _required_table_names(parsed):
            if tname not in cte_names:
                sources.add(tname)
    return sources


def reduce_used_columns(
    failing_cte: str,
    query_decomposed: list[dict],
    used_columns: list[dict],
    dialect: str = "bigquery",
) -> list[dict]:
    """Restreint `used_columns` aux tables **requises** de la closure de `failing_cte`.

    `used_columns` : liste de `{project, database, table, used_columns, …}`
    (convention du projet, cf. CLAUDE.md). On filtre par nom de table (insensible
    à la casse), sur la closure du graphe requis → les tables LEFT-optionnelles /
    anti-jointes sont exclues de la génération. Sous-ensemble du `used_columns`
    complet → prompt de génération plus court (spec §6.3).
    """
    sources = _required_source_tables(failing_cte, query_decomposed, dialect)
    return [
        entry
        for entry in (used_columns or [])
        if (entry.get("table") or "").lower() in sources
    ]


def isolate_cte(
    failing_cte: str,
    query_decomposed: list[dict],
    used_columns: list[dict],
    dialect: str = "bigquery",
    schema: dict | None = None,
) -> dict:
    """Isole `failing_cte` pour la génération focalisée (spec §5.1 / §6.2-6.3).

    Retourne `{"focus_sql", "focus_used_columns", "sub_ctes"}`.

    Si `schema` (mapping `{table: {col: type}}` attendu par `optimize_query`) est
    fourni, la sous-requête est qualifiée/optimisée via le même chemin que la
    validation (`validator.optimize_query`). En cas d'échec de qualification
    (R1 : alias résolus plus haut, `QUALIFY`…), on retombe sur le SQL non qualifié.
    """
    sub_sql = build_isolated_sql(failing_cte, query_decomposed, dialect)
    focus_used_columns = reduce_used_columns(
        failing_cte, query_decomposed, used_columns, dialect
    )
    graph = build_cte_dependency_graph(query_decomposed, dialect)
    sub_ctes = _sub_closure(failing_cte, graph)

    if schema is not None:
        try:
            from build_query.validator import optimize_query

            parsed = sqlglot.parse_one(sub_sql, read=dialect)
            optimized = optimize_query(parsed, schema, dialect=dialect)
            sub_sql = optimized.sql(dialect=dialect, pretty=True)
        except Exception:
            pass  # fallback : SQL non qualifié (pattern du CLI, spec R1)

    return {
        "focus_sql": sub_sql,
        "focus_used_columns": focus_used_columns,
        "sub_ctes": sub_ctes,
    }


def classify_blocking_ctes(
    query_decomposed: list[dict],
    cte_trace: dict | None,
    dialect: str = "bigquery",
) -> list[str]:
    """CTEs **vides ET réellement bloquantes**, en ordre topologique (spec §7).

    Une CTE vide ne bloque le résultat que si elle est **atteignable depuis le
    résultat final par des arêtes requises** : `FROM` / `INNER` / `CROSS` / OUTER
    JOIN forçant. Les CTEs consommées uniquement via `LEFT/RIGHT/FULL JOIN`
    optionnel ou en anti-join (`IS NULL`, `NOT IN`, `NOT EXISTS`, `<> ALL`) ne
    bloquent pas — et ne doivent jamais être ciblées (cf. `TMP_MR` / `SIRET_ONUS`
    dans c1). Inversement un OUTER JOIN avec prédicat forçant (`WHERE x.col = …`)
    **est** bloquant (cf. `RESEAU` dans c1).

    L'atteignabilité depuis `final_query` propage la requiredness transitivement :
    une CTE seulement LEFT-jointe ne « contamine » pas ses propres dépendances.

    Corrige le diagnostic actuel du `cte_trace`, qui étiquette à tort ces CTEs
    comme « filtre bloquant ».
    """
    empty = {
        name
        for name, info in (cte_trace or {}).items()
        if isinstance(info, dict) and info.get("row_count") == 0
    }
    if not empty:
        return []

    req_graph = build_required_dependency_graph(query_decomposed, dialect)

    # Atteignabilité depuis le résultat final via les arêtes requises.
    if "final_query" in req_graph:
        reachable = transitive_deps(req_graph, "final_query") | {"final_query"}
    else:
        # Pas de nœud final identifiable : retombe sur « requise quelque part ».
        reachable = {dep for deps in req_graph.values() for dep in deps}

    order = topo_sort(build_cte_dependency_graph(query_decomposed, dialect))
    return [n for n in order if n in empty and n in reachable]
