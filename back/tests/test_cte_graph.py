"""Tests des fondations de la génération focalisée (build_query/cte_graph.py).

Fonctions pures — pas de DuckDB, pas de LLM. Couvre :
- build_cte_dependency_graph / transitive_deps / topo_sort (Étape 0a, spec §6.1)
- classify_blocking_ctes (Étape 0b, spec §7)
"""

import os

import pytest

import duckdb

from build_query.cte_graph import (
    build_cte_dependency_graph,
    build_required_dependency_graph,
    transitive_deps,
    topo_sort,
    classify_blocking_ctes,
    build_isolated_sql,
    reduce_used_columns,
    isolate_cte,
)
from build_query.query_chain import _lightweight_query_decomposed


# --------------------------------------------------------------------------- #
# Helpers / fixtures
# --------------------------------------------------------------------------- #

C1_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "..",
    "examples",
    "spider_complexified",
    "models",
    "c1.sql",
)


def _decompose(sql: str, dialect: str = "bigquery") -> list[dict]:
    import json

    return json.loads(_lightweight_query_decomposed(sql, dialect))


@pytest.fixture(scope="module")
def c1_decomposed() -> list[dict]:
    with open(C1_PATH, encoding="utf-8") as fh:
        sql = fh.read()
    decomposed = _decompose(sql)
    assert decomposed, "c1.sql devrait se décomposer en CTEs"
    return decomposed


# --------------------------------------------------------------------------- #
# 0a — DAG de dépendances
# --------------------------------------------------------------------------- #


def test_dependency_graph_simple_chain():
    sql = """
    WITH a AS (SELECT 1 AS x FROM base_table),
         b AS (SELECT x FROM a),
         c AS (SELECT x FROM b JOIN a USING (x))
    SELECT * FROM c
    """
    graph = build_cte_dependency_graph(_decompose(sql))
    assert graph["a"] == set()  # ne dépend que d'une table de base
    assert graph["b"] == {"a"}
    assert graph["c"] == {"a", "b"}
    assert graph["final_query"] == {"c"}


def test_transitive_deps_closure():
    sql = """
    WITH a AS (SELECT 1 AS x FROM base_table),
         b AS (SELECT x FROM a),
         c AS (SELECT x FROM b)
    SELECT * FROM c
    """
    graph = build_cte_dependency_graph(_decompose(sql))
    assert transitive_deps(graph, "c") == {"a", "b"}
    assert transitive_deps(graph, "b") == {"a"}
    assert transitive_deps(graph, "a") == set()


def test_topo_sort_deps_before_dependents():
    sql = """
    WITH a AS (SELECT 1 AS x FROM base_table),
         b AS (SELECT x FROM a),
         c AS (SELECT x FROM b)
    SELECT * FROM c
    """
    graph = build_cte_dependency_graph(_decompose(sql))
    order = topo_sort(graph)
    assert order.index("a") < order.index("b") < order.index("c")
    assert order.index("c") < order.index("final_query")


def test_topo_sort_detects_cycle():
    # Cycle artificiel : a→b, b→a (impossible via SQL réel, mais on protège la fn).
    graph = {"a": {"b"}, "b": {"a"}}
    with pytest.raises(ValueError):
        topo_sort(graph)


def test_dependency_closure_c1(c1_decomposed):
    """Sur c1 : tmp_final_bp dépend (transitivement) de rcomp, banques, coface…"""
    graph = build_cte_dependency_graph(c1_decomposed)
    names = {c["name"] for c in c1_decomposed}

    # TMP_FINAL_BP existe et dépend de RCOMP via un FROM direct.
    assert "TMP_FINAL_BP" in names
    deps_star = transitive_deps(graph, "TMP_FINAL_BP")
    assert "RCOMP" in deps_star
    assert "BANQUES" in deps_star
    assert "COFACE" in deps_star

    # Le tri topologique place les dépendances avant TMP_FINAL_BP.
    order = topo_sort(graph)
    for dep in ("RCOMP", "BANQUES", "COFACE"):
        assert order.index(dep) < order.index("TMP_FINAL_BP")


# --------------------------------------------------------------------------- #
# 0b — Classification bloquant / non-bloquant (spec §7)
# --------------------------------------------------------------------------- #


def test_blocking_from_inner_is_blocking():
    sql = """
    WITH a AS (SELECT 1 AS x FROM base_table),
         b AS (SELECT x FROM a INNER JOIN base2 USING (x))
    SELECT * FROM b
    """
    decomposed = _decompose(sql)
    trace = {"a": {"row_count": 0}, "b": {"row_count": 0}}
    blocking = classify_blocking_ctes(decomposed, trace)
    # a est en FROM de b → bloquante ; b est en FROM du final → bloquante.
    assert "a" in blocking
    assert "b" in blocking
    # ordre topo : a avant b
    assert blocking.index("a") < blocking.index("b")


def test_blocking_left_join_is_not_blocking():
    sql = """
    WITH base AS (SELECT id, v FROM base_table),
         lookup AS (SELECT id, label FROM ref_table)
    SELECT base.id, lookup.label
    FROM base
    LEFT JOIN lookup ON base.id = lookup.id
    """
    decomposed = _decompose(sql)
    # lookup est vide mais seulement LEFT-jointe → ne doit pas bloquer.
    trace = {"base": {"row_count": 5}, "lookup": {"row_count": 0}}
    blocking = classify_blocking_ctes(decomposed, trace)
    assert "lookup" not in blocking


def test_blocking_anti_join_not_in_is_not_blocking():
    sql = """
    WITH base AS (SELECT id FROM base_table),
         excluded AS (SELECT id FROM ref_table)
    SELECT id FROM base
    WHERE id NOT IN (SELECT id FROM excluded)
    """
    decomposed = _decompose(sql)
    trace = {"base": {"row_count": 5}, "excluded": {"row_count": 0}}
    blocking = classify_blocking_ctes(decomposed, trace)
    # excluded est en FROM mais sous NOT IN → anti-join → non bloquante.
    assert "excluded" not in blocking


def test_blocking_no_empty_ctes_returns_empty():
    sql = """
    WITH a AS (SELECT 1 AS x FROM base_table)
    SELECT * FROM a
    """
    decomposed = _decompose(sql)
    trace = {"a": {"row_count": 3}}
    assert classify_blocking_ctes(decomposed, trace) == []


def test_blocking_classification_c1(c1_decomposed):
    """Cas réel c1 : TMP_MR (LEFT JOIN) et SIRET_ONUS (anti-join LEFT) NON bloquants ;
    une CTE consommée en FROM (RCOMP) bloquante si vide."""
    names = {c["name"] for c in c1_decomposed}
    assert {"TMP_MR", "SIRET_ONUS", "RCOMP"} <= names

    # On simule un trace où tout le monde est vide pour isoler la classification.
    trace = {n: {"row_count": 0} for n in names if n != "final_query"}
    blocking = classify_blocking_ctes(c1_decomposed, trace)

    assert "TMP_MR" not in blocking, "TMP_MR n'est consommée qu'en LEFT JOIN"
    assert "SIRET_ONUS" not in blocking, "SIRET_ONUS est un anti-join LEFT"
    assert "RCOMP" in blocking, "RCOMP est consommée en FROM → bloquante si vide"


# --------------------------------------------------------------------------- #
# Étape 1 — Isolation de la CTE (spec §6.2-6.3)
# --------------------------------------------------------------------------- #

_ISO_SQL = """
WITH src_a AS (SELECT id, va FROM table_a),
     src_b AS (SELECT id, vb FROM table_b),
     joined AS (SELECT a.id AS id, a.va AS va, b.vb AS vb
                FROM src_a a JOIN src_b b USING (id))
SELECT id, va, vb FROM joined
"""

_ISO_USED_COLS = [
    {"project": "p", "database": "d", "table": "table_a", "used_columns": ["id", "va"]},
    {"project": "p", "database": "d", "table": "table_b", "used_columns": ["id", "vb"]},
]


def test_isolated_sql_executes_on_duckdb():
    decomposed = _decompose(_ISO_SQL, dialect="duckdb")
    sub_sql = build_isolated_sql("joined", decomposed, dialect="duckdb")

    con = duckdb.connect()
    con.execute("CREATE TABLE table_a AS SELECT 1 AS id, 10 AS va")
    con.execute("CREATE TABLE table_b AS SELECT 1 AS id, 100 AS vb")
    rows = con.execute(sub_sql).fetchall()
    assert rows == [(1, 10, 100)]


def test_isolated_sql_includes_only_closure(c1_decomposed):
    """La sous-requête pour TMP_FINAL_BP contient ses deps* et pas les CTEs hors closure."""
    graph = build_cte_dependency_graph(c1_decomposed)
    closure = transitive_deps(graph, "TMP_FINAL_BP") | {"TMP_FINAL_BP"}

    sub_sql = build_isolated_sql("TMP_FINAL_BP", c1_decomposed)
    declared = {c["name"] for c in c1_decomposed if f"{c['name']} AS (" in sub_sql}
    assert declared == closure
    # TMP_FINAL (qui dépend DE tmp_final_bp) ne doit pas y figurer.
    assert "final_query" not in declared
    assert sub_sql.rstrip().endswith("SELECT * FROM TMP_FINAL_BP")


def test_isolated_sql_unknown_cte_raises():
    decomposed = _decompose(_ISO_SQL, dialect="duckdb")
    with pytest.raises(KeyError):
        build_isolated_sql("does_not_exist", decomposed)


def test_reduce_used_columns_is_strict_subset():
    decomposed = _decompose(_ISO_SQL, dialect="duckdb")
    reduced = reduce_used_columns("src_a", decomposed, _ISO_USED_COLS, dialect="duckdb")
    tables = {e["table"] for e in reduced}
    assert tables == {"table_a"}
    assert len(reduced) < len(_ISO_USED_COLS)  # ⊊ strict


def test_reduce_used_columns_keeps_all_for_root():
    decomposed = _decompose(_ISO_SQL, dialect="duckdb")
    reduced = reduce_used_columns(
        "joined", decomposed, _ISO_USED_COLS, dialect="duckdb"
    )
    assert {e["table"] for e in reduced} == {"table_a", "table_b"}


def test_isolate_cte_orchestrator():
    decomposed = _decompose(_ISO_SQL, dialect="duckdb")
    out = isolate_cte("src_b", decomposed, _ISO_USED_COLS, dialect="duckdb")
    assert set(out) == {"focus_sql", "focus_used_columns", "sub_ctes"}
    assert {e["table"] for e in out["focus_used_columns"]} == {"table_b"}
    assert out["sub_ctes"] == ["src_b"]
    assert "FROM src_b" in out["focus_sql"]


# --------------------------------------------------------------------------- #
# Raffinement « arête requise » : LEFT forçant + anti-joins élargis (§7)
# --------------------------------------------------------------------------- #


def test_forcing_left_join_is_blocking():
    """LEFT JOIN avec prédicat forçant dans le WHERE → de facto INNER → bloquant."""
    sql = """
    WITH base AS (SELECT id, v FROM base_table),
         table_x AS (SELECT id, status FROM ref_table)
    SELECT base.id
    FROM base
    LEFT JOIN table_x x ON base.id = x.id
    WHERE x.status = 'active'
    """
    decomposed = _decompose(sql)
    trace = {"base": {"row_count": 5}, "table_x": {"row_count": 0}}
    assert "table_x" in classify_blocking_ctes(decomposed, trace)


def test_forcing_left_join_c1_reseau_is_blocking(c1_decomposed):
    """c1 : RESEAU est LEFT-jointe mais WHERE RESEAU.reseau IN (...) la rend bloquante."""
    names = {c["name"] for c in c1_decomposed}
    trace = {n: {"row_count": 0} for n in names if n != "final_query"}
    blocking = classify_blocking_ctes(c1_decomposed, trace)
    assert "RESEAU" in blocking
    # … sans casser les non-bloquants
    assert "TMP_MR" not in blocking
    assert "SIRET_ONUS" not in blocking


@pytest.mark.parametrize(
    "where",
    [
        "id <> ALL (SELECT id FROM excluded)",
        "NOT (id IN (SELECT id FROM excluded))",
        "NOT EXISTS (SELECT 1 FROM excluded e WHERE e.id = base.id)",
    ],
)
def test_broadened_anti_joins_are_not_blocking(where):
    sql = f"""
    WITH base AS (SELECT id FROM base_table),
         excluded AS (SELECT id FROM ref_table)
    SELECT id FROM base WHERE {where}
    """
    decomposed = _decompose(sql)
    trace = {"base": {"row_count": 5}, "excluded": {"row_count": 0}}
    assert "excluded" not in classify_blocking_ctes(decomposed, trace)


# --------------------------------------------------------------------------- #
# Réduction du périmètre : ne pas générer les tables LEFT optionnelles (§6.3)
# --------------------------------------------------------------------------- #

_OPT_SQL = """
WITH base AS (SELECT id, v FROM table_base),
     opt AS (SELECT id, label FROM table_opt),
     joined AS (
        SELECT base.id, base.v, o.label
        FROM base
        LEFT JOIN opt o ON base.id = o.id
     )
SELECT * FROM joined
"""

_OPT_USED_COLS = [
    {
        "project": "p",
        "database": "d",
        "table": "table_base",
        "used_columns": ["id", "v"],
    },
    {
        "project": "p",
        "database": "d",
        "table": "table_opt",
        "used_columns": ["id", "label"],
    },
]


def test_reduce_excludes_optional_left_joined_table():
    """opt est LEFT-jointe sans filtre → table_opt ne doit PAS être générée."""
    decomposed = _decompose(_OPT_SQL, dialect="duckdb")
    reduced = reduce_used_columns(
        "joined", decomposed, _OPT_USED_COLS, dialect="duckdb"
    )
    assert {e["table"] for e in reduced} == {"table_base"}


def test_reduce_keeps_forcing_left_joined_table():
    """Avec un WHERE forçant sur opt, table_opt redevient requise."""
    sql = _OPT_SQL.replace(
        "LEFT JOIN opt o ON base.id = o.id\n     )",
        "LEFT JOIN opt o ON base.id = o.id\n        WHERE o.label = 'x'\n     )",
    )
    decomposed = _decompose(sql, dialect="duckdb")
    reduced = reduce_used_columns(
        "joined", decomposed, _OPT_USED_COLS, dialect="duckdb"
    )
    assert {e["table"] for e in reduced} == {"table_base", "table_opt"}


def test_isolated_sql_keeps_optional_cte_for_validity():
    """Même exclue de la génération, opt reste dans le WITH (validité SQL) → NULLs."""
    decomposed = _decompose(_OPT_SQL, dialect="duckdb")
    sub_sql = build_isolated_sql("joined", decomposed, dialect="duckdb")
    assert "opt AS (" in sub_sql  # structurellement présent

    con = duckdb.connect()
    con.execute("CREATE TABLE table_base AS SELECT 1 AS id, 10 AS v")
    con.execute("CREATE TABLE table_opt(id INT, label VARCHAR)")  # vide
    rows = con.execute(sub_sql).fetchall()
    assert rows == [(1, 10, None)]  # LEFT JOIN → label NULL


def test_required_graph_drops_optional_edges():
    decomposed = _decompose(_OPT_SQL, dialect="duckdb")
    full = build_cte_dependency_graph(decomposed, dialect="duckdb")
    req = build_required_dependency_graph(decomposed, dialect="duckdb")
    assert "opt" in full["joined"]  # arête structurelle présente
    assert "opt" not in req["joined"]  # mais pas requise
    assert "base" in req["joined"]
