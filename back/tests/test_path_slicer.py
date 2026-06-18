"""Tests du path-slicer UNION ALL (génération focalisée par branche).

Test-before-fix : écrits avant le câblage. Fixtures exécutables sur DuckDB pour
vérifier que le SQL slicé reste valide.
"""

import duckdb
import pytest
import sqlglot

import json

from build_query.path_slicer import (
    build_path_plans,
    list_union_paths,
    resolve_active_sql,
    slice_to_path,
    slice_used_columns,
)


def _decompose(sql: str, dialect: str = "duckdb") -> list[dict]:
    """Mini-décomposition {name, code} équivalente à validator.split_query.

    Suffisant pour les fonctions pures du path_slicer (qui n'utilisent que
    name + code). Le dernier nœud est `final_query`.
    """
    parsed = sqlglot.parse_one(sql, read=dialect)
    nodes: list[dict] = []
    for cte in parsed.ctes:
        nodes.append({"name": cte.alias_or_name, "code": cte.this.sql(dialect=dialect)})
    body = parsed.copy()
    body.args.pop("with", None)
    body.args.pop("with_", None)
    nodes.append({"name": "final_query", "code": body.sql(dialect=dialect)})
    return nodes


# --- Fixtures (exécutables sur DuckDB, sans tables externes) --------------------------

# UNION ALL dans une CTE, 3 branches lisant des CTE propres disjointes
THREE_BRANCH = """
WITH a AS (SELECT 1 AS id),
     b AS (SELECT 2 AS id),
     c AS (SELECT 3 AS id),
     combined AS (
         SELECT id, 'a' AS kind FROM a
         UNION ALL
         SELECT id, 'b' AS kind FROM b
         UNION ALL
         SELECT id, 'c' AS kind FROM c
     )
SELECT id, kind FROM combined ORDER BY id
"""

# UNION ALL au niveau de la requête finale
FINAL_UNION = """
WITH a AS (SELECT 1 AS id),
     b AS (SELECT 2 AS id)
SELECT id FROM a
UNION ALL
SELECT id FROM b
"""

# Pas d'UNION ALL
NO_UNION = """
WITH a AS (SELECT 1 AS id)
SELECT id FROM a
"""

# UNION imbriqué dans une sous-requête FROM (pas un set-op de 1er niveau)
NESTED_UNION = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT t.id FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS t
"""

# UNION DISTINCT (pas UNION ALL)
UNION_DISTINCT = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT id FROM a
UNION
SELECT id FROM b
"""

# Deux CTE chacune avec un UNION ALL de 1er niveau (ambigu → path all)
TWO_UNIONS = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id),
     u1 AS (SELECT id FROM a UNION ALL SELECT id FROM b),
     u2 AS (SELECT id FROM a UNION ALL SELECT id FROM b)
SELECT u1.id FROM u1 JOIN u2 USING (id)
"""


# --- list_union_paths -----------------------------------------------------------------


def test_list_union_paths_three_branches():
    paths = list_union_paths(_decompose(THREE_BRANCH))
    assert [p.name for p in paths] == ["a", "b", "c"]
    assert [p.branch_index for p in paths] == [0, 1, 2]
    assert {p.host_cte for p in paths} == {"combined"}


def test_list_union_paths_final_query_host():
    paths = list_union_paths(_decompose(FINAL_UNION))
    assert [p.name for p in paths] == ["a", "b"]
    assert {p.host_cte for p in paths} == {"final_query"}


def test_list_union_paths_no_union():
    assert list_union_paths(_decompose(NO_UNION)) == []


def test_list_union_paths_nested_union_bails():
    assert list_union_paths(_decompose(NESTED_UNION)) == []


def test_list_union_paths_union_distinct_bails():
    assert list_union_paths(_decompose(UNION_DISTINCT)) == []


def test_list_union_paths_multiple_unions_bails():
    assert list_union_paths(_decompose(TWO_UNIONS)) == []


# --- slice_to_path --------------------------------------------------------------------


def test_slice_to_path_keeps_only_target_branch_and_prunes_orphans():
    sliced = slice_to_path(THREE_BRANCH, "a", _decompose(THREE_BRANCH), "duckdb")
    parsed = sqlglot.parse_one(sliced, read="duckdb")
    # Plus aucun set-op
    assert not list(parsed.find_all(sqlglot.exp.Union))
    # CTE orphelines b et c retirées du WITH ; a et combined conservées
    cte_names = {c.alias_or_name for c in parsed.ctes}
    assert "a" in cte_names and "combined" in cte_names
    assert "b" not in cte_names and "c" not in cte_names


def test_slice_to_path_executable_on_duckdb():
    sliced = slice_to_path(THREE_BRANCH, "b", _decompose(THREE_BRANCH), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(2, "b")]


def test_slice_to_path_final_query_host():
    sliced = slice_to_path(FINAL_UNION, "a", _decompose(FINAL_UNION), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(1,)]
    assert not list(
        sqlglot.parse_one(sliced, read="duckdb").find_all(sqlglot.exp.Union)
    )


def test_slice_to_path_unknown_path_raises():
    with pytest.raises((KeyError, ValueError)):
        slice_to_path(THREE_BRANCH, "zzz", _decompose(THREE_BRANCH), "duckdb")


def test_slice_to_path_all_is_noop():
    # "all" et None → SQL inchangé sémantiquement (toutes les branches)
    sliced = slice_to_path(THREE_BRANCH, "all", _decompose(THREE_BRANCH), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


# --- slice_used_columns ---------------------------------------------------------------


def test_slice_used_columns_drops_other_branch_tables():
    # used_columns sur des tables de base ; après slice sur la branche 'a',
    # seules les tables encore référencées survivent.
    used = [
        {"project": "p", "database": "d", "table": "a", "used_columns": ["id"]},
        {"project": "p", "database": "d", "table": "b", "used_columns": ["id"]},
        {"project": "p", "database": "d", "table": "c", "used_columns": ["id"]},
    ]
    sliced = slice_to_path(THREE_BRANCH, "a", _decompose(THREE_BRANCH), "duckdb")
    reduced = slice_used_columns(used, sliced)
    tables = {e["table"] for e in reduced}
    assert "a" in tables
    assert "b" not in tables and "c" not in tables
    assert len(reduced) < len(used)


# --- build_path_plans -----------------------------------------------------------------


def test_build_path_plans_three_branches_plus_all():
    used = [
        {"table": "a", "used_columns": ["id"]},
        {"table": "b", "used_columns": ["id"]},
        {"table": "c", "used_columns": ["id"]},
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    assert set(plans) == {"a", "b", "c", "all"}
    # path 'a' : SQL slicé exécutable, used_columns réduit
    assert duckdb.sql(plans["a"]["sliced_sql"]).fetchall() == [(1, "a")]
    assert {e["table"] for e in plans["a"]["used_columns"]} == {"a"}
    # 'all' : SQL complet + used_columns complet
    assert plans["all"]["sliced_sql"] == THREE_BRANCH
    assert len(plans["all"]["used_columns"]) == 3


def test_build_path_plans_none_without_union():
    assert build_path_plans(NO_UNION, _decompose(NO_UNION), [], "duckdb") is None


def test_build_path_plans_tolerates_json_string_used_columns():
    # En CLI, state["used_columns"] est une liste de strings JSON (pas de dicts).
    # build_path_plans doit normaliser sans planter (régression e2e bq002).
    used = [
        json.dumps({"table": "a", "used_columns": ["id"]}),
        json.dumps({"table": "b", "used_columns": ["id"]}),
        json.dumps({"table": "c", "used_columns": ["id"]}),
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    assert set(plans) == {"a", "b", "c", "all"}
    assert {e["table"] for e in plans["a"]["used_columns"]} == {"a"}


# --- resolve_active_sql ---------------------------------------------------------------


def test_resolve_active_sql_returns_sliced_for_target_path():
    used = [
        {"table": "a", "used_columns": ["id"]},
        {"table": "b", "used_columns": ["id"]},
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    state = {
        "optimized_sql": THREE_BRANCH,
        "used_columns": [json.dumps(u) for u in used],
        "path_plans": json.dumps(plans),
        "target_path": "b",
    }
    sql, uc = resolve_active_sql(state)
    assert duckdb.sql(sql).fetchall() == [(2, "b")]
    assert {e["table"] for e in uc} == {"b"}


def test_resolve_active_sql_falls_back_to_full_for_all_or_missing():
    used = [{"table": "a", "used_columns": ["id"]}]
    base = {
        "optimized_sql": THREE_BRANCH,
        "used_columns": [json.dumps(u) for u in used],
        "path_plans": None,
    }
    # target "all" → complet
    sql, _ = resolve_active_sql({**base, "target_path": "all"})
    assert sql == THREE_BRANCH
    # target None → complet
    sql, _ = resolve_active_sql({**base, "target_path": None})
    assert sql == THREE_BRANCH
    # path inconnu + catalogue absent → complet (défensif)
    sql, _ = resolve_active_sql({**base, "target_path": "zzz"})
    assert sql == THREE_BRANCH


# --- régression -----------------------------------------------------------------------


def test_no_union_query_unaffected():
    assert list_union_paths(_decompose(NO_UNION)) == []
