"""Régression : UNNEST(arr) WITH OFFSET AS offset (BigQuery) → exécutable sur DuckDB.

sqlglot transpile ``WITH OFFSET AS offset`` en ``WITH ORDINALITY`` en perdant le
nom de la colonne (DuckDB : "Referenced column offset not found") et change la base
(0-based BigQuery → 1-based DuckDB). ``_fix_unnest_with_offset`` réécrit l'UNNEST en
sous-requête corrélée réexposant un ``offset`` 0-based sous son nom d'origine.
"""

import duckdb
import sqlglot

from utils.examples import _fix_unnest_with_offset


def _to_duck(bq_sql: str) -> str:
    tree = sqlglot.parse_one(bq_sql, read="bigquery")
    _fix_unnest_with_offset(tree)
    return tree.sql(dialect="duckdb")


def test_offset_is_zero_based_and_binds():
    bq = """
    WITH fhs AS (SELECT 'A' AS k, [10, 20, 30] AS lags)
    SELECT k, offset, offset + 1 AS o, lags AS preceding
    FROM fhs, UNNEST(fhs.lags) WITH OFFSET AS offset
    """
    duck = _to_duck(bq)
    rows = duckdb.connect().execute(duck).fetchall()
    # offset 0-based : {0,1,2} (et non {1,2,3}) ; offset+1 → {1,2,3}.
    offsets = sorted(r[1] for r in rows)
    assert offsets == [0, 1, 2]
    assert sorted(r[2] for r in rows) == [1, 2, 3]
    # Le tableau d'origine reste accessible.
    assert all(r[3] == [10, 20, 30] for r in rows)


def test_multiple_unnests_get_distinct_aliases():
    bq = """
    WITH t AS (SELECT [1, 2] AS a, [9, 8] AS b)
    SELECT o1, o2
    FROM t, UNNEST(t.a) WITH OFFSET AS o1, UNNEST(t.b) WITH OFFSET AS o2
    """
    duck = _to_duck(bq)
    rows = duckdb.connect().execute(duck).fetchall()
    assert len(rows) == 4  # produit cartésien 2×2
    assert {r[0] for r in rows} == {0, 1}
    assert {r[1] for r in rows} == {0, 1}


def test_unnest_without_offset_is_untouched():
    bq = "SELECT x FROM t, UNNEST(t.arr) AS x"
    tree = sqlglot.parse_one(bq, read="bigquery")
    before = tree.sql(dialect="duckdb")
    _fix_unnest_with_offset(tree)
    assert tree.sql(dialect="duckdb") == before
