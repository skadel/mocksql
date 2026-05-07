"""
Tests for _qualify_group_order_by_aliases — résolution des ambiguïtés GROUP BY / ORDER BY
entre BigQuery et DuckDB lorsque des colonnes partagées entre plusieurs CTEs jointes
rendent une référence bare ambiguë côté DuckDB.

Pipeline de référence :
    bq_sql → sqlglot.parse_one(dialect='bigquery') → _qualify_group_order_by_aliases()
           → .sql(dialect='duckdb') → DuckDB execute (doit réussir)

Le canary test vérifie que sqlglot ne corrige pas encore ce cas nativement.
"""

import duckdb
import pytest
import sqlglot

from utils.examples import _qualify_group_order_by_aliases


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

BQ_SQL_AMBIGUOUS = """
WITH cte_a AS (
    SELECT a.year AS year, SUM(a.value) AS total_a
    FROM `proj.ds.table_a` AS a
    GROUP BY a.year
),
cte_b AS (
    SELECT b.year AS year, SUM(b.value) AS total_b
    FROM `proj.ds.table_b` AS b
    GROUP BY b.year
)
SELECT
    t.year AS year,
    t.total_a,
    b.total_b
FROM cte_a AS t
JOIN cte_b AS b ON t.year = b.year
GROUP BY year, b.total_b
ORDER BY year
"""


def _transpile_with_fix(bq_sql: str) -> str:
    tree = sqlglot.parse_one(bq_sql, dialect="bigquery")
    _qualify_group_order_by_aliases(tree)
    return tree.sql(dialect="duckdb")


def _transpile_without_fix(bq_sql: str) -> str:
    return sqlglot.parse_one(bq_sql, dialect="bigquery").sql(dialect="duckdb")


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE cte_a AS
        SELECT 2020 AS year, 100 AS total_a, 50 AS total_b
    """)
    c.execute("""
        CREATE TABLE cte_b AS
        SELECT 2020 AS year, 200 AS total_b
    """)
    return c


# ---------------------------------------------------------------------------
# Tests fonctionnels
# ---------------------------------------------------------------------------


def test_group_by_alias_ambiguous_join():
    """
    GROUP BY year doit devenir GROUP BY t.year quand SELECT a t.year AS year
    et qu'un JOIN expose aussi b.year.
    """
    sql = """
        SELECT t.year AS year, SUM(t.v) AS s
        FROM cte_a AS t
        JOIN cte_b AS b ON t.year = b.year
        GROUP BY year
    """
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    _qualify_group_order_by_aliases(tree)
    result = tree.sql(dialect="duckdb")

    assert "GROUP BY" in result
    assert "t.year" in result.lower() or "t.`year`" in result.lower()


def test_order_by_alias_ambiguous_join():
    """
    ORDER BY year doit devenir ORDER BY t.year dans le même contexte.
    """
    sql = """
        SELECT t.year AS year, SUM(t.v) AS s
        FROM cte_a AS t
        JOIN cte_b AS b ON t.year = b.year
        GROUP BY t.year
        ORDER BY year
    """
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    _qualify_group_order_by_aliases(tree)
    result = tree.sql(dialect="duckdb")

    assert "ORDER BY" in result
    assert "t.year" in result.lower() or "t.`year`" in result.lower()


def test_already_qualified_group_by_unchanged():
    """
    Non-régression : GROUP BY t.year déjà qualifié ne doit pas être altéré.
    """
    sql = """
        SELECT t.year AS year, SUM(t.v) AS s
        FROM cte_a AS t
        JOIN cte_b AS b ON t.year = b.year
        GROUP BY t.year
    """
    before = sqlglot.parse_one(sql, dialect="bigquery").sql(dialect="duckdb")
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    _qualify_group_order_by_aliases(tree)
    after = tree.sql(dialect="duckdb")

    assert before == after


def test_duckdb_executes_after_fix(con):
    """
    Vérifie que la requête s'exécute sur DuckDB après application du fix,
    en recréant le contexte CTE inline.
    """
    sql = """
        SELECT t.year AS year, t.total_a, b.total_b
        FROM cte_a AS t
        JOIN cte_b AS b ON t.year = b.year
        GROUP BY year, t.total_a, b.total_b
        ORDER BY year
    """
    duckdb_sql = _transpile_with_fix(sql)
    rows = con.execute(duckdb_sql).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == 2020


# ---------------------------------------------------------------------------
# Canary test
# ---------------------------------------------------------------------------


def test_alert_when_sqlglot_fixes_group_by_aliases_natively(con):
    """
    CANARY TEST : Le jour où sqlglot résout nativement l'ambiguïté GROUP BY
    lors de la transpilation BigQuery→DuckDB, ce test échouera.
    → Supprimer _qualify_group_order_by_aliases et son appel dans parse_test_query.
    """
    sql = """
        SELECT t.year AS year, t.total_a, b.total_b
        FROM cte_a AS t
        JOIN cte_b AS b ON t.year = b.year
        GROUP BY year, t.total_a, b.total_b
        ORDER BY year
    """
    duckdb_sql = _transpile_without_fix(sql)

    with pytest.raises(duckdb.Error):
        con.execute(duckdb_sql).fetchall()
