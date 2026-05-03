"""
Tests for _fix_unnest_alias_conflicts.

Pipeline :
    BQ SQL -> parse_one(dialect=bigquery) -> _fix_unnest_alias_conflicts -> .sql(dialect=duckdb)
    -> DuckDB exécute sans erreur d ambiguïté.
"""

import duckdb
import pytest
import sqlglot

from utils.examples import _fix_unnest_alias_conflicts


def bq_to_duck(sql: str) -> str:
    tree = sqlglot.parse_one(sql, dialect="bigquery")
    tree = _fix_unnest_alias_conflicts(tree)
    return tree.sql(dialect="duckdb")


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE ds_ga_sfx (id TEXT, hits STRUCT(type TEXT, hitNumber INT)[])")
    c.execute(
        "INSERT INTO ds_ga_sfx VALUES "
        "('v1', [ROW('PAGE', 1)::STRUCT(type TEXT, hitNumber INT)])"
    )
    return c


# ---------------------------------------------------------------------------
# Cas : alias UNNEST == nom de la colonne source (ambiguïté DuckDB)
# ---------------------------------------------------------------------------

class TestConflictingAlias:
    def test_rename_col_alias(self):
        duck = bq_to_duck(
            "SELECT hits.type FROM ds.ga CROSS JOIN UNNEST(hits) AS hits"
        )
        assert "_hits_u" in duck
        assert "hits.type" not in duck or "_hits_u.type" in duck

    def test_multiple_fields(self):
        duck = bq_to_duck(
            "SELECT hits.type, hits.hitNumber FROM ds.ga CROSS JOIN UNNEST(hits) AS hits"
        )
        assert "_hits_u.type" in duck
        assert "_hits_u.hitNumber" in duck

    def test_source_ref_inside_unnest_not_renamed(self):
        """La référence à hits DANS l UNNEST (UNNEST(t.hits)) ne doit pas être renommée."""
        duck = bq_to_duck(
            "SELECT hits.type FROM ds.ga AS t CROSS JOIN UNNEST(t.hits) AS hits"
        )
        # La source dans UNNEST doit rester t.hits (ou hits sans table alias)
        assert "UNNEST(t.hits)" in duck or "UNNEST(hits)" in duck

    def test_nested_struct_field_renamed(self):
        """hits.page.pagePath (3-level) doit être renommé en _hits_u.page.pagePath."""
        duck = bq_to_duck(
            "SELECT hits.page.pagePath FROM ds.ga CROSS JOIN UNNEST(hits) AS hits"
        )
        assert "_hits_u" in duck
        assert "hits.page.pagePath" not in duck
        assert "_hits_u.page.pagePath" in duck

    def test_duckdb_executes_without_error(self, con):
        duck = bq_to_duck(
            "SELECT hits.type FROM ds.ga_sfx CROSS JOIN UNNEST(hits) AS hits"
        )
        # Adapter le nom de table au fixture (ds_ga_sfx déjà créée)
        duck = duck.replace("ds.ga_sfx", "ds_ga_sfx")
        result = con.execute(duck).fetchall()
        assert result == [("PAGE",)]


# ---------------------------------------------------------------------------
# Cas : pas de conflit — la requête ne doit pas être modifiée
# ---------------------------------------------------------------------------

class TestNoConflict:
    def test_alias_different_from_source(self):
        sql = "SELECT h.type FROM ds.ga CROSS JOIN UNNEST(hits) AS h"
        before = sqlglot.parse_one(sql, dialect="bigquery")
        after = _fix_unnest_alias_conflicts(before.copy())
        assert before.sql(dialect="duckdb") == after.sql(dialect="duckdb")

    def test_no_unnest(self):
        sql = "SELECT id FROM ds.ga WHERE id = '1'"
        before = sqlglot.parse_one(sql, dialect="bigquery")
        after = _fix_unnest_alias_conflicts(before.copy())
        assert before.sql(dialect="duckdb") == after.sql(dialect="duckdb")

    def test_unnest_no_alias(self):
        sql = "SELECT * FROM ds.ga, UNNEST(hits)"
        before = sqlglot.parse_one(sql, dialect="bigquery")
        after = _fix_unnest_alias_conflicts(before.copy())
        assert before.sql(dialect="duckdb") == after.sql(dialect="duckdb")
