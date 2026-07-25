"""
Sémantique runtime du parsing de dates BigQuery → DuckDB.

Ces tests n'assertent **jamais** le SQL rendu par sqlglot : ils exécutent la
requête dans DuckDB et assertent la *valeur*. C'est le contrat qui compte pour
MockSQL : les fonctions PARSE_* strictes doivent lever comme en production,
tandis que les variantes SAFE.PARSE_* doivent rendre NULL.

Le SQL rendu, lui, change à chaque bump de sqlglot (30.11 → 30.12 a fait passer
PARSE_DATETIME de `PARSE_DATETIME(col, fmt)` à `STRPTIME('1970 ' || col, fmt)`).
Asserter la sémantique plutôt que le texte rend ces tests stables au bump.
"""

import duckdb
import pytest
import sqlglot

from utils.examples import fix_duck_db_sql


def _run(con, bq_expr: str):
    """bigquery → duckdb → fix_duck_db_sql → exécution. Retourne la valeur."""
    raw = sqlglot.parse_one(f"SELECT {bq_expr} AS v FROM t", dialect="bigquery").sql(
        dialect="duckdb"
    )
    return con.execute(fix_duck_db_sql(raw, "bigquery")).fetchone()[0]


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE t AS SELECT 'pas-une-date' AS s, '2024-01-15' AS ok")
    return c


# ---------------------------------------------------------------------------
# Valeur incompatible : erreur stricte, NULL pour SAFE
# ---------------------------------------------------------------------------


class TestIncompatibleValuePreservesBigQuerySemantics:
    """Le replay local ne doit jamais masquer une erreur de production."""

    def test_parse_datetime_incompatible(self, con):
        with pytest.raises(duckdb.Error):
            _run(con, "PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s)")

    def test_parse_timestamp_incompatible(self, con):
        with pytest.raises(duckdb.Error):
            _run(con, "PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', s)")

    def test_parse_date_incompatible(self, con):
        with pytest.raises(duckdb.Error):
            _run(con, "PARSE_DATE('%Y-%m-%d', s)")

    def test_safe_parse_date_incompatible(self, con):
        assert _run(con, "SAFE.PARSE_DATE('%Y-%m-%d', s)") is None

    def test_safe_parse_timestamp_incompatible(self, con):
        assert _run(con, "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', s)") is None

    def test_safe_parse_datetime_incompatible(self, con):
        assert _run(con, "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s)") is None


# ---------------------------------------------------------------------------
# Non-régression : une valeur valide continue de parser correctement
# ---------------------------------------------------------------------------


class TestValidValueStillParses:
    """Le patch SAFE ne doit pas affecter les cas stricts nominaux."""

    def test_parse_datetime_valid(self, con):
        assert (
            _run(con, "PARSE_DATETIME('%Y-%m-%d', ok)")
            .isoformat()
            .startswith("2024-01-15")
        )

    def test_parse_timestamp_valid(self, con):
        assert (
            _run(con, "PARSE_TIMESTAMP('%Y-%m-%d', ok)")
            .isoformat()
            .startswith("2024-01-15")
        )

    def test_parse_date_valid(self, con):
        assert _run(con, "PARSE_DATE('%Y-%m-%d', ok)").isoformat() == "2024-01-15"

    def test_null_input_stays_null(self, con):
        assert _run(con, "PARSE_DATETIME('%Y-%m-%d', NULL)") is None
