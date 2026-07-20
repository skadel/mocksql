"""
Sémantique runtime du parsing de dates BigQuery → DuckDB.

Ces tests n'assertent **jamais** le SQL rendu par sqlglot : ils exécutent la
requête dans DuckDB et assertent la *valeur*. C'est le contrat qui compte pour
MockSQL — les données synthétiques du LLM ont constamment des décalages de
format, et un parsing incompatible doit rendre NULL, pas faire tomber le test
entier.

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
# Valeur incompatible avec le format → NULL (jamais de raise)
# ---------------------------------------------------------------------------


class TestIncompatibleValueYieldsNull:
    """Une valeur qui ne matche pas le format doit rendre NULL.

    C'est la garantie qui permet à un test MockSQL de rester lisible : une
    colonne mal formatée sort NULL et le verdict le pointe, au lieu d'une
    InvalidInputException qui masque tout le reste du test.
    """

    def test_parse_datetime_incompatible(self, con):
        assert _run(con, "PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s)") is None

    def test_parse_timestamp_incompatible(self, con):
        assert _run(con, "PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', s)") is None

    def test_parse_date_incompatible(self, con):
        assert _run(con, "PARSE_DATE('%Y-%m-%d', s)") is None

    def test_safe_parse_date_incompatible(self, con):
        assert _run(con, "SAFE.PARSE_DATE('%Y-%m-%d', s)") is None

    def test_safe_parse_timestamp_incompatible(self, con):
        assert _run(con, "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', s)") is None


# ---------------------------------------------------------------------------
# Non-régression : une valeur valide continue de parser correctement
# ---------------------------------------------------------------------------


class TestValidValueStillParses:
    """Le TRY ne doit pas avaler les cas nominaux."""

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
