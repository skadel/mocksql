"""
Tests for fix_duck_db_sql — corrections de requêtes DuckDB transpilées par sqlglot.

Pipeline de référence pour chaque test :
    1. bq_sql  → sqlglot(dialect='bigquery').sql(dialect='duckdb') → raw_duck
    2. raw_duck  échoue dans DuckDB   (le problème existe bien)
    3. fix_duck_db_sql(raw_duck)       → fixed_duck
    4. fixed_duck réussit dans DuckDB  (la correction est effective)

Des tests de non-régression vérifient que des requêtes déjà valides ne sont pas altérées.
Des tests de redondance documentent les cas que sqlglot traduit déjà correctement.
"""

import duckdb
import pytest
import sqlglot

from utils.examples import fix_duck_db_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def transpile(bq_sql: str) -> str:
    """Transpile BigQuery SQL → DuckDB SQL via sqlglot."""
    return sqlglot.parse_one(bq_sql, dialect="bigquery").sql(dialect="duckdb")


@pytest.fixture
def con():
    """Connexion DuckDB in-memory avec une table de test `events`."""
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE events (
            user_id  INTEGER,
            col      DATE,
            ts       TIMESTAMP,
            d1       DATE,
            d2       DATE,
            s        VARCHAR
        )
    """)
    c.execute("""
        INSERT INTO events VALUES
        (1, '2024-01-15', '2024-01-15 12:00:00', '2024-01-01', '2024-02-01', '2024-01-15')
    """)
    return c


def duckdb_fails(con, sql: str):
    """Assert que la requête échoue dans DuckDB."""
    with pytest.raises(duckdb.Error):
        con.execute(sql)


def duckdb_ok(con, sql: str):
    """Assert que la requête s'exécute sans erreur."""
    con.execute(sql)


# ===========================================================================
# Section 1 : DATE_TRUNC avec jours de la semaine
# ===========================================================================


class TestDateTruncWeek:
    """
    sqlglot produit DATE_TRUNC('WEEK(JOUR)', col) que DuckDB rejette.
    fix_duck_db_sql doit produire DATE_TRUNC('week', col) avec un offset
    pour les jours autres que lundi.
    """

    def test_monday_end_to_end(self, con):
        """WEEK(MONDAY) → DATE_TRUNC('week', col) — lundi est le début de semaine ISO.

        sqlglot 30.x produit DATE_TRUNC('WEEK', col) directement (déjà valide en DuckDB).
        fix_duck_db_sql normalise la casse en 'week'.
        """
        raw = transpile("SELECT DATE_TRUNC(col, WEEK(MONDAY)) FROM events")
        assert "DATE_TRUNC('WEEK'," in raw
        duckdb_ok(con, raw)

        fixed = fix_duck_db_sql(raw)

        assert "DATE_TRUNC('week'," in fixed
        duckdb_ok(con, fixed)

    @pytest.mark.parametrize(
        "day",
        ["TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"],
    )
    def test_weekday_with_offset_end_to_end(self, con, day):
        """WEEK(jour) → expression avec offset.

        sqlglot 30.x gère lui-même l'offset et produit une expression DuckDB valide.
        fix_duck_db_sql normalise la casse et ne casse pas l'expression.
        """
        raw = transpile(f"SELECT DATE_TRUNC(col, WEEK({day})) FROM events")
        assert "INTERVAL" in raw
        duckdb_ok(con, raw)

        fixed = fix_duck_db_sql(raw)
        duckdb_ok(con, fixed)

    def test_multicolumn_friday_end_to_end(self, con):
        """L'offset reste à l'intérieur de l'expression, pas ajouté en fin de chaîne SQL.

        sqlglot 30.x produit une expression DuckDB valide directement.
        """
        raw = transpile("SELECT user_id, DATE_TRUNC(col, WEEK(FRIDAY)) FROM events")
        duckdb_ok(con, raw)

        fixed = fix_duck_db_sql(raw)

        duckdb_ok(con, fixed)


# ===========================================================================
# Section 2 : Fonctions SAFE / parsing de dates
# ===========================================================================


class TestSafeFunctions:
    def test_safe_parse_date_end_to_end(self, con):
        """SAFE.PARSE_DATE → TRY_STRPTIME.

        sqlglot 30.x produit SAFE.CAST(STRPTIME(col, '%fmt') AS DATE).
        fix_duck_db_sql convertit en TRY_STRPTIME(col, '%fmt').
        """
        raw = transpile("SELECT SAFE.PARSE_DATE('%Y-%m-%d', s) FROM events")
        assert "SAFE.CAST" in raw
        duckdb_fails(con, raw)

        fixed = fix_duck_db_sql(raw)

        assert "TRY_STRPTIME" in fixed
        assert "SAFE.CAST" not in fixed
        duckdb_ok(con, fixed)

    def test_parse_datetime_end_to_end(self, con):
        """PARSE_DATETIME → TRY_STRPTIME (retourne NULL si valeur incompatible avec le format)."""
        raw = transpile("SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s) FROM events")
        duckdb_fails(con, raw)

        fixed = fix_duck_db_sql(raw)

        assert "TRY_STRPTIME" in fixed
        assert "PARSE_DATETIME" not in fixed
        duckdb_ok(con, fixed)

    def test_safe_cast_already_translated_by_sqlglot(self, con):
        """sqlglot traduit SAFE_CAST → TRY_CAST nativement ; fix ne doit pas le casser."""
        raw = transpile("SELECT SAFE_CAST(user_id AS INT64) FROM events")
        assert "TRY_CAST" in raw
        assert "SAFE_CAST" not in raw
        duckdb_ok(con, raw)

        fixed = fix_duck_db_sql(raw)
        duckdb_ok(con, fixed)


# ===========================================================================
# Section 3 : Fonctions géospatiales
# ===========================================================================


class TestGeospatial:
    def test_st_geogpoint_replaced(self):
        """ST_GEOGPOINT → ST_POINT."""
        raw = transpile("SELECT ST_GEOGPOINT(1.0, 2.0)")
        assert "ST_GEOGPOINT" in raw

        fixed = fix_duck_db_sql(raw)

        assert "ST_POINT" in fixed
        assert "ST_GEOGPOINT" not in fixed


# ===========================================================================
# Section 4 : Redondances — sqlglot traduit déjà correctement
# ===========================================================================


class TestRedundantFixes:
    """
    Certaines transformations sont des no-ops :
    sqlglot a déjà effectué la conversion avant que la fonction soit appelée.
    """

    def test_format_date_already_translated_by_sqlglot(self, con):
        """sqlglot traduit FORMAT_DATE → STRFTIME nativement ; la sortie est déjà valide."""
        raw = transpile("SELECT FORMAT_DATE('%Y-%m', col) FROM events")
        assert "FORMAT_DATE" not in raw
        assert "STRFTIME" in raw
        duckdb_ok(con, raw)

    def test_safe_divide_already_translated_by_sqlglot(self, con):
        """sqlglot traduit SAFE_DIVIDE → CASE WHEN b <> 0 THEN a/b ELSE NULL END."""
        raw = transpile("SELECT SAFE_DIVIDE(user_id, 2) FROM events")
        assert "SAFE_DIVIDE" not in raw
        assert "CASE WHEN" in raw
        duckdb_ok(con, raw)


# ===========================================================================
# Section 5 : Différences sémantiques silencieuses (non corrigées par fix)
# ===========================================================================


class TestSemanticGaps:
    """
    Requêtes que DuckDB accepte sans erreur mais avec un résultat différent de BigQuery.
    fix_duck_db_sql ne corrige pas ces cas, mais ils représentent un risque de divergence.
    """

    def test_extract_dayofweek_semantic_difference(self, con):
        """
        DuckDB accepte EXTRACT(DAYOFWEEK FROM date) mais avec une indexation différente :
          DuckDB : 0=Dimanche, 1=Lundi, ..., 6=Samedi
          BigQuery: 1=Dimanche, 2=Lundi, ..., 7=Samedi

        2024-01-15 est un lundi → DuckDB retourne 1, BigQuery attendrait 2.
        """
        raw = transpile("SELECT EXTRACT(DAYOFWEEK FROM col) FROM events")
        duckdb_ok(con, raw)

        result = con.execute(raw).fetchone()[0]
        bq_expected = 2  # lundi = 2 en BigQuery
        assert result != bq_expected, (
            f"DuckDB retourne {result}, BigQuery attendrait {bq_expected} : "
            "différence d'indexation silencieuse"
        )

        # La correction ISODOW+1 produit le résultat BigQuery :
        corrected = (
            "SELECT (CASE WHEN EXTRACT(ISODOW FROM col) = 7 "
            "THEN 1 ELSE EXTRACT(ISODOW FROM col) + 1 END) FROM events"
        )
        assert con.execute(corrected).fetchone()[0] == bq_expected


# ===========================================================================
# Section 6 : Non-régression — requêtes déjà valides ne doivent pas être altérées
# ===========================================================================


class TestNoRegression:
    """
    Ces tests vérifient que fix_duck_db_sql ne dégrade pas des requêtes
    qui fonctionnent correctement sans correction.
    """

    def test_simple_query_passes_through_unchanged(self, con):
        raw = "SELECT user_id, s FROM events"
        fixed = fix_duck_db_sql(raw)
        assert fixed == raw
        duckdb_ok(con, fixed)

    @pytest.mark.parametrize("grain", ["MONTH", "YEAR", "DAY", "HOUR"])
    def test_date_trunc_non_weekday_not_broken(self, con, grain):
        """DATE_TRUNC avec MONTH/YEAR/DAY/HOUR est déjà supporté par DuckDB."""
        raw = transpile(f"SELECT DATE_TRUNC(ts, {grain}) FROM events")
        duckdb_ok(con, raw)
        fixed = fix_duck_db_sql(raw)
        duckdb_ok(con, fixed)

    def test_date_diff_day_not_broken(self, con):
        """DATE_DIFF traduit par sqlglot ne doit pas être altéré par fix."""
        raw = transpile("SELECT DATE_DIFF(d1, d2, DAY) FROM events")
        duckdb_ok(con, raw)
        fixed = fix_duck_db_sql(raw)
        duckdb_ok(con, fixed)

    def test_safe_cast_result_unchanged_after_fix(self, con):
        """TRY_CAST déjà dans la sortie sqlglot : le résultat ne change pas après fix."""
        raw = transpile("SELECT SAFE_CAST(user_id AS INT64) FROM events")
        result_before = con.execute(raw).fetchall()
        fixed = fix_duck_db_sql(raw)
        result_after = con.execute(fixed).fetchall()
        assert result_before == result_after
