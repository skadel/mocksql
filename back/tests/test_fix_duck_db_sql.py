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

import re

import duckdb
import pytest
import sqlglot

from utils.examples import fix_duck_db_sql


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def transpile(bq_sql: str, source: str = "bigquery") -> str:
    """Transpile SQL d'un dialecte source → DuckDB via sqlglot."""
    return sqlglot.parse_one(bq_sql, dialect=source).sql(dialect="duckdb")


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


# ===========================================================================
# Section 7 : SUBSTR avec position 0 (divergence sémantique BigQuery / DuckDB)
# ===========================================================================


class TestSubstrZeroIndex:
    """
    BigQuery : SUBSTR('ABCD', 0, 2)  → 'AB'  (position 0 clampée à 1)
    DuckDB   : SUBSTR('ABCD', 0, 2)  → 'A'   (position 0 = avant le 1er char → 1 char perdu)

    fix_duck_db_sql remplace SUBSTR(expr, 0, n) → SUBSTR(expr, 1, n).
    """

    def test_canary_sqlglot_still_broken(self, con):
        """
        CANARY — échoue si sqlglot corrige un jour SUBSTR(str, 0, n) côté DuckDB.
        Si ce test échoue, la correction dans fix_duck_db_sql est devenue redondante.
        """
        raw = transpile("SELECT SUBSTR('ABCD', 0, 2)")
        result = con.execute(raw).fetchone()[0]
        assert result == "A", (
            f"CANARY: sqlglot a corrigé SUBSTR(str, 0, n) — résultat DuckDB: {result!r} "
            "(attendu 'A' tant que le bug existe). "
            "La correction dans fix_duck_db_sql est désormais redondante."
        )

    def test_fix_corrects_substr_zero_start_literal(self, con):
        """SUBSTR sur littéral string : position 0 → 1, résultat 'AB' comme BigQuery."""
        raw = transpile("SELECT SUBSTR('ABCD', 0, 2)")
        fixed = fix_duck_db_sql(raw)

        assert re.search(r"SUBSTR\('ABCD',\s*1,", fixed, re.IGNORECASE), fixed
        assert con.execute(fixed).fetchone()[0] == "AB"

    def test_fix_corrects_substr_zero_start_column(self, con):
        """SUBSTR sur colonne : SUBSTR(s, 0, 4) → SUBSTR(s, 1, 4), retourne '2024'."""
        raw = "SELECT SUBSTR(s, 0, 4) FROM events"
        fixed = fix_duck_db_sql(raw)

        assert re.search(r"SUBSTR\(s,\s*1,", fixed, re.IGNORECASE)
        assert con.execute(fixed).fetchone()[0] == "2024"

    def test_fix_preserves_nonzero_start(self, con):
        """SUBSTR avec position non nulle ne doit pas être modifié."""
        raw = transpile("SELECT SUBSTR('ABCD', 2, 2)")
        fixed = fix_duck_db_sql(raw)

        assert con.execute(fixed).fetchone()[0] == "BC"

    def test_fix_does_not_alter_position_10(self, con):
        """SUBSTR(str, 10, n) — le '0' dans '10' ne doit pas déclencher le fix."""
        raw = "SELECT SUBSTR('ABCDEFGHIJKLMNOP', 10, 3)"
        fixed = fix_duck_db_sql(raw)

        assert fixed == raw
        assert con.execute(fixed).fetchone()[0] == "JKL"


# ===========================================================================
# Section 8 : Garde source_dialect — fixes BigQuery non appliqués hors BigQuery
# ===========================================================================


class TestSourceDialectGuard:
    """
    Les corrections BigQuery ne doivent pas s'appliquer quand source_dialect != "bigquery".
    """

    def test_postgres_safe_cast_not_altered(self):
        """SAFE_CAST dans du SQL postgres-transpilé ne doit pas être remplacé par TRY_CAST."""
        sql = "SELECT SAFE_CAST(x AS INTEGER)"
        assert fix_duck_db_sql(sql, source_dialect="postgres") == sql

    def test_postgres_st_geogpoint_not_altered(self):
        """ST_GEOGPOINT dans du SQL postgres-transpilé ne doit pas être remplacé."""
        sql = "SELECT ST_GEOGPOINT(1.0, 2.0)"
        assert fix_duck_db_sql(sql, source_dialect="postgres") == sql

    def test_postgres_substr_zero_not_altered(self):
        """SUBSTR(expr, 0, n) dans du SQL postgres-transpilé ne doit pas être modifié."""
        sql = "SELECT SUBSTR('ABCD', 0, 2)"
        assert fix_duck_db_sql(sql, source_dialect="postgres") == sql

    def test_bigquery_default_still_applies_fixes(self):
        """Sans source_dialect explicite (défaut bigquery), les fixes s'appliquent."""
        sql = "SELECT SAFE_CAST(x AS INTEGER)"
        assert fix_duck_db_sql(sql) == "SELECT TRY_CAST(x AS INTEGER)"

    def test_bigquery_explicit_applies_fixes(self):
        """Avec source_dialect='bigquery' explicite, les fixes s'appliquent."""
        sql = "SELECT ST_GEOGPOINT(1.0, 2.0)"
        assert (
            fix_duck_db_sql(sql, source_dialect="bigquery")
            == "SELECT ST_POINT(1.0, 2.0)"
        )
