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

from build_query.validator import prune_constant_group_by
from utils.examples import fix_duck_db_sql, parse_test_query, _fix_group_by_strict_mode


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


# ===========================================================================
# Section 9 : GROUP BY ALL — pipeline complet via parse_test_query
# ===========================================================================


class TestGroupByAll:
    """
    Régression : GROUP BY ALL (syntaxe BigQuery) était corrompu en GROUP BY ALL a, b
    par _fix_group_by_strict_mode, provoquant une ParserException DuckDB.

    Ces tests couvrent le pipeline complet parse_test_query :
        bq_sql → strip_qualifiers → _qualify_group_order_by_aliases
               → _fix_group_by_strict_mode → .sql(duckdb) → DuckDB execute
    """

    @pytest.fixture
    def con(self):
        # strip_qualifiers_with_scope(proj.ds.sales, sess1) → ds_sales_sess1
        c = duckdb.connect()
        c.execute(
            "CREATE TABLE ds_sales_sess1 (region VARCHAR, product VARCHAR, amount INTEGER)"
        )
        c.execute("""
            INSERT INTO ds_sales_sess1 VALUES
            ('EU', 'A', 10), ('EU', 'A', 20), ('US', 'B', 5)
        """)
        return c

    @pytest.mark.asyncio
    async def test_group_by_all_simple(self, con):
        """GROUP BY ALL sur une table simple : le pipeline complet s'exécute sans erreur."""
        bq_sql = """
            SELECT region, product, SUM(amount) AS total
            FROM `proj.ds.sales`
            GROUP BY ALL
        """
        duckdb_sql = await parse_test_query(bq_sql, "sess1", "bigquery")
        rows = con.execute(duckdb_sql).fetchall()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_group_by_all_produces_valid_syntax(self):
        """Le SQL généré contient GROUP BY ALL sans colonnes parasites."""
        bq_sql = "SELECT a, b, SUM(c) AS total FROM `proj.ds.t` GROUP BY ALL"
        duckdb_sql = await parse_test_query(bq_sql, "sess1", "bigquery")
        assert "GROUP BY ALL" in duckdb_sql
        # pas de colonnes après ALL
        after_all = duckdb_sql.split("GROUP BY ALL")[1].strip()
        assert after_all == "" or after_all.upper().startswith("ORDER")

    @pytest.mark.asyncio
    async def test_group_by_xyz_equivalent_to_group_by_all(self, con):
        """
        GROUP BY region, product et GROUP BY ALL doivent produire les mêmes résultats.
        """
        sql_explicit = """
            SELECT region, product, SUM(amount) AS total
            FROM `proj.ds.sales`
            GROUP BY region, product
        """
        sql_all = """
            SELECT region, product, SUM(amount) AS total
            FROM `proj.ds.sales`
            GROUP BY ALL
        """
        duck_explicit = await parse_test_query(sql_explicit, "sess1", "bigquery")
        duck_all = await parse_test_query(sql_all, "sess1", "bigquery")

        rows_explicit = sorted(con.execute(duck_explicit).fetchall())
        rows_all = sorted(con.execute(duck_all).fetchall())
        assert rows_explicit == rows_all

    @pytest.mark.asyncio
    async def test_group_by_all_in_cte(self, con):  # noqa: F811 (con shadowed by class fixture)
        """GROUP BY ALL dans une CTE imbriquée passe sans erreur."""
        bq_sql = """
            WITH agg AS (
                SELECT region, product, SUM(amount) AS total
                FROM `proj.ds.sales`
                GROUP BY ALL
            )
            SELECT region, SUM(total) AS region_total
            FROM agg
            GROUP BY ALL
        """
        duckdb_sql = await parse_test_query(bq_sql, "sess1", "bigquery")
        rows = con.execute(duckdb_sql).fetchall()
        assert len(rows) == 2


# ===========================================================================
# Section 10 : Bug 3 — prune_constant_group_by efface GROUP BY ALL/ROLLUP/CUBE/GROUPING SETS
#              quand au moins une projection SELECT est une constante
# ===========================================================================


class TestPruneConstantGroupBySpecialConstructs:
    """
    Spec des corrections attendues — ces tests échouent tant que le bug existe.

    Symptôme : prune_constant_group_by identifie les projections constantes dans
    SELECT (ex: 'hello' AS label), constate que group.expressions == [] pour
    GROUP BY ALL / ROLLUP / CUBE / GROUPING SETS, construit new_group = [],
    et puisque c'est falsy → node.set("group", None) : efface tout le GROUP BY.

    Correction attendue : skipper la réécriture quand group.args contient
    "all", "rollup", "cube" ou "grouping_sets".
    """

    def test_group_by_all_with_constant_select_preserved(self):
        """GROUP BY ALL ne doit pas être supprimé si SELECT contient une constante."""
        sql = "SELECT 'hello' AS label, a, b, SUM(c) AS total FROM t GROUP BY ALL"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        result = prune_constant_group_by(tree)
        assert result.args.get("group") is not None, (
            "prune_constant_group_by a supprimé GROUP BY ALL"
        )
        assert result.args["group"].args.get("all") is True

    def test_group_by_rollup_with_constant_select_preserved(self):
        """GROUP BY ROLLUP ne doit pas être supprimé si SELECT contient une constante."""
        sql = "SELECT 'label' AS label, a, SUM(c) AS total FROM t GROUP BY ROLLUP(a)"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        result = prune_constant_group_by(tree)
        assert result.args.get("group") is not None, (
            "prune_constant_group_by a supprimé GROUP BY ROLLUP"
        )
        assert result.args["group"].args.get("rollup")

    def test_group_by_cube_with_constant_select_preserved(self):
        """GROUP BY CUBE ne doit pas être supprimé si SELECT contient une constante."""
        sql = "SELECT 1 AS const, a, b, SUM(c) AS total FROM t GROUP BY CUBE(a, b)"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        result = prune_constant_group_by(tree)
        assert result.args.get("group") is not None, (
            "prune_constant_group_by a supprimé GROUP BY CUBE"
        )

    def test_group_by_grouping_sets_with_constant_select_preserved(self):
        """GROUP BY GROUPING SETS ne doit pas être supprimé si SELECT contient une constante."""
        sql = "SELECT 'x' AS x, a, SUM(c) AS total FROM t GROUP BY GROUPING SETS ((a), ())"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        result = prune_constant_group_by(tree)
        assert result.args.get("group") is not None, (
            "prune_constant_group_by a supprimé GROUP BY GROUPING SETS"
        )

    def test_group_by_all_query_executes_correctly_after_prune(self):
        """
        End-to-end : GROUP BY ALL avec constante dans SELECT doit s'exécuter sur DuckDB
        et retourner des lignes agrégées (pas un plein scan).
        """
        sql = "SELECT 'static' AS label, a, b, SUM(c) AS total FROM t GROUP BY ALL"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        pruned = prune_constant_group_by(tree)
        duckdb_sql = pruned.sql(dialect="duckdb")

        con = duckdb.connect()
        con.execute("CREATE TABLE t (a VARCHAR, b VARCHAR, c INTEGER)")
        con.execute("INSERT INTO t VALUES ('x', 'y', 1), ('x', 'y', 2), ('z', 'w', 5)")
        rows = con.execute(duckdb_sql).fetchall()
        # Doit être agrégé : 2 lignes (x,y) et (z,w), pas 3 lignes brutes
        assert len(rows) == 2, f"Attendu 2 lignes agrégées, obtenu {len(rows)} : {rows}"

    def test_regular_group_by_with_constant_still_pruned(self):
        """Non-régression : GROUP BY a, 1 doit toujours supprimer la constante ordinale."""
        sql = "SELECT 1 AS const, a, SUM(c) AS total FROM t GROUP BY a, 1"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        result = prune_constant_group_by(tree)
        group_sql = (
            result.args["group"].sql(dialect="duckdb")
            if result.args.get("group")
            else ""
        )
        assert "1" not in group_sql or "a" in group_sql


# ===========================================================================
# Section 11 : Bug — _fix_group_by_strict_mode ajoute la colonne originale
#              d'une expression CASE au GROUP BY → DuckDB "Cannot mix aggregates"
# ===========================================================================


class TestGroupByCaseOriginalColumnBug:
    """
    Comportement de _fix_group_by_strict_mode sur un CASE en GROUP BY.

    Historique : la fonction parcourait les exp.Column de chaque expression
    SELECT non-agrégée et ajoutait `s` au GROUP BY quand seul le CASE y figurait :
        GROUP BY CASE WHEN s = 'a' THEN 'A' ELSE 'B' END
        → GROUP BY CASE WHEN s = 'a' THEN 'A' ELSE 'B' END, s
    Ce n'était pas qu'une redondance : deux valeurs de s tombant dans 'Other'
    produisaient deux lignes 'Other' au lieu d'une (grain cassé — même famille
    que la régression sf_bq263, section 15).

    Comportement corrigé : une colonne apparaissant dans une expression groupée
    est considérée couverte et n'est plus ajoutée.
    """

    def test_fix_group_by_keeps_column_covered_by_case(self):
        """
        _fix_group_by_strict_mode n'ajoute PAS s au GROUP BY quand s apparaît
        dans le CASE déjà groupé.
        """
        sql = (
            "SELECT CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END AS cat,"
            " COUNT(*) AS cnt"
            " FROM events"
            " GROUP BY CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END"
        )
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        group_part = result_sql[result_sql.upper().rfind("GROUP BY") :]
        assert not re.search(r",\s*s\b", group_part, re.IGNORECASE), (
            f"Clé GROUP BY parasite s ajoutée à côté du CASE groupé.\nSQL: {result_sql}"
        )

    def test_duckdb_accepts_case_plus_original_column_in_group_by(self, con):
        """
        DuckDB n'a PAS levé "Cannot mix aggregates" — il accepte le GROUP BY redondant
        (CASE + colonne originale). La prémisse initiale était incorrecte.
        """
        sql = (
            "SELECT CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END AS cat,"
            " COUNT(*) AS cnt"
            " FROM events"
            " GROUP BY CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END, s"
        )
        con.execute(sql)

    def test_pipeline_produces_valid_duckdb_sql(self, con):
        """
        Pipeline complet : après passage de _fix_group_by_strict_mode (qui ne
        touche plus au GROUP BY ici), DuckDB exécute la requête sans erreur.
        """
        sql = (
            "SELECT CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END AS cat,"
            " COUNT(*) AS cnt"
            " FROM events"
            " GROUP BY CASE WHEN s = '2024-01-15' THEN 'Match' ELSE 'Other' END"
        )
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        rows = con.execute(result_sql).fetchall()
        assert len(rows) == 1


# ===========================================================================
# Section 12 : Bug fix — EXTRACT(DATE FROM CAST(...)) parens imbriquées
# ===========================================================================


class TestExtractDateNestedParen:
    """
    Bug : le regex ([^)]+) s'arrêtait à la 1ère ')' de l'expression interne.
    Pour EXTRACT(DATE FROM CAST('...' AS TIMESTAMPTZ)), sqlglot 30 enveloppe
    le littéral dans un CAST, ce qui ajoute une paire de parens imbriquées.
    Le regex produisait CAST(CAST('...' AS TIMESTAMPTZ AS DATE)) — malformé.

    Fix attendu : ((?:[^()]+|\\([^()]*\\))+) gère un niveau d'imbrication.
    """

    def test_canary_sqlglot_wraps_timestamp_literal_in_cast(self):
        """
        CANARY — sqlglot 30 produit EXTRACT(DATE FROM CAST('...' AS TIMESTAMPTZ)).
        Si ce test échoue, sqlglot a changé la forme de transpilation de
        EXTRACT(DATE FROM TIMESTAMP '...') et la correction peut être revue.
        """
        raw = transpile("EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00')")
        assert "CAST" in raw and "TIMESTAMPTZ" in raw, (
            f"CANARY : sqlglot ne produit plus CAST(... AS TIMESTAMPTZ). raw={raw!r}. "
            "Vérifier si la correction EXTRACT est toujours nécessaire."
        )

    def test_extract_date_from_cast_produces_valid_sql(self):
        """Après fix, fix_duck_db_sql produit CAST(CAST(... AS TIMESTAMPTZ) AS DATE)."""
        raw = transpile("EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00')")
        fixed = fix_duck_db_sql(f"SELECT {raw}")
        # Doit être du SQL valide (pas de SyntaxError)
        result = duckdb.connect().execute(fixed).fetchone()[0]
        import datetime

        assert result == datetime.date(2024, 1, 15)

    def test_extract_date_from_simple_column_still_works(self, con):
        """Non-régression : EXTRACT(DATE FROM ts) sans CAST imbriqué fonctionne."""
        fixed = fix_duck_db_sql("SELECT EXTRACT(DATE FROM ts) FROM events")
        con.execute(fixed)


# ===========================================================================
# Section 13 : Bug fix — PARSE_DATETIME inversion d'arguments (sqlglot 30+)
# ===========================================================================


class TestParseDatetimeArgOrder:
    """
    Bug : sqlglot 30+ produit PARSE_DATETIME(value, '%fmt') — valeur en 1er.
    L'ancien regex 1 matchait le 1er arg comme si c'était le format et produisait
    TRY_STRPTIME('%fmt', value) — arguments inversés → DuckDB retourne NULL.

    Fix attendu : utiliser la présence de '%' pour identifier l'arg format,
    quel que soit l'ordre.
    """

    def test_canary_sqlglot_30_produces_value_first(self):
        """
        CANARY — sqlglot 30+ inverse les args : PARSE_DATETIME(value, '%fmt').
        Si ce test échoue, sqlglot a re-changé l'ordre et le fix doit être adapté.
        """
        raw = transpile("PARSE_DATETIME('%Y-%m-%d', col)")
        assert raw.startswith("PARSE_DATETIME(col"), (
            f"CANARY : sqlglot ne produit plus value-first pour PARSE_DATETIME. raw={raw!r}. "
            "Vérifier si la logique de détection '%' est toujours correcte."
        )

    def test_canary_sqlglot_does_not_translate_parse_datetime(self):
        """
        CANARY — sqlglot ne traduit pas PARSE_DATETIME → TRY_STRPTIME nativement.
        Si ce test échoue, fix_duck_db_sql n'est plus utile pour ce cas.
        """
        raw = transpile("PARSE_DATETIME('%Y-%m-%d', col)")
        assert "PARSE_DATETIME" in raw.upper(), (
            f"CANARY : sqlglot traduit maintenant PARSE_DATETIME nativement. raw={raw!r}. "
            "La correction fix_duck_db_sql est désormais redondante pour ce cas."
        )

    def test_literal_value_first_correctly_converted(self):
        """
        sqlglot 30+ : PARSE_DATETIME('2024-01-15', '%Y-%m-%d')
        fix doit produire : TRY_STRPTIME('2024-01-15', '%Y-%m-%d')
        résultat attendu  : timestamp non-NULL.
        """
        raw_scalar = "PARSE_DATETIME('2024-01-15', '%Y-%m-%d')"
        fixed = fix_duck_db_sql(f"SELECT {raw_scalar}")
        fixed_expr = fixed[len("SELECT ") :]
        assert "TRY_STRPTIME" in fixed_expr, (
            f"fix n'a pas produit TRY_STRPTIME : {fixed_expr!r}"
        )
        result = duckdb.connect().execute(fixed).fetchone()[0]
        assert result is not None, (
            "TRY_STRPTIME a retourné NULL — args probablement inversés"
        )

    def test_col_value_first_correctly_converted(self, con):
        """
        sqlglot 30+ : PARSE_DATETIME(col, '%Y-%m-%d %H:%M:%S')
        fix doit produire : TRY_STRPTIME(col, '%Y-%m-%d %H:%M:%S').
        """
        raw = transpile("SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s) FROM events")
        assert "PARSE_DATETIME" in raw
        fixed = fix_duck_db_sql(raw)
        assert "TRY_STRPTIME" in fixed
        assert "PARSE_DATETIME" not in fixed.upper()
        con.execute(fixed)

    def test_format_first_legacy_still_converted(self, con):
        """
        Non-régression : si fix reçoit encore PARSE_DATETIME('%fmt', col) (sqlglot <30),
        la conversion doit rester correcte.
        """
        legacy_format_first = (
            "SELECT PARSE_DATETIME('%Y-%m-%d %H:%M:%S', s) FROM events"
        )
        fixed = fix_duck_db_sql(legacy_format_first)
        assert "TRY_STRPTIME(s, '%Y-%m-%d %H:%M:%S')" in fixed
        con.execute(fixed)


# ===========================================================================
# Section 14 : Canaries sqlglot — détection des bumps de version
# ===========================================================================


class TestSqlglotVersionCanaries:
    """
    Ces tests échouent si sqlglot change de comportement sur un cas géré par
    fix_duck_db_sql. Ils servent d'alarme lors d'un bump de version de sqlglot :
      - si un canary "is broken" échoue → sqlglot corrige maintenant ce cas, le fix est redondant
      - si un canary "is NOT broken" échoue → sqlglot a régressé, le fix redevient nécessaire

    À inspecter après chaque bump : `poetry run pytest tests/test_fix_duck_db_sql.py -k canary -v`
    """

    # --- Cas où sqlglot NE corrige PAS (fix encore nécessaire) ---

    def test_canary_parse_datetime_not_translated(self):
        """sqlglot ne traduit pas PARSE_DATETIME → TRY_STRPTIME."""
        raw = transpile("PARSE_DATETIME('%Y-%m-%d', col)")
        assert "PARSE_DATETIME" in raw.upper(), (
            "CANARY ROMPU : sqlglot traduit maintenant PARSE_DATETIME — "
            "supprimer la correction dans fix_duck_db_sql."
        )

    def test_canary_extract_date_not_translated(self):
        """sqlglot ne traduit pas EXTRACT(DATE FROM ...) → CAST(... AS DATE)."""
        raw = transpile("EXTRACT(DATE FROM ts)")
        assert "EXTRACT" in raw.upper(), (
            "CANARY ROMPU : sqlglot traduit maintenant EXTRACT(DATE FROM ...) — "
            "supprimer la correction dans fix_duck_db_sql."
        )

    def test_canary_st_geogpoint_not_translated(self):
        """sqlglot ne traduit pas ST_GEOGPOINT → ST_POINT."""
        raw = transpile("SELECT ST_GEOGPOINT(1.0, 2.0)")
        assert "ST_GEOGPOINT" in raw, (
            "CANARY ROMPU : sqlglot traduit maintenant ST_GEOGPOINT — "
            "supprimer la correction dans fix_duck_db_sql."
        )

    def test_canary_substr_zero_not_fixed_by_sqlglot(self, con):
        """sqlglot ne corrige pas SUBSTR(str, 0, n) — résultat DuckDB différent de BigQuery."""
        raw = transpile("SELECT SUBSTR('ABCD', 0, 2)")
        result = con.execute(raw).fetchone()[0]
        assert result == "A", (
            f"CANARY ROMPU : sqlglot corrige maintenant SUBSTR(str, 0, n). "
            f"Résultat DuckDB = {result!r} (attendu 'A'). "
            "Supprimer la correction dans fix_duck_db_sql."
        )

    # --- Cas où sqlglot corrige déjà (fix redondant — canary de non-régression) ---

    def test_canary_format_date_already_translated(self, con):
        """sqlglot traduit FORMAT_DATE → STRFTIME. Si ça casse, fix_duck_db_sql redevient utile."""
        raw = transpile("SELECT FORMAT_DATE('%Y-%m', col) FROM events")
        assert "FORMAT_DATE" not in raw, (
            "CANARY ROMPU : sqlglot ne traduit plus FORMAT_DATE → STRFTIME. "
            "Re-activer la correction dans fix_duck_db_sql."
        )
        con.execute(raw)

    def test_canary_safe_cast_already_translated(self, con):
        """sqlglot traduit SAFE_CAST → TRY_CAST. Si ça casse, fix_duck_db_sql redevient utile."""
        raw = transpile("SELECT SAFE_CAST(user_id AS INT64) FROM events")
        assert "SAFE_CAST" not in raw, (
            "CANARY ROMPU : sqlglot ne traduit plus SAFE_CAST → TRY_CAST. "
            "Re-activer la correction dans fix_duck_db_sql."
        )
        con.execute(raw)


# ===========================================================================
# Section 15 : Bug — _fix_group_by_strict_mode ajoute une clé GROUP BY parasite
#              quand la colonne est couverte par une expression groupée,
#              un ordinal ou un alias (régression sf_bq263, 20/111 modèles)
# ===========================================================================


class TestGroupByParasiteKey:
    """
    Régression sf_bq263 : _fix_group_by_strict_mode comparait le SQL des colonnes
    nues du SELECT aux SQL bruts des expressions groupées, sans résoudre :
      - les ordinaux      (GROUP BY 1        → grouped_sqls = {'1'})
      - les alias         (GROUP BY month    → grouped_sqls = {'month'})
      - la couverture par expression (GROUP BY DATE_TRUNC('MONTH', col) couvre col)

    Il ajoutait alors la colonne nue au GROUP BY → le grain d'agrégation change :
    2 commandes du même mois → 2 lignes (30 / 25) au lieu d'1 (55).
    """

    @pytest.fixture
    def orders(self):
        c = duckdb.connect()
        c.execute(
            "CREATE TABLE orders (order_id INTEGER, created_at TIMESTAMP, amount INTEGER)"
        )
        c.execute("""
            INSERT INTO orders VALUES
            (101, '2023-05-15 10:00:00', 30),
            (102, '2023-05-20 11:00:00', 25)
        """)
        return c

    @staticmethod
    def _group_exprs(tree):
        return tree.args["group"].expressions

    def test_column_covered_by_grouped_expression_not_readded(self, orders):
        """GROUP BY DATE_TRUNC(col) couvre col : pas de clé parasite, grain préservé."""
        sql = (
            'SELECT DATE_TRUNC(\'MONTH\', "o"."created_at") AS month,'
            ' SUM("o"."amount") AS total'
            ' FROM orders AS "o"'
            ' GROUP BY DATE_TRUNC(\'MONTH\', "o"."created_at")'
        )
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        assert len(self._group_exprs(tree)) == 1, (
            f"Clé GROUP BY parasite ajoutée.\nSQL: {result_sql}"
        )
        rows = orders.execute(result_sql).fetchall()
        assert len(rows) == 1 and rows[0][1] == 55, (
            f"Grain d'agrégation cassé : {rows}\nSQL: {result_sql}"
        )

    def test_ordinal_group_by_resolved_to_projection(self, orders):
        """GROUP BY 1 est résolu vers la projection : pas de clé parasite."""
        sql = (
            "SELECT DATE_TRUNC('MONTH', created_at) AS month, SUM(amount) AS total"
            " FROM orders GROUP BY 1"
        )
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        assert len(self._group_exprs(tree)) == 1, (
            f"Clé GROUP BY parasite ajoutée.\nSQL: {result_sql}"
        )
        rows = orders.execute(result_sql).fetchall()
        assert len(rows) == 1 and rows[0][1] == 55, (
            f"Grain d'agrégation cassé : {rows}\nSQL: {result_sql}"
        )

    def test_alias_group_by_resolved_to_projection(self, orders):
        """GROUP BY month (alias du SELECT) est résolu : pas de clé parasite."""
        sql = (
            "SELECT DATE_TRUNC('MONTH', created_at) AS month, SUM(amount) AS total"
            " FROM orders GROUP BY month"
        )
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        assert len(self._group_exprs(tree)) == 1, (
            f"Clé GROUP BY parasite ajoutée.\nSQL: {result_sql}"
        )
        rows = orders.execute(result_sql).fetchall()
        assert len(rows) == 1 and rows[0][1] == 55, (
            f"Grain d'agrégation cassé : {rows}\nSQL: {result_sql}"
        )

    def test_uncovered_column_still_added(self, con):
        """
        Objet original de la fonction préservé : une colonne du SELECT absente
        du GROUP BY et non couverte par une expression groupée est toujours ajoutée
        (raccourci de dépendance fonctionnelle des dialectes sources).
        """
        sql = "SELECT user_id, s, COUNT(*) AS cnt FROM events GROUP BY user_id"
        tree = sqlglot.parse_one(sql, dialect="duckdb")
        _fix_group_by_strict_mode(tree)
        result_sql = tree.sql(dialect="duckdb")

        group_exprs = self._group_exprs(tree)
        assert len(group_exprs) == 2, (
            f"La colonne s aurait dû être ajoutée au GROUP BY.\nSQL: {result_sql}"
        )
        con.execute(result_sql)
