"""
Tests for _qualify_group_order_by_aliases and _fix_group_by_strict_mode.

Pipeline de référence :
    bq_sql → sqlglot.parse_one(dialect='bigquery') → _qualify_group_order_by_aliases()
           → _fix_group_by_strict_mode() → .sql(dialect='duckdb') → DuckDB execute

Le canary test vérifie que sqlglot ne corrige pas encore ce cas nativement.
"""

import duckdb
import pytest
import sqlglot

from utils.examples import _qualify_group_order_by_aliases, _fix_group_by_strict_mode


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


# ---------------------------------------------------------------------------
# GROUP BY ALL — _fix_group_by_strict_mode doit être no-op
# ---------------------------------------------------------------------------


class TestFixGroupByStrictModeGroupByAll:
    """
    Régression : _fix_group_by_strict_mode ajoutait les colonnes SELECT après ALL,
    produisant GROUP BY ALL a, b — syntaxe invalide en DuckDB.
    """

    def test_group_by_all_ast_unchanged(self):
        """GROUP BY ALL dans l'AST : group.args['all'] = True, expressions = []."""
        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY ALL"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        group = tree.args["group"]
        assert group.args.get("all") is True
        assert group.expressions == []

    def test_group_by_all_sql_unchanged(self):
        """Le SQL généré après fix doit contenir GROUP BY ALL sans colonne supplémentaire."""
        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY ALL"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        result = tree.sql(dialect="duckdb")
        assert "GROUP BY ALL" in result
        assert "GROUP BY ALL a" not in result
        assert "GROUP BY ALL b" not in result

    def test_group_by_all_executes_on_duckdb(self):
        """End-to-end : GROUP BY ALL BigQuery → pipeline fix → DuckDB s'exécute sans erreur."""
        con = duckdb.connect()
        con.execute("CREATE TABLE t (a VARCHAR, b VARCHAR, c INTEGER)")
        con.execute("INSERT INTO t VALUES ('x', 'y', 1), ('x', 'y', 2), ('z', 'w', 5)")

        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY ALL"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        _fix_group_by_strict_mode(tree)
        duckdb_sql = tree.sql(dialect="duckdb")

        rows = con.execute(duckdb_sql).fetchall()
        assert len(rows) == 2
        totals = {(r[0], r[1]): r[2] for r in rows}
        assert totals[("x", "y")] == 3
        assert totals[("z", "w")] == 5

    def test_group_by_all_with_cte_executes_on_duckdb(self):
        """GROUP BY ALL dans une CTE : toutes les étapes du pipeline passent."""
        con = duckdb.connect()
        con.execute(
            "CREATE TABLE orders (region VARCHAR, product VARCHAR, amount INTEGER)"
        )
        con.execute("""
            INSERT INTO orders VALUES
            ('EU', 'A', 10), ('EU', 'A', 20), ('US', 'B', 5)
        """)

        sql = """
            WITH agg AS (
                SELECT region, product, SUM(amount) AS total
                FROM orders
                GROUP BY ALL
            )
            SELECT region, SUM(total) AS region_total FROM agg GROUP BY ALL
        """
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        _fix_group_by_strict_mode(tree)
        duckdb_sql = tree.sql(dialect="duckdb")

        rows = con.execute(duckdb_sql).fetchall()
        assert len(rows) == 2

    def test_regular_group_by_still_gets_strict_fix(self):
        """Non-régression : GROUP BY x, y (pas ALL) reçoit toujours le strict-mode fix."""
        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY a"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        result = tree.sql(dialect="duckdb")
        # b doit avoir été ajouté au GROUP BY
        assert "b" in result.split("GROUP BY")[1]


class TestFixGroupByStrictModeGroupingSets:
    """
    Régression : ROLLUP, CUBE et GROUPING SETS avaient group.expressions == []
    mais la fonction leur ajoutait quand même des colonnes, produisant des résultats
    silencieusement faux (pas d'erreur DuckDB, mais les subtotals NULL disparaissent).
    """

    @pytest.fixture
    def con(self):
        c = duckdb.connect()
        c.execute("CREATE TABLE t (a VARCHAR, b VARCHAR, c INTEGER)")
        c.execute("INSERT INTO t VALUES ('x', 'y', 1), ('x', 'y', 2), ('z', 'w', 5)")
        return c

    @pytest.mark.parametrize(
        "sql,keyword",
        [
            (
                "SELECT a, b, SUM(c) AS total FROM t GROUP BY ROLLUP(a, b)",
                "ROLLUP",
            ),
            (
                "SELECT a, b, SUM(c) AS total FROM t GROUP BY CUBE(a, b)",
                "CUBE",
            ),
            (
                "SELECT a, b, SUM(c) AS total FROM t GROUP BY GROUPING SETS ((a, b), (a), ())",
                "GROUPING SETS",
            ),
        ],
    )
    def test_sql_unchanged_after_fix(self, sql, keyword):
        """ROLLUP/CUBE/GROUPING SETS ne doivent pas recevoir de colonnes supplémentaires."""
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        result = tree.sql(dialect="duckdb")
        after_group_by = result.split("GROUP BY")[1]
        assert keyword in after_group_by
        assert not after_group_by.strip().startswith("a")
        assert not after_group_by.strip().startswith("b")

    def test_rollup_produces_correct_subtotals(self, con):
        """ROLLUP doit produire les lignes NULL de subtotal — pas de dérive silencieuse."""
        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY ROLLUP(a, b)"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        duckdb_sql = tree.sql(dialect="duckdb")

        rows = con.execute(duckdb_sql).fetchall()
        # ROLLUP(a, b) → (a,b), (a), () = 5 lignes dont 2 avec NULL
        null_rows = [r for r in rows if r[0] is None or r[1] is None]
        assert len(null_rows) >= 2, f"Subtotals NULL absents : {rows}"

    def test_grouping_sets_produces_correct_rows(self, con):
        """GROUPING SETS ((a,b),(a),()) doit produire le bon nombre de lignes."""
        sql = "SELECT a, b, SUM(c) AS total FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _fix_group_by_strict_mode(tree)
        duckdb_sql = tree.sql(dialect="duckdb")

        rows = con.execute(duckdb_sql).fetchall()
        # 2 lignes (a,b) + 2 lignes (a) + 1 ligne () = 5
        assert len(rows) == 5


# ---------------------------------------------------------------------------
# Bug 5 — _qualify_group_order_by_aliases ne résout pas les alias
#          à l'intérieur de ROLLUP / CUBE / GROUPING SETS
# ---------------------------------------------------------------------------


class TestQualifyAliasesInsideGroupingSets:
    """
    Spec des corrections attendues — ces tests échouent tant que le bug existe.

    Symptôme : quand un alias ambigu (même nom dans deux tables jointes) est
    référencé à l'intérieur d'un nœud ROLLUP, CUBE ou GROUPING SETS,
    _qualify_group_order_by_aliases ne le résout pas → DuckDB lève
    "Binder Error: Ambiguous reference to column name".

    Correction attendue : parcourir aussi les colonnes dans rollup/cube/grouping_sets
    et appliquer le même remplacement alias → colonne qualifiée.
    """

    @pytest.fixture
    def con(self):
        c = duckdb.connect()
        c.execute("CREATE TABLE cte_a AS SELECT 2020 AS year, 100 AS v")
        c.execute("CREATE TABLE cte_b AS SELECT 2020 AS year, 50 AS v")
        return c

    def test_rollup_with_ambiguous_alias_is_resolved(self):
        """
        GROUP BY ROLLUP(year) où year est un alias de t.year doit devenir
        GROUP BY ROLLUP(t.year) après _qualify_group_order_by_aliases.
        """
        sql = """
            SELECT t.year AS year, SUM(t.v) AS s
            FROM cte_a AS t
            JOIN cte_b AS b ON t.year = b.year
            GROUP BY ROLLUP(year)
        """
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        result = tree.sql(dialect="duckdb")
        # year dans ROLLUP doit être qualifié en t.year
        rollup_part = result.split("ROLLUP")[1]
        assert "t.year" in rollup_part.lower() or "t.`year`" in rollup_part.lower(), (
            result
        )

    def test_rollup_with_ambiguous_alias_executes_on_duckdb(self, con):
        """
        Après le fix, la requête avec ROLLUP et alias ambigu doit s'exécuter
        sans Binder Error et retourner les subtotals NULL attendus.
        """
        sql = """
            SELECT t.year AS year, SUM(t.v) AS s
            FROM cte_a AS t
            JOIN cte_b AS b ON t.year = b.year
            GROUP BY ROLLUP(year)
        """
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        duckdb_sql = tree.sql(dialect="duckdb")
        rows = con.execute(duckdb_sql).fetchall()
        # ROLLUP(year) → (year) + () = 2 lignes dont une avec NULL
        null_rows = [r for r in rows if r[0] is None]
        assert len(null_rows) == 1, f"Subtotal NULL attendu : {rows}"

    def test_cube_with_ambiguous_alias_is_resolved(self):
        """GROUP BY CUBE(year) : year doit être qualifié en t.year."""
        sql = """
            SELECT t.year AS year, SUM(t.v) AS s
            FROM cte_a AS t
            JOIN cte_b AS b ON t.year = b.year
            GROUP BY CUBE(year)
        """
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        result = tree.sql(dialect="duckdb")
        cube_part = result.split("CUBE")[1]
        assert "t.year" in cube_part.lower() or "t.`year`" in cube_part.lower(), result

    def test_grouping_sets_with_ambiguous_alias_is_resolved(self):
        """GROUP BY GROUPING SETS ((year), ()) : year doit être qualifié."""
        sql = """
            SELECT t.year AS year, SUM(t.v) AS s
            FROM cte_a AS t
            JOIN cte_b AS b ON t.year = b.year
            GROUP BY GROUPING SETS ((year), ())
        """
        tree = sqlglot.parse_one(sql, dialect="bigquery")
        _qualify_group_order_by_aliases(tree)
        result = tree.sql(dialect="duckdb")
        gs_part = result.split("GROUPING SETS")[1]
        assert "t.year" in gs_part.lower() or "t.`year`" in gs_part.lower(), result


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
