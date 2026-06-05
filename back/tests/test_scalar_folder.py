"""
TDD tests for build_query.scalar_folder.

Stratégie :
- _is_foldable     : détection pure (AST seul, pas de DuckDB)
- _to_literal      : conversion Python → nœud sqlglot
- _eval_with_duckdb: évaluation locale via DuckDB
- fold_scalar_expressions : intégration bout-en-bout sur un AST SELECT
"""

import datetime
import decimal

import sqlglot
from sqlglot import expressions as exp

from build_query.scalar_folder import (
    _eval_with_duckdb,
    _is_foldable,
    _to_literal,
    fold_scalar_expressions,
)


# ---------------------------------------------------------------------------
# _is_foldable
# ---------------------------------------------------------------------------


class TestIsFoldable:
    def test_column_ref_not_foldable(self):
        node = sqlglot.parse_one("col_a", read="bigquery")
        assert _is_foldable(node) is False

    def test_column_in_expr_not_foldable(self):
        node = sqlglot.parse_one("col_a + 1", read="bigquery")
        assert _is_foldable(node) is False

    def test_plain_integer_literal_not_foldable(self):
        assert _is_foldable(exp.Literal.number(42)) is False

    def test_plain_string_literal_not_foldable(self):
        assert _is_foldable(exp.Literal.string("hello")) is False

    def test_null_not_foldable(self):
        assert _is_foldable(exp.null()) is False

    def test_boolean_not_foldable(self):
        assert _is_foldable(exp.Boolean(this=True)) is False

    def test_current_date_not_foldable(self):
        node = sqlglot.parse_one("CURRENT_DATE()", read="bigquery")
        assert _is_foldable(node) is False

    def test_current_timestamp_not_foldable(self):
        node = sqlglot.parse_one("CURRENT_TIMESTAMP()", read="bigquery")
        assert _is_foldable(node) is False

    def test_aggregate_sum_not_foldable(self):
        node = sqlglot.parse_one("SUM(1)", read="bigquery")
        assert _is_foldable(node) is False

    def test_aggregate_count_not_foldable(self):
        node = sqlglot.parse_one("COUNT(*)", read="bigquery")
        assert _is_foldable(node) is False

    def test_window_function_not_foldable(self):
        node = sqlglot.parse_one("ROW_NUMBER() OVER (ORDER BY 1)", read="bigquery")
        assert _is_foldable(node) is False

    def test_subquery_not_foldable(self):
        node = sqlglot.parse_one("(SELECT 1)", read="bigquery")
        assert _is_foldable(node) is False

    def test_arithmetic_on_literals_is_foldable(self):
        node = sqlglot.parse_one("1 + 2", read="bigquery")
        assert _is_foldable(node) is True

    def test_string_function_on_literals_is_foldable(self):
        node = sqlglot.parse_one("UPPER('hello')", read="bigquery")
        assert _is_foldable(node) is True

    def test_cast_on_literal_is_foldable(self):
        node = sqlglot.parse_one("CAST('123' AS INT64)", read="bigquery")
        assert _is_foldable(node) is True

    def test_date_diff_bq_is_foldable(self):
        node = sqlglot.parse_one(
            "DATE_DIFF(DATE '2024-12-31', DATE '2024-01-01', DAY)", read="bigquery"
        )
        assert _is_foldable(node) is True

    def test_nested_scalar_function_is_foldable(self):
        node = sqlglot.parse_one("LENGTH(UPPER('hello'))", read="bigquery")
        assert _is_foldable(node) is True


# ---------------------------------------------------------------------------
# _to_literal
# ---------------------------------------------------------------------------


class TestToLiteral:
    def test_int(self):
        node = _to_literal(42)
        assert isinstance(node, exp.Literal)
        assert node.is_number

    def test_float(self):
        node = _to_literal(3.14)
        assert isinstance(node, exp.Literal)
        assert node.is_number

    def test_decimal(self):
        node = _to_literal(decimal.Decimal("1.5"))
        assert isinstance(node, exp.Literal)
        assert node.is_number

    def test_string(self):
        node = _to_literal("hello")
        assert isinstance(node, exp.Literal)
        assert node.is_string

    def test_bool_true(self):
        node = _to_literal(True)
        assert isinstance(node, exp.Boolean)
        assert node.this is True

    def test_bool_false(self):
        node = _to_literal(False)
        assert isinstance(node, exp.Boolean)
        assert node.this is False

    def test_none(self):
        node = _to_literal(None)
        assert isinstance(node, exp.Null)

    def test_date_becomes_string_literal(self):
        node = _to_literal(datetime.date(2024, 1, 1))
        assert isinstance(node, exp.Literal)
        assert node.is_string
        assert "2024-01-01" in node.this


# ---------------------------------------------------------------------------
# _eval_with_duckdb
# ---------------------------------------------------------------------------


class TestEvalWithDuckDB:
    def test_integer_arithmetic(self):
        assert _eval_with_duckdb("1 + 2") == 3

    def test_string_upper(self):
        assert _eval_with_duckdb("UPPER('hello')") == "HELLO"

    def test_string_length(self):
        assert _eval_with_duckdb("LENGTH('hello')") == 5

    def test_date_diff(self):
        result = _eval_with_duckdb(
            "date_diff('day', DATE '2024-01-01', DATE '2024-12-31')"
        )
        assert (
            result == 365
        )  # Jan 1 → Dec 31, 2024 (année bissextile = 366 j, mais diff = 365)

    def test_null_result(self):
        assert _eval_with_duckdb("NULL") is None

    def test_boolean_result(self):
        assert _eval_with_duckdb("1 = 1") is True


# ---------------------------------------------------------------------------
# fold_scalar_expressions — intégration
# ---------------------------------------------------------------------------


class TestFoldScalarExpressions:
    def _fold(self, sql: str, dialect: str = "bigquery") -> str:
        ast = sqlglot.parse_one(sql, read=dialect)
        result = fold_scalar_expressions(ast, source_dialect=dialect)
        return result.sql(dialect=dialect)

    # -- expressions qui doivent être repliées --

    def test_integer_arithmetic_folded(self):
        result = self._fold("SELECT 1 + 2 AS n FROM t")
        assert "1 + 2" not in result
        assert "3" in result

    def test_string_function_folded(self):
        result = self._fold("SELECT UPPER('hello') AS s FROM t")
        assert "UPPER" not in result
        assert "HELLO" in result

    def test_alias_preserved_after_fold(self):
        result = self._fold("SELECT 1 + 2 AS my_alias FROM t")
        assert "my_alias" in result
        assert "3" in result

    def test_bigquery_date_diff_transpiled_and_folded(self):
        sql = "SELECT DATE_DIFF(DATE '2024-12-31', DATE '2024-01-01', DAY) AS n FROM t"
        result = self._fold(sql, dialect="bigquery")
        assert "DATE_DIFF" not in result
        assert "365" in result  # transpilé BQ→DuckDB puis évalué localement

    # -- expressions qui NE doivent PAS être repliées --

    def test_column_ref_preserved(self):
        result = self._fold("SELECT col_a * 2 AS doubled FROM t")
        assert "col_a" in result

    def test_current_date_preserved(self):
        result = self._fold("SELECT CURRENT_DATE() AS today FROM t")
        assert "CURRENT_DATE" in result.upper()

    def test_aggregate_preserved(self):
        result = self._fold("SELECT SUM(amount) AS total FROM t")
        assert "SUM" in result.upper()
        assert "amount" in result

    # -- projections mixtes --

    def test_mixed_projections(self):
        sql = "SELECT 1 + 2 AS scalar_val, col_a AS passthrough FROM t"
        result = self._fold(sql)
        assert "3" in result
        assert "col_a" in result
        assert "1 + 2" not in result

    # -- CTE : seules les projections scalaires dans chaque scope sont repliées --

    def test_cte_scalar_projection_folded(self):
        sql = """
        WITH base AS (
            SELECT 10 * 10 AS hundred, col_a FROM t
        )
        SELECT hundred, col_a FROM base
        """
        result = self._fold(sql)
        assert "10 * 10" not in result
        assert "100" in result

    # -- fallback silencieux sur erreur DuckDB --

    def test_invalid_expr_kept_unchanged(self):
        # Une fonction inexistante dans DuckDB doit laisser l'expression intacte
        sql = "SELECT UNKNOWN_FUNC_XYZ(1, 2) AS x FROM t"
        ast = sqlglot.parse_one(sql)
        # Ne doit pas lever d'exception
        result = fold_scalar_expressions(ast, source_dialect="bigquery")
        result_sql = result.sql()
        assert "UNKNOWN_FUNC_XYZ" in result_sql.upper()


# ---------------------------------------------------------------------------
# fold_scalar_expressions — WHERE / HAVING / expressions imbriquées
# ---------------------------------------------------------------------------


class TestFoldScalarExpressionsWhereClause:
    """
    fold_scalar_expressions doit replier les sous-expressions scalaires dans
    toutes les clauses (WHERE, HAVING, JOIN ON), pas seulement les projections SELECT.
    """

    def _fold(self, sql: str, dialect: str = "bigquery") -> str:
        ast = sqlglot.parse_one(sql, read=dialect)
        result = fold_scalar_expressions(ast, source_dialect=dialect)
        return result.sql(dialect=dialect)

    def test_scalar_in_where_folded(self):
        """Sous-expression scalaire dans WHERE repliée en littéral."""
        result = self._fold("SELECT col FROM t WHERE col >= 10 * 10")
        assert "10 * 10" not in result
        assert "100" in result
        assert "col" in result  # la référence de colonne reste

    def test_column_comparison_preserved(self):
        """La comparaison avec une colonne n'est pas repliée."""
        result = self._fold("SELECT col FROM t WHERE col >= 100")
        assert "col" in result
        assert ">=" in result

    def test_parse_date_in_where_folded(self):
        """
        Cas utilisateur : PARSE_DATE('%d-%m-%Y', '01-01-2026') dans WHERE
        doit être remplacé par la date calculée.
        """
        sql = (
            "SELECT MAX(partition_date) FROM `proj.ds.t` "
            "WHERE partition_date <= PARSE_DATE('%d-%m-%Y', '01-01-2026')"
        )
        result = self._fold(sql, dialect="bigquery")
        assert "PARSE_DATE" not in result
        assert "2026-01-01" in result

    def test_scalar_in_having_folded(self):
        """Sous-expression scalaire dans HAVING repliée."""
        result = self._fold(
            "SELECT col, COUNT(*) AS n FROM t GROUP BY col HAVING COUNT(*) > 2 + 3"
        )
        assert "2 + 3" not in result
        assert "5" in result

    def test_alias_preserved_with_where_fold(self):
        """L'alias de projection est préservé même après repliage dans WHERE."""
        result = self._fold("SELECT 1 + 1 AS two, col FROM t WHERE col > 3 * 3")
        assert "two" in result  # alias conservé
        assert "3 * 3" not in result
        assert "9" in result
        assert "1 + 1" not in result
        assert "2" in result
