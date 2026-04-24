"""
Unit tests for build_query/constraint_simplifier.py

All tests are pure-Python, no DB, no LLM.
They cover the five reference examples from the spec plus additional edge cases.
"""

import sqlglot
from sqlglot import expressions as exp
from build_query.constraint_simplifier import (
    ColumnRef,
    FilterConstraint,
    SimplificationResult,
    _LineageResolver,
    _build_column_sql,
    extract_constraints,
    simplify,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


# ─── Helpers ──────────────────────────────────────────────────────────────────


def col(table: str, column: str) -> ColumnRef:
    return ColumnRef(table.lower(), column.lower())


def source_cols_of(filters: list, table: str, column: str) -> list[ColumnRef]:
    """Return source_columns of the first filter whose column matches table.column."""
    c = col(table, column)
    for f in filters:
        if f.column == c:
            return f.source_columns
    return []


def filter_ops(result: SimplificationResult, table: str, column: str) -> list[str]:
    """Return the op-list of filters on a given source column."""
    c = col(table, column)
    return [f.op for f in result.source_columns.get(c, [])]


def is_source(result: SimplificationResult, table: str, column: str) -> bool:
    return col(table, column) in result.source_columns


def is_derived(result: SimplificationResult, table: str, column: str) -> bool:
    return col(table, column) in result.derived_columns


def derived_from(
    result: SimplificationResult, table: str, column: str
) -> ColumnRef | None:
    entry = result.derived_columns.get(col(table, column))
    return entry[0] if entry else None


def same_class(result: SimplificationResult, *cols: tuple[str, str]) -> bool:
    """Return True if all given (table, col) pairs belong to the same equivalence class."""
    refs = {col(t, c) for t, c in cols}
    for cls in result.equivalence_classes:
        if refs.issubset(cls):
            return True
    return False


# ─── Anti-join patterns (LEFT/RIGHT/FULL JOIN + IS NULL) ─────────────────────


class TestAntiJoin:
    """Verify that outer-join + IS NULL combinations are NOT treated as equalities."""

    def test_left_join_right_is_null_no_equality(self):
        """LEFT JOIN b ON a.id = b.id WHERE b.id IS NULL → anti-join, no equality."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE b.id IS NULL
        """
        filters, equalities, _, col_ineq = extract_constraints(sql)
        pairs = [frozenset({a, b}) for a, b in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) not in pairs
        ineq_pairs = [frozenset({a, b}) for a, b in col_ineq]
        assert frozenset({col("a", "id"), col("b", "id")}) in ineq_pairs

    def test_left_join_right_is_null_filter_preserved(self):
        """The IS NULL filter on b.id must still appear in filters."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE b.id IS NULL
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(f.column == col("b", "id") and f.op == "is_null" for f in filters)

    def test_left_join_no_null_filter_keeps_equality(self):
        """Without IS NULL in WHERE, a LEFT JOIN equality is still captured."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE a.status = 'active'
        """
        _, equalities, _, _ = extract_constraints(sql)
        pairs = [frozenset({a, b}) for a, b in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) in pairs

    def test_right_join_left_is_null_no_equality(self):
        """RIGHT JOIN b ON a.id = b.id WHERE a.id IS NULL → anti-join, no equality."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        RIGHT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE a.id IS NULL
        """
        _, equalities, _, col_ineq = extract_constraints(sql)
        pairs = [frozenset({a, b}) for a, b in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) not in pairs
        ineq_pairs = [frozenset({a, b}) for a, b in col_ineq]
        assert frozenset({col("a", "id"), col("b", "id")}) in ineq_pairs

    def test_full_join_is_null_no_equality(self):
        """FULL JOIN b ON a.id = b.id WHERE a.id IS NULL → no equality."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        FULL JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE a.id IS NULL
        """
        _, equalities, _, col_ineq = extract_constraints(sql)
        pairs = [frozenset({a, b}) for a, b in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) not in pairs
        ineq_pairs = [frozenset({a, b}) for a, b in col_ineq]
        assert frozenset({col("a", "id"), col("b", "id")}) in ineq_pairs

    def test_anti_join_other_on_filters_preserved(self):
        """Extra filter in ON clause must still be captured even in anti-join."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id AND b.status = 'active'
        WHERE b.id IS NULL
        """
        filters, equalities, _, col_ineq = extract_constraints(sql)
        # The equality must be gone, but the pair must be in col_inequalities
        pairs = [frozenset({x, y}) for x, y in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) not in pairs
        ineq_pairs = [frozenset({x, y}) for x, y in col_ineq]
        assert frozenset({col("a", "id"), col("b", "id")}) in ineq_pairs
        # The extra ON filter must be kept
        assert any(f.column == col("b", "status") and f.op == "eq" for f in filters)

    def test_anti_join_no_equivalence_class(self):
        """simplify() must not put anti-join columns in the same equivalence class."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE b.id IS NULL
        """
        r = simplify(sql)
        assert not same_class(r, ("a", "id"), ("b", "id"))

    def test_anti_join_in_simplify_result(self):
        """simplify() must expose the anti-join pair in col_inequalities."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        LEFT JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE b.id IS NULL
        """
        r = simplify(sql)
        ineq_pairs = [frozenset({a, b}) for a, b in r.col_inequalities]
        assert frozenset({col("a", "id"), col("b", "id")}) in ineq_pairs

    def test_inner_join_is_null_keeps_equality(self):
        """INNER JOIN is unaffected by the anti-join detection (no outer semantics)."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE b.other IS NULL
        """
        _, equalities, _, _ = extract_constraints(sql)
        pairs = [frozenset({x, y}) for x, y in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) in pairs


# ─── SAFE_CAST constraints ────────────────────────────────────────────────────


class TestSafeCastConstraints:
    """SAFE_CAST → safe_cast_not_null FilterConstraint."""

    def test_safe_cast_in_select(self):
        sql = """
        SELECT SAFE_CAST(a.col AS INT64)
        FROM myproject.analytics.a AS a
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(
            f.column == col("a", "col") and f.op == "safe_cast_not_null"
            for f in filters
        )

    def test_safe_cast_target_type_in_value(self):
        sql = """
        SELECT SAFE_CAST(a.col AS INT64)
        FROM myproject.analytics.a AS a
        """
        filters, _, _, _ = extract_constraints(sql)
        f = next((f for f in filters if f.op == "safe_cast_not_null"), None)
        assert f is not None
        assert "INT64" in f.value.upper()

    def test_safe_cast_in_where(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE SAFE_CAST(a.dt AS DATE) >= '2024-01-01'
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(
            f.column == col("a", "dt") and f.op == "safe_cast_not_null" for f in filters
        )

    def test_safe_cast_in_join_on(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON SAFE_CAST(a.col AS INT64) = b.id
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(
            f.column == col("a", "col") and f.op == "safe_cast_not_null"
            for f in filters
        )

    def test_multiple_safe_casts_different_columns(self):
        sql = """
        SELECT SAFE_CAST(a.x AS INT64), SAFE_CAST(a.y AS DATE)
        FROM myproject.analytics.a AS a
        """
        filters, _, _, _ = extract_constraints(sql)
        safe_cast = [f for f in filters if f.op == "safe_cast_not_null"]
        cols = {f.column.column for f in safe_cast}
        assert "x" in cols
        assert "y" in cols

    def test_safe_cast_deduplication(self):
        """Same SAFE_CAST appearing twice in the same SELECT produces only one constraint."""
        sql = """
        SELECT SAFE_CAST(a.col AS INT64), SAFE_CAST(a.col AS INT64)
        FROM myproject.analytics.a AS a
        """
        filters, _, _, _ = extract_constraints(sql)
        safe_cast = [
            f
            for f in filters
            if f.op == "safe_cast_not_null" and f.column == col("a", "col")
        ]
        assert len(safe_cast) == 1

    def test_safe_cast_in_simplify(self):
        """simplify() exposes safe_cast_not_null in source_columns."""
        sql = """
        SELECT SAFE_CAST(a.col AS INT64)
        FROM myproject.analytics.a AS a
        """
        r = simplify(sql)
        c = col("a", "col")
        assert c in r.source_columns
        assert any(f.op == "safe_cast_not_null" for f in r.source_columns[c])

    def test_safe_cast_needs_llm_false(self):
        f = FilterConstraint(
            column=col("a", "x"), op="safe_cast_not_null", value="INT64"
        )
        assert f.needs_llm() is False

    def test_safe_cast_deduplication_select_and_where(self):
        """Same SAFE_CAST in SELECT and WHERE produces only one constraint."""
        sql = """
        SELECT SAFE_CAST(a.col AS INT64)
        FROM myproject.analytics.a AS a
        WHERE SAFE_CAST(a.col AS INT64) > 0
        """
        filters, _, _, _ = extract_constraints(sql)
        safe_cast = [
            f
            for f in filters
            if f.op == "safe_cast_not_null" and f.column == col("a", "col")
        ]
        assert len(safe_cast) == 1


# ─── _build_column_sql ────────────────────────────────────────────────────────


class TestBuildColumnSql:
    """Tests for the minimal executable SQL builder."""

    def _source(self, sql: str) -> exp.Select:
        return sqlglot.parse_one(sql, dialect="bigquery")

    def test_strips_where_simple_column(self):
        """WHERE clause must be absent; FROM table must be present."""
        source = self._source(
            "SELECT p.z FROM myproject.analytics.p AS p WHERE p.p1 = 'x'"
        )
        col_expr = exp.column("z", table="p")
        out = _build_column_sql(col_expr, source, "bigquery")
        assert "WHERE" not in out.upper()
        assert "SELECT" in out.upper()
        assert "p.z" in out or "z" in out

    def test_keeps_needed_join(self):
        """A JOIN whose alias appears in col_expr must be preserved."""
        source = self._source("""
            SELECT p.p1 + a.b2 AS z
            FROM myproject.analytics.p AS p
            JOIN myproject.analytics.a AS a ON p.x = a.x
            WHERE p.p1 = 'x'
        """)
        col_expr = exp.Add(
            this=exp.column("p1", table="p"),
            expression=exp.column("b2", table="a"),
        )
        out = _build_column_sql(col_expr, source, "bigquery")
        assert "WHERE" not in out.upper()
        assert "JOIN" in out.upper()
        assert "b2" in out

    def test_drops_unneeded_join(self):
        """A JOIN not referenced in col_expr must be dropped."""
        source = self._source("""
            SELECT p.z AS z
            FROM myproject.analytics.p AS p
            JOIN myproject.analytics.a AS a ON p.x = a.x
            WHERE p.z > 0
        """)
        col_expr = exp.column("z", table="p")
        out = _build_column_sql(col_expr, source, "bigquery")
        assert "WHERE" not in out.upper()
        assert "JOIN" not in out.upper()

    def test_non_select_source_wraps_subquery(self):
        """Non-Select source must be wrapped as a subquery."""
        inner = sqlglot.parse_one(
            "SELECT x FROM myproject.analytics.t", dialect="bigquery"
        )
        subq = exp.Subquery(this=inner)
        col_expr = exp.column("x")
        out = _build_column_sql(col_expr, subq, "bigquery")
        assert "FROM" in out.upper()
        assert "SELECT" in out.upper()


# ─── Lineage SQL field ────────────────────────────────────────────────────────


class TestLineageSql:
    """Check that _LineageResolver.resolve() sets a clean lineage SQL."""

    def test_cte_lineage_no_where(self):
        """lineage field for a CTE column must be executable SQL without WHERE."""
        sql = """
        WITH cte1 AS (
            SELECT a.x, b.z
            FROM myproject.analytics.a AS a
            JOIN myproject.analytics.b AS b ON a.x = b.x
            WHERE b.z > 100
        )
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        resolved = resolver.resolve(ColumnRef("cte1", "z"))
        assert resolved.lineage != ""
        assert "WHERE" not in resolved.lineage.upper()
        assert "SELECT" in resolved.lineage.upper()

    def test_cte_lineage_drops_unneeded_join(self):
        """When col_expr only references the FROM table, the JOIN must be dropped."""
        sql = """
        WITH cte1 AS (
            SELECT b.z, a.x
            FROM myproject.analytics.b AS b
            JOIN myproject.analytics.a AS a ON b.x = a.x
        )
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        # b.z only references b (which is in FROM); the JOIN on a is unneeded
        resolved = resolver.resolve(ColumnRef("cte1", "z"))
        assert "WHERE" not in resolved.lineage.upper()
        assert "JOIN" not in resolved.lineage.upper()


# ─── source_columns on FilterConstraint ──────────────────────────────────────


class TestFilterConstraintSourceColumns:
    """Verify that source_columns is correctly populated for every constraint op."""

    def test_direct_column_eq(self):
        """Simple base-table equality: source_columns == [column]."""
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE a.y = 10"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.column == col("a", "y"))
        assert f.source_columns == [col("a", "y")]

    def test_direct_column_like(self):
        sql = "SELECT * FROM myproject.analytics.b AS b WHERE b.b1 LIKE '%abc%'"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "like")
        assert f.source_columns == [col("b", "b1")]

    def test_direct_column_is_null(self):
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE a.deleted_at IS NULL"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "is_null")
        assert f.source_columns == [col("a", "deleted_at")]

    def test_direct_column_is_not_null(self):
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE a.name IS NOT NULL"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "is_not_null")
        assert f.source_columns == [col("a", "name")]

    def test_direct_column_between(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.dt BETWEEN '2020-01-01' AND '2025-01-01'
        """
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "between")
        assert f.source_columns == [col("a", "dt")]

    def test_direct_column_in(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.status IN ('active', 'pending')
        """
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "in")
        assert f.source_columns == [col("a", "status")]

    def test_direct_column_not_in(self):
        sql = (
            "SELECT * FROM myproject.analytics.a AS a WHERE a.status NOT IN ('deleted')"
        )
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "not_in")
        assert f.source_columns == [col("a", "status")]

    def test_direct_column_not_like(self):
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE a.name NOT LIKE '%test%'"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "not_like")
        assert f.source_columns == [col("a", "name")]

    def test_literal_on_left_flipped(self):
        """10 = a.y — operator is flipped but source_columns still tracks a.y."""
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE 10 = a.y"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.column == col("a", "y"))
        assert f.source_columns == [col("a", "y")]

    def test_cte_filter_resolved_to_base_column(self):
        """CTE filter: source_columns contains the resolved base-table column."""
        sql = """
        WITH cte1 AS (
            SELECT b.z FROM myproject.analytics.b AS b WHERE b.z > 100
        )
        SELECT * FROM cte1
        """
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.column == col("b", "z"))
        assert col("b", "z") in f.source_columns

    def test_computed_cte_filter_multiple_source_columns(self):
        """Filter on a computed CTE column: source_columns lists all input columns."""
        sql = """
        WITH cte1 AS (
            SELECT a.x1 + b.d2 AS cte_mix
            FROM myproject.analytics.a AS a
            JOIN myproject.analytics.b AS b ON a.id = b.id
        )
        SELECT * FROM cte1 WHERE cte1.cte_mix = 'AADZ'
        """
        filters, _, _, _ = extract_constraints(sql)
        # The filter column may resolve to one of the base cols; find by source_columns
        f = next(
            (
                f
                for f in filters
                if any(c.column in ("x1", "d2") for c in f.source_columns)
            ),
            None,
        )
        assert f is not None, "Expected a filter with source_columns from a.x1 + b.d2"
        src_names = {c.column for c in f.source_columns}
        assert "x1" in src_names
        assert "d2" in src_names
        assert len(f.source_columns) == 2

    def test_safe_cast_source_columns(self):
        """SAFE_CAST constraint carries source_columns."""
        sql = "SELECT SAFE_CAST(a.col AS INT64) FROM myproject.analytics.a AS a"
        filters, _, _, _ = extract_constraints(sql)
        f = next(f for f in filters if f.op == "safe_cast_not_null")
        assert f.source_columns == [col("a", "col")]

    def test_multi_level_cte_filter_resolves_to_base(self):
        """cte2.a1 LIKE 'adda%' inside cte3 must resolve to a.a1.

        a1 is NOT projected in cte3 nor in the outermost SELECT — lineage
        must still trace: cte2.a1 → cte1.a1 → a.a1.
        """
        sql = """
        WITH cte1 AS (
            SELECT a.x, a.a1
            FROM myproject.analytics.a AS a
            WHERE a.y = 1
        ),
        cte2 AS (
            SELECT cte1.x, cte1.a1, b.b1
            FROM cte1
            JOIN myproject.analytics.b AS b ON cte1.x = b.x
        ),
        cte3 AS (
            SELECT cte2.x, cte2.b1
            FROM cte2
            JOIN myproject.analytics.c AS c ON cte2.x = c.x
            WHERE cte2.a1 LIKE 'adda%'
        )
        SELECT x, b1 FROM cte3
        """
        filters, _, _, _ = extract_constraints(sql)
        like_filter = next((f for f in filters if f.op == "like"), None)
        assert like_filter is not None, "Expected a LIKE filter on cte2.a1"
        assert col("a", "a1") in like_filter.source_columns
