"""
Unit tests for build_query/constraint_simplifier.py

All tests are pure-Python, no DB, no LLM.
They cover the five reference examples from the spec plus additional edge cases.
"""

from build_query.constraint_simplifier import (
    ColumnRef,
    FilterConstraint,
    SimplificationResult,
    _UnionFind,
    _LineageResolver,
    extract_constraints,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


# ─── Helpers ──────────────────────────────────────────────────────────────────


def col(table: str, column: str) -> ColumnRef:
    return ColumnRef(table.lower(), column.lower())


def same_class(result: SimplificationResult, *cols: tuple[str, str]) -> bool:
    """Return True if all given (table, col) pairs belong to the same equivalence class."""
    refs = {col(t, c) for t, c in cols}
    for cls in result.equivalence_classes:
        if refs.issubset(cls):
            return True
    return False


# ─── Union-Find ───────────────────────────────────────────────────────────────


class TestUnionFind:
    def test_singleton(self):
        uf = _UnionFind()
        a = col("a", "x")
        uf.add(a)
        assert uf.find(a) == a

    def test_union_two(self):
        uf = _UnionFind()
        a, b = col("a", "x"), col("b", "x")
        uf.union(a, b)
        assert uf.find(a) == uf.find(b)

    def test_union_three_transitivity(self):
        uf = _UnionFind()
        a, b, c = col("a", "x"), col("b", "x"), col("c", "x")
        uf.union(a, b)
        uf.union(b, c)
        assert uf.find(a) == uf.find(b) == uf.find(c)

    def test_groups_non_singleton(self):
        uf = _UnionFind()
        a, b, c = col("a", "x"), col("b", "x"), col("c", "y")
        uf.union(a, b)
        uf.add(c)
        groups = uf.groups()
        assert len(groups) == 1
        assert frozenset({a, b}) in groups

    def test_groups_empty(self):
        uf = _UnionFind()
        assert uf.groups() == []

    def test_path_compression(self):
        uf = _UnionFind()
        cols = [col("t", str(i)) for i in range(5)]
        for i in range(4):
            uf.union(cols[i], cols[i + 1])
        root = uf.find(cols[0])
        # After find all nodes should point directly to root
        for c in cols:
            assert uf.find(c) == root


# ─── CTE map builder ──────────────────────────────────────────────────────────


class TestBuildCteMap:
    def test_simple_cte(self):
        import sqlglot

        sql = """
        WITH cte1 AS (
            SELECT a.x, b.z
            FROM myproject.analytics.a AS a
            JOIN myproject.analytics.b AS b ON a.x = b.x
        )
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        assert "cte1" in resolver._cte_names
        assert resolver.resolve(ColumnRef("cte1", "x")) == ColumnRef("a", "x")
        assert resolver.resolve(ColumnRef("cte1", "z")) == ColumnRef("b", "z")

    def test_no_cte(self):
        import sqlglot

        sql = "SELECT a.x FROM myproject.analytics.a AS a WHERE a.y = 1"
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        assert resolver._cte_names == set()

    def test_resolve_all_non_cte_passthrough(self):
        """Non-CTE column: resolve_all returns a single-element list unchanged."""
        import sqlglot

        sql = "SELECT a.x FROM myproject.analytics.a AS a"
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        c = ColumnRef("a", "x")
        assert resolver.resolve_all(c) == [c]

    def test_resolve_all_simple_cte_single_source(self):
        """CTE column mapped to a single base column → list of one element."""
        import sqlglot

        sql = """
        WITH cte1 AS (
            SELECT b.z FROM myproject.analytics.b AS b
        )
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        result = resolver.resolve_all(ColumnRef("cte1", "z"))
        assert result == [col("b", "z")]

    def test_resolve_all_computed_cte_two_sources(self):
        """CTE column computed from two columns → both appear in resolve_all result."""
        import sqlglot

        sql = """
        WITH cte1 AS (
            SELECT a.x1 + b.d2 AS cte_mix
            FROM myproject.analytics.a AS a
            JOIN myproject.analytics.b AS b ON a.id = b.id
        )
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        result = resolver.resolve_all(ColumnRef("cte1", "cte_mix"))
        result_set = set(result)
        assert col("a", "x1") in result_set
        assert col("b", "d2") in result_set
        assert len(result) == 2

    def test_resolve_all_cached(self):
        """Second call returns the cached result (same object)."""
        import sqlglot

        sql = """
        WITH cte1 AS (SELECT b.z FROM myproject.analytics.b AS b)
        SELECT * FROM cte1
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        r1 = resolver.resolve_all(ColumnRef("cte1", "z"))
        r2 = resolver.resolve_all(ColumnRef("cte1", "z"))
        assert r1 is r2

    def test_resolve_all_multi_level_cte_not_projected(self):
        """cte2.a1 → cte1.a1 → a.a1, even when a1 is NOT in the outermost SELECT."""
        import sqlglot

        sql = """
        WITH cte1 AS (
            SELECT a.x, a.a1 FROM myproject.analytics.a AS a
        ),
        cte2 AS (
            SELECT cte1.x, cte1.a1 FROM cte1
        )
        SELECT x FROM cte2
        """
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        result = resolver.resolve_all(ColumnRef("cte2", "a1"))
        assert col("a", "a1") in result


# ─── extract_constraints ──────────────────────────────────────────────────────


class TestExtractConstraints:
    def test_example1_join_filter(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        WHERE a.y = 10
        """
        filters, equalities, functional, _ = extract_constraints(sql)

        assert any(a == col("a", "x") and b == col("b", "x") for a, b in equalities), (
            "Expected a.x = b.x equality"
        )
        assert any(
            f.column == col("a", "y") and f.op == "eq" and f.value == 10
            for f in filters
        ), "Expected a.y = 10 filter"

    def test_example2_like_and_functional(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.a1 = TRIM(b.b1)
        WHERE b.b1 LIKE '%abc%'
        """
        filters, equalities, functional, _ = extract_constraints(sql)

        assert any(
            f.column == col("b", "b1") and f.op == "like" and "abc" in str(f.value)
            for f in filters
        ), "Expected b.b1 LIKE '%abc%'"
        assert any(
            fc.derived == col("a", "a1") and fc.source == col("b", "b1")
            for fc in functional
        ), "Expected a.a1 = TRIM(b.b1) functional"

    def test_example5_chain_join(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        JOIN myproject.analytics.c AS c ON b.x = c.x
        WHERE c.x > 50
        """
        filters, equalities, functional, _ = extract_constraints(sql)

        pairs = set(map(frozenset, equalities))
        assert frozenset({col("a", "x"), col("b", "x")}) in pairs
        assert frozenset({col("b", "x"), col("c", "x")}) in pairs
        assert any(f.column == col("c", "x") and f.op == "gt" for f in filters)

    def test_between(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.dt BETWEEN '2020-01-01' AND '2025-01-01'
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(f.op == "between" for f in filters)

    def test_in_operator(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.status IN ('active', 'pending')
        """
        filters, _, _, _ = extract_constraints(sql)
        f = next((f for f in filters if f.op == "in"), None)
        assert f is not None
        assert set(f.value) == {"active", "pending"}

    def test_not_in_operator(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.status NOT IN ('deleted')
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(f.op == "not_in" for f in filters)

    def test_is_null(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.deleted_at IS NULL
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(f.op == "is_null" for f in filters)

    def test_is_not_null(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.name IS NOT NULL
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(f.op == "is_not_null" for f in filters)

    def test_literal_on_left(self):
        """10 = a.y should be interpreted as a.y = 10."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE 10 = a.y
        """
        filters, _, _, _ = extract_constraints(sql)
        assert any(
            f.column == col("a", "y") and f.op == "eq" and f.value == 10
            for f in filters
        )

    def test_lt_lte_operators(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.t <= '2025-01-01' AND a.amount > 0
        """
        filters, _, _, _ = extract_constraints(sql)
        ops = {f.op for f in filters}
        assert "lte" in ops
        assert "gt" in ops

    def test_union_all_both_branches(self):
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        filters, _, _, _ = extract_constraints(sql)
        values = {f.value for f in filters}
        assert "alpha" in values
        assert "beta" in values


# ─── FilterConstraint.needs_llm ───────────────────────────────────────────────


class TestNeedsLlm:
    def test_like_needs_llm(self):
        f = FilterConstraint(column=col("a", "x"), op="like", value="%foo%")
        assert f.needs_llm() is True

    def test_eq_no_llm(self):
        f = FilterConstraint(column=col("a", "x"), op="eq", value="val")
        assert f.needs_llm() is False

    def test_gt_needs_llm(self):
        f = FilterConstraint(column=col("a", "x"), op="gt", value=50)
        assert f.needs_llm() is True

    def test_in_no_llm(self):
        f = FilterConstraint(column=col("a", "x"), op="in", value=["a", "b"])
        assert f.needs_llm() is False

    def test_between_no_llm(self):
        f = FilterConstraint(column=col("a", "x"), op="between", value=(1, 10))
        assert f.needs_llm() is False
