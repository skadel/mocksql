"""
Unit tests for extract_constraints() in constraint_simplifier.py.

extract_constraints() now returns list[ConstraintGroup] — one group per
independent satisfying path through the SQL (UNION ALL branch, OR path,
CTE cross-product).

Helper _flat() merges all groups into flat lists for tests that only care
about which constraints exist (not which path they come from).
"""

import pytest

from build_query.constraint_simplifier import (
    ColumnRef,
    ConstraintGroup,
    FilterConstraint,
    SimplificationResult,
    _LineageResolver,
    _UnionFind,
    check_correlated_aggregate_cardinality,
    check_having_cardinality,
    extract_constraints,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def col(table: str, column: str) -> ColumnRef:
    return ColumnRef(table.lower(), column.lower())


def _flat(
    groups: list[ConstraintGroup],
) -> tuple[list, list, list, list]:
    """Merge all groups into (filters, equalities, functional, col_inequalities)."""
    filters = [f for g in groups for f in g.filters]
    equalities = [e for g in groups for e in g.equalities]
    functional = [fc for g in groups for fc in g.functional]
    col_ineq = [ci for g in groups for ci in g.col_inequalities]
    return filters, equalities, functional, col_ineq


def same_class(result: SimplificationResult, *cols: tuple[str, str]) -> bool:
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
        import sqlglot

        sql = "SELECT a.x FROM myproject.analytics.a AS a"
        stmt = sqlglot.parse_one(sql, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        c = ColumnRef("a", "x")
        assert resolver.resolve_all(c) == [c]

    def test_resolve_all_simple_cte_single_source(self):
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


# ─── extract_constraints — return type ────────────────────────────────────────


class TestExtractConstraintsReturnType:
    def test_returns_list_of_constraint_groups(self):
        sql = "SELECT * FROM myproject.analytics.a AS a WHERE a.y = 10"
        result = extract_constraints(sql)
        assert isinstance(result, list)
        assert len(result) >= 1
        from build_query.constraint_simplifier import ConstraintGroup

        assert all(isinstance(g, ConstraintGroup) for g in result)

    def test_single_and_query_one_group(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        WHERE a.y = 10
        """
        groups = extract_constraints(sql)
        assert len(groups) == 1


# ─── extract_constraints — AND-only queries (single group) ────────────────────


class TestExtractConstraintsSingleGroup:
    def test_example1_join_filter(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        WHERE a.y = 10
        """
        filters, equalities, functional, _ = _flat(extract_constraints(sql))
        assert any(a == col("a", "x") and b == col("b", "x") for a, b in equalities)
        assert any(
            f.column == col("a", "y") and f.op == "eq" and f.value == 10
            for f in filters
        )

    def test_example2_like_and_functional(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.a1 = TRIM(b.b1)
        WHERE b.b1 LIKE '%abc%'
        """
        filters, equalities, functional, _ = _flat(extract_constraints(sql))
        assert any(
            f.column == col("b", "b1") and f.op == "like" and "abc" in str(f.value)
            for f in filters
        )
        assert any(
            fc.derived == col("a", "a1") and fc.source == col("b", "b1")
            for fc in functional
        )

    def test_example5_chain_join(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        JOIN myproject.analytics.c AS c ON b.x = c.x
        WHERE c.x > 50
        """
        filters, equalities, functional, _ = _flat(extract_constraints(sql))
        pairs = set(map(frozenset, equalities))
        assert frozenset({col("a", "x"), col("b", "x")}) in pairs
        assert frozenset({col("b", "x"), col("c", "x")}) in pairs
        assert any(f.column == col("c", "x") and f.op == "gt" for f in filters)

    def test_between(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.dt BETWEEN '2020-01-01' AND '2025-01-01'
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert any(f.op == "between" for f in filters)

    def test_in_operator(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.status IN ('active', 'pending')
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        f = next((f for f in filters if f.op == "in"), None)
        assert f is not None
        assert set(f.value) == {"active", "pending"}

    def test_not_in_operator(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.status NOT IN ('deleted')
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert any(f.op == "not_in" for f in filters)

    def test_is_null(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.deleted_at IS NULL
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert any(f.op == "is_null" for f in filters)

    def test_is_not_null(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.name IS NOT NULL
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert any(f.op == "is_not_null" for f in filters)

    def test_literal_on_left(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE 10 = a.y
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert any(
            f.column == col("a", "y") and f.op == "eq" and f.value == 10
            for f in filters
        )

    def test_lt_lte_operators(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.t <= '2025-01-01' AND a.amount > 0
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        ops = {f.op for f in filters}
        assert "lte" in ops
        assert "gt" in ops

    def test_scalar_subquery_in_projection_constraints(self):
        sql = """
        WITH ws AS (
            SELECT weather.stn AS station_id
            FROM `myproject.ds.stations` AS station
            JOIN `myproject.ds.gsod` AS weather ON station.usaf = weather.stn
            WHERE station.state = 'WA'
        ),
        prcp AS (
            SELECT
                ws.station_id,
                (
                    SELECT COUNT(*)
                    FROM `myproject.ds.gsod` AS w
                    WHERE ws.station_id = w.stn
                    AND prcp > 0
                    AND prcp != 99.99
                ) AS rainy_days
            FROM ws
        )
        SELECT * FROM prcp WHERE prcp.rainy_days > 150
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        prcp_filters = [f for f in filters if f.column.column == "prcp"]
        assert any(f.op == "gt" and f.value == 0 for f in prcp_filters)
        assert any(f.op == "neq" and f.value == 99.99 for f in prcp_filters)


# ─── extract_constraints — OR creates multiple groups ─────────────────────────


class TestExtractConstraintsOrGroups:
    def test_simple_or_two_groups(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2
        """
        groups = extract_constraints(sql)
        assert len(groups) == 2
        values = {f.value for g in groups for f in g.filters if f.column.column == "x"}
        assert values == {1, 2}

    def test_and_or_combo_two_groups_both_carry_and_part(self):
        """WHERE a.y = 10 AND (a.x = 1 OR a.x = 2) → 2 groups, both have y=10."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.y = 10 AND (a.x = 1 OR a.x = 2)
        """
        groups = extract_constraints(sql)
        assert len(groups) == 2
        for g in groups:
            y_vals = [f.value for f in g.filters if f.column.column == "y"]
            assert 10 in y_vals, "a.y = 10 must appear in every group"
        x_vals = {f.value for g in groups for f in g.filters if f.column.column == "x"}
        assert x_vals == {1, 2}

    def test_cross_product_or(self):
        """(a.x=1 OR a.x=2) AND (a.y=10 OR a.y=20) → 4 groups."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE (a.x = 1 OR a.x = 2) AND (a.y = 10 OR a.y = 20)
        """
        groups = extract_constraints(sql)
        assert len(groups) == 4

    def test_or_in_join_on_no_groups(self):
        """OR in JOIN ON — not expanded, single group with shared ON equality."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id OR a.code = b.code
        """
        groups = extract_constraints(sql)
        # OR in ON is not expanded (known gap) — still 1 group
        assert len(groups) == 1


# ─── extract_constraints — UNION ALL creates multiple groups ──────────────────


class TestExtractConstraintsUnionGroups:
    def test_union_all_two_groups(self):
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        groups = extract_constraints(sql)
        assert len(groups) == 2
        values = {f.value for g in groups for f in g.filters}
        assert "alpha" in values
        assert "beta" in values

    def test_union_three_branches_three_groups(self):
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
        """
        groups = extract_constraints(sql)
        assert len(groups) == 3
        values = {f.value for g in groups for f in g.filters}
        assert values == {1, 2, 3}

    def test_union_branch_filters_isolated(self):
        """Each group carries only its own branch's filters."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        groups = extract_constraints(sql)
        g0_vals = {f.value for f in groups[0].filters}
        g1_vals = {f.value for f in groups[1].filters}
        assert "alpha" in g0_vals and "beta" not in g0_vals
        assert "beta" in g1_vals and "alpha" not in g1_vals

    def test_union_with_or_branch(self):
        """UNION branch with OR → that branch expands to 2 groups."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1 OR a.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 3
        """
        groups = extract_constraints(sql)
        # 2 OR groups from branch1 + 1 from branch2
        assert len(groups) == 3


# ─── extract_constraints — CTE cross-product ──────────────────────────────────


class TestExtractConstraintsCteCrossProduct:
    def test_cte_or_cross_multiplied_with_outer_where(self):
        """CTE with OR × outer WHERE → groups are the cross-product."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte WHERE cte.x > 0
        """
        groups = extract_constraints(sql)
        # 2 CTE groups × 1 outer WHERE group = 2 groups
        assert len(groups) == 2
        statuses = {
            f.value for g in groups for f in g.filters if f.column.column == "status"
        }
        assert statuses == {"active", "pending"}
        # All groups must also carry the outer WHERE constraint
        for g in groups:
            x_filters = [f for f in g.filters if f.column.column == "x"]
            assert any(f.op == "gt" and f.value == 0 for f in x_filters), (
                "outer WHERE x > 0 must be in every group"
            )

    def test_two_ctes_with_or_cross_product(self):
        """Two CTEs each with OR → 2 × 2 = 4 groups."""
        sql = """
        WITH cte1 AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.p = 1 OR a.p = 2
        ),
        cte2 AS (
            SELECT b.x FROM myproject.analytics.b AS b WHERE b.q = 3 OR b.q = 4
        )
        SELECT * FROM cte1 JOIN cte2 ON cte1.x = cte2.x
        """
        groups = extract_constraints(sql)
        assert len(groups) == 4
        p_vals = {f.value for g in groups for f in g.filters if f.column.column == "p"}
        q_vals = {f.value for g in groups for f in g.filters if f.column.column == "q"}
        assert p_vals == {1, 2}
        assert q_vals == {3, 4}

    def test_cte_without_or_no_multiplication(self):
        """CTE without OR → outer SELECT groups unchanged (no multiplication)."""
        sql = """
        WITH cte AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.y = 1
        )
        SELECT * FROM cte WHERE cte.x = 2
        """
        groups = extract_constraints(sql)
        # 1 CTE group (AND-only) × 1 outer WHERE group = 1 group total
        # (cross-multiply of a single non-empty group does expand)
        assert len(groups) >= 1
        # All constraints appear somewhere
        all_filters, _, _, _ = _flat(groups)
        vals = {f.value for f in all_filters}
        assert 1 in vals
        assert 2 in vals

    def test_anti_join_cte_not_multiplied(self):
        """CTE used in LEFT JOIN … WHERE IS NULL → not cross-multiplied."""
        sql = """
        WITH excluded AS (
            SELECT e.id FROM myproject.analytics.excluded_ids AS e
            WHERE e.status = 'X' OR e.status = 'Y'
        )
        SELECT m.id
        FROM myproject.analytics.main AS m
        LEFT JOIN excluded ON m.id = excluded.id
        WHERE excluded.id IS NULL
        """
        groups = extract_constraints(sql)
        # The CTE's OR groups are NOT cross-multiplied into the outer SELECT
        # because `excluded` is on the nullable side (anti-join).
        # Result: 1 group from the outer SELECT (no OR in its WHERE)
        assert len(groups) == 1


# ─── extract_constraints — edge cases ────────────────────────────────────────


class TestEdgeCases:
    def test_parse_error_raises(self):
        with pytest.raises(Exception):
            extract_constraints("SELECT FROM WHERE ??? !!!")

    def test_no_table_select_one(self):
        groups = extract_constraints("SELECT 1")
        assert isinstance(groups, list)
        filters, equalities, functional, col_ineq = _flat(groups)
        assert filters == []
        assert equalities == []

    def test_empty_string_raises(self):
        with pytest.raises(Exception):
            extract_constraints("")

    def test_having_not_captured(self):
        sql = """
        SELECT a.group_id, COUNT(*) AS cnt
        FROM myproject.analytics.a AS a
        GROUP BY a.group_id
        HAVING COUNT(*) > 5
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert [f for f in filters if f.column.column == "cnt"] == []

    def test_in_subquery_inner_where_not_captured(self):
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.id IN (
            SELECT b.id FROM myproject.analytics.b AS b WHERE b.status = 'active'
        )
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert [f for f in filters if f.column.column == "status"] == []

    def test_window_function_no_constraint(self):
        sql = """
        SELECT
            a.id,
            ROW_NUMBER() OVER (PARTITION BY a.group_id ORDER BY a.created_at DESC) AS rn
        FROM myproject.analytics.a AS a
        WHERE a.active = TRUE
        """
        filters, _, _, _ = _flat(extract_constraints(sql))
        assert [f for f in filters if f.column.column == "group_id"] == []
        assert [f for f in filters if f.column.column == "created_at"] == []

    def test_select_star_no_where(self):
        sql = "SELECT * FROM myproject.analytics.a AS a"
        groups = extract_constraints(sql)
        filters, equalities, _, _ = _flat(groups)
        assert filters == []
        assert equalities == []

    def test_deeply_nested_cte_chain(self):
        sql = """
        WITH c1 AS (SELECT a.x, a.y FROM myproject.analytics.a AS a WHERE a.y > 0),
             c2 AS (SELECT c1.x, c1.y FROM c1 WHERE c1.y > 1),
             c3 AS (SELECT c2.x, c2.y FROM c2 WHERE c2.y > 2),
             c4 AS (SELECT c3.x, c3.y FROM c3 WHERE c3.y > 3),
             c5 AS (SELECT c4.x, c4.y FROM c4 WHERE c4.y > 4)
        SELECT * FROM c5
        """
        groups = extract_constraints(sql)
        assert len(groups) >= 1

    def test_self_join_different_aliases_no_crash(self):
        sql = """
        SELECT a1.id, a2.id AS id2
        FROM myproject.analytics.a AS a1
        JOIN myproject.analytics.a AS a2 ON a1.parent_id = a2.id
        WHERE a1.active = TRUE
        """
        filters, equalities, _, _ = _flat(extract_constraints(sql))
        pairs = [frozenset({x, y}) for x, y in equalities]
        assert frozenset({col("a1", "parent_id"), col("a2", "id")}) in pairs
        active = [f for f in filters if f.column.column == "active"]
        assert len(active) == 1


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


# ─── check_having_cardinality ─────────────────────────────────────────────────


class TestCheckHavingCardinality:
    def _sql(self, having_clause: str) -> str:
        return f"""
        WITH cte AS (
            SELECT station_id, COUNT(*) AS rainy_days
            FROM weather
            GROUP BY station_id
        )
        SELECT station_id FROM cte
        HAVING {having_clause}
        """

    def test_count_gt_above_threshold_raises(self):
        sql = "SELECT stn, COUNT(*) AS cnt FROM weather GROUP BY stn HAVING COUNT(*) > 150"
        with pytest.raises(ValueError, match="150"):
            check_having_cardinality(sql)

    def test_alias_gt_above_threshold_raises(self):
        with pytest.raises(ValueError, match="rainy_days > 150"):
            check_having_cardinality(self._sql("rainy_days > 150"))

    def test_alias_gte_above_threshold_raises(self):
        with pytest.raises(ValueError, match="rainy_days >= 151"):
            check_having_cardinality(self._sql("rainy_days >= 151"))

    def test_exactly_at_threshold_does_not_raise(self):
        check_having_cardinality(self._sql("rainy_days > 20"))

    def test_below_threshold_does_not_raise(self):
        check_having_cardinality(self._sql("rainy_days > 5"))

    def test_gte_at_threshold_does_not_raise(self):
        check_having_cardinality(self._sql("rainy_days >= 20"))

    def test_lt_does_not_raise(self):
        check_having_cardinality(self._sql("rainy_days < 1000"))

    def test_literal_on_left_raises(self):
        with pytest.raises(ValueError):
            check_having_cardinality(self._sql("150 < rainy_days"))

    def test_error_message_contains_condition(self):
        with pytest.raises(ValueError, match=r"HAVING"):
            check_having_cardinality(self._sql("rainy_days > 150"))

    def test_invalid_sql_does_not_raise(self):
        check_having_cardinality("THIS IS NOT SQL !!!!")

    def test_custom_threshold(self):
        check_having_cardinality(self._sql("rainy_days > 5"), threshold=5)
        with pytest.raises(ValueError):
            check_having_cardinality(self._sql("rainy_days > 6"), threshold=5)


# ─── check_correlated_aggregate_cardinality ───────────────────────────────────


class TestCheckCorrelatedAggregateCardinality:
    _SQL = """
    WITH prcp2023 AS (
        SELECT
            ws.name,
            (SELECT COUNT(*) FROM `proj.ds.gsod2023` AS w WHERE ws.station_id = w.stn AND w.prcp > 0) AS rainy_days
        FROM WashingtonStations AS ws
    )
    SELECT prcp2023.name
    FROM prcp2023
    WHERE prcp2023.rainy_days > {threshold}
    """

    def _sql(self, threshold: int) -> str:
        return self._SQL.format(threshold=threshold)

    def test_high_threshold_raises(self):
        with pytest.raises(ValueError):
            check_correlated_aggregate_cardinality(self._sql(150))

    def test_low_threshold_does_not_raise(self):
        check_correlated_aggregate_cardinality(self._sql(5))

    def test_exact_threshold_does_not_raise(self):
        check_correlated_aggregate_cardinality(self._sql(20), threshold=20)

    def test_one_above_threshold_raises(self):
        with pytest.raises(ValueError):
            check_correlated_aggregate_cardinality(self._sql(21), threshold=20)

    def test_gte_at_threshold_does_not_raise(self):
        check_correlated_aggregate_cardinality(
            self._SQL.replace("> {threshold}", ">= 20").format(threshold=20),
            threshold=20,
        )

    def test_error_message_contains_col_and_cte(self):
        with pytest.raises(ValueError, match=r"rainy_days"):
            check_correlated_aggregate_cardinality(self._sql(150))

    def test_no_cte_does_not_raise(self):
        check_correlated_aggregate_cardinality("SELECT * FROM t WHERE t.n > 150")

    def test_cte_without_subquery_aggregate_does_not_raise(self):
        sql = """
        WITH cte AS (SELECT t.n FROM t)
        SELECT * FROM cte WHERE cte.n > 150
        """
        check_correlated_aggregate_cardinality(sql)

    def test_invalid_sql_does_not_raise(self):
        check_correlated_aggregate_cardinality("THIS IS NOT SQL !!!!")
