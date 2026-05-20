"""
test_constraint_paths.py — Tests for constraint_groups, UNION, OR, CTE cross-products.

Sections:
  1. TestConstraintGroups     — simplify().constraint_groups feature
  2. TestOrGroups             — OR paths via DNF in extract_constraints / simplify
  3. TestUnionGroups          — UNION ALL creates independent groups
  4. TestCteGroups            — CTE OR cross-multiplied with outer SELECT
  5. TestGroupsLimit          — _MAX_CONSTRAINT_GROUPS truncation
  6. TestEdgeCases            — empty/malformed SQL, no-table, subquery, window
"""

import pytest

from build_query.constraint_simplifier import (
    ColumnRef,
    SimplificationResult,
    _MAX_CONSTRAINT_GROUPS,
    extract_constraints,
    simplify,
)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def col(table: str, column: str) -> ColumnRef:
    return ColumnRef(table.lower(), column.lower())


def filter_ops(result: SimplificationResult, table: str, column: str) -> list[str]:
    c = col(table, column)
    return [f.op for f in result.source_columns.get(c, [])]


def is_source(result: SimplificationResult, table: str, column: str) -> bool:
    return col(table, column) in result.source_columns


def _flat_filters(groups):
    return [f for g in groups for f in g.filters]


def _flat_equalities(groups):
    return [e for g in groups for e in g.equalities]


# ─── 1. constraint_groups feature ─────────────────────────────────────────────


class TestConstraintGroups:
    """simplify() populates result.constraint_groups when multiple paths exist."""

    def test_no_or_no_union_empty_groups(self):
        """AND-only WHERE, no UNION → constraint_groups is empty (single path)."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 AND a.y = 2
        """
        r = simplify(sql)
        assert r.constraint_groups == []

    def test_or_where_populates_groups(self):
        """WHERE a.x = 1 OR a.x = 2 → flat single group, constraint_groups empty."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        values = {f.value for f in r.filters if f.column.column == "x"}
        assert values == {1, 2}

    def test_union_all_populates_groups(self):
        """UNION ALL → 2 constraint_groups."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        assert len(r.constraint_groups) == 2

    def test_groups_isolated_union(self):
        """Each group from UNION ALL contains only its own branch filters."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        g0_vals = {f.value for f in r.constraint_groups[0].filters}
        g1_vals = {f.value for f in r.constraint_groups[1].filters}
        assert "alpha" in g0_vals and "beta" not in g0_vals
        assert "beta" in g1_vals and "alpha" not in g1_vals

    def test_groups_isolated_or(self):
        """AND + OR → single flat group carrying all constraints."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.y = 10 AND (a.x = 1 OR a.x = 2)
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        y_vals = [f.value for f in r.filters if f.column.column == "y"]
        assert 10 in y_vals
        x_vals = {f.value for f in r.filters if f.column.column == "x"}
        assert x_vals == {1, 2}

    def test_three_branch_union_three_groups(self):
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
        """
        r = simplify(sql)
        assert len(r.constraint_groups) == 3
        all_found = {1, 2, 3}
        found = set()
        for g in r.constraint_groups:
            vals = {f.value for f in g.filters}
            found |= vals
        assert found == all_found
        for g in r.constraint_groups:
            assert len({f.value for f in g.filters} & all_found) == 1

    def test_union_branches_have_isolated_equalities(self):
        """Each UNION branch's join equalities are independent."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id
        WHERE a.flag = 'x'
        UNION ALL
        SELECT *
        FROM myproject.analytics.c AS c
        JOIN myproject.analytics.d AS d ON c.id = d.id
        WHERE c.flag = 'y'
        """
        r = simplify(sql)
        assert len(r.constraint_groups) == 2

        def has_pair(result, t1, c1, t2, c2):
            target = frozenset({col(t1, c1), col(t2, c2)})
            return any(target.issubset(cls) for cls in result.equivalence_classes)

        assert has_pair(r.constraint_groups[0], "a", "id", "b", "id")
        assert has_pair(r.constraint_groups[1], "c", "id", "d", "id")
        assert not has_pair(r.constraint_groups[0], "c", "id", "d", "id")
        assert not has_pair(r.constraint_groups[1], "a", "id", "b", "id")

    def test_flat_result_merges_all_groups(self):
        """result.filters contains the union of all groups' constraints."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        flat_values = {f.value for f in r.filters}
        assert "alpha" in flat_values
        assert "beta" in flat_values


# ─── 2. OR groups ─────────────────────────────────────────────────────────────


class TestOrGroups:
    def test_simple_or_two_groups(self):
        """OR → flat single group; both OR values appear in filters."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        values = {f.value for f in r.filters if f.column.column == "x"}
        assert values == {1, 2}

    def test_and_or_combo_distribution(self):
        """WHERE a.y = 10 AND (a.x = 1 OR a.x = 2) → flat group with all constraints."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.y = 10 AND (a.x = 1 OR a.x = 2)
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        y_vals = [f.value for f in r.filters if f.column.column == "y"]
        assert 10 in y_vals
        x_vals = {f.value for f in r.filters if f.column.column == "x"}
        assert x_vals == {1, 2}

    def test_three_way_or(self):
        """Three OR branches → flat group with all three values."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2 OR a.x = 3
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        values = {f.value for f in r.filters if f.column.column == "x"}
        assert values == {1, 2, 3}

    def test_cross_product_or(self):
        """Two sets of OR conditions → flat group with all values."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE (a.x = 1 OR a.x = 2) AND (a.y = 10 OR a.y = 20)
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        x_vals = {f.value for f in r.filters if f.column.column == "x"}
        y_vals = {f.value for f in r.filters if f.column.column == "y"}
        assert x_vals == {1, 2}
        assert y_vals == {10, 20}

    def test_or_in_join_on_no_expansion(self):
        """OR in JOIN ON — not expanded (known gap), single path."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id OR a.code = b.code
        """
        r = simplify(sql)
        assert r.constraint_groups == []

    def test_and_only_no_groups(self):
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 AND a.y = 2
        """
        r = simplify(sql)
        assert r.constraint_groups == []


# ─── 3. UNION groups ──────────────────────────────────────────────────────────


class TestUnionGroups:
    def test_two_branch_union_count(self):
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        assert len(r.constraint_groups) == 2

    def test_union_no_or_two_groups(self):
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        # No extra groups beyond the 2 UNION branches
        assert len(r.constraint_groups) == 2

    def test_union_branch_with_or_expands(self):
        """UNION branch with OR → OR collapses into 1 group per UNION branch."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1 OR a.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 3
        """
        r = simplify(sql)
        # OR collapses to 1 flat group per branch → 2 total UNION groups
        assert len(r.constraint_groups) == 2

    def test_cte_union_branches_preserve_lineage(self):
        sql = """
        WITH cte AS (
            SELECT a.x, a.z
            FROM myproject.analytics.a AS a
            WHERE a.z > 100
        )
        SELECT x FROM cte WHERE cte.z > 200
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 42
        """
        r = simplify(sql)
        assert len(r.constraint_groups) == 2
        b_vals = {f.value for f in r.constraint_groups[1].filters}
        assert 42 in b_vals


# ─── 4. CTE groups — cross-product ────────────────────────────────────────────


class TestCteGroups:
    def test_cte_or_cross_multiplied(self):
        """CTE with OR merged into a flat single group."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte WHERE cte.x > 0
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        status_vals = {f.value for f in r.filters if f.column.column == "status"}
        assert status_vals == {"active", "pending"}
        x_filters = [f for f in r.filters if f.column.column == "x"]
        assert any(f.op == "gt" and f.value == 0 for f in x_filters)

    def test_two_ctes_four_groups(self):
        """Two CTEs each with OR → all constraints merged into one flat group."""
        sql = """
        WITH cte1 AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.p = 1 OR a.p = 2
        ),
        cte2 AS (
            SELECT b.x FROM myproject.analytics.b AS b WHERE b.q = 3 OR b.q = 4
        )
        SELECT * FROM cte1 JOIN cte2 ON cte1.x = cte2.x
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        all_vals = {f.value for f in r.filters}
        assert {1, 2}.issubset(all_vals)
        assert {3, 4}.issubset(all_vals)

    def test_cte_without_or_no_extra_groups(self):
        sql = """
        WITH cte AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.y = 1
        )
        SELECT * FROM cte WHERE cte.x > 0
        """
        r = simplify(sql)
        # Single CTE group (no OR), cross-multiply gives 1 group → flat result
        # constraint_groups may be empty (single path) or have 1 entry
        all_filters = list(r.filters) + [
            f for g in r.constraint_groups for f in g.filters
        ]
        vals = {f.value for f in all_filters}
        assert 1 in vals

    def test_cte_no_or_outer_or_two_groups(self):
        """CTE AND-only × outer WHERE OR → flat group with all constraints."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.z
            FROM myproject.analytics.a AS a
            WHERE a.x = 1
        )
        SELECT * FROM cte WHERE cte.z = 10 OR cte.z = 20
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        z_vals = {f.value for f in r.filters if f.column.column == "z"}
        assert z_vals == {10, 20}

    def test_anti_join_cte_not_cross_multiplied(self):
        """CTE used as anti-join source (LEFT JOIN … WHERE IS NULL) → no multiplication."""
        sql = """
        WITH excl AS (
            SELECT e.id FROM myproject.analytics.excl AS e
            WHERE e.cat = 'A' OR e.cat = 'B'
        )
        SELECT m.id FROM myproject.analytics.main AS m
        LEFT JOIN excl ON m.id = excl.id
        WHERE excl.id IS NULL
        """
        r = simplify(sql)
        # anti-join: excl groups NOT cross-multiplied → 1 group
        assert len(r.constraint_groups) == 0  # single flat path

    def test_cte_or_paths_surfaced(self):
        """CTE OR → flat result with both status values in filters."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        values = {f.value for f in r.filters if f.column.column == "status"}
        assert values == {"active", "pending"}

    def test_multiple_ctes_groups_accumulated(self):
        """Two CTEs each with OR → flat group accumulates all constraints."""
        sql = """
        WITH cte1 AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.p = 1 OR a.p = 2
        ),
        cte2 AS (
            SELECT b.x FROM myproject.analytics.b AS b WHERE b.q = 3 OR b.q = 4
        )
        SELECT * FROM cte1 JOIN cte2 ON cte1.x = cte2.x
        """
        r = simplify(sql)
        assert r.constraint_groups == []
        all_vals = {f.value for f in r.filters}
        assert {1, 2}.issubset(all_vals)
        assert {3, 4}.issubset(all_vals)


# ─── 5. Groups limit (truncation) ─────────────────────────────────────────────


class TestGroupsLimit:
    # 33 UNION ALL branches > _MAX_CONSTRAINT_GROUPS = 32
    _SQL_MANY_UNION = " UNION ALL ".join(
        f"SELECT {i} AS v FROM myproject.analytics.t{i} AS a WHERE a.x = {i}"
        for i in range(1, 34)
    )
    _SQL_4_UNION = """
    SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
    UNION ALL SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
    UNION ALL SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
    UNION ALL SELECT x FROM myproject.analytics.d AS d WHERE d.v = 4
    """

    def test_below_limit_no_truncation(self):
        r = simplify(self._SQL_4_UNION)
        assert len(r.constraint_groups) == 4
        assert r.constraint_groups_truncated is False

    def test_above_limit_truncates(self):
        r = simplify(self._SQL_MANY_UNION)
        assert len(r.constraint_groups) == _MAX_CONSTRAINT_GROUPS
        assert r.constraint_groups_truncated is True

    def test_truncated_groups_are_valid(self):
        r = simplify(self._SQL_MANY_UNION)
        for g in r.constraint_groups:
            assert isinstance(g, SimplificationResult)
            assert g.source_columns or g.filters

    def test_truncated_flag_set(self):
        r = simplify(self._SQL_MANY_UNION)
        assert r.constraint_groups_truncated is True

    def test_non_truncated_flag_not_set(self):
        r = simplify(self._SQL_4_UNION)
        assert r.constraint_groups_truncated is False


# ─── 6. Edge cases ────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_parse_error_raises(self):
        with pytest.raises(Exception):
            extract_constraints("SELECT FROM WHERE ??? !!!")

    def test_no_table_select_one(self):
        groups = extract_constraints("SELECT 1")
        assert isinstance(groups, list)

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
        groups = extract_constraints(sql)
        filters = [f for g in groups for f in g.filters]
        assert [f for f in filters if f.column.column == "cnt"] == []

    def test_window_function_no_constraint(self):
        sql = """
        SELECT
            a.id,
            ROW_NUMBER() OVER (PARTITION BY a.group_id ORDER BY a.created_at DESC) AS rn
        FROM myproject.analytics.a AS a
        WHERE a.active = TRUE
        """
        groups = extract_constraints(sql)
        filters = [f for g in groups for f in g.filters]
        assert [f for f in filters if f.column.column == "group_id"] == []
        assert [f for f in filters if f.column.column == "created_at"] == []
