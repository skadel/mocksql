"""
test_constraint_paths.py — Tests for OR gaps, UNION branch tracking, and edge cases.

Sections:
  1. TestOrPredicatesGap         — document that OR predicates are silently dropped (known gap)
  2. TestUnionBranchesFlat       — document current flat-merge behavior for UNION
  3. TestEdgeCases               — empty/malformed SQL, no-table, HAVING, subquery, window
  4. TestUnionBranches           — union_branches feature (path grouping per branch)
  5. TestOrPaths                 — or_paths DNF feature in simplify()
  6. TestOrPathsLimit            — _MAX_OR_PATHS_EMIT truncation behavior
  7. TestCteOrPaths              — OR paths from CTE body WHEREs
"""

import pytest

from build_query.constraint_simplifier import (
    ColumnRef,
    SimplificationResult,
    _MAX_OR_PATHS_EMIT,
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


# ─── 1. OR predicates gap ─────────────────────────────────────────────────────


class TestOrPredicatesGap:
    """
    OR predicates are currently NOT captured by extract_constraints.
    _flatten_and stops at exp.Or nodes — the Or itself is passed to
    _dispatch_pred which has no handler for it and silently drops it.

    These tests document the known gap so it can be detected when OR support
    is eventually implemented (the assertions will need to be reversed then).
    """

    def test_simple_or_no_filter_captured(self):
        """WHERE a.x = 1 OR a.x = 2 — both branches are lost."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2
        """
        filters, equalities, _, _ = extract_constraints(sql)
        # Known gap: OR predicate is not dispatched → zero filters
        assert len(filters) == 0

    def test_and_or_combo_only_and_part_captured(self):
        """WHERE a.y = 10 AND (a.x = 1 OR a.x = 2) — only a.y = 10 is captured."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.y = 10 AND (a.x = 1 OR a.x = 2)
        """
        filters, _, _, _ = extract_constraints(sql)
        ops_y = [f.op for f in filters if f.column == col("a", "y")]
        ops_x = [f.op for f in filters if f.column == col("a", "x")]
        assert ops_y == ["eq"]  # AND part captured
        assert ops_x == []  # OR part dropped

    def test_or_in_join_on_dropped(self):
        """ON a.id = b.id OR a.code = b.code — OR drops the entire condition."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id OR a.code = b.code
        """
        _, equalities, _, _ = extract_constraints(sql)
        pairs = [frozenset({x, y}) for x, y in equalities]
        # Both equality pairs are lost because the OR stops flattening
        assert frozenset({col("a", "id"), col("b", "id")}) not in pairs
        assert frozenset({col("a", "code"), col("b", "code")}) not in pairs

    def test_cte_or_predicate_dropped(self):
        """OR inside a CTE WHERE is also dropped."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte
        """
        filters, _, _, _ = extract_constraints(sql)
        # Neither branch of the OR is captured
        status_filters = [f for f in filters if f.column.column == "status"]
        assert status_filters == []


# ─── 2. UNION branches — current flat behavior ────────────────────────────────


class TestUnionBranchesFlat:
    """
    With a UNION ALL, _walk_tree visits both branches and appends to the same
    flat lists.  Filters from both branches co-exist in extract_constraints output,
    indistinguishable from each other.  This is the current behavior.
    """

    def test_union_all_both_filters_present(self):
        """Both branch filters appear in the flat filter list."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        filters, _, _, _ = extract_constraints(sql)
        values = {f.value for f in filters}
        assert "alpha" in values
        assert "beta" in values

    def test_union_three_branches_all_filters_merged(self):
        """Three-way UNION ALL — all three branch filters appear flat."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
        """
        filters, _, _, _ = extract_constraints(sql)
        values = {f.value for f in filters}
        assert values == {1, 2, 3}

    def test_union_equalities_merged(self):
        """Equalities from both branches are merged flat."""
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
        filters, equalities, _, _ = extract_constraints(sql)
        pairs = [frozenset({x, y}) for x, y in equalities]
        assert frozenset({col("a", "id"), col("b", "id")}) in pairs
        assert frozenset({col("c", "id"), col("d", "id")}) in pairs
        flag_values = {f.value for f in filters if f.column.column == "flag"}
        assert "x" in flag_values
        assert "y" in flag_values


# ─── 3. Edge cases ────────────────────────────────────────────────────────────


class TestEdgeCases:
    def test_parse_error_raises(self):
        """Malformed SQL raises during sqlglot.parse_one."""
        with pytest.raises(Exception):
            extract_constraints("SELECT FROM WHERE ??? !!!")

    def test_no_table_select_one(self):
        """SELECT 1 (no tables) — returns empty results without crashing."""
        filters, equalities, functional, col_ineq = extract_constraints("SELECT 1")
        assert filters == []
        assert equalities == []
        assert functional == []
        assert col_ineq == []

    def test_empty_string_raises(self):
        """Empty string raises (sqlglot.parse_one returns None → AttributeError)."""
        with pytest.raises(Exception):
            extract_constraints("")

    def test_having_not_captured(self):
        """HAVING clause constraints are NOT captured (known gap)."""
        sql = """
        SELECT a.group_id, COUNT(*) AS cnt
        FROM myproject.analytics.a AS a
        GROUP BY a.group_id
        HAVING COUNT(*) > 5
        """
        filters, _, _, _ = extract_constraints(sql)
        # HAVING predicates are on aggregate expressions, not columns — not captured
        having_filters = [f for f in filters if f.column.column == "cnt"]
        assert having_filters == []

    def test_in_subquery_inner_where_not_captured(self):
        """WHERE a.id IN (subquery) — inner subquery constraints not extracted."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.id IN (
            SELECT b.id FROM myproject.analytics.b AS b WHERE b.status = 'active'
        )
        """
        filters, _, _, _ = extract_constraints(sql)
        # Inner WHERE b.status = 'active' is inside a subquery: not captured
        inner_filters = [f for f in filters if f.column.column == "status"]
        assert inner_filters == []

    def test_window_function_no_constraint(self):
        """PARTITION BY / ORDER BY in OVER() produce no constraints."""
        sql = """
        SELECT
            a.id,
            ROW_NUMBER() OVER (PARTITION BY a.group_id ORDER BY a.created_at DESC) AS rn
        FROM myproject.analytics.a AS a
        WHERE a.active = TRUE
        """
        filters, _, _, _ = extract_constraints(sql)
        # Only the WHERE filter on a.active is captured; PARTITION BY / ORDER BY are not
        partition_filters = [f for f in filters if f.column.column == "group_id"]
        assert partition_filters == []
        order_filters = [f for f in filters if f.column.column == "created_at"]
        assert order_filters == []

    def test_case_expression_no_constraint(self):
        """CASE WHEN branches produce no filter constraints (known gap)."""
        sql = """
        SELECT
            CASE WHEN a.score > 90 THEN 'A' WHEN a.score > 70 THEN 'B' ELSE 'C' END AS grade
        FROM myproject.analytics.a AS a
        """
        filters, _, _, _ = extract_constraints(sql)
        # CASE branches not captured
        assert filters == []

    def test_select_star_no_where(self):
        """SELECT * with no filter — empty constraint lists, no crash."""
        sql = "SELECT * FROM myproject.analytics.a AS a"
        filters, equalities, functional, col_ineq = extract_constraints(sql)
        assert filters == []
        assert equalities == []

    def test_deeply_nested_cte_chain(self):
        """5-level CTE chain resolves without crashing."""
        sql = """
        WITH c1 AS (SELECT a.x, a.y FROM myproject.analytics.a AS a WHERE a.y > 0),
             c2 AS (SELECT c1.x, c1.y FROM c1 WHERE c1.y > 1),
             c3 AS (SELECT c2.x, c2.y FROM c2 WHERE c2.y > 2),
             c4 AS (SELECT c3.x, c3.y FROM c3 WHERE c3.y > 3),
             c5 AS (SELECT c4.x, c4.y FROM c4 WHERE c4.y > 4)
        SELECT * FROM c5
        """
        filters, _, _, _ = extract_constraints(sql)
        gt_filters = [f for f in filters if f.op == "gt"]
        # At minimum, some gt filters are captured (y > 0 through y > 4)
        assert len(gt_filters) >= 1

    def test_self_join_different_aliases_no_crash(self):
        """Self-join with two aliases — doesn't crash, aliases treated as distinct tables."""
        sql = """
        SELECT a1.id, a2.id AS id2
        FROM myproject.analytics.a AS a1
        JOIN myproject.analytics.a AS a2 ON a1.parent_id = a2.id
        WHERE a1.active = TRUE
        """
        filters, equalities, _, _ = extract_constraints(sql)
        pairs = [frozenset({x, y}) for x, y in equalities]
        assert frozenset({col("a1", "parent_id"), col("a2", "id")}) in pairs
        active_filters = [f for f in filters if f.column.column == "active"]
        assert len(active_filters) == 1


# ─── 4. UNION branch tracking (union_branches feature) ────────────────────────


class TestUnionBranches:
    """
    simplify() populates result.union_branches when the query has UNION/UNION ALL.
    Each branch is a fully independent SimplificationResult isolating that branch's
    constraints — the key invariant the LLM relies on to generate non-interfering rows.
    """

    def test_no_union_empty_branches(self):
        """Simple SELECT without UNION → union_branches is empty."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        WHERE a.x = 1
        """
        r = simplify(sql)
        assert r.union_branches == []

    def test_two_branch_union_count(self):
        """2-branch UNION ALL → exactly 2 entries in union_branches."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        assert len(r.union_branches) == 2

    def test_union_branch_filters_isolated(self):
        """Branch 0 has only alpha; branch 1 has only beta."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        b0_values = {
            f.value
            for filters in r.union_branches[0].source_columns.values()
            for f in filters
        }
        b1_values = {
            f.value
            for filters in r.union_branches[1].source_columns.values()
            for f in filters
        }
        assert "alpha" in b0_values
        assert "beta" not in b0_values
        assert "beta" in b1_values
        assert "alpha" not in b1_values

    def test_three_branch_union_count(self):
        """Three-way UNION ALL → 3 branches."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
        """
        r = simplify(sql)
        assert len(r.union_branches) == 3

    def test_three_branch_each_has_own_filter(self):
        """Each branch carries only its own filter value."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.c AS c WHERE c.v = 3
        """
        r = simplify(sql)
        branch_values = []
        for branch in r.union_branches:
            vals = {
                f.value for filters in branch.source_columns.values() for f in filters
            }
            branch_values.append(vals)
        # Each branch should contain exactly one of {1, 2, 3} and not the others
        all_found = {1, 2, 3}
        found = set()
        for vals in branch_values:
            found |= vals
        assert found == all_found
        # No branch contains more than one of the three values
        for vals in branch_values:
            assert len(vals & all_found) == 1

    def test_cte_union_branches_preserve_cte_lineage(self):
        """WITH cte … SELECT … UNION ALL SELECT … — CTE lineage resolves per branch."""
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
        assert len(r.union_branches) == 2
        # Branch 1 must have b.v = 42 isolated
        b1_values = {
            f.value
            for filters in r.union_branches[1].source_columns.values()
            for f in filters
        }
        assert 42 in b1_values

    def test_union_branches_have_isolated_equalities(self):
        """Each branch's join equalities are independent."""
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
        assert len(r.union_branches) == 2

        def pairs_of(branch: SimplificationResult):
            return [
                frozenset({x, y}) for x, y in branch.filters if hasattr(x, "table")
            ]  # crude check

        b0_classes = r.union_branches[0].equivalence_classes
        b1_classes = r.union_branches[1].equivalence_classes

        def has_pair(classes, t1, c1, t2, c2):
            target = frozenset({col(t1, c1), col(t2, c2)})
            return any(target.issubset(cls) for cls in classes)

        assert has_pair(b0_classes, "a", "id", "b", "id")
        assert has_pair(b1_classes, "c", "id", "d", "id")
        # Cross-contamination must be absent
        assert not has_pair(b0_classes, "c", "id", "d", "id")
        assert not has_pair(b1_classes, "a", "id", "b", "id")


# ─── 5. OR path grouping (DNF) ────────────────────────────────────────────────


class TestOrPaths:
    """
    simplify() populates result.or_paths when the outermost WHERE has OR nodes.
    Each path is an independent list of FilterConstraints covering one satisfying
    branch — the LLM generates one row per path.

    Note: extract_constraints() (lower-level) still does NOT capture OR predicates
    in its flat filter list — that gap is documented in TestOrPredicatesGap.
    or_paths lives exclusively in SimplificationResult (simplify output).
    """

    def test_no_or_empty_paths(self):
        """AND-only WHERE → or_paths is empty (flat behavior covers it)."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 AND a.y = 2
        """
        r = simplify(sql)
        assert r.or_paths == []

    def test_simple_or_two_paths(self):
        """WHERE a.x = 1 OR a.x = 2 → 2 independent paths."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2
        """
        r = simplify(sql)
        assert len(r.or_paths) == 2
        values = {path[0].value for path in r.or_paths if path}
        assert values == {1, 2}

    def test_and_or_combo_distribution(self):
        """WHERE a.y = 10 AND (a.x = 1 OR a.x = 2) → 2 paths, both carry y=10."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.y = 10 AND (a.x = 1 OR a.x = 2)
        """
        r = simplify(sql)
        assert len(r.or_paths) == 2
        for path in r.or_paths:
            y_vals = [f.value for f in path if f.column.column == "y"]
            assert 10 in y_vals, "a.y = 10 must appear in every OR path"
        x_vals = {
            f.value for path in r.or_paths for f in path if f.column.column == "x"
        }
        assert x_vals == {1, 2}

    def test_three_way_or(self):
        """WHERE a.x = 1 OR a.x = 2 OR a.x = 3 → 3 paths."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE a.x = 1 OR a.x = 2 OR a.x = 3
        """
        r = simplify(sql)
        assert len(r.or_paths) == 3
        values = {f.value for path in r.or_paths for f in path}
        assert values == {1, 2, 3}

    def test_cross_product_or(self):
        """WHERE (a.x=1 OR a.x=2) AND (a.y=10 OR a.y=20) → 4 paths (2×2)."""
        sql = """
        SELECT * FROM myproject.analytics.a AS a
        WHERE (a.x = 1 OR a.x = 2) AND (a.y = 10 OR a.y = 20)
        """
        r = simplify(sql)
        assert len(r.or_paths) == 4
        # Each path must have exactly one x value and one y value
        for path in r.or_paths:
            x_vals = [f.value for f in path if f.column.column == "x"]
            y_vals = [f.value for f in path if f.column.column == "y"]
            assert len(x_vals) == 1
            assert len(y_vals) == 1
        all_x = {f.value for path in r.or_paths for f in path if f.column.column == "x"}
        all_y = {f.value for path in r.or_paths for f in path if f.column.column == "y"}
        assert all_x == {1, 2}
        assert all_y == {10, 20}

    def test_or_in_join_condition_no_or_paths(self):
        """OR in JOIN ON (not WHERE) → or_paths stays empty (ON OR not handled)."""
        sql = """
        SELECT *
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.id = b.id OR a.code = b.code
        """
        r = simplify(sql)
        # OR in ON clause is not expanded (known limitation, same as TestOrPredicatesGap)
        assert r.or_paths == []

    def test_union_branch_with_or_has_or_paths(self):
        """UNION branch with OR → that branch's or_paths is populated."""
        sql = """
        SELECT x FROM myproject.analytics.a AS a WHERE a.v = 1 OR a.v = 2
        UNION ALL
        SELECT x FROM myproject.analytics.b AS b WHERE b.v = 3
        """
        r = simplify(sql)
        assert len(r.union_branches) == 2
        branch0 = r.union_branches[0]
        branch1 = r.union_branches[1]
        assert len(branch0.or_paths) == 2, "branch with OR must have 2 or_paths"
        assert branch1.or_paths == [], "branch without OR must have empty or_paths"

    def test_or_no_paths_for_union_top_level(self):
        """UNION at top level → top-level or_paths stays empty (handled per branch)."""
        sql = """
        SELECT x FROM myproject.analytics.p AS p WHERE p.p1 = 'alpha'
        UNION ALL
        SELECT x FROM myproject.analytics.q AS q WHERE q.q1 = 'beta'
        """
        r = simplify(sql)
        assert r.or_paths == [], "UNION top-level result should not have or_paths"
        assert len(r.union_branches) == 2


# ─── 6. OR path limit (truncation) ───────────────────────────────────────────


class TestOrPathsLimit:
    """
    _to_dnf is capped at _MAX_OR_PATHS_COMPUTE (128) internally.
    _extract_or_paths emits at most _MAX_OR_PATHS_EMIT (32) paths.
    When the expansion exceeds _MAX_OR_PATHS_EMIT, or_paths_truncated is True
    and a logger.warning is emitted — the first _MAX_OR_PATHS_EMIT paths are kept.
    """

    # SQL with 2×2 = 4 paths — well below the emit limit
    _SQL_4_PATHS = """
    SELECT * FROM myproject.analytics.a AS a
    WHERE (a.x = 1 OR a.x = 2) AND (a.y = 10 OR a.y = 20)
    """

    # SQL with 6 independent OR clauses × 2 alternatives = 2^6 = 64 paths > 32
    _SQL_64_PATHS = "SELECT * FROM myproject.analytics.a AS a WHERE " + " AND ".join(
        f"(a.c{i} = {i * 10} OR a.c{i} = {i * 10 + 1})" for i in range(1, 7)
    )

    def test_below_emit_limit_no_truncation(self):
        """4 OR paths (< _MAX_OR_PATHS_EMIT) → or_paths_truncated is False."""
        r = simplify(self._SQL_4_PATHS)
        assert len(r.or_paths) == 4
        assert r.or_paths_truncated is False

    def test_above_emit_limit_truncates(self):
        """64 computed paths → capped at _MAX_OR_PATHS_EMIT, truncated is True."""
        r = simplify(self._SQL_64_PATHS)
        assert len(r.or_paths) == _MAX_OR_PATHS_EMIT
        assert r.or_paths_truncated is True

    def test_truncated_paths_are_valid_constraints(self):
        """Even when truncated, each emitted path contains FilterConstraints."""
        r = simplify(self._SQL_64_PATHS)
        for path in r.or_paths:
            assert len(path) > 0, "every emitted path must have at least one constraint"

    def test_truncated_hint_has_flag(self):
        """paths_truncated: true appears in the JSON hint when truncated."""
        import json

        from build_query.examples_generator import _simplification_to_hint

        r = simplify(self._SQL_64_PATHS)
        assert r.or_paths_truncated is True
        hint = json.loads(_simplification_to_hint(r))
        assert hint.get("paths_truncated") is True

    def test_non_truncated_hint_no_flag(self):
        """paths_truncated key is absent when not truncated."""
        import json

        from build_query.examples_generator import _simplification_to_hint

        r = simplify(self._SQL_4_PATHS)
        assert r.or_paths_truncated is False
        hint = json.loads(_simplification_to_hint(r))
        assert "paths_truncated" not in hint


# ─── 7. OR paths from CTE bodies ─────────────────────────────────────────────


class TestCteOrPaths:
    """
    simplify() now also extracts OR paths from CTE body WHERE clauses.
    These are added to result.or_paths alongside any paths from the outer SELECT WHERE.
    extract_constraints() still drops CTE OR predicates from its flat filter list
    (documented in TestOrPredicatesGap.test_cte_or_predicate_dropped — unchanged).
    """

    def test_cte_or_surfaced_in_or_paths(self):
        """CTE body OR → result.or_paths populated with 2 paths."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte
        """
        r = simplify(sql)
        assert len(r.or_paths) == 2
        values = {f.value for path in r.or_paths for f in path}
        assert values == {"active", "pending"}

    def test_outer_and_cte_or(self):
        """CTE OR + outer WHERE constraint → OR paths from CTE, outer in source_columns."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.z
            FROM myproject.analytics.a AS a
            WHERE a.x = 1 OR a.x = 2
        )
        SELECT * FROM cte WHERE cte.z > 0
        """
        r = simplify(sql)
        # CTE OR produces 2 paths
        assert len(r.or_paths) == 2
        x_vals = {
            f.value for path in r.or_paths for f in path if f.column.column == "x"
        }
        assert x_vals == {1, 2}
        # Outer WHERE z > 0 is captured in flat source_columns (not in OR paths)
        z_col = next((c for c in r.source_columns if c.column == "z"), None)
        assert z_col is not None, "z > 0 from outer WHERE must be in source_columns"

    def test_no_cte_or_empty_or_paths(self):
        """CTE without OR in its WHERE → or_paths stays empty."""
        sql = """
        WITH cte AS (
            SELECT a.x FROM myproject.analytics.a AS a WHERE a.y = 1
        )
        SELECT * FROM cte
        """
        r = simplify(sql)
        assert r.or_paths == []

    def test_multiple_ctes_or_paths_merged(self):
        """Two CTEs each with OR → or_paths accumulates paths from both."""
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
        # 2 paths from cte1 + 2 paths from cte2 = 4 total
        assert len(r.or_paths) == 4
        all_vals = {f.value for path in r.or_paths for f in path}
        assert {1, 2}.issubset(all_vals)  # cte1 paths
        assert {3, 4}.issubset(all_vals)  # cte2 paths

    def test_cte_or_not_captured_by_extract_constraints(self):
        """Confirms that extract_constraints() still drops CTE OR (gap unchanged)."""
        sql = """
        WITH cte AS (
            SELECT a.x, a.status
            FROM myproject.analytics.a AS a
            WHERE a.status = 'active' OR a.status = 'pending'
        )
        SELECT * FROM cte
        """
        filters, _, _, _ = extract_constraints(sql)
        status_filters = [f for f in filters if f.column.column == "status"]
        assert status_filters == [], (
            "extract_constraints still drops CTE OR (known gap)"
        )
