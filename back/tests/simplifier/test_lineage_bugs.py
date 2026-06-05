"""
Regression tests for lineage bugs in build_conditions_hint / _LineageResolver.

Bug 1 — Aggregate column in lineage (COUNT(*), SUM, etc.):
  When a CTE column is defined as an aggregate (COUNT(*) AS cnt) and that
  column appears in a WHERE/JOIN condition, _resolve_via_lineage walks the
  lineage tree and calls _build_column_sql with col_expr = Count(*). This
  produces a nonsensical lineage like "SELECT COUNT(*) FROM orders AS orders".

  Root cause: the for-loop in _resolve_via_lineage overwrites lineage_sql on
  EVERY non-Table node — including aggregate nodes that should be ignored.

Bug 2 — SELECT ? FROM (?) in lineage:
  When sqlglot can't resolve a source (fully-qualified backtick tables,
  unresolvable aliases), it creates a Placeholder node. The fallback branch
  in _build_column_sql then produces "SELECT ? FROM (?)" which is noise in
  the LLM hint.
"""

from build_query.constraint_simplifier import (
    ColumnRef,
    _LineageResolver,
    build_conditions_hint,
)
import sqlglot


# ─── Bug 1: aggregate column produces COUNT(*) in lineage ─────────────────────

# Minimal reproducer: a GROUP BY CTE where COUNT(*) AS cnt appears in a WHERE.
# _resolve_via_lineage is called for ColumnRef("agg", "cnt") and its lineage
# walk produces a node with col_expr=Count(*), giving "SELECT COUNT(*) FROM …".
_SQL_AGG_IN_WHERE = """
WITH agg AS (
    SELECT id, COUNT(*) AS cnt
    FROM `project.dataset.orders`
    GROUP BY 1
)
SELECT * FROM agg WHERE agg.cnt > 10
"""


class TestLineageNoAggregate:
    def test_aggregate_column_lineage_is_empty_not_count(self):
        """Lineage for an aggregate column must not render as SELECT COUNT(*)."""
        stmt = sqlglot.parse_one(_SQL_AGG_IN_WHERE, dialect="bigquery")
        resolver = _LineageResolver(stmt, None, "bigquery")
        resolved = resolver.resolve(ColumnRef("agg", "cnt"))
        assert "COUNT" not in resolved.lineage, (
            f"Aggregate leaked into lineage: {resolved.lineage!r}"
        )

    def test_build_conditions_hint_lineages_have_no_count(self):
        """build_conditions_hint must not emit a lineage string containing COUNT(*)."""
        result = build_conditions_hint(_SQL_AGG_IN_WHERE, "bigquery")
        lineages = result.get("lineages", [])
        bad = [lin for lin in lineages if "COUNT" in lin]
        assert not bad, f"Aggregate leaked into lineages: {bad}"

    def test_sum_aggregate_not_in_lineage(self):
        """Same invariant for SUM aggregates."""
        sql = """
        WITH agg AS (
            SELECT id, SUM(amount) AS total
            FROM `project.dataset.orders`
            GROUP BY 1
        )
        SELECT * FROM agg WHERE agg.total > 1000
        """
        result = build_conditions_hint(sql, "bigquery")
        lineages = result.get("lineages", [])
        bad = [lin for lin in lineages if "SUM(" in lin]
        assert not bad, f"Aggregate leaked into lineages: {bad}"


# ─── Bug 2: SELECT ? FROM (?) placeholder in lineage ──────────────────────────

# A CTE over a fully-qualified backtick table with a WHERE subquery. When
# sqlglot cannot resolve the source of a column (producing a Placeholder node),
# _build_column_sql falls into the non-Select fallback and returns
# "SELECT ? FROM (?)" because Placeholder.sql() → "?".
_SQL_PLACEHOLDER = """
WITH
  reseau AS (
    SELECT DISTINCT code_banque, reseau
    FROM `marketing.referentiels.banques`
    WHERE
      reseau IN ('BP', 'CE')
      AND partition_date = (
        SELECT MAX(partition_date)
        FROM `marketing.referentiels.banques`
        WHERE partition_date <= '2026-01-01'
      )
  )
SELECT *
FROM reseau
WHERE reseau.reseau = 'BP'
"""


class TestLineageNoPlaceholder:
    def test_build_conditions_hint_lineages_have_no_placeholder(self):
        """build_conditions_hint must not emit lineages with '?' placeholders."""
        result = build_conditions_hint(_SQL_PLACEHOLDER, "bigquery")
        lineages = result.get("lineages", [])
        bad = [lin for lin in lineages if "?" in lin]
        assert not bad, f"Placeholder leaked into lineages: {bad}"

    def test_lineage_string_never_contains_question_mark(self):
        """No lineage string anywhere should contain a bare '?' — that is a
        sqlglot Placeholder rendered to SQL, indicating an unresolved source."""
        for sql in [_SQL_AGG_IN_WHERE, _SQL_PLACEHOLDER]:
            result = build_conditions_hint(sql, "bigquery")
            for lin in result.get("lineages", []):
                assert "?" not in lin, (
                    f"Placeholder in lineage for SQL snippet: {lin!r}"
                )
