"""
Tests for the static fan-out guard:
- detect_fanout_risk (constraint_simplifier): pure AST heuristic flagging a JOIN
  combined with a row-multiplication-sensitive aggregate (AVG/SUM/STDDEV/CORR…).
- _build_fanout_hint_block (prompt_tools): formats the warning injected into the
  generator prompt.

An unintended many-to-many join silently inflates these aggregates (cartesian
fan-out, e.g. bq143's spurious correlation) — the guard tells the generator to
keep the dimension-side join key unique.
"""

from build_query.constraint_simplifier import detect_fanout_risk
from build_query.prompt_tools import _build_fanout_hint_block


class TestDetectFanoutRisk:
    def test_join_with_corr_flags_risk(self):
        sql = "SELECT CORR(a.x, b.y) AS c FROM t1 AS a JOIN t2 AS b ON a.id = b.id"
        assert detect_fanout_risk(sql) == ["b"]

    def test_join_with_sum_flags_risk(self):
        sql = (
            "SELECT SUM(l.amount) FROM orders AS o "
            "JOIN line_items AS l ON o.id = l.order_id"
        )
        assert detect_fanout_risk(sql) == ["l"]

    def test_join_with_avg_and_stddev(self):
        sql = (
            "SELECT AVG(b.v), STDDEV(b.v) FROM t1 AS a "
            "JOIN t2 AS b ON a.k = b.k JOIN t3 AS c ON a.k = c.k"
        )
        assert detect_fanout_risk(sql) == ["b", "c"]

    def test_no_join_returns_empty(self):
        sql = "SELECT AVG(x) FROM t1"
        assert detect_fanout_risk(sql) == []

    def test_join_without_sensitive_aggregate_returns_empty(self):
        sql = "SELECT a.id, b.name FROM t1 AS a JOIN t2 AS b ON a.id = b.id"
        assert detect_fanout_risk(sql) == []

    def test_count_distinct_only_is_not_flagged(self):
        # COUNT(DISTINCT ...) is robust to row multiplication → no fan-out risk
        sql = "SELECT COUNT(DISTINCT b.id) FROM t1 AS a JOIN t2 AS b ON a.id = b.id"
        assert detect_fanout_risk(sql) == []

    def test_invalid_sql_returns_empty(self):
        assert detect_fanout_risk("THIS IS NOT SQL ((") == []

    def test_empty_sql_returns_empty(self):
        assert detect_fanout_risk("") == []


class TestBuildFanoutHintBlock:
    def test_block_mentions_joined_table(self):
        sql = "SELECT CORR(a.x, b.y) AS c FROM t1 AS a JOIN t2 AS b ON a.id = b.id"
        block = _build_fanout_hint_block(sql)
        assert block != ""
        assert "b" in block
        # the warning must steer toward unique dimension-side keys
        assert "uniqu" in block.lower()

    def test_no_risk_returns_empty_block(self):
        sql = "SELECT a.id FROM t1 AS a JOIN t2 AS b ON a.id = b.id"
        assert _build_fanout_hint_block(sql) == ""

    def test_empty_sql_returns_empty_block(self):
        assert _build_fanout_hint_block("") == ""
