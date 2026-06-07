"""
Tests for _alias_unnamed_final_projections (utils/examples.py).

The optimizer's qualify_columns aliases unnamed final-SELECT expressions as
`_col_0`, `_col_1`… These opaque names surface in the result schema and in the
generated assertions, which the eval judge penalizes heavily for readability.
This pass replaces unnamed / `_col_N` outer projections with a readable alias
derived from the expression (e.g. `AVG(corr)` -> `avg_corr`), and keeps any
ORDER BY / GROUP BY references to the renamed alias in sync.

Only the OUTERMOST select is touched — inner CTE/subquery names are referenced
elsewhere and must stay stable.
"""

import sqlglot
from sqlglot import exp

from utils.examples import _alias_unnamed_final_projections


def _roundtrip(sql: str, dialect: str = "duckdb") -> str:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    _alias_unnamed_final_projections(tree)
    return tree.sql(dialect=dialect)


def _output_names(sql: str, dialect: str = "duckdb") -> list[str]:
    tree = sqlglot.parse_one(sql, dialect=dialect)
    _alias_unnamed_final_projections(tree)
    select = tree if isinstance(tree, exp.Select) else tree.find(exp.Select)
    return [e.alias_or_name for e in select.expressions]


class TestAliasFinalProjections:
    def test_unnamed_aggregate_gets_readable_alias(self):
        names = _output_names(
            "SELECT sample_type, AVG(corr) FROM pval GROUP BY sample_type"
        )
        assert names == ["sample_type", "avg_corr"]

    def test_col_n_alias_is_replaced(self):
        names = _output_names(
            "SELECT sample_type, AVG(corr) AS _col_1 FROM pval GROUP BY sample_type"
        )
        assert names == ["sample_type", "avg_corr"]

    def test_order_by_reference_kept_in_sync(self):
        out = _roundtrip(
            "SELECT x, MAX(y) AS _col_1 FROM t GROUP BY x ORDER BY _col_1 DESC"
        )
        assert "_col_1" not in out
        assert "max_y" in out
        # the ORDER BY must still reference the renamed projection
        tree = sqlglot.parse_one(out, dialect="duckdb")
        order = tree.args.get("order")
        assert "max_y" in order.sql(dialect="duckdb")

    def test_named_projections_untouched(self):
        out = _roundtrip("SELECT a AS foo, b FROM t")
        assert out == sqlglot.parse_one("SELECT a AS foo, b FROM t").sql(
            dialect="duckdb"
        )

    def test_bare_columns_untouched(self):
        # bare columns already have a meaningful name — do not alias them
        names = _output_names("SELECT a, b FROM t")
        assert names == ["a", "b"]

    def test_collision_is_deduplicated(self):
        names = _output_names("SELECT AVG(x), AVG(x) FROM t")
        assert names[0] == "avg_x"
        assert names[1] == "avg_x_2"

    def test_corr_two_args_uses_first_column(self):
        names = _output_names("SELECT CORR(a, b) FROM t")
        assert names == ["corr_a"]

    def test_union_root_not_crashed(self):
        # non-Select root: must not raise and must leave projections alone
        out = _roundtrip("SELECT a FROM t UNION ALL SELECT a FROM u")
        assert "a" in out

    def test_star_projection_untouched(self):
        names = _output_names("SELECT * FROM t")
        assert names == ["*"]
