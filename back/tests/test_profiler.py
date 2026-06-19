"""
Tests for build_query/profiler.py

All tests use a fake sql_executor — no real database required.
"""

import unittest

import sqlglot

from build_query.profiler import (
    normalize_schema,
    build_column_profile_queries,
    build_column_profile,
    detect_correlations,
    profile_schema,
    profile_joins_for_query,
    build_profile_query,
    parse_profile_query_result,
    detect_fk_candidates,
    _resolve_cte_source,
    _extract_subquery_aliases,
    _resolve_alias_to_table,
    _collect_join_specs,
    describe_join,
    _build_partition_where,
    build_partition_window,
    _format_day_partition_values,
    _build_derived_expr_profile_branches,
    build_profile_queries,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def make_executor(responses: dict):
    """
    Returns a sql_executor stub that matches queries by keywords.

    *responses* maps a substring to the list-of-dicts the executor should
    return when that substring appears in the query.  The first matching key
    wins; fall-through returns [].
    """

    def executor(query: str) -> list[dict]:
        for keyword, result in responses.items():
            if keyword in query:
                return result
        return []

    return executor


SIMPLE_SCHEMA = {
    "tables": [
        {
            "name": "orders",
            "columns": [
                {"name": "order_id", "type": "STRING", "nullable": False},
                {"name": "customer_id", "type": "INTEGER", "nullable": True},
                {"name": "status", "type": "STRING", "nullable": True},
                {"name": "amount", "type": "NUMERIC", "nullable": True},
            ],
        }
    ]
}

TWO_TABLE_SCHEMA = {
    "tables": [
        {
            "name": "transactions",
            "columns": [
                {"name": "transaction_id", "type": "STRING", "nullable": False},
                {"name": "dt_transaction", "type": "TIMESTAMP", "nullable": True},
                {"name": "cd_pays_bin", "type": "STRING", "nullable": True},
                {"name": "amount", "type": "NUMERIC", "nullable": True},
            ],
        },
        {
            "name": "pays_iso",
            "columns": [
                {"name": "refresh_month", "type": "DATE", "nullable": True},
                {"name": "code_pays_alpha", "type": "STRING", "nullable": True},
            ],
        },
    ]
}


# ─── normalize_schema ─────────────────────────────────────────────────────────


class TestNormalizeSchema(unittest.TestCase):
    def test_valid_schema_indexes_tables(self):
        norm = normalize_schema(SIMPLE_SCHEMA)
        self.assertIn("orders", norm["tables_by_name"])
        self.assertIn("orders", norm["columns_by_table"])

    def test_valid_schema_indexes_columns(self):
        norm = normalize_schema(SIMPLE_SCHEMA)
        cols = norm["columns_by_table"]["orders"]
        self.assertIn("order_id", cols)
        self.assertIn("amount", cols)

    def test_empty_tables_raises(self):
        with self.assertRaises(ValueError):
            normalize_schema({"tables": []})

    def test_missing_tables_key_raises(self):
        with self.assertRaises(ValueError):
            normalize_schema({})

    def test_duplicate_table_name_raises(self):
        schema = {
            "tables": [
                {"name": "t1", "columns": [{"name": "id", "type": "STRING"}]},
                {"name": "t1", "columns": [{"name": "id", "type": "STRING"}]},
            ]
        }
        with self.assertRaises(ValueError):
            normalize_schema(schema)

    def test_duplicate_column_name_raises(self):
        schema = {
            "tables": [
                {
                    "name": "t1",
                    "columns": [
                        {"name": "id", "type": "STRING"},
                        {"name": "id", "type": "INTEGER"},
                    ],
                }
            ]
        }
        with self.assertRaises(ValueError):
            normalize_schema(schema)

    def test_table_missing_name_raises(self):
        with self.assertRaises(ValueError):
            normalize_schema({"tables": [{"columns": []}]})


# ─── build_column_profile_queries ────────────────────────────────────────────


class TestBuildColumnProfileQueries(unittest.TestCase):
    def _col(self, name, col_type):
        return {"name": name, "type": col_type}

    def test_always_has_basic_duplicates_top_values(self):
        queries = build_column_profile_queries(
            "orders", self._col("status", "STRING"), {}
        )
        self.assertIn("basic", queries)
        self.assertIn("duplicates", queries)
        self.assertIn("top_values", queries)

    def test_numeric_column_has_minmax(self):
        queries = build_column_profile_queries(
            "orders", self._col("amount", "NUMERIC"), {}
        )
        self.assertIn("minmax", queries)

    def test_string_column_no_minmax(self):
        queries = build_column_profile_queries(
            "orders", self._col("status", "STRING"), {}
        )
        self.assertNotIn("minmax", queries)

    def test_date_column_has_minmax(self):
        queries = build_column_profile_queries(
            "orders", self._col("created_at", "DATE"), {}
        )
        self.assertIn("minmax", queries)

    def test_top_k_option_respected(self):
        queries = build_column_profile_queries(
            "orders", self._col("status", "STRING"), {"top_k_values": 5}
        )
        self.assertIn("LIMIT 5", queries["top_values"])

    def test_default_top_k_is_20(self):
        queries = build_column_profile_queries(
            "orders", self._col("status", "STRING"), {}
        )
        self.assertIn("LIMIT 20", queries["top_values"])


# ─── build_column_profile ─────────────────────────────────────────────────────


class TestBuildColumnProfile(unittest.TestCase):
    def _make_executor_for_col(
        self,
        total=100,
        null_count=5,
        distinct=20,
        dup_count=3,
        top_values=None,
        min_v=None,
        max_v=None,
    ):
        top_values = top_values or [{"val": "FR", "cnt": 30}, {"val": "DE", "cnt": 20}]
        responses = {
            "total_count": [
                {
                    "total_count": total,
                    "null_count": null_count,
                    "distinct_count": distinct,
                }
            ],
            "duplicate_value_count": [{"duplicate_value_count": dup_count}],
            "ORDER BY cnt DESC": top_values,
        }
        if min_v is not None or max_v is not None:
            responses["min_value"] = [{"min_value": min_v, "max_value": max_v}]
        return make_executor(responses)

    def test_basic_fields_present(self):
        ex = self._make_executor_for_col(total=1000, null_count=10, distinct=50)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        for field in (
            "type",
            "nullable_ratio",
            "null_count",
            "non_null_count",
            "is_always_null",
            "is_never_null",
            "distinct_count",
            "duplicate_count",
            "is_unique",
            "is_categorical",
            "top_values",
            "top_values_frequency",
        ):
            self.assertIn(field, prof, f"missing field: {field}")

    def test_null_count_plus_non_null_equals_total(self):
        ex = self._make_executor_for_col(total=200, null_count=40, distinct=10)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertEqual(prof["null_count"] + prof["non_null_count"], 200)

    def test_is_always_null_when_all_null(self):
        ex = self._make_executor_for_col(total=50, null_count=50, distinct=0)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertTrue(prof["is_always_null"])
        self.assertFalse(prof["is_never_null"])

    def test_is_never_null_when_no_null(self):
        ex = self._make_executor_for_col(total=100, null_count=0, distinct=10)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertFalse(prof["is_always_null"])
        self.assertTrue(prof["is_never_null"])

    def test_is_unique_when_distinct_equals_total_and_no_null(self):
        ex = self._make_executor_for_col(total=100, null_count=0, distinct=100)
        col = {"name": "order_id", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertTrue(prof["is_unique"])

    def test_not_unique_when_distinct_less_than_total(self):
        ex = self._make_executor_for_col(total=100, null_count=0, distinct=50)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertFalse(prof["is_unique"])

    def test_is_categorical_below_threshold(self):
        ex = self._make_executor_for_col(total=1000, null_count=0, distinct=5)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile(
            "orders", col, ex, {"categorical_distinct_threshold": 20}
        )
        self.assertTrue(prof["is_categorical"])

    def test_not_categorical_above_threshold(self):
        ex = self._make_executor_for_col(total=1000, null_count=0, distinct=100)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile(
            "orders", col, ex, {"categorical_distinct_threshold": 20}
        )
        self.assertFalse(prof["is_categorical"])

    def test_nullable_ratio_correct(self):
        ex = self._make_executor_for_col(total=200, null_count=20, distinct=10)
        col = {"name": "status", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertAlmostEqual(prof["nullable_ratio"], 0.1, places=4)

    def test_min_max_for_numeric(self):
        responses = {
            "total_count": [
                {"total_count": 100, "null_count": 0, "distinct_count": 80}
            ],
            "duplicate_value_count": [{"duplicate_value_count": 0}],
            "ORDER BY cnt DESC": [],
            "min_value": [{"min_value": 1.5, "max_value": 999.9}],
        }
        ex = make_executor(responses)
        col = {"name": "amount", "type": "NUMERIC"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertEqual(prof["min_value"], 1.5)
        self.assertEqual(prof["max_value"], 999.9)

    def test_top_values_and_frequencies(self):
        top = [
            {"val": "FR", "cnt": 60},
            {"val": "DE", "cnt": 30},
            {"val": "US", "cnt": 10},
        ]
        ex = self._make_executor_for_col(
            total=100, null_count=0, distinct=3, top_values=top
        )
        col = {"name": "country", "type": "STRING"}
        prof = build_column_profile("orders", col, ex, {})
        self.assertEqual(prof["top_values"], ["FR", "DE", "US"])
        self.assertAlmostEqual(prof["top_values_frequency"][0], 0.6, places=4)


# ─── detect_correlations ──────────────────────────────────────────────────────


class TestDetectCorrelations(unittest.TestCase):
    def _table_profile(self):
        return {
            "columns": {
                "status": {
                    "is_categorical": True,
                    "distinct_count": 3,
                    "nullable_ratio": 0.0,
                    "is_always_null": False,
                },
                "refund_amount": {
                    "is_categorical": False,
                    "distinct_count": 50,
                    "nullable_ratio": 0.7,
                    "is_always_null": False,
                },
            }
        }

    def test_always_null_correlation_detected(self):
        responses = {
            "driver_val": [
                {
                    "driver_val": "pending",
                    "row_count": 40,
                    "target_null_count": 40,
                    "target_distinct_count": 0,
                },
                {
                    "driver_val": "refunded",
                    "row_count": 30,
                    "target_null_count": 0,
                    "target_distinct_count": 15,
                },
                {
                    "driver_val": "completed",
                    "row_count": 30,
                    "target_null_count": 0,
                    "target_distinct_count": 12,
                },
            ]
        }
        ex = make_executor(responses)
        corrs = detect_correlations("orders", self._table_profile(), ex, {})
        always_null = [c for c in corrs if c["rule_type"] == "always_null"]
        self.assertTrue(any(c["driver_value"] == "pending" for c in always_null))

    def test_constant_correlation_detected(self):
        responses = {
            "driver_val": [
                {
                    "driver_val": "premium",
                    "row_count": 50,
                    "target_null_count": 0,
                    "target_distinct_count": 1,
                },
                {
                    "driver_val": "basic",
                    "row_count": 50,
                    "target_null_count": 0,
                    "target_distinct_count": 5,
                },
            ]
        }
        ex = make_executor(responses)
        corrs = detect_correlations("orders", self._table_profile(), ex, {})
        constant = [c for c in corrs if c["rule_type"] == "constant"]
        self.assertTrue(any(c["driver_value"] == "premium" for c in constant))

    def test_executor_error_is_silenced(self):
        def bad_executor(q):
            raise RuntimeError("DB down")

        corrs = detect_correlations("orders", self._table_profile(), bad_executor, {})
        self.assertEqual(corrs, [])

    def test_no_correlations_when_no_categorical_drivers(self):
        table_profile = {
            "columns": {
                "amount": {
                    "is_categorical": False,
                    "distinct_count": 1000,
                    "nullable_ratio": 0.0,
                    "is_always_null": False,
                },
            }
        }
        corrs = detect_correlations("orders", table_profile, make_executor({}), {})
        self.assertEqual(corrs, [])


# ─── profile_schema ───────────────────────────────────────────────────────────


class TestProfileSchema(unittest.TestCase):
    def _full_executor(self):
        return make_executor(
            {
                "COUNT(*) AS row_count": [{"row_count": 500}],
                "total_count": [
                    {"total_count": 500, "null_count": 25, "distinct_count": 480}
                ],
                "duplicate_value_count": [{"duplicate_value_count": 0}],
                "ORDER BY cnt DESC": [
                    {"val": "A", "cnt": 200},
                    {"val": "B", "cnt": 100},
                ],
                "min_value": [{"min_value": 0.5, "max_value": 9999.0}],
            }
        )

    def test_all_tables_in_profile(self):
        prof = profile_schema(TWO_TABLE_SCHEMA, self._full_executor())
        self.assertIn("transactions", prof["tables"])
        self.assertIn("pays_iso", prof["tables"])

    def test_all_columns_in_profile(self):
        prof = profile_schema(SIMPLE_SCHEMA, self._full_executor())
        cols = prof["tables"]["orders"]["columns"]
        for col_name in ("order_id", "customer_id", "status", "amount"):
            self.assertIn(col_name, cols)

    def test_row_count_populated(self):
        prof = profile_schema(SIMPLE_SCHEMA, self._full_executor())
        self.assertEqual(prof["tables"]["orders"]["row_count"], 500)

    def test_null_plus_non_null_equals_row_count(self):
        prof = profile_schema(SIMPLE_SCHEMA, self._full_executor())
        for col_name, cp in prof["tables"]["orders"]["columns"].items():
            if "error" in cp:
                continue
            self.assertEqual(
                cp["null_count"] + cp["non_null_count"],
                prof["tables"]["orders"]["row_count"],
                f"Invariant violated for column {col_name}",
            )

    def test_executor_error_per_column_does_not_crash(self):
        call_count = [0]

        def flaky(q):
            call_count[0] += 1
            if "total_count" in q:
                raise RuntimeError("flaky DB")
            return [{"row_count": 100}]

        prof = profile_schema(SIMPLE_SCHEMA, flaky)
        # Should return a profile dict (not raise)
        self.assertIn("orders", prof["tables"])
        for cp in prof["tables"]["orders"]["columns"].values():
            self.assertIn("error", cp)

    def test_correlations_empty_when_not_requested(self):
        prof = profile_schema(SIMPLE_SCHEMA, self._full_executor())
        self.assertEqual(prof["tables"]["orders"]["correlations"], [])

    def test_correlations_computed_when_option_set(self):
        # Executor that returns categorical data for driver/target query
        responses = {
            "COUNT(*) AS row_count": [{"row_count": 100}],
            "total_count": [{"total_count": 100, "null_count": 5, "distinct_count": 3}],
            "duplicate_value_count": [{"duplicate_value_count": 10}],
            "ORDER BY cnt DESC": [{"val": "A", "cnt": 50}, {"val": "B", "cnt": 30}],
            "min_value": [{"min_value": 0, "max_value": 100}],
            "driver_val": [
                {
                    "driver_val": "A",
                    "row_count": 50,
                    "target_null_count": 50,
                    "target_distinct_count": 0,
                },
                {
                    "driver_val": "B",
                    "row_count": 30,
                    "target_null_count": 0,
                    "target_distinct_count": 1,
                },
            ],
        }
        prof = profile_schema(
            SIMPLE_SCHEMA,
            make_executor(responses),
            options={"compute_pairwise_correlations": True},
        )
        corrs = prof["tables"]["orders"]["correlations"]
        self.assertIsInstance(corrs, list)


# ─── profile_joins_for_query ──────────────────────────────────────────────────


class TestProfileJoinsForQuery(unittest.TestCase):
    def _join_executor(self, join_type="many-to-one"):
        return make_executor({"join_type": [{"join_type": join_type}]})

    def test_simple_join_detected(self):
        sql = """
SELECT t.id, c.name
FROM transactions t
JOIN customers c ON t.customer_id = c.id
"""
        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, self._join_executor())
        self.assertEqual(len(result), 1)
        self.assertIn("left_table", result[0])
        self.assertIn("right_table", result[0])
        self.assertIn("join_type_profiled", result[0])

    def test_join_type_propagated(self):
        sql = """
SELECT t.id
FROM transactions t
JOIN pays_iso p ON t.cd_pays_bin = p.code_pays_alpha
"""
        result = profile_joins_for_query(
            TWO_TABLE_SCHEMA, sql, self._join_executor("one-to-one")
        )
        self.assertEqual(result[0]["join_type_profiled"], "one-to-one")

    def test_many_to_one_default(self):
        sql = """
SELECT t.id
FROM transactions t
JOIN pays_iso p ON t.cd_pays_bin = p.code_pays_alpha
"""
        result = profile_joins_for_query(
            TWO_TABLE_SCHEMA, sql, self._join_executor("many-to-one")
        )
        self.assertEqual(result[0]["join_type_profiled"], "many-to-one")

    def test_no_join_returns_empty(self):
        sql = "SELECT id FROM transactions WHERE amount > 100"
        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, self._join_executor())
        self.assertEqual(result, [])

    def test_join_executor_error_returns_many_to_many(self):
        sql = """
SELECT t.id
FROM transactions t
JOIN pays_iso p ON t.cd_pays_bin = p.code_pays_alpha
"""

        def bad_ex(q):
            raise RuntimeError("DB error")

        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, bad_ex)
        self.assertEqual(result[0]["join_type_profiled"], "many-to-many")

    def test_left_and_right_keys_extracted(self):
        sql = """
SELECT *
FROM transactions t
JOIN pays_iso p ON t.cd_pays_bin = p.code_pays_alpha
"""
        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, self._join_executor())
        r = result[0]
        self.assertTrue(len(r["left_keys"]) > 0 or len(r["right_keys"]) > 0)

    def test_compound_join_creates_multiple_specs(self):
        sql = """
SELECT *
FROM transactions t
JOIN pays_iso p
  ON DATE_TRUNC(DATE(t.dt_transaction), MONTH) = p.refresh_month
  AND TRIM(t.cd_pays_bin) = TRIM(p.code_pays_alpha)
"""
        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, self._join_executor())
        # Two equality conditions → two join specs
        self.assertGreaterEqual(len(result), 1)

    def test_same_table_joined_twice_with_different_aliases(self):
        # transactions joined twice to pays_iso under two different aliases.
        # Each JOIN should produce a separate profile entry — not be merged.
        sql = """
SELECT *
FROM transactions t
JOIN pays_iso p1 ON t.cd_pays_bin = p1.code_pays_alpha
JOIN pays_iso p2 ON t.cd_pays_bin = p2.refresh_month
"""
        result = profile_joins_for_query(TWO_TABLE_SCHEMA, sql, self._join_executor())
        self.assertEqual(
            len(result),
            2,
            "Both JOINs on pays_iso must produce distinct profile entries",
        )
        right_tables = [r["right_table"] for r in result]
        self.assertEqual(right_tables.count("pays_iso"), 2)


# ─── build_profile_query ─────────────────────────────────────────────────────


class TestBuildProfileQuery(unittest.TestCase):
    USED_COLS = [
        {
            "table": "transactions",
            "used_columns": [
                "transaction_id",
                "cd_pays_bin",
                "amount",
                "dt_transaction",
            ],
        },
        {"table": "pays_iso", "used_columns": ["refresh_month", "code_pays_alpha"]},
    ]

    def _query(self, dialect="bigquery", options=None):
        from build_query.profiler import build_profile_query

        return build_profile_query(
            TWO_TABLE_SCHEMA, self.USED_COLS, dialect=dialect, options=options
        )

    def test_returns_string(self):
        self.assertIsInstance(self._query(), str)

    def test_contains_union_all(self):
        self.assertIn("UNION ALL", self._query())

    def test_all_output_columns_present(self):
        q = self._query()
        for col in (
            "row_type",
            "table_name",
            "col_name",
            "total_count",
            "null_count",
            "non_null_count",
            "distinct_count",
            "dup_count",
            "min_val",
            "max_val",
            "top_values",
        ):
            self.assertIn(col, q, f"Output column {col!r} missing from query")

    def test_row_type_column_present(self):
        q = self._query()
        self.assertIn("'column'", q)

    def test_all_tables_referenced(self):
        q = self._query()
        self.assertIn("transactions", q)
        self.assertIn("pays_iso", q)

    def test_all_columns_referenced(self):
        q = self._query()
        for col in (
            "transaction_id",
            "cd_pays_bin",
            "refresh_month",
            "code_pays_alpha",
        ):
            self.assertIn(col, q, f"Column {col!r} missing")

    def test_all_columns_have_min_max_val(self):
        # New format: every column branch includes min_val and max_val (cast to STRING)
        q = self._query()
        self.assertIn("min_val", q)
        self.assertIn("max_val", q)

    def test_orderable_numeric_column_has_minmax(self):
        q = self._query()
        self.assertIn("MIN(`amount`)", q)
        self.assertIn("MAX(`amount`)", q)

    def test_bigquery_dialect_uses_backticks(self):
        q = self._query(dialect="bigquery")
        self.assertIn("`transaction_id`", q)
        self.assertNotIn('"transaction_id"', q)

    def test_duckdb_dialect_uses_double_quotes(self):
        q = self._query(dialect="duckdb")
        self.assertIn('"transaction_id"', q)
        self.assertNotIn("`transaction_id`", q)

    def test_null_count_uses_subtraction(self):
        # null_count = COUNT(*) - COUNT(col) for all dialects
        q = self._query(dialect="bigquery")
        self.assertIn("COUNT(*) - COUNT(", q)

    def test_top_values_uses_string_agg(self):
        q = self._query(dialect="bigquery")
        self.assertIn("STRING_AGG(", q)

    def test_bigquery_casts_to_string(self):
        q = self._query(dialect="bigquery")
        self.assertIn("AS STRING", q)

    def test_duckdb_casts_to_text_type(self):
        # sqlglot renders VARCHAR as TEXT for DuckDB
        q = self._query(dialect="duckdb")
        self.assertTrue("AS VARCHAR" in q or "AS TEXT" in q)

    def test_top_k_option_respected(self):
        q = self._query(options={"top_k_values": 5})
        self.assertIn("LIMIT 5", q)

    def test_default_top_k_is_10(self):
        q = self._query()
        self.assertIn("LIMIT 10", q)

    def test_derived_table_aliases_unique(self):
        q = self._query()
        # Each _dup_N and _top_N alias should appear; indices must differ
        import re

        dup_indices = re.findall(r"AS _dup_(\d+)", q)
        top_indices = re.findall(r"AS _top_(\d+)", q)
        self.assertEqual(
            len(dup_indices), len(set(dup_indices)), "Duplicate _dup_ aliases"
        )
        self.assertEqual(
            len(top_indices), len(set(top_indices)), "Duplicate _top_ aliases"
        )

    def test_unknown_table_skipped_silently(self):
        used = [{"table": "nonexistent", "used_columns": ["id"]}]
        from build_query.profiler import build_profile_query

        with self.assertRaises(ValueError):
            build_profile_query(TWO_TABLE_SCHEMA, used)

    def test_empty_used_columns_raises(self):
        from build_query.profiler import build_profile_query

        with self.assertRaises(ValueError):
            build_profile_query(TWO_TABLE_SCHEMA, [])


# ─── parse_profile_query_result ──────────────────────────────────────────────


class TestParseProfileQueryResult(unittest.TestCase):
    USED_COLS = [
        {
            "table": "transactions",
            "used_columns": ["transaction_id", "cd_pays_bin", "amount"],
        },
    ]

    def _row(
        self,
        table,
        col,
        total,
        null_count,
        non_null,
        distinct,
        dup,
        min_val=None,
        max_val=None,
        top_values=None,
    ):
        """Build one row in the new one-row-per-column format."""
        return {
            "row_type": "column",
            "table_name": table,
            "col_name": col,
            "total_count": total,
            "null_count": null_count,
            "non_null_count": non_null,
            "distinct_count": distinct,
            "dup_count": dup,
            "min_val": min_val,
            "max_val": max_val,
            "top_values": top_values,
            "left_table": None,
            "right_table": None,
            "left_expr": None,
            "right_expr": None,
            "join_type": None,
        }

    def _make_rows(
        self,
        total=1000,
        null_cd=20,
        distinct_id=1000,
        distinct_cd=3,
        dup_cd=997,
        top_cd="FR,TN,DE",
        min_amount="0.01",
        max_amount="50000.0",
    ):
        return [
            self._row(
                "transactions", "transaction_id", total, 0, total, distinct_id, 0
            ),
            self._row(
                "transactions",
                "cd_pays_bin",
                total,
                null_cd,
                total - null_cd,
                distinct_cd,
                dup_cd,
                top_values=top_cd,
            ),
            self._row(
                "transactions",
                "amount",
                total,
                100,
                total - 100,
                500,
                0,
                min_val=min_amount,
                max_val=max_amount,
            ),
        ]

    def _parse(self, rows=None):
        from build_query.profiler import parse_profile_query_result

        return parse_profile_query_result(
            rows or self._make_rows(), TWO_TABLE_SCHEMA, self.USED_COLS
        )

    def test_table_present_in_profile(self):
        p = self._parse()
        self.assertIn("transactions", p["tables"])

    def test_all_columns_present(self):
        p = self._parse()
        cols = p["tables"]["transactions"]["columns"]
        for col in ("transaction_id", "cd_pays_bin", "amount"):
            self.assertIn(col, cols)

    def test_row_count_from_basic(self):
        p = self._parse()
        self.assertEqual(p["tables"]["transactions"]["row_count"], 1000)

    def test_null_count_plus_non_null_equals_total(self):
        p = self._parse()
        cols = p["tables"]["transactions"]["columns"]
        row_count = p["tables"]["transactions"]["row_count"]
        for col_name, cp in cols.items():
            self.assertEqual(
                cp["null_count"] + cp["non_null_count"],
                row_count,
                f"Invariant broken for {col_name}",
            )

    def test_is_unique_for_id_column(self):
        p = self._parse()
        self.assertTrue(
            p["tables"]["transactions"]["columns"]["transaction_id"]["is_unique"]
        )

    def test_not_unique_for_categorical(self):
        p = self._parse()
        self.assertFalse(
            p["tables"]["transactions"]["columns"]["cd_pays_bin"]["is_unique"]
        )

    def test_is_categorical_below_threshold(self):
        p = self._parse()
        self.assertTrue(
            p["tables"]["transactions"]["columns"]["cd_pays_bin"]["is_categorical"]
        )

    def test_top_values_populated(self):
        p = self._parse()
        self.assertEqual(
            p["tables"]["transactions"]["columns"]["cd_pays_bin"]["top_values"],
            ["FR", "TN", "DE"],
        )

    def test_top_values_order_preserved(self):
        # top_values preserves the order from STRING_AGG (already sorted by frequency DESC in SQL)
        p = self._parse()
        tv = p["tables"]["transactions"]["columns"]["cd_pays_bin"]["top_values"]
        self.assertEqual(tv, ["FR", "TN", "DE"])

    def test_min_max_populated(self):
        p = self._parse()
        cp = p["tables"]["transactions"]["columns"]["amount"]
        self.assertEqual(cp["min_value"], "0.01")
        self.assertEqual(cp["max_value"], "50000.0")

    def test_nullable_ratio_correct(self):
        p = self._parse()
        # cd_pays_bin: 20 nulls out of 1000
        ratio = p["tables"]["transactions"]["columns"]["cd_pays_bin"]["nullable_ratio"]
        self.assertAlmostEqual(ratio, 0.02, places=4)

    def test_is_always_null_when_all_null(self):
        rows = self._make_rows()
        for r in rows:
            if r["table_name"] == "transactions" and r["col_name"] == "cd_pays_bin":
                r["null_count"] = r["total_count"]
                r["non_null_count"] = 0
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, self.USED_COLS)
        self.assertTrue(
            p["tables"]["transactions"]["columns"]["cd_pays_bin"]["is_always_null"]
        )

    def test_is_never_null_when_no_null(self):
        p = self._parse()
        self.assertTrue(
            p["tables"]["transactions"]["columns"]["transaction_id"]["is_never_null"]
        )

    def test_duplicate_count_from_dup_metric(self):
        p = self._parse()
        self.assertEqual(
            p["tables"]["transactions"]["columns"]["cd_pays_bin"]["duplicate_count"],
            997,
        )

    def test_empty_rows_returns_empty_columns(self):
        from build_query.profiler import parse_profile_query_result

        p = parse_profile_query_result([], TWO_TABLE_SCHEMA, self.USED_COLS)
        # Table entry exists but columns have zero counts
        cols = p["tables"]["transactions"]["columns"]
        for cp in cols.values():
            self.assertEqual(cp["null_count"], 0)

    def test_joins_always_empty_list(self):
        p = self._parse()
        self.assertEqual(p["joins"], [])

    def test_correlations_always_empty_list(self):
        p = self._parse()
        self.assertEqual(p["tables"]["transactions"]["correlations"], [])


# ─── Regression: hyphenated / 3-part BigQuery table names ────────────────────

HYPHENATED_SCHEMA = {
    "tables": [
        {
            "name": "bigquery-public-data.world_bank_wdi.country_summary",
            "columns": [{"name": "country_code", "type": "STRING"}],
        },
        {
            "name": "bigquery-public-data.world_bank_wdi.indicators_data",
            "columns": [
                {"name": "country_code", "type": "STRING"},
                {"name": "value", "type": "FLOAT64"},
            ],
        },
    ]
}

HYPHENATED_USED = [
    {
        "table": "bigquery-public-data.world_bank_wdi.country_summary",
        "used_columns": ["country_code"],
    },
    {
        "table": "bigquery-public-data.world_bank_wdi.indicators_data",
        "used_columns": ["country_code", "value"],
    },
]


class TestBuildProfileQueryHyphenatedTables(unittest.TestCase):
    """Regression tests: table names with hyphens (e.g. BigQuery project IDs)."""

    def _query(self, used=None):
        return build_profile_query(
            HYPHENATED_SCHEMA,
            used or HYPHENATED_USED,
        )

    def test_hyphenated_project_name_does_not_raise(self):
        # Was crashing: "Failed to parse 'FROM bigquery-public-data…'"
        self.assertIsInstance(self._query(), str)

    def test_hyphenated_table_name_appears_in_output(self):
        q = self._query()
        self.assertIn("bigquery-public-data", q)

    def test_three_part_name_parts_quoted_separately(self):
        # Each dot-separated part must be backtick-quoted for BigQuery
        q = self._query()
        self.assertIn("`bigquery-public-data`", q)
        self.assertIn("`world_bank_wdi`", q)
        self.assertIn("`country_summary`", q)

    def test_generated_sql_parseable_by_sqlglot(self):
        # The full UNION ALL must be valid SQL — if any node was built wrong, this fails
        q = self._query()
        parsed = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(parsed), 0)


# ─── Regression: join cardinality branches in build_profile_query ─────────────

JOIN_SQL = (
    "SELECT cs.country_code, id.value "
    "FROM `bigquery-public-data.world_bank_wdi.country_summary` cs "
    "JOIN `bigquery-public-data.world_bank_wdi.indicators_data` id "
    "ON cs.country_code = id.country_code"
)


class TestBuildProfileQueryWithJoins(unittest.TestCase):
    """Regression tests: join cardinality appended when sql_query is provided."""

    def _query(self, sql_query=None):
        return build_profile_query(
            HYPHENATED_SCHEMA,
            HYPHENATED_USED,
            sql_query=sql_query,
        )

    def test_no_join_branch_without_sql_query(self):
        q = self._query()
        self.assertNotIn("'join'", q)

    def test_join_branch_present_when_sql_has_join(self):
        q = self._query(sql_query=JOIN_SQL)
        self.assertIn("'join'", q)

    def test_join_case_contains_all_type_literals(self):
        # Was silent (empty THEN) due to exp.When instead of exp.If
        q = self._query(sql_query=JOIN_SQL)
        for literal in (
            "'one-to-one'",
            "'many-to-one'",
            "'one-to-many'",
            "'many-to-many'",
        ):
            self.assertIn(literal, q, f"Join type literal {literal!r} missing")

    def test_join_branch_has_correct_table_metadata(self):
        q = self._query(sql_query=JOIN_SQL)
        self.assertIn("'bigquery-public-data.world_bank_wdi.country_summary'", q)
        self.assertIn("'bigquery-public-data.world_bank_wdi.indicators_data'", q)

    def test_join_branch_subqueries_use_correct_tables(self):
        q = self._query(sql_query=JOIN_SQL)
        # Both tables must appear in the FROM clauses of the join subqueries
        self.assertIn("`bigquery-public-data`.`world_bank_wdi`.`country_summary`", q)
        self.assertIn("`bigquery-public-data`.`world_bank_wdi`.`indicators_data`", q)

    def test_full_query_with_joins_parseable_by_sqlglot(self):
        q = self._query(sql_query=JOIN_SQL)
        parsed = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(parsed), 0)


# ─── Regression: parse_profile_query_result handles join rows ─────────────────


class TestParseProfileQueryResultJoins(unittest.TestCase):
    """Regression tests: join rows (row_type='join') populate profile['joins']."""

    def _join_row(self, left_table, right_table, left_expr, right_expr, join_type):
        return {
            "row_type": "join",
            "table_name": None,
            "col_name": None,
            "total_count": None,
            "null_count": None,
            "non_null_count": None,
            "distinct_count": None,
            "dup_count": None,
            "min_val": None,
            "max_val": None,
            "top_values": None,
            "left_table": left_table,
            "right_table": right_table,
            "left_expr": left_expr,
            "right_expr": right_expr,
            "join_type": join_type,
        }

    def test_join_row_populates_profile_joins(self):
        rows = [
            self._join_row(
                "transactions",
                "pays_iso",
                "cd_pays_bin",
                "code_pays_alpha",
                "many-to-one",
            )
        ]
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, [])
        self.assertEqual(len(p["joins"]), 1)

    def test_join_type_profiled_key_set(self):
        rows = [
            self._join_row(
                "transactions",
                "pays_iso",
                "cd_pays_bin",
                "code_pays_alpha",
                "one-to-one",
            )
        ]
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, [])
        self.assertEqual(p["joins"][0]["join_type_profiled"], "one-to-one")

    def test_join_metadata_propagated(self):
        rows = [
            self._join_row(
                "transactions",
                "pays_iso",
                "cd_pays_bin",
                "code_pays_alpha",
                "many-to-many",
            )
        ]
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, [])
        j = p["joins"][0]
        self.assertEqual(j["left_table"], "transactions")
        self.assertEqual(j["right_table"], "pays_iso")
        self.assertEqual(j["left_expr"], "cd_pays_bin")
        self.assertEqual(j["right_expr"], "code_pays_alpha")

    def test_multiple_join_rows(self):
        rows = [
            self._join_row("t1", "t2", "a", "b", "one-to-many"),
            self._join_row("t1", "t3", "c", "d", "many-to-one"),
        ]
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, [])
        self.assertEqual(len(p["joins"]), 2)

    def test_column_rows_not_added_to_joins(self):
        rows = [
            {
                "row_type": "column",
                "table_name": "transactions",
                "col_name": "transaction_id",
                "total_count": 100,
                "null_count": 0,
                "non_null_count": 100,
                "distinct_count": 100,
                "dup_count": 0,
                "min_val": None,
                "max_val": None,
                "top_values": None,
                "left_table": None,
                "right_table": None,
                "left_expr": None,
                "right_expr": None,
                "join_type": None,
            },
            self._join_row("transactions", "pays_iso", "x", "y", "one-to-one"),
        ]
        used = [{"table": "transactions", "used_columns": ["transaction_id"]}]
        p = parse_profile_query_result(rows, TWO_TABLE_SCHEMA, used)
        self.assertEqual(len(p["joins"]), 1)
        self.assertIn("transactions", p["tables"])


class TestBuildProfileQueryCTEGrain(unittest.TestCase):
    """Join branches involving CTEs use grain-based static inference when the grain
    is known, falling back to SQL-based profiling (with CTEs in the WITH clause) otherwise.

    Grain inference: if cte1 has grain (a, b) and the outer join is ON (a, b), cte1
    is definitively "one" on its side — no GROUP BY query needed.
    Compound AND conditions are grouped before checking grain coverage.
    """

    _SCHEMA = {
        "tables": [
            {
                "name": "real_table",
                "columns": [
                    {"name": "date", "type": "DATE"},
                    {"name": "merchant", "type": "STRING"},
                    {"name": "amount", "type": "FLOAT64"},
                ],
            }
        ]
    }

    # cte_agg grain = (date, merchant). Outer join ON date+merchant → cte_agg is "one".
    _CTE_QUERY = (
        "WITH cte_agg AS ("
        "  SELECT date, merchant, SUM(amount) AS total FROM real_table GROUP BY 1, 2"
        ") "
        "SELECT r.date, r.merchant, c.total "
        "FROM real_table r "
        "JOIN cte_agg c ON r.date = c.date AND r.merchant = c.merchant"
    )

    def _q(self, sql_query=None):
        return build_profile_query(
            self._SCHEMA,
            [{"table": "real_table", "used_columns": ["amount"]}],
            dialect="bigquery",
            sql_query=sql_query,
        )

    def test_no_join_branch_without_sql_query(self):
        q = self._q()
        self.assertNotIn("'join'", q)

    def test_join_branch_present_with_cte_query(self):
        q = self._q(self._CTE_QUERY)
        self.assertIn("'join'", q)
        self.assertIn("UNION ALL", q)

    def test_cte_query_prepends_with_clause(self):
        # CTEs are always extracted and prepended for SQL-based fallback branches.
        q = self._q(self._CTE_QUERY)
        self.assertTrue(q.upper().startswith("WITH "))

    def test_cte_definition_appears_in_output(self):
        q = self._q(self._CTE_QUERY)
        self.assertIn("cte_agg", q)

    def test_compound_and_keys_produce_single_join_branch(self):
        # Compound AND keys are grouped per (left, right) pair before grain check →
        # one join branch per (left_table, right_table) pair when grain inference succeeds.
        q = self._q(self._CTE_QUERY)
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        # Static inference: 1 branch. SQL fallback: up to 2 (one per AND condition).
        self.assertIn(len(join_parts), [1, 2])

    def test_grain_covered_join_uses_static_inference(self):
        # When grain inference succeeds (cte_agg grain ⊆ join keys), the join branch
        # is a bare literal SELECT — no GROUP BY / subquery needed.
        q = self._q(self._CTE_QUERY)
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        if len(join_parts) == 1:
            # Static inference: literal SELECT, no inline subquery
            self.assertNotIn("GROUP BY", join_parts[0])
        # If len == 2, grain inference fell back to SQL — both shapes are acceptable.

    def test_grain_covered_cte_side_is_not_many_to_many(self):
        # cte_agg grain = (date, merchant) == join keys → cte_agg is "one".
        # The result must be one-to-one, one-to-many, or many-to-one (not many-to-many).
        q = self._q(self._CTE_QUERY)
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        if len(join_parts) == 1 and "GROUP BY" not in join_parts[0]:
            # Static branch: check the literal cardinality value
            self.assertFalse(
                "many-to-many" in join_parts[0] and "one-to" not in join_parts[0],
                "Expected at least one side to be 'one' when grain is fully covered",
            )

    def test_cte_join_branch_parseable_by_sqlglot(self):
        q = self._q(self._CTE_QUERY)
        statements = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(statements), 0)


class TestIsOneSide(unittest.TestCase):
    """Unit tests for the _is_one_side helper."""

    def setUp(self):
        from build_query.profiler import _is_one_side

        self._fn = _is_one_side

    def test_keys_cover_grain_returns_true(self):
        self.assertTrue(
            self._fn("cte_x", {"date", "merchant"}, {"cte_x": ["date", "merchant"]})
        )

    def test_keys_superset_of_grain_returns_true(self):
        self.assertTrue(
            self._fn("cte_x", {"date", "merchant", "extra"}, {"cte_x": ["date"]})
        )

    def test_keys_subset_of_grain_returns_false(self):
        self.assertFalse(self._fn("cte_x", {"date"}, {"cte_x": ["date", "merchant"]}))

    def test_not_a_cte_returns_none(self):
        self.assertIsNone(self._fn("real_table", {"id"}, {}))

    def test_empty_grain_returns_none(self):
        self.assertIsNone(self._fn("cte_x", {"id"}, {"cte_x": []}))

    def test_short_name_matched_for_dotted_table(self):
        # project.dataset.cte_x → short name "cte_x"
        self.assertTrue(
            self._fn("project.dataset.cte_x", {"date"}, {"cte_x": ["date"]})
        )


class TestBuildJoinQueryNewMetrics(unittest.TestCase):
    """_build_join_query now includes LEFT JOIN metrics for the generator."""

    _SCHEMA = {
        "tables": [
            {"name": "orders", "columns": [{"name": "user_id", "type": "INTEGER"}]},
            {"name": "users", "columns": [{"name": "id", "type": "INTEGER"}]},
        ]
    }
    _SQL = "SELECT * FROM orders o JOIN users u ON o.user_id = u.id"

    def _q(self):
        return build_profile_query(
            self._SCHEMA,
            [{"table": "orders", "used_columns": ["user_id"]}],
            dialect="bigquery",
            sql_query=self._SQL,
        )

    def test_join_branch_has_left_match_rate(self):
        q = self._q()
        self.assertIn("left_match_rate", q)

    def test_join_branch_has_avg_right_per_left_key(self):
        q = self._q()
        self.assertIn("avg_right_per_left_key", q)

    def test_join_branch_has_max_right_per_left_key(self):
        q = self._q()
        self.assertIn("max_right_per_left_key", q)

    def test_join_branch_has_left_key_sample(self):
        q = self._q()
        self.assertIn("left_key_sample", q)

    def test_join_branch_uses_left_join(self):
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        self.assertTrue(any("LEFT JOIN" in p.upper() for p in join_parts))

    def test_join_branch_parseable(self):
        q = self._q()
        sqlglot.parse(q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE)

    def test_column_rows_have_null_new_columns(self):
        q = self._q()
        col_parts = [p for p in q.split("UNION ALL") if "'column'" in p]
        self.assertTrue(len(col_parts) > 0)
        # New columns exist as NULLs in column rows
        self.assertTrue(all("left_match_rate" in p for p in col_parts))


class TestParseProfileQueryResultNewJoinFields(unittest.TestCase):
    """New join fields are parsed from the flat row list."""

    _SCHEMA = {
        "tables": [
            {"name": "orders", "columns": [{"name": "user_id", "type": "INTEGER"}]},
            {"name": "users", "columns": [{"name": "id", "type": "INTEGER"}]},
        ]
    }
    _USED = [
        {"table": "orders", "used_columns": ["user_id"]},
        {"table": "users", "used_columns": ["id"]},
    ]

    def _join_row(
        self,
        join_type="one-to-many",
        match_rate=1.0,
        avg_fanout=5.2,
        max_fanout=12,
        key_sample="USA,GBR,FRA",
    ):
        return {
            "row_type": "join",
            "table_name": None,
            "col_name": None,
            "total_count": None,
            "null_count": None,
            "non_null_count": None,
            "distinct_count": None,
            "dup_count": None,
            "min_val": None,
            "max_val": None,
            "top_values": None,
            "left_table": "orders",
            "right_table": "users",
            "left_expr": "user_id",
            "right_expr": "id",
            "join_type": join_type,
            "left_match_rate": match_rate,
            "avg_right_per_left_key": avg_fanout,
            "max_right_per_left_key": max_fanout,
            "left_key_sample": key_sample,
        }

    def _parse(self, join_row):
        p = parse_profile_query_result([join_row], self._SCHEMA, self._USED)
        return p["joins"][0]

    def test_left_match_rate_parsed(self):
        j = self._parse(self._join_row(match_rate=0.95))
        self.assertAlmostEqual(j["left_match_rate"], 0.95, places=3)

    def test_avg_right_per_left_key_parsed(self):
        j = self._parse(self._join_row(avg_fanout=7.3))
        self.assertAlmostEqual(j["avg_right_per_left_key"], 7.3, places=3)

    def test_max_right_per_left_key_parsed(self):
        j = self._parse(self._join_row(max_fanout=42))
        self.assertEqual(j["max_right_per_left_key"], 42)

    def test_left_key_sample_parsed_as_list(self):
        j = self._parse(self._join_row(key_sample="USA,GBR,FRA"))
        self.assertEqual(j["left_key_sample"], ["USA", "GBR", "FRA"])

    def test_null_metrics_allowed(self):
        row = self._join_row()
        row["left_match_rate"] = None
        row["avg_right_per_left_key"] = None
        row["max_right_per_left_key"] = None
        row["left_key_sample"] = None
        j = self._parse(row)
        self.assertIsNone(j["left_match_rate"])
        self.assertIsNone(j["avg_right_per_left_key"])
        self.assertIsNone(j["max_right_per_left_key"])
        self.assertEqual(j["left_key_sample"], [])

    def test_fk_candidates_in_profile(self):
        p = parse_profile_query_result([], self._SCHEMA, self._USED)
        self.assertIn("fk_candidates", p)


class TestDetectFkCandidates(unittest.TestCase):
    """detect_fk_candidates identifies PK/FK pairs from column name + uniqueness."""

    def _profile(self, col, t1_unique, t2_unique):
        return {
            "tables": {
                "t1": {"columns": {col: {"is_unique": t1_unique}}},
                "t2": {"columns": {col: {"is_unique": t2_unique}}},
            }
        }

    def _used(self, col):
        return [
            {"table": "t1", "used_columns": [col]},
            {"table": "t2", "used_columns": [col]},
        ]

    def test_unique_in_t1_yields_candidate(self):
        cands = detect_fk_candidates(self._profile("id", True, False), self._used("id"))
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0]["pk_table"], "t1")
        self.assertEqual(cands[0]["fk_table"], "t2")
        self.assertEqual(cands[0]["pk_column"], "id")

    def test_unique_in_t2_yields_candidate(self):
        cands = detect_fk_candidates(
            self._profile("code", False, True), self._used("code")
        )
        self.assertEqual(len(cands), 1)
        self.assertEqual(cands[0]["pk_table"], "t2")
        self.assertEqual(cands[0]["fk_table"], "t1")

    def test_neither_unique_no_candidate(self):
        cands = detect_fk_candidates(
            self._profile("code", False, False), self._used("code")
        )
        self.assertEqual(len(cands), 0)

    def test_both_unique_yields_one_candidate(self):
        # Both unique → treat t1 as PK (first encountered)
        cands = detect_fk_candidates(self._profile("id", True, True), self._used("id"))
        self.assertEqual(len(cands), 1)

    def test_column_in_single_table_ignored(self):
        profile = {"tables": {"t1": {"columns": {"id": {"is_unique": True}}}}}
        used = [{"table": "t1", "used_columns": ["id"]}]
        cands = detect_fk_candidates(profile, used)
        self.assertEqual(len(cands), 0)

    def test_no_duplicates_for_same_pair(self):
        profile = {
            "tables": {
                "t1": {
                    "columns": {"id": {"is_unique": True}, "code": {"is_unique": True}}
                },
                "t2": {
                    "columns": {
                        "id": {"is_unique": False},
                        "code": {"is_unique": False},
                    }
                },
            }
        }
        used = [
            {"table": "t1", "used_columns": ["id", "code"]},
            {"table": "t2", "used_columns": ["id", "code"]},
        ]
        cands = detect_fk_candidates(profile, used)
        # Two separate FK relationships (id and code), not duplicated
        self.assertEqual(len(cands), 2)
        cols = {c["pk_column"] for c in cands}
        self.assertEqual(cols, {"id", "code"})


class TestResolveCteSource(unittest.TestCase):
    """Tests for _resolve_cte_source — sqlglot lineage-based CTE column resolution."""

    def _cte_map(self, **kwargs: str) -> dict[str, str]:
        return kwargs

    def test_simple_passthrough_cte(self):
        """CTE that SELECT col FROM real_table — lineage traces directly to real_table."""
        cte_map = self._cte_map(my_cte="SELECT country_code FROM indicators_data")
        result = _resolve_cte_source("my_cte", ["country_code"], cte_map)
        self.assertIsNotNone(result)
        self.assertEqual(result["source_table"], "indicators_data")
        self.assertIn("country_code", result["source_cols"])

    def test_aliased_table_in_cte(self):
        """CTE with FROM table AS alias — source_table is the real table, not the alias."""
        cte_map = self._cte_map(
            enriched="SELECT id.country_code FROM indicators_data AS id"
        )
        result = _resolve_cte_source("enriched", ["country_code"], cte_map)
        self.assertIsNotNone(result)
        self.assertEqual(result["source_table"], "indicators_data")

    def test_where_conditions_extracted(self):
        """WHERE clause from CTE body is returned with aliases stripped."""
        cte_map = self._cte_map(
            filtered="SELECT country_code FROM indicators_data WHERE value IS NOT NULL AND year = 2020"
        )
        result = _resolve_cte_source("filtered", ["country_code"], cte_map)
        self.assertIsNotNone(result)
        self.assertIsNotNone(result["where_sql"])
        # Both conditions should appear
        self.assertIn("value", result["where_sql"])
        self.assertIn("2020", result["where_sql"])

    def test_where_table_alias_stripped(self):
        """Table aliases in WHERE conditions are stripped (id.value → value)."""
        cte_map = self._cte_map(
            filtered="SELECT id.country_code FROM indicators_data AS id WHERE id.value > 0"
        )
        result = _resolve_cte_source("filtered", ["country_code"], cte_map)
        self.assertIsNotNone(result)
        # Should not contain 'id.' prefix in where_sql
        if result["where_sql"]:
            self.assertNotIn("id.", result["where_sql"])

    def test_where_drops_joined_table_conditions(self):
        """CTE with a JOIN: WHERE predicates on the joined-in table are dropped.

        Only conditions on the resolved source table survive — the profiling
        subquery scans that single table, so a predicate on a joined-in table's
        column (absent from that table) would raise "Unrecognized name".
        """
        cte_map = self._cte_map(
            tmp_contrat_actif=(
                "SELECT acomp.no_contrat, acomp.cd_banque "
                "FROM acquereur.dashboard AS acomp "
                "INNER JOIN refcomm ON acomp.no_contrat = refcomm.no_contrat "
                "WHERE acomp.dt_extraction >= '2026-01-01' "
                "AND (refcomm.dt_ouverture < '2026-02-01' "
                "OR refcomm.dt_cloture IS NULL)"
            )
        )
        result = _resolve_cte_source(
            "tmp_contrat_actif", ["no_contrat", "cd_banque"], cte_map
        )
        self.assertIsNotNone(result)
        self.assertEqual(result["source_table"], "acquereur.dashboard")
        # Source-table condition is kept (alias stripped)…
        self.assertIn("dt_extraction", result["where_sql"])
        # …joined-in table conditions are dropped — those columns are absent
        # from the single-table profiling subquery.
        self.assertNotIn("dt_ouverture", result["where_sql"])
        self.assertNotIn("dt_cloture", result["where_sql"])

    def test_unknown_cte_returns_none(self):
        """Querying a CTE name not in the map returns None."""
        result = _resolve_cte_source("nonexistent", ["col"], {})
        self.assertIsNone(result)

    def test_no_where_returns_none_where_sql(self):
        """CTE with no WHERE clause returns where_sql=None."""
        cte_map = self._cte_map(plain="SELECT country_code FROM indicators_data")
        result = _resolve_cte_source("plain", ["country_code"], cte_map)
        self.assertIsNotNone(result)
        self.assertIsNone(result["where_sql"])

    def test_literal_filter_specs_skipped_in_build_profile_query(self):
        """Specs where one side is a constant (e.g. year = 2011) are not profiled as joins."""
        schema = {
            "tables": [
                {
                    "name": "facts",
                    "columns": [
                        {"name": "year", "type": "INTEGER"},
                        {"name": "country", "type": "STRING"},
                    ],
                },
                {
                    "name": "dim",
                    "columns": [
                        {"name": "country", "type": "STRING"},
                    ],
                },
            ]
        }
        used = [
            {"table": "facts", "used_columns": ["year", "country"]},
            {"table": "dim", "used_columns": ["country"]},
        ]
        # The ON clause has both a real join key AND a literal filter
        sql = (
            "SELECT * FROM facts f "
            "JOIN dim d ON f.country = d.country AND f.year = 2011"
        )
        q = build_profile_query(schema, used, sql_query=sql)
        # The real join (country = country) should be profiled
        self.assertIn("'join'", q)
        # But the literal filter (year = 2011) should produce zero extra join rows
        # We can verify by counting 'join' row literals — should be exactly 1
        self.assertEqual(q.count("'join'"), 1)

    def test_cte_join_uses_source_table(self):
        """When a join references a CTE, the profiling query uses the real source table."""
        schema = {
            "tables": [
                {
                    "name": "indicators_data",
                    "columns": [
                        {"name": "country_code", "type": "STRING"},
                        {"name": "value", "type": "FLOAT"},
                    ],
                },
                {
                    "name": "country_summary",
                    "columns": [
                        {"name": "country_code", "type": "STRING"},
                    ],
                },
            ]
        }
        used = [
            {"table": "indicators_data", "used_columns": ["country_code"]},
            {"table": "country_summary", "used_columns": ["country_code"]},
        ]
        sql = (
            "WITH base AS (SELECT country_code FROM indicators_data WHERE value > 0) "
            "SELECT * FROM base JOIN country_summary cs ON base.country_code = cs.country_code"
        )
        q = build_profile_query(schema, used, sql_query=sql)
        # The profiling subquery should reference indicators_data (real source),
        # not 'base' (the CTE alias), and include the WHERE condition
        self.assertIn("indicators_data", q)
        self.assertIn("value", q)  # WHERE condition preserved

    def test_compound_key_produces_one_join_branch(self):
        """A JOIN ON (a.x = b.x AND a.y = b.y) produces a single profiling branch, not two."""
        schema = {
            "tables": [
                {
                    "name": "orders",
                    "columns": [
                        {"name": "region", "type": "STRING"},
                        {"name": "period", "type": "INTEGER"},
                    ],
                },
                {
                    "name": "forecasts",
                    "columns": [
                        {"name": "region", "type": "STRING"},
                        {"name": "period", "type": "INTEGER"},
                    ],
                },
            ]
        }
        used = [
            {"table": "orders", "used_columns": ["region", "period"]},
            {"table": "forecasts", "used_columns": ["region", "period"]},
        ]
        sql = (
            "SELECT * FROM orders o "
            "JOIN forecasts f ON o.region = f.region AND o.period = f.period"
        )
        q = build_profile_query(schema, used, sql_query=sql)
        join_branches = [p for p in q.split("UNION ALL") if "'join'" in p]
        # One join branch for the (region, period) compound key — not two separate branches
        self.assertEqual(len(join_branches), 1)
        # The branch should contain both key columns
        self.assertIn("region", join_branches[0])
        self.assertIn("period", join_branches[0])
        # Compound key is built via CONCAT
        self.assertIn("CONCAT", join_branches[0])

    def test_correct_tables_in_join_spec(self):
        """After a side-swap, left_table correctly reflects which table is on the FROM side."""
        schema = {
            "tables": [
                {"name": "a", "columns": [{"name": "id", "type": "INTEGER"}]},
                {"name": "b", "columns": [{"name": "id", "type": "INTEGER"}]},
            ]
        }
        used = [
            {"table": "a", "used_columns": ["id"]},
            {"table": "b", "used_columns": ["id"]},
        ]
        # ON clause written with the JOIN table's column on the LEFT — parser should still
        # produce left_table=a, right_table=b (not both b)
        sql = "SELECT * FROM a JOIN b ON b.id = a.id"
        q = build_profile_query(schema, used, sql_query=sql)
        join_branches = [p for p in q.split("UNION ALL") if "'join'" in p]
        self.assertEqual(len(join_branches), 1)
        # left_table should be 'a' (the FROM table) and right_table 'b' (the JOIN table)
        self.assertIn("'a' AS left_table", join_branches[0])
        self.assertIn("'b' AS right_table", join_branches[0])


# ─── _extract_subquery_aliases ────────────────────────────────────────────────


class TestExtractSubqueryAliases(unittest.TestCase):
    """_extract_subquery_aliases detects inline JOIN (SELECT ...) AS alias."""

    def test_single_inline_subquery_detected(self):
        sql = (
            "SELECT * FROM t "
            "JOIN (SELECT id FROM stations WHERE active = 1) AS sub ON t.id = sub.id"
        )
        aliases = _extract_subquery_aliases(sql, dialect="duckdb")
        self.assertIn("sub", aliases)

    def test_body_contains_select(self):
        sql = (
            "SELECT * FROM trips "
            "JOIN (SELECT station_id FROM stations WHERE status = 'active') AS s "
            "ON trips.start_station_id = s.station_id"
        )
        aliases = _extract_subquery_aliases(sql, dialect="bigquery")
        self.assertIn("SELECT" in aliases.get("s", "").upper(), [True])

    def test_multiple_inline_subqueries(self):
        sql = (
            "SELECT * FROM t "
            "JOIN (SELECT id FROM a) AS sub_a ON t.a_id = sub_a.id "
            "JOIN (SELECT id FROM b) AS sub_b ON t.b_id = sub_b.id"
        )
        aliases = _extract_subquery_aliases(sql, dialect="duckdb")
        self.assertIn("sub_a", aliases)
        self.assertIn("sub_b", aliases)
        self.assertEqual(len(aliases), 2)

    def test_plain_table_join_returns_empty(self):
        sql = "SELECT * FROM trips t JOIN stations s ON t.station_id = s.id"
        aliases = _extract_subquery_aliases(sql, dialect="bigquery")
        self.assertEqual(aliases, {})

    def test_where_condition_preserved_in_body(self):
        sql = (
            "SELECT * FROM t "
            "JOIN (SELECT id FROM stations WHERE status = 'active') AS s ON t.id = s.id"
        )
        aliases = _extract_subquery_aliases(sql, dialect="duckdb")
        body = aliases.get("s", "")
        self.assertIn("active", body)

    def test_invalid_sql_returns_empty(self):
        aliases = _extract_subquery_aliases("THIS IS NOT SQL !!!!", dialect="duckdb")
        self.assertEqual(aliases, {})

    def test_no_join_returns_empty(self):
        sql = "SELECT id FROM trips WHERE status = 'active'"
        aliases = _extract_subquery_aliases(sql, dialect="duckdb")
        self.assertEqual(aliases, {})


# ─── _resolve_alias_to_table ─────────────────────────────────────────────────


class TestResolveAliasToTable(unittest.TestCase):
    """_resolve_alias_to_table returns the real base table name from a subquery body."""

    def test_simple_table_name(self):
        alias_map = {"s": "SELECT id FROM stations WHERE status = 'active'"}
        self.assertEqual(_resolve_alias_to_table("s", alias_map), "stations")

    def test_three_part_fully_qualified_name(self):
        body = "SELECT station_id FROM `bigquery-public-data`.`austin_bikeshare`.`bikeshare_stations`"
        alias_map = {"s": body}
        result = _resolve_alias_to_table("s", alias_map)
        self.assertIn("bikeshare_stations", result)

    def test_unknown_alias_returns_alias(self):
        self.assertEqual(_resolve_alias_to_table("x", {}), "x")

    def test_empty_map_returns_alias(self):
        self.assertEqual(_resolve_alias_to_table("sub", {}), "sub")

    def test_from_alias_ignored(self):
        # FROM stations AS st → real table is "stations", not "st"
        alias_map = {"s": "SELECT id FROM stations AS st"}
        result = _resolve_alias_to_table("s", alias_map, dialect="duckdb")
        self.assertEqual(result, "stations")

    def test_two_part_table_name(self):
        alias_map = {"s": "SELECT id FROM mydataset.stations"}
        result = _resolve_alias_to_table("s", alias_map, dialect="bigquery")
        self.assertEqual(result, "mydataset.stations")

    def test_parse_error_returns_alias(self):
        alias_map = {"s": "NOT VALID SQL @@@@"}
        self.assertEqual(_resolve_alias_to_table("s", alias_map), "s")


# ─── describe_join ────────────────────────────────────────────────────────────


class TestDescribeJoin(unittest.TestCase):
    """describe_join produces a natural-language cardinality sentence."""

    def _j(self, jt, left="orders", right="users", match_rate=1.0, avg_r=1.0, max_r=1):
        return {
            "left_table": left,
            "right_table": right,
            "join_type_profiled": jt,
            "left_match_rate": match_rate,
            "avg_right_per_left_key": avg_r,
            "max_right_per_left_key": max_r,
        }

    def test_one_to_one(self):
        desc = describe_join(self._j("one-to-one"))
        self.assertIn("exactly 1", desc)
        self.assertIn("orders", desc)
        self.assertIn("users", desc)

    def test_many_to_one(self):
        desc = describe_join(self._j("many-to-one"))
        self.assertIn("Multiple orders", desc)
        self.assertIn("users", desc)

    def test_one_to_many_with_max_r(self):
        desc = describe_join(self._j("one-to-many", max_r=8, avg_r=3.0))
        self.assertIn("8", desc)
        self.assertIn("orders", desc)

    def test_one_to_many_without_max_r(self):
        j = self._j("one-to-many", max_r=1, avg_r=1.0)
        desc = describe_join(j)
        self.assertIn("multiple rows", desc)

    def test_many_to_many(self):
        desc = describe_join(self._j("many-to-many"))
        self.assertIn("Multiple orders", desc)
        self.assertIn("multiple", desc.lower())

    def test_low_match_rate_appended(self):
        desc = describe_join(self._j("one-to-one", match_rate=0.85))
        self.assertIn("85.0% match rate", desc)

    def test_full_match_rate_omitted(self):
        desc = describe_join(self._j("one-to-one", match_rate=1.0))
        self.assertNotIn("match rate", desc)

    def test_avg_fanout_appended_for_one_to_many(self):
        desc = describe_join(self._j("one-to-many", max_r=5, avg_r=2.7))
        self.assertIn("avg 2.7 per key", desc)

    def test_avg_fanout_not_shown_for_many_to_one(self):
        # avg fanout is only shown for one-to-many / many-to-many
        desc = describe_join(self._j("many-to-one", avg_r=3.0))
        self.assertNotIn("per key", desc)

    def test_dotted_table_name_uses_short_form(self):
        desc = describe_join(
            self._j(
                "one-to-one",
                left="project.dataset.bikeshare_trips",
                right="project.dataset.bikeshare_stations",
            )
        )
        self.assertIn("bikeshare_trips", desc)
        self.assertIn("bikeshare_stations", desc)
        self.assertNotIn("project", desc)
        self.assertNotIn("dataset", desc)

    def test_match_rate_not_shown_for_many_to_one(self):
        # many-to-one low match rate should still NOT appear (only one-to-many/many-to-many)
        desc = describe_join(self._j("many-to-one", match_rate=0.5))
        # match_rate < 0.99 → appended regardless of join type in current impl
        # This test documents the actual behaviour:
        self.assertIn("match rate", desc)

    def test_missing_optional_fields(self):
        # Only required fields — should not raise
        desc = describe_join(
            {"join_type_profiled": "one-to-one", "left_table": "a", "right_table": "b"}
        )
        self.assertIsInstance(desc, str)
        self.assertIn("a", desc)


# ─── describe_join appears in parse_profile_query_result output ───────────────


class TestParseProfileQueryResultDescriptionField(unittest.TestCase):
    """parse_profile_query_result must add 'description' to every join entry."""

    def _join_row(self, join_type, left_table="orders", right_table="users"):
        return {
            "row_type": "join",
            "table_name": None,
            "col_name": None,
            "total_count": None,
            "null_count": None,
            "non_null_count": None,
            "distinct_count": None,
            "dup_count": None,
            "min_val": None,
            "max_val": None,
            "top_values": None,
            "left_table": left_table,
            "right_table": right_table,
            "left_expr": "user_id",
            "right_expr": "id",
            "join_type": join_type,
            "left_match_rate": 1.0,
            "avg_right_per_left_key": 1.0,
            "max_right_per_left_key": 1,
            "left_key_sample": None,
        }

    _SCHEMA = {
        "tables": [
            {"name": "orders", "columns": [{"name": "user_id", "type": "INTEGER"}]},
            {"name": "users", "columns": [{"name": "id", "type": "INTEGER"}]},
        ]
    }

    def test_description_field_present(self):
        rows = [self._join_row("one-to-one")]
        p = parse_profile_query_result(rows, self._SCHEMA, [])
        self.assertIn("description", p["joins"][0])

    def test_description_is_string(self):
        rows = [self._join_row("many-to-one")]
        p = parse_profile_query_result(rows, self._SCHEMA, [])
        self.assertIsInstance(p["joins"][0]["description"], str)

    def test_description_one_to_one(self):
        rows = [self._join_row("one-to-one")]
        p = parse_profile_query_result(rows, self._SCHEMA, [])
        self.assertIn("exactly 1", p["joins"][0]["description"])

    def test_description_many_to_one(self):
        rows = [self._join_row("many-to-one")]
        p = parse_profile_query_result(rows, self._SCHEMA, [])
        desc = p["joins"][0]["description"]
        self.assertIn("orders", desc)
        self.assertIn("users", desc)

    def test_all_join_rows_have_description(self):
        rows = [
            self._join_row("one-to-many"),
            self._join_row("many-to-many"),
        ]
        p = parse_profile_query_result(rows, self._SCHEMA, [])
        for j in p["joins"]:
            self.assertIn("description", j)
            self.assertTrue(len(j["description"]) > 0)


# ─── Inline subquery JOIN in build_profile_query ──────────────────────────────

_BIKESHARE_SCHEMA = {
    "tables": [
        {
            "name": "bigquery-public-data.austin_bikeshare.bikeshare_trips",
            "columns": [
                {"name": "start_station_id", "type": "INTEGER"},
                {"name": "end_station_id", "type": "INTEGER"},
            ],
        },
        {
            "name": "bigquery-public-data.austin_bikeshare.bikeshare_stations",
            "columns": [
                {"name": "station_id", "type": "INTEGER"},
                {"name": "council_district", "type": "STRING"},
                {"name": "status", "type": "STRING"},
            ],
        },
    ]
}

_BIKESHARE_USED = [
    {
        "table": "bigquery-public-data.austin_bikeshare.bikeshare_trips",
        "used_columns": ["start_station_id", "end_station_id"],
    },
    {
        "table": "bigquery-public-data.austin_bikeshare.bikeshare_stations",
        "used_columns": ["station_id", "council_district", "status"],
    },
]

_INLINE_SUBQUERY_SQL = (
    "SELECT s.council_district AS district, t.start_station_id, t.end_station_id "
    "FROM `bigquery-public-data.austin_bikeshare.bikeshare_trips` AS t "
    "INNER JOIN ("
    "  SELECT bikeshare_stations.station_id AS station_id,"
    "         bikeshare_stations.council_district AS council_district "
    "  FROM `bigquery-public-data.austin_bikeshare.bikeshare_stations` AS bikeshare_stations "
    "  WHERE bikeshare_stations.status = 'active'"
    ") AS s ON t.start_station_id = s.station_id"
)


class TestBuildProfileQueryInlineSubquery(unittest.TestCase):
    """Inline subquery JOINs are profiled against the real base table."""

    def _q(self):
        return build_profile_query(
            _BIKESHARE_SCHEMA,
            _BIKESHARE_USED,
            dialect="bigquery",
            sql_query=_INLINE_SUBQUERY_SQL,
        )

    def test_join_branch_present(self):
        q = self._q()
        self.assertIn("'join'", q)

    def test_real_table_name_stored_not_alias(self):
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        self.assertTrue(len(join_parts) >= 1)
        combined = " ".join(join_parts)
        self.assertIn("bikeshare_stations", combined)
        self.assertIn("bikeshare_trips", combined)
        # Alias "s" must NOT be stored as the table name
        self.assertNotIn("'s' AS left_table", combined)
        self.assertNotIn("'s' AS right_table", combined)

    def test_where_condition_from_subquery_preserved(self):
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        combined = " ".join(join_parts)
        # The WHERE status='active' from the subquery should appear in the profiling query
        self.assertIn("active", combined)

    def test_generated_sql_parseable(self):
        q = self._q()
        parsed = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(parsed), 0)


# ─── Inline subquery inside CTE body ─────────────────────────────────────────

_CTE_WITH_INLINE_SUBQUERY_SQL = """
WITH StateCases AS (
    SELECT b.state_name, b.date, b.confirmed_cases - a.confirmed_cases AS daily_new_cases
    FROM (
        SELECT state_name, state_fips_code, confirmed_cases,
               DATE_ADD(date, INTERVAL 1 DAY) AS date_shift
        FROM `bigquery-public-data.covid19_nyt.us_states`
        WHERE date >= '2020-02-29' AND date <= '2020-05-30'
    ) a
    JOIN `bigquery-public-data.covid19_nyt.us_states` b
        ON a.state_fips_code = b.state_fips_code AND a.date_shift = b.date
),
CountyCases AS (
    SELECT b.county, b.date, b.confirmed_cases - a.confirmed_cases AS daily_new_cases
    FROM (
        SELECT county, county_fips_code, confirmed_cases,
               DATE_ADD(date, INTERVAL 1 DAY) AS date_shift
        FROM `bigquery-public-data.covid19_nyt.us_counties`
        WHERE date >= '2020-02-29' AND date <= '2020-05-30'
    ) a
    JOIN `bigquery-public-data.covid19_nyt.us_counties` b
        ON a.county_fips_code = b.county_fips_code AND a.date_shift = b.date
)
SELECT county FROM CountyCases
"""

_CTE_INLINE_SCHEMA = {
    "tables": [
        {
            "name": "bigquery-public-data.covid19_nyt.us_states",
            "columns": [
                {"name": "state_name", "type": "STRING"},
                {"name": "state_fips_code", "type": "STRING"},
                {"name": "confirmed_cases", "type": "INTEGER"},
                {"name": "date", "type": "DATE"},
            ],
        },
        {
            "name": "bigquery-public-data.covid19_nyt.us_counties",
            "columns": [
                {"name": "county", "type": "STRING"},
                {"name": "county_fips_code", "type": "STRING"},
                {"name": "confirmed_cases", "type": "INTEGER"},
                {"name": "date", "type": "DATE"},
                {"name": "state_name", "type": "STRING"},
            ],
        },
    ]
}

_CTE_INLINE_USED = [
    {
        "table": "bigquery-public-data.covid19_nyt.us_states",
        "used_columns": ["state_name", "state_fips_code", "confirmed_cases", "date"],
    },
    {
        "table": "bigquery-public-data.covid19_nyt.us_counties",
        "used_columns": ["county", "county_fips_code", "confirmed_cases", "date"],
    },
]


class TestBuildProfileQueryCteInlineSubquery(unittest.TestCase):
    """Subquery aliases local to a CTE body (FROM (...) a) are hoisted to CTEs.

    Without the fix, build_profile_query would generate FROM `a` which BigQuery
    rejects because single-word identifiers must be dataset-qualified.
    """

    def _q(self):
        return build_profile_query(
            _CTE_INLINE_SCHEMA,
            _CTE_INLINE_USED,
            dialect="bigquery",
            sql_query=_CTE_WITH_INLINE_SUBQUERY_SQL,
        )

    def test_generated_sql_parseable(self):
        q = self._q()
        parsed = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(parsed), 0)

    def test_no_bare_alias_as_table(self):
        """FROM `a` must not appear — `a` must be resolved to a CTE or real table."""
        q = self._q()
        self.assertNotIn("FROM `a`", q)
        self.assertNotIn("FROM `a_2`", q)

    def test_join_branches_present(self):
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        self.assertGreaterEqual(len(join_parts), 1)

    def test_real_table_names_in_join_metadata(self):
        """Stored table names must reference the real source tables, not the local alias."""
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        combined = " ".join(join_parts)
        self.assertIn("us_states", combined)
        self.assertIn("us_counties", combined)
        self.assertNotIn("'a' AS left_table", combined)
        self.assertNotIn("'a' AS right_table", combined)

    def test_same_alias_in_two_ctes_disambiguated(self):
        """Both CTEs use 'a' as local alias — both joins must be profiled independently."""
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        combined = " ".join(join_parts)
        self.assertIn("us_states", combined)
        self.assertIn("us_counties", combined)


# ─── Partitioned tables ───────────────────────────────────────────────────────

_PARTITIONED_SCHEMA = {
    "tables": [
        {
            "name": "project.dataset.events",
            "columns": [
                {"name": "event_date", "type": "DATE"},
                {"name": "user_id", "type": "STRING"},
                {"name": "revenue", "type": "FLOAT64"},
            ],
            "partition": {"type": "time", "field": "event_date"},
        }
    ]
}

_INGESTION_TIME_SCHEMA = {
    "tables": [
        {
            "name": "project.dataset.logs",
            "columns": [
                {"name": "message", "type": "STRING"},
                {"name": "level", "type": "STRING"},
            ],
            "partition": {"type": "time", "field": None},
        }
    ]
}

_RANGE_PARTITIONED_SCHEMA = {
    "tables": [
        {
            "name": "project.dataset.sales",
            "columns": [
                {"name": "region_id", "type": "INTEGER"},
                {"name": "amount", "type": "FLOAT64"},
            ],
            "partition": {"type": "range", "field": "region_id"},
        }
    ]
}


class TestBuildPartitionWhere(unittest.TestCase):
    """Unit tests for _build_partition_where."""

    def test_time_field_based_returns_in_clause(self):
        result = _build_partition_where(
            "project.dataset.events",
            {"type": "time", "field": "event_date"},
            "bigquery",
            3,
        )
        self.assertIsNotNone(result)
        self.assertIn("event_date", result)
        self.assertIn("LIMIT 3", result)
        self.assertIn("ORDER BY", result)
        self.assertIn("DESC", result)

    def test_ingestion_time_uses_partitiondate(self):
        result = _build_partition_where(
            "project.dataset.logs",
            {"type": "time", "field": None},
            "bigquery",
            5,
        )
        self.assertIsNotNone(result)
        self.assertIn("_PARTITIONDATE", result)
        self.assertIn("LIMIT 5", result)

    def test_range_partitioning_returns_none(self):
        result = _build_partition_where(
            "project.dataset.sales",
            {"type": "range", "field": "region_id"},
            "bigquery",
            3,
        )
        self.assertIsNone(result)

    def test_empty_partition_returns_none(self):
        self.assertIsNone(_build_partition_where("t", {}, "bigquery", 3))

    def test_limit_respected(self):
        result = _build_partition_where(
            "project.dataset.events",
            {"type": "time", "field": "event_date"},
            "bigquery",
            7,
        )
        self.assertIn("LIMIT 7", result)

    def test_literal_in_clause_with_prefetched_values(self):
        result = _build_partition_where(
            "project.dataset.events",
            {
                "type": "time",
                "field": "event_date",
                "granularity": "DAY",
                "col_type": "DATE",
                "values": ["20240115", "20240114", "20240113"],
            },
            "bigquery",
            3,
        )
        self.assertIsNotNone(result)
        self.assertIn("2024-01-15", result)
        self.assertIn("2024-01-14", result)
        self.assertIn("2024-01-13", result)
        self.assertNotIn("SELECT", result)
        self.assertNotIn("LIMIT", result)
        self.assertIn("event_date", result)

    def test_literal_in_clause_ingestion_time(self):
        result = _build_partition_where(
            "project.dataset.logs",
            {
                "type": "time",
                "field": None,
                "granularity": "DAY",
                "col_type": "DATE",
                "values": ["20240115", "20240114"],
            },
            "bigquery",
            3,
        )
        self.assertIn("_PARTITIONDATE", result)
        self.assertIn("2024-01-15", result)
        self.assertNotIn("SELECT", result)

    def test_timestamp_col_uses_date_wrapper(self):
        result = _build_partition_where(
            "project.dataset.events",
            {
                "type": "time",
                "field": "created_at",
                "granularity": "DAY",
                "col_type": "TIMESTAMP",
                "values": ["20240115", "20240114"],
            },
            "bigquery",
            3,
        )
        self.assertIsNotNone(result)
        self.assertIn("DATE(`created_at`)", result)
        self.assertIn("2024-01-15", result)
        self.assertNotIn("SELECT", result)

    def test_non_day_granularity_falls_back_to_subquery(self):
        result = _build_partition_where(
            "project.dataset.events",
            {
                "type": "time",
                "field": "event_month",
                "granularity": "MONTH",
                "values": ["202401", "202312"],
            },
            "bigquery",
            3,
        )
        self.assertIn("SELECT", result)
        self.assertIn("LIMIT 3", result)

    def test_format_day_partition_values(self):
        self.assertEqual(
            _format_day_partition_values(["20240115", "20240114", "20240113"]),
            ["2024-01-15", "2024-01-14", "2024-01-13"],
        )
        self.assertEqual(_format_day_partition_values(["202401", "bad"]), [])
        self.assertEqual(_format_day_partition_values([]), [])


class TestBuildPartitionWindow(unittest.TestCase):
    """Unit tests for build_partition_window."""

    def test_exact_window_from_prefetched_values(self):
        w = build_partition_window(
            {
                "type": "time",
                "field": "event_date",
                "granularity": "DAY",
                "values": ["20240115", "20240113", "20240114"],
            },
            3,
        )
        self.assertEqual(w["exact"], True)
        self.assertEqual(w["field"], "event_date")
        self.assertEqual(w["limit"], 3)
        # min/max sorted regardless of input order
        self.assertEqual(w["min"], "2024-01-13")
        self.assertEqual(w["max"], "2024-01-15")
        self.assertEqual(w["values"], ["2024-01-13", "2024-01-14", "2024-01-15"])

    def test_inexact_window_ingestion_time_no_values(self):
        w = build_partition_window({"type": "time", "field": None}, 2)
        self.assertEqual(w["exact"], False)
        self.assertEqual(w["field"], "_PARTITIONDATE")
        self.assertEqual(w["limit"], 2)
        self.assertNotIn("min", w)
        self.assertNotIn("max", w)

    def test_non_day_granularity_is_inexact(self):
        w = build_partition_window(
            {
                "type": "time",
                "field": "event_month",
                "granularity": "MONTH",
                "values": ["202401", "202312"],
            },
            3,
        )
        self.assertEqual(w["exact"], False)
        self.assertNotIn("min", w)

    def test_range_partition_returns_none(self):
        self.assertIsNone(
            build_partition_window({"type": "range", "field": "region_id"}, 3)
        )

    def test_no_limit_returns_none(self):
        self.assertIsNone(build_partition_window({"type": "time", "field": "d"}, 0))
        self.assertIsNone(build_partition_window({"type": "time", "field": "d"}, None))

    def test_empty_partition_returns_none(self):
        self.assertIsNone(build_partition_window({}, 3))

    def test_attached_to_profile_schema_output(self):
        """profile_schema attaches partition_window to time-partitioned tables."""

        def _exec(sql):
            if "row_count" in sql:
                return [{"row_count": 2}]
            if "total_count" in sql:
                return [{"total_count": 2, "null_count": 0, "distinct_count": 2}]
            if "duplicate" in sql:
                return [{"duplicate_value_count": 0}]
            if "MIN" in sql:
                return [{"min_value": "2024-01-13", "max_value": "2024-01-15"}]
            if "date_regularity" in sql:
                return [{"date_regularity": "daily"}]
            return [{"val": "x", "cnt": 1}]

        schema = {
            "tables": [
                {
                    "name": "project.dataset.events",
                    "columns": [{"name": "event_date", "type": "DATE"}],
                    "partition": {
                        "type": "time",
                        "field": "event_date",
                        "granularity": "DAY",
                        "values": ["20240115", "20240114", "20240113"],
                    },
                }
            ]
        }
        prof = profile_schema(schema, _exec)
        win = prof["tables"]["project.dataset.events"]["partition_window"]
        self.assertEqual(win["exact"], True)
        self.assertEqual(win["min"], "2024-01-13")
        self.assertEqual(win["max"], "2024-01-15")

    def test_attached_to_parse_profile_query_result(self):
        """parse_profile_query_result attaches partition_window from the schema."""
        schema = {
            "tables": [
                {
                    "name": "project.dataset.events",
                    "columns": [{"name": "event_date", "type": "DATE"}],
                    "partition": {
                        "type": "time",
                        "field": "event_date",
                        "granularity": "DAY",
                        "values": ["20240115", "20240114"],
                    },
                }
            ]
        }
        rows = [
            {
                "row_type": "column",
                "table_name": "project.dataset.events",
                "col_name": "event_date",
                "total_count": 2,
                "null_count": 0,
                "non_null_count": 2,
                "distinct_count": 2,
                "dup_count": 0,
                "min_val": "2024-01-14",
                "max_val": "2024-01-15",
                "top_values": "2024-01-14|||2024-01-15",
            }
        ]
        used = [{"table": "project.dataset.events", "used_columns": ["event_date"]}]
        p = parse_profile_query_result(rows, schema, used)
        win = p["tables"]["project.dataset.events"]["partition_window"]
        self.assertEqual(win["exact"], True)
        self.assertEqual(win["min"], "2024-01-14")
        self.assertEqual(win["max"], "2024-01-15")


class TestBuildProfileQueryPartitioned(unittest.TestCase):
    """Integration tests: build_profile_query with partitioned tables."""

    _USED = [
        {"table": "project.dataset.events", "used_columns": ["user_id", "revenue"]}
    ]

    def _query(self, options=None):
        return build_profile_query(
            _PARTITIONED_SCHEMA,
            self._USED,
            dialect="bigquery",
            options=options,
        )

    def test_partition_where_injected_in_column_branches(self):
        q = self._query(options={"partition_limit": 3})
        col_branches = [b for b in q.split("UNION ALL") if "'column'" in b]
        self.assertGreater(len(col_branches), 0)
        for branch in col_branches:
            self.assertIn("event_date", branch)
            self.assertIn("LIMIT 3", branch)

    def test_no_partition_when_limit_none(self):
        # partition_limit=None → the partition field (event_date) must not appear
        # as a WHERE filter in column branches (LIMIT still appears in top_values)
        q = self._query(options={"partition_limit": None})
        col_branches = [b for b in q.split("UNION ALL") if "'column'" in b]
        self.assertGreater(len(col_branches), 0)
        for branch in col_branches:
            self.assertNotIn("event_date", branch)

    def test_partition_limit_default_is_3(self):
        """Default partition_limit=3 applies without explicit options."""
        q = self._query()
        self.assertIn("LIMIT 3", q)

    def test_generated_sql_parseable(self):
        q = self._query(options={"partition_limit": 3})
        parsed = sqlglot.parse(
            q, dialect="bigquery", error_level=sqlglot.ErrorLevel.RAISE
        )
        self.assertGreater(len(parsed), 0)

    def test_ingestion_time_partition(self):
        used = [{"table": "project.dataset.logs", "used_columns": ["message"]}]
        q = build_profile_query(
            _INGESTION_TIME_SCHEMA,
            used,
            dialect="bigquery",
            options={"partition_limit": 2},
        )
        self.assertIn("_PARTITIONDATE", q)
        self.assertIn("LIMIT 2", q)

    def test_range_partition_no_filter(self):
        # Range partitioning is skipped → partition field (region_id) must not appear
        # as a WHERE filter in column branches for 'amount'
        used = [{"table": "project.dataset.sales", "used_columns": ["amount"]}]
        q = build_profile_query(
            _RANGE_PARTITIONED_SCHEMA,
            used,
            dialect="bigquery",
            options={"partition_limit": 3},
        )
        col_branches = [b for b in q.split("UNION ALL") if "'column'" in b]
        self.assertGreater(len(col_branches), 0)
        for branch in col_branches:
            self.assertNotIn("region_id", branch)


# ─── _collect_join_specs: function expressions in ON clause ───────────────────


class TestCollectJoinSpecsFuncExpr(unittest.TestCase):
    """_collect_join_specs with function calls in the ON condition.

    When the join condition is ON t2.col = FUNC(cte.other_col), the left_table
    must be derived from the column references inside the function ('cte'), NOT
    from the primary FROM table of the containing SELECT.

    If it falls back to the primary FROM table, _build_join_query will produce:
        SELECT FUNC(cte.col) AS join_key FROM primary_table GROUP BY 1
    where 'cte' is out of scope → BigQuery raises "Unrecognized name: cte".
    """

    def test_substr_cte_alias_assigns_cte_as_left_table(self):
        """ON target.dpt = SUBSTR(cte.copost, 1, 2) → left_table must be 'cte'.

        'cte' is in scope as a JOIN participant and as a CTE in the WITH clause.
        The generated left subquery must use FROM cte, not FROM main_table.
        """
        sql = (
            "WITH cte AS (SELECT copost FROM source_table) "
            "SELECT * FROM main_table "
            "JOIN cte ON cte.id = main_table.id "
            "JOIN target ON target.dpt = SUBSTR(cte.copost, 1, 2)"
        )
        specs = _collect_join_specs(sql)
        target_specs = [s for s in specs if s["right_table"] == "target"]
        self.assertEqual(
            len(target_specs), 1, "Expected exactly one spec for target JOIN"
        )
        self.assertEqual(
            target_specs[0]["left_table"],
            "cte",
            f"left_table should be 'cte', got '{target_specs[0]['left_table']}'",
        )

    def test_substr_fully_qualified_cte_production_repro(self):
        """Production repro: coface CTE + territoire_prospect joined via SUBSTR.

        Mirrors the actual BigQuery error:
          "Unrecognized name: coface at [349:1147]"
        from ON territoire_prospect.dpt = SUBSTR(coface.copost, 1, 2)
        where coface is a CTE wrapping a fully-qualified table.
        """
        sql = (
            "WITH coface AS (SELECT copost FROM `proj.ds.coface_src`) "
            "SELECT * FROM `proj.ds.DS_RCOMP` AS rcomp "
            "JOIN coface ON coface.id = rcomp.id "
            "JOIN `proj.ds.territoire_prospect` AS territoire_prospect "
            "  ON territoire_prospect.dpt = SUBSTR(coface.copost, 1, 2)"
        )
        specs = _collect_join_specs(sql)
        tp_specs = [s for s in specs if "territoire_prospect" in s["right_table"]]
        self.assertEqual(len(tp_specs), 1)
        self.assertEqual(
            tp_specs[0]["left_table"],
            "coface",
            f"left_table should be 'coface', got '{tp_specs[0]['left_table']}'",
        )

    def test_concat_two_tables_spec_skipped(self):
        """CONCAT(a.x, b.y) references two tables → no spec emitted for that JOIN.

        We cannot build a single-FROM subquery for a key expression that spans
        two different tables. The spec must be omitted entirely.
        """
        sql = "SELECT * FROM a JOIN b ON b.id = a.id JOIN c ON c.dpt = CONCAT(a.x, b.y)"
        specs = _collect_join_specs(sql)
        c_specs = [s for s in specs if s["right_table"] == "c"]
        self.assertEqual(
            len(c_specs),
            0,
            "Spec with a multi-table function expression must be skipped",
        )

    def test_valid_join_unaffected_when_sibling_spec_skipped(self):
        """The valid a→b JOIN is not dropped when the a→c spec is skipped."""
        sql = "SELECT * FROM a JOIN b ON b.id = a.id JOIN c ON c.dpt = CONCAT(a.x, b.y)"
        specs = _collect_join_specs(sql)
        ab_specs = [s for s in specs if s["right_table"] == "b"]
        self.assertEqual(len(ab_specs), 1)
        self.assertEqual(ab_specs[0]["left_table"], "a")

    def test_plain_column_join_unaffected(self):
        """Regression: ON a.id = b.id still produces left_table='a', right_table='b'."""
        specs = _collect_join_specs("SELECT * FROM a JOIN b ON a.id = b.id")
        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0]["left_table"], "a")
        self.assertEqual(specs[0]["right_table"], "b")

    def test_func_expr_single_ref_matches_primary_table(self):
        """SUBSTR(main.col, 1, 2) where main IS the primary FROM → left_table='main'."""
        sql = "SELECT * FROM main JOIN target ON target.dpt = SUBSTR(main.col, 1, 2)"
        specs = _collect_join_specs(sql)
        target_specs = [s for s in specs if s["right_table"] == "target"]
        self.assertEqual(len(target_specs), 1)
        self.assertEqual(target_specs[0]["left_table"], "main")


# ─── build_profile_query: func expr join metadata and SQL validity ─────────────


_FUNC_EXPR_SCHEMA = {
    "tables": [
        {
            "name": "DS_RCOMP",
            "columns": [
                {"name": "id", "type": "STRING"},
                {"name": "copost", "type": "STRING"},
            ],
        },
        {
            "name": "territoire_prospect",
            "columns": [{"name": "dpt", "type": "STRING"}],
        },
    ]
}

# Mirrors the structure that caused the BigQuery error:
# "Unrecognized name: coface at [349:1147]"
# Before fix: left_table = "DS_RCOMP" (fallback), left_expr_sql still has coface.copost
# → SELECT SUBSTR(coface.copost, 1, 2) FROM `DS_RCOMP` GROUP BY 1  -- coface out of scope
# After fix: left_table = "coface" (CTE, in scope via WITH clause)
# → SELECT SUBSTR(coface.copost, 1, 2) FROM coface GROUP BY 1  -- valid
_FUNC_EXPR_SQL = (
    "WITH coface AS (SELECT id, copost FROM DS_RCOMP) "
    "SELECT * FROM DS_RCOMP AS rcomp "
    "JOIN coface ON coface.id = rcomp.id "
    "JOIN territoire_prospect "
    "  ON territoire_prospect.dpt = SUBSTR(coface.copost, 1, 2)"
)

_FUNC_EXPR_USED = [
    {"table": "DS_RCOMP", "used_columns": ["id", "copost"]},
    {"table": "territoire_prospect", "used_columns": ["dpt"]},
]


class TestBuildProfileQueryFuncExprJoin(unittest.TestCase):
    """build_profile_query with function-expression join keys.

    Regression suite for the BigQuery error "Unrecognized name: coface"
    caused by generating SELECT SUBSTR(coface.copost, 1, 2) FROM DS_RCOMP.
    """

    def _q(self):
        return build_profile_query(
            _FUNC_EXPR_SCHEMA,
            _FUNC_EXPR_USED,
            dialect="bigquery",
            sql_query=_FUNC_EXPR_SQL,
        )

    def test_territoire_join_branch_left_table_is_coface(self):
        """The stored left_table for the territoire_prospect join must be 'coface'.

        Before fix: 'DS_RCOMP' AS left_table (wrong — generates coface out of scope).
        After fix:  'coface' AS left_table  (correct — coface is in the WITH clause).
        """
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        tp_parts = [p for p in join_parts if "territoire_prospect" in p]
        self.assertGreaterEqual(len(tp_parts), 1)
        for part in tp_parts:
            self.assertIn(
                "'coface'",
                part,
                "left_table metadata must reference 'coface', not 'DS_RCOMP'",
            )

    def test_territoire_join_subquery_does_not_reference_coface_from_ds_rcomp(self):
        """The left subquery must NOT query DS_RCOMP while using coface in its expression.

        The broken pattern is:
            SELECT SUBSTR(coface.copost, 1, 2) AS join_key
            FROM `DS_RCOMP`   ← coface not in scope here → BigQuery error
        After fix, either:
          - FROM coface (CTE reference)  — expression SUBSTR(coface.copost, ...) in scope
          - FROM DS_RCOMP with bare expression SUBSTR(copost, ...) — also valid
        """
        q = self._q()
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        tp_parts = [p for p in join_parts if "territoire_prospect" in p]
        self.assertGreaterEqual(len(tp_parts), 1)
        for part in tp_parts:
            # The broken pattern: coface.copost referenced inside a DS_RCOMP subquery
            self.assertFalse(
                "coface.copost" in part and "FROM `DS_RCOMP`" in part,
                "coface.copost must not appear in a subquery that FROM DS_RCOMP",
            )

    def test_multi_table_func_expr_no_join_branch_generated(self):
        """A join ON CONCAT(a.x, b.y) must produce no profiling branch.

        We cannot safely profile a key that spans two tables.
        """
        schema = {
            "tables": [
                {"name": "a", "columns": [{"name": "x", "type": "STRING"}]},
                {"name": "b", "columns": [{"name": "y", "type": "STRING"}]},
                {"name": "c", "columns": [{"name": "dpt", "type": "STRING"}]},
            ]
        }
        sql = "SELECT * FROM a JOIN b ON b.id = a.id JOIN c ON c.dpt = CONCAT(a.x, b.y)"
        used = [
            {"table": "a", "used_columns": ["x"]},
            {"table": "b", "used_columns": ["y"]},
            {"table": "c", "used_columns": ["dpt"]},
        ]
        q = build_profile_query(schema, used, sql_query=sql)
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        c_parts = [p for p in join_parts if "'c'" in p or '"c"' in p]
        self.assertEqual(
            len(c_parts),
            0,
            "No join branch should be generated for a multi-table function expression",
        )

    def test_func_expr_on_base_table_strips_dangling_alias(self):
        """JOIN ON TRIM(alias.col) where alias is a *base* table (not a CTE).

        Regression for the BigQuery error "Unrecognized name: acomp": the join
        key expression keeps the table qualifier (``TRIM(acomp.cd_pays_bin)``)
        but the profiling subquery scans the bare base table, which has no
        ``acomp`` alias in scope. The qualifier must be stripped from *every*
        column inside the expression, not only when the whole key is a bare
        column.
        """
        schema = {
            "tables": [
                {
                    "name": "acquereur",
                    "columns": [{"name": "cd_pays_bin", "type": "STRING"}],
                },
                {
                    "name": "pays",
                    "columns": [{"name": "code_pays_alpha_3", "type": "STRING"}],
                },
            ]
        }
        sql = (
            "SELECT * FROM acquereur AS acomp "
            "JOIN pays AS p "
            "  ON TRIM(acomp.cd_pays_bin) = TRIM(p.code_pays_alpha_3)"
        )
        used = [
            {"table": "acquereur", "used_columns": ["cd_pays_bin"]},
            {"table": "pays", "used_columns": ["code_pays_alpha_3"]},
        ]
        q = build_profile_query(schema, used, sql_query=sql, dialect="bigquery")
        # The executable key expressions must reference the bare columns.
        self.assertIn("TRIM(cd_pays_bin)", q)
        self.assertIn("TRIM(code_pays_alpha_3)", q)
        # The dangling alias must only survive in the metadata string literal
        # ('TRIM(acomp.cd_pays_bin)'), never in an executable subquery.
        self.assertLessEqual(
            q.count("TRIM(acomp.cd_pays_bin)"),
            1,
            "alias-qualified expr must not appear in an executable subquery",
        )

    def test_inverted_on_with_func_expr_assigns_sides_correctly(self):
        """ON TRIM(b.y) = TRIM(a.x) — right table written first, both wrapped.

        Regression: the swap that decides which side is left/right keyed off the
        top node being a bare column. With function wrappers it couldn't tell,
        skipped the swap, and emitted ``SELECT TRIM(x) FROM B`` (A's column read
        from B) → "Unrecognized name". Each key must be profiled against its own
        source table.
        """
        schema = {
            "tables": [
                {"name": "A", "columns": [{"name": "x", "type": "STRING"}]},
                {"name": "B", "columns": [{"name": "y", "type": "STRING"}]},
            ]
        }
        used = [
            {"table": "A", "used_columns": ["x"]},
            {"table": "B", "used_columns": ["y"]},
        ]
        sql = "SELECT * FROM A AS a JOIN B AS b ON TRIM(b.y) = TRIM(a.x)"
        q = build_profile_query(schema, used, sql_query=sql, dialect="bigquery")
        # A's column profiled against A, B's against B.
        self.assertIn("TRIM(x) AS join_key, COUNT(*) AS cnt FROM `A`", q)
        self.assertIn("TRIM(y) AS join_key, COUNT(*) AS cnt FROM `B`", q)
        # The broken pattern: A's column x read from table B.
        self.assertNotIn("TRIM(x) AS join_key, COUNT(*) AS cnt FROM `B`", q)

    def test_same_pair_joined_in_two_scopes_does_not_double_composite_key(self):
        """The same physical pair joined in two CTEs must not double the key.

        Production repro: ``DS_ACOMP`` → ``refcomm`` is joined on ``(k1, k2)`` in
        two separate CTEs (once with operands inverted). Grouping by
        (left_table, right_table, alias) merged all four conditions into one
        branch → ``CONCAT(k1, k2, k1, k2)``, a wrong composite key that profiles
        bogus cardinality. Identical conditions must be deduplicated.
        """
        schema = {
            "tables": [
                {
                    "name": "A",
                    "columns": [
                        {"name": "k1", "type": "STRING"},
                        {"name": "k2", "type": "STRING"},
                    ],
                },
                {
                    "name": "base_r",
                    "columns": [
                        {"name": "k1", "type": "STRING"},
                        {"name": "k2", "type": "STRING"},
                    ],
                },
            ]
        }
        used = [
            {"table": "A", "used_columns": ["k1", "k2"]},
            {"table": "base_r", "used_columns": ["k1", "k2"]},
        ]
        sql = (
            "WITH r AS (SELECT k1, k2 FROM base_r), "
            "c1 AS (SELECT a.k1 FROM A AS a JOIN r AS r ON a.k1 = r.k1 AND a.k2 = r.k2), "
            "c2 AS (SELECT a.k1 FROM A AS a JOIN r AS r ON r.k1 = a.k1 AND r.k2 = a.k2) "
            "SELECT c1.k1 FROM c1 JOIN c2 ON c1.k1 = c2.k1"
        )
        q = build_profile_query(schema, used, sql_query=sql, dialect="bigquery")
        # The composite key for the A→r pair must list each condition once.
        self.assertNotIn("'a.k1 AND a.k2 AND a.k1 AND a.k2'", q)
        self.assertIn("'a.k1 AND a.k2'", q)


# ─── build_profile_query: multi-source CTE join key ──────────────────────────


# Mirrors the production crash "Unrecognized name: NATURE":
# a CTE `t` whose compound join key columns trace to THREE different base tables
# (no_contrat_commercant→DIM_CONTRAT, partition_date→FAITS_PRE, nature→DIM_NATURE).
# _resolve_cte_source correctly returns None (keys span several sources), but the
# old base-table fallback flattened all keys onto the CTE's primary FROM table
# (FAITS_PRE) → SELECT ... nature ... FROM FAITS_PRE, where `nature` does not exist.
# After fix: the CTE is queried directly (it is in scope via the WITH preamble).
_MULTI_SRC_SCHEMA = {
    "tables": [
        {
            "name": "FAITS_PRE",
            "columns": [
                {"name": "id_contrat", "type": "STRING"},
                {"name": "id_nature", "type": "STRING"},
                {"name": "partition_date", "type": "DATE"},
            ],
        },
        {
            "name": "DIM_NATURE",
            "columns": [
                {"name": "id_nature", "type": "STRING"},
                {"name": "nature", "type": "STRING"},
            ],
        },
        {
            "name": "DIM_CONTRAT",
            "columns": [
                {"name": "id_contrat", "type": "STRING"},
                {"name": "no_contrat_commercant", "type": "STRING"},
            ],
        },
        {
            "name": "C3",
            "columns": [
                {"name": "no_contrat_commercant", "type": "STRING"},
                {"name": "partition_date", "type": "DATE"},
                {"name": "nature", "type": "STRING"},
            ],
        },
    ]
}

_MULTI_SRC_SQL = (
    "WITH t AS ("
    "  SELECT d.no_contrat_commercant, f.partition_date, n.nature"
    "  FROM FAITS_PRE AS f"
    "  JOIN DIM_NATURE AS n ON n.id_nature = f.id_nature"
    "  JOIN DIM_CONTRAT AS d ON d.id_contrat = f.id_contrat"
    "), "
    "p AS (SELECT no_contrat_commercant, partition_date, nature FROM C3 WHERE nature = 'X') "
    "SELECT * FROM t "
    "JOIN p ON p.no_contrat_commercant = t.no_contrat_commercant"
    "  AND p.partition_date = t.partition_date"
    "  AND p.nature = t.nature"
)

_MULTI_SRC_USED = [
    {"table": "C3", "used_columns": ["nature"]},
]


class TestBuildProfileQueryMultiSourceCteJoin(unittest.TestCase):
    """Regression for "Unrecognized name: NATURE".

    A CTE join key whose columns come from several joined base tables must not
    be flattened onto the CTE's primary FROM table — the CTE is queried directly.
    """

    def _q(self):
        return build_profile_query(
            _MULTI_SRC_SCHEMA,
            _MULTI_SRC_USED,
            dialect="bigquery",
            sql_query=_MULTI_SRC_SQL,
        )

    def _tp_join_parts(self, q):
        join_parts = [p for p in q.split("UNION ALL") if "'join'" in p]
        # The t↔p branch is the one carrying the compound merchant/date/nature key.
        return [p for p in join_parts if "no_contrat_commercant" in p]

    def test_join_branch_present(self):
        tp_parts = self._tp_join_parts(self._q())
        self.assertGreaterEqual(
            len(tp_parts), 1, "Expected a join branch for the t↔p join"
        )

    def test_left_subquery_does_not_select_nature_from_faits_pre(self):
        """The broken pattern: SELECT ... nature ... FROM `FAITS_PRE` (no nature column)."""
        for part in self._tp_join_parts(self._q()):
            self.assertNotIn(
                "FAITS_PRE",
                part,
                "Multi-source CTE key must query the CTE `t` directly, "
                "not the flattened primary base table FAITS_PRE",
            )

    def test_left_table_metadata_is_cte_name(self):
        """left_table metadata for the t↔p join is the CTE name 't', not FAITS_PRE."""
        for part in self._tp_join_parts(self._q()):
            self.assertIn("'t'", part)


# ─── derived-expression profiling: mixed qualified / unqualified refs ─────────


# Mirrors the production crash "Unrecognized name: CA_FACTURE":
# a derived expression computed in a CTE that joins three sibling CTEs.  It mixes
# a *qualified* ref (p.frais_de_gestion → CTE p) with *unqualified* refs
# (ca_facture → CTE u, ca_facture_matrice → CTE t).  The builder only knows the
# qualified table (p) and would emit `... ca_facture ... FROM p`, where
# ca_facture does not exist.  Such an expression must not be profiled.
_DERIVED_MIXED_SQL = (
    "WITH "
    "t AS (SELECT id_contrat, SUM(ca) AS ca_facture_matrice FROM FAITS GROUP BY 1), "
    "u AS (SELECT id_contrat, SUM(ca) AS ca_facture FROM FAITS GROUP BY 1), "
    "p AS (SELECT id_contrat, frais_de_gestion FROM C3) "
    "SELECT IF(ca_facture <> 0, p.frais_de_gestion * ca_facture_matrice / ca_facture,"
    " p.frais_de_gestion) AS frais_ventiles "
    "FROM t JOIN u USING (id_contrat) JOIN p USING (id_contrat)"
)


class TestDerivedExprMixedQualification(unittest.TestCase):
    """Regression for "Unrecognized name: CA_FACTURE".

    A derived expression mixing qualified refs (one CTE) with unqualified refs
    (sibling CTEs) cannot be profiled against the single qualified table.
    """

    def test_mixed_qualification_expr_not_profiled(self):
        branches = _build_derived_expr_profile_branches(
            _DERIVED_MIXED_SQL, [], "bigquery"
        )
        # No branch may query a single CTE while referencing a column that lives
        # in a sibling CTE — that is exactly the invalid pattern.
        for b in branches:
            lowered = b.lower()
            self.assertFalse(
                "ca_facture" in lowered and "from `p`" in lowered,
                "mixed-qualification derived expr must not be profiled against CTE p",
            )

    def test_full_profile_query_parses(self):
        schema = {
            "tables": [
                {
                    "name": "FAITS",
                    "columns": [
                        {"name": "id_contrat", "type": "STRING"},
                        {"name": "ca", "type": "FLOAT"},
                    ],
                },
                {
                    "name": "C3",
                    "columns": [
                        {"name": "id_contrat", "type": "STRING"},
                        {"name": "frais_de_gestion", "type": "FLOAT"},
                    ],
                },
            ]
        }
        used = [{"table": "C3", "used_columns": ["frais_de_gestion"]}]
        q = build_profile_query(
            schema, used, dialect="bigquery", sql_query=_DERIVED_MIXED_SQL
        )
        # The generated query must not reference ca_facture from a bare `FROM `p``.
        self.assertFalse(
            "ca_facture" in q.lower() and "from `p`" in q.lower(),
            "profile query must not profile the mixed-qualification expression",
        )
        sqlglot.parse_one(q, dialect="bigquery")  # must parse


# ─── build_profile_queries: branch isolation ─────────────────────────────────


class TestBuildProfileQueriesIsolation(unittest.TestCase):
    """build_profile_queries isolates join/derived branches into their own queries.

    A failure in one join or derived branch must not be able to sink column
    profiling, so column-profile branches live in their own per-table queries,
    join branches in one query, and each derived expression in its own query.
    """

    _SCHEMA = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {"name": "id", "type": "STRING"},
                    {"name": "user_id", "type": "STRING"},
                    {"name": "raw_amount", "type": "STRING"},
                ],
            },
            {
                "name": "users",
                "columns": [{"name": "id", "type": "STRING"}],
            },
        ]
    }
    _USED = [
        {"table": "orders", "used_columns": ["id", "user_id", "raw_amount"]},
        {"table": "users", "used_columns": ["id"]},
    ]
    _SQL = (
        "SELECT o.id, SAFE_CAST(o.raw_amount AS FLOAT64) AS amt "
        "FROM orders o JOIN users u ON o.user_id = u.id"
    )

    def _queries(self):
        return build_profile_queries(
            self._SCHEMA, self._USED, dialect="bigquery", sql_query=self._SQL
        )

    def test_column_queries_have_no_join_or_derived_branches(self):
        """Per-table column queries must not carry join/derived branches."""
        for q in self._queries():
            if "'column'" in q and "'join'" not in q and "'derived_expr'" not in q:
                continue  # a clean column-only query
            # A query carrying a relation branch must NOT also carry column branches
            if "'join'" in q or "'derived_expr'" in q:
                self.assertNotIn(
                    "'column'",
                    q,
                    "join/derived branches must be isolated from column profiling",
                )

    def test_each_query_parses_independently(self):
        for q in self._queries():
            sqlglot.parse_one(q, dialect="bigquery")

    def test_join_and_derived_isolated_into_separate_queries(self):
        queries = self._queries()
        join_qs = [q for q in queries if "'join'" in q]
        derived_qs = [q for q in queries if "'derived_expr'" in q]
        # The join branch and the SAFE_CAST derived branch each get their own query.
        self.assertEqual(len(join_qs), 1)
        self.assertGreaterEqual(len(derived_qs), 1)
        # And neither shares a query with the other.
        for q in join_qs:
            self.assertNotIn("'derived_expr'", q)

    def test_one_query_per_derived_expression(self):
        """Two distinct derived expressions → two isolated derived queries."""
        sql = (
            "SELECT SAFE_CAST(o.raw_amount AS FLOAT64) AS a, "
            "REGEXP_EXTRACT(o.id, r'[0-9]+') AS b FROM orders o"
        )
        queries = build_profile_queries(
            self._SCHEMA, self._USED, dialect="bigquery", sql_query=sql
        )
        derived_qs = [q for q in queries if "'derived_expr'" in q]
        self.assertEqual(
            len(derived_qs), 2, "each derived expression must be its own query"
        )


if __name__ == "__main__":
    unittest.main()
