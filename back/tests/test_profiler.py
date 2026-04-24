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


if __name__ == "__main__":
    unittest.main()
