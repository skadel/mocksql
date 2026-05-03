"""
Tests for build_query/examples_executor.py — pure and DuckDB-testable functions.

Functions with LLM calls (_generate_assertions_from_result, _regenerate_assertion)
and those requiring the full LangGraph state / external DB are excluded.
"""

import json
import pytest
import duckdb
import pandas as pd
import sqlglot
from sqlglot import exp

from langchain_core.messages import AIMessage

from build_query.examples_executor import (
    filter_schemas_by_used_columns,
    _extract_conditions,
    _build_countif_expressions,
    _build_cte_sql_with_suffix,
    _determine_global_status,
    _decompose_cte_in_steps,
    _evaluate_assertions,
    _parse_unit_tests_from_state,
    _prepare_test_data,
    format_result,
)
from utils.msg_types import MsgType


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_schemas():
    return [
        {
            "table_name": "project.dataset.orders",
            "description": "",
            "columns": [
                {"name": "order_id", "type": "INTEGER"},
                {"name": "customer_id", "type": "INTEGER"},
                {"name": "amount", "type": "FLOAT"},
            ],
            "primary_keys": ["order_id"],
        },
        {
            "table_name": "project.dataset.customers",
            "description": "",
            "columns": [
                {"name": "customer_id", "type": "INTEGER"},
                {"name": "name", "type": "STRING"},
                {"name": "email", "type": "STRING"},
            ],
            "primary_keys": ["customer_id"],
        },
    ]


@pytest.fixture
def con():
    """In-memory DuckDB connection with a simple result table."""
    c = duckdb.connect()
    c.execute("CREATE TABLE __result__ (id INTEGER, value TEXT, amount FLOAT)")
    c.execute(
        "INSERT INTO __result__ VALUES (1, 'a', 10.0), (2, 'b', 20.0), (3, 'b', 30.0)"
    )
    return c


# ---------------------------------------------------------------------------
# filter_schemas_by_used_columns
# ---------------------------------------------------------------------------


class TestFilterSchemasByUsedColumns:
    def test_returns_only_used_tables(self, simple_schemas):
        used = [
            {
                "database": "dataset",
                "table": "orders",
                "used_columns": ["order_id", "amount"],
            }
        ]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert len(result) == 1
        assert result[0]["table_name"] == "project.dataset.orders"

    def test_returns_only_used_columns(self, simple_schemas):
        used = [
            {"database": "dataset", "table": "orders", "used_columns": ["order_id"]}
        ]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        cols = [c["name"] for c in result[0]["columns"]]
        assert cols == ["order_id"]
        assert "amount" not in cols

    def test_multiple_tables(self, simple_schemas):
        used = [
            {"database": "dataset", "table": "orders", "used_columns": ["order_id"]},
            {
                "database": "dataset",
                "table": "customers",
                "used_columns": ["customer_id", "name"],
            },
        ]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert len(result) == 2

    def test_unknown_table_excluded(self, simple_schemas):
        used = [
            {"database": "dataset", "table": "unknown_table", "used_columns": ["col"]}
        ]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert result == []

    def test_empty_used_columns_excludes_table(self, simple_schemas):
        used = [{"database": "dataset", "table": "orders", "used_columns": []}]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert result == []

    def test_empty_schemas(self):
        used = [{"database": "dataset", "table": "orders", "used_columns": ["id"]}]
        result = filter_schemas_by_used_columns([], used)
        assert result == []

    def test_no_database_qualifier(self):
        schemas = [
            {
                "table_name": "orders",
                "description": "",
                "columns": [{"name": "id", "type": "INTEGER"}],
                "primary_keys": [],
            }
        ]
        used = [{"database": None, "table": "orders", "used_columns": ["id"]}]
        result = filter_schemas_by_used_columns(schemas, used)
        assert len(result) == 1

    def test_column_name_case_insensitive(self, simple_schemas):
        used = [
            {
                "database": "dataset",
                "table": "orders",
                "used_columns": ["ORDER_ID", "AMOUNT"],
            }
        ]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert len(result[0]["columns"]) == 2


class TestFilterSchemasWithRecordColumns:
    """Vérifie que les sous-champs RECORD sont inclus quand le parent est référencé."""

    @pytest.fixture
    def record_schemas(self):
        return [
            {
                "table_name": "project.dataset.ga_sessions",
                "description": "",
                "columns": [
                    {"name": "fullVisitorId", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "visitId", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "trafficSource", "type": "RECORD", "mode": "NULLABLE"},
                    {"name": "trafficSource.campaign", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "trafficSource.keyword", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "hits", "type": "RECORD", "mode": "REPEATED"},
                    {"name": "hits.type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "hits.hitNumber", "type": "INTEGER", "mode": "NULLABLE"},
                ],
                "primary_keys": [],
            }
        ]

    def test_record_parent_includes_subfields(self, record_schemas):
        used = [{"database": "dataset", "table": "ga_sessions", "used_columns": ["trafficSource"]}]
        result = filter_schemas_by_used_columns(record_schemas, used)
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "trafficSource" in col_names
        assert "trafficSource.campaign" in col_names
        assert "trafficSource.keyword" in col_names

    def test_record_subfields_not_included_for_unrelated_parent(self, record_schemas):
        used = [{"database": "dataset", "table": "ga_sessions", "used_columns": ["trafficSource"]}]
        result = filter_schemas_by_used_columns(record_schemas, used)
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "hits" not in col_names
        assert "hits.type" not in col_names

    def test_multiple_record_parents(self, record_schemas):
        used = [
            {
                "database": "dataset",
                "table": "ga_sessions",
                "used_columns": ["fullVisitorId", "trafficSource", "hits"],
            }
        ]
        result = filter_schemas_by_used_columns(record_schemas, used)
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "trafficSource.campaign" in col_names
        assert "hits.type" in col_names
        assert "hits.hitNumber" in col_names
        assert "fullVisitorId" in col_names

    def test_flat_schema_unaffected(self, simple_schemas):
        """Régression : les schémas sans RECORD ne changent pas."""
        used = [{"database": "dataset", "table": "orders", "used_columns": ["order_id"]}]
        result = filter_schemas_by_used_columns(simple_schemas, used)
        assert [c["name"] for c in result[0]["columns"]] == ["order_id"]


# ---------------------------------------------------------------------------
# _extract_conditions
# ---------------------------------------------------------------------------


class TestExtractConditions:
    def test_single_condition(self):
        expr = sqlglot.parse_one("a = 1", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 1

    def test_two_and_conditions(self):
        expr = sqlglot.parse_one("a = 1 AND b = 2", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 2

    def test_three_and_conditions(self):
        expr = sqlglot.parse_one("a = 1 AND b = 2 AND c = 3", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 3

    def test_non_and_returned_as_is(self):
        expr = sqlglot.parse_one("a > 10", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 1
        assert isinstance(conditions[0], exp.GT)


# ---------------------------------------------------------------------------
# _build_countif_expressions
# ---------------------------------------------------------------------------


class TestBuildCountifExpressions:
    def test_single_where_condition(self):
        parsed = sqlglot.parse_one("SELECT * FROM t WHERE a > 10", read="duckdb")
        where_expr = parsed.args["where"]
        result = _build_countif_expressions(where_expr)
        assert len(result) == 1
        assert "count_cond1" in result[0].sql()

    def test_two_where_conditions(self):
        parsed = sqlglot.parse_one(
            "SELECT * FROM t WHERE a > 10 AND b = 'x'", read="duckdb"
        )
        where_expr = parsed.args["where"]
        result = _build_countif_expressions(where_expr)
        assert len(result) == 2
        sqls = [r.sql() for r in result]
        assert any("count_cond1" in s for s in sqls)
        assert any("count_cond2" in s for s in sqls)


# ---------------------------------------------------------------------------
# _build_cte_sql_with_suffix
# ---------------------------------------------------------------------------


class TestBuildCteSqlWithSuffix:
    def test_replaces_cte_backtick_references(self):
        sql = "WITH `cte1` AS (SELECT 1), `cte2` AS (SELECT * FROM `cte1`) SELECT * FROM `cte2`"
        decomposed = [{"name": "cte1"}, {"name": "cte2"}]
        result = _build_cte_sql_with_suffix(sql, decomposed, "sess1")
        assert "`cte1_sess1`" in result
        assert "`cte2_sess1`" in result
        assert "`cte1`" not in result

    def test_no_match_leaves_sql_unchanged(self):
        sql = "SELECT * FROM other_table"
        decomposed = [{"name": "cte1"}]
        result = _build_cte_sql_with_suffix(sql, decomposed, "sess1")
        assert result == sql

    def test_empty_decomposed(self):
        sql = "SELECT 1"
        result = _build_cte_sql_with_suffix(sql, [], "suffix")
        assert result == sql


# ---------------------------------------------------------------------------
# _determine_global_status
# ---------------------------------------------------------------------------


class TestDetermineGlobalStatus:
    def test_empty_results_on_first_test(self):
        results = [{"status": "empty_results"}, {"status": "complete"}]
        assert _determine_global_status(results) == "empty_results"

    def test_complete_when_first_test_complete(self):
        results = [{"status": "complete"}, {"status": "empty_results"}]
        assert _determine_global_status(results) == "complete"

    def test_complete_when_all_complete(self):
        results = [{"status": "complete"}, {"status": "complete"}]
        assert _determine_global_status(results) == "complete"

    def test_empty_list_returns_complete(self):
        assert _determine_global_status([]) == "complete"

    def test_single_error_status(self):
        results = [{"status": "error"}]
        assert _determine_global_status(results) == "complete"


# ---------------------------------------------------------------------------
# format_result
# ---------------------------------------------------------------------------


class TestFormatResult:
    @pytest.mark.asyncio
    async def test_basic_dataframe(self):
        df = pd.DataFrame({"id": [1, 2], "val": ["a", "b"]})
        result = await format_result(df)
        parsed = json.loads(result)
        assert len(parsed) == 2
        assert parsed[0]["id"] == 1

    @pytest.mark.asyncio
    async def test_empty_dataframe(self):
        df = pd.DataFrame({"id": [], "val": []})
        result = await format_result(df)
        parsed = json.loads(result)
        assert parsed == []

    @pytest.mark.asyncio
    async def test_returns_string(self):
        df = pd.DataFrame({"x": [1]})
        result = await format_result(df)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# _evaluate_assertions
# ---------------------------------------------------------------------------


class TestEvaluateAssertions:
    def test_passing_assertion(self, con):
        con.execute("CREATE VIEW v_result AS SELECT * FROM __result__")
        assertions = [
            {
                "description": "no duplicate ids",
                "sql": "SELECT id FROM __result__ GROUP BY id HAVING COUNT(*) > 1",
            }
        ]
        results = _evaluate_assertions(assertions, "v_result", con)
        assert len(results) == 1
        assert results[0]["passed"] is True
        assert results[0]["failing_rows"] == []

    def test_failing_assertion(self, con):
        con.execute("CREATE VIEW v_result2 AS SELECT * FROM __result__")
        assertions = [
            {
                "description": "no value b",
                "sql": "SELECT * FROM __result__ WHERE value = 'b'",
            }
        ]
        results = _evaluate_assertions(assertions, "v_result2", con)
        assert results[0]["passed"] is False
        assert len(results[0]["failing_rows"]) == 2

    def test_invalid_sql_returns_error(self, con):
        con.execute("CREATE VIEW v_result3 AS SELECT * FROM __result__")
        assertions = [
            {"description": "bad sql", "sql": "SELECT non_existent_col FROM __result__"}
        ]
        results = _evaluate_assertions(assertions, "v_result3", con)
        assert results[0]["passed"] is False
        assert "error" in results[0]

    def test_empty_assertions(self, con):
        results = _evaluate_assertions([], "v_result", con)
        assert results == []

    def test_multiple_assertions(self, con):
        con.execute("CREATE VIEW v_result4 AS SELECT * FROM __result__")
        assertions = [
            {
                "description": "ids positive",
                "sql": "SELECT * FROM __result__ WHERE id <= 0",
            },
            {
                "description": "no value b",
                "sql": "SELECT * FROM __result__ WHERE value = 'b'",
            },
        ]
        results = _evaluate_assertions(assertions, "v_result4", con)
        assert results[0]["passed"] is True
        assert results[1]["passed"] is False


# ---------------------------------------------------------------------------
# _parse_unit_tests_from_state
# ---------------------------------------------------------------------------


class TestParseUnitTestsFromState:
    def _make_state(self, user_tables="", examples=None):
        return {
            "user_tables": user_tables,
            "examples": examples or [],
            "messages": [],
            "session": "sess",
            "dialect": "bigquery",
            "project": "proj",
            "query": "",
            "status": "",
            "gen_retries": 2,
            "request_id": None,
            "optimized_sql": "",
            "query_decomposed": "[]",
            "used_columns": [],
            "rerun_all_tests": False,
            "error": None,
        }

    def test_user_tables_dict_returns_list(self):
        test = {"test_index": "1", "data": {}}
        state = self._make_state(user_tables=json.dumps(test))
        result = _parse_unit_tests_from_state(state)
        assert isinstance(result, list)
        assert result[0]["test_index"] == "1"

    def test_user_tables_list_returned_as_is(self):
        tests = [{"test_index": "1"}, {"test_index": "2"}]
        state = self._make_state(user_tables=json.dumps(tests))
        result = _parse_unit_tests_from_state(state)
        assert len(result) == 2

    def test_no_examples_returns_none(self):
        state = self._make_state()
        result = _parse_unit_tests_from_state(state)
        assert result is None

    def test_examples_message_dict_returns_list(self):
        test = {"test_index": "1", "data": {}}
        msg = AIMessage(
            content=json.dumps(test),
            additional_kwargs={"type": MsgType.EXAMPLES},
        )
        state = self._make_state(examples=[msg])
        result = _parse_unit_tests_from_state(state)
        assert result == [test]

    def test_examples_message_list_returned_as_is(self):
        tests = [{"test_index": "1"}, {"test_index": "2"}]
        msg = AIMessage(
            content=json.dumps(tests),
            additional_kwargs={"type": MsgType.EXAMPLES},
        )
        state = self._make_state(examples=[msg])
        result = _parse_unit_tests_from_state(state)
        assert result == tests


# ---------------------------------------------------------------------------
# _decompose_cte_in_steps
# ---------------------------------------------------------------------------


class TestDecomposeCtesInSteps:
    def test_simple_query_has_final_step(self):
        sql = "SELECT id FROM orders"
        steps = _decompose_cte_in_steps(sql, "bigquery")
        assert steps[-1]["name"] == ""
        assert "orders" in steps[-1]["code"]

    def test_join_produces_join_step(self):
        sql = "SELECT o.id FROM orders AS o JOIN customers AS c ON o.customer_id = c.customer_id"
        steps = _decompose_cte_in_steps(sql, "bigquery")
        join_steps = [s for s in steps if "join" in s["name"]]
        assert len(join_steps) >= 1

    def test_where_produces_where_steps(self):
        sql = "SELECT id FROM orders WHERE amount > 10"
        steps = _decompose_cte_in_steps(sql, "bigquery")
        names = [s["name"] for s in steps]
        assert "step_before_where" in names
        assert "step_where" in names

    def test_no_join_no_where_only_final(self):
        sql = "SELECT 1 AS x"
        steps = _decompose_cte_in_steps(sql, "bigquery")
        assert len(steps) == 1
        assert steps[0]["name"] == ""

    def test_join_with_multiple_on_conditions(self):
        sql = (
            "SELECT * FROM orders AS o "
            "JOIN customers AS c ON o.customer_id = c.customer_id AND o.id > 0"
        )
        steps = _decompose_cte_in_steps(sql, "bigquery")
        join_steps = [s for s in steps if "join" in s["name"]]
        assert len(join_steps) >= 2


# ---------------------------------------------------------------------------
# _prepare_test_data
# ---------------------------------------------------------------------------


class TestPrepareTestData:
    def test_returns_data_dict(self):
        test_case = {"data": {"orders": [{"order_id": 1, "amount": 10.0}]}}
        schemas = [
            {
                "table_name": "orders",
                "columns": [
                    {"name": "order_id", "type": "INTEGER"},
                    {"name": "amount", "type": "FLOAT"},
                ],
            }
        ]
        result = _prepare_test_data(test_case, schemas)
        assert "orders" in result

    def test_empty_data(self):
        result = _prepare_test_data({"data": {}}, [])
        assert result == {}
