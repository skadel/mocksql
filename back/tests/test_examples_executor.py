"""
Tests for build_query/examples_executor.py — pure and DuckDB-testable functions.

Functions with LLM calls (_generate_assertions_and_evaluate, _regenerate_assertion)
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
    _build_count_steps_query,
    _extract_right_key_from_join,
    _select_failing_cte,
    _determine_global_status,
    _decompose_cte_in_steps,
    _evaluate_assertions,
    _assertion_sql_from_condition,
    _assertion_to_executable,
    _Assertion,
    _parse_unit_tests_from_state,
    _prepare_test_data,
    format_result,
)
from build_query.examples_generator import _format_cte_trace_hint
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
                    {
                        "name": "trafficSource.campaign",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {
                        "name": "trafficSource.keyword",
                        "type": "STRING",
                        "mode": "NULLABLE",
                    },
                    {"name": "hits", "type": "RECORD", "mode": "REPEATED"},
                    {"name": "hits.type", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "hits.hitNumber", "type": "INTEGER", "mode": "NULLABLE"},
                ],
                "primary_keys": [],
            }
        ]

    def test_record_parent_includes_subfields(self, record_schemas):
        used = [
            {
                "database": "dataset",
                "table": "ga_sessions",
                "used_columns": ["trafficSource"],
            }
        ]
        result = filter_schemas_by_used_columns(record_schemas, used)
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "trafficSource" in col_names
        assert "trafficSource.campaign" in col_names
        assert "trafficSource.keyword" in col_names

    def test_record_subfields_not_included_for_unrelated_parent(self, record_schemas):
        used = [
            {
                "database": "dataset",
                "table": "ga_sessions",
                "used_columns": ["trafficSource"],
            }
        ]
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
        used = [
            {"database": "dataset", "table": "orders", "used_columns": ["order_id"]}
        ]
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

    def test_duplicate_conditions_deduplicated(self):
        expr = sqlglot.parse_one("a = 1 AND b = 2 AND a = 1", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 2
        sqls = [c.sql() for c in conditions]
        assert sqls.count("a = 1") == 1

    def test_duplicate_order_preserved(self):
        expr = sqlglot.parse_one("a = 1 AND b = 2 AND a = 1 AND c = 3", read="duckdb")
        conditions = _extract_conditions(expr)
        assert len(conditions) == 3
        assert conditions[0].sql() == "a = 1"
        assert conditions[1].sql() == "b = 2"
        assert conditions[2].sql() == "c = 3"


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
        assert _determine_global_status(results) == "error"


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

    def test_missing_sql_returns_error_without_crash(self, con):
        # Régression : un dict d'assertion sans clé `sql` (ex. _Assertion.model_dump()
        # qui n'a que description/expected_condition) produisait con.execute("") → None
        # → "'NoneType' object has no attribute 'fetchdf'". On doit obtenir une erreur
        # explicite, pas un crash opaque.
        con.execute("CREATE VIEW v_result_nosql AS SELECT * FROM __result__")
        results = _evaluate_assertions(
            [{"description": "sans sql", "expected_condition": "amount > 0"}],
            "v_result_nosql",
            con,
        )
        assert results[0]["passed"] is False
        assert "vide" in results[0]["error"]
        assert "NoneType" not in results[0]["error"]

    def test_blank_sql_returns_error_without_crash(self, con):
        con.execute("CREATE VIEW v_result_blank AS SELECT * FROM __result__")
        results = _evaluate_assertions(
            [{"description": "sql vide", "sql": "   "}],
            "v_result_blank",
            con,
        )
        assert results[0]["passed"] is False
        assert "vide" in results[0]["error"]

    def test_assertion_to_executable_derives_sql(self, con):
        # Le chemin assertion_generator convertit _Assertion → dict exécutable via
        # _assertion_to_executable, qui DOIT remplir `sql` à partir de expected_condition.
        a = _Assertion(description="montant positif", expected_condition="amount > 0")
        executable = _assertion_to_executable(a)
        assert executable["sql"].strip()  # non vide
        con.execute("CREATE VIEW v_result_conv AS SELECT * FROM __result__")
        results = _evaluate_assertions([executable], "v_result_conv", con)
        assert "error" not in results[0]
        assert results[0]["passed"] is True

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
# _assertion_sql_from_condition — wrapper "condition positive" → dbt failing-rows
# ---------------------------------------------------------------------------


class TestAssertionSqlFromCondition:
    """Le LLM exprime une condition positive (vérité métier attendue sur chaque
    ligne) ; MockSQL la négocie mécaniquement en requête dbt-style (0 ligne = OK).
    Le LLM n'écrit jamais de `!=`/`NOT` inversé — la négation est gérée ici."""

    def test_wraps_positive_condition(self):
        sql = _assertion_sql_from_condition("date = '2016-01-02'")
        assert sql == "SELECT * FROM __result__ WHERE (date = '2016-01-02') IS NOT TRUE"

    def test_strips_whitespace_and_trailing_semicolon(self):
        sql = _assertion_sql_from_condition("  amount > 0 ;  ")
        assert sql == "SELECT * FROM __result__ WHERE (amount > 0) IS NOT TRUE"

    def test_condition_with_subquery_on_result(self):
        cond = "z_score = (SELECT MAX(z_score) FROM __result__)"
        sql = _assertion_sql_from_condition(cond)
        assert sql == f"SELECT * FROM __result__ WHERE ({cond}) IS NOT TRUE"

    def test_passes_when_all_rows_satisfy(self, con):
        # amount est strictement positif sur toutes les lignes de la fixture
        sql = _assertion_sql_from_condition("amount > 0")
        results = _evaluate_assertions(
            [{"description": "montant positif", "sql": sql}], "__result__", con
        )
        assert results[0]["passed"] is True
        assert results[0]["failing_rows"] == []

    def test_fails_when_some_rows_violate(self, con):
        # value vaut 'b' sur 2 lignes → la condition positive value = 'a' est violée
        sql = _assertion_sql_from_condition("value = 'a'")
        results = _evaluate_assertions(
            [{"description": "value vaut a", "sql": sql}], "__result__", con
        )
        assert results[0]["passed"] is False
        assert len(results[0]["failing_rows"]) == 2

    def test_null_counts_as_violation(self):
        # IS NOT TRUE (et non NOT (...)) : un NULL là où on attend une valeur est une violation
        c = duckdb.connect()
        c.execute("CREATE TABLE __result__ (id INTEGER, value TEXT)")
        c.execute("INSERT INTO __result__ VALUES (1, 'a'), (2, NULL)")
        sql = _assertion_sql_from_condition("value = 'a'")
        results = _evaluate_assertions(
            [{"description": "value vaut a", "sql": sql}], "__result__", c
        )
        assert results[0]["passed"] is False
        assert len(results[0]["failing_rows"]) == 1  # la ligne NULL


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
            "gen_retries": 1,
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


# ---------------------------------------------------------------------------
# _extract_right_key_from_join — doit retourner la colonne de la table JOINTE,
# pas l'opérande syntaxiquement à droite du `=`.
# ---------------------------------------------------------------------------


class TestExtractRightKeyFromJoin:
    @staticmethod
    def _join(sql: str) -> exp.Expression:
        return sqlglot.parse_one(sql, read="bigquery").args["joins"][0]

    def test_joined_col_on_right_operand(self):
        j = self._join("SELECT 1 FROM a LEFT JOIN b ON a.x = b.y")
        col = _extract_right_key_from_join(j)
        assert col.table == "b" and col.name == "y"

    def test_joined_col_on_left_operand(self):
        # Régression : l'ancien code retournait `a.x` (côté base) car c'est
        # l'opérande de droite. Doit retourner `b.y` (la table nouvellement jointe).
        j = self._join("SELECT 1 FROM a LEFT JOIN b ON b.y = a.x")
        col = _extract_right_key_from_join(j)
        assert col.table == "b" and col.name == "y"

    def test_aliased_join_anti_join_key(self):
        j = self._join(
            "SELECT 1 FROM rcomp "
            "LEFT JOIN siret_onus AS onus ON onus.no_siret = rcomp.no_siret"
        )
        col = _extract_right_key_from_join(j)
        assert col.table == "onus" and col.name == "no_siret"


# ---------------------------------------------------------------------------
# _build_count_steps_query — un LEFT JOIN non-matché ne doit PAS être étiqueté
# bloquant ; le vrai bloqueur (anti-join WHERE … IS NULL) doit remonter.
# ---------------------------------------------------------------------------


class TestBuildCountStepsQuery:
    @staticmethod
    def _run(con, cte_code):
        sql, labels = _build_count_steps_query(cte_code, [], "duckdb")
        df = con.execute(sql).fetchdf()
        row = df.iloc[0].to_dict()
        cols = list(row.keys())
        return [(labels[i], int(row[cols[i]])) for i in range(len(labels))]

    def test_left_join_non_match_not_blocking_anti_join_surfaces(self):
        c = duckdb.connect()
        c.execute("CREATE TABLE rcomp (no_siret TEXT)")
        c.execute("INSERT INTO rcomp VALUES ('999')")
        c.execute("CREATE TABLE coface (cosirt TEXT, coapna TEXT)")
        c.execute("INSERT INTO coface VALUES ('111', 'X')")  # ne matche pas rcomp
        c.execute("CREATE TABLE naf (niv5 TEXT)")  # vide → naf.niv5 NULL via LEFT JOIN
        c.execute("CREATE TABLE onus (no_siret TEXT)")
        c.execute("INSERT INTO onus VALUES ('999')")  # matche → anti-join bloque

        cte = """
            SELECT rcomp.no_siret AS s
            FROM rcomp AS rcomp
            LEFT JOIN coface AS coface ON coface.cosirt = rcomp.no_siret
            LEFT JOIN naf AS naf ON coface.coapna = naf.niv5
            LEFT JOIN onus AS onus ON onus.no_siret = rcomp.no_siret
            WHERE onus.no_siret IS NULL
        """
        steps = self._run(c, cte)

        # Aucune étape de JOIN ne doit tomber à 0 : ce sont tous des LEFT JOIN,
        # la ligne de base survit même sans correspondance (naf vide inclus).
        join_steps = [(lbl, cnt) for lbl, cnt in steps if "JOIN" in lbl]
        assert join_steps, steps
        assert all(cnt > 0 for _, cnt in join_steps), join_steps

        # Le vrai bloqueur est l'anti-join WHERE onus.no_siret IS NULL.
        where_zero = [lbl for lbl, cnt in steps if "WHERE" in lbl and cnt == 0]
        assert where_zero and "onus" in where_zero[0], steps

    def test_inner_join_non_match_still_blocking(self):
        # Un INNER JOIN non-matché DOIT rester bloquant (pas de régression).
        c = duckdb.connect()
        c.execute("CREATE TABLE a (x TEXT)")
        c.execute("INSERT INTO a VALUES ('1')")
        c.execute("CREATE TABLE b (y TEXT)")
        c.execute("INSERT INTO b VALUES ('2')")  # ne matche pas

        cte = "SELECT a.x AS x FROM a AS a JOIN b AS b ON b.y = a.x"
        steps = self._run(c, cte)
        join_zero = [lbl for lbl, cnt in steps if "JOIN" in lbl and cnt == 0]
        assert join_zero and "b.y" in join_zero[0], steps


# ---------------------------------------------------------------------------
# _format_cte_trace_hint — ne flaguer « filtre bloquant » que les CTEs
# réellement bloquantes (annotation `blocking`), pas toute CTE vide.
# ---------------------------------------------------------------------------


class TestFormatCteTraceHintBlocking:
    def test_non_blocking_empty_cte_not_flagged(self):
        trace = {
            "opt_dim": {"row_count": 0, "blocking": False},
            "main": {
                "row_count": 0,
                "blocking": True,
                "steps": [
                    {"label": "base", "count": 1},
                    {"label": "+ WHERE x.id IS NULL", "count": 0},
                ],
            },
        }
        out = _format_cte_trace_hint("main", trace)
        # opt_dim est vide mais NON bloquante → pas de marqueur alarmant.
        assert "`opt_dim` : 0 ligne(s)" in out
        assert "opt_dim` : 0 ligne(s) ←" not in out
        # main est bloquante → marqueur + étape bloquante.
        assert "`main` : 0 ligne(s) ← **0 ligne — filtre bloquant**" in out
        assert "étape bloquante" in out

    def test_backward_compat_without_blocking_key(self):
        # Sans clé `blocking` → repli sur l'ancien comportement (row_count == 0).
        trace = {"c": {"row_count": 0}}
        out = _format_cte_trace_hint("c", trace)
        assert "filtre bloquant" in out


# ---------------------------------------------------------------------------
# _select_failing_cte — choisit la CTE réellement bloquante et annote la trace.
# ---------------------------------------------------------------------------


class TestSelectFailingCte:
    def test_picks_blocking_over_optional_empty_cte(self):
        # `opt_dim` est vide mais seulement LEFT-jointe (optionnelle) → non bloquante.
        # `main` (final) est vide et requise → c'est elle qu'il faut cibler.
        query_decomposed = [
            {"name": "opt_dim", "code": "SELECT 1 AS id WHERE 1 = 0"},
            {
                "name": "main",
                "code": (
                    "SELECT base.id AS id FROM base AS base "
                    "LEFT JOIN opt_dim AS opt_dim ON opt_dim.id = base.id "
                    "WHERE base.id IS NULL"
                ),
            },
            {"name": "final_query", "code": "SELECT main.id FROM main AS main"},
        ]
        trace = {
            "opt_dim": {"row_count": 0},
            "main": {"row_count": 0},
        }
        failing = _select_failing_cte(query_decomposed, trace, "duckdb")
        assert failing == "main"
        assert trace["main"].get("blocking") is True
        assert trace["opt_dim"].get("blocking") is False
