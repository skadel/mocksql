"""
Unit tests for the "query understanding" card payload builder.

Pure-Python — no DB, no LLM. Exercises _build_understanding_payload, which feeds
the persistent "Ce que MockSQL a compris" card in the frontend.
"""

from build_query.examples_generator import _build_understanding_payload


def _used_columns():
    return [
        {
            "database": "",
            "table": "orders",
            "used_columns": ["id", "customer_id", "amount"],
        },
        {"database": "", "table": "customers", "used_columns": ["id", "name"]},
    ]


class TestBuildUnderstandingPayload:
    def test_multi_table_query_yields_tables_and_constraints(self):
        sql = (
            "SELECT o.id, SAFE_CAST(o.amount AS INT64) AS amt, c.name "
            "FROM orders o JOIN customers c ON o.customer_id = c.id "
            "WHERE o.amount > 0"
        )
        state = {"dialect": "duckdb", "optimized_sql": sql}

        payload = _build_understanding_payload(state, _used_columns())

        assert payload is not None
        tables = {t["table"] for t in payload["tables"]}
        assert tables == {"orders", "customers"}
        # constraints come from _branch_to_dict (joins / filters / anti_joins / referenced)
        assert isinstance(payload["constraints"], dict)
        # SAFE_CAST is a non-trivial derived expression → must be surfaced
        assert any(
            "SAFE_CAST" in d["expr"].upper() or "TRY_CAST" in d["expr"].upper()
            for d in payload["derived_expressions"]
        )
        assert payload["optimized_sql"] == sql

    def test_falls_back_to_query_when_no_optimized_sql(self):
        state = {"dialect": "duckdb", "query": "SELECT id FROM orders o WHERE o.id > 5"}
        payload = _build_understanding_payload(
            state, [{"database": "", "table": "orders", "used_columns": ["id"]}]
        )
        assert payload is not None
        assert payload["optimized_sql"] == state["query"]

    def test_empty_input_returns_none(self):
        payload = _build_understanding_payload(
            {"dialect": "duckdb", "optimized_sql": ""}, []
        )
        assert payload is None

    def test_garbage_sql_never_raises(self):
        # Best-effort contract: a card must never break generation.
        state = {"dialect": "duckdb", "optimized_sql": "NOT A VALID )))( SQL"}
        payload = _build_understanding_payload(
            state, [{"database": "", "table": "orders", "used_columns": ["id"]}]
        )
        # tables still present (from used_columns); constraints/derived simply empty
        assert payload is not None
        assert payload["tables"][0]["table"] == "orders"
