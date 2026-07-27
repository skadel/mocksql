"""Regression coverage for cache-only DuckDB CLI generation."""

import json
from pathlib import Path

import pytest

from utils.examples import (
    create_test_tables,
    filter_columns,
    initialize_duckdb,
    run_query_on_test_dataset,
)
from utils.sql_code import extract_used_columns_from_sql


@pytest.mark.asyncio
async def test_cache_only_duckdb_unqualified_table_reaches_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A cached ``FROM orders`` must produce used_columns before graph routing."""
    from cli import generate

    config = tmp_path / "mocksql.yml"
    config.write_text(
        "dialect: duckdb\nprovider: openai\nmodels_path: ./models\n",
        encoding="utf-8",
    )
    model = tmp_path / "models" / "orders.sql"
    model.parent.mkdir()
    model.write_text(
        "SELECT customer_id, amount FROM orders WHERE amount > 0", encoding="utf-8"
    )
    generate.save_schema_cache(
        str(tmp_path / ".mocksql" / "schema_cache.json"),
        [
            {
                "table_name": "orders",
                "columns": [
                    {"name": "customer_id", "type": "TEXT"},
                    {"name": "amount", "type": "NUMBER"},
                ],
            }
        ],
    )

    async def noop(*_args, **_kwargs):
        return None

    captured = {}

    class FakeGraph:
        async def ainvoke(self, state, config):
            captured.update(state)
            return {"messages": [], "error": ""}

    monkeypatch.setattr("models.env_variables.validate_required_env", lambda: None)
    monkeypatch.setattr("models.database.db_pool.init_pool", noop)
    monkeypatch.setattr("init.init_db.run_migrations", noop)
    monkeypatch.setattr(
        "build_query.query_chain.build_query_graph", lambda: FakeGraph()
    )

    await generate.run_generate(model, config, tmp_path / ".mocksql" / "tests")

    used = [json.loads(item) for item in captured["used_columns"]]
    assert used == [
        {
            "project": "",
            "database": "",
            "table": "orders",
            "used_columns": ["amount", "customer_id"],
        }
    ]


def test_filter_columns_accepts_unqualified_duckdb_cache_table():
    assert filter_columns(
        [
            {
                "table_name": "orders",
                "columns": [
                    {"name": "customer_id", "type": "TEXT"},
                    {"name": "amount", "type": "NUMBER"},
                ],
            }
        ],
        [{"database": "", "table": "orders", "used_columns": ["amount"]}],
    ) == [
        {
            "table_name": "orders",
            "columns": [{"name": "amount", "type": "NUMBER"}],
        }
    ]


@pytest.mark.asyncio
async def test_unqualified_duckdb_cache_table_executes_against_suffixed_fixture():
    schemas = [
        {
            "table_name": "orders",
            "columns": [{"name": "id", "type": "INTEGER"}],
        }
    ]

    with initialize_duckdb(":memory:") as con:
        create_test_tables(schemas, "case1", con, "duckdb")
        con.execute("INSERT INTO orders_case1 VALUES (42)")

        result, executed_sql = await run_query_on_test_dataset(
            "SELECT id FROM orders",
            "case1",
            "project",
            "duckdb",
            con,
        )

    assert result.to_dict("records") == [{"id": 42}]
    assert "orders_case1" in executed_sql


def test_unqualified_cte_matching_cached_table_is_not_a_source():
    schemas = [
        {
            "table_name": "orders",
            "columns": [{"name": "id", "type": "INTEGER"}],
        }
    ]

    used = extract_used_columns_from_sql(
        "WITH orders AS (SELECT 1 AS id) SELECT id FROM orders",
        "duckdb",
        schemas,
    )

    assert used == []


def test_same_name_physical_table_inside_cte_remains_a_source():
    schemas = [
        {
            "table_name": "orders",
            "columns": [{"name": "id", "type": "INTEGER"}],
        }
    ]

    used = [
        json.loads(item)
        for item in extract_used_columns_from_sql(
            "WITH orders AS (SELECT id FROM orders) SELECT id FROM orders",
            "duckdb",
            schemas,
        )
    ]

    assert used == [
        {
            "project": "",
            "database": "",
            "table": "orders",
            "used_columns": ["id"],
        }
    ]
