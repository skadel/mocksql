"""Snowflake CLI schema import must never route through BigQuery."""

from pathlib import Path

import pytest


SF_ROWS = [
    {
        "table_catalog": "ANALYTICS",
        "table_schema": "PUBLIC",
        "table_name": "ORDERS",
        "field_path": "ID",
        "data_type": "NUMBER(38,0)",
        "mode": "REQUIRED",
        "description": "",
    }
]


def _snowflake_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BQ_TEST_PROJECT", raising=False)
    monkeypatch.delenv("VERTEX_PROJECT", raising=False)
    for name, value in {
        "SNOWFLAKE_ACCOUNT": "org-account",
        "SNOWFLAKE_USER": "mocksql",
        "SNOWFLAKE_PASSWORD": "secret",
        "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH",
        "SNOWFLAKE_DATABASE": "ANALYTICS",
    }.items():
        monkeypatch.setenv(name, value)


def test_snowflake_configuration_error_lists_missing_settings(
    monkeypatch: pytest.MonkeyPatch,
):
    from models.env_variables import validate_snowflake_env

    for name in (
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
    ):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(RuntimeError, match="Configuration Snowflake incomplète") as exc:
        validate_snowflake_env()
    assert "SNOWFLAKE_ACCOUNT" in str(exc.value)
    assert "pip install mocksql[snowflake]" in str(exc.value)


def test_snowflake_database_is_optional_for_fully_qualified_refs(
    monkeypatch: pytest.MonkeyPatch,
):
    from models.env_variables import validate_snowflake_env

    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_DATABASE")

    validate_snowflake_env(["ANALYTICS.PUBLIC.ORDERS"])


def test_snowflake_database_is_required_for_two_part_refs(
    monkeypatch: pytest.MonkeyPatch,
):
    from models.env_variables import validate_snowflake_env

    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_DATABASE")

    with pytest.raises(RuntimeError, match="SNOWFLAKE_DATABASE"):
        validate_snowflake_env(["PUBLIC.ORDERS"])


def test_snowflake_connection_omits_empty_database_and_schema(
    monkeypatch: pytest.MonkeyPatch,
):
    import utils.snowflake_connector as connector

    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_DATABASE")
    monkeypatch.setattr(connector, "SNOWFLAKE_DATABASE", "")
    monkeypatch.setattr(connector, "_sf_conn", None)
    captured = {}

    class Connection:
        def is_closed(self):
            return False

    class FakeConnector:
        @staticmethod
        def connect(**kwargs):
            captured.update(kwargs)
            return Connection()

    monkeypatch.setattr(connector, "_import_snowflake", lambda: FakeConnector)

    connector.get_sf_connection()

    assert "database" not in captured
    assert "schema" not in captured


@pytest.mark.asyncio
async def test_generate_fetches_missing_snowflake_schema_without_bigquery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The generate import branch uses the Snowflake fetcher with no BQ project."""
    from cli import generate

    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_DATABASE")
    config = tmp_path / "mocksql.yml"
    config.write_text("dialect: snowflake\nmodels_path: ./models\n", encoding="utf-8")
    model = tmp_path / "models" / "orders.sql"
    model.parent.mkdir()
    model.write_text("SELECT ID FROM ANALYTICS.PUBLIC.ORDERS", encoding="utf-8")

    async def fake_sf_fetch(refs):
        assert refs == ["ANALYTICS.PUBLIC.ORDERS"]
        return SF_ROWS, []

    async def no_bigquery(*_args, **_kwargs):
        raise AssertionError("BigQuery must not be called for dialect: snowflake")

    async def noop(*_args, **_kwargs):
        return None

    class StopAfterImport(Exception):
        pass

    monkeypatch.setattr("models.env_variables.validate_required_env", lambda: None)
    monkeypatch.setattr(
        "build_query.schema_fetcher.fetch_tables_schema_snowflake", fake_sf_fetch
    )
    monkeypatch.setattr(generate, "fetch_tables_schema", no_bigquery)
    monkeypatch.setattr("models.database.db_pool.init_pool", noop)
    monkeypatch.setattr("init.init_db.run_migrations", noop)
    monkeypatch.setattr(
        generate,
        "build_initial_state",
        lambda *_args: (_ for _ in ()).throw(StopAfterImport()),
    )

    with pytest.raises(StopAfterImport):
        await generate.run_generate(model, config, tmp_path / ".mocksql" / "tests")

    cached = generate.load_schema_cache(
        str(tmp_path / ".mocksql" / "schema_cache.json")
    )
    assert cached[0]["table_name"] == "ANALYTICS.PUBLIC.ORDERS"


def test_refresh_schemas_fetches_snowflake_without_bigquery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """refresh-schemas has a first-class Snowflake branch, like Trino."""
    from cli import main
    from cli.generate import save_schema_cache

    _snowflake_env(monkeypatch)
    monkeypatch.delenv("SNOWFLAKE_DATABASE")
    config = tmp_path / "mocksql.yml"
    config.write_text("dialect: snowflake\n", encoding="utf-8")
    save_schema_cache(
        str(tmp_path / ".mocksql" / "schema_cache.json"),
        [{"table_name": "ANALYTICS.PUBLIC.ORDERS", "columns": []}],
    )

    async def fake_sf_fetch(refs):
        assert refs == ["ANALYTICS.PUBLIC.ORDERS"]
        return SF_ROWS, []

    def no_required_env():
        raise AssertionError(
            "refresh-schemas Snowflake must not require Vertex/BigQuery"
        )

    async def no_bigquery(*_args, **_kwargs):
        raise AssertionError("BigQuery must not be called for dialect: snowflake")

    monkeypatch.setattr(
        "build_query.schema_fetcher.fetch_tables_schema_snowflake", fake_sf_fetch
    )
    monkeypatch.setattr("build_query.schema_fetcher.fetch_tables_schema", no_bigquery)
    monkeypatch.setattr("models.env_variables.validate_required_env", no_required_env)

    main.refresh_schemas(config=config, tables=[], from_tests=False)

    from cli.generate import load_schema_cache

    cached = load_schema_cache(str(tmp_path / ".mocksql" / "schema_cache.json"))
    assert cached[0]["columns"][0]["name"] == "ID"
