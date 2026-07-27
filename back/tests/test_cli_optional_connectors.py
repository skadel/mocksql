"""Optional source connectors must fail early with an actionable CLI error."""

from pathlib import Path

import pytest
import typer


async def _noop(*_args, **_kwargs) -> None:
    return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dialect", "sql", "extra"),
    [
        (
            "bigquery",
            "SELECT order_id FROM `demo-project.analytics.orders`",
            "mocksql[bigquery]",
        ),
        (
            "snowflake",
            "SELECT ORDER_ID FROM ANALYTICS.PUBLIC.ORDERS",
            "mocksql[snowflake]",
        ),
    ],
)
async def test_generate_cache_miss_reports_missing_connector_without_traceback(
    dialect: str,
    sql: str,
    extra: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    from cli import generate

    config = tmp_path / "mocksql.yml"
    config.write_text(f"dialect: {dialect}\nmodels_path: ./models\n", encoding="utf-8")
    model = tmp_path / "models" / "orders.sql"
    model.parent.mkdir()
    model.write_text(sql, encoding="utf-8")

    def missing_connector():
        raise ImportError(f"Connecteur absent. Installez l'extra : pip install {extra}")

    monkeypatch.setattr("models.env_variables.validate_required_env", lambda: None)
    monkeypatch.setattr("models.database.db_pool.init_pool", _noop)
    monkeypatch.setattr("init.init_db.run_migrations", _noop)
    monkeypatch.setattr(f"utils.optional_deps.import_{dialect}", missing_connector)
    monkeypatch.setattr(
        generate,
        "fetch_tables_schema",
        lambda *_args, **_kwargs: pytest.fail(
            "BigQuery must not be called after connector preflight fails"
        ),
    )

    with pytest.raises(typer.Exit) as exc:
        await generate.run_generate(model, config, tmp_path / ".mocksql" / "tests")

    output = capfd.readouterr()
    combined = output.out + output.err
    assert exc.value.exit_code == 1
    assert f"pip install {extra}" in combined
    assert "Traceback" not in combined
    other_extra = "mocksql[snowflake]" if dialect == "bigquery" else "mocksql[bigquery]"
    assert other_extra not in combined
