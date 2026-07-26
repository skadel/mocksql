"""Small regression checks for provider-aware ``mocksql init`` guidance."""

from pathlib import Path

from typer.testing import CliRunner

from cli.main import app


def test_init_openai_duckdb_does_not_print_vertex_or_bigquery_guidance(
    tmp_path: Path, monkeypatch
) -> None:
    async def _no_database_setup() -> None:
        return None

    import init.init_db

    monkeypatch.setattr(init.init_db, "main", _no_database_setup)
    result = CliRunner().invoke(
        app,
        [
            "init",
            "--path",
            str(tmp_path),
            "--dialect",
            "duckdb",
            "--llm-provider",
            "openai",
            "--non-interactive",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "OPENAI_API_KEY" in result.output
    assert "VERTEX_PROJECT" not in result.output
    assert "BQ_TEST_PROJECT" not in result.output
    assert "schema_cache first" in result.output
    assert "test_dataset:" not in (tmp_path / "mocksql.yml").read_text(encoding="utf-8")


def test_init_vertex_bigquery_prints_bigquery_guidance(
    tmp_path: Path, monkeypatch
) -> None:
    async def _no_database_setup() -> None:
        return None

    import init.init_db

    monkeypatch.setattr(init.init_db, "main", _no_database_setup)
    result = CliRunner().invoke(
        app,
        [
            "init",
            "--path",
            str(tmp_path),
            "--dialect",
            "bigquery",
            "--llm-provider",
            "vertexai",
            "--non-interactive",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "VERTEX_PROJECT" in result.output
    assert "BQ_TEST_PROJECT" in result.output
    assert "OPENAI_API_KEY" not in result.output
