import asyncio
import os
from pathlib import Path

import typer
import yaml

app = typer.Typer(
    name="mocksql",
    help="MockSQL — TDD engine for Analytics Engineering.",
    no_args_is_help=True,
)


@app.callback()
def _callback() -> None:
    """MockSQL — TDD engine for Analytics Engineering."""


CONFIG_FILE = "mocksql.yml"

DIALECTS = ["bigquery", "postgres"]
LLM_PROVIDERS = ["vertexai", "openai"]

_SKIP_DIRS = {".venv", "venv", "node_modules", "__pycache__", ".git", ".tox"}
_SQL_HINTS = {"models", "jobs", "target"}


def _contains_sql(folder: Path) -> bool:
    try:
        return any(folder.rglob("*.sql"))
    except PermissionError:
        return False


def _find_models_candidates(root: Path) -> list[Path]:
    candidates: list[Path] = []
    try:
        for entry in sorted(root.iterdir()):
            if (
                not entry.is_dir()
                or entry.name.startswith(".")
                or entry.name in _SKIP_DIRS
            ):
                continue
            if entry.name in _SQL_HINTS:
                if _contains_sql(entry):
                    candidates.append(entry)
            else:
                for hint in _SQL_HINTS:
                    nested = entry / hint
                    if nested.is_dir() and _contains_sql(nested):
                        candidates.append(nested)
    except PermissionError:
        pass
    return candidates


def _prompt_models_path(root: Path) -> str:
    candidates = _find_models_candidates(root)
    if not candidates:
        return typer.prompt("Path to your SQL models folder", default="./models")

    typer.echo("\nSQL model folders found:")
    for i, c in enumerate(candidates, 1):
        typer.echo(f"  [{i}] {c}")
    typer.echo("  [0] Enter a custom path")

    default_display = str(candidates[0])
    raw = typer.prompt(
        "Pick a folder (number or path)",
        default=default_display,
    )
    try:
        idx = int(raw)
        if idx == 0:
            return typer.prompt("Custom path", default="./models")
        return str(candidates[idx - 1])
    except (ValueError, IndexError):
        return raw


@app.command()
def init(
    path: Path = typer.Option(
        Path("."),
        "--path",
        "-p",
        help="Directory where mocksql.yml will be created.",
    ),
) -> None:
    """Initialize a MockSQL project and generate mocksql.yml."""
    config_path = path / CONFIG_FILE

    if config_path.exists():
        overwrite = typer.confirm(
            f"{CONFIG_FILE} already exists. Overwrite?", default=False
        )
        if not overwrite:
            typer.echo("Aborted.")
            raise typer.Exit()

    dialect = typer.prompt(
        f"SQL dialect ({'/'.join(DIALECTS)})",
        default="bigquery",
    )
    while dialect not in DIALECTS:
        typer.echo(f"Invalid dialect. Choose from: {', '.join(DIALECTS)}")
        dialect = typer.prompt(
            f"SQL dialect ({'/'.join(DIALECTS)})", default="bigquery"
        )

    models_path = _prompt_models_path(path)

    llm_provider = typer.prompt(
        f"LLM provider ({'/'.join(LLM_PROVIDERS)})",
        default="vertexai",
    )
    while llm_provider not in LLM_PROVIDERS:
        typer.echo(f"Invalid provider. Choose from: {', '.join(LLM_PROVIDERS)}")
        llm_provider = typer.prompt(
            f"LLM provider ({'/'.join(LLM_PROVIDERS)})", default="vertexai"
        )

    duckdb_path = str((path / "data" / "mocksql.duckdb").resolve())

    test_dataset = typer.prompt(
        "BigQuery test dataset (where temp tables are created during validation)",
        default="test_dataset",
    )

    langchain_api_key = (
        typer.prompt(
            "LangSmith API key (LANGCHAIN_API_KEY, optional — press Enter to skip)",
            default="",
        ).strip()
        or None
    )

    config: dict = {
        "version": "2",
        "dialect": dialect,
        "models_path": models_path,
        "duckdb_path": duckdb_path,
        "llm": {
            "provider": llm_provider,
        },
        "schema_cache": ".mocksql/schema_cache.json",
        "test_dataset": test_dataset,
        "langchain_tracing": bool(langchain_api_key),
    }

    if langchain_api_key:
        config["langchain_api_key"] = langchain_api_key

    path.mkdir(parents=True, exist_ok=True)
    (path / "data").mkdir(exist_ok=True)

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    typer.echo(f"\n[OK] {config_path} created.")

    os.environ["DUCKDB_PATH"] = duckdb_path

    typer.echo("[DB] Initializing local database...")
    from init.init_db import main as init_db_main

    asyncio.run(init_db_main())
    typer.echo(f"[DB] Database ready at {duckdb_path}")

    typer.echo(
        "\nRequired environment variables:\n"
        "  VERTEX_PROJECT=<gcp-project-for-vertex-ai>\n"
        "  BQ_TEST_PROJECT=<gcp-project-for-bigquery>  # defaults to VERTEX_PROJECT if not set\n"
        "  GOOGLE_CLOUD_LOCATION=us-central1\n"
        f"  DUCKDB_PATH={duckdb_path}\n"
        "\nNext step: mocksql generate <your_model.sql>"
    )


@app.command()
def generate(
    model: Path = typer.Argument(..., help="Path to the .sql model file."),
    config: Path = typer.Option(
        Path("mocksql.yml"),
        "--config",
        "-c",
        help="Path to mocksql.yml config.",
    ),
    output_dir: Path = typer.Option(
        Path(".mocksql/tests"),
        "--output",
        "-o",
        help="Directory where test files will be written.",
    ),
) -> None:
    """Parse a SQL model, fetch missing schemas, and generate test data."""
    import asyncio

    from cli.generate import run_generate

    asyncio.run(run_generate(model, config, output_dir))


@app.command()
def test(
    model: list[str] = typer.Option(
        [],
        "--model",
        "-m",
        help="Only run tests for this model (repeatable, e.g. -m orders -m customers). Default: all.",
    ),
    config: Path = typer.Option(
        Path("mocksql.yml"),
        "--config",
        "-c",
        help="Path to mocksql.yml config.",
    ),
    output_json: bool = typer.Option(
        False, "--json", help="Output results as JSON (useful for CI pipelines)."
    ),
    fail_fast: bool = typer.Option(
        False, "--fail-fast", "-x", help="Stop after the first failing test."
    ),
) -> None:
    """Re-run saved test cases against DuckDB. No LLM calls. Exits 1 if any test fails."""
    import asyncio
    import json as _json

    from cli.test_runner import run_tests

    model_filters = list(model) or None
    exit_code, model_results = asyncio.run(run_tests(config, model_filters, fail_fast))

    if output_json:
        typer.echo(_json.dumps(model_results, indent=2, default=str))
    else:
        _print_test_results(model_results)

    raise typer.Exit(exit_code)


def _print_test_results(model_results: list) -> None:
    import json as _json

    if not model_results:
        typer.echo("No tests found. Run `mocksql generate <model.sql>` first.")
        return

    total = passed = failed = skipped = 0

    for mr in model_results:
        cases = mr["cases"]
        n_pass = sum(1 for c in cases if c["status"] == "pass")
        n_fail = sum(1 for c in cases if c["status"] in ("fail", "error"))
        n_skip = sum(1 for c in cases if c["status"] == "skip")
        total += len(cases)
        passed += n_pass
        failed += n_fail
        skipped += n_skip

        icon = "✓" if n_fail == 0 else "✗"
        skip_label = f", {n_skip} skipped" if n_skip else ""
        typer.echo(
            f"\n  {icon} {mr['model']}  ({n_pass}/{len(cases)} passed{skip_label})"
        )

        for c in cases:
            status = c["status"]
            label = {"pass": "PASS", "fail": "FAIL", "error": "ERR ", "skip": "SKIP"}[
                status
            ]
            typer.echo(f"  [{label}] {c.get('name', f'Test {c['index']}')}")

            if status in ("fail", "error"):
                if c.get("error"):
                    typer.echo(f"           error: {c['error']}")
                for a in c.get("assertions", []):
                    if not a.get("passed"):
                        typer.echo(
                            f"           assertion: {a.get('description', '(unnamed)')}"
                        )
                        if a.get("error"):
                            typer.echo(f"           sql error: {a['error']}")
                        elif a.get("failing_rows"):
                            rows_preview = _json.dumps(
                                a["failing_rows"][:3], default=str
                            )
                            typer.echo(
                                f"           failing rows (first 3): {rows_preview}"
                            )

            elif status == "skip":
                typer.echo(f"           skipped: {c.get('reason', '')}")

    typer.echo(f"\n  {'─' * 52}")
    skip_summary = f", {skipped} skipped" if skipped else ""
    typer.echo(f"  Results: {passed}/{total} tests passed{skip_summary}")
    if failed:
        typer.echo(f"  {failed} test(s) FAILED — exit code 1")


@app.command()
def ui(
    port: int = typer.Option(8080, "--port", "-p", help="Port for the MockSQL server."),
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Don't open browser automatically."
    ),
) -> None:
    """Start the MockSQL server and open the web UI."""
    import threading
    import webbrowser

    import uvicorn

    static_dir = Path(__file__).parent.parent / "static"
    server_module = "server:app"

    if not (static_dir / "index.html").exists():
        typer.echo(
            "[ERROR] UI not installed.\n"
            "  From source : cd back && make build-ui\n"
            "  From wheel  : pip install mocksql-ui",
            err=True,
        )
        raise typer.Exit(1)

    config_path = Path(CONFIG_FILE)
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f) or {}
        if duckdb_path_from_cfg := cfg.get("duckdb_path"):
            os.environ.setdefault("DUCKDB_PATH", duckdb_path_from_cfg)
        if test_dataset_from_cfg := cfg.get("test_dataset"):
            os.environ.setdefault("BQ_TEST_DATASET", test_dataset_from_cfg)
        if langchain_api_key_from_cfg := cfg.get("langchain_api_key"):
            os.environ.setdefault("LANGCHAIN_API_KEY", langchain_api_key_from_cfg)
        tracing = cfg.get("langchain_tracing", bool(cfg.get("langchain_api_key")))
        os.environ.setdefault("LANGCHAIN_TRACING_V2", "true" if tracing else "false")

    base_url = f"http://localhost:{port}"
    os.environ.setdefault("FRONT_URL", base_url)

    url = f"{base_url}/static/"
    typer.echo(f"Starting MockSQL at {url} ...")

    if not no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    uvicorn.run(server_module, host="0.0.0.0", port=port)


def main() -> None:
    app()
