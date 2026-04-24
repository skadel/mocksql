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

    models_path = typer.prompt(
        "Path to your SQL models folder",
        default="./models",
    )

    dbt_project = typer.confirm("Is this a dbt project?", default=False)
    compiled_path: str | None = None
    if dbt_project:
        compiled_path = typer.prompt(
            "Path to dbt compiled folder",
            default="./target/compiled",
        )

    llm_provider = typer.prompt(
        f"LLM provider ({'/'.join(LLM_PROVIDERS)})",
        default="vertexai",
    )
    while llm_provider not in LLM_PROVIDERS:
        typer.echo(f"Invalid provider. Choose from: {', '.join(LLM_PROVIDERS)}")
        llm_provider = typer.prompt(
            f"LLM provider ({'/'.join(LLM_PROVIDERS)})", default="vertexai"
        )

    config: dict = {
        "version": "2",
        "dialect": dialect,
        "models_path": models_path,
        "llm": {
            "provider": llm_provider,
        },
        "schema_cache": ".mocksql/schema_cache.json",
    }

    if compiled_path:
        config["compiled_path"] = compiled_path

    path.mkdir(parents=True, exist_ok=True)
    (path / "data").mkdir(exist_ok=True)

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    typer.echo(f"\n[OK] {config_path} created.")
    typer.echo(
        "\nRequired environment variables:\n"
        "  PROJECT_ID=<your-gcp-project>\n"
        "  GOOGLE_CLOUD_PROJECT=<your-gcp-project>\n"
        "  GOOGLE_CLOUD_LOCATION=us-central1\n"
        "  DUCKDB_PATH=data/mocksql.duckdb\n"
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
def ui(
    port: int = typer.Option(8080, "--port", "-p", help="Port for the MockSQL server."),
    no_browser: bool = typer.Option(
        False, "--no-browser", help="Don't open browser automatically."
    ),
) -> None:
    """Start the MockSQL server and open the web UI."""
    import os
    import threading
    import webbrowser

    import uvicorn

    try:
        import mocksql_ui as _ui_pkg
    except ImportError:
        _ui_pkg = None

    if _ui_pkg is not None:
        static_dir = Path(_ui_pkg.__file__).parent / "static"
        server_module = "mocksql_ui.server:app"
    else:
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

    base_url = f"http://localhost:{port}"
    os.environ.setdefault("FRONT_URL", base_url)

    url = f"{base_url}/static/"
    typer.echo(f"Starting MockSQL at {url} ...")

    if not no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    uvicorn.run(server_module, host="0.0.0.0", port=port)


def main() -> None:
    app()
