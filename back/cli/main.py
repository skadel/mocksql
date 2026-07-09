import asyncio
import logging
import os
import sys
from pathlib import Path

import typer
import yaml
from dotenv import load_dotenv

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[union-attr]

import utils.logger  # noqa: F401 — registers DIAG level (15) before basicConfig

app = typer.Typer(
    name="mocksql",
    help="MockSQL — TDD engine for Analytics Engineering.",
    no_args_is_help=True,
)


@app.callback()
def _callback() -> None:
    """MockSQL — TDD engine for Analytics Engineering."""
    load_dotenv(dotenv_path=Path(".env"))
    raw = os.getenv("LOG_LEVEL", "WARNING").upper()
    try:
        log_level: int | str = int(raw)
    except ValueError:
        log_level = raw
    logging.basicConfig(level=log_level, format="%(name)s %(levelname)s %(message)s")


CONFIG_FILE = "mocksql.yml"

DIALECTS = ["bigquery", "postgres", "snowflake", "trino", "duckdb"]
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

    from storage.config import ensure_mocksql_dir

    mocksql_dir = (path / ".mocksql").resolve()
    ensure_mocksql_dir(mocksql_dir)
    (mocksql_dir / "data").mkdir(exist_ok=True)
    duckdb_path = str(mocksql_dir / "data" / "mocksql.duckdb")

    with open(config_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    typer.echo(f"\n[OK] {config_path} created.")

    os.environ["DUCKDB_PATH"] = duckdb_path

    typer.echo("[DB] Initializing local database...")
    from init.init_db import main as init_db_main

    asyncio.run(init_db_main())
    typer.echo(f"[DB] Database ready at {duckdb_path}")

    typer.echo(
        "\nRequired environment variables (env var or .env file at project root):\n"
        "  VERTEX_PROJECT=<gcp-project>          # Vertex AI / LLM\n"
        "  GOOGLE_CLOUD_LOCATION=us-central1\n"
        "  BQ_TEST_PROJECT=<gcp-project>         # optional, defaults to VERTEX_PROJECT\n"
        "\nNext step: mocksql generate <your_model.sql>"
    )


@app.command()
def profile(
    model: Path = typer.Argument(..., help="Path to the .sql model file."),
    config: Path = typer.Option(
        Path("mocksql.yml"),
        "--config",
        "-c",
        help="Path to mocksql.yml config.",
    ),
    output_dir: Path = typer.Option(
        Path(".mocksql/profiles"),
        "--output",
        "-o",
        help="Directory where profile JSON files will be written.",
    ),
) -> None:
    """Run BigQuery profiling on a SQL model and save the profile JSON."""
    import asyncio

    from cli.profile import run_profile

    config = config.resolve()
    if not output_dir.is_absolute():
        output_dir = (config.parent / output_dir).resolve()
    asyncio.run(run_profile(model, config, output_dir))


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
    profile: bool = typer.Option(
        False,
        "--profile",
        help="Run BigQuery profiling before generation to improve data quality.",
    ),
    overwrite: bool = typer.Option(
        False,
        "--overwrite",
        help="Rebuild the whole suite from scratch (DESTRUCTIVE: drops existing tests "
        "and assert specs). Default is additive — generate only ever adds a test.",
    ),
    instruction: str = typer.Option(
        None,
        "--instruction",
        "-i",
        help="Natural-language scenario for the test to add, e.g. "
        '"un client avec deux cartes → trajet dupliqué". Additive mode only.',
    ),
) -> None:
    """Parse a SQL model, fetch missing schemas, and generate test data.

    Additive by default: if a suite already exists, generate ADDS a test (targeted
    by -i/--instruction) and preserves existing tests + assert specs. Use --overwrite
    to rebuild the full suite from scratch.
    """
    import asyncio

    from cli.generate import run_generate

    config = config.resolve()
    if not output_dir.is_absolute():
        output_dir = (config.parent / output_dir).resolve()
    asyncio.run(
        run_generate(
            model,
            config,
            output_dir,
            profile=profile,
            overwrite=overwrite,
            instruction=instruction,
        )
    )


@app.command("update-test")
def update_test(
    model: Path = typer.Argument(..., help="Path to the .sql model file."),
    test_uid: str = typer.Option(
        ..., "--test-uid", "-u", help="test_uid of the existing test to modify."
    ),
    instruction: str = typer.Option(
        ...,
        "--instruction",
        "-i",
        help='What to change, e.g. "ajoute une ligne : un client avec 2 cartes".',
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
    output_dir: Path = typer.Option(Path(".mocksql/tests"), "--output", "-o"),
) -> None:
    """Modify an existing test via the LLM (add/edit data). Preserves assert specs.

    Unlike `generate` (which only ADDS a test), update-test targets one existing test
    by test_uid and lets the agent change its data, then re-runs it. Spec assertions
    (added via `mocksql assert`) are carried over untouched.
    """
    import asyncio

    from cli.generate import run_generate

    config = config.resolve()
    if not output_dir.is_absolute():
        output_dir = (config.parent / output_dir).resolve()
    asyncio.run(
        run_generate(
            model,
            config,
            output_dir,
            instruction=instruction,
            update_uid=test_uid,
        )
    )


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
    frozen: bool = typer.Option(
        False,
        "--frozen",
        help="Replay the SQL snapshot frozen in the test JSON instead of the live "
        ".sql source file (default: read from disk).",
    ),
) -> None:
    """Re-run saved test cases against DuckDB. No LLM calls. Exits 1 if any test fails.

    By default the SQL is read fresh from the source .sql file (so it reflects your
    latest edits — the basis of the agent fix loop). Use --frozen to replay the
    snapshot stored at generate time.
    """
    import asyncio
    import json as _json

    from cli.test_runner import run_tests

    model_filters = list(model) or None
    exit_code, model_results = asyncio.run(
        run_tests(config, model_filters, fail_fast, frozen=frozen)
    )

    if output_json:
        typer.echo(_json.dumps(model_results, indent=2, default=str))
    else:
        _print_test_results(model_results)
        for mr in model_results:
            if mr.get("sql_source") == "snapshot-fallback":
                typer.echo(
                    f"  [WARN] {mr['model']}: source .sql introuvable — rejoué sur le "
                    "snapshot figé du JSON (dérive possible).",
                    err=True,
                )

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
            idx = c.get("index", "?")
            title = c.get("name") or f"Test {idx}"
            typer.echo(f"  [{label}] {title}")
            # Description complète en sous-ligne quand elle apporte plus que le titre.
            desc = c.get("description")
            if desc and desc != title:
                typer.echo(f"           {desc}")

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
def check(
    model: list[str] = typer.Option(
        [],
        "--model",
        "-m",
        help="Only check these models (repeatable). Default: all.",
    ),
    config: Path = typer.Option(
        Path("mocksql.yml"),
        "--config",
        "-c",
        help="Path to mocksql.yml config.",
    ),
    strict: bool = typer.Option(
        False,
        "--strict",
        help="Also fail when a model has no tests at all.",
    ),
    output_json: bool = typer.Option(
        False, "--json", help="Output results as JSON (useful for CI pipelines)."
    ),
) -> None:
    """Check that all SQL models have up-to-date test files. Exits 1 if any model is stale."""
    import json as _json

    from cli.checker import check_models

    model_filters = list(model) or None
    exit_code, results = check_models(config, model_filters, strict=strict)

    if output_json:
        typer.echo(_json.dumps(results, indent=2))
    else:
        _print_check_results(results)

    raise typer.Exit(exit_code)


def _print_check_results(results: list[dict]) -> None:
    if not results:
        typer.echo("No SQL models found. Check your models_path in mocksql.yml.")
        return

    icons = {"ok": "✓", "stale": "✗", "missing": "─"}
    n_stale = sum(1 for r in results if r["status"] == "stale")
    n_missing = sum(1 for r in results if r["status"] == "missing")

    for r in results:
        icon = icons.get(r["status"], "?")
        detail = f"  {r['detail']}" if r["detail"] else ""
        typer.echo(f"  {icon}  {r['model']:<40}{detail}")

    typer.echo(f"\n  {'─' * 52}")
    if n_stale == 0 and n_missing == 0:
        typer.echo(f"  All {len(results)} model(s) up to date.")
    else:
        if n_stale:
            typer.echo(
                f"  {n_stale} model(s) stale — run `mocksql generate <model.sql>` — exit code 1"
            )
        if n_missing:
            typer.echo(
                f"  {n_missing} model(s) with no tests — run `mocksql generate <model.sql>`"
            )


assert_app = typer.Typer(
    name="assert",
    help="Manage assertions (specs) on a test case — list/add/update/remove.",
    no_args_is_help=True,
)
app.add_typer(assert_app, name="assert")


def _emit(payload: dict) -> None:
    import json as _json

    typer.echo(_json.dumps(payload, indent=2, ensure_ascii=False, default=str))


@assert_app.command("list")
def assert_list(
    model: str = typer.Argument(
        ..., help="Model name (e.g. orders, demo/payment_summary)."
    ),
    test_uid: str = typer.Option(..., "--test-uid", "-u", help="Target test_uid."),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """List assertions on a test case (backfills short assertion_uids)."""
    from cli.assert_cmd import AssertError, run_list

    try:
        _emit(run_list(config.resolve(), model, test_uid))
    except AssertError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@assert_app.command("add")
def assert_add(
    model: str = typer.Argument(...),
    test_uid: str = typer.Option(..., "--test-uid", "-u"),
    description: str = typer.Option(..., "--description", "-d", help="Human spec."),
    sql: str = typer.Option(
        ...,
        "--sql",
        "-s",
        help="dbt-style assertion SQL: SELECT the FAILING rows (0 rows = pass). "
        "Use __result__ as the model output table.",
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Add a spec assertion and re-run it against the live .sql to confirm red/green."""
    import asyncio

    from cli.assert_cmd import AssertError, run_add

    try:
        result = asyncio.run(
            run_add(config.resolve(), model, test_uid, description, sql)
        )
        _emit(result)
    except AssertError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@assert_app.command("update")
def assert_update(
    model: str = typer.Argument(...),
    test_uid: str = typer.Option(..., "--test-uid", "-u"),
    assertion_uid: str = typer.Option(..., "--assertion-id", "-a"),
    description: str = typer.Option(None, "--description", "-d"),
    sql: str = typer.Option(None, "--sql", "-s"),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Edit an existing assertion and re-run it against the live .sql."""
    import asyncio

    from cli.assert_cmd import AssertError, run_update

    if description is None and sql is None:
        typer.echo(
            "[ERROR] Rien à modifier : passe --description et/ou --sql.", err=True
        )
        raise typer.Exit(1)
    try:
        result = asyncio.run(
            run_update(
                config.resolve(), model, test_uid, assertion_uid, description, sql
            )
        )
        _emit(result)
    except AssertError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@assert_app.command("remove")
def assert_remove(
    model: str = typer.Argument(...),
    test_uid: str = typer.Option(..., "--test-uid", "-u"),
    assertion_uid: str = typer.Option(..., "--assertion-id", "-a"),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Remove an assertion from a test case."""
    from cli.assert_cmd import AssertError, run_remove

    try:
        _emit(run_remove(config.resolve(), model, test_uid, assertion_uid))
    except AssertError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@app.command("remove-test")
def remove_test(
    model: str = typer.Argument(
        ..., help="Model name (e.g. orders, demo/payment_summary)."
    ),
    test_uid: str = typer.Option(
        ..., "--test-uid", "-u", help="test_uid of the test to remove."
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Remove a test case from the suite. Deterministic, no LLM.

    Équivalent CLI de la suppression via le chat (delete_test_node) : retire le cas
    du fichier .mocksql/tests/{model}.json, assertions-specs comprises.
    """
    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_remove_test

    try:
        _emit(run_remove_test(config.resolve(), model, test_uid))
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@app.command()
def validate(
    model: str = typer.Argument(
        ..., help="Model name (e.g. orders, demo/payment_summary)."
    ),
    test_uid: str = typer.Option(
        ..., "--test-uid", "-u", help="test_uid of the test awaiting validation."
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Accept the actual output of a test awaiting validation.

    Équivalent du bouton « Je valide l'état actuel » de l'UI (accept_validation) :
    applique la description réalignée proposée par l'évaluateur (corrected_description,
    sans LLM) et flippe le verdict à « Bon ». Fallback LLM uniquement pour les tests
    anciens sans ce champ.
    """
    import asyncio

    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_validate

    try:
        _emit(asyncio.run(run_validate(config.resolve(), model, test_uid)))
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


suggest_app = typer.Typer(
    name="suggest",
    help="Manage coverage suggestions — list/regenerate/use/dismiss.",
    no_args_is_help=True,
)
app.add_typer(suggest_app, name="suggest")


@suggest_app.command("list")
def suggest_list(
    model: str = typer.Argument(
        ..., help="Model name (e.g. orders, demo/payment_summary)."
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """List pending coverage suggestions (numbered for `use`/`dismiss`)."""
    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_suggest_list

    try:
        _emit(run_suggest_list(config.resolve(), model))
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@suggest_app.command("regenerate")
def suggest_regenerate(
    model: str = typer.Argument(...),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Regenerate the suggestion panel via LLM (replace mode).

    Équivalent du bouton « Régénérer » du panneau : tient compte des suggestions déjà
    acceptées/rejetées pour ne pas les reproposer. Pas de profil en CLI → pas de [PROD].
    """
    import asyncio

    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_suggest_regenerate

    try:
        result = asyncio.run(run_suggest_regenerate(config.resolve(), model))
        _emit(result)
        if not result["suggestions"]:
            typer.echo("[WARN] Le LLM n'a produit aucune suggestion.", err=True)
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@suggest_app.command("use")
def suggest_use(
    model: str = typer.Argument(...),
    number: int = typer.Option(
        None, "--number", "-n", help="1-based index from `suggest list`."
    ),
    text: str = typer.Option(
        None, "--text", "-t", help="Exact suggestion text (alternative to --number)."
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
    output_dir: Path = typer.Option(Path(".mocksql/tests"), "--output", "-o"),
) -> None:
    """Turn a pending suggestion into a test, then consume it.

    Équivalent du clic sur une suggestion dans le panneau : génère le test (mode additif,
    focus par branche préservé), puis retire la suggestion du panneau (accepted_suggestions).
    """
    import asyncio

    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_suggest_use

    config = config.resolve()
    if not output_dir.is_absolute():
        output_dir = (config.parent / output_dir).resolve()
    try:
        _emit(asyncio.run(run_suggest_use(config, model, number, text, output_dir)))
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@suggest_app.command("dismiss")
def suggest_dismiss(
    model: str = typer.Argument(...),
    number: int = typer.Option(
        None, "--number", "-n", help="1-based index from `suggest list`."
    ),
    text: str = typer.Option(
        None, "--text", "-t", help="Exact suggestion text (alternative to --number)."
    ),
    config: Path = typer.Option(Path("mocksql.yml"), "--config", "-c"),
) -> None:
    """Dismiss a pending suggestion (it will never be re-proposed). No LLM."""
    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_suggest_dismiss

    try:
        _emit(run_suggest_dismiss(config.resolve(), model, number, text))
    except TestDocError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)


@app.command("refresh-schemas")
def refresh_schemas(
    config: Path = typer.Option(
        Path("mocksql.yml"),
        "--config",
        "-c",
        help="Path to mocksql.yml config.",
    ),
    tables: list[str] = typer.Option(
        [],
        "--table",
        "-t",
        help="Re-import only these tables (project.dataset.table). Default: all cached BQ tables.",
    ),
    from_tests: bool = typer.Option(
        False,
        "--from-tests",
        help="Re-import every table referenced by saved tests (.mocksql/tests/), "
        "even those not yet cached. Ce que `mocksql test` exige.",
    ),
) -> None:
    """Re-import schemas from BigQuery to pick up partition info on existing tables."""

    async def _run() -> None:
        from models.env_variables import validate_required_env
        from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
        from cli.generate import (
            load_config,
            load_schema_cache,
            merge_into_cache,
            save_schema_cache,
        )
        from utils.schema_utils import generate_tables_and_columns_from_project_schema

        validate_required_env()

        cfg = load_config(config)
        dialect = cfg.get("dialect", "bigquery")
        cache_path = str(
            config.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
        )
        cached = load_schema_cache(cache_path)

        if dialect == "trino":
            from build_query.schema_fetcher import fetch_tables_schema_trino

            if tables:
                refs = list(tables)
            elif from_tests:
                from cli.test_runner import collect_test_table_refs

                tests_root = config.parent / ".mocksql" / "tests"
                refs = collect_test_table_refs(tests_root)
                if not refs:
                    typer.echo(
                        "No tables referenced by saved tests in .mocksql/tests/."
                    )
                    raise typer.Exit()
            else:
                refs = [
                    t["table_name"]
                    for t in cached
                    if isinstance(t, dict) and t.get("table_name")
                ]
            if not refs:
                typer.echo(
                    "No tables to import. Pass --table catalog.schema.table (or "
                    "schema.table with TRINO_CATALOG set), or run `mocksql generate` first."
                )
                raise typer.Exit()
            typer.echo(f"Re-importing {len(refs)} table(s) from Trino...")
            schema_rows, failed = await fetch_tables_schema_trino(refs)
            partitions = {}
        else:
            billing_project = os.getenv("BQ_TEST_PROJECT") or os.getenv(
                "VERTEX_PROJECT"
            )
            if not billing_project:
                typer.echo(
                    "[ERROR] BQ_TEST_PROJECT not set. Define it in your .env or shell environment.",
                    err=True,
                )
                raise typer.Exit(1)

            if tables:
                refs = [t for t in tables if validate_bq_ref(t)]
                invalid = [t for t in tables if not validate_bq_ref(t)]
                if invalid:
                    typer.echo(f"[WARN] Ignored (not a valid BQ ref): {invalid}")
            elif from_tests:
                from cli.test_runner import collect_test_table_refs

                tests_root = config.parent / ".mocksql" / "tests"
                all_refs = collect_test_table_refs(tests_root)
                refs = [t for t in all_refs if validate_bq_ref(t)]
                invalid = [t for t in all_refs if not validate_bq_ref(t)]
                if invalid:
                    typer.echo(f"[WARN] Ignored (not a valid BQ ref): {invalid}")
                if not refs:
                    typer.echo(
                        "No BigQuery tables referenced by saved tests in .mocksql/tests/."
                    )
                    raise typer.Exit()
            else:
                refs = [
                    t["table_name"]
                    for t in cached
                    if isinstance(t, dict)
                    and validate_bq_ref(t.get("table_name", ""))
                    and len(t["table_name"].split(".")) == 3
                ]

            if not refs:
                typer.echo(
                    "No BigQuery tables found in cache. Run `mocksql generate` first."
                )
                raise typer.Exit()

            typer.echo(f"Re-importing {len(refs)} table(s) from BigQuery...")
            schema_rows, failed, partitions = await fetch_tables_schema(
                refs, billing_project
            )

        if failed:
            typer.echo(f"[WARN] Could not fetch: {[f['table'] for f in failed]}")
        if not schema_rows:
            typer.echo("[ERROR] No data returned from BigQuery.", err=True)
            raise typer.Exit(1)

        new_tables = generate_tables_and_columns_from_project_schema(
            {"data": schema_rows}
        )
        partitioned = []
        for tbl in new_tables:
            full_name = tbl.get("table_name", "")
            info = partitions.get(full_name) or partitions.get(full_name.split(".")[-1])
            if info:
                tbl["partition"] = info
                partitioned.append(full_name)

        updated = merge_into_cache(cached, new_tables)
        save_schema_cache(cache_path, updated)

        typer.echo(f"[OK] {len(new_tables)} table(s) refreshed.")
        if partitioned:
            typer.echo(f"[OK] Partition info stored for: {partitioned}")
        else:
            typer.echo("[INFO] No partitioned tables detected.")

    asyncio.run(_run())


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
        if langchain_api_key_from_cfg := cfg.get("langchain_api_key"):
            os.environ.setdefault("LANGCHAIN_API_KEY", langchain_api_key_from_cfg)
        tracing = cfg.get("langchain_tracing", bool(cfg.get("langchain_api_key")))
        os.environ.setdefault("LANGCHAIN_TRACING_V2", "true" if tracing else "false")

    base_url = f"http://localhost:{port}"
    os.environ.setdefault("FRONT_URL", base_url)

    url = f"{base_url}/static/"
    typer.echo(f"Starting MockSQL at {url} ...")

    if not no_browser:

        def _open_when_ready() -> None:
            import urllib.error
            import urllib.request

            for _ in range(30):
                try:
                    urllib.request.urlopen(f"http://localhost:{port}/", timeout=1)
                    break
                except (urllib.error.URLError, OSError):
                    threading.Event().wait(0.5)
            webbrowser.open(url)

        threading.Thread(target=_open_when_ready, daemon=True).start()

    uvicorn.run(server_module, host="0.0.0.0", port=port)


def main() -> None:
    app()
