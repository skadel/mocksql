"""mocksql generate — parse SQL, fetch schemas, generate test data."""

import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import sqlglot
import yaml

from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
from cli.schema_cache import (
    load_schema_cache,
    match_refs_against_cache,
    merge_into_cache,
    save_schema_cache,
)
from storage.config import load_preprocessor_fn
from utils.schema_utils import generate_tables_and_columns_from_project_schema
from utils.sql_code import (
    extract_real_table_refs,
    extract_select_statement,
    extract_used_columns_from_sql,
)


# ── Config ────────────────────────────────────────────────────────────────────


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Run `mocksql init` first."
        )
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── SQL reading ───────────────────────────────────────────────────────────────


def read_sql(
    model_path: Path,
    preprocessor_fn: str | None,
    config_dir: Path,
    dialect: str = "bigquery",
) -> str:
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    raw_sql = model_path.read_text(encoding="utf-8")
    sql = (
        load_preprocessor_fn(preprocessor_fn, config_dir)(raw_sql)
        if preprocessor_fn
        else raw_sql
    )
    clean = extract_select_statement(sql, dialect)
    return clean if clean is not None else sql


# ── State builder ─────────────────────────────────────────────────────────────


def build_used_columns(
    schemas: list[dict], sql: str = "", dialect: str = "bigquery"
) -> list[str]:
    if sql:
        try:
            return extract_used_columns_from_sql(sql, dialect, schemas)
        except Exception:
            pass

    result = []
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        project = parts[0] if len(parts) == 3 else ""
        database = parts[1] if len(parts) >= 2 else ""
        table = parts[-1]
        cols = [c["name"] for c in tbl.get("columns", [])]
        result.append(
            json.dumps(
                {
                    "project": project,
                    "database": database,
                    "table": table,
                    "used_columns": cols,
                }
            )
        )
    return result


def find_used_columns_missing_from_schema(
    used_columns: list, schemas: list[dict]
) -> list[tuple[str, list[str]]]:
    """Colonnes référencées dans le SQL (used_columns) mais ABSENTES du schéma
    en cache de leur table.

    Sans cette détection, ces colonnes sont droppées silencieusement (modèle de
    génération + création de table DuckDB) → "column not found" à l'exécution.
    La cause est un schéma en cache périmé : on échoue tôt avec un message clair
    (cf. refresh-schemas) plutôt que de fabriquer un test vert sur des données
    impossibles. Retourne [(table_name complet, [colonnes manquantes]), ...].
    """
    used_index: dict[str, list[str]] = {}
    for uc in used_columns:
        entry = json.loads(uc) if isinstance(uc, str) else uc
        db = entry.get("database") or ""
        table = entry.get("table") or ""
        key = (f"{db}.{table}" if db else table).lower()
        used_index[key] = entry.get("used_columns", [])

    problems: list[tuple[str, list[str]]] = []
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        key = ".".join(parts[-2:]).lower() if len(parts) >= 2 else parts[-1].lower()
        if key not in used_index:
            continue
        schema_cols = {c["name"].lower() for c in tbl.get("columns", [])}
        missing = sorted(
            c for c in used_index[key] if "." not in c and c.lower() not in schema_cols
        )
        if missing:
            problems.append((tbl["table_name"], missing))
    return problems


def build_initial_state(
    sql: str,
    dialect: str,
    schemas: list[dict],
    project_id: str,
    session_id: str,
) -> dict[str, Any]:
    msg_id = str(uuid.uuid4())
    return {
        "query": sql,
        "validated_sql": sql,
        "optimized_sql": sql,
        "dialect": dialect,
        "session": session_id,
        "project": project_id,
        "schemas": schemas,
        "used_columns": build_used_columns(schemas, sql, dialect),
        "used_columns_changed": True,
        "gen_retries": 10,
        "debug_retries": 3,
        "status": None,
        "input": "",
        "user_tables": "",
        "user_message_id": msg_id,
        "parent_message_id": msg_id,
        "request_id": str(uuid.uuid4()),
        "messages": [],
        "examples": [],
        "history": [],
        "query_decomposed": "[]",
        "title": "",
        "route": "generator",
        "error": "",
        "reasoning": "",
        "current_query": "",
        "test_index": None,
        "profile_result": None,
        "profile_complete": None,
        "profile": None,
        "profile_billing_tb": None,
        "rerun_all_tests": False,
        "optimize": False,
        "save": None,
        "changed_message_id": "",
    }


def _inject_schemas_into_cache(project_id: str, schemas: list[dict]) -> None:
    """Pre-populate the in-memory schema cache so generator/executor skip the DB."""
    import models.schemas as _s

    _s._cache = schemas
    _s._cache_by_name = {t["table_name"]: t for t in schemas}
    _s._cache_time = datetime.now() + timedelta(hours=1)


def _patch_db_calls() -> None:
    """Neutralise history_saver for CLI — no session exists in DB to save to."""
    import build_query.query_chain as _qc

    async def _noop(_state):
        return {}

    _qc.history_saver = _noop


# ── Output extraction ─────────────────────────────────────────────────────────


def _extract_test_cases(final_state: dict) -> list | None:
    """Return the full list of test-case result dicts from the executor message.

    The executor emits an AIMessage whose content is a JSON-serialised list where
    each element is a test-case dict containing at minimum:
      - "data":             {table_name: [rows]}
      - "assertion_results": [{sql, description, ...}]
    """
    for msg in reversed(final_state.get("messages", [])):
        try:
            content = json.loads(msg.content)
        except Exception:
            continue

        if not isinstance(content, list) or not content:
            continue

        if isinstance(content[0], dict) and content[0].get("data") is not None:
            return content

    return None


def _extract_suggestions(final_state: dict) -> list[str]:
    """Extract suggestions from the SUGGESTIONS message in final state."""
    from utils.msg_types import MsgType
    from utils.saver import get_message_type

    for msg in reversed(final_state.get("messages", [])):
        if get_message_type(msg) == MsgType.SUGGESTIONS:
            try:
                suggestions = json.loads(msg.content)
                if isinstance(suggestions, list):
                    return [s for s in suggestions if isinstance(s, str)]
            except Exception:
                pass
    return []


def _write_test_file(
    model: Path,
    output_dir: Path,
    sql: str,
    used_columns: list[str],
    test_cases: list,
    suggestions: list[str] | None = None,
) -> Path:
    """Write a single {stem}.json test file in the format expected by test_runner."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{model.stem}.json"
    doc: dict = {
        "sql": sql,
        "used_columns": used_columns,
        "test_cases": test_cases,
    }
    if suggestions:
        doc["suggestions"] = suggestions
    out_path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    return out_path


# ── Business context ──────────────────────────────────────────────────────────


def _load_model_context(model_name: str, models_base: Path) -> str:
    """Collect mocksql.md files for model_name relative to models_base."""
    if not model_name:
        return ""
    parts = Path(model_name).parts
    fragments: list[str] = []
    for i in range(len(parts)):
        level_dir = models_base.joinpath(*parts[:i])
        candidate = level_dir / "mocksql.md"
        if candidate.exists():
            text = candidate.read_text(encoding="utf-8").strip()
            if text:
                fragments.append(text)
    file_md = models_base / f"{model_name}.md"
    if file_md.exists():
        text = file_md.read_text(encoding="utf-8").strip()
        if text:
            fragments.append(text)
    return "\n\n---\n\n".join(fragments)


# ── Entrypoint ────────────────────────────────────────────────────────────────


def _run_profile_bq(
    schemas: list[dict], sql: str, dialect: str, billing_project: str
) -> dict:
    """Run BigQuery profiling queries and return a normalized profile dict."""
    from google.cloud import bigquery as _bq

    from build_query.profile_checker import _to_profiler_schema
    from build_query.profiler import profile_joins_for_query, profile_schema

    client = _bq.Client(project=billing_project)

    def executor(bq_sql: str) -> list[dict]:
        return [dict(row) for row in client.query(bq_sql).result()]

    schema_for_profiler = _to_profiler_schema(schemas)
    profile = profile_schema(schema_for_profiler, executor, dialect=dialect)
    profile["joins"] = profile_joins_for_query(
        schema_for_profiler, sql, executor, dialect=dialect
    )
    return profile


async def run_generate(
    model: Path, config: Path, output_dir: Path, profile: bool = False
) -> None:
    import typer

    import storage.config as storage_config
    from models.env_variables import validate_required_env

    # Aligne storage.config (lu par apply_duckdb_extensions, etc.) sur le projet
    # ciblé par --config, sinon il retombe sur le cwd (back/) sans mocksql.yml.
    os.environ["MOCKSQL_BASE_DIR"] = str(config.resolve().parent)
    storage_config.load_config.cache_clear()

    validate_required_env()

    from init.init_db import run_migrations
    from models.database import db_pool

    await db_pool.init_pool()
    await run_migrations()

    cfg = load_config(config)
    dialect = cfg.get("dialect", "bigquery")
    cache_path = str(
        config.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    preprocessor_fn = cfg.get("preprocessor_fn")

    # dbt connector : si un bloc `dbt:` est configuré, MockSQL lit le SQL **compilé**
    # (refs résolus, macros rendues) et infère les schémas amont depuis le manifest —
    # sans jamais interroger l'entrepôt.
    dbt_project = storage_config.get_dbt_project()
    models_path_str = cfg.get("models_path", "./models")
    models_base = (config.parent / models_path_str).resolve()
    try:
        model_name = model.resolve().relative_to(models_base).with_suffix("").as_posix()
    except ValueError:
        model_name = model.stem

    # Step 1 — read SQL (DECLARE/SET preambles are stripped inside read_sql)
    typer.echo(f"Reading {model}...")
    if dbt_project and dbt_project.is_dbt_model(model_name):
        typer.echo(f"[dbt] SQL compilé depuis le manifest pour '{model_name}'.")
        compiled = dbt_project.compiled_sql_for_model(model_name)
        sql = extract_select_statement(compiled, dialect) or compiled
    else:
        sql = read_sql(model, preprocessor_fn, config.parent, dialect)

    # Step 1.5 — fail fast if the query requires generating too many rows
    from build_query.constraint_simplifier import (
        check_correlated_aggregate_cardinality,
        check_having_cardinality,
    )

    try:
        check_having_cardinality(sql, dialect)
        check_correlated_aggregate_cardinality(sql, dialect)
    except ValueError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)

    # Step 2 — extract table refs
    refs = extract_real_table_refs(sql, dialect)
    if not refs:
        typer.echo("[WARN] No source tables found in the SQL.")
    else:
        ref_names = [".".join(p for p in [r.catalog, r.db, r.name] if p) for r in refs]
        typer.echo(f"Found {len(refs)} source table(s): {ref_names}")

    billing_project = os.getenv("BQ_TEST_PROJECT") or os.getenv("VERTEX_PROJECT")

    # Step 3 — resolve schemas via cache local + fetch BigQuery des manquants.
    # En mode dbt, le SQL est déjà compilé (refs résolus en noms réels) ; la résolution
    # de schéma passe par ce MÊME chemin — le connecteur dbt ne fournit pas les schémas.
    cached = load_schema_cache(cache_path)
    schemas, missing = match_refs_against_cache(refs, cached)

    if missing:
        typer.echo(f"Fetching schema for: {missing}")
        if not billing_project:
            typer.echo(
                "[ERROR] BQ_TEST_PROJECT not set. Cannot fetch schemas from BigQuery. "
                "Set it in your .env or shell environment."
            )
            raise typer.Exit(1)

        unqualified = [r for r in missing if not validate_bq_ref(r)]
        if unqualified:
            typer.echo(
                f"[WARN] Unqualified table refs (need project.dataset.table): {unqualified}"
            )

        to_fetch = [r for r in missing if validate_bq_ref(r)]
        if to_fetch:
            schema_rows, failed, partitions = await fetch_tables_schema(
                to_fetch, billing_project
            )
            if failed:
                typer.echo(f"[WARN] Could not fetch: {[f['table'] for f in failed]}")
            if schema_rows:
                new_tables = generate_tables_and_columns_from_project_schema(
                    {"data": schema_rows}
                )
                if partitions:
                    for tbl in new_tables:
                        full_name = tbl.get("table_name", "")
                        info = partitions.get(full_name) or partitions.get(
                            full_name.split(".")[-1]
                        )
                        if info:
                            tbl["partition"] = info
                updated = merge_into_cache(cached, new_tables)
                save_schema_cache(cache_path, updated)
                typer.echo(f"[OK] Schema cache updated ({len(new_tables)} table(s)).")
                schemas, _ = match_refs_against_cache(refs, updated)

    if not schemas:
        typer.echo("[ERROR] No schemas available — cannot generate tests.")
        raise typer.Exit(1)

    # Step 3.5 — profile (optional)
    profile_data: dict | None = None
    if profile:
        if not billing_project:
            typer.echo(
                "[ERROR] --profile requires BQ_TEST_PROJECT. "
                "Set it in your .env or shell environment."
            )
            raise typer.Exit(1)
        typer.echo("Profiling tables on BigQuery (this may take a moment)...")
        try:
            profile_data = _run_profile_bq(schemas, sql, dialect, billing_project)
            typer.echo(
                f"[OK] Profile complete ({len(profile_data.get('tables', {}))} table(s), "
                f"{len(profile_data.get('joins', []))} join(s))."
            )
        except Exception as exc:
            typer.echo(f"[WARN] Profiling failed: {exc}. Continuing without profile.")

    # Step 4 — build state + inject schemas into in-memory cache
    # (model_name / models_base déjà calculés en amont pour la résolution dbt)
    project_id = model.stem
    session_id = str(uuid.uuid4())

    model_context = _load_model_context(model_name, models_base) or None

    state = build_initial_state(sql, dialect, schemas, project_id, session_id)

    # Garde-fou : colonnes du SQL absentes du schéma en cache → schéma périmé.
    # On échoue tôt (avant l'appel LLM) avec la commande de refresh ciblée.
    schema_gaps = find_used_columns_missing_from_schema(state["used_columns"], schemas)
    if schema_gaps:
        typer.echo(
            "[ERROR] Colonne(s) référencée(s) dans le SQL mais absente(s) du schéma "
            "en cache :",
            err=True,
        )
        for table_name, cols in schema_gaps:
            typer.echo(f"  - {table_name}: {', '.join(cols)}", err=True)
        refreshable = [tn for tn, _ in schema_gaps if len(tn.split(".")) == 3]
        typer.echo(
            "\nLe schéma en cache est probablement périmé. Rafraîchis-le puis "
            "relance la génération :",
            err=True,
        )
        if refreshable:
            hint = " ".join(f"-t {tn}" for tn in refreshable)
            typer.echo(f"  mocksql refresh-schemas {hint}", err=True)
        else:
            typer.echo("  mocksql refresh-schemas", err=True)
        typer.echo(
            "(Si la colonne n'existe vraiment pas dans la table, corrige le SQL.)",
            err=True,
        )
        raise typer.Exit(1)

    if model_context:
        state["model_context"] = model_context
        typer.echo(f"[OK] Business context loaded ({len(model_context)} chars).")
    if profile_data:
        state["profile"] = profile_data
        state["profile_complete"] = True

    _inject_schemas_into_cache(project_id, schemas)

    # Qualify the SQL using the same optimize_query path as the UI validator.
    # This applies qualify_columns + _fix_unnest_alias_conflicts + _fix_unnest_scope_leak,
    # which prevents DuckDB "Ambiguous reference" errors on UNNEST aliases.
    from common_vars import get_tables_mapping
    from build_query.validator import expand_positional_group_by, optimize_query

    try:
        tables_mapping = await get_tables_mapping(project_id)
        parsed_ast = sqlglot.parse_one(sql, read=dialect)
        qualified_ast = optimize_query(parsed_ast, tables_mapping, dialect=dialect)
        state["optimized_sql"] = qualified_ast.sql(dialect=dialect, pretty=True)
    except Exception as e:
        typer.echo(f"[WARN] SQL qualification failed ({e}), using raw SQL.")
        # Repli sur le SQL brut : qualify n'a pas tourné, donc le GROUP BY positionnel
        # survit. On le binde au moins aux colonnes du SELECT pour éviter un
        # « GROUP BY out of range » si une projection est élaguée plus loin.
        try:
            guarded = expand_positional_group_by(sqlglot.parse_one(sql, read=dialect))
            state["optimized_sql"] = guarded.sql(dialect=dialect, pretty=True)
        except Exception:
            pass

    from build_query.query_chain import _lightweight_query_decomposed

    state["query_decomposed"] = _lightweight_query_decomposed(
        state.get("optimized_sql") or sql, dialect
    )

    _patch_db_calls()

    # Step 5 — run graph (same as UI, history_saver neutralised above)
    from build_query.query_chain import build_query_graph

    typer.echo(f"Generating tests for {project_id} ({len(schemas)} table(s))...")
    graph = build_query_graph()
    final_state = await graph.ainvoke(state, config={"recursion_limit": 50})

    if final_state.get("error"):
        err = final_state["error"]
        typer.echo(f"[ERROR] {err[:500]}{'…' if len(err) > 500 else ''}")
        raise typer.Exit(1)

    # Step 6 — write outputs
    test_cases = _extract_test_cases(final_state)
    suggestions = _extract_suggestions(final_state)
    if test_cases:
        out_path = _write_test_file(
            model, output_dir, sql, state["used_columns"], test_cases, suggestions
        )
        typer.echo(f"[OK] {len(test_cases)} test case(s) written to {out_path}")
    else:
        typer.echo("[WARN] No output produced — check the SQL and schemas.")

    if suggestions:
        typer.echo("\nSuggestions de cas non couverts :")
        for i, s in enumerate(suggestions, 1):
            typer.echo(f"  {i}. {s}")
