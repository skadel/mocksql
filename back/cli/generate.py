"""mocksql generate — parse SQL, fetch schemas, generate test data."""

import json
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import yaml

from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
from utils.schema_utils import generate_tables_and_columns_from_project_schema
from utils.sql_code import extract_real_table_refs


# ── Config ────────────────────────────────────────────────────────────────────


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Run `mocksql init` first."
        )
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── SQL reading ───────────────────────────────────────────────────────────────


def _load_preprocessor_fn(fn_ref: str, config_dir: Path):
    import importlib
    import sys

    if ":" not in fn_ref:
        raise ValueError(
            f"preprocessor_fn must be in 'module:function' format, got: {fn_ref!r}"
        )
    module_name, func_name = fn_ref.split(":", 1)

    config_dir_str = str(config_dir.resolve())
    if config_dir_str not in sys.path:
        sys.path.insert(0, config_dir_str)

    module = importlib.import_module(module_name)
    fn = getattr(module, func_name, None)
    if fn is None:
        raise AttributeError(
            f"Function '{func_name}' not found in module '{module_name}'"
        )
    return fn


def read_sql(model_path: Path, preprocessor_fn: str | None, config_dir: Path) -> str:
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    raw_sql = model_path.read_text(encoding="utf-8")
    if not preprocessor_fn:
        return raw_sql
    fn = _load_preprocessor_fn(preprocessor_fn, config_dir)
    return fn(raw_sql)


# ── Schema cache ──────────────────────────────────────────────────────────────


def load_schema_cache(cache_path: str) -> list[dict]:
    p = Path(cache_path)
    if not p.exists():
        return []
    with open(p) as f:
        return json.load(f)


def save_schema_cache(cache_path: str, tables: list[dict]) -> None:
    p = Path(cache_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(tables, f, indent=2)


def merge_into_cache(existing: list[dict], new_tables: list[dict]) -> list[dict]:
    by_name = {t["table_name"]: t for t in existing}
    for tbl in new_tables:
        by_name[tbl["table_name"]] = tbl
    return list(by_name.values())


# ── Table ref matching ────────────────────────────────────────────────────────


def match_refs_against_cache(
    refs: list, cached: list[dict]
) -> tuple[list[dict], list[str]]:
    """Return (matched_schemas, missing_qualified_refs)."""
    cached_by_name = {t["table_name"].lower(): t for t in cached}

    matched: list[dict] = []
    missing: list[str] = []

    for ref in refs:
        # Build qualified name from sqlglot Table node
        parts = [p for p in [ref.catalog, ref.db, ref.name] if p]
        qualified = ".".join(parts).lower()

        if qualified in cached_by_name:
            matched.append(cached_by_name[qualified])
            continue

        # Try suffix match (dataset.table or just table)
        candidates = [v for k, v in cached_by_name.items() if k.endswith(qualified)]
        if candidates:
            matched.extend(candidates)
        else:
            missing.append(qualified)

    return matched, missing


# ── State builder ─────────────────────────────────────────────────────────────


def build_used_columns(schemas: list[dict]) -> list[str]:
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
        "used_columns": build_used_columns(schemas),
        "used_columns_changed": True,
        "gen_retries": 2,
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


# ── CLI graph (DB-free) ───────────────────────────────────────────────────────


def _build_cli_graph():
    """Stripped graph: generator → executor → END (no DB nodes)."""
    from langgraph.graph import END, START, StateGraph

    from build_query.examples_executor import run_on_examples
    from build_query.examples_generator import generate_examples
    from build_query.state import QueryState

    builder = StateGraph(QueryState)
    builder.add_node("generator", generate_examples)
    builder.add_node("executor", run_on_examples)

    def route_executor(state: QueryState):
        if state.get("error"):
            return END
        if (
            state.get("status") == "empty_results"
            and (state.get("gen_retries") or 0) > 0
        ):
            return "generator"
        return END

    builder.add_edge(START, "generator")
    builder.add_edge("generator", "executor")
    builder.add_conditional_edges("executor", route_executor)

    return builder.compile()


def _inject_schemas_into_cache(project_id: str, schemas: list[dict]) -> None:
    """Pre-populate the in-memory schema cache so generator/executor skip the DB."""
    import models.schemas as _s

    _s._cache[project_id] = schemas
    # Set TTL far in the future so it won't expire during the run
    _s._cache_time[project_id] = datetime.now() + timedelta(hours=1)


def _patch_db_calls() -> None:
    """Replace DB-hitting helpers with no-ops for CLI usage."""
    import build_query.examples_executor as _ex

    async def _no_db(_session_id: str):
        return []

    _ex._load_existing_tests_from_db = _no_db


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

        if content[0].get("data") is not None:
            return content

    return None


def _write_test_file(
    model: Path,
    output_dir: Path,
    sql: str,
    used_columns: list[str],
    test_cases: list,
) -> Path:
    """Write a single {stem}.json test file in the format expected by test_runner."""
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{model.stem}.json"
    doc = {
        "sql": sql,
        "used_columns": used_columns,
        "test_cases": test_cases,
    }
    out_path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
    return out_path


# ── Entrypoint ────────────────────────────────────────────────────────────────


async def run_generate(model: Path, config: Path, output_dir: Path) -> None:
    import typer

    from init.init_db import run_migrations
    from models.db_pool import db_pool

    await db_pool.init_pool()
    await run_migrations()

    cfg = load_config(config)
    dialect = cfg.get("dialect", "bigquery")
    cache_path = str(
        config.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    preprocessor_fn = cfg.get("preprocessor_fn")

    # Step 1 — read SQL
    typer.echo(f"Reading {model}...")
    sql = read_sql(model, preprocessor_fn, config.parent)

    # Step 2 — extract table refs
    refs = extract_real_table_refs(sql, dialect)
    if not refs:
        typer.echo("[WARN] No source tables found in the SQL.")
    else:
        ref_names = [".".join(p for p in [r.catalog, r.db, r.name] if p) for r in refs]
        typer.echo(f"Found {len(refs)} source table(s): {ref_names}")

    # Step 3 — resolve schemas from cache + fetch missing
    cached = load_schema_cache(cache_path)
    schemas, missing = match_refs_against_cache(refs, cached)

    if missing:
        typer.echo(f"Fetching schema for: {missing}")
        billing_project = os.getenv("PROJECT_ID") or cfg.get("billing_project")
        if not billing_project:
            typer.echo(
                "[ERROR] PROJECT_ID not set. Cannot fetch schemas from BigQuery. "
                "Set it in your environment or add billing_project to mocksql.yml."
            )
            raise typer.Exit(1)

        unqualified = [r for r in missing if not validate_bq_ref(r)]
        if unqualified:
            typer.echo(
                f"[WARN] Unqualified table refs (need project.dataset.table): {unqualified}"
            )

        to_fetch = [r for r in missing if validate_bq_ref(r)]
        if to_fetch:
            schema_rows, failed = await fetch_tables_schema(to_fetch, billing_project)
            if failed:
                typer.echo(f"[WARN] Could not fetch: {[f['table'] for f in failed]}")
            if schema_rows:
                new_tables = generate_tables_and_columns_from_project_schema(
                    {"data": schema_rows}
                )
                updated = merge_into_cache(cached, new_tables)
                save_schema_cache(cache_path, updated)
                typer.echo(f"[OK] Schema cache updated ({len(new_tables)} table(s)).")
                schemas, _ = match_refs_against_cache(refs, updated)

    if not schemas:
        typer.echo("[ERROR] No schemas available — cannot generate tests.")
        raise typer.Exit(1)

    # Step 4 — build state + inject schemas into in-memory cache
    project_id = model.stem
    session_id = str(uuid.uuid4())
    state = build_initial_state(sql, dialect, schemas, project_id, session_id)

    _inject_schemas_into_cache(project_id, schemas)
    _patch_db_calls()

    # Step 5 — run CLI graph
    typer.echo(f"Generating tests for {project_id} ({len(schemas)} table(s))...")
    graph = _build_cli_graph()
    final_state = await graph.ainvoke(state)

    if final_state.get("error"):
        typer.echo(f"[ERROR] {final_state['error']}")
        raise typer.Exit(1)

    # Step 6 — write outputs
    test_cases = _extract_test_cases(final_state)
    if test_cases:
        out_path = _write_test_file(
            model, output_dir, sql, state["used_columns"], test_cases
        )
        typer.echo(f"[OK] {len(test_cases)} test case(s) written to {out_path}")
    else:
        typer.echo("[WARN] No output produced — check the SQL and schemas.")
