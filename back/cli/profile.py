"""mocksql profile — run BigQuery profiling on a SQL model."""

import json
import logging
import os
from pathlib import Path

import utils.logger  # noqa: F401 — registers DIAG level (15)

logger = logging.getLogger(__name__)


async def run_profile(model: Path, config: Path, output_dir: Path) -> None:
    import typer

    from cli.generate import (
        load_config,
        load_schema_cache,
        match_refs_against_cache,
        merge_into_cache,
        read_sql,
        save_schema_cache,
    )
    from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
    from build_query.profile_checker import _to_profiler_schema
    from build_query.profiler import profile_joins_for_query, profile_schema
    from utils.schema_utils import generate_tables_and_columns_from_project_schema
    from utils.sql_code import extract_real_table_refs

    cfg = load_config(config)
    dialect = cfg.get("dialect", "bigquery")
    cache_path = str(config.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json"))
    preprocessor_fn = cfg.get("preprocessor_fn")

    billing_project = (
        os.getenv("BQ_TEST_PROJECT")
        or os.getenv("VERTEX_PROJECT")
        or cfg.get("billing_project")
    )
    if not billing_project:
        typer.echo(
            "[ERROR] BQ_TEST_PROJECT not set. Set it in your environment or add "
            "billing_project to mocksql.yml.",
            err=True,
        )
        raise typer.Exit(1)

    typer.echo(f"Reading {model}...")
    sql = read_sql(model, preprocessor_fn, config.parent)

    refs = extract_real_table_refs(sql, dialect)
    if not refs:
        typer.echo("[WARN] No source tables found in the SQL.")
        raise typer.Exit(1)

    ref_names = [".".join(p for p in [r.catalog, r.db, r.name] if p) for r in refs]
    typer.echo(f"Found {len(refs)} source table(s): {ref_names}")

    cached = load_schema_cache(cache_path)
    schemas, missing = match_refs_against_cache(refs, cached)

    if missing:
        typer.echo(f"Fetching schema for: {missing}")
        unqualified = [r for r in missing if not validate_bq_ref(r)]
        if unqualified:
            typer.echo(f"[WARN] Unqualified table refs: {unqualified}")
        to_fetch = [r for r in missing if validate_bq_ref(r)]
        if to_fetch:
            schema_rows, failed = await fetch_tables_schema(to_fetch, billing_project)
            if failed:
                typer.echo(f"[WARN] Could not fetch: {[f['table'] for f in failed]}")
            if schema_rows:
                new_tables = generate_tables_and_columns_from_project_schema({"data": schema_rows})
                updated = merge_into_cache(cached, new_tables)
                save_schema_cache(cache_path, updated)
                typer.echo(f"[OK] Schema cache updated ({len(new_tables)} table(s)).")
                schemas, _ = match_refs_against_cache(refs, updated)

    if not schemas:
        typer.echo("[ERROR] No schemas available — cannot profile.")
        raise typer.Exit(1)

    from google.cloud import bigquery as _bq

    client = _bq.Client(project=billing_project)

    def executor(bq_sql: str) -> list[dict]:
        logger.diag("[profile] BQ query (%d chars):\n%s", len(bq_sql), bq_sql)
        rows = [dict(row) for row in client.query(bq_sql).result()]
        logger.diag("[profile] → %d row(s): %s", len(rows), json.dumps(rows, default=str))
        return rows

    schema_for_profiler = _to_profiler_schema(schemas)
    logger.diag("[profile] schema_for_profiler:\n%s", json.dumps(schema_for_profiler, indent=2, default=str))

    typer.echo("Running profile_schema...")
    result = profile_schema(schema_for_profiler, executor, dialect=dialect)
    typer.echo(f"[OK] profile_schema -> {len(result.get('tables', {}))} table(s)")

    typer.echo("Running profile_joins...")
    result["joins"] = profile_joins_for_query(schema_for_profiler, sql, executor, dialect=dialect)
    typer.echo(f"[OK] profile_joins -> {len(result['joins'])} join(s)")

    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"{model.stem}.json"
    out_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    typer.echo(f"[OK] Profile saved -> {out_path}")
