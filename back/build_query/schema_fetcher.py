"""Shared BigQuery schema fetching logic — usable from both the web API and the CLI."""

import asyncio
import json
import os
import re
from typing import Any

_BQ_IDENT_RE = re.compile(r"^[a-zA-Z0-9_\-]+$")


def validate_bq_ref(ref: str) -> bool:
    parts = ref.split(".")
    return len(parts) >= 2 and all(_BQ_IDENT_RE.match(p) for p in parts)


def parse_ref(ref: str, billing_project: str) -> tuple[str, str, str]:
    parts = ref.split(".")
    if len(parts) == 2:
        return billing_project, parts[0], parts[1]
    if len(parts) >= 3:
        return parts[0], parts[1], parts[2]
    raise ValueError(
        f"Invalid table ref (expected dataset.table or project.dataset.table): {ref}"
    )


def _bq_ddl_type(field: dict) -> str:
    """Generate a BigQuery DDL type string from a nested field dict (from _schema_fields_to_dicts).
    Handles STRUCT<...> and ARRAY<...> so sqlglot can transpile correctly to DuckDB.
    """
    base = field["type"].upper()
    mode = field.get("mode", "NULLABLE").upper()
    if base in ("RECORD", "STRUCT") and field.get("fields"):
        inner = ", ".join(f"{f['name']} {_bq_ddl_type(f)}" for f in field["fields"])
        bq_type = f"STRUCT<{inner}>"
    else:
        bq_type = base
    return f"ARRAY<{bq_type}>" if mode == "REPEATED" else bq_type


def _flatten_bq_schema(fields: list, prefix: str = "") -> list:
    rows = []
    for field in fields:
        field_path = f"{prefix}.{field['name']}" if prefix else field["name"]
        rows.append(
            {
                "field_path": field_path,
                "data_type": field["type"],
                "mode": field.get("mode", "NULLABLE"),
                "description": field.get("description", ""),
                "bq_ddl_type": _bq_ddl_type(field),
            }
        )
        if field.get("fields"):
            rows.extend(_flatten_bq_schema(field["fields"], field_path))
    return rows


def _schema_fields_to_dicts(fields: Any) -> list:
    result = []
    for f in fields:
        entry = {
            "name": f.name,
            "type": f.field_type,
            "mode": f.mode,
            "description": f.description or "",
        }
        if f.fields:
            entry["fields"] = _schema_fields_to_dicts(f.fields)
        result.append(entry)
    return result


async def _fetch_table_via_api(client: Any, ref: str, billing_project: str) -> list:
    proj, dataset, table = parse_ref(ref, billing_project)
    bq_table = await asyncio.to_thread(client.get_table, f"{proj}.{dataset}.{table}")
    fields = _schema_fields_to_dicts(bq_table.schema)
    return [
        {"table_catalog": proj, "table_schema": dataset, "table_name": table, **row}
        for row in _flatten_bq_schema(fields)
    ]


async def _fetch_table_via_cli(ref: str, billing_project: str) -> list:
    import subprocess

    proj, dataset, table = parse_ref(ref, billing_project)
    bq_ref = f"{proj}:{dataset}.{table}"
    cmd = [
        "bq",
        f"--project_id={billing_project}",
        "show",
        "--schema",
        "--format=prettyjson",
        bq_ref,
    ]
    print(f"[import-cli] {cmd}")
    result = await asyncio.to_thread(
        lambda c=cmd: subprocess.run(
            c, capture_output=True, text=True, shell=(os.name == "nt")
        )
    )
    if result.returncode != 0:
        raise RuntimeError(f"bq show failed for {bq_ref}: {result.stderr.strip()}")
    fields = json.loads(result.stdout)
    return [
        {"table_catalog": proj, "table_schema": dataset, "table_name": table, **row}
        for row in _flatten_bq_schema(fields)
    ]


async def _fetch_single_table(
    sem: asyncio.Semaphore, client: Any, ref: str, billing_project: str
) -> tuple[str, list, str | None]:
    async with sem:
        try:
            rows = await _fetch_table_via_api(client, ref, billing_project)
            return ref, rows, None
        except Exception as api_exc:
            print(f"[import] API failed for {ref} ({api_exc}), trying CLI fallback")
            try:
                rows = await _fetch_table_via_cli(ref, billing_project)
                return ref, rows, None
            except Exception as cli_exc:
                return ref, [], str(cli_exc)


async def fetch_tables_schema(
    refs: list[str],
    billing_project: str,
    concurrency: int = 3,
) -> tuple[list[dict], list[dict]]:
    """Fetch BigQuery schema for a list of table refs.

    Returns (schema_rows, failed) where:
      - schema_rows: flat list of INFORMATION_SCHEMA-style dicts
      - failed: list of {"table": ref, "error": msg}
    """
    from google.cloud import bigquery as _bq

    client = _bq.Client(project=billing_project)
    sem = asyncio.Semaphore(concurrency)
    tasks = [_fetch_single_table(sem, client, ref, billing_project) for ref in refs]
    results = await asyncio.gather(*tasks)

    schema_rows: list[dict] = []
    failed: list[dict] = []
    for ref, rows, error in results:
        if error:
            failed.append({"table": ref, "error": error})
        else:
            schema_rows.extend(rows)

    return schema_rows, failed
