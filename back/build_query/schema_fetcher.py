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


_API_TIMEOUT = 60.0
_CLI_TIMEOUT = 60


async def _fetch_table_via_api(client: Any, ref: str, billing_project: str) -> list:
    proj, dataset, table = parse_ref(ref, billing_project)
    bq_table = await asyncio.wait_for(
        asyncio.to_thread(client.get_table, f"{proj}.{dataset}.{table}"),
        timeout=_API_TIMEOUT,
    )
    fields = _schema_fields_to_dicts(bq_table.schema)
    return [
        {"table_catalog": proj, "table_schema": dataset, "table_name": table, **row}
        for row in _flatten_bq_schema(fields)
    ]


_AUTH_KEYWORDS = (
    "reauthentication is needed",
    "invalid_grant",
    "could not refresh access token",
    "token has been expired or revoked",
)


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


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
    extra: dict = {}
    if os.name == "nt":
        extra["creationflags"] = subprocess.CREATE_NO_WINDOW
    env = os.environ.copy()
    env["CLOUDSDK_CORE_DISABLE_PROMPTS"] = "1"
    try:
        result = await asyncio.to_thread(
            lambda c=cmd: subprocess.run(
                c,
                capture_output=True,
                text=True,
                shell=True,
                stdin=subprocess.DEVNULL,
                timeout=_CLI_TIMEOUT,
                env=env,
                **extra,
            )
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"bq CLI timed out after {_CLI_TIMEOUT}s for {bq_ref}")
    _auth_kws = ("login", "credential", "auth", "password")
    combined = (result.stdout + result.stderr).lower()
    if any(kw in combined for kw in _auth_kws):
        raise RuntimeError(
            f"bq CLI not authenticated for {bq_ref}. Run: gcloud auth login"
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
            if _is_auth_error(api_exc):
                return (
                    ref,
                    [],
                    (
                        "Reauthentication required. Run: gcloud auth application-default login"
                    ),
                )
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


async def fetch_tables_schema_snowflake(
    refs: list[str],
) -> tuple[list[dict], list[dict]]:
    """Fetch Snowflake schema for a list of table refs (schema.table or database.schema.table).

    Returns (schema_rows, failed) in the same INFORMATION_SCHEMA-style format as
    fetch_tables_schema(), so generate_tables_and_columns_from_project_schema() can
    consume it unchanged.
    """
    from utils.snowflake_connector import run_sf_query

    schema_rows: list[dict] = []
    failed: list[dict] = []

    for ref in refs:
        parts = ref.split(".")
        if len(parts) == 2:
            schema_name, table_name = parts
            database_name = ""
        elif len(parts) >= 3:
            database_name, schema_name, table_name = parts[0], parts[1], parts[2]
        else:
            failed.append(
                {
                    "table": ref,
                    "error": "Invalid ref (expected schema.table or db.schema.table)",
                }
            )
            continue

        schema_filter = (
            f"AND TABLE_SCHEMA = '{schema_name.upper()}'" if schema_name else ""
        )
        db_filter = (
            f"AND TABLE_CATALOG = '{database_name.upper()}'" if database_name else ""
        )

        sql = f"""
            SELECT
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME    AS field_path,
                DATA_TYPE      AS data_type,
                IS_NULLABLE,
                COMMENT        AS description
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}'
            {schema_filter}
            {db_filter}
            ORDER BY ORDINAL_POSITION
        """
        try:
            rows = await asyncio.to_thread(run_sf_query, sql)
            if not rows:
                failed.append(
                    {
                        "table": ref,
                        "error": "Table not found in Snowflake INFORMATION_SCHEMA",
                    }
                )
                continue
            for row in rows:
                schema_rows.append(
                    {
                        "table_catalog": (
                            row.get("TABLE_CATALOG") or database_name
                        ).lower(),
                        "table_schema": (
                            row.get("TABLE_SCHEMA") or schema_name
                        ).lower(),
                        "table_name": (row.get("TABLE_NAME") or table_name).lower(),
                        "field_path": row.get("field_path")
                        or row.get("COLUMN_NAME", ""),
                        "data_type": row.get("data_type") or row.get("DATA_TYPE", ""),
                        "mode": "NULLABLE"
                        if (row.get("IS_NULLABLE") or "YES") == "YES"
                        else "REQUIRED",
                        "description": row.get("description")
                        or row.get("COMMENT")
                        or "",
                    }
                )
        except Exception as exc:
            failed.append({"table": ref, "error": str(exc)})

    return schema_rows, failed
