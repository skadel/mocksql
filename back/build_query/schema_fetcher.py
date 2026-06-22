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


def _extract_partition_info(bq_table: Any, fields: list | None = None) -> dict | None:
    """Extract partition metadata from a BigQuery Table object."""
    if bq_table.time_partitioning:
        field_name = bq_table.time_partitioning.field
        col_type = "DATE"
        if field_name and fields:
            col_type = next(
                (f["type"] for f in fields if f["name"] == field_name),
                "DATE",
            )
        return {
            "type": "time",
            "granularity": bq_table.time_partitioning.type_,  # DAY, HOUR, MONTH, YEAR
            "field": field_name,  # None = ingestion time (_PARTITIONDATE)
            "col_type": col_type,
        }
    if bq_table.range_partitioning:
        return {
            "type": "range",
            "field": bq_table.range_partitioning.field,
        }
    return None


async def _fetch_partition_values_api(
    client: Any,
    proj: str,
    dataset: str,
    table: str,
    limit: int = 3,
) -> tuple[list[str], bool]:
    """Fetch the last *limit* partition IDs from INFORMATION_SCHEMA.PARTITIONS.

    Returns (values, has_null_partition) where values are raw partition_ids
    (e.g. '20240115' for DAY granularity), excluding __NULL__ and __UNPARTITIONED__.
    Ordering DESC puts __NULL__/__UNPARTITIONED__ first (underscore > digits in ASCII),
    so fetching limit+5 rows is enough to capture both special entries and real dates.
    """
    query = (
        f"SELECT partition_id "
        f"FROM `{proj}.{dataset}.INFORMATION_SCHEMA.PARTITIONS` "
        f"WHERE table_name = '{table}' "
        f"ORDER BY partition_id DESC "
        f"LIMIT {limit + 5}"
    )
    rows = await asyncio.wait_for(
        asyncio.to_thread(lambda: list(client.query(query).result())),
        timeout=_API_TIMEOUT,
    )
    all_ids = [row["partition_id"] for row in rows if row["partition_id"] is not None]
    has_null = "__NULL__" in all_ids
    values = [pid for pid in all_ids if pid not in ("__NULL__", "__UNPARTITIONED__")][
        :limit
    ]
    return values, has_null


async def _fetch_table_via_api(
    client: Any, ref: str, billing_project: str
) -> tuple[list, dict | None]:
    proj, dataset, table = parse_ref(ref, billing_project)
    bq_table = await asyncio.wait_for(
        asyncio.to_thread(client.get_table, f"{proj}.{dataset}.{table}"),
        timeout=_API_TIMEOUT,
    )
    fields = _schema_fields_to_dicts(bq_table.schema)
    rows = [
        {"table_catalog": proj, "table_schema": dataset, "table_name": table, **row}
        for row in _flatten_bq_schema(fields)
    ]
    partition = _extract_partition_info(bq_table, fields)
    if (
        partition
        and partition.get("type") == "time"
        and partition.get("granularity") == "DAY"
    ):
        try:
            values, has_null = await _fetch_partition_values_api(
                client, proj, dataset, table
            )
            partition["values"] = values
            partition["has_null_partition"] = has_null
        except Exception as exc:
            print(f"[import] partition values fetch failed for {ref}: {exc}")
    return rows, partition


_AUTH_KEYWORDS = (
    "reauthentication is needed",
    "invalid_grant",
    "could not refresh access token",
    "token has been expired or revoked",
)


def _is_auth_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(kw in msg for kw in _AUTH_KEYWORDS)


async def _run_bq_cli(
    cmd: list[str], billing_project: str, timeout: int = _CLI_TIMEOUT
) -> str:
    """Run a bq CLI command and return stdout, raising on auth errors or non-zero exit."""
    import subprocess

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
                timeout=timeout,
                env=env,
                **extra,
            )
        )
    except subprocess.TimeoutExpired:
        raise RuntimeError(f"bq CLI timed out after {timeout}s: {cmd}")
    combined = (result.stdout + result.stderr).lower()
    if any(kw in combined for kw in ("login", "credential", "auth", "password")):
        raise RuntimeError("bq CLI not authenticated. Run: gcloud auth login")
    if result.returncode != 0:
        raise RuntimeError(f"bq CLI failed: {result.stderr.strip()}")
    return result.stdout


async def _fetch_table_via_cli(
    ref: str, billing_project: str
) -> tuple[list, dict | None]:
    """Fetch schema rows and partition metadata using the bq CLI.

    Uses ``bq show --format=prettyjson`` (full metadata) instead of
    ``bq show --schema`` so that timePartitioning info is available.
    """
    proj, dataset, table = parse_ref(ref, billing_project)
    bq_ref = f"{proj}:{dataset}.{table}"
    cmd = [
        "bq",
        f"--project_id={billing_project}",
        "show",
        "--format=prettyjson",
        bq_ref,
    ]
    print(f"[import-cli] {cmd}")
    stdout = await _run_bq_cli(cmd, billing_project)
    metadata = json.loads(stdout)
    raw_fields = (metadata.get("schema") or {}).get("fields", [])
    rows = [
        {"table_catalog": proj, "table_schema": dataset, "table_name": table, **row}
        for row in _flatten_bq_schema(raw_fields)
    ]
    partition = _extract_partition_info_from_cli_metadata(metadata, raw_fields)
    return rows, partition


def _extract_partition_info_from_cli_metadata(
    metadata: dict, fields: list
) -> dict | None:
    """Parse partition metadata from the ``bq show --format=prettyjson`` output."""
    tp = metadata.get("timePartitioning")
    if tp:
        field_name = tp.get("field") or None
        col_type = "DATE"
        if field_name and fields:
            col_type = next(
                (f["type"] for f in fields if f.get("name") == field_name),
                "DATE",
            )
        return {
            "type": "time",
            "granularity": tp.get("type", "DAY"),
            "field": field_name,
            "col_type": col_type,
        }
    rp = metadata.get("rangePartitioning")
    if rp:
        return {
            "type": "range",
            "field": (rp.get("field") or {}).get("field")
            if isinstance(rp.get("field"), dict)
            else rp.get("field"),
        }
    return None


async def _fetch_partition_values_cli(
    ref: str,
    billing_project: str,
    limit: int = 3,
) -> tuple[list[str], bool]:
    """Fetch the last *limit* partition IDs via ``bq query`` on INFORMATION_SCHEMA."""
    proj, dataset, table = parse_ref(ref, billing_project)
    sql = (
        f"SELECT partition_id "
        f"FROM `{proj}.{dataset}.INFORMATION_SCHEMA.PARTITIONS` "
        f"WHERE table_name = '{table}' "
        f"ORDER BY partition_id DESC "
        f"LIMIT {limit + 5}"
    )
    cmd = [
        "bq",
        f"--project_id={billing_project}",
        "query",
        "--use_legacy_sql=false",
        "--format=json",
        "--quiet",
        sql,
    ]
    stdout = await _run_bq_cli(cmd, billing_project)
    rows = json.loads(stdout) if stdout.strip() else []
    all_ids = [row["partition_id"] for row in rows if row.get("partition_id")]
    has_null = "__NULL__" in all_ids
    values = [pid for pid in all_ids if pid not in ("__NULL__", "__UNPARTITIONED__")][
        :limit
    ]
    return values, has_null


async def _fetch_single_table(
    sem: asyncio.Semaphore, client: Any, ref: str, billing_project: str
) -> tuple[str, list, dict | None, str | None]:
    async with sem:
        try:
            rows, partition = await _fetch_table_via_api(client, ref, billing_project)
            return ref, rows, partition, None
        except Exception as api_exc:
            if _is_auth_error(api_exc):
                return (
                    ref,
                    [],
                    None,
                    "Reauthentication required. Run: gcloud auth application-default login",
                )
            print(f"[import] API failed for {ref} ({api_exc}), trying CLI fallback")
            try:
                rows, partition = await _fetch_table_via_cli(ref, billing_project)
                if (
                    partition
                    and partition.get("type") == "time"
                    and partition.get("granularity") == "DAY"
                ):
                    try:
                        values, has_null = await _fetch_partition_values_cli(
                            ref, billing_project
                        )
                        partition["values"] = values
                        partition["has_null_partition"] = has_null
                    except Exception as pv_exc:
                        print(
                            f"[import-cli] partition values fetch failed for {ref}: {pv_exc}"
                        )
                return ref, rows, partition, None
            except Exception as cli_exc:
                return ref, [], None, str(cli_exc)


async def fetch_tables_schema(
    refs: list[str],
    billing_project: str,
    concurrency: int = 3,
) -> tuple[list[dict], list[dict], dict[str, dict]]:
    """Fetch BigQuery schema for a list of table refs.

    Returns (schema_rows, failed, partitions) where:
      - schema_rows: flat list of INFORMATION_SCHEMA-style dicts
      - failed: list of {"table": ref, "error": msg}
      - partitions: {ref: {"type": "time"|"range", "granularity": str, "field": str|None}}
    """
    from utils.optional_deps import import_bigquery

    _bq = import_bigquery()
    client = _bq.Client(project=billing_project)
    sem = asyncio.Semaphore(concurrency)
    tasks = [_fetch_single_table(sem, client, ref, billing_project) for ref in refs]
    results = await asyncio.gather(*tasks)

    schema_rows: list[dict] = []
    failed: list[dict] = []
    partitions: dict[str, dict] = {}
    for ref, rows, partition, error in results:
        if error:
            failed.append({"table": ref, "error": error})
        else:
            schema_rows.extend(rows)
            if partition:
                partitions[ref] = partition

    return schema_rows, failed, partitions


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

        # Qualifier INFORMATION_SCHEMA par la base de la ref : Snowflake n'a pas de
        # base de session garantie, et chaque base a son propre INFORMATION_SCHEMA.
        # Sans ça : "session does not have a current database" + impossible de viser
        # une autre base que celle de la connexion.
        info_schema = (
            f"{_sf_quote(database_name)}.INFORMATION_SCHEMA.COLUMNS"
            if database_name
            else "INFORMATION_SCHEMA.COLUMNS"
        )

        sql = f"""
            SELECT
                TABLE_CATALOG,
                TABLE_SCHEMA,
                TABLE_NAME,
                COLUMN_NAME,
                DATA_TYPE,
                NUMERIC_PRECISION,
                NUMERIC_SCALE,
                IS_NULLABLE,
                COMMENT
            FROM {info_schema}
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
                        # Conserver la casse renvoyée par Snowflake (identifiants en
                        # MAJUSCULES pour des objets non-quotés) — le SQL gold les
                        # référence en MAJUSCULES. Pas de .lower() (cassait le match).
                        "table_catalog": _sf_get(row, "TABLE_CATALOG") or database_name,
                        "table_schema": _sf_get(row, "TABLE_SCHEMA") or schema_name,
                        "table_name": _sf_get(row, "TABLE_NAME") or table_name,
                        # Le DictCursor Snowflake renvoie les clés en MAJUSCULES :
                        # lire "COLUMN_NAME" (pas "field_path") sinon toutes les
                        # colonnes sont droppées → cache vide.
                        "field_path": _sf_get(row, "COLUMN_NAME"),
                        "data_type": _sf_snow_data_type(row),
                        "mode": "NULLABLE"
                        if (_sf_get(row, "IS_NULLABLE") or "YES") == "YES"
                        else "REQUIRED",
                        "description": _sf_get(row, "COMMENT"),
                    }
                )
        except Exception as exc:
            failed.append({"table": ref, "error": str(exc)})

    return schema_rows, failed


def _sf_quote(identifier: str) -> str:
    """Quote a Snowflake identifier for safe interpolation in FROM clauses."""
    return '"' + identifier.replace('"', '""') + '"'


def _sf_get(row: dict, key: str) -> str:
    """Read a value from a Snowflake DictCursor row, robust to key casing.

    snowflake.connector.DictCursor returns column names in UPPERCASE; older code
    paths may pass lowercase keys. Match case-insensitively before giving up.
    """
    val = row.get(key)
    if val is None:
        val = row.get(key.upper())
    if val is None:
        val = row.get(key.lower())
    return "" if val is None else str(val)


def _sf_snow_data_type(row: dict) -> str:
    """Reconstruct a faithful Snowflake type string, restoring NUMBER(p,s).

    INFORMATION_SCHEMA.DATA_TYPE collapses all fixed-point types to "NUMBER" and
    exposes precision/scale separately. Without them the type degrades to DuckDB's
    default DECIMAL(18,3), which overflows on large integers (epoch µs, ids…).
    """
    data_type = _sf_get(row, "DATA_TYPE")
    if data_type.upper() == "NUMBER":
        precision = _sf_get(row, "NUMERIC_PRECISION")
        scale = _sf_get(row, "NUMERIC_SCALE")
        if precision:
            return f"NUMBER({precision},{scale or '0'})"
    return data_type
