import asyncio
import json
from typing import Optional


from build_query.profiler import build_profile_query
from build_query.state import QueryState
from storage.test_repository import get_test


def _load_model_profile_skipped(session_id: str) -> bool:
    """Return True if the user chose to skip profiling for this test."""
    test = get_test(session_id)
    if not test:
        return False
    return bool(test.get("profile_skipped"))


def _load_model_profile() -> Optional[dict]:
    """Retrieve the stored profile from schema_cache (shared across all models)."""
    from models.schemas import get_profile

    return get_profile() or None


def _save_model_profile(profile: dict) -> None:
    """Persist the profile in schema_cache (shared across all models)."""
    from models.schemas import save_profile

    save_profile(profile)


def _normalize_profile(raw) -> Optional[dict]:
    """
    Convert a profile to {"tables": {...}, "joins": [...]} form.

    Accepts:
    - Already-normalized dict  → returned as-is
    - Raw list of row dicts    → converted from the BigQuery profiler flat format
    - None / falsy             → returns None
    """
    if not raw:
        return None
    if isinstance(raw, dict):
        return raw
    # Raw list from BigQuery profiler
    tables: dict = {}
    joins: list = []
    for row in raw:
        row_type = row.get("row_type")
        if row_type == "column":
            tbl = row.get("table_name", "")
            col = row.get("col_name", "")
            if not tbl or not col:
                continue
            if tbl not in tables:
                tables[tbl] = {"columns": {}}
            tables[tbl]["columns"][col] = {
                k: v
                for k, v in row.items()
                if k not in ("row_type", "table_name", "col_name")
            }
        elif row_type == "join":
            joins.append({k: v for k, v in row.items() if k != "row_type"})
    return {"tables": tables, "joins": joins}


def _merge_profiles(base: Optional[dict], incoming: Optional[dict]) -> dict:
    """Merge two profile dicts (incoming overrides base at column level)."""
    if not base and not incoming:
        return {"tables": {}, "joins": []}
    if not base:
        return incoming
    if not incoming:
        return base

    merged = json.loads(json.dumps(base))  # deep copy
    for tbl, tbl_data in incoming.get("tables", {}).items():
        if tbl not in merged["tables"]:
            merged["tables"][tbl] = tbl_data
        else:
            existing_cols = merged["tables"][tbl].get("columns", {})
            for col, col_data in tbl_data.get("columns", {}).items():
                existing_cols[col] = col_data
            merged["tables"][tbl]["columns"] = existing_cols
    return merged


def _to_profiler_schema(schemas: list) -> dict:
    """Convert state["schemas"] (list of table dicts with "table_name") to
    the {"tables": [{"name": ..., "columns": [...]}]} format expected by profiler.

    Always uses the fully-qualified table name so the generated SQL FROM clause
    is correct (e.g. project.dataset.country_summary, not just country_summary).
    """
    tables = []
    for t in schemas:
        full_name = t.get("table_name") or t.get("name", "")
        tables.append(
            {
                "name": full_name,
                "columns": t.get("columns", []),
            }
        )
    return {"tables": tables}


def _resolve_full_table_names(used_columns: list, schemas: list) -> list:
    """Replace short table names in used_columns with fully-qualified names.

    The validator stores short names (e.g. "country_summary") in used_columns,
    but the SQL FROM clause needs the full name ("project.dataset.country_summary").
    """
    name_map: dict[str, str] = {}
    for t in schemas:
        full = t.get("table_name") or t.get("name", "")
        short = full.split(".")[-1]
        name_map[short] = full
        name_map[full] = full  # identity mapping so full names pass through

    resolved = []
    for entry in used_columns:
        if isinstance(entry, str):
            entry = json.loads(entry)
        tbl = entry["table"]
        resolved.append(
            {
                "table": name_map.get(tbl, tbl),
                "used_columns": list(dict.fromkeys(entry["used_columns"])),
            }
        )
    return resolved


def _extract_expected_join_pairs(sql: str, dialect: str) -> list[dict]:
    """Extract distinct (left_table, right_table) join pairs from the SQL query."""
    if not sql:
        return []
    try:
        from build_query.profiler import _collect_join_specs

        seen: set[tuple] = set()
        pairs: list[dict] = []
        for spec in _collect_join_specs(sql, dialect=dialect):
            if spec.get("left_keys") and spec.get("right_keys"):
                key = (spec["left_table"], spec["right_table"])
                if key not in seen:
                    seen.add(key)
                    pairs.append(
                        {
                            "left_table": spec["left_table"],
                            "right_table": spec["right_table"],
                        }
                    )
        return pairs
    except Exception:
        return []


def _validate_profile_result(
    incoming_profile: dict,
    expected_columns: list,
    expected_joins: list,
) -> tuple[list, list]:
    """
    Validate that incoming_profile covers the expected columns and join profiles.

    Args:
        incoming_profile: normalized profile dict from the user's uploaded result
        expected_columns: list of {"table", "used_columns"} that were requested
        expected_joins:   list of {"left_table", "right_table"} pairs from the SQL

    Returns:
        (still_missing_cols, missing_join_pairs):
        - still_missing_cols: entries not yet covered by incoming_profile
        - missing_join_pairs: {"left_table", "right_table"} pairs absent from the result
        Both empty means the result is valid.
    """
    still_missing_cols = _find_missing_columns(incoming_profile, expected_columns)

    incoming_joins = incoming_profile.get("joins", [])
    missing_join_pairs: list[dict] = []

    if expected_joins:
        for exp_join in expected_joins:
            left, right = exp_join["left_table"], exp_join["right_table"]
            found = any(
                j.get("left_table") == left and j.get("right_table") == right
                for j in incoming_joins
            )
            if not found:
                missing_join_pairs.append(exp_join)
    elif expected_columns:
        # Fallback when expected_joins not provided: check at least one join exists
        requested_tables: set[str] = set()
        for entry in expected_columns:
            if isinstance(entry, str):
                entry = json.loads(entry)
            requested_tables.add(entry["table"])
        if len(requested_tables) > 1 and not incoming_joins:
            missing_join_pairs = [
                {"left_table": t, "right_table": ""} for t in sorted(requested_tables)
            ]

    return still_missing_cols, missing_join_pairs


def _find_missing_columns(profile: dict, used_columns: list) -> list:
    """
    Returns list of {"table": str, "used_columns": [str]} for columns
    that are in used_columns but not yet in the profile.
    """
    missing = []
    for entry in used_columns:
        if isinstance(entry, str):
            entry = json.loads(entry)
        tbl_short = entry["table"]
        tbl = f"{entry['project']}.{entry['database']}.{tbl_short}"
        profiled_cols = set()
        # Check by full name or short name
        for key in (tbl, tbl_short):
            if key in profile.get("tables", {}):
                profiled_cols = set(profile["tables"][key].get("columns", {}).keys())
                break

        missing_cols = [c for c in entry["used_columns"] if c not in profiled_cols]
        if missing_cols:
            missing.append({"table": tbl, "used_columns": missing_cols})

    return missing


async def _estimate_profile_bytes(sql: str, billing_project: str) -> Optional[float]:
    """Dry-run the profile SQL on BigQuery and return estimated bytes processed (as TB)."""
    try:
        from google.cloud import bigquery as _bq

        client = _bq.Client(project=billing_project)
        job_config = _bq.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = await asyncio.to_thread(client.query, sql, job_config)
        return job.total_bytes_processed / 1e12
    except Exception:
        return None


async def check_profile(state: QueryState) -> dict:
    """
    Check whether the stored profile covers all used_columns.

    Returns:
    - {"profile_complete": True, "profile": ...}               if complete or skipped
    - {"profile_complete": False, "profile": ..., "missing_columns": [...]}  if not
    """
    session_id = state.get("session", "")
    used_columns = state.get("used_columns") or []

    # 0. If user previously skipped profiling for this session, skip immediately
    if session_id and _load_model_profile_skipped(session_id):
        return {"profile_complete": True, "profile": {}}

    # 1. Load stored profile from schema_cache
    profile = _normalize_profile(_load_model_profile()) or {
        "tables": {},
        "joins": [],
    }

    # 2. No used_columns → nothing to check
    if not used_columns:
        return {"profile_complete": True, "profile": profile}

    # 3. Find which columns still need profiling
    missing = _find_missing_columns(profile, used_columns)

    if not missing:
        return {"profile_complete": True, "profile": profile}

    return {"profile_complete": False, "profile": profile, "missing_columns": missing}


async def build_profile_request(state: QueryState, missing: list) -> dict:
    """
    Build the profile SQL request for the given missing columns.

    Returns profile_sql, missing_columns (resolved), expected_joins, profile_billing_tb.
    """
    project_id = state["project"]
    schemas = state.get("schemas") or []
    if not schemas:
        from models.schemas import get_schemas

        try:
            schemas = await get_schemas(project_id=project_id)
        except Exception:
            schemas = []
    dialect = state.get("dialect", "bigquery")
    sql_query = state.get("query") or None
    missing_resolved = _resolve_full_table_names(missing, schemas)
    profile_sql = build_profile_query(
        schema=_to_profiler_schema(schemas),
        used_columns=missing_resolved,
        dialect=dialect,
        sql_query=sql_query,
    )
    expected_joins = _extract_expected_join_pairs(sql_query or "", dialect)

    profile_billing_tb: Optional[float] = None
    if dialect == "bigquery" and profile_sql:
        from models.env_variables import BQ_SCHEMA_BILLING_PROJECT

        billing_project = BQ_SCHEMA_BILLING_PROJECT
        if billing_project:
            profile_billing_tb = await _estimate_profile_bytes(
                profile_sql, billing_project
            )

    return {
        "profile_sql": profile_sql,
        "missing_columns": missing_resolved,
        "expected_joins": expected_joins,
        "profile_billing_tb": profile_billing_tb,
    }


async def check_and_request_profile(state: QueryState):
    """
    LangGraph node inserted between parser and generator.

    - Loads stored profile from DB.
    - Checks if all used_columns are covered.
    - If complete  → profile_complete=True, passes profile to state.
    - If incomplete → returns profile_sql to request missing data (profile_complete=False).
    """
    checked = await check_profile(state)
    if checked["profile_complete"]:
        return checked

    request = await build_profile_request(state, checked["missing_columns"])
    return {
        "profile_complete": False,
        "profile": checked["profile"],
        **request,
    }
