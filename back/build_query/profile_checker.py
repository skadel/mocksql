import asyncio
import json
from typing import Optional


from build_query.profiler import build_profile_query, build_profile_queries
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
    _COL_SKIP = {
        "row_type",
        "table_name",
        "col_name",
        "left_table",
        "right_table",
        "left_expr",
        "right_expr",
        "join_type",
        "left_match_rate",
        "avg_right_per_left_key",
        "max_right_per_left_key",
        "left_key_sample",
        "right_where_sql",
    }
    # Fields that belong to column profiling and are NULL noise in join rows
    _JOIN_COL_NOISE = {
        "row_type",
        "table_name",
        "col_name",
        "total_count",
        "null_count",
        "non_null_count",
        "distinct_count",
        "dup_count",
        "min_val",
        "max_val",
        "top_values",
    }
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
            col_data = {k: v for k, v in row.items() if k not in _COL_SKIP}
            tv = col_data.get("top_values")
            if isinstance(tv, str) and tv:
                sep = "|||"
                col_data["top_values"] = [v.strip() for v in tv.split(sep) if v.strip()]
            tables[tbl]["columns"][col] = col_data
        elif row_type == "join":
            joins.append({k: v for k, v in row.items() if k not in _JOIN_COL_NOISE})
        elif row_type == "derived_expr":
            src_str = row.get("table_name") or ""
            expr_sql = row.get("col_name") or ""
            top_raw = row.get("top_values") or ""
            if not (src_str and expr_sql):
                continue
            sep = "|||"
            top_vals = [v.strip() for v in top_raw.split(sep) if v.strip()]
            for tbl in src_str.split(","):
                tbl = tbl.strip()
                if not tbl:
                    continue
                if tbl not in tables:
                    tables[tbl] = {"columns": {}}
                tables[tbl].setdefault("derived_expressions", []).append(
                    {"expr_sql": expr_sql, "top_values": top_vals}
                )
    return {"tables": tables, "joins": joins}


def _merge_profiles(base: Optional[dict], incoming: Optional[dict]) -> dict:
    """Merge two profile dicts.

    - Columns: incoming overrides base at column level.
    - Joins: accumulated by (left_table, right_table, left_expr, right_expr) key.
      Different join variants between the same tables (e.g. direct vs CTE-deduplicated)
      are kept as separate entries in the list.
    """
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
            incoming_derived = tbl_data.get("derived_expressions")
            if incoming_derived is not None:
                merged["tables"][tbl]["derived_expressions"] = incoming_derived

    # Accumulate joins: deduplicate by the four-key signature, keep all variants
    existing_join_keys: set[tuple] = {
        (
            j.get("left_table"),
            j.get("right_table"),
            j.get("left_expr"),
            j.get("right_expr"),
        )
        for j in merged.get("joins", [])
    }
    for j in incoming.get("joins", []):
        key = (
            j.get("left_table"),
            j.get("right_table"),
            j.get("left_expr"),
            j.get("right_expr"),
        )
        if key not in existing_join_keys:
            merged.setdefault("joins", []).append(j)
            existing_join_keys.add(key)

    return merged


def _extract_cte_map(sql: str, dialect: str = "bigquery") -> dict[str, str]:
    """Return {cte_name_lower: cte_sql_pretty} for all CTEs found in *sql*."""
    if not sql:
        return {}
    try:
        import sqlglot
        import sqlglot.expressions as _exp

        tree = sqlglot.parse_one(sql, dialect=dialect)
        ctes: dict[str, str] = {}
        for cte in tree.find_all(_exp.CTE):
            name = cte.alias_or_name.lower()
            ctes[name] = cte.this.sql(dialect=dialect, pretty=True)
        return ctes
    except Exception:
        return {}


def enrich_joins_with_cte_context(
    joins: list, sql: str, dialect: str = "bigquery"
) -> list:
    """Add left_cte_sql / right_cte_sql to join entries whose table is a CTE.

    Called once at profile-storage time (phase 1) so the profile is self-contained
    and the CTE context is available at format time without re-parsing the SQL.
    """
    if not joins or not sql:
        return joins
    cte_map = _extract_cte_map(sql, dialect)
    if not cte_map:
        return joins
    enriched = []
    for j in joins:
        j = dict(j)
        for side in ("left", "right"):
            tbl = j.get(f"{side}_table", "")
            short = tbl.split(".")[-1].lower()
            cte_sql = cte_map.get(short)
            if cte_sql:
                j[f"{side}_cte_sql"] = cte_sql
        enriched.append(j)
    return enriched


def _to_profiler_schema(schemas: list) -> dict:
    """Convert state["schemas"] (list of table dicts with "table_name") to
    the {"tables": [{"name": ..., "columns": [...], "partition": ...}]} format
    expected by profiler.

    Always uses the fully-qualified table name so the generated SQL FROM clause
    is correct (e.g. project.dataset.country_summary, not just country_summary).
    """
    tables = []
    for t in schemas:
        full_name = t.get("table_name") or t.get("name", "")
        entry: dict = {"name": full_name, "columns": t.get("columns", [])}
        if t.get("partition"):
            entry["partition"] = t["partition"]
        tables.append(entry)
    return {"tables": tables}


def _resolve_full_table_names(used_columns: list, schemas: list) -> list:
    """Replace short table names in used_columns with fully-qualified names.

    The validator stores short names (e.g. "country_summary") in used_columns,
    but the SQL FROM clause needs the full name ("project.dataset.country_summary").
    """
    from models.schemas import get_schema_by_name

    def _full_name(tbl: str) -> str:
        t = get_schema_by_name(tbl)
        if t:
            return t.get("table_name") or t.get("name", tbl)
        # fallback: scan the passed-in list (e.g. root_only filtered schemas)
        for s in schemas:
            full = s.get("table_name") or s.get("name", "")
            if full == tbl or full.split(".")[-1] == tbl:
                return full
        return tbl

    resolved = []
    for entry in used_columns:
        if isinstance(entry, str):
            entry = json.loads(entry)
        resolved.append(
            {
                "table": _full_name(entry["table"]),
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
    sql_query = state.get("optimized_sql") or state.get("query") or None
    missing_resolved = _resolve_full_table_names(missing, schemas)
    partition_limit: int | None = state.get("profile_partition_limit", 3)
    profiler_schema = _to_profiler_schema(schemas)
    profiler_options = {"partition_limit": partition_limit}
    profile_sql = build_profile_query(
        schema=profiler_schema,
        used_columns=missing_resolved,
        dialect=dialect,
        sql_query=sql_query,
        options=profiler_options,
    )
    queries = build_profile_queries(
        schema=profiler_schema,
        used_columns=missing_resolved,
        dialect=dialect,
        sql_query=sql_query,
        options=profiler_options,
    )
    expected_joins = _extract_expected_join_pairs(sql_query or "", dialect)

    profile_billing_tb: Optional[float] = None
    if dialect == "bigquery" and queries:
        from models.env_variables import BQ_TEST_PROJECT

        billing_project = BQ_TEST_PROJECT
        if billing_project:
            billing_results = await asyncio.gather(
                *[_estimate_profile_bytes(q, billing_project) for q in queries],
                return_exceptions=True,
            )
            total: float = 0.0
            for r in billing_results:
                if isinstance(r, float):
                    total += r
            profile_billing_tb = total if total > 0 else None

    return {
        "profile_sql": profile_sql,
        "profile_queries": queries,
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
