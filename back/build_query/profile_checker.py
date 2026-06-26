import asyncio
import json
from typing import Optional


from build_query.profiler import (
    _join_pair_key,
    build_profile_query,
    build_profile_queries_labeled,
)
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
            incoming_window = tbl_data.get("partition_window")
            if incoming_window is not None:
                merged["tables"][tbl]["partition_window"] = incoming_window

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


def enrich_tables_with_partition_window(
    profile: Optional[dict], schemas: list, partition_limit: int | None = 3
) -> Optional[dict]:
    """Attach ``partition_window`` metadata to each profiled table.

    Sourced from the stored schema's ``partition`` info — the flat profile rows
    don't carry it, so the server path (`/auto-profile`) enriches here at
    storage time. Lets the generator distinguish "the warehouse only holds the
    last 3 days" from "profiling only scanned the last 3 partitions".
    """
    if not profile or not profile.get("tables"):
        return profile
    from build_query.profiler import build_partition_window

    part_by_name: dict[str, dict] = {}
    for s in schemas:
        full = s.get("table_name") or s.get("name", "")
        part = s.get("partition")
        if full and part:
            part_by_name[full] = part
            part_by_name[full.split(".")[-1]] = part

    for tbl_key, tbl_data in profile["tables"].items():
        part = part_by_name.get(tbl_key) or part_by_name.get(tbl_key.split(".")[-1])
        if not part:
            continue
        window = build_partition_window(part, partition_limit)
        if window:
            tbl_data["partition_window"] = window
    return profile


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


def _find_missing_joins(profile: dict, expected_joins: list) -> list:
    """
    Returns the expected join pairs whose table-set is not yet in the profile.

    Matching is order-independent: the key is the set of tables used in the join
    (``a JOIN b`` is considered covered by an already-profiled ``b JOIN a``), so
    a join already present in ``profile["joins"]`` is excluded from what needs
    re-profiling.
    """
    profiled_keys = {
        _join_pair_key(j.get("left_table", ""), j.get("right_table", ""))
        for j in profile.get("joins", [])
    }
    missing = []
    for exp_join in expected_joins or []:
        key = _join_pair_key(
            exp_join.get("left_table", ""), exp_join.get("right_table", "")
        )
        if key not in profiled_keys:
            missing.append(exp_join)

    return missing


async def _estimate_profile_bytes(sql: str, billing_project: str) -> Optional[float]:
    """Dry-run the profile SQL on BigQuery and return estimated bytes processed (as TB)."""
    try:
        from utils.optional_deps import import_bigquery

        _bq = import_bigquery()
        client = _bq.Client(project=billing_project)
        job_config = _bq.QueryJobConfig(dry_run=True, use_query_cache=False)
        job = await asyncio.to_thread(client.query, sql, job_config)
        return job.total_bytes_processed / 1e12
    except Exception:
        return None


async def check_profile(state: QueryState) -> dict:
    """
    Check whether the stored profile covers all used_columns *and* every join.

    A column is "covered" when it appears under its table in ``profile["tables"]``.
    A join is "covered" when a profiled join relates the same set of tables
    (order-independent — see :func:`_find_missing_joins`). Anything already
    covered is excluded from what gets re-profiled.

    Returns:
    - {"profile_complete": True, "profile": ...}               if complete or skipped
    - {"profile_complete": False, "profile": ..., "missing_columns": [...],
       "missing_joins": [...]}                                  if not
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

    # 2. Find which joins still need profiling (table-set key, order-independent)
    sql_query = state.get("optimized_sql") or state.get("query") or ""
    dialect = state.get("dialect", "bigquery")
    expected_joins = _extract_expected_join_pairs(sql_query, dialect)
    missing_joins = _find_missing_joins(profile, expected_joins)

    # 3. No columns to check and no missing joins → nothing to do
    if not used_columns and not missing_joins:
        return {"profile_complete": True, "profile": profile}

    # 4. Find which columns still need profiling
    missing = _find_missing_columns(profile, used_columns)

    if not missing and not missing_joins:
        return {"profile_complete": True, "profile": profile}

    return {
        "profile_complete": False,
        "profile": profile,
        "missing_columns": missing,
        "missing_joins": missing_joins,
    }


async def build_profile_request(
    state: QueryState,
    missing: list,
    profile: Optional[dict] = None,
    budget_tb: Optional[float] = None,
) -> dict:
    """
    Build the profile SQL request for the given missing columns and joins.

    Joins already present in the stored profile (matched by table-set, regardless
    of order) are excluded from the generated SQL — only new joins are profiled.

    When *budget_tb* is provided (BigQuery only), each profile query is dry-run to
    estimate its scan; queries whose estimate exceeds the budget are **deferred**
    (left out of ``profile_queries``) and reported under ``deferred`` so the UI can
    surface a partial profile + a "compléter" affordance. ``budget_tb=None`` keeps
    the historical behaviour (profile everything).

    Returns profile_sql, profile_queries (within budget), missing_columns
    (resolved), expected_joins, profile_billing_tb, deferred, budget_tb.
    """
    # Parité avec get_profile_budget_tb() : un budget <= 0 (ou absent) signifie
    # « pas de budget » (on profile tout), pas « différer toutes les tables ».
    if budget_tb is not None and budget_tb <= 0:
        budget_tb = None

    if profile is None:
        profile = _normalize_profile(_load_model_profile()) or {
            "tables": {},
            "joins": [],
        }
    exclude_join_pairs = {
        _join_pair_key(j.get("left_table", ""), j.get("right_table", ""))
        for j in profile.get("joins", [])
    }

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
        exclude_join_pairs=exclude_join_pairs,
    )
    labeled = build_profile_queries_labeled(
        schema=profiler_schema,
        used_columns=missing_resolved,
        dialect=dialect,
        sql_query=sql_query,
        options=profiler_options,
        exclude_join_pairs=exclude_join_pairs,
    )
    # Only the not-yet-profiled joins are profiled, so validation must expect
    # exactly those (not the joins already covered by the stored profile).
    expected_joins = _find_missing_joins(
        profile, _extract_expected_join_pairs(sql_query or "", dialect)
    )

    # Per-query scan estimate (BigQuery only). Used both for the displayed total
    # and — when a budget is set — to defer the queries that would scan too much.
    per_query_tb: list[Optional[float]] = [None] * len(labeled)
    if dialect == "bigquery" and labeled:
        from models.env_variables import BQ_TEST_PROJECT

        billing_project = BQ_TEST_PROJECT
        if billing_project:
            billing_results = await asyncio.gather(
                *[_estimate_profile_bytes(sql, billing_project) for _, sql in labeled],
                return_exceptions=True,
            )
            for i, r in enumerate(billing_results):
                if isinstance(r, float):
                    per_query_tb[i] = r

    within_queries: list[str] = []
    deferred: list[dict] = []
    within_total = 0.0
    have_estimate = False
    for (label, sql), est in zip(labeled, per_query_tb):
        if est is not None:
            have_estimate = True
        # Defer only when we have an estimate AND it exceeds an explicit budget.
        # Unknown estimates (None) are always profiled — the partition window
        # already bounds their cost.
        if budget_tb is not None and est is not None and est > budget_tb:
            deferred.append({"scope": label, "billing_tb": round(est, 4)})
        else:
            within_queries.append(sql)
            if est is not None:
                within_total += est

    profile_billing_tb: Optional[float] = (
        round(within_total, 4) if have_estimate and within_total > 0 else None
    )

    return {
        "profile_sql": profile_sql,
        "profile_queries": within_queries,
        "missing_columns": missing_resolved,
        "expected_joins": expected_joins,
        "profile_billing_tb": profile_billing_tb,
        # Tables/relations dont le scan estimé dépasse le budget : non profilées,
        # complétables à la demande ("Compléter le profil").
        "deferred": deferred,
        "budget_tb": budget_tb,
        # Echoed back so /auto-profile records the same window the SQL scanned,
        # instead of defaulting to 3.
        "partition_limit": partition_limit,
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

    request = await build_profile_request(
        state, checked["missing_columns"], profile=checked.get("profile")
    )
    return {
        "profile_complete": False,
        "profile": checked["profile"],
        **request,
    }
