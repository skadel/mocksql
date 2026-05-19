import random
from faker import Faker

_faker = Faker()


def _table_uc_key(table_name: str) -> str:
    """Convert a fully-qualified table name to the uc_key used in filtered_schema."""
    parts = table_name.split(".")
    return "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]


def _fake_value_for_type(bq_type: str):
    """Return a plausible fake value for the given BigQuery DDL type."""
    upper = bq_type.upper().strip()
    if upper.startswith("ARRAY<") or upper.startswith("STRUCT<"):
        return None
    if any(
        t in upper
        for t in ("INT64", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT", "INT")
    ):
        return random.randint(1, 10_000)
    if any(
        t in upper for t in ("FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL")
    ):
        return round(random.uniform(0.01, 9_999.99), 2)
    if "TIMESTAMP" in upper or "DATETIME" in upper:
        return _faker.date_time_between("-2y", "now").strftime("%Y-%m-%dT%H:%M:%S")
    if "DATE" in upper:
        return _faker.date_between("-2y", "today").isoformat()
    if "TIME" in upper:
        return _faker.time()
    if "BOOL" in upper:
        return random.choice([True, False])
    if "BYTES" in upper:
        return None
    return _faker.word()


def _build_profile_index(profile: dict | None) -> dict[str, dict[str, dict]]:
    """Build {uc_key: {col_name_lower: stats}} from profile."""
    if not profile or not profile.get("tables"):
        return {}
    index: dict[str, dict[str, dict]] = {}
    for tbl_key, tbl_data in profile["tables"].items():
        parts = tbl_key.split(".")
        uc_key = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        cols = tbl_data.get("columns", {})
        index[uc_key] = {col_name.lower(): stats for col_name, stats in cols.items()}
    return index


def _value_from_profile(stats: dict, bq_type: str):
    """Return a value drawn from profile stats, or None if not applicable."""
    top_values = stats.get("top_values") or []
    if top_values:
        return random.choice(top_values)

    min_v = (
        stats.get("min_value")
        if stats.get("min_value") is not None
        else stats.get("min_val")
    )
    max_v = (
        stats.get("max_value")
        if stats.get("max_value") is not None
        else stats.get("max_val")
    )
    if min_v is None or max_v is None:
        return None

    upper = bq_type.upper().strip()
    try:
        if any(
            t in upper
            for t in (
                "INT64",
                "INTEGER",
                "SMALLINT",
                "BIGINT",
                "TINYINT",
                "BYTEINT",
                "INT",
            )
        ):
            lo, hi = int(float(min_v)), int(float(max_v))
            return random.randint(lo, hi) if lo <= hi else lo
        if any(
            t in upper for t in ("FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL")
        ):
            lo, hi = float(min_v), float(max_v)
            return round(random.uniform(lo, hi), 2) if lo <= hi else lo
    except (ValueError, TypeError):
        pass
    return None


def generate_faker_rows(
    schema: list,
    faker_cols_by_uc_key: dict[str, set[str]],
    filled_data: dict[str, list],
    profile: dict | None = None,
) -> dict[str, list[dict]]:
    """Generate rows for unconstrained columns, preferring profile stats over Faker.

    Priority per column:
      1. top_values from profile → random.choice
      2. min/max from profile (int/float) → random in range
      3. Faker fallback

    Args:
        schema: Full project schema (list of table dicts with columns).
        faker_cols_by_uc_key: Mapping from uc_key (database_table) to the set of
            column names that should be filled.
        filled_data: LLM-generated data keyed by uc_key; used to determine row count.
        profile: Optional statistical profile dict (same structure as QueryState.profile).

    Returns:
        Mapping from uc_key to list of row dicts containing only the filled columns.
    """
    # Build a column-type index keyed by uc_key
    type_index: dict[str, dict[str, str]] = {}
    for table_entry in schema:
        key = _table_uc_key(table_entry["table_name"])
        if key not in faker_cols_by_uc_key:
            continue
        type_index[key] = {
            col["name"].lower(): col.get("bq_ddl_type") or col.get("type", "STRING")
            for col in table_entry["columns"]
        }

    profile_index = _build_profile_index(profile)

    # Determine row count from LLM data; fall back to 3
    default_n = 3
    if filled_data:
        for rows in filled_data.values():
            if isinstance(rows, list) and rows:
                default_n = len(rows)
                break

    result: dict[str, list[dict]] = {}
    for uc_key, col_names in faker_cols_by_uc_key.items():
        col_types = type_index.get(uc_key, {})
        col_profile = profile_index.get(uc_key, {})
        llm_rows = filled_data.get(uc_key) or []
        n_rows = len(llm_rows) if llm_rows else default_n
        rows = []
        for _ in range(n_rows):
            row = {}
            for col in col_names:
                bq_type = col_types.get(col, "STRING")
                stats = col_profile.get(col)
                val = _value_from_profile(stats, bq_type) if stats else None
                row[col] = val if val is not None else _fake_value_for_type(bq_type)
            rows.append(row)
        result[uc_key] = rows
    return result
