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


def generate_faker_rows(
    schema: list,
    faker_cols_by_uc_key: dict[str, set[str]],
    filled_data: dict[str, list],
) -> dict[str, list[dict]]:
    """Generate Faker rows for unconstrained columns.

    Args:
        schema: Full project schema (list of table dicts with columns).
        faker_cols_by_uc_key: Mapping from uc_key (database_table) to the set of
            column names that should be Faker-generated.
        filled_data: LLM-generated data keyed by uc_key; used to determine row count.

    Returns:
        Mapping from uc_key to list of row dicts containing only the Faker columns.
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
        llm_rows = filled_data.get(uc_key) or []
        n_rows = len(llm_rows) if llm_rows else default_n
        rows = []
        for _ in range(n_rows):
            row = {}
            for col in col_names:
                bq_type = col_types.get(col, "STRING")
                row[col] = _fake_value_for_type(bq_type)
            rows.append(row)
        result[uc_key] = rows
    return result
