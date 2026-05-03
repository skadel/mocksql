"""
sparse_filler.py — Post-generation filler for unconstrained columns.

Unconstrained columns (present in used_columns but absent from both
source_columns and derived_columns in SimplificationResult) are excluded
from LLM generation to reduce token usage. This module builds a value pool
for those columns and fills them into the generated data rows.

Value pool priority per column:
  1. profile top_values         → sample from real BigQuery data
  2. profile min_value/max_value → generate values in range (numeric/date)
  3. sample_values in schema_cache → reuse previously LLM-generated values
  4. LLM call (lightweight)     → generate + persist to schema_cache
"""

from __future__ import annotations

import json
import random
from datetime import date, timedelta
from typing import Any

from langchain_core.messages import HumanMessage


# ---------------------------------------------------------------------------
# Pool construction
# ---------------------------------------------------------------------------

_POOL_SIZE = 8


def _pool_from_top_values(top_values: list[str]) -> list[Any]:
    """Parse the profiler's top_values strings into typed Python values."""
    parsed = []
    for v in top_values:
        if v is None:
            continue
        s = str(v).strip()
        try:
            parsed.append(int(s))
            continue
        except ValueError:
            pass
        try:
            parsed.append(float(s))
            continue
        except ValueError:
            pass
        parsed.append(s)
    return parsed or [None]


def _pool_from_range(min_val: Any, max_val: Any, col_type: str) -> list[Any]:
    """Generate _POOL_SIZE values uniformly between min_val and max_val."""
    t = col_type.upper()
    try:
        if any(k in t for k in ("INT", "NUMERIC", "BIGNUMERIC", "FLOAT", "REAL")):
            lo, hi = float(str(min_val)), float(str(max_val))
            if lo == hi:
                return [lo] * _POOL_SIZE
            step = (hi - lo) / (_POOL_SIZE - 1)
            vals = [lo + i * step for i in range(_POOL_SIZE)]
            if "INT" in t:
                return [int(v) for v in vals]
            return [round(v, 4) for v in vals]
        if any(k in t for k in ("DATE", "TIMESTAMP", "DATETIME")):
            lo = date.fromisoformat(str(min_val)[:10])
            hi = date.fromisoformat(str(max_val)[:10])
            span = (hi - lo).days
            if span <= 0:
                return [str(lo)] * _POOL_SIZE
            step = max(1, span // (_POOL_SIZE - 1))
            return [str(lo + timedelta(days=i * step)) for i in range(_POOL_SIZE)]
    except Exception:
        pass
    return []


async def _llm_sample_values(
    col_name: str, col_type: str, table_name: str, llm
) -> list[Any]:
    """Ask the LLM to produce _POOL_SIZE realistic values for a column."""
    prompt = (
        f'Generate exactly {_POOL_SIZE} realistic sample values for column "{col_name}" '
        f'of SQL type {col_type} in table "{table_name}". '
        f"Return ONLY a JSON array of values, no explanation."
    )
    try:
        response = await llm.ainvoke([HumanMessage(content=prompt)])
        content = response.content.strip()
        # Extract JSON array even if wrapped in markdown
        start = content.find("[")
        end = content.rfind("]") + 1
        if start >= 0 and end > start:
            return json.loads(content[start:end])
    except Exception:
        pass
    return []


async def build_unconstrained_pool(
    unconstrained_cols: list[dict],
    profile: dict | None,
    llm,
) -> dict[str, list[Any]]:
    """Build a value pool for each unconstrained column.

    Args:
        unconstrained_cols: [{table, col_name, col_type}, ...]
        profile: normalized profile dict {"tables": {table: {"columns": {col: stats}}}}
        llm: LangChain LLM instance

    Returns:
        {"table.col_name": [val1, val2, ...]}
    """
    from models.schemas import get_sample_values, save_sample_values

    stored = get_sample_values()
    pool: dict[str, list[Any]] = {}

    for entry in unconstrained_cols:
        table = entry["table"]
        col = entry["col_name"]
        col_type = entry.get("col_type", "STRING")
        key = f"{table}.{col}"

        col_stats: dict = {}
        if profile:
            tbl_data = profile.get("tables", {}).get(table) or profile.get(
                "tables", {}
            ).get(table.split(".")[-1], {})
            col_stats = tbl_data.get("columns", {}).get(col, {})

        # 1. top_values from profile
        top_values = col_stats.get("top_values") or []
        if top_values:
            pool[key] = _pool_from_top_values(top_values)
            continue

        # 2. min/max range from profile
        min_val = col_stats.get("min_value")
        max_val = col_stats.get("max_value")
        if min_val is not None and max_val is not None:
            range_pool = _pool_from_range(min_val, max_val, col_type)
            if range_pool:
                pool[key] = range_pool
                continue

        # 3. previously persisted sample_values
        if key in stored:
            pool[key] = stored[key]
            continue

        # 4. LLM fallback — generate and persist
        values = await _llm_sample_values(col, col_type, table, llm)
        if values:
            save_sample_values(key, values)
            pool[key] = values

    return pool


# ---------------------------------------------------------------------------
# Filler
# ---------------------------------------------------------------------------


def fill_unconstrained(data: dict, pool: dict[str, list[Any]]) -> dict:
    """Add unconstrained column values to each row in data.

    Args:
        data: {"qualified_table_name": [row_dict, ...], ...}
              The table keys use the underscore-qualified format from filter_columns
              (e.g. "dataset_orders"), NOT the original dot-separated name.
        pool: {"original_table.col_name": [val1, val2, ...]}

    Columns in pool that are already present in a row are NOT overwritten —
    the LLM may have generated them intentionally (shouldn't happen given the
    filtered model, but defensive).
    """
    if not pool:
        return data

    result: dict = {}
    for table_key, rows in data.items():
        if not isinstance(rows, list):
            result[table_key] = rows
            continue

        # Determine which pool keys apply to this table.
        # pool key format: "short_table.col" ; table_key format: "dataset_table"
        # Match on the table part (last segment of pool key matches last segment of table_key)
        applicable: dict[str, list[Any]] = {}
        for pool_key, values in pool.items():
            pool_table, col = pool_key.rsplit(".", 1)
            if table_key.endswith(pool_table) or table_key.endswith(
                pool_table.replace(".", "_")
            ):
                applicable[col] = values

        if not applicable:
            result[table_key] = rows
            continue

        filled_rows = []
        for row in rows:
            new_row = dict(row)
            for col, values in applicable.items():
                if col not in new_row or new_row[col] is None:
                    new_row[col] = random.choice(values)
            filled_rows.append(new_row)
        result[table_key] = filled_rows

    return result
