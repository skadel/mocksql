from __future__ import annotations

from collections import defaultdict
from typing import List, Dict, Any

from pydantic import ValidationError

from utils import ProjectSchema


def _norm_desc(desc: Any) -> str:
    return (desc or "").strip()


def _get(obj: Any, key: str, default=None):
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _canonicalize_columns(columns: List[Dict[str, Any]]):
    return tuple(
        sorted(
            (
                c.get("name", ""),
                c.get("type", ""),
                _norm_desc(c.get("description", "")),
            )
            for c in (columns or [])
            if c.get("name") and c.get("type")
        )
    )


def update_schema(
    old_schema: List[Dict[str, Any]],
    new_schema: List[Dict[str, Any]],
    *,
    remove_missing: bool = False,
) -> List[Dict[str, Any]]:
    old_by_name = {
        t.get("table_name"): t for t in (old_schema or []) if t.get("table_name")
    }
    new_by_name = {
        t.get("table_name"): t for t in (new_schema or []) if t.get("table_name")
    }

    merged: Dict[str, Dict[str, Any]] = {}
    merged.update(old_by_name)

    for name, new_tbl in new_by_name.items():
        old_tbl = old_by_name.get(name)
        if old_tbl is None:
            merged[name] = new_tbl
            continue

        old_cols = _canonicalize_columns(old_tbl.get("columns", []))
        new_cols = _canonicalize_columns(new_tbl.get("columns", []))
        old_desc = _norm_desc(old_tbl.get("description", ""))
        new_desc = _norm_desc(new_tbl.get("description", ""))

        if (old_cols != new_cols) or (old_desc != new_desc):
            merged[name] = new_tbl
        else:
            merged[name] = old_tbl

    if remove_missing:
        for name in list(merged.keys()):
            if name not in new_by_name:
                del merged[name]

    return [merged[k] for k in sorted(merged.keys())]


def generate_tables_and_columns_from_project_schema(
    project_schema: Dict[str, Any],
) -> List[Dict[str, Any]]:
    try:
        validated_schema = ProjectSchema(**project_schema)
    except ValidationError as e:
        raise ValueError(f"Invalid project schema: {e}")

    if not validated_schema.data:
        return []

    grouped_columns = defaultdict(list)
    for column in validated_schema.data:
        cat = _get(column, "table_catalog", "")
        sch = _get(column, "table_schema", "")
        tbl = _get(column, "table_name", "")
        grouped_columns[(cat, sch, tbl)].append(column)

    tables_and_columns: List[Dict[str, Any]] = []

    for (catalog, schema, table_name), columns in grouped_columns.items():
        tbl_desc = ""
        for col in columns:
            td = _norm_desc(_get(col, "table_description", ""))
            if td:
                tbl_desc = td
                break

        full_table_name = f"{catalog}.{schema}.{table_name}".strip(".")
        table_info = {
            "table_name": full_table_name,
            "description": tbl_desc,
            "columns": [],
        }

        for col in columns:
            name = _get(col, "field_path", "")
            dtype = _get(col, "data_type", "")
            if not name or not dtype:
                continue
            entry: dict = {
                "name": name,
                "type": dtype,
                "mode": _get(col, "mode", "NULLABLE"),
                "description": _norm_desc(_get(col, "description", "")),
            }
            if bq_ddl_type := _get(col, "bq_ddl_type", ""):
                entry["bq_ddl_type"] = bq_ddl_type
            table_info["columns"].append(entry)

        tables_and_columns.append(table_info)

    return tables_and_columns
