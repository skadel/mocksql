"""CLI schema cache helpers — shared by generate, profile, and any future CLI command."""

import json
from pathlib import Path


def load_schema_cache(cache_path: str) -> list[dict]:
    p = Path(cache_path)
    if not p.exists():
        return []
    with open(p) as f:
        return json.load(f)


def save_schema_cache(cache_path: str, tables: list[dict]) -> None:
    p = Path(cache_path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(tables, f, indent=2)


def merge_into_cache(existing: list[dict], new_tables: list[dict]) -> list[dict]:
    by_name = {t["table_name"]: t for t in existing}
    for tbl in new_tables:
        by_name[tbl["table_name"]] = tbl
    return list(by_name.values())


def match_refs_against_cache(
    refs: list, cached: list[dict]
) -> tuple[list[dict], list[str]]:
    """Return (matched_schemas, missing_qualified_refs).

    Cache lookup is case-insensitive; missing refs preserve original case so
    downstream BigQuery API calls get the correct dataset/table names.
    """
    cached_by_name = {t["table_name"].lower(): t for t in cached}

    matched: list[dict] = []
    missing: list[str] = []

    for ref in refs:
        parts = [p for p in [ref.catalog, ref.db, ref.name] if p]
        qualified_lower = ".".join(parts).lower()

        if qualified_lower in cached_by_name:
            matched.append(cached_by_name[qualified_lower])
            continue

        # Try suffix match (dataset.table or just table)
        candidates = [v for k, v in cached_by_name.items() if k.endswith(qualified_lower)]
        if candidates:
            matched.extend(candidates)
        else:
            missing.append(".".join(parts))  # preserve original case for BQ API

    return matched, missing
