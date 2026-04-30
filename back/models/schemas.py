import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from models.env_variables import SCHEMA_CACHE_PATH

_cache: Optional[List[Dict[str, Any]]] = None
_cache_time: Optional[datetime] = None
_profile_cache: Optional[Dict[str, Any]] = None
CACHE_EXPIRATION = timedelta(minutes=10)

# ---------------------------------------------------------------------------
# File I/O — schema_cache.json stores {"tables": [...], "profile": {...}}
# ---------------------------------------------------------------------------


def _load_raw() -> Dict[str, Any]:
    p = Path(SCHEMA_CACHE_PATH)
    if not p.exists():
        return {"tables": [], "profile": {}}
    with open(p) as f:
        data = json.load(f)
    # Migrate: old format was a plain list of tables
    if isinstance(data, list):
        return {"tables": data, "profile": {}}
    return data


def _save_raw(data: Dict[str, Any]) -> None:
    p = Path(SCHEMA_CACHE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(data, f, indent=2)


def _load_from_file() -> List[Dict[str, Any]]:
    return _load_raw().get("tables", [])


def _save_to_file(tables: List[Dict[str, Any]]) -> None:
    raw = _load_raw()
    raw["tables"] = tables
    _save_raw(raw)


# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------


async def get_schemas(
    project_id: str = None, root_only: bool = False, **_
) -> List[Dict[str, Any]]:
    global _cache, _cache_time
    if _cache is not None and _cache_time is not None:
        if datetime.now() - _cache_time < CACHE_EXPIRATION:
            data = _cache
        else:
            _cache = None
            _cache_time = None
            data = None
    else:
        data = None

    if data is None:
        data = _load_from_file()
        _cache = data
        _cache_time = datetime.now()

    if root_only:
        data = [
            {
                **t,
                "columns": [
                    col for col in t["columns"] if "." not in col.get("name", "")
                ],
            }
            for t in data
        ]
    return data


def save_schemas(new_tables: List[Dict[str, Any]]) -> None:
    global _cache, _cache_time
    existing = _load_from_file()
    by_name = {t["table_name"]: t for t in existing}
    for tbl in new_tables:
        by_name[tbl["table_name"]] = tbl
    _save_to_file(list(by_name.values()))
    _cache = None
    _cache_time = None


def invalidate_project_cache(project_id: str = None):
    global _cache, _cache_time
    _cache = None
    _cache_time = None


# ---------------------------------------------------------------------------
# Profile cache (column stats + join data, shared across all models)
# ---------------------------------------------------------------------------


def get_profile() -> Optional[Dict[str, Any]]:
    """Return the stored profile dict (tables + joins) from schema_cache."""
    global _profile_cache
    if _profile_cache is not None:
        return _profile_cache
    profile = _load_raw().get("profile") or {}
    _profile_cache = profile or None
    return _profile_cache


def save_profile(profile: Dict[str, Any]) -> None:
    """Persist profile to schema_cache (merges with existing)."""
    global _profile_cache
    raw = _load_raw()
    raw["profile"] = profile
    _save_raw(raw)
    _profile_cache = profile


# ---------------------------------------------------------------------------
# Sample values cache (LLM-generated values for unconstrained columns)
# Stored as {"table.col": ["val1", "val2", ...]} under "sample_values" key.
# No in-memory cache — reads are infrequent and values are stable.
# ---------------------------------------------------------------------------


def get_sample_values() -> Dict[str, List[Any]]:
    """Return the stored sample_values dict from schema_cache."""
    return _load_raw().get("sample_values") or {}


def save_sample_values(key: str, values: List[Any]) -> None:
    """Persist sample values for one column key (format: 'table.col')."""
    raw = _load_raw()
    existing = raw.get("sample_values") or {}
    existing[key] = values
    raw["sample_values"] = existing
    _save_raw(raw)
