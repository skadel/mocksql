import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from models.env_variables import SCHEMA_CACHE_PATH

_cache: Optional[List[Dict[str, Any]]] = None
_cache_time: Optional[datetime] = None
CACHE_EXPIRATION = timedelta(minutes=10)


def _load_from_file() -> List[Dict[str, Any]]:
    p = Path(SCHEMA_CACHE_PATH)
    if not p.exists():
        return []
    with open(p) as f:
        return json.load(f)


def _save_to_file(tables: List[Dict[str, Any]]) -> None:
    p = Path(SCHEMA_CACHE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w") as f:
        json.dump(tables, f, indent=2)


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
