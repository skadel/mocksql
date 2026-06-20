import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

from models.env_variables import SCHEMA_CACHE_PATH, PROFILE_CACHE_PATH

_cache: Optional[List[Dict[str, Any]]] = None
_cache_by_name: Optional[Dict[str, Dict[str, Any]]] = None
_cache_time: Optional[datetime] = None
_profile_cache: Optional[Dict[str, Any]] = None
CACHE_EXPIRATION = timedelta(minutes=10)

# ---------------------------------------------------------------------------
# File I/O — schema_cache.json stores {"tables": [...]} (COMMITÉ, sans PII).
# Le profil + sample_values (valeurs brutes) vivent dans profile.json (gitignoré).
# ---------------------------------------------------------------------------


def _load_raw() -> Dict[str, Any]:
    p = Path(SCHEMA_CACHE_PATH)
    if not p.exists():
        return {"tables": []}
    with open(p) as f:
        data = json.load(f)
    # Migrate: old format was a plain list of tables
    if isinstance(data, list):
        return {"tables": data}
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
    # Ne jamais (ré)écrire le profil dans le cache schéma commité.
    raw.pop("profile", None)
    raw.pop("sample_values", None)
    _save_raw(raw)


# ---------------------------------------------------------------------------
# Table schemas
# ---------------------------------------------------------------------------


def _build_name_index(tables: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index tables by full name and short name (last segment after '.')."""
    index: Dict[str, Dict[str, Any]] = {}
    for t in tables:
        full = t.get("table_name") or t.get("name", "")
        if full:
            index[full] = t
            short = full.split(".")[-1]
            if short != full:
                index.setdefault(short, t)
    return index


async def get_schemas(
    project_id: str = None, root_only: bool = False, **_
) -> List[Dict[str, Any]]:
    global _cache, _cache_by_name, _cache_time
    if _cache is not None and _cache_time is not None:
        if datetime.now() - _cache_time < CACHE_EXPIRATION:
            data = _cache
        else:
            _cache = None
            _cache_by_name = None
            _cache_time = None
            data = None
    else:
        data = None

    if data is None:
        data = _load_from_file()
        _cache = data
        _cache_by_name = _build_name_index(data)
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


def get_schema_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Return a table dict by full or short name, or None if not found.

    Uses the in-memory index — O(1) vs O(n) scan over the list.
    Falls back to disk if the cache has not been populated yet.
    """
    global _cache_by_name
    if _cache_by_name is None:
        _cache_by_name = _build_name_index(_load_from_file())
    return _cache_by_name.get(name)


def save_schemas(new_tables: List[Dict[str, Any]]) -> None:
    global _cache, _cache_by_name, _cache_time
    existing = _load_from_file()
    by_name = {t["table_name"]: t for t in existing}
    for tbl in new_tables:
        by_name[tbl["table_name"]] = tbl
    _save_to_file(list(by_name.values()))
    _cache = None
    _cache_by_name = None
    _cache_time = None


def invalidate_project_cache(project_id: str = None):
    global _cache, _cache_by_name, _cache_time
    _cache = None
    _cache_by_name = None
    _cache_time = None


# ---------------------------------------------------------------------------
# Profile cache (column stats + join data + sample_values, shared across models)
#
# Stocké SÉPARÉMENT dans profile.json (gitignoré) car il contient des valeurs
# brutes issues de l'entrepôt (top_values, left_key_sample, sample_values) = PII.
# Ce cache n'est jamais commité et n'est PAS requis pour le réplay CI.
# Format: {"profile": {...}, "sample_values": {"table.col": [...]}}.
# ---------------------------------------------------------------------------


def _load_profile_raw() -> Dict[str, Any]:
    """Charge profile.json en migrant au passage un schema_cache.json legacy."""
    _migrate_legacy_profile()
    p = Path(PROFILE_CACHE_PATH)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
        return json.load(f) or {}


def _save_profile_raw(data: Dict[str, Any]) -> None:
    p = Path(PROFILE_CACHE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _migrate_legacy_profile() -> None:
    """Déplace un profil/sample_values inline d'un schema_cache.json legacy vers
    profile.json, puis réécrit schema_cache.json sans ces clés. Idempotent."""
    sc_path = Path(SCHEMA_CACHE_PATH)
    if not sc_path.exists():
        return
    with open(sc_path, encoding="utf-8") as f:
        sc = json.load(f)
    if not isinstance(sc, dict) or ("profile" not in sc and "sample_values" not in sc):
        return
    legacy_profile = sc.pop("profile", None)
    legacy_samples = sc.pop("sample_values", None)

    prof_path = Path(PROFILE_CACHE_PATH)
    prof_raw: Dict[str, Any] = {}
    if prof_path.exists():
        with open(prof_path, encoding="utf-8") as f:
            prof_raw = json.load(f) or {}
    # profile.json (plus récent) gagne ; on ne réinjecte le legacy que s'il manque.
    if legacy_profile and not prof_raw.get("profile"):
        prof_raw["profile"] = legacy_profile
    if legacy_samples and not prof_raw.get("sample_values"):
        prof_raw["sample_values"] = legacy_samples
    _save_profile_raw(prof_raw)
    _save_raw(sc)


def get_profile() -> Optional[Dict[str, Any]]:
    """Return the stored profile dict (tables + joins) from profile.json."""
    global _profile_cache
    if _profile_cache is not None:
        return _profile_cache
    profile = _load_profile_raw().get("profile") or {}
    _profile_cache = profile or None
    return _profile_cache


def save_profile(profile: Dict[str, Any]) -> None:
    """Persist profile to profile.json (gitignoré)."""
    global _profile_cache
    raw = _load_profile_raw()
    raw["profile"] = profile
    raw["profiled_at"] = datetime.now().astimezone().isoformat()
    _save_profile_raw(raw)
    _profile_cache = profile


def get_profiled_at() -> Optional[str]:
    """Return the ISO timestamp of the last profiling run, or None if never profiled."""
    return _load_profile_raw().get("profiled_at")


def get_sample_values() -> Dict[str, List[Any]]:
    """Return the stored sample_values dict from profile.json."""
    return _load_profile_raw().get("sample_values") or {}


def save_sample_values(key: str, values: List[Any]) -> None:
    """Persist sample values for one column key (format: 'table.col')."""
    raw = _load_profile_raw()
    existing = raw.get("sample_values") or {}
    existing[key] = values
    raw["sample_values"] = existing
    _save_profile_raw(raw)
