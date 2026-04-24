import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from storage.config import get_mocksql_dir, get_models_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tests_root() -> Path:
    return get_mocksql_dir() / "tests"


def _model_dir(model_name: str) -> Path:
    d = _tests_root() / model_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def _test_path(model_name: str, session_id: str) -> Path:
    return _model_dir(model_name) / f"{session_id}.json"


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(p: Path, data: Dict[str, Any]) -> None:
    p.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# Models (SQL files)
# ---------------------------------------------------------------------------


def list_models() -> List[Dict[str, str]]:
    """Return all .sql files found in models_path."""
    models_path = get_models_path()
    if not models_path.exists():
        return []
    return [{"name": p.stem, "path": str(p)} for p in sorted(models_path.glob("*.sql"))]


def read_model_sql(model_name: str) -> Optional[str]:
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return None
    return sql_file.read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def list_tests(model_name: str) -> List[Dict[str, Any]]:
    d = _tests_root() / model_name
    if not d.exists():
        return []
    results = []
    for f in sorted(d.glob("*.json")):
        data = _read_json(f)
        if data:
            results.append(data)
    return results


def get_test(
    session_id: str, model_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Find a test by session ID. If model_name is known, does a direct lookup; otherwise scans all."""
    if model_name:
        p = _test_path(model_name, session_id)
        return _read_json(p) if p.exists() else None

    root = _tests_root()
    if not root.exists():
        return None
    for model_dir in root.iterdir():
        if not model_dir.is_dir():
            continue
        p = model_dir / f"{session_id}.json"
        if p.exists():
            return _read_json(p)
    return None


def create_test(model_name: str) -> Dict[str, Any]:
    """Create a new empty test file for a model. Returns the test dict."""
    test_id = str(uuid.uuid4())
    now = datetime.now().isoformat()
    sql = read_model_sql(model_name) or ""
    test: Dict[str, Any] = {
        "test_id": test_id,
        "model_name": model_name,
        "test_name": None,
        "created_at": now,
        "updated_at": now,
        "sql": sql,
        "optimized_sql": "",
        "used_columns": [],
        "profile": {},
        "profile_skipped": False,
        "last_error": "",
        "test_cases": [],
    }
    p = _test_path(model_name, test_id)
    _write_json(p, test)
    return test


def update_test(
    session_id: str, updates: Dict[str, Any], model_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Merge `updates` into the test file. Returns updated test or None if not found."""
    test = get_test(session_id, model_name)
    if test is None:
        return None
    mn = test["model_name"]
    test.update(updates)
    test["updated_at"] = datetime.now().isoformat()
    _write_json(_test_path(mn, session_id), test)
    return test


def delete_test(session_id: str, model_name: str) -> bool:
    p = _test_path(model_name, session_id)
    if not p.exists():
        return False
    p.unlink()
    return True


def merge_test_cases(
    session_id: str,
    new_cases: List[Dict[str, Any]],
    rerun_all: bool = False,
    model_name: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Merge new test_cases into the existing test file (by test_index), or replace all if rerun_all."""
    test = get_test(session_id, model_name)
    if test is None:
        return None

    if rerun_all:
        merged = new_cases
    else:
        existing: Dict[str, Any] = {
            item["test_index"]: item for item in test.get("test_cases", [])
        }
        for case in new_cases:
            existing[case["test_index"]] = case
        merged = sorted(existing.values(), key=lambda x: int(str(x["test_index"])))

    return update_test(
        session_id, {"test_cases": merged}, model_name=test["model_name"]
    )
