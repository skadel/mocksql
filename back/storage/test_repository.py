import hashlib
import json
import os
import re
import subprocess
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from storage.config import (
    get_mocksql_dir,
    get_models_path,
    get_preprocessor_fn,
    load_preprocessor_fn,
)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _tests_root() -> Path:
    return get_mocksql_dir() / "tests"


def _test_path(model_name: str) -> Path:
    """Single file per model: .mocksql/tests/{model_id}.json (supports nested paths)."""
    p = _tests_root() / f"{model_name}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def _read_json(p: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_json(p: Path, data: Dict[str, Any]) -> None:
    p.write_text(
        json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )


def _migrate_old_structure() -> None:
    """One-time migration: merge {model_name}/{session_id}.json → {model_name}.json.

    Old layout had one subdirectory per model, each containing multiple session UUID files.
    New layout stores a single flat file per model with all test_cases merged.
    Old directories are left in place (not deleted) after migration.
    """
    root = _tests_root()
    if not root.exists():
        return
    for model_dir in list(root.iterdir()):
        if not model_dir.is_dir():
            continue
        model_name = model_dir.name
        new_path = root / f"{model_name}.json"
        if new_path.exists():
            continue  # already migrated
        # Old-format dirs contained UUID-named session files; skip new nested model dirs.
        uuid_files = [
            f for f in sorted(model_dir.glob("*.json")) if _UUID_RE.match(f.stem)
        ]
        sessions = []
        for f in uuid_files:
            data = _read_json(f)
            if data:
                sessions.append(data)
        if not sessions:
            continue
        # Use the most recent session as the base; merge all test_cases by index
        sessions.sort(key=lambda x: x.get("updated_at", ""))
        base = sessions[-1].copy()
        all_cases: Dict[str, Any] = {}
        for s in sessions:
            for case in s.get("test_cases", []):
                all_cases[str(case.get("test_index", ""))] = case
        base["test_cases"] = sorted(
            all_cases.values(), key=lambda x: int(str(x.get("test_index", 0)))
        )
        _write_json(new_path, base)


# ---------------------------------------------------------------------------
# Models (SQL files)
# ---------------------------------------------------------------------------


def list_models() -> List[Dict[str, str]]:
    """Return all .sql files found recursively in models_path.

    The model ID is the relative path from models_path without the .sql suffix
    (e.g. "staging/orders"), so two files with the same name in different
    subdirectories are never confused.
    """
    models_path = get_models_path()
    if not models_path.exists():
        return []
    result = []
    for p in sorted(models_path.rglob("*.sql")):
        rel = p.relative_to(models_path).with_suffix("")
        model_id = rel.as_posix()  # forward slashes on all platforms
        result.append({"name": model_id, "path": str(p)})
    return result


def read_model_sql(model_name: str) -> Optional[str]:
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return None
    raw_sql = sql_file.read_text(encoding="utf-8")
    fn_ref = get_preprocessor_fn()
    if not fn_ref:
        return raw_sql
    fn = load_preprocessor_fn(fn_ref, Path(os.getcwd()))
    return fn(raw_sql)


def get_model_file_git_sha(model_name: str) -> Optional[str]:
    """Return the SHA of the last commit that touched the model's .sql file, or None."""
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return None
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%H", "--", str(sql_file)],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(sql_file.parent),
        )
        sha = result.stdout.strip()
        return sha or None
    except Exception:
        return None


def get_model_file_hash(model_name: str) -> Optional[str]:
    """Return a short SHA-256 hash of the model file's current content, or None."""
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return None
    try:
        content = sql_file.read_bytes()
        return hashlib.sha256(content).hexdigest()[:16]
    except Exception:
        return None


def get_commits_since_sha(model_name: str, source_sha: str) -> int:
    """Return the number of commits that touched the model file since source_sha."""
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return 0
    try:
        result = subprocess.run(
            ["git", "rev-list", "--count", f"{source_sha}..HEAD", "--", str(sql_file)],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(sql_file.parent),
        )
        count_str = result.stdout.strip()
        return int(count_str) if count_str.isdigit() else 0
    except Exception:
        return 0


def get_recent_commits(model_name: str, days: int = 90) -> int:
    """Count commits touching this model file in the last N days."""
    sql_file = get_models_path() / f"{model_name}.sql"
    if not sql_file.exists():
        return 0
    try:
        result = subprocess.run(
            [
                "git",
                "log",
                f"--since={days} days ago",
                "--oneline",
                "--",
                str(sql_file),
            ],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(sql_file.parent),
        )
        lines = [line for line in result.stdout.strip().splitlines() if line]
        return len(lines)
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def list_all_tests() -> List[Dict[str, Any]]:
    """Return all tests (one per model), sorted by updated_at desc."""
    root = _tests_root()
    _migrate_old_structure()
    if not root.exists():
        return []
    results = []
    for f in root.rglob("*.json"):
        data = _read_json(f)
        if data:
            results.append(data)
    return sorted(results, key=lambda x: x.get("updated_at", ""), reverse=True)


def list_tests(model_name: str) -> List[Dict[str, Any]]:
    """Return the single test file for a model (as a list for API compatibility)."""
    p = _test_path(model_name)
    data = _read_json(p) if p.exists() else None
    return [data] if data else []


def get_test(
    session_id: str, model_name: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """Find a test by session ID. If model_name is known, tries direct lookup first."""
    if model_name:
        p = _test_path(model_name)
        if p.exists():
            data = _read_json(p)
            if data and data.get("test_id") == session_id:
                return data

    root = _tests_root()
    if not root.exists():
        return None
    for f in root.rglob("*.json"):
        data = _read_json(f)
        if data and data.get("test_id") == session_id:
            return data
    return None


def create_test(model_name: str) -> Dict[str, Any]:
    """Return existing test for this model, or create a new one."""
    p = _test_path(model_name)
    if p.exists():
        existing = _read_json(p)
        if existing:
            return existing

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
        "profile_skipped": False,
        "last_error": "",
        "test_cases": [],
    }
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
    _write_json(_test_path(mn), test)
    return test


def delete_test(session_id: str, model_name: str) -> bool:
    p = _test_path(model_name)
    if not p.exists():
        return False
    data = _read_json(p)
    if not data or data.get("test_id") != session_id:
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
