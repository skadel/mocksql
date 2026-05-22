"""mocksql check — verify that SQL models have up-to-date test files."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Literal

import yaml

Status = Literal["ok", "stale", "missing"]


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def _load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _index_test_files(tests_root: Path) -> dict[str, Path]:
    """Return a dict keyed by stem and by relative posix path (without .json)."""
    index: dict[str, Path] = {}
    if not tests_root.exists():
        return index
    for f in tests_root.rglob("*.json"):
        rel = f.relative_to(tests_root).with_suffix("").as_posix()
        index[rel] = f
        index[f.stem] = f
    return index


def check_models(
    config_path: Path,
    model_filters: list[str] | None = None,
    strict: bool = False,
) -> tuple[int, list[dict]]:
    """
    Verify SQL models have up-to-date test files.

    Returns (exit_code, results).
    exit_code 1 when any model is stale (SQL changed since last generate).
    With strict=True, also exit 1 when tests are missing entirely.
    """
    cfg = _load_config(config_path)

    models_path = Path(cfg.get("models_path", "models"))
    if not models_path.is_absolute():
        models_path = config_path.parent / models_path

    preprocessor_fn: str | None = cfg.get("preprocessor_fn")
    tests_root = config_path.parent / ".mocksql" / "tests"

    preprocess = None
    if preprocessor_fn:
        from storage.config import load_preprocessor_fn

        try:
            preprocess = load_preprocessor_fn(preprocessor_fn, config_path.parent)
        except Exception:
            pass

    test_index = _index_test_files(tests_root)

    sql_files: list[Path] = []
    if models_path.exists():
        sql_files = sorted(models_path.rglob("*.sql"))

    results: list[dict] = []
    has_failures = False

    for sql_file in sql_files:
        rel = sql_file.relative_to(models_path).with_suffix("").as_posix()

        if (
            model_filters
            and rel not in model_filters
            and sql_file.stem not in model_filters
        ):
            continue

        test_file = test_index.get(rel) or test_index.get(sql_file.stem)

        if test_file is None:
            results.append(
                {"model": rel, "status": "missing", "detail": "no tests generated"}
            )
            if strict:
                has_failures = True
            continue

        raw_sql = sql_file.read_text(encoding="utf-8")
        current_sql = preprocess(raw_sql) if preprocess else raw_sql

        try:
            doc = json.loads(test_file.read_text(encoding="utf-8"))
            stored_sql: str = doc.get("sql", "")
        except Exception as exc:
            results.append(
                {
                    "model": rel,
                    "status": "stale",
                    "detail": f"cannot read test file: {exc}",
                }
            )
            has_failures = True
            continue

        if _normalize_sql(current_sql) != _normalize_sql(stored_sql):
            results.append(
                {
                    "model": rel,
                    "status": "stale",
                    "detail": "SQL modified since last generate",
                }
            )
            has_failures = True
        else:
            results.append({"model": rel, "status": "ok", "detail": ""})

    return (1 if has_failures else 0), results
