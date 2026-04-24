"""mocksql test — replay saved test cases against DuckDB (no LLM calls)."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Any

import yaml

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


# ── Config / cache helpers ────────────────────────────────────────────────────


def _load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_schema_cache(cache_path: str) -> list[dict]:
    p = Path(cache_path)
    if not p.exists():
        return []
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def _read_json(p: Path) -> dict | None:
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


# ── Schema resolution ─────────────────────────────────────────────────────────


def _schemas_from_cache(used_columns_raw: list[str], cache: list[dict]) -> list[dict]:
    """Build filtered schema list from cache, guided by the saved used_columns."""
    idx: dict[str, dict] = {}
    for s in cache:
        name = s["table_name"].lower()
        idx[name] = s
        parts = name.split(".")
        if len(parts) >= 2:
            idx[".".join(parts[-2:])] = s
        if parts:
            idx[parts[-1]] = s

    result: list[dict] = []
    for raw in used_columns_raw:
        try:
            u = json.loads(raw)
        except Exception:
            continue
        project = u.get("project", "")
        database = u.get("database", "")
        table = u.get("table", "")
        used_cols: list[str] = u.get("used_columns", [])

        candidates: list[str] = []
        if project and database:
            candidates.append(f"{project}.{database}.{table}".lower())
        if database:
            candidates.append(f"{database}.{table}".lower())
        candidates.append(table.lower())

        schema = next((idx[c] for c in candidates if c in idx), None)
        if not schema:
            continue
        if used_cols:
            used_lower = {c.lower() for c in used_cols}
            filtered_cols = [
                col for col in schema["columns"] if col["name"].lower() in used_lower
            ]
            result.append({**schema, "columns": filtered_cols})
        else:
            result.append(schema)
    return result


_DUCK_TYPE_MAP: list[tuple[type, str]] = [
    (bool, "BOOLEAN"),
    (int, "BIGINT"),
    (float, "DOUBLE"),
    (str, "TEXT"),
]


def _infer_duck_type(value: Any) -> str:
    if value is None:
        return "TEXT"
    for py_type, duck in _DUCK_TYPE_MAP:
        if isinstance(value, py_type):
            return duck
    return "TEXT"


def _infer_schema_from_rows(table_name: str, rows: list[dict]) -> dict:
    """Derive a DuckDB-typed schema from the data rows themselves."""
    seen: dict[str, str] = {}
    for row in rows:
        for k, v in row.items():
            if k not in seen:
                seen[k] = _infer_duck_type(v)
    return {
        "table_name": table_name,
        "columns": [{"name": k, "type": t, "description": ""} for k, t in seen.items()],
        "description": "",
        "primary_keys": [],
    }


def _resolve_schemas(
    used_columns_raw: list[str],
    schema_cache: list[dict],
    data: dict,
) -> list[dict]:
    """Return schemas: prefer cache lookup, fall back to type inference from data."""
    if used_columns_raw and schema_cache:
        schemas = _schemas_from_cache(used_columns_raw, schema_cache)
        if schemas:
            return schemas
    # Fallback: infer from data values (works without schema cache)
    return [
        _infer_schema_from_rows(tname, rows)
        for tname, rows in data.items()
        if isinstance(rows, list) and rows
    ]


# ── Single test-case execution ────────────────────────────────────────────────


async def _run_one_case(
    test_case: dict,
    sql: str,
    schemas: list[dict],
    used_columns_parsed: list[dict],
    dialect: str,
    suffix: str,
    con,
) -> dict:
    from build_query.examples_executor import _evaluate_assertions
    from utils.examples import (
        create_test_tables,
        execute_queries,
        run_query_on_test_dataset,
    )
    from utils.insert_examples import insert_examples, replace_missing_with_null

    test_index = str(test_case.get("test_index", "0"))
    name = (
        test_case.get("unit_test_description")
        or test_case.get("test_name")
        or f"Test {test_index}"
    )
    case_suffix = f"{suffix}{test_index}"
    data: dict = test_case.get("data") or {}
    saved_assertions = [
        a for a in (test_case.get("assertion_results") or []) if a.get("sql")
    ]

    if not data:
        return {
            "index": test_index,
            "name": name,
            "status": "skip",
            "reason": "no data",
            "assertions": [],
        }
    if not saved_assertions:
        return {
            "index": test_index,
            "name": name,
            "status": "skip",
            "reason": "no assertions",
            "assertions": [],
        }

    try:
        test_data = replace_missing_with_null(data, schemas)
        duckdb_schemas = create_test_tables(
            tables=schemas, suffix=case_suffix, overwrite=True, con=con, dialect=dialect
        )
        insert_stmts = list(
            insert_examples(
                data_dict=test_data,
                schemas=duckdb_schemas,
                suffix=case_suffix,
                used_columns=used_columns_parsed or None,
            )
        )
        execute_queries(insert_stmts, con)

        result_df, _ = await run_query_on_test_dataset(
            sql, case_suffix, "cli", dialect, con
        )

        view_name = f"__result__{case_suffix}"
        con.register(view_name, result_df)
        try:
            assertion_results = _evaluate_assertions(saved_assertions, view_name, con)
        finally:
            con.execute(f'DROP VIEW IF EXISTS "{view_name}"')

        all_passed = all(a.get("passed", False) for a in assertion_results)
        return {
            "index": test_index,
            "name": name,
            "status": "pass" if all_passed else "fail",
            "assertions": assertion_results,
        }
    except Exception as exc:
        return {
            "index": test_index,
            "name": name,
            "status": "error",
            "error": str(exc),
            "assertions": [],
        }


# ── Main entrypoint ───────────────────────────────────────────────────────────


async def run_tests(
    config_path: Path,
    model_filters: list[str] | None = None,
    fail_fast: bool = False,
) -> tuple[int, list[dict]]:
    """
    Replay all saved test cases from .mocksql/tests/ against DuckDB.

    Returns (exit_code, model_results):
      - exit_code 0 = all pass, 1 = at least one failure / error
      - model_results is a list of {model, cases} dicts
    """
    from utils.examples import DB_PATH, initialize_duckdb

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    cache_path = str(
        config_path.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    schema_cache = _load_schema_cache(cache_path)

    tests_root = config_path.parent / ".mocksql" / "tests"
    if not tests_root.exists():
        return 0, []

    # Collect model test files (skip old UUID-named session files)
    test_files = sorted(
        f for f in tests_root.rglob("*.json") if not _UUID_RE.match(f.stem)
    )
    if not test_files:
        return 0, []

    session_prefix = uuid.uuid4().hex[:8]
    model_results: list[dict] = []
    has_failures = False

    with initialize_duckdb(DB_PATH) as con:
        for test_file in test_files:
            rel = test_file.relative_to(tests_root).with_suffix("")
            model_name = rel.as_posix()

            if model_filters and model_name not in model_filters:
                continue

            test_doc = _read_json(test_file)
            if not test_doc:
                continue

            sql: str = test_doc.get("sql", "")
            used_columns_raw: list[str] = test_doc.get("used_columns") or []
            used_columns_parsed: list[dict] = []
            for raw in used_columns_raw:
                try:
                    used_columns_parsed.append(json.loads(raw))
                except Exception:
                    pass

            test_cases: list[dict] = test_doc.get("test_cases") or []
            case_results: list[dict] = []
            # Unique suffix per model to avoid table collisions between models
            model_suffix = (
                f"{session_prefix}_{re.sub(r'[^a-z0-9]', '_', model_name.lower())}"
            )

            for tc in test_cases:
                data: dict = tc.get("data") or {}
                schemas = _resolve_schemas(used_columns_raw, schema_cache, data)
                result = await _run_one_case(
                    test_case=tc,
                    sql=sql,
                    schemas=schemas,
                    used_columns_parsed=used_columns_parsed,
                    dialect=dialect,
                    suffix=model_suffix,
                    con=con,
                )
                case_results.append(result)

                if result["status"] in ("fail", "error"):
                    has_failures = True
                    if fail_fast:
                        model_results.append(
                            {"model": model_name, "cases": case_results}
                        )
                        return 1, model_results

            model_results.append({"model": model_name, "cases": case_results})

    return (1 if has_failures else 0), model_results
