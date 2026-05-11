import logging
from pathlib import Path
from typing import Any, Dict, List, Set

import duckdb
import sqlglot
import yaml
from sqlglot import expressions as exp
from sqlglot.optimizer.scope import traverse_scope, find_all_in_scope

from storage.config import get_mocksql_dir, get_models_path
from utils.examples import initialize_duckdb, DB_PATH, parse_test_query

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lecture des specs
# ---------------------------------------------------------------------------


def list_integration_files() -> List[str]:
    d = get_mocksql_dir() / "integration"
    if not d.exists():
        return []
    return sorted(f.name for f in d.glob("*.yml"))


def load_integration_spec(filename: str) -> dict:
    path = get_mocksql_dir() / "integration" / filename
    if not path.exists():
        raise FileNotFoundError(f"Fichier d'intégration introuvable : {path}")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ---------------------------------------------------------------------------
# Analyse statique de la chaîne : tables sources + validation
# ---------------------------------------------------------------------------


def get_source_tables(
    chain: List[Dict], dialect: str, models_path: Path
) -> Dict[str, List[str]]:
    """
    For each step, returns the real source tables that need importing.
    Intermediate tables declared in 'produces' are excluded — they are created
    by previous steps and will already exist in DuckDB when the step runs.

    Returns: {"step_sql_path": ["project.dataset.table", ...]}
    """
    produces_normalized: Set[str] = {_normalize_ref(step["produces"]) for step in chain}

    result = {}
    for step in chain:
        sql_file = models_path / step["sql"]
        if not sql_file.exists():
            result[step["sql"]] = []
            continue
        sql = sql_file.read_text(encoding="utf-8")
        refs = _extract_source_table_refs(sql, dialect)
        result[step["sql"]] = [
            r for r in refs if _normalize_ref(r) not in produces_normalized
        ]
    return result


def validate_chain(
    chain: List[Dict], dialect: str, models_path: Path
) -> List[Dict[str, Any]]:
    """
    Validates each step's SQL with sqlglot (syntax + parse).
    Returns one entry per step: {"sql": ..., "valid": bool, "error": str|None}
    """
    results = []
    for step in chain:
        sql_file = models_path / step["sql"]
        entry: Dict[str, Any] = {
            "sql": step["sql"],
            "produces": step["produces"],
            "valid": False,
            "error": None,
        }
        if not sql_file.exists():
            entry["error"] = f"Fichier introuvable : {sql_file}"
            results.append(entry)
            continue
        sql = sql_file.read_text(encoding="utf-8")
        errors = sqlglot.validate(sql, dialect=dialect)
        if errors:
            entry["error"] = "; ".join(str(e) for e in errors)
        else:
            try:
                sqlglot.parse_one(sql, read=dialect)
                entry["valid"] = True
            except Exception as e:
                entry["error"] = str(e)
        results.append(entry)
    return results


def _extract_source_table_refs(sql: str, dialect: str) -> List[str]:
    """
    Returns fully-qualified table references from the SQL, excluding CTEs.
    Uses traverse_scope so CTE names are never returned as source tables.
    Only returns tables that have at least a dataset qualifier (dataset.table),
    since bare table names can't be meaningfully imported.
    """
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return []

    cte_names: Set[str] = {cte.alias.lower() for cte in tree.find_all(exp.CTE)}

    refs: Set[str] = set()
    for scope in traverse_scope(tree):
        for table in find_all_in_scope(scope.expression, exp.Table):
            if not table.this:
                continue
            name = table.this.name
            if name.lower() in cte_names:
                continue
            parts = []
            catalog = table.args.get("catalog")
            if catalog:
                parts.append(catalog.name)
            if table.db:
                parts.append(table.db)
            parts.append(name)
            if len(parts) >= 2:
                refs.add(".".join(parts))
    return sorted(refs)


def _normalize_ref(ref: str) -> str:
    """'project.dataset.table' → 'dataset.table' (lowercase, last 2 parts)."""
    parts = ref.replace("`", "").split(".")
    return ".".join(parts[-2:]).lower()


# ---------------------------------------------------------------------------
# Exécution de la chaîne de tests
# ---------------------------------------------------------------------------


async def run_integration_file(
    filename: str,
    project: str,
    dialect: str,
) -> Dict[str, Any]:
    spec = load_integration_spec(filename)
    chain = spec.get("chain", [])
    tests = spec.get("tests", [])

    if not chain:
        raise ValueError("La chaîne ('chain') est vide ou absente")

    models_path = get_models_path()
    results = []
    with initialize_duckdb(DB_PATH) as con:
        for idx, test in enumerate(tests):
            result = await _run_single_test(
                test=test,
                chain=chain,
                all_tests=tests,
                models_path=models_path,
                project=project,
                dialect=dialect,
                con=con,
                suffix=f"int{idx}",
            )
            results.append(result)

    passed = sum(1 for r in results if r["status"] == "pass")
    return {
        "name": spec.get("name", filename),
        "file": filename,
        "total": len(results),
        "passed": passed,
        "failed": len(results) - passed,
        "tests": results,
    }


async def _run_single_test(
    test: Dict[str, Any],
    chain: List[Dict[str, Any]],
    all_tests: List[Dict[str, Any]],
    models_path: Path,
    project: str,
    dialect: str,
    con: duckdb.DuckDBPyConnection,
    suffix: str,
) -> Dict[str, Any]:
    title = test.get("title", "")
    data = test.get("data", {})
    last_sql = None

    try:
        # 1. Créer les tables sources (données synthétiques ou importées)
        for table_ref, rows in data.items():
            duckdb_name = _to_duckdb_name(table_ref, suffix)
            cols = _infer_columns(table_ref, rows, all_tests)
            _create_table(con, duckdb_name, rows or [], cols)

        # 2. Exécuter chaque étape dans l'ordre.
        #    Les tables produites par les étapes précédentes sont déjà en DuckDB
        #    sous le nom _to_duckdb_name(step["produces"], suffix) — parse_test_query
        #    applique le même strip_qualifiers_with_scope, donc les références
        #    inter-étapes se résolvent automatiquement.
        last_df = None
        for step in chain:
            sql_file = models_path / step["sql"]
            if not sql_file.exists():
                raise FileNotFoundError(f"SQL introuvable : {sql_file}")

            raw_sql = sql_file.read_text(encoding="utf-8").strip()
            produces_name = _to_duckdb_name(step["produces"], suffix)

            last_sql = await parse_test_query(raw_sql, suffix, dialect)
            con.execute(f"CREATE OR REPLACE TABLE {produces_name} AS ({last_sql})")
            last_df = con.execute(f"SELECT * FROM {produces_name}").fetchdf()

        if last_df is None:
            raise ValueError("La chaîne ne contient aucune étape")

        # 3. Évaluer les assertions sur la sortie finale
        assertion_results = _evaluate(con, last_df, suffix, test)
        all_passed = all(a.get("passed", False) for a in assertion_results)
        return {
            "title": title,
            "status": "pass" if all_passed else "fail",
            "rows_produced": len(last_df),
            "assertion_results": assertion_results,
            "data": data,
        }

    except Exception as e:
        logger.error(
            "Integration test '%s' erreur : %s\nSQL:\n%s",
            title,
            e,
            last_sql or "N/A",
            exc_info=True,
        )
        return {"title": title, "status": "error", "error": str(e), "data": data}


# ---------------------------------------------------------------------------
# Évaluation des assertions
# ---------------------------------------------------------------------------


def _evaluate(
    con: duckdb.DuckDBPyConnection,
    result_df,
    suffix: str,
    test: Dict[str, Any],
) -> List[Dict[str, Any]]:
    if test.get("expected_empty", False):
        passed = len(result_df) == 0
        return [
            {
                "description": "Le résultat final doit être vide (0 ligne)",
                "passed": passed,
                "failing_rows": []
                if passed
                else result_df.head(5).to_dict(orient="records"),
            }
        ]

    assertions = test.get("assertions", [])
    if not assertions:
        return [
            {
                "description": "Le pipeline s'exécute sans erreur",
                "passed": True,
                "failing_rows": [],
            }
        ]

    view_name = f"__result__{suffix}"
    con.register(view_name, result_df)
    results = []
    try:
        for a in assertions:
            sql = a.get("sql", "").replace("__result__", view_name)
            try:
                fail_df = con.execute(sql).fetchdf()
                passed = len(fail_df) == 0
                results.append(
                    {
                        "description": a.get("description", ""),
                        "sql": a.get("sql", ""),
                        "passed": passed,
                        "failing_rows": fail_df.to_dict(orient="records")
                        if not passed
                        else [],
                    }
                )
            except Exception as e:
                results.append(
                    {
                        "description": a.get("description", ""),
                        "sql": a.get("sql", ""),
                        "passed": False,
                        "error": str(e),
                    }
                )
    finally:
        con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_duckdb_name(table_ref: str, suffix: str) -> str:
    """'project.dataset.table' ou 'dataset.table' → 'dataset_table_{suffix}'."""
    parts = table_ref.replace("`", "").split(".")
    return "_".join(parts[-2:]) + "_" + suffix.replace("-", "_")


def _infer_columns(
    table_ref: str, rows: List[Dict], all_tests: List[Dict[str, Any]]
) -> List[str]:
    """Infer column names from the first non-empty row set for this table."""
    if rows:
        return list(rows[0].keys())
    for t in all_tests:
        other_rows = t.get("data", {}).get(table_ref, [])
        if other_rows:
            return list(other_rows[0].keys())
    return []


def _create_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    rows: List[Dict[str, Any]],
    cols: List[str],
) -> None:
    if rows:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
    elif cols:
        col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} ({col_defs})")
    else:
        con.execute(f"CREATE OR REPLACE TABLE {table_name} (__placeholder__ BOOLEAN)")
