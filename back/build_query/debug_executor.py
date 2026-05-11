import json
import logging
from typing import Any, Dict, List, Optional

import duckdb
import sqlglot
from sqlglot import exp

from build_query.examples_executor import _extract_conditions
from storage.test_repository import get_test
from utils.examples import (
    run_query_on_test_dataset,
    create_test_tables,
    execute_queries,
    initialize_duckdb,
    DB_PATH,
)
from utils.insert_examples import replace_missing_with_null, insert_examples

logger = logging.getLogger(__name__)


def _extract_right_key_from_join(join_expr: exp.Join) -> Optional[exp.Column]:
    """Return the right-side column of the first equality in the ON clause.
    Used to build an IS NOT NULL check after converting INNER → LEFT JOIN.
    """
    on = join_expr.args.get("on")
    if on:
        for eq in on.find_all(exp.EQ):
            right = eq.expression  # right operand
            if isinstance(right, exp.Column):
                return right
        cols = list(on.find_all(exp.Column))
        if cols:
            return cols[-1]
    using = join_expr.args.get("using")
    if using and isinstance(using, list):
        for item in using:
            if isinstance(item, exp.Column):
                return item
            if isinstance(item, exp.Identifier):
                return exp.column(item.name)
    return None


async def _setup_test_tables(
    session_id: str,
    test_index: str,
    schemas: List[Dict[str, Any]],
    used_columns: List[Dict[str, Any]],
    dialect: str,
    con: duckdb.DuckDBPyConnection,
) -> str:
    """Create DuckDB tables for the given test case. Returns the suffix used."""
    test = get_test(session_id)
    if not test:
        raise ValueError(f"Test not found for session {session_id}")

    test_case = next(
        (
            tc
            for tc in (test.get("test_cases") or [])
            if str(tc.get("test_index")) == str(test_index)
        ),
        None,
    )
    if not test_case:
        raise ValueError(f"Test case {test_index} not found")

    session_id_duckdb = session_id.replace("-", "_")
    suffix = f"{session_id_duckdb}{test_index}"

    test_data = replace_missing_with_null(test_case.get("data", {}), schemas)
    duckdb_schema = create_test_tables(
        tables=schemas,
        suffix=suffix,
        overwrite=True,
        con=con,
        dialect=dialect,
    )
    execute_queries(
        list(
            insert_examples(
                data_dict=test_data,
                schemas=duckdb_schema,
                suffix=suffix,
                used_columns=used_columns,
            )
        ),
        con,
    )
    return suffix


async def execute_run_cte(
    session_id: str,
    test_index: str,
    cte_name: str,
    column: Optional[str],
    query_decomposed: str,
    project: str,
    dialect: str,
    schemas: List[Dict[str, Any]],
    used_columns: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Execute SQL up to and including `cte_name` with the test's data. Returns rows."""
    ctes = json.loads(query_decomposed or "[]")

    target_idx = next(
        (i for i, c in enumerate(ctes) if c["name"].lower() == cte_name.lower()),
        None,
    )
    if target_idx is None:
        available = [c["name"] for c in ctes]
        return {"error": f"CTE '{cte_name}' not found. Available: {available}"}

    select_clause = f"SELECT {column}" if column else "SELECT *"

    if ctes[target_idx]["name"] == "final_query":
        debug_sql = ctes[target_idx]["code"]
        if column:
            debug_sql = f"SELECT {column} FROM ({debug_sql})"
    else:
        with_parts = [
            f"`{ctes[j]['name']}` AS ({ctes[j]['code']})"
            for j in range(target_idx + 1)
            if ctes[j]["name"] != "final_query"
        ]
        debug_sql = f"WITH {', '.join(with_parts)}\n{select_clause} FROM `{cte_name}`"

    lineage_info = None
    if column:
        try:
            from sqlglot.lineage import lineage as sqlglot_lineage

            node = sqlglot_lineage(column, debug_sql, dialect=dialect)
            lineage_info = str(node)
        except Exception:
            pass

    with initialize_duckdb(DB_PATH) as con:
        suffix = await _setup_test_tables(
            session_id, test_index, schemas, used_columns, dialect, con
        )
        df, _ = await run_query_on_test_dataset(
            debug_sql, suffix, project, dialect, con
        )

    result: Dict[str, Any] = {
        "cte_name": cte_name,
        "column": column,
        "rows": df.to_dict(orient="records"),
        "row_count": len(df),
    }
    if lineage_info:
        result["lineage"] = lineage_info
    return result


def _build_count_steps_query(
    cte_code: str,
    preceding_ctes: List[Dict[str, str]],
    dialect: str,
) -> tuple[str, List[str]]:
    """Build a single COUNT query with SUM(CASE WHEN …) for each JOIN and WHERE step.

    Converts all INNER JOINs to LEFT JOINs so every base row is preserved, then uses
    cumulative CASE expressions to count how many rows survive each join condition and
    each WHERE predicate — all in one round-trip to DuckDB.

    Returns (full_sql, labels) where labels[i] describes the i-th SELECT column.
    """
    tree = sqlglot.parse_one(cte_code, read=dialect)
    from_expr: Optional[exp.From] = tree.args.get("from")
    joins: List[exp.Join] = tree.args.get("joins") or []
    where: Optional[exp.Where] = tree.args.get("where")

    labels: List[str] = []
    select_parts: List[str] = ["COUNT(*) AS base_count"]

    base_name = from_expr.this.alias_or_name if from_expr else "base"
    labels.append(base_name)

    # JOINs → LEFT JOIN + cumulative IS NOT NULL predicates
    join_null_conditions: List[str] = []
    left_join_sqls: List[str] = []

    for i, join in enumerate(joins):
        join_copy = join.copy()
        join_copy.set("side", "LEFT")
        join_copy.set("kind", None)
        left_join_sqls.append(join_copy.sql(dialect=dialect))

        right_col = _extract_right_key_from_join(join)
        if right_col:
            col_sql = right_col.sql(dialect=dialect)
            join_null_conditions.append(f"{col_sql} IS NOT NULL")
            cumul = " AND ".join(join_null_conditions)
            select_parts.append(
                f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_join_{i + 1}"
            )
            labels.append(f"+ JOIN ({col_sql} IS NOT NULL)")
        else:
            select_parts.append(f"COUNT(*) AS after_join_{i + 1}")
            labels.append(f"+ JOIN {i + 1}")

    # WHERE conditions → cumulative CASE expressions
    where_conds = _extract_conditions(where.this) if where else []
    cumul_parts = list(join_null_conditions)

    for j, cond in enumerate(where_conds):
        cond_sql = cond.sql(dialect=dialect)
        cumul_parts.append(f"({cond_sql})")
        cumul = " AND ".join(cumul_parts)
        select_parts.append(
            f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_cond_{j + 1}"
        )
        labels.append(f"+ WHERE {cond_sql}")

    from_sql = from_expr.sql(dialect=dialect) if from_expr else ""
    joins_sql = ("\n" + "\n".join(left_join_sqls)) if left_join_sqls else ""
    body = f"SELECT\n  {',\n  '.join(select_parts)}\n{from_sql}{joins_sql}"

    if preceding_ctes:
        with_parts = [f"`{c['name']}` AS ({c['code']})" for c in preceding_ctes]
        return f"WITH {', '.join(with_parts)}\n{body}", labels

    return body, labels


async def execute_count_cte_steps(
    session_id: str,
    test_index: str,
    cte_name: str,
    query_decomposed: str,
    project: str,
    dialect: str,
    schemas: List[Dict[str, Any]],
    used_columns: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Count rows step-by-step (per JOIN then per WHERE condition) for a CTE.

    Uses a single DuckDB query with SUM(CASE WHEN …) columns so the result is
    one row with counts at each step — no N+1 queries.
    """
    ctes = json.loads(query_decomposed or "[]")

    target_idx = next(
        (i for i, c in enumerate(ctes) if c["name"].lower() == cte_name.lower()),
        None,
    )
    if target_idx is None:
        available = [c["name"] for c in ctes]
        return {"error": f"CTE '{cte_name}' not found. Available: {available}"}

    preceding = [c for c in ctes[:target_idx] if c["name"] != "final_query"]
    full_sql, labels = _build_count_steps_query(
        ctes[target_idx]["code"], preceding, dialect
    )

    with initialize_duckdb(DB_PATH) as con:
        suffix = await _setup_test_tables(
            session_id, test_index, schemas, used_columns, dialect, con
        )
        try:
            df, _ = await run_query_on_test_dataset(
                full_sql, suffix, project, dialect, con
            )
        except Exception as e:
            logger.error("count_cte_steps failed: %s\nSQL:\n%s", e, full_sql)
            return {"error": str(e), "cte_name": cte_name}

    steps: List[Dict[str, Any]] = []
    if not df.empty:
        row = df.iloc[0].to_dict()
        col_names = list(row.keys())
        for i, label in enumerate(labels):
            val = row.get(col_names[i]) if i < len(col_names) else 0
            steps.append({"label": label, "count": int(val) if val is not None else 0})
    else:
        steps = [{"label": lbl, "count": 0} for lbl in labels]

    return {"cte_name": cte_name, "steps": steps}
