import json
import logging
from typing import Any, Dict, List, Optional

import duckdb
from sqlglot import exp

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


def _quote_ident(name: str, dialect: str) -> str:
    """Quote an identifier with the dialect's quote char.

    Le SQL de debug est reparsé par ``run_query_on_test_dataset`` avec
    ``read=dialect`` : des backticks codés en dur cassaient le parse sur tout
    dialecte non-BigQuery (snowflake/postgres/duckdb → ParseError "Expecting (").
    """
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)


async def _setup_test_tables(
    session_id: str,
    test_index: str,
    schemas: List[Dict[str, Any]],
    used_columns: List[Dict[str, Any]],
    dialect: str,
    con: duckdb.DuckDBPyConnection,
    test_cases: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Create DuckDB tables for the given test case. Returns the suffix used."""
    # Use in-state test cases first (not yet persisted), fall back to filesystem
    cases = test_cases
    if not cases:
        test = get_test(session_id)
        cases = test.get("test_cases") or [] if test else []

    test_case = next(
        (tc for tc in cases if str(tc.get("test_index")) == str(test_index)),
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
    test_cases: Optional[List[Dict[str, Any]]] = None,
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
            f"{_quote_ident(ctes[j]['name'], dialect)} AS ({ctes[j]['code']})"
            for j in range(target_idx + 1)
            if ctes[j]["name"] != "final_query"
        ]
        debug_sql = (
            f"WITH {', '.join(with_parts)}\n"
            f"{select_clause} FROM {_quote_ident(cte_name, dialect)}"
        )

    lineage_info = None
    if column:
        try:
            from sqlglot.lineage import lineage as sqlglot_lineage

            node = sqlglot_lineage(column, debug_sql, dialect=dialect)
            lineage_info = str(node)
        except Exception:
            pass

    try:
        with initialize_duckdb(DB_PATH) as con:
            suffix = await _setup_test_tables(
                session_id, test_index, schemas, used_columns, dialect, con, test_cases
            )
            df, _ = await run_query_on_test_dataset(
                debug_sql, suffix, project, dialect, con
            )
    except Exception as exc:
        # run_cte est un OUTIL appelé par le conversational_agent : une exécution qui
        # échoue (erreur de données, idiome non transpilable…) doit remonter comme
        # diagnostic à l'agent, jamais crasher tout le graphe.
        logger.warning(
            "run_cte failed for CTE '%s': %s\nSQL:\n%s", cte_name, exc, debug_sql
        )
        return {"cte_name": cte_name, "column": column, "error": str(exc)}

    result: Dict[str, Any] = {
        "cte_name": cte_name,
        "column": column,
        "rows": df.to_dict(orient="records"),
        "row_count": len(df),
    }
    if lineage_info:
        result["lineage"] = lineage_info
    return result
