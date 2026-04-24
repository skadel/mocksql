import asyncio
import json
import re
import uuid
from typing import Any, Dict, Tuple, List

import pandas as pd
import sqlglot
from langchain_core.messages import AIMessage

from build_query.state import QueryState
from models.env_variables import DUCKDB_PATH
from utils.errors import handle_execution_exceptions

ALLOWED_ROOT_TYPES = {"Select", "With", "Union"}


def _validate_sql(sql: str, dialect: str) -> None:
    try:
        expr = sqlglot.parse_one(sql, read=dialect)
    except sqlglot.errors.ParseError as e:
        raise ValueError(f"SQL parsing failed: {e}") from e
    root_type = type(expr).__name__
    if root_type not in ALLOWED_ROOT_TYPES:
        raise ValueError(
            f"Only SELECT/WITH/UNION queries are allowed (got {root_type})."
        )


def _has_limit(sql: str) -> bool:
    return re.search(r"\blimit\b", sql, re.I) is not None


# ---------- Runners concrets ----------
def run_query_duckdb(
    sql: str, db_path: str, *, limit: int = 30, offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    # import paresseux pour éviter l’exigence si DuckDB n’est pas utilisé
    import duckdb

    base_sql = sql.strip().rstrip(";")
    _validate_sql(base_sql, dialect="duckdb")

    has_limit = _has_limit(base_sql)
    if has_limit:
        paginated_sql = base_sql
        params = None
    else:
        paginated_sql = f"""
        WITH paged AS (
            {base_sql}
            LIMIT ? OFFSET ?
        )
        SELECT * FROM paged
        """
        params = [limit, offset]

    with duckdb.connect(db_path, read_only=False) as con:
        q = con.execute(paginated_sql, params) if params else con.execute(paginated_sql)
        rows = q.fetchall()
        cols = [d[0] for d in q.description]
        total = con.execute(f"SELECT COUNT(*) FROM ({base_sql}) t").fetchone()[0]

    return [dict(zip(cols, r)) for r in rows], total


# ---------- Router (DuckDB only) ----------
async def run_query(
    sql: str, *, dialect: str, project_id: str = None, limit: int = 30, offset: int = 0
) -> Tuple[List[Dict[str, Any]], int]:
    return run_query_duckdb(sql, DUCKDB_PATH, limit=limit, offset=offset)


async def format_result(res: pd.DataFrame) -> str:
    return res.to_json(orient="records", date_format="iso", date_unit="s")


async def run_on_data(state: QueryState) -> Dict[str, Any]:
    last_query = None
    try:
        dialect = state["dialect"]
        last_query = state.get("query", "")

        rows, total = await run_query(
            last_query,
            dialect=dialect,
            project_id=state["project"],
            limit=20,
            offset=0,
        )

        df = pd.DataFrame(rows)
        rows_json_str = await format_result(df)

        full_payload = {
            "rows": json.loads(rows_json_str),
            "total": total,
            "limit": 20,
            "offset": 0,
            "sql": last_query,
        }
        json_result = json.dumps(full_payload)

        return {
            "messages": [
                AIMessage(
                    content=json_result,
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": "data_results",
                        "parent": state["messages"][-1].id,
                        "request_id": state.get("request_id"),
                        "is_analysis": state.get("is_analysis"),
                    },
                )
            ],
            "status": "complete",
        }

    except asyncio.CancelledError:
        return {"messages": [], "status": "cancelled"}

    except Exception as e:
        # Déléguer toute autre erreur d'exécution
        return handle_execution_exceptions(exc=e, state=state, sql=last_query)
