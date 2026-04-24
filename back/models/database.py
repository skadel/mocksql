import asyncio
import faulthandler
import logging
import platform
import sys
from typing import Optional, Any, List, Dict, Tuple

from models.db_pool import DBPool
from models.env_variables import DUCKDB_PATH

faulthandler.enable()

print(f"[BOOT] Python={sys.version.split()[0]} Platform={platform.platform()}")
print(f"[BOOT] DUCKDB_PATH={DUCKDB_PATH!r}")

logger = logging.getLogger(__name__)

db_pool = DBPool("duckdb", prewarm_count=0)


def _normalize_params(*args: Any) -> Tuple[Any, ...]:
    if len(args) == 1 and isinstance(args[0], tuple):
        return args[0]
    return tuple(args)


async def _duckdb_fetchall(
    conn: Any, sql: str, params: Tuple[Any, ...]
) -> Optional[List[Dict[str, Any]]]:
    def run():
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        if not rows:
            return None
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in rows]

    return await asyncio.to_thread(run)


async def _duckdb_select(
    conn: Any, sql: str, params: Tuple[Any, ...]
) -> List[Dict[str, Any]]:
    def run():
        cur = conn.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, row)) for row in rows]

    return await asyncio.to_thread(run)


async def execute(sql_query: str, *args: Any) -> Optional[List[Dict[str, Any]]]:
    params = _normalize_params(*args)
    async with db_pool.connection() as conn:
        logger.debug("<< acquire connection >>")
        return await _duckdb_fetchall(conn, sql_query, params)


async def query(
    sql_query: str,
    params: Optional[Tuple[Any, ...]] = None,
) -> List[Dict[str, Any]]:
    p = params or ()
    async with db_pool.connection() as conn:
        return await _duckdb_select(conn, sql_query, p)
