from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import snowflake.connector
    import snowflake.connector.cursor

from models.env_variables import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_SCHEMA_NAME,
    SNOWFLAKE_USER,
    SNOWFLAKE_WAREHOUSE,
)

_sf_conn: snowflake.connector.SnowflakeConnection | None = None


def _import_snowflake():
    try:
        import snowflake.connector

        return snowflake.connector
    except ImportError as e:
        raise ImportError(
            "Le connecteur Snowflake n'est pas installé. "
            "Installez l'extra correspondant : pip install mocksql[snowflake]"
        ) from e


def get_sf_connection() -> snowflake.connector.SnowflakeConnection:
    snowflake_connector = _import_snowflake()

    global _sf_conn
    if _sf_conn is None or _sf_conn.is_closed():
        _sf_conn = snowflake_connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA_NAME,
        )
    return _sf_conn


def run_sf_query(sql: str, dry: bool = False) -> list[dict]:
    """Execute SQL on Snowflake. If dry=True, run EXPLAIN instead (syntax validation only)."""
    snowflake_connector = _import_snowflake()

    conn = get_sf_connection()
    cur = conn.cursor(snowflake_connector.DictCursor)
    target = f"EXPLAIN {sql}" if dry else sql
    cur.execute(target)
    return cur.fetchall()
