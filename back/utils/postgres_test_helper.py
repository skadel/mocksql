import asyncio
import re
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

# Utilitaires de connexion et pool CloudSQL
from models.database import POOLS
from utils.examples import modify_test_dataset_for_bigquery_exec


def _revert_table_refs_in_error(
    error_message: str, tables: List[Dict[str, Any]], session_id: str
) -> str:
    """
    Replace suffixed test table names in an error message with original table keys.
    """
    suffix = session_id.replace("-", "_")
    mapping = {}
    for t in tables:
        parts = t["table_name"].split(".")
        base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        qualified = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        mapping[f"{base}_{suffix}"] = qualified
    pattern = re.compile(r"\b(" + "|".join(re.escape(k) for k in mapping) + r")\b")
    return pattern.sub(lambda m: mapping[m.group(0)], error_message)


def query_on_test_dataset(query, table_suffix):
    return modify_test_dataset_for_bigquery_exec(
        sql_query=query, session_id=table_suffix, dialect="postgres"
    )


class PostgresTestHelper:
    """
    Helper to create temporary test tables in PostgreSQL, insert data, and run queries,
    using CloudSQLPool (db_pool) from models.database.

    Expects columns definitions with native PostgreSQL types in 'type' field.
    """

    def __init__(self):
        self.pool = POOLS.get("postgres")

    async def create_table(
        self,
        session_id: str,
        table_name_key: str,
        columns: List[Dict[str, Any]],
    ):
        """
        Create or replace a test table with session-specific suffix.

        columns: list of dicts with 'name' and 'type' (pure PostgreSQL types, e.g. "TEXT", "INTEGER[]", "JSONB").
        expiration_hours is ignored since PostgreSQL has no built-in TTL.
        """
        suffix = session_id.replace("-", "_")
        table_name = f"{table_name_key}_{suffix}"
        # Build DDL statements
        col_defs = [
            f"{col['name']} {col['type']}" for col in columns if "." not in col["name"]
        ]

        statements = [
            f"DROP TABLE IF EXISTS {table_name};",
            f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(col_defs) + "\n);",
        ]

        async with self.pool.connection() as conn:
            for stmt in statements:
                await conn.execute(stmt)
        print(f"Created table {table_name}")

    async def insert_data(
        self, session_id: str, table_name_key: str, records: List[Dict[str, Any]]
    ):
        """
        Batch insert of list of dict records into the test table.
        """
        if not records:
            print("No records to insert.")
            return
        suffix = session_id.replace("-", "_")
        table_name = f"{table_name_key}_{suffix}"
        columns = list(records[0].keys())
        placeholders = ", ".join(f"${i + 1}" for i in range(len(columns)))
        col_list = ", ".join(columns)
        sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"
        values = [tuple(r.get(col) for col in columns) for r in records]

        async with self.pool.connection() as conn:
            await conn.executemany(sql, values)
        print(f"Inserted {len(records)} rows into {table_name}")

    async def create_and_insert_and_query(
        self,
        session_id: str,
        data_dict: Dict[str, List[Dict[str, Any]]],
        query: str,
        tables_and_columns: List[Dict[str, Any]],
        overwrite: bool = True,
    ) -> pd.DataFrame:
        """
        For each table spec in tables_and_columns, create table and insert data,
        then execute the provided SQL query and return a pandas DataFrame.
        """
        if overwrite:
            tasks = []
            for tbl in tables_and_columns:
                parts = tbl["table_name"].split(".")
                key = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
                if key in data_dict:
                    tasks.append(self.create_table(session_id, key, tbl["columns"]))
                    tasks.append(self.insert_data(session_id, key, data_dict[key]))
            await asyncio.gather(*tasks)

        async with self.pool.connection() as conn:
            try:
                results = await conn.fetch(query)
                df = pd.DataFrame([dict(row) for row in results])
                return df
            except Exception as e:
                msg = _revert_table_refs_in_error(
                    str(e), tables_and_columns, session_id
                )
                print("Error with restored table references:", msg)
                raise

    async def create_empty_tables(
        self, session_id: str, tables_and_columns: List[Dict[str, Any]]
    ):
        """
        Create empty test tables for each table spec in tables_and_columns.

        Uses the same session_id suffixing as create_table.
        """
        tasks = []
        for tbl in tables_and_columns:
            parts = tbl["table_name"].split(".")
            key = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            tasks.append(self.create_table(session_id, key, tbl["columns"]))
        await asyncio.gather(*tasks)

    async def execute_query(
        self, sql: str, params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Execute a raw SQL query against the database and return a pandas DataFrame.
        """
        async with self.pool.connection() as conn:
            try:
                results = await conn.fetch(sql, *(params or []))
                df = pd.DataFrame([dict(row) for row in results])
                return df
            except Exception as e:
                print(f"Error executing query: {e}")
                raise

    async def run_query(self, sql: str, dry: bool = True) -> Optional[Any]:
        """
        Validate or execute a SQL query.
        If dry=True: prepares the statement to check syntax without running; returns None.
        If dry=False: executes and returns a pandas DataFrame.
        """
        async with self.pool.connection() as conn:
            if dry:
                try:
                    # Prepare validates syntax without execution
                    await conn.prepare(sql)
                    print("Query syntax is valid.")
                    return None
                except Exception as e:
                    print(f"Query validation error: {e}")
                    raise e
            else:
                return await self.execute_query(sql)
