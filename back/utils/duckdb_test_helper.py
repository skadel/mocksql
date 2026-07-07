import asyncio
import re
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd

from utils.examples import modify_test_dataset_for_bigquery_exec


def _revert_table_refs_in_error(
    error_message: str, tables: List[Dict[str, Any]], session_id: str
) -> str:
    """
    Remplace dans le message d'erreur les noms de tables suffixés par la clé d'origine.
    Exemple:
        mytable_123e4567_e89b_12d3_a456_426614174000 -> mytable
    """
    suffix = session_id.replace("-", "_")
    mapping = {}
    for t in tables:
        parts = t["table_name"].split(".")
        base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        qualified = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        mapping[f"{base}_{suffix}"] = qualified
    if not mapping:
        return error_message
    pattern = re.compile(r"\b(" + "|".join(re.escape(k) for k in mapping) + r")\b")
    return pattern.sub(lambda m: mapping[m.group(0)], error_message)


def query_on_test_dataset(query: str, table_suffix: str) -> str:
    """
    Réécrit la requête pour pointer vers les tables de test suffixées.
    Le dialecte 'duckdb' permet au réécrivain de conserver la bonne syntaxe.
    """
    return modify_test_dataset_for_bigquery_exec(
        sql_query=query, session_id=table_suffix, dialect="duckdb"
    )


class DuckDBTestHelper:
    """
    Helper pour créer des tables de test DuckDB, insérer des données et exécuter/valider des requêtes.

    Paramètres:
        db_path: chemin vers la base DuckDB. ':memory:' pour une base en mémoire.
                 ⚠ Si vous utilisez ':memory:', la **même connexion** doit être réutilisée
                   entre create_table/insert_data/run_query (ce helper s'en charge).

    Remarques:
        - Les types de colonnes doivent être valides en DuckDB (INTEGER, BIGINT, DOUBLE,
          BOOLEAN, VARCHAR, DATE, TIMESTAMP, TIMESTAMPTZ, JSON, etc.).
        - Les placeholders de paramètres sont '?'.
        - Le dry-run utilise PREPARE/DEALLOCATE, ce qui valide la syntaxe et la résolution
          des objets sans exécuter la requête (les tables référencées doivent exister).
    """

    def __init__(self, db_path: str = ":memory:"):
        from storage.config import open_duckdb_connection

        self.db_path = db_path
        # Connexion persistante PRÊTE (extensions + macros type hexstr_to_double) :
        # le dry-run PREPARE doit résoudre les mêmes fonctions que l'exécution
        # réelle, sinon il rejette du SQL valide. Via la factory unique pour ne
        # pas diverger des autres sites de connexion.
        self.conn = open_duckdb_connection(self.db_path)
        # Lock pour sérialiser l'accès concurrent à la connexion
        self._lock = asyncio.Lock()

    # ---------- Implémentations synchrones (exécutées dans un thread) ----------

    def _sync_exec_many(self, statements: List[str]) -> None:
        for stmt in statements:
            self.conn.execute(stmt)

    def _sync_insert_many(self, table: str, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        cols = list(records[0].keys())
        placeholders = ", ".join(["?"] * len(cols))
        col_list = ", ".join(cols)
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"
        values = [tuple(r.get(c) for c in cols) for r in cols and records]
        # NOTE: on préfère executemany côté DuckDB pour des batches homogènes
        self.conn.executemany(sql, values)

    def _sync_fetchdf(
        self, sql: str, params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        if params:
            return self.conn.execute(sql, params).fetchdf()
        return self.conn.execute(sql).fetchdf()

    def _sync_prepare_only(self, sql: str) -> None:
        # Compile sans exécution réelle: valide syntaxe + résolution de schéma
        self.conn.execute(f"PREPARE __duck_compile__ AS {sql}")
        self.conn.execute("DEALLOCATE PREPARE __duck_compile__")

    async def execute_query(
        self, sql: str, params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Exécute une requête et retourne un DataFrame. Utiliser '?' pour les paramètres.
        """
        try:
            async with self._lock:
                df = await asyncio.to_thread(self._sync_fetchdf, sql, params)
                return df
        except Exception as e:
            print(f"Error executing query: {e}")
            raise

    async def run_query(
        self,
        sql: str,
        dry: bool = True,
        *,
        tables_and_columns: Optional[List[Dict[str, Any]]] = None,
        session_id: Optional[str] = None,
    ) -> Optional[pd.DataFrame]:
        """
        Valide ou exécute une requête.
        - dry=True : compile/valide via PREPARE/DEALLOCATE (les objets doivent exister); retourne None.
        - dry=False: exécute et retourne un DataFrame.

        En cas d'erreur et si tables_and_columns + session_id sont fournis,
        les références de tables dans le message d'erreur sont restaurées.
        """
        try:
            if dry:
                async with self._lock:
                    await asyncio.to_thread(self._sync_prepare_only, sql)
                print("Query syntax is valid.")
                return None
            else:
                return await self.execute_query(sql)
        except Exception as e:
            if tables_and_columns and session_id:
                msg = _revert_table_refs_in_error(
                    str(e), tables_and_columns, session_id
                )
                print("Query validation error (restored):", msg)
            else:
                print(f"Query validation/execution error: {e}")
            raise

    async def close(self):
        """
        Ferme proprement la connexion persistante.
        """
        async with self._lock:
            self.conn.close()
