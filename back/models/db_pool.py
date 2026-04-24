import asyncio
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any, Deque

from models.env_variables import DUCKDB_PATH

logger = logging.getLogger(__name__)


class _PoolConnectionInfo:
    def __init__(self, connection: Any):
        self.connection = connection
        self.created_at = time.monotonic()
        self.use_count = 0

    def is_closed(self) -> bool:
        return getattr(self.connection, "is_closed", lambda: False)()


class DBPool:
    """DuckDB connection pool."""

    def __init__(
        self, mode: str = "duckdb", max_size: int = 10, prewarm_count: int = 2
    ):
        self.mode = "duckdb"
        self.max_size = max_size
        self.prewarm_count = max(0, min(prewarm_count, max_size))

        self._pool: Deque[_PoolConnectionInfo] = deque()
        self._pool_semaphore = asyncio.Semaphore(self.max_size)
        self._lock = asyncio.Lock()
        self._initialized = False
        self._duckdb_checked = False

        logger.info(
            "DBPool init: max_size=%d prewarm=%d", self.max_size, self.prewarm_count
        )

    async def init_pool(self) -> None:
        async with self._lock:
            if self._initialized:
                return
            self._initialized = True
            logger.info("Init DuckDB pool…")

            if not self._duckdb_checked:
                self._duckdb_sanity_check_or_fail(DUCKDB_PATH)
                self._duckdb_checked = True

            if self.prewarm_count > 0:
                await self._prewarm_connections()
            logger.info("Pool initialisé (size=%d).", len(self._pool))

    async def _prewarm_connections(self) -> None:
        tasks = [self._create_new_connection() for _ in range(self.prewarm_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        ok = 0
        for res in results:
            if isinstance(res, Exception):
                logger.error("Préchauffage: exception: %r", res)
            else:
                self._pool.append(_PoolConnectionInfo(res))
                ok += 1
        logger.info(
            "Préchauffage terminé: %d/%d connexions prêtes.", ok, self.prewarm_count
        )

    async def acquire(self) -> Any:
        await self.init_pool()
        await self._pool_semaphore.acquire()
        async with self._lock:
            if self._pool:
                pci = self._pool.pop()
                if pci.is_closed():
                    conn = await self._create_new_connection()
                    return conn
                pci.use_count += 1
                return pci.connection
        return await self._create_new_connection()

    async def release(self, conn: Any) -> None:
        try:

            def _rollback():
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass

            await asyncio.to_thread(_rollback)
            async with self._lock:
                self._pool.append(_PoolConnectionInfo(conn))
        finally:
            self._pool_semaphore.release()

    async def close(self) -> None:
        logger.info("Fermeture du pool…")
        async with self._lock:
            while self._pool:
                pci = self._pool.pop()
                await self._close_if_possible(pci.connection)
        logger.info("Pool fermé.")

    @asynccontextmanager
    async def connection(self):
        conn = await self.acquire()
        try:
            yield conn
        finally:
            await self.release(conn)

    async def _close_if_possible(self, conn: Any) -> None:
        try:
            close_method = getattr(conn, "close", None)
            if not close_method:
                return
            res = close_method()
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            logger.debug("Erreur fermeture connexion ignorée: %r", e)

    async def _create_new_connection(self) -> Any:
        import duckdb

        return duckdb.connect(DUCKDB_PATH)

    @staticmethod
    def _duckdb_sanity_check_or_fail(path: str) -> None:
        try:
            import duckdb  # noqa

            tmp = duckdb.connect(":memory:")
            tmp.close()
        except Exception as e:
            raise RuntimeError(
                "DuckDB module semble cassé (échec sur ':memory:')."
            ) from e

        ap = os.path.abspath(path)
        dirn = os.path.dirname(ap) or os.getcwd()
        try:
            os.makedirs(dirn, exist_ok=True)
        except Exception as e:
            raise RuntimeError(f"Impossible de créer le répertoire {dirn!r}.") from e

        if os.path.exists(ap):
            try:
                sz = os.path.getsize(ap)
            except Exception:
                sz = -1
            if sz == 0:
                raise RuntimeError(
                    f"Fichier DuckDB 0 octet détecté à {ap!r}. "
                    "Supprime/renomme le fichier puis relance."
                )
