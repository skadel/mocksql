from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import trino

from models.env_variables import (
    TRINO_CATALOG,
    TRINO_HOST,
    TRINO_HTTP_SCHEME,
    TRINO_PASSWORD,
    TRINO_PORT,
    TRINO_SCHEMA,
    TRINO_USER,
)

_trino_conn: trino.dbapi.Connection | None = None


def _import_trino():
    try:
        import trino

        return trino
    except ImportError as e:
        raise ImportError(
            "Le connecteur Trino n'est pas installé. "
            "Installez l'extra correspondant : pip install mocksql[trino]"
        ) from e


def get_trino_connection() -> trino.dbapi.Connection:
    trino = _import_trino()

    global _trino_conn
    if _trino_conn is None:
        kwargs: dict = {
            "host": TRINO_HOST,
            "port": TRINO_PORT,
            "user": TRINO_USER,
            "http_scheme": TRINO_HTTP_SCHEME,
        }
        # Catalog/schema par défaut : permettent des refs non qualifiées (schema.table
        # ou même table nue). Optionnels — chaque requête peut aussi qualifier en dur.
        if TRINO_CATALOG:
            kwargs["catalog"] = TRINO_CATALOG
        if TRINO_SCHEMA:
            kwargs["schema"] = TRINO_SCHEMA
        # Mot de passe → BasicAuth sur HTTPS (Trino refuse BasicAuth en clair).
        if TRINO_PASSWORD:
            kwargs["auth"] = trino.auth.BasicAuthentication(TRINO_USER, TRINO_PASSWORD)
            kwargs["http_scheme"] = "https"
        _trino_conn = trino.dbapi.connect(**kwargs)
    return _trino_conn


def run_trino_query(sql: str, dry: bool = False) -> list[dict]:
    """Exécute du SQL sur Trino.

    Si ``dry=True``, lance ``EXPLAIN (TYPE VALIDATE)`` — Trino analyse et lie la
    requête (colonnes/tables/types) sans l'exécuter → validation pure. Retourne une
    liste de dicts (colonne → valeur) ; liste vide en dry-run.
    """
    _import_trino()

    conn = get_trino_connection()
    cur = conn.cursor()
    target = f"EXPLAIN (TYPE VALIDATE) {sql}" if dry else sql
    cur.execute(target)
    rows = cur.fetchall()
    if dry:
        return []
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, row)) for row in rows]
