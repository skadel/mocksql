"""mocksql parity — audit de parité DuckDB ↔ warehouse (cf. docs/spec-parity.md).

Rejoue les tests sauvegardés d'un modèle sur la warehouse de l'utilisateur avec les
MÊMES données synthétiques que le rejeu DuckDB (tables remplacées par des CTEs inline
typées — aucune table de prod n'est lue, aucune PII ne transite), puis compare les deux
jeux de résultats. Un test concordant reçoit une attestation d'empreinte, committée
dans la définition du test (`tests/{model}.json`) et affichée par `mocksql test`.

Côté warehouse, le SQL du modèle s'exécute dans son dialecte NATIF (pas de
transpilation) : la différence mesurée est donc bien celle de la couche d'émulation
DuckDB (sqlglot + adaptations maison).

Dialectes v1 : bigquery, snowflake. Postgres est volontairement hors v1 (parité
quasi-triviale, pas de chemin connecteur CLI existant — cf. question ouverte n°2 de
la spec).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import math
import re
import uuid
from collections import Counter
from decimal import Decimal, InvalidOperation
from functools import lru_cache
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Dialectes warehouse supportés par l'audit de parité v1.
PARITY_DIALECTS = ("bigquery", "snowflake")

# Tolérance relative sur les flottants : les moteurs n'additionnent pas dans le même
# ordre. Absolue en repli pour les valeurs proches de zéro.
FLOAT_REL_TOL = 1e-9
FLOAT_ABS_TOL = 1e-12

# Nombre de lignes divergentes montrées de chaque côté dans le rapport de diff.
_DIFF_PREVIEW_ROWS = 5

# Suffixe des CTEs mock — reproduit le renommage `{db}_{table}_{suffix}` de
# strip_qualifiers_with_scope, comme le fait l'executor DuckDB.
_MOCK_SUFFIX = "mocksql_parity"


class ParityExecutionError(Exception):
    """Erreur d'exécution warehouse (credentials, réseau, SQL rejeté) → exit code 2.

    Un DIFF n'est PAS une erreur : c'est une information d'audit (exit code 1).
    """


# ── Empreinte de vérification ─────────────────────────────────────────────────


def transpiler_version() -> str:
    """Versions mocksql + sqlglot — un fix ou une régression de transpilation change
    ce que DuckDB exécute, donc invalide l'attestation (granularité package, cf. spec)."""
    import sqlglot

    try:
        from importlib.metadata import version

        mocksql_version = version("mocksql")
    except Exception:
        mocksql_version = "0.0.0-dev"
    return f"mocksql={mocksql_version};sqlglot={getattr(sqlglot, '__version__', '?')}"


@lru_cache(maxsize=64)
def _normalize_sql(sql: str, dialect: str) -> str:
    """SQL canonique (AST sqlglot re-émis, commentaires exclus) — l'empreinte doit
    survivre à un reformatage sans changement sémantique. Repli : espaces réduits."""
    import sqlglot

    try:
        return sqlglot.parse_one(sql, read=dialect).sql(dialect=dialect, comments=False)
    except Exception:
        return re.sub(r"\s+", " ", sql).strip()


def compute_fingerprint(sql: str, data: dict | None, dialect: str) -> str:
    """Empreinte d'attestation : sha256(sql normalisé + données du test canoniques
    + dialecte + version du transpileur). Chaque composant invalide la parité s'il
    change — un boolean « verified » mentirait (cf. spec-parity.md)."""
    payload = "\n".join(
        [
            _normalize_sql(sql, dialect),
            json.dumps(
                data or {},
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                default=str,
            ),
            dialect,
            transpiler_version(),
        ]
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def parity_state(case: dict, fingerprint: str, dialect: str) -> str:
    """État d'attestation d'un cas : verified / stale / unverified (jamais bloquant)."""
    attestation = case.get("parity") or {}
    if not attestation.get("fingerprint"):
        return "unverified"
    if (
        attestation["fingerprint"] == fingerprint
        and attestation.get("dialect") == dialect
    ):
        return "verified"
    return "stale"


# ── Requête mockée en dialecte warehouse ──────────────────────────────────────

_TEXT_BASE_TYPES = {"STRING", "TEXT", "VARCHAR", "CHAR", "CHARACTER", "NVARCHAR"}
_INT_BASE_TYPES = {"INT", "INT64", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}
_FLOAT_BASE_TYPES = {"FLOAT", "FLOAT64", "DOUBLE", "REAL"}
_TEMPORAL_BASE_TYPES = {
    "DATE",
    "TIME",
    "DATETIME",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "TIMESTAMP_LTZ",
    "TIMESTAMP_TZ",
}
_SEMI_STRUCTURED_TYPES = {"VARIANT", "OBJECT", "JSON"}


def _native_type(col: dict) -> str:
    """Type warehouse natif d'une colonne du schema_cache (bq_ddl_type prioritaire :
    il porte les STRUCT<>/ARRAY<> complets côté BigQuery)."""
    return (col.get("bq_ddl_type") or col.get("type") or "STRING").strip()


def _escape_str(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("'", "''")


def _source_expr(value: Any, native_type: str, dialect: str) -> str:
    """Littéral SQL typé pour une valeur de test, dans le dialecte SOURCE.

    Miroir de `to_duck_expr` (utils/insert_examples.py) : la même valeur générée doit
    devenir la même donnée des deux côtés, sinon la divergence mesurée serait celle de
    la sérialisation, pas de la couche d'émulation DuckDB.
    """
    from utils.examples import _split_ddl_struct_fields
    from utils.insert_examples import (
        _strip_nested_quote_artifact,
        _strip_surrounding_quotes,
        _to_duck_bool,
    )

    native_type = native_type.strip()
    upper = native_type.upper()

    if value is None:
        return f"CAST(NULL AS {native_type})"

    # BigQuery imbriqué : ARRAY<...> / STRUCT<...> reconstruits récursivement.
    if upper.startswith("ARRAY<"):
        inner = native_type[6:-1]
        if isinstance(value, list):
            if not value:
                return f"CAST([] AS {native_type})"
            elements = ", ".join(_source_expr(v, inner, dialect) for v in value)
            return f"[{elements}]"
        return f"CAST(NULL AS {native_type})"
    if upper.startswith("STRUCT<"):
        pairs = _split_ddl_struct_fields(native_type[7:-1])
        if isinstance(value, dict) and pairs:
            parts = []
            for fname, ftype in pairs:
                v = next(
                    (val for k, val in value.items() if k.lower() == fname.lower()),
                    None,
                )
                parts.append(f"{_source_expr(v, ftype, dialect)} AS {fname}")
            return f"STRUCT({', '.join(parts)})"
        return f"CAST(NULL AS {native_type})"

    # Semi-structuré (VARIANT/OBJECT Snowflake, JSON BigQuery) : PARSE_JSON.
    base = upper.split("(")[0].strip()
    if base in _SEMI_STRUCTURED_TYPES or (upper == "ARRAY" and dialect == "snowflake"):
        payload = (
            value
            if isinstance(value, str)
            else json.dumps(value, ensure_ascii=False, default=str)
        )
        return f"PARSE_JSON('{_escape_str(payload)}')"

    if base == "GEOGRAPHY":
        clean = _strip_surrounding_quotes(value)
        return f"ST_GEOGFROMTEXT('{_escape_str(clean)}')"

    if base in _TEXT_BASE_TYPES:
        cleaned = _strip_nested_quote_artifact(value)
        return f"'{_escape_str(cleaned)}'"

    if base in ("BOOL", "BOOLEAN"):
        return _to_duck_bool(value)

    # Scalaires typés non-texte : mêmes nettoyages d'artefacts que le chemin DuckDB.
    clean = _strip_surrounding_quotes(value)
    if base in _INT_BASE_TYPES:
        return str(int(clean))
    if base in _FLOAT_BASE_TYPES:
        return str(float(clean))
    if base in _TEMPORAL_BASE_TYPES:
        return f"CAST('{_escape_str(clean)}' AS {native_type})"

    # Défaut (NUMBER/DECIMAL/NUMERIC…) : littéral string casté vers le type natif.
    return f"CAST('{_escape_str(clean)}' AS {native_type})"


def _mock_cte_body(columns: list[dict], rows: list[dict], dialect: str) -> str:
    """Corps d'une CTE mock : une ligne = un SELECT sans FROM, typé colonne par
    colonne selon le vrai schéma warehouse (« vrai schéma, zéro inférence », même
    principe que le réplay). Table vide → typage préservé via LIMIT 0."""
    from utils.sqlglot_ast import quote_identifier

    root_columns = [c for c in columns if "." not in c["name"]]

    if not rows:
        exprs = ", ".join(
            f"CAST(NULL AS {_native_type(col)}) AS "
            f"{quote_identifier(col['name'], dialect)}"
            for col in root_columns
        )
        return f"SELECT {exprs} LIMIT 0"

    selects = []
    for row in rows:
        exprs = []
        for col in root_columns:
            value = next(
                (v for k, v in row.items() if k.lower() == col["name"].lower()),
                None,
            )
            exprs.append(
                f"{_source_expr(value, _native_type(col), dialect)} AS "
                f"{quote_identifier(col['name'], dialect)}"
            )
        selects.append("SELECT " + ", ".join(exprs))
    return "\nUNION ALL\n".join(selects)


def _flatten_table_key(name: str) -> str:
    from cli.test_runner import _flatten_table_key as flatten

    return flatten(name)


def build_mocked_warehouse_sql(
    sql: str,
    dialect: str,
    schemas: list[dict],
    case_data: dict,
) -> str:
    """Requête mockée émise dans le dialecte SOURCE : les refs de tables physiques
    sont remplacées par des CTEs inline portant les lignes synthétiques du cas.

    Même mécanique de renommage que l'executor DuckDB (strip_qualifiers_with_scope,
    suffixe stable) — mais AUCUNE transpilation : côté warehouse le SQL du modèle
    s'exécute quasi-nativement, la différence mesurée est celle de DuckDB.
    """
    import sqlglot
    from sqlglot import expressions as exp

    from utils.examples import strip_qualifiers_with_scope

    # 1. Tables physiques réellement référencées (avec dataset/schema), telles
    #    qu'épelées dans le SQL — le renommage reprend cette épellation.
    tree = sqlglot.parse_one(sql, read=dialect)
    referenced: dict[str, str] = {}  # flat_key (lower) → nom de CTE renommé
    for table in tree.find_all(exp.Table):
        if not table.db:
            continue
        flat_key = _flatten_table_key(f"{table.db}.{table.name}")
        referenced.setdefault(flat_key, f"{table.db}_{table.name}_{_MOCK_SUFFIX}")

    # 2. Index schémas + données par clé aplatie (même rapprochement que le réplay).
    schema_by_key = {_flatten_table_key(s["table_name"]): s for s in schemas}
    data_by_key: dict[str, list[dict]] = {}
    for tname, rows in (case_data or {}).items():
        if isinstance(rows, list):
            data_by_key[_flatten_table_key(tname)] = rows

    mock_ctes: list[tuple[str, str]] = []
    for flat_key, cte_name in referenced.items():
        schema = schema_by_key.get(flat_key)
        if schema is None:
            # Même contrat que le réplay : le vrai schéma est obligatoire.
            from cli.test_runner import SchemaMissingError

            raise SchemaMissingError([flat_key], {})
        rows = data_by_key.get(flat_key) or []
        mock_ctes.append((cte_name, _mock_cte_body(schema["columns"], rows, dialect)))

    # 3. Renommage des refs (mécanique executor), puis fusion des CTEs mock EN TÊTE
    #    du WITH existant — une CTE ne peut référencer que des CTEs définies avant.
    stripped = strip_qualifiers_with_scope(
        sql_query=sql, dialect=dialect, suffix=_MOCK_SUFFIX
    )
    mocked = sqlglot.parse_one(stripped, read=dialect)
    existing_with = mocked.args.get("with") or mocked.args.get("with_")
    n_existing = len(existing_with.expressions) if existing_with else 0
    for cte_name, body in mock_ctes:
        mocked = mocked.with_(cte_name, as_=body, dialect=dialect, copy=False)
    merged_with = mocked.args.get("with") or mocked.args.get("with_")
    if n_existing and merged_with is not None:
        exprs = list(merged_with.expressions)
        merged_with.set("expressions", exprs[n_existing:] + exprs[:n_existing])
    return mocked.sql(dialect=dialect, pretty=True)


def has_terminal_order_by(sql: str, dialect: str) -> bool:
    """ORDER BY terminal → la comparaison devient ordonnée (sinon multiset)."""
    import sqlglot

    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return False
    return tree.args.get("order") is not None


# ── Exécution warehouse ───────────────────────────────────────────────────────


def _execute_on_warehouse(sql: str, dialect: str) -> list[dict]:
    """Exécute la requête mockée sur la warehouse via les connecteurs existants
    (imports gardés — BigQuery/Snowflake restent des extras optionnels)."""
    try:
        if dialect == "bigquery":
            from models.env_variables import BQ_TEST_PROJECT
            from utils.optional_deps import import_bigquery

            if not BQ_TEST_PROJECT:
                raise ParityExecutionError(
                    "BQ_TEST_PROJECT (ou VERTEX_PROJECT) non défini — requis pour "
                    "exécuter la requête mockée sur BigQuery."
                )
            bigquery = import_bigquery()
            client = bigquery.Client(project=BQ_TEST_PROJECT)
            return [dict(row) for row in client.query(sql).result()]
        if dialect == "snowflake":
            from utils.snowflake_connector import run_sf_query

            return run_sf_query(sql)
        raise ParityExecutionError(
            f"Dialecte '{dialect}' non supporté par `mocksql parity` v1 "
            f"(supportés : {', '.join(PARITY_DIALECTS)})."
        )
    except ParityExecutionError:
        raise
    except ImportError as exc:
        raise ParityExecutionError(str(exc)) from exc
    except Exception as exc:
        logger.error(
            "Exécution warehouse (%s) échouée : %s\n  Requête : %s", dialect, exc, sql
        )
        raise ParityExecutionError(f"Exécution warehouse échouée : {exc}") from exc


# ── Comparateur de résultats ──────────────────────────────────────────────────


def _canon_json(value: Any) -> Any:
    """Canonicalise une valeur JSON (VARIANT Snowflake pretty-printé vs JSON DuckDB
    compact) : floats entiers → int, récursif sur dict/list."""
    if isinstance(value, dict):
        return {k: _canon_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_canon_json(v) for v in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def normalize_value(value: Any) -> Any:
    """Normalisation AVANT comparaison (règles v1 de la spec) :
    NULL ≡ NULL quel que soit le type porteur ; numériques → Decimal canonique ;
    dates/timestamps → ISO 8601 UTC ; chaînes telles quelles (une différence de
    casse/trim EST un diff) ; JSON/structs → forme canonique."""
    if value is None:
        return None
    # Tableaux/scalaires numpy (résultats DuckDB via pandas) → types Python.
    if type(value).__module__ == "numpy":
        value = value.tolist()
    try:
        if value != value:  # NaN / NaT (pandas)
            return None
    except Exception:
        pass
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        try:
            return Decimal(str(value))
        except InvalidOperation:
            return str(value)
    if isinstance(value, dt.datetime):
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value.isoformat(sep="T", timespec="microseconds")
    if isinstance(value, dt.date):
        return value.isoformat()
    if isinstance(value, dt.time):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray)):
        return "0x" + bytes(value).hex()
    if isinstance(value, str):
        stripped = value.strip()
        if stripped[:1] in "[{":
            try:
                return json.dumps(
                    _canon_json(json.loads(stripped)),
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=False,
                )
            except Exception:
                return value
        return value
    if isinstance(value, (dict, list)):
        return json.dumps(
            _canon_json(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            default=str,
        )
    return str(value)


def _canon_decimal(value: Decimal) -> str:
    """Représentation stable d'un Decimal pour la clé de multiset (12 chiffres
    significatifs — sous la tolérance relative 1e-9)."""
    if value == 0:
        return "0"
    try:
        quantized = Decimal(f"{value:.12g}")
    except (InvalidOperation, ValueError):
        quantized = value
    return format(quantized.normalize(), "f")


def _row_key(row: list[Any]) -> str:
    return json.dumps(
        [_canon_decimal(v) if isinstance(v, Decimal) else v for v in row],
        ensure_ascii=False,
        default=str,
    )


def _values_equal(a: Any, b: Any) -> bool:
    if isinstance(a, Decimal) and isinstance(b, Decimal):
        if a == b:
            return True
        return math.isclose(
            float(a), float(b), rel_tol=FLOAT_REL_TOL, abs_tol=FLOAT_ABS_TOL
        )
    return a == b


def _rows_equal(a: list[Any], b: list[Any]) -> bool:
    return len(a) == len(b) and all(_values_equal(x, y) for x, y in zip(a, b))


def _align_rows(
    local_rows: list[dict], warehouse_rows: list[dict]
) -> tuple[list[list], list[list], list[str]] | None:
    """Aligne les deux jeux en listes de valeurs normalisées.

    Noms de colonnes comparés insensibles à la casse (Snowflake upper-case par
    défaut). Si les noms divergent (projections non nommées : `_col_0` DuckDB vs
    `f0_` BigQuery) mais que le NOMBRE de colonnes concorde, on aligne par position
    — un nom auto-généré par le moteur ne doit pas fabriquer un faux DIFF.
    Retourne None si l'alignement est impossible (nombre de colonnes différent).
    """
    local_cols = [str(c).lower() for c in (local_rows[0].keys() if local_rows else [])]
    wh_cols = [
        str(c).lower() for c in (warehouse_rows[0].keys() if warehouse_rows else [])
    ]

    if local_rows and warehouse_rows and len(local_cols) != len(wh_cols):
        return None

    by_name = bool(local_cols) and sorted(local_cols) == sorted(wh_cols)

    def _local(row: dict) -> list:
        return [normalize_value(v) for v in row.values()]

    def _warehouse(row: dict) -> list:
        if by_name:
            lowered = {str(k).lower(): v for k, v in row.items()}
            return [normalize_value(lowered[c]) for c in local_cols]
        return [normalize_value(v) for v in row.values()]

    return (
        [_local(r) for r in local_rows],
        [_warehouse(r) for r in warehouse_rows],
        local_cols or wh_cols,
    )


def _preview(rows: list[list], columns: list[str]) -> list[dict]:
    out = []
    for row in rows[:_DIFF_PREVIEW_ROWS]:
        if columns and len(columns) == len(row):
            out.append(
                {
                    c: str(v) if isinstance(v, Decimal) else v
                    for c, v in zip(columns, row)
                }
            )
        else:
            out.append({"row": [str(v) for v in row]})
    return out


def compare_results(
    local_rows: list[dict],
    warehouse_rows: list[dict],
    ordered: bool,
) -> dict | None:
    """Compare les résultats DuckDB et warehouse. Retourne None si parité, sinon un
    diff {reason, local_count, warehouse_count, local_only, warehouse_only}.

    Tout ce qui ne rentre pas dans les règles de normalisation EST un diff — on
    préfère un diff explicable à une normalisation trop agressive qui masquerait
    une vraie divergence.
    """
    aligned = _align_rows(local_rows, warehouse_rows)
    if aligned is None:
        return {
            "reason": "columns_mismatch",
            "local_count": len(local_rows),
            "warehouse_count": len(warehouse_rows),
            "local_only": _preview([list(r.values()) for r in local_rows], []),
            "warehouse_only": _preview([list(r.values()) for r in warehouse_rows], []),
        }
    local, warehouse, columns = aligned

    if ordered:
        mismatched = [
            i for i, (a, b) in enumerate(zip(local, warehouse)) if not _rows_equal(a, b)
        ]
        if not mismatched and len(local) == len(warehouse):
            return None
        local_only = [local[i] for i in mismatched] + local[len(warehouse) :]
        wh_only = [warehouse[i] for i in mismatched] + warehouse[len(local) :]
    else:
        local_counter = Counter(_row_key(r) for r in local)
        wh_counter = Counter(_row_key(r) for r in warehouse)
        if local_counter == wh_counter:
            return None
        # Restes après annulation exacte → appariement tolérant (flottants proches
        # de la frontière d'arrondi de la clé canonique).
        local_rest = _leftover(local, local_counter - wh_counter)
        wh_rest = _leftover(warehouse, wh_counter - local_counter)
        local_only, wh_only = _tolerant_match(local_rest, wh_rest)
        if not local_only and not wh_only:
            return None

    return {
        "reason": "rows_mismatch",
        "local_count": len(local),
        "warehouse_count": len(warehouse),
        "local_only": _preview(local_only, columns),
        "warehouse_only": _preview(wh_only, columns),
    }


def _leftover(rows: list[list], excess: Counter) -> list[list]:
    """Extrait les lignes correspondant aux clés en excès (multiset)."""
    remaining = Counter(excess)
    out = []
    for row in rows:
        key = _row_key(row)
        if remaining.get(key, 0) > 0:
            remaining[key] -= 1
            out.append(row)
    return out


def _tolerant_match(
    local_rest: list[list], wh_rest: list[list]
) -> tuple[list[list], list[list]]:
    """Appariement glouton avec tolérance flottante sur les restes du multiset."""
    unmatched_wh = list(wh_rest)
    local_only = []
    for row in local_rest:
        for i, candidate in enumerate(unmatched_wh):
            if _rows_equal(row, candidate):
                unmatched_wh.pop(i)
                break
        else:
            local_only.append(row)
    return local_only, unmatched_wh


# ── Orchestration ─────────────────────────────────────────────────────────────


def _is_deadborn(case: dict) -> bool:
    """Mort-né = non exécutable, sauté par l'audit. Couvre la forme mémoire
    (`status`, cache présent) ET la forme définition seule (`exec_status`, clone/CI
    sans sidecar) — même exemption « PASS vide intentionnel » que is_deadborn_case."""
    from storage.test_files import (
        _SUCCESS_VERDICTS,
        FAILED_EXEC_STATUSES,
        is_deadborn_case,
    )

    if is_deadborn_case(case):
        return True
    exec_status = case.get("exec_status")
    if exec_status not in FAILED_EXEC_STATUSES:
        return False
    return not (
        exec_status == "empty_results" and case.get("verdict") in _SUCCESS_VERDICTS
    )


async def _run_duckdb_case(
    test_case: dict,
    duckdb_schemas: list[dict],
    used_columns_parsed: list[dict],
    suffix: str,
    con,
    precompiled_sql: str,
    sql: str,
    dialect: str,
) -> list[dict]:
    """Rejoue UN cas sur DuckDB (chemin `mocksql test`) et retourne les lignes brutes."""
    from utils.examples import execute_queries, run_query_on_test_dataset
    from utils.insert_examples import insert_examples, replace_missing_with_null

    for schema in duckdb_schemas:
        con.execute(f'DELETE FROM "{schema["table_name"]}"')

    test_data = replace_missing_with_null(test_case.get("data") or {}, duckdb_schemas)
    insert_stmts = list(
        insert_examples(
            data_dict=test_data,
            schemas=duckdb_schemas,
            suffix=suffix,
            used_columns=used_columns_parsed or None,
        )
    )
    execute_queries(insert_stmts, con)

    result_df, _ = await run_query_on_test_dataset(
        sql, suffix, "cli", dialect, con, precompiled_sql=precompiled_sql
    )
    return result_df.to_dict("records")


def _utc_now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def run_parity(
    config_path: Path,
    model_filters: list[str] | None = None,
    force_all: bool = False,
) -> tuple[int, list[dict]]:
    """Audit de parité sur les tests sauvegardés.

    Idempotent par défaut : seuls les tests non vérifiés ou à empreinte périmée sont
    rejoués (`--all` force le rejeu). Une parité constatée écrit l'attestation dans la
    définition committée ; un diff n'écrit RIEN (le diff est dans la sortie CLI).

    Retourne (exit_code, model_results) :
      0 = tous les tests rejoués concordent (ou rien à rejouer)
      1 = au moins un diff
      2 = erreur d'exécution warehouse (credentials, réseau, SQL rejeté)
    """
    import os

    import storage.config as storage_config
    from cli.test_runner import (
        _load_config,
        _load_schema_cache,
        _read_json,
        _resolve_model_schemas,
        _setup_model,
        _UUID_RE,
        resolve_run_sql,
    )
    from storage.test_files import write_test_doc
    from utils.examples import DB_PATH, initialize_duckdb

    # Aligne storage.config (extensions DuckDB, etc.) sur le projet ciblé par
    # --config — même piège MOCKSQL_BASE_DIR que `mocksql generate`.
    os.environ["MOCKSQL_BASE_DIR"] = str(config_path.resolve().parent)
    storage_config.load_config.cache_clear()

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    if dialect not in PARITY_DIALECTS:
        raise ParityExecutionError(
            f"Dialecte '{dialect}' non supporté par `mocksql parity` v1 "
            f"(supportés : {', '.join(PARITY_DIALECTS)})."
        )
    cache_path = str(
        config_path.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    schema_cache = _load_schema_cache(cache_path)

    tests_root = config_path.parent / ".mocksql" / "tests"
    if not tests_root.exists():
        return 0, []
    test_files = sorted(
        f for f in tests_root.rglob("*.json") if not _UUID_RE.match(f.stem)
    )

    session_prefix = uuid.uuid4().hex[:8]
    model_results: list[dict] = []
    has_diff = False
    has_error = False

    with initialize_duckdb(DB_PATH) as con:
        for test_file in test_files:
            model_name = test_file.relative_to(tests_root).with_suffix("").as_posix()
            if model_filters and model_name not in model_filters:
                continue
            doc = _read_json(test_file)
            if not doc:
                continue

            sql, sql_source = resolve_run_sql(
                cfg=cfg,
                config_path=config_path,
                model_name=model_name,
                snapshot_sql=doc.get("sql", ""),
                frozen=False,
            )
            used_columns_raw: list[str] = doc.get("used_columns") or []
            used_columns_parsed: list[dict] = []
            for raw in used_columns_raw:
                try:
                    used_columns_parsed.append(json.loads(raw))
                except Exception:
                    pass
            test_cases: list[dict] = doc.get("test_cases") or []

            model_suffix = (
                f"{session_prefix}_{re.sub(r'[^a-z0-9]', '_', model_name.lower())}"
            )
            duckdb_schemas: list[dict] = []
            schemas: list[dict] = []
            precompiled_sql = ""
            setup_error: str | None = None
            try:
                schemas = _resolve_model_schemas(
                    used_columns_raw, schema_cache, test_cases
                )
                duckdb_schemas, precompiled_sql = await _setup_model(
                    schemas=schemas,
                    sql=sql,
                    dialect=dialect,
                    suffix=model_suffix,
                    con=con,
                )
            except Exception as exc:
                setup_error = str(exc)

            ordered = has_terminal_order_by(sql, dialect)
            case_results: list[dict] = []
            doc_changed = False

            for tc in test_cases:
                name = (
                    (tc.get("test_name") or "").strip()
                    or (tc.get("unit_test_description") or "").strip()
                    or f"Test {tc.get('test_index', '?')}"
                )
                base = {"index": str(tc.get("test_index", "?")), "name": name}

                if _is_deadborn(tc):
                    case_results.append(
                        {
                            **base,
                            "state": "skip",
                            "detail": "test mort-né, non exécutable",
                        }
                    )
                    continue
                if not tc.get("data"):
                    case_results.append(
                        {**base, "state": "skip", "detail": "aucune donnée de test"}
                    )
                    continue

                fingerprint = compute_fingerprint(sql, tc.get("data"), dialect)
                if (
                    not force_all
                    and parity_state(tc, fingerprint, dialect) == "verified"
                ):
                    case_results.append({**base, "state": "verified_cached"})
                    continue

                if setup_error is not None:
                    has_error = True
                    case_results.append(
                        {**base, "state": "error", "detail": setup_error}
                    )
                    continue

                try:
                    local_rows = await _run_duckdb_case(
                        test_case=tc,
                        duckdb_schemas=duckdb_schemas,
                        used_columns_parsed=used_columns_parsed,
                        suffix=model_suffix,
                        con=con,
                        precompiled_sql=precompiled_sql,
                        sql=sql,
                        dialect=dialect,
                    )
                    mocked_sql = build_mocked_warehouse_sql(
                        sql=sql,
                        dialect=dialect,
                        schemas=schemas,
                        case_data=tc.get("data") or {},
                    )
                    warehouse_rows = _execute_on_warehouse(mocked_sql, dialect)
                except ParityExecutionError as exc:
                    has_error = True
                    case_results.append({**base, "state": "error", "detail": str(exc)})
                    continue
                except Exception as exc:
                    has_error = True
                    logger.error(
                        "Rejeu parité échoué (%s / %s) : %s\n  Requête : %s",
                        model_name,
                        name,
                        exc,
                        sql,
                    )
                    case_results.append({**base, "state": "error", "detail": str(exc)})
                    continue

                diff = compare_results(local_rows, warehouse_rows, ordered)
                if diff is None:
                    tc["parity"] = {
                        "fingerprint": fingerprint,
                        "verified_at": _utc_now_iso(),
                        "dialect": dialect,
                    }
                    doc_changed = True
                    case_results.append({**base, "state": "verified"})
                else:
                    # Un diff n'écrit RIEN (pas d'attestation négative committée).
                    has_diff = True
                    case_results.append({**base, "state": "diff", "diff": diff})

            if doc_changed:
                write_test_doc(test_file, doc)

            model_results.append(
                {
                    "model": model_name,
                    "dialect": dialect,
                    "sql_source": sql_source,
                    "cases": case_results,
                }
            )

    exit_code = 2 if has_error else 1 if has_diff else 0
    return exit_code, model_results
