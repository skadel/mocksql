import asyncio
import json
from typing import Optional, List, Any

from langgraph.errors import GraphRecursionError

import dotenv
import sqlglot
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from common_vars import get_tables_mapping
from models.env_variables import (
    AUTO_SCHEMA_IMPORT,
    AUTO_PROFILING,
    BQ_TEST_PROJECT,
)
from storage.test_repository import (
    get_test,
    get_model_file_git_sha,
    get_model_file_hash,
    update_test,
)
from utils.sql_code import extract_real_table_refs

dotenv.load_dotenv()

router = APIRouter()


async def get_connection_url(request: Request) -> str:
    db_url: Optional[str] = request.state.pg_connection_url
    if not db_url:
        raise HTTPException(400, "Postgres URL missing.")
    return db_url


class ValidateQueryRequest(BaseModel):
    sql: str
    project: str
    dialect: str
    session: str
    parent_message_id: str = ""


def _extract_validate_error(result: dict) -> str:
    if result.get("error"):
        return str(result["error"])
    if result.get("compilation_error"):
        return str(result["compilation_error"])
    for msg in result.get("messages", []) + result.get("solver_messages", []):
        msg_type = (getattr(msg, "additional_kwargs", None) or {}).get("type", "")
        if msg_type == "error":
            return str(getattr(msg, "content", ""))
    return "Requête invalide"


def _qualify_two_part_refs(
    sql: str, all_tables: list, billing_project: str, dialect: str
) -> str:
    """Prepend billing_project to dataset.table refs that have no catalog."""
    two_part_refs = {
        (t.db.lower(), t.name.lower()) for t in all_tables if t.db and not t.catalog
    }
    if not two_part_refs:
        return sql

    def _qualify_node(node):
        if (
            isinstance(node, sqlglot.exp.Table)
            and node.db
            and not node.catalog
            and (node.db.lower(), node.name.lower()) in two_part_refs
        ):
            # quoted=True : un project id peut contenir un tiret (ex. `pipetalk-493612`).
            # Sans backticks, BigQuery lit `mon-project` comme une soustraction et le
            # dry-run échoue. Quoter le seul segment projet est toujours valide.
            node.set(
                "catalog",
                sqlglot.exp.Identifier(this=billing_project, quoted=True),
            )
        return node

    return (
        sqlglot.parse_one(sql, dialect=dialect)
        .transform(_qualify_node)
        .sql(dialect=dialect, pretty=True)
    )


@router.post("/validate-query")
async def validate_query_route(body: ValidateQueryRequest):
    from build_query.validator import validate_query as _validate_query

    billing_project = BQ_TEST_PROJECT

    # Cache check: if the session already has a validated result for the same SQL,
    # return it immediately without recompiling.
    cached_session = get_test(body.session) if body.session else None
    if (
        cached_session
        and cached_session.get("sql") == body.sql
        and cached_session.get("used_columns")
        and cached_session.get("optimized_sql")
    ):
        return {
            "valid": True,
            "used_columns": cached_session["used_columns"],
            "query_decomposed": "",
            "optimized_sql": cached_session["optimized_sql"],
            "sql_message_id": "",
            "sql_history_id": "",
        }

    # Pre-check: verify all tables referenced in the query exist in the schema.
    # Only the SQL parsing step is guarded — I/O errors (DB, network) propagate normally.
    sql = body.sql
    all_tables = []
    try:
        all_tables = extract_real_table_refs(body.sql, body.dialect)
    except Exception:
        pass  # parsing failure → let the validator handle it

    if all_tables:
        query_tables = {
            (f"{t.db}.{t.name}" if t.db else t.name).lower() for t in all_tables
        }
        tables_mapping = await get_tables_mapping(project_id=body.project)
        schema_tables = {name.lower() for name in tables_mapping}
        missing = sorted(query_tables - schema_tables)
        if missing:
            response: dict = {
                "valid": False,
                "missing_tables": missing,
                "error": f"Tables introuvables dans le schéma : {', '.join(missing)}",
            }
            if AUTO_SCHEMA_IMPORT:
                name_to_full: dict = {}
                for t in all_tables:
                    parts = [p for p in [t.catalog, t.db, t.name] if p]
                    key = (f"{t.db}.{t.name}" if t.db else t.name).lower()
                    name_to_full[key] = ".".join(parts)
                response["auto_import_available"] = True
                response["tables_to_import"] = [name_to_full.get(m, m) for m in missing]
            return response

        if body.dialect == "bigquery" and billing_project:
            try:
                sql = _qualify_two_part_refs(
                    body.sql, all_tables, billing_project, body.dialect
                )
            except Exception:
                pass  # qualification failure → use original SQL

    state = {
        "query": sql,
        "project": body.project,
        "dialect": body.dialect,
        "user": "local",
        "route": "",
        "messages": [],
        "optimize": False,
        "used_columns": [],
    }

    try:
        result = await _validate_query(sql, body.project, body.dialect, None, state)
    except HTTPException as exc:
        return {"valid": False, "error": exc.detail}
    except Exception as exc:
        return {"valid": False, "error": str(exc)}

    if result.get("status") != "success":
        return {"valid": False, "error": _extract_validate_error(result)}

    used_columns = result.get("used_columns", [])
    optimized_sql = result.get("optimized_sql", "")

    if optimized_sql and body.dialect == "bigquery" and billing_project and all_tables:
        try:
            optimized_sql = _qualify_two_part_refs(
                optimized_sql, all_tables, billing_project, body.dialect
            )
        except Exception:
            pass

    if body.session:
        model_name = (cached_session or {}).get("model_name", "")
        source_sha = get_model_file_git_sha(model_name) if model_name else None
        source_hash = get_model_file_hash(model_name) if model_name else None
        session_update: dict = {
            "sql": body.sql,
            "used_columns": used_columns,
            "optimized_sql": optimized_sql,
            # Catalogue des paths UNION ALL (None si pas d'union de 1er niveau) : écrit
            # ici avec optimized_sql pour qu'un changement de SQL rafraîchisse/efface le
            # catalogue périmé (apparié à la requête validée).
            "path_plans": result.get("path_plans"),
        }
        if source_sha:
            session_update["source_sha"] = source_sha
        if source_hash:
            session_update["source_hash"] = source_hash
        update_test(body.session, session_update)

    return {
        "valid": True,
        "used_columns": used_columns,
        "query_decomposed": result.get("query_decomposed", ""),
        "optimized_sql": optimized_sql,
        "sql_message_id": "",
        "sql_history_id": "",
    }


class CheckProfileRequest(BaseModel):
    sql: str
    project: str
    dialect: str
    session: str
    used_columns: list
    force: bool = False


@router.post("/check-profile")
async def check_profile_route(body: CheckProfileRequest):
    import logging
    from build_query.profile_checker import (
        check_profile,
        _find_missing_columns,
    )

    used_columns = body.used_columns
    if not used_columns and body.session:
        test = get_test(body.session)
        if test:
            used_columns = test.get("used_columns") or []

    state = {
        "project": body.project,
        "user": "local",
        "dialect": body.dialect,
        "session": body.session,
        "query": body.sql,
        "used_columns": used_columns,
        "schemas": [],
        "messages": [],
        "parent_message_id": "",
        "request_id": "",
    }

    if body.force:
        missing_columns = _find_missing_columns({}, used_columns)
        if not missing_columns:
            return {"profile_complete": True}
    else:
        try:
            checked = await check_profile(state)
        except Exception as exc:
            logging.getLogger(__name__).error("[check_profile] profiler error: %s", exc)
            return {"profile_complete": False, "profile_error": str(exc)}

        if checked["profile_complete"]:
            return {"profile_complete": True}

        missing_columns = checked["missing_columns"]

    from storage.config import get_profile_budget_tb

    return {
        "profile_complete": False,
        "auto_profile_available": AUTO_PROFILING,
        "missing_columns": missing_columns,
        # Budget de scan configuré (mocksql.yml / env). None => le front demande
        # une valeur à l'utilisateur (défaut proposé 0.3 To).
        "profile_budget_tb": get_profile_budget_tb(),
    }


@router.get("/profile-meta")
async def profile_meta_route():
    """Return metadata about the stored profile (freshness timestamp).

    Used by the UI to show "profilé il y a N j" next to the refresh button,
    giving a concrete reason to re-scan the schema/profile.
    """
    from models.schemas import get_profiled_at

    return {"profiled_at": get_profiled_at()}


class BuildProfileRequestBody(BaseModel):
    sql: str
    project: str
    dialect: str
    session: str
    missing_columns: list
    # Budget de scan (To). None => profile tout (pas de budget appliqué).
    budget_tb: Optional[float] = None


@router.post("/build-profile-request")
async def build_profile_request_route(body: BuildProfileRequestBody):
    import logging
    from build_query.profile_checker import build_profile_request

    state = {
        "project": body.project,
        "user": "local",
        "dialect": body.dialect,
        "session": body.session,
        "query": body.sql,
        "used_columns": [],
        "schemas": [],
        "messages": [],
        "parent_message_id": "",
        "request_id": "",
    }

    try:
        request = await build_profile_request(
            state, body.missing_columns, budget_tb=body.budget_tb
        )
    except Exception as exc:
        logging.getLogger(__name__).error("[build_profile_request] error: %s", exc)
        raise HTTPException(status_code=500, detail=str(exc))

    profile_request: dict = {
        "profile_query": request.get("profile_sql", ""),
        "profile_queries": request.get("profile_queries", []),
        "missing_columns": request.get("missing_columns", []),
        "expected_joins": request.get("expected_joins", []),
        "partition_limit": request.get("partition_limit", 3),
        # Tables différées (au-dessus du budget) + budget appliqué — pilotent la
        # bannière "profil partiel" et le bouton "Compléter le profil".
        "deferred": request.get("deferred", []),
        "budget_tb": request.get("budget_tb"),
    }
    billing_tb = request.get("profile_billing_tb")
    if billing_tb is not None:
        profile_request["billing_tb"] = billing_tb

    return {"profile_request": profile_request}


class SkipProfilingRequest(BaseModel):
    session: str
    user: str = "local"


@router.post("/skip-profile")
async def skip_profiling_route(body: SkipProfilingRequest):
    if body.session:
        update_test(body.session, {"profile_skipped": True})
    return {"skipped": True}


class AutoProfileRequest(BaseModel):
    profile_sql: str
    profile_queries: List[str] = []
    project: str
    user: str = "local"
    session: str
    # The partition window the profile SQL was built with (echoed from
    # /build-profile-request). Defaults to 3 to match the build-side default.
    partition_limit: int = 3


@router.post("/auto-profile")
async def auto_profile_route(body: AutoProfileRequest):

    from utils.optional_deps import import_bigquery
    from build_query.profile_checker import (
        _normalize_profile,
        _load_model_profile,
        _merge_profiles,
        _save_model_profile,
        enrich_joins_with_cte_context,
        enrich_tables_with_partition_window,
        flag_disjoint_partition_joins,
    )

    _bq = import_bigquery()
    billing_project = BQ_TEST_PROJECT
    client = _bq.Client(project=billing_project)

    async def _run_query(sql: str) -> list:
        return await asyncio.to_thread(lambda: list(client.query(sql).result()))

    raw_rows: list = []
    errors: list = []

    if body.profile_queries:
        for i, q in enumerate(body.profile_queries):
            try:
                rows = await _run_query(q)
                raw_rows.extend(rows)
            except Exception as exc:
                errors.append({"query_index": i, "error": str(exc)})
    else:
        # Fallback: single combined query (legacy callers)
        try:
            raw_rows = await _run_query(body.profile_sql)
        except Exception as exc:
            raise HTTPException(
                status_code=502, detail=f"BigQuery profiling error: {exc}"
            )

    profile_status = "complete" if not errors else "partial" if raw_rows else "failed"

    if not raw_rows:
        return {"saved": False, "profile_status": profile_status, "errors": errors}

    raw_profile = [dict(row) for row in raw_rows]
    incoming_profile = _normalize_profile(raw_profile)
    if not incoming_profile:
        return {"saved": False, "profile_status": "failed", "errors": errors}

    # Tag each table with the partition window the profile was restricted to, so
    # the generator/agents read min/max dates as the *profiling scope*, not the
    # table's full history.
    try:
        from models.schemas import get_schemas

        _schemas = await get_schemas(project_id=body.project)
        incoming_profile = enrich_tables_with_partition_window(
            incoming_profile, _schemas or [], body.partition_limit
        )
        # A 0% match between two partitioned tables is almost always disjoint
        # per-side windows, not an empty join → flag it so the prompt formatter
        # masks the misleading 0% (the join is kept, so no re-profiling loop).
        incoming_profile = flag_disjoint_partition_joins(incoming_profile)
    except Exception as _pw_exc:
        import logging

        logging.getLogger(__name__).warning(
            "[auto-profile] partition-window enrichment failed: %s", _pw_exc
        )

    # Enrich join entries with their CTE SQL (phase 1) so the profile is self-contained
    if incoming_profile.get("joins"):
        from storage.test_repository import get_test

        test = get_test(body.session)
        if test:
            model_sql = test.get("optimized_sql") or test.get("sql") or ""
            dialect = test.get("dialect", "bigquery")
            if model_sql:
                incoming_profile["joins"] = enrich_joins_with_cte_context(
                    incoming_profile["joins"], model_sql, dialect
                )

    stored_profile = _normalize_profile(_load_model_profile())
    merged = _merge_profiles(stored_profile, incoming_profile)
    _save_model_profile(merged)

    return {"saved": True, "profile_status": profile_status, "errors": errors}


class ImportMissingTablesRequest(BaseModel):
    tables_to_import: List[str]
    project: str
    dialect: str = "bigquery"


@router.post("/import-missing-tables")
async def import_missing_tables_route(body: ImportMissingTablesRequest):
    from build_query.schema_fetcher import (
        fetch_tables_schema,
        fetch_tables_schema_snowflake,
        fetch_tables_schema_trino,
        validate_bq_ref,
    )
    from utils.schema_utils import generate_tables_and_columns_from_project_schema

    if body.dialect == "snowflake":
        schema_data, failed = await fetch_tables_schema_snowflake(body.tables_to_import)
        partitions: dict = {}
        source_label = "Snowflake"
    elif body.dialect == "trino":
        schema_data, failed = await fetch_tables_schema_trino(body.tables_to_import)
        partitions = {}
        source_label = "Trino"
    else:
        unqualified = [t for t in body.tables_to_import if not validate_bq_ref(t)]
        if unqualified:
            raise HTTPException(
                status_code=422,
                detail={
                    "needs_manual_config": True,
                    "unqualified_tables": unqualified,
                    "message": (
                        f"Tables sans qualification complète (project.dataset.table) : "
                        f"{', '.join(unqualified)}. Configurez-les manuellement dans les paramètres du projet."
                    ),
                },
            )
        billing_project = BQ_TEST_PROJECT
        schema_data, failed, partitions = await fetch_tables_schema(
            body.tables_to_import, billing_project
        )
        source_label = "BigQuery"

    if failed and not schema_data:
        raise HTTPException(
            status_code=502,
            detail=f"Impossible de récupérer le schéma des tables : {failed}",
        )

    if failed:
        print(f"[import] Échec partiel — tables non importées : {failed}")

    if not schema_data:
        raise HTTPException(
            status_code=400,
            detail=f"Aucune donnée de schéma retournée depuis {source_label}",
        )

    new_schema = generate_tables_and_columns_from_project_schema({"data": schema_data})

    # Enrich each table dict with partition info when available (keyed by short or full ref).
    if partitions:
        for tbl in new_schema:
            full_name = tbl.get("table_name", "")
            short_name = full_name.split(".")[-1] if full_name else ""
            info = partitions.get(full_name) or partitions.get(short_name)
            if info:
                tbl["partition"] = info

    from models.schemas import save_schemas

    save_schemas(new_schema)

    return {
        "imported": len(new_schema),
        "tables": [t["table_name"] for t in new_schema],
    }


class RefreshSchemasRequest(BaseModel):
    tables: list[str] = []  # empty = refresh all stored BQ tables


@router.post("/refresh-schemas")
async def refresh_schemas_route(body: RefreshSchemasRequest):
    """Re-import schemas from BigQuery to pick up partition info on existing tables."""
    from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
    from utils.schema_utils import generate_tables_and_columns_from_project_schema
    from models.schemas import get_schemas, save_schemas

    if body.tables:
        refs = [t for t in body.tables if validate_bq_ref(t)]
    else:
        existing = await get_schemas()
        refs = [
            t["table_name"]
            for t in existing
            if validate_bq_ref(t.get("table_name", ""))
            and len(t["table_name"].split(".")) == 3
        ]

    if not refs:
        return {"refreshed": 0, "tables": []}

    billing_project = BQ_TEST_PROJECT
    schema_data, failed, partitions = await fetch_tables_schema(refs, billing_project)

    if failed:
        print(f"[refresh-schemas] Échec partiel : {failed}")

    if not schema_data:
        raise HTTPException(
            status_code=502,
            detail=f"Aucune donnée retournée depuis BigQuery : {failed}",
        )

    new_schema = generate_tables_and_columns_from_project_schema({"data": schema_data})

    if partitions:
        for tbl in new_schema:
            full_name = tbl.get("table_name", "")
            short_name = full_name.split(".")[-1] if full_name else ""
            info = partitions.get(full_name) or partitions.get(short_name)
            if info:
                tbl["partition"] = info

    save_schemas(new_schema)

    partitioned = [t["table_name"] for t in new_schema if t.get("partition")]
    return {
        "refreshed": len(new_schema),
        "tables": [t["table_name"] for t in new_schema],
        "partitioned": partitioned,
    }


class StreamEventsRequest(BaseModel):
    input: Any
    config: Any = None
    kwargs: Any = None
    diff: bool = False


_query_graph = None


class ResetModelRequest(BaseModel):
    model_name: str


@router.post("/dev/reset-model")
async def dev_reset_model_route(body: ResetModelRequest):
    """Supprime les tests et l'historique d'un modèle — usage démo/dev uniquement."""
    from storage.test_repository import list_tests, delete_test
    from models.message_service import delete_all_messages

    # Le fichier de test est indexé par model_name (test_id est un UUID interne).
    # On le récupère donc directement par model_name, pas par session_id, sinon le
    # lookup échoue silencieusement et le reset ne supprime rien (état accumulé).
    tests = list_tests(body.model_name)
    test = tests[0] if tests else {}
    session_id = test.get("test_id")
    if session_id:
        await delete_all_messages(session_id)
        delete_test(session_id, body.model_name)
    return {"reset": True, "session_id": session_id}


@router.post("/dev/clear-schemas")
async def dev_clear_schemas_route():
    """Réinitialise les schémas (pas le profil) — usage démo/dev uniquement."""
    import json
    from pathlib import Path
    import models.schemas as _schemas_mod
    from models.env_variables import SCHEMA_CACHE_PATH

    p = Path(SCHEMA_CACHE_PATH)
    raw = json.loads(p.read_text()) if p.exists() else {}
    raw["tables"] = []
    p.write_text(json.dumps(raw, indent=2))
    _schemas_mod._cache = None
    _schemas_mod._cache_by_name = None
    _schemas_mod._cache_time = None
    _schemas_mod._profile_cache = None
    return {"cleared": True}


def _get_query_graph():
    global _query_graph
    if _query_graph is None:
        from build_query.query_chain import build_query_graph

        _query_graph = build_query_graph()
    return _query_graph


@router.post("/query/build/stream_events")
async def stream_events_route(body: StreamEventsRequest):
    graph = _get_query_graph()
    graph_input = body.input if isinstance(body.input, dict) else {}

    def _make_serializable(obj):
        if isinstance(obj, (str, int, float, bool, type(None))):
            return obj
        if isinstance(obj, dict):
            return {k: _make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_make_serializable(v) for v in obj]
        if hasattr(obj, "model_dump"):
            try:
                return _make_serializable(obj.model_dump(mode="json"))
            except TypeError:
                return _make_serializable(obj.model_dump())
        if hasattr(obj, "dict"):
            return _make_serializable(obj.dict())
        return str(obj)

    _CAPTURED_STEPS = {"parser", "generator", "executor"}
    _EXCLUDED_EVENT_TYPES = {
        "on_llm_start",
        "on_llm_end",
        "on_chat_model_start",
        "on_chat_model_end",
        "on_tool_start",
        "on_tool_end",
    }

    async def event_generator():
        try:
            async for event in graph.astream_events(
                graph_input,
                version="v2",
                config={"recursion_limit": 80},
                exclude_types=list(_EXCLUDED_EVENT_TYPES),
            ):
                ev_type = event.get("event", "")
                ev_name = event.get("name", "")

                if (
                    ev_type in ("on_chain_start", "on_chain_end")
                    and ev_name not in _CAPTURED_STEPS
                ):
                    continue
                if ev_type == "on_chain_stream" and ev_name != "routing":
                    if not event.get("data", {}).get("chunk", {}).get("messages"):
                        continue

                yield f"data: {json.dumps(_make_serializable(event))}\n\n"
        except asyncio.CancelledError:
            return  # client disconnected — stop streaming silently
        except GraphRecursionError:
            _recursion_payload = json.dumps(
                {
                    "error": "recursion_limit",
                    "message": "Trop d'itérations — le graph a dépassé la limite de 50 étapes.",
                }
            )
            yield f"data: {_recursion_payload}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'error': str(exc)})}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
