import asyncio
import json
from typing import Optional, List, Any

import dotenv
import sqlglot
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from common_vars import get_tables_mapping
from models.env_variables import (
    AUTO_SCHEMA_IMPORT,
    BQ_SCHEMA_BILLING_PROJECT,
    AUTO_PROFILING,
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
            node.set("catalog", sqlglot.exp.Identifier(this=billing_project))
        return node

    return (
        sqlglot.parse_one(sql, dialect=dialect)
        .transform(_qualify_node)
        .sql(dialect=dialect, pretty=True)
    )


@router.post("/validate-query")
async def validate_query_route(body: ValidateQueryRequest):
    from build_query.validator import validate_query as _validate_query

    billing_project = BQ_SCHEMA_BILLING_PROJECT

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


@router.post("/check-profile")
async def check_profile_route(body: CheckProfileRequest):
    from build_query.profile_checker import check_profile, build_profile_request

    state = {
        "project": body.project,
        "user": "local",
        "dialect": body.dialect,
        "session": body.session,
        "query": body.sql,
        "used_columns": body.used_columns,
        "schemas": [],
        "messages": [],
        "parent_message_id": "",
        "request_id": "",
    }

    checked = await check_profile(state)

    if checked["profile_complete"]:
        return {"profile_complete": True}

    request = await build_profile_request(state, checked["missing_columns"])

    profile_request: dict = {
        "profile_query": request.get("profile_sql", ""),
        "missing_columns": request.get("missing_columns", []),
        "expected_joins": request.get("expected_joins", []),
    }
    billing_tb = request.get("profile_billing_tb")
    if billing_tb is not None:
        profile_request["billing_tb"] = billing_tb

    return {
        "profile_complete": False,
        "auto_profile_available": AUTO_PROFILING,
        "profile_request": profile_request,
    }


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
    project: str
    user: str = "local"
    session: str


@router.post("/auto-profile")
async def auto_profile_route(body: AutoProfileRequest):

    from google.cloud import bigquery as _bq
    from build_query.profile_checker import (
        _normalize_profile,
        _load_model_profile,
        _merge_profiles,
        _save_model_profile,
    )

    billing_project = BQ_SCHEMA_BILLING_PROJECT

    try:
        client = _bq.Client(project=billing_project)
        rows = await asyncio.to_thread(
            lambda: list(client.query(body.profile_sql).result())
        )
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"BigQuery profiling error: {exc}")

    raw_profile = [dict(row) for row in rows]
    incoming_profile = _normalize_profile(raw_profile)
    if not incoming_profile:
        raise HTTPException(
            status_code=400, detail="Le profiling n'a retourné aucun résultat"
        )

    stored_profile = _normalize_profile(_load_model_profile())
    merged = _merge_profiles(stored_profile, incoming_profile)
    _save_model_profile(merged)

    return {"saved": True}


class ImportMissingTablesRequest(BaseModel):
    tables_to_import: List[str]
    project: str


@router.post("/import-missing-tables")
async def import_missing_tables_route(body: ImportMissingTablesRequest):
    from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
    from utils.schema_utils import generate_tables_and_columns_from_project_schema

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

    billing_project = BQ_SCHEMA_BILLING_PROJECT
    schema_data, failed = await fetch_tables_schema(
        body.tables_to_import, billing_project
    )

    if failed and not schema_data:
        raise HTTPException(
            status_code=502,
            detail=f"Impossible de récupérer le schéma des tables : {failed}",
        )

    if failed:
        print(f"[import] Échec partiel — tables non importées : {failed}")

    if not schema_data:
        raise HTTPException(
            status_code=400, detail="Aucune donnée de schéma retournée depuis BigQuery"
        )

    new_schema = generate_tables_and_columns_from_project_schema({"data": schema_data})

    from models.schemas import save_schemas

    save_schemas(new_schema)

    return {
        "imported": len(new_schema),
        "tables": [t["table_name"] for t in new_schema],
    }


class StreamEventsRequest(BaseModel):
    input: Any
    config: Any = None
    kwargs: Any = None
    diff: bool = False


_query_graph = None


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

    async def event_generator():
        try:
            async for event in graph.astream_events(graph_input, version="v2"):
                yield f"data: {json.dumps(_make_serializable(event))}\n\n"
        except asyncio.CancelledError:
            return  # client disconnected — stop streaming silently
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
