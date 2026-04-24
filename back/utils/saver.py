import json
import logging
from typing import List, Optional, Any, Dict

from langchain_core.messages import BaseMessage, messages_from_dict

from build_query.state import QueryState
from common_vars import COMMON_HISTORY_TABLE_NAME
from models.database import execute, query
from models.env_variables import DB_MODE
from models.message_service import get_messages_history, get_messages_after_data_id
from storage.test_repository import update_test, merge_test_cases
from utils.msg_types import MsgType
from utils.sql_code import process_sql

logger = logging.getLogger(__name__)


def get_message_type(m: BaseMessage) -> str:
    return m.additional_kwargs.get("type", "")


async def history_saver(state: QueryState) -> Dict[str, str]:
    """
    Sauvegarde l'historique des messages (DuckDB/Postgres) et persiste les
    données du test dans le fichier .mocksql correspondant.
    """
    use_jsonb = DB_MODE in ("postgres", "cloudsql")
    session = state["session"]

    all_msgs = list(merge_examples(state["messages"]))
    last_results_idx = next(
        (
            i
            for i, m in reversed(list(enumerate(all_msgs)))
            if get_message_type(m) == MsgType.RESULTS
        ),
        None,
    )

    for i, msg in enumerate(all_msgs):
        if not msg.content:
            continue
        if get_message_type(msg) == MsgType.EXAMPLES:
            continue
        if get_message_type(msg) == MsgType.RESULTS and i != last_results_idx:
            continue
        logger.debug(
            "Saving message | type=%s id=%s content_preview=%.200s",
            get_message_type(msg),
            msg.additional_kwargs.get("id", "n/a"),
            msg.content,
        )
        msg_dict = msg.dict()
        data_json = json.dumps(msg_dict)
        cast_clause = "::jsonb" if use_jsonb else ""
        sql_stmt = f"""
        INSERT INTO {COMMON_HISTORY_TABLE_NAME} (session_id, data, type)
        VALUES ($1, $2{cast_clause}, $3)
        """
        await execute(sql_stmt, session, data_json, msg.type)

    # Persister sql + used_columns + last_error dans le fichier test
    file_updates: Dict[str, Any] = {}

    raw_sql = state.get("query", "").strip()
    if raw_sql:
        file_updates["sql"] = raw_sql

    used_columns = state.get("used_columns")
    if used_columns:
        file_updates["used_columns"] = used_columns

    file_updates["last_error"] = state.get("error") or ""

    if file_updates:
        update_test(session, file_updates)

    # Persister les résultats (merge par test_index)
    results_msgs = [
        m for m in state["messages"] if get_message_type(m) == MsgType.RESULTS
    ]
    if results_msgs:
        last_results = results_msgs[-1]
        is_rerun_all = last_results.additional_kwargs.get("rerun_all", False)
        new_results = json.loads(last_results.content)
        if not isinstance(new_results, list):
            new_results = [new_results]
        merge_test_cases(session, new_results, rerun_all=is_rerun_all)

    # Persister le profil dans schema_cache (partagé entre tous les modèles)
    profile_msgs = [
        m for m in state["messages"] if get_message_type(m) == MsgType.PROFILE_RESULT
    ]
    if profile_msgs:
        from build_query.profile_checker import (
            _normalize_profile,
            _merge_profiles,
            _save_model_profile,
            _load_model_profile,
        )

        try:
            raw = json.loads(profile_msgs[-1].content)
        except Exception:
            raw = {}
        incoming = _normalize_profile(raw)
        if incoming:
            merged = _merge_profiles(_load_model_profile(), incoming)
            _save_model_profile(merged)

    return {"save": "success"}


def get_history_from_state(
    state: QueryState, msg_type: Optional[List[str]] = None
) -> List[BaseMessage]:
    history = state.get("history", [])
    if msg_type is None:
        return history
    return [m for m in history if get_message_type(m) in msg_type]


async def common_history_retriever(
    session_id: str,
    last_message_id: Optional[str] = None,
    msg_type: Optional[List[str]] = None,
    filtered_types: Optional[List[str]] = None,
    max_results: Optional[int] = None,
) -> List[BaseMessage]:
    if filtered_types is None:
        filtered_types = []
    if msg_type is None:
        msg_type = []

    def _build_rows_query() -> tuple:
        sql = f"SELECT data, type FROM {COMMON_HISTORY_TABLE_NAME} WHERE session_id = $1 ORDER BY id ASC"
        return sql, (session_id,)

    def _rows_to_msgs(rows) -> List[BaseMessage]:
        return messages_from_dict(
            [
                {
                    "data": (
                        r["data"]
                        if isinstance(r["data"], dict)
                        else json.loads(r["data"])
                    ),
                    "type": r["type"],
                }
                for r in rows
            ]
        )

    if last_message_id == "":
        msgs: List[BaseMessage] = []
    elif last_message_id:
        msgs = await get_messages_history(
            session_id=session_id, message_data_id=last_message_id
        )
        if not msgs:
            sql, params = _build_rows_query()
            msgs = _rows_to_msgs(await query(sql, params))
    else:
        sql, params = _build_rows_query()
        msgs = _rows_to_msgs(await query(sql, params))

    msgs = [m for m in msgs if get_message_type(m) not in filtered_types]
    if msg_type:
        msgs = [m for m in msgs if get_message_type(m) in msg_type]
    if max_results:
        msgs = msgs[-max_results:]
    return msgs


def examples_state_retriever(state: QueryState) -> List[BaseMessage]:
    types = [MsgType.EXAMPLES, MsgType.USER_EXAMPLES]
    return [m for m in state.get("examples", []) if get_message_type(m) in types]


async def examples_history_retriever_after_id(
    session_id: str, message_id: Optional[str] = None
) -> List[BaseMessage]:
    if message_id:
        msgs = await get_messages_after_data_id(session_id, message_id)
    else:
        msgs = await common_history_retriever(session_id)
    return [
        m
        for m in msgs
        if get_message_type(m) in [MsgType.EXAMPLES, MsgType.EXAMPLES_INSTRUCTION]
    ]


def _find_validated_queries(
    messages: List[BaseMessage], n: int, dialect: str, sql_decomposed: bool = False
) -> List[Any]:
    msgs = [m.content for m in messages if get_message_type(m) == MsgType.SQL]
    latest = msgs[-n:] if len(msgs) >= n else msgs
    processed: List[Any] = []
    for q in latest:
        obj = json.loads(q) if isinstance(q, str) else q
        if not sql_decomposed:
            obj = process_sql(obj, dialect)
        processed.append(obj)
    return processed


async def find_last_validated_query(
    state: QueryState, sql_decomposed: bool = False
) -> Any:
    res = _find_validated_queries(
        state.get("messages", []), 1, state["dialect"], sql_decomposed
    )
    return res[0] if res else None


async def find_last_validated_query_in_history(
    state: QueryState, sql_decomposed: bool = False
) -> Any:
    msgs = await common_history_retriever(
        state["session"], msg_type=[MsgType.SQL], max_results=1
    )
    res = _find_validated_queries(msgs, 1, state["dialect"], sql_decomposed)
    return res[0] if res else None


async def find_last_validated_queries(
    state: QueryState, n: int, dialect: str, sql_decomposed: bool = False
) -> List[Any]:
    return _find_validated_queries(
        state.get("messages", []), n, dialect, sql_decomposed
    )


async def find_changed_query(state: QueryState, sql_decomposed: bool = False) -> Any:
    msgs = [
        m.content
        for m in state.get("messages", [])
        if get_message_type(m) == MsgType.SQL
    ]
    if not msgs:
        return None
    obj = json.loads(msgs[-1]) if isinstance(msgs[-1], str) else msgs[-1]
    return obj if sql_decomposed else process_sql(obj, state.get("dialect", ""))


async def find_last_asked_question(state: QueryState) -> Optional[str]:
    msgs = [
        m.content
        for m in state.get("messages", [])
        if get_message_type(m) == MsgType.QUERY
    ]
    return msgs[-1] if msgs else None


def merge_examples(messages: List[BaseMessage]) -> List[BaseMessage]:
    return list(messages)


def merge_dicts(d1: Dict[Any, Any], d2: Dict[Any, Any]) -> Dict[Any, Any]:
    result: Dict[Any, Any] = {}
    for k in set(d1) | set(d2):
        if k in d1 and d1[k] and k in d2 and d2[k]:
            result[k] = d1[k] + d2[k]
        elif k in d1 and d1[k]:
            result[k] = d1[k]
        else:
            result[k] = d2[k]
    return result
