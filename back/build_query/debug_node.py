import json
import uuid
from typing import Any, Dict

from langchain_core.messages import AIMessage

from build_query.debug_executor import execute_run_cte, execute_count_cte_steps
from build_query.examples_executor import filter_schemas_by_used_columns
from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.msg_types import MsgType


async def debug_test_node(state: QueryState) -> Dict[str, Any]:
    """Execute one or more debug tools (run_cte / count_cte_steps) and emit all results."""
    tool_call = state.get("agent_tool_call")
    args = state.get("agent_tool_args") or {}

    # Build a normalised list of calls regardless of single vs batch invocation
    if tool_call == "debug_batch":
        calls = args.get("calls", [])
    elif tool_call in ("run_cte", "count_cte_steps"):
        calls = [{"tool": tool_call, "args": args}]
    else:
        return {}

    if not calls:
        return {}

    from models.schemas import get_schemas

    schemas_raw = await get_schemas(project_id=state["project"])
    used_columns = [
        json.loads(c) if isinstance(c, str) else c
        for c in (state.get("used_columns") or [])
    ]
    schemas = filter_schemas_by_used_columns(schemas_raw, used_columns)
    test_cases = await retrieve_existing_tests(state["session"], state)

    debug_messages: list[AIMessage] = []

    for call in calls:
        call_tool = call.get("tool", "")
        call_args = call.get("args", {})
        test_index = str(call_args.get("test_index", ""))
        cte_name = str(call_args.get("cte_name", ""))

        if not test_index or not cte_name:
            continue

        common = dict(
            session_id=state["session"],
            test_index=test_index,
            cte_name=cte_name,
            query_decomposed=state.get("query_decomposed") or "[]",
            project=state["project"],
            dialect=state["dialect"],
            schemas=schemas,
            used_columns=used_columns,
            test_cases=test_cases or None,
        )

        if call_tool == "run_cte":
            result = await execute_run_cte(
                **common,
                column=call_args.get("column") or None,
            )
            msg_type = MsgType.DEBUG_RUN_CTE
        elif call_tool == "count_cte_steps":
            result = await execute_count_cte_steps(**common)
            msg_type = MsgType.DEBUG_COUNT_STEPS
        else:
            continue

        debug_messages.append(
            AIMessage(
                content=json.dumps(result, ensure_ascii=False, default=str),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": msg_type,
                    "parent": state["messages"][-1].id if state.get("messages") else state.get("parent_message_id"),
                    "request_id": state.get("request_id"),
                    "test_index": test_index,
                },
            )
        )

    if not debug_messages:
        return {}

    debug_retries = max(0, (state.get("debug_retries") or 0) - 1)
    return {
        "messages": debug_messages,
        "history": debug_messages,
        "debug_retries": debug_retries,
    }
