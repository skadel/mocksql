import json
import uuid

from langchain_core.messages import AIMessage

from build_query.state import QueryState
from storage.test_repository import get_test, update_test
from utils.msg_types import MsgType


async def delete_test_node(state: QueryState):
    """Remove a test case from storage and emit a delete_test SSE event."""
    args = state.get("agent_tool_args") or {}
    test_index = args.get("test_index")
    if test_index is None:
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    updated_cases = [
        c
        for c in (test.get("test_cases") or [])
        if str(c.get("test_index")) != str(test_index)
    ]
    update_test(state["session"], {"test_cases": updated_cases})

    return {
        "messages": [
            AIMessage(
                content=json.dumps({"test_index": test_index}),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.DELETE_TEST,
                    "parent": state.get("parent_message_id"),
                    "request_id": state.get("request_id"),
                    "test_index": test_index,
                },
            )
        ]
    }
