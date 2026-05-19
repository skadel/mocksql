import json
import uuid

from langchain_core.messages import AIMessage

from build_query.state import QueryState
from storage.test_repository import get_test, update_test
from utils.msg_types import MsgType


async def update_test_node(state: QueryState):
    """Update test_name and/or unit_test_description of an existing test case."""
    args = state.get("agent_tool_args") or {}
    test_index = args.get(
        "test_index"
    )  # already resolved from test_uid by conversational_agent
    new_name = (args.get("new_name") or "").strip()
    new_description = (args.get("new_description") or "").strip()

    if test_index is None or (not new_name and not new_description):
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    updated_cases = []
    for c in test.get("test_cases") or []:
        if str(c.get("test_index")) == str(test_index):
            c = dict(c)
            if new_name:
                c["test_name"] = new_name
            if new_description:
                c["unit_test_description"] = new_description
        updated_cases.append(c)

    update_test(state["session"], {"test_cases": updated_cases})

    return {
        "messages": [
            AIMessage(
                content=json.dumps(
                    {
                        "test_index": test_index,
                        "new_name": new_name,
                        "new_description": new_description,
                    }
                ),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.UPDATE_TEST,
                    "parent": state["messages"][-1].id if state.get("messages") else state.get("parent_message_id"),
                    "request_id": state.get("request_id"),
                    "test_index": test_index,
                },
            )
        ]
    }
