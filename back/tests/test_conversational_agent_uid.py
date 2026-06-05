"""Regression tests for conversational_agent uid handling in the data-patch batch path.

Bug: when the LLM emits a patch_test_field/remove_test_row/add_test_row call with a
test_uid that is not in the current test list, the batch collector silently dropped it,
leaving agent_tool_call=None → route_agent_output falls through to history_saver and the
user's request becomes a no-op. The single-action path already retries with the valid
ids; these tests pin the same behavior for the batch path.
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.conversational_agent import conversational_agent
from utils.msg_types import MsgType


class FakeLLM:
    """Returns queued responses in order; records the messages it was invoked with."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _patch_call(test_uid, value="2025-04-01"):
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": test_uid,
                    "table": "MARKETING_Referentiels_banques",
                    "row_index": 0,
                    "field": "partition_date",
                    "value_json": json.dumps(value),
                },
                "id": "call_1",
            }
        ],
    )


def _state_with_one_test():
    results_content = json.dumps(
        [
            {
                "test_uid": "a3f9",
                "test_index": "1",
                "test_name": "Référentiel banques",
                "unit_test_description": "partition_date à jour",
                "data": {
                    "MARKETING_Referentiels_banques": [{"partition_date": "2025-01-01"}]
                },
                "results_json": "[]",
                "assertion_results": [],
            }
        ]
    )
    results_msg = AIMessage(
        content=results_content, additional_kwargs={"type": MsgType.RESULTS}
    )
    return {
        "session": "sess1",
        "messages": [results_msg],
        "dialect": "duckdb",
        "query": "SELECT * FROM MARKETING_Referentiels_banques",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "Update the partition_date in MARKETING_Referentiels_banques to 2025-04-01",
        "gen_retries": 2,
    }


@pytest.mark.asyncio
async def test_invalid_uid_then_valid_retries_into_data_batch(monkeypatch):
    """Unknown uid in a patch call → retry with valid ids → resolves to data_batch."""
    fake = FakeLLM([_patch_call("a3f9c2"), _patch_call("a3f9")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    # Two LLM invocations: the first with the bad uid, the second after feedback.
    assert len(fake.calls) == 2
    # The retry must surface the valid id back to the LLM.
    feedback = fake.calls[1][-1]
    assert isinstance(feedback, HumanMessage)
    assert "a3f9c2" in feedback.content and "a3f9" in feedback.content

    # The corrected call routes to the data patcher, not history_saver.
    assert update["agent_tool_call"] == "data_batch"
    calls = update["agent_tool_args"]["calls"]
    assert len(calls) == 1
    assert calls[0]["tool"] == "patch_test_field"
    assert calls[0]["args"]["test_index"] == "1"


@pytest.mark.asyncio
async def test_persistently_invalid_uid_does_not_loop(monkeypatch):
    """If the LLM never supplies a valid uid, retries are bounded and we don't hang."""
    fake = FakeLLM([_patch_call("bad1"), _patch_call("bad2"), _patch_call("bad3")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    # _UID_RETRY_MAX == 2 → at most 3 invocations (initial + 2 retries), then give up.
    assert len(fake.calls) <= 3
    assert update["agent_tool_call"] is None
