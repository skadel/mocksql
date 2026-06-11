"""Regression tests: patch operations targeting a non-existent table/row/field.

Bug: when the LLM emits a patch_test_field call with a field that does not exist in
the targeted row, data_patcher silently CREATED the phantom field (corrupting the test
data); a non-existent table or out-of-range row_index was silently skipped — the turn
became a no-op without the agent ever learning its request was invalid.

Expected behavior (mirrors the invalid-uid retry): validate the patch targets against
the real test data BEFORE routing to data_patcher, feed the error back to the LLM with
the available tables/fields so it re-emits a corrected request, bounded by the same
retry budget.
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.conversational_agent import conversational_agent
from build_query.data_patcher import apply_single_patch
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


def _patch_call(
    field="partition_date", table="MARKETING_Referentiels_banques", row_index=0
):
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": "a3f9",
                    "table": table,
                    "row_index": row_index,
                    "field": field,
                    "value_json": json.dumps("2025-04-01"),
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
                    "MARKETING_Referentiels_banques": [
                        {"partition_date": "2025-01-01", "code_banque": "BP"}
                    ]
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
        "input": "Update the partition_date to 2025-04-01",
        "gen_retries": 2,
    }


@pytest.mark.asyncio
async def test_unknown_field_feeds_back_then_retries(monkeypatch):
    """Field typo → feedback lists the available fields → corrected call goes through."""
    fake = FakeLLM([_patch_call(field="partition_dat"), _patch_call()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert len(fake.calls) == 2
    feedback = fake.calls[1][-1]
    assert isinstance(feedback, HumanMessage)
    # The error names the bad field and surfaces the valid ones.
    assert "partition_dat" in feedback.content
    assert "partition_date" in feedback.content

    assert update["agent_tool_call"] == "data_batch"
    calls = update["agent_tool_args"]["calls"]
    assert len(calls) == 1
    assert calls[0]["args"]["field"] == "partition_date"


@pytest.mark.asyncio
async def test_unknown_table_feeds_back_available_tables(monkeypatch):
    fake = FakeLLM([_patch_call(table="referentiel_banques"), _patch_call()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert len(fake.calls) == 2
    feedback = fake.calls[1][-1]
    assert isinstance(feedback, HumanMessage)
    assert "referentiel_banques" in feedback.content
    assert "MARKETING_Referentiels_banques" in feedback.content
    assert update["agent_tool_call"] == "data_batch"


@pytest.mark.asyncio
async def test_row_index_out_of_range_feeds_back(monkeypatch):
    fake = FakeLLM([_patch_call(row_index=5), _patch_call()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert len(fake.calls) == 2
    assert isinstance(fake.calls[1][-1], HumanMessage)
    assert update["agent_tool_call"] == "data_batch"
    assert update["agent_tool_args"]["calls"][0]["args"]["row_index"] == 0


@pytest.mark.asyncio
async def test_patch_on_row_added_in_same_batch_is_valid(monkeypatch):
    """add_test_row then patch on the new index in the same batch must NOT be rejected:
    the validator simulates the batch in order (one row added per table)."""
    batch = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "add_test_row",
                "args": {
                    "test_uid": "a3f9",
                    "tables": ["MARKETING_Referentiels_banques"],
                    "instruction": "ligne réseau CE",
                },
                "id": "call_a",
            },
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": "a3f9",
                    "table": "MARKETING_Referentiels_banques",
                    "row_index": 1,  # valide uniquement grâce à l'add précédent
                    "field": "code_banque",
                    "value_json": json.dumps("CE"),
                },
                "id": "call_b",
            },
        ],
    )
    fake = FakeLLM([batch])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    # No retry needed: a single LLM invocation, both ops kept in order.
    assert len(fake.calls) == 1
    assert update["agent_tool_call"] == "data_batch"
    assert [op["tool"] for op in update["agent_tool_args"]["calls"]] == [
        "add_test_row",
        "patch_test_field",
    ]


@pytest.mark.asyncio
async def test_persistently_invalid_field_is_bounded(monkeypatch):
    """If the LLM never fixes its target, retries stay bounded and the invalid op
    is dropped instead of corrupting the data."""
    fake = FakeLLM(
        [
            _patch_call(field="bad1"),
            _patch_call(field="bad2"),
            _patch_call(field="bad3"),
        ]
    )
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    # _UID_RETRY_MAX == 2 → at most 3 invocations (initial + 2 retries), then give up.
    assert len(fake.calls) <= 3
    assert update["agent_tool_call"] is None


@pytest.mark.asyncio
async def test_apply_single_patch_does_not_create_phantom_field():
    """Defense in depth: even if an invalid patch reaches the data_patcher,
    it must skip the op — never create a field absent from the row."""
    data = {"t": [{"partition_date": "2025-01-01"}]}
    out = await apply_single_patch(
        state={},
        test_case={},
        data=data,
        tool_name="patch_test_field",
        args={
            "test_index": "1",
            "table": "t",
            "row_index": 0,
            "field": "partition_dat",  # typo — n'existe pas dans la ligne
            "value_json": '"2025-04-01"',
        },
    )
    assert out["t"][0] == {"partition_date": "2025-01-01"}
    assert "partition_dat" not in out["t"][0]
