"""Regression tests — retry sur réponse LLM vide (MALFORMED_FUNCTION_CALL).

Incident 2026-06-11 : sur gemini-2.5-flash-lite, l'appel d'outil de la boucle
bad_data sort en `finish_reason: MALFORMED_FUNCTION_CALL` → langchain renvoie un
AIMessage vide sans tool_calls. Sans retry, le tour brûle quand même un
`gen_retries` et retombe sur le generator (régénération complète) — exactement
le thrashing que la correction incrémentale doit éviter. Ces tests épinglent le
retry : on redemande une émission propre avant d'abandonner le tour.
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


def _malformed_response():
    """AIMessage tel que renvoyé par langchain quand Gemini sort en
    MALFORMED_FUNCTION_CALL : contenu vide, aucun tool_call."""
    return AIMessage(
        content="",
        response_metadata={"finish_reason": "MALFORMED_FUNCTION_CALL"},
    )


def _patch_call(test_uid="a3f9", value="2025-04-01"):
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
async def test_malformed_then_valid_call_is_retried(monkeypatch):
    """Réponse vide (MALFORMED_FUNCTION_CALL) → retry → l'appel corrigé aboutit."""
    fake = FakeLLM([_malformed_response(), _patch_call()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert len(fake.calls) == 2
    # Le retry doit signaler au LLM que sa réponse était vide/malformée.
    feedback = fake.calls[1][-1]
    assert isinstance(feedback, HumanMessage)
    assert "vide" in feedback.content or "malformé" in feedback.content

    assert update["agent_tool_call"] == "data_batch"
    calls = update["agent_tool_args"]["calls"]
    assert calls[0]["tool"] == "patch_test_field"


@pytest.mark.asyncio
async def test_malformed_exhausts_retries_then_gives_up(monkeypatch):
    """Réponses vides répétées → on abandonne après le budget de retries
    (route_agent_output retombera sur le generator), sans boucle infinie."""
    fake = FakeLLM([_malformed_response() for _ in range(5)])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert update["agent_tool_call"] is None
    # 1 appel initial + budget de retries borné (pas d'épuisement de la file).
    assert 2 <= len(fake.calls) <= 4


@pytest.mark.asyncio
async def test_plain_text_response_is_not_retried(monkeypatch):
    """Une réponse texte légitime (sans outil) ne déclenche PAS le retry."""
    fake = FakeLLM([AIMessage(content="Voici l'explication demandée.")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert len(fake.calls) == 1
    assert update["agent_tool_call"] is None
