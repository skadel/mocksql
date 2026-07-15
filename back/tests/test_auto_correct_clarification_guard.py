"""Régression root-cause spider2-snow — pas de `ask_clarification` terminal dans la
boucle de correction auto (bad_data) SANS prémisse utilisateur.

Incident (repro sf_local022) : diagnostic aveugle (INSERT avalé) → l'agent conclut à
raison que les données ne sont pas le problème et appelle `ask_clarification`. Outil
terminal → history_saver : le run se termine avec 5 retries restants, et en CLI
personne ne peut répondre. Le garde-fou anti-no-op était contourné parce que
`ask_clarification` est traité avant lui.

Le fix étend l'interception « no recipient » de la boucle batch à la boucle
`auto_correct` : feedback + retry capé, puis break sans outil → `route_agent_output`
retombe sur le generator (via `evaluation_feedback == "bad_data"`).

EXCEPTION préservée (TICKET-1, test_premise_protection) : une clarification qui
protège une `user_premise` reste TERMINALE — l'utilisateur doit trancher, jamais de
régénération silencieuse de la valeur énoncée.
"""

import json

import pytest
from langchain_core.messages import AIMessage

from build_query.conversational_agent import conversational_agent
from build_query.query_chain import route_agent_output
from utils.msg_types import MsgType


class FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _state_auto_correct(user_premise=None):
    test = {
        "test_uid": "a3f9",
        "test_index": "1",
        "test_name": "Manches",
        "unit_test_description": "runs par manche pour un match",
        "data": {"ball_by_ball": [{"match_id": "M001", "striker": "P001"}]},
        "status": "empty_results",
        "failing_cte": None,
        "results_json": "[]",
        "assertion_results": [],
    }
    if user_premise is not None:
        test["user_premise"] = user_premise
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS},
    )
    eval_msg = AIMessage(
        content="**Insuffisant** — résultat vide",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": "1",
            "parent": "r1",
        },
    )
    return {
        "session": "sess-auto",
        "messages": [results_msg, eval_msg],
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "",
        "evaluation_feedback": "bad_data",
        "auto_correct": True,
        "gen_retries": 5,
    }


def _clarification_call():
    return {
        "name": "ask_clarification",
        "args": {"question": "Le SQL est-il correct pour ce scénario ?"},
        "id": "call_clarif",
    }


def _update_call():
    return {
        "name": "update_test_data",
        "args": {"test_uid": "a3f9", "instruction": "ids numériques cohérents"},
        "id": "call_upd",
    }


@pytest.mark.asyncio
async def test_clarification_in_auto_loop_gets_feedback_then_agent_acts(monkeypatch):
    """Clarification seule en boucle auto sans prémisse → feedback « no recipient » +
    retry ; le 2ᵉ tour agit."""
    fake = FakeLLM(
        [
            AIMessage(content="", tool_calls=[_clarification_call()]),
            AIMessage(content="", tool_calls=[_update_call()]),
        ]
    )
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct())

    assert update["agent_tool_call"] == "update_test_data"
    # Aucune question orpheline émise.
    assert all(
        not m.additional_kwargs.get("pending_intent")
        for m in update.get("messages", [])
    )


@pytest.mark.asyncio
async def test_clarification_exhausted_falls_back_to_generator(monkeypatch):
    """L'agent insiste malgré les retries → tour sans outil, et le routage retombe sur
    le generator (jamais history_saver avec des retries restants)."""
    responses = [AIMessage(content="", tool_calls=[_clarification_call()])] * 3
    fake = FakeLLM(responses)
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _state_auto_correct()
    update = await conversational_agent(state)

    assert update["agent_tool_call"] is None
    assert route_agent_output({**state, **update}) == "generator"


@pytest.mark.asyncio
async def test_co_emitted_action_wins_in_auto_loop(monkeypatch):
    """Clarification + action dans la même réponse en boucle auto : l'action gagne."""
    fake = FakeLLM(
        [AIMessage(content="", tool_calls=[_clarification_call(), _update_call()])]
    )
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct())

    assert update["agent_tool_call"] == "update_test_data"
    assert len(fake.calls) == 1  # pas de retry : l'action co-émise suffit


@pytest.mark.asyncio
async def test_premise_protected_clarification_stays_terminal(monkeypatch):
    """TICKET-1 intact : avec `user_premise`, la clarification traverse (tour terminal,
    l'utilisateur tranche) et le routage reste history_saver."""
    fake = FakeLLM([AIMessage(content="", tool_calls=[_clarification_call()])])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _state_auto_correct(user_premise="pour le match M001 j'attends 2 manches")
    update = await conversational_agent(state)

    assert update["agent_tool_call"] == "ask_clarification"
    assert len(fake.calls) == 1  # pas de retry : la question est légitime
    assert route_agent_output({**state, **update}) == "history_saver"
