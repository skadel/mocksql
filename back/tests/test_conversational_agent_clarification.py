"""Régression — `ask_clarification` est exclusif et terminal.

Incident : l'agent renvoie dans une SEULE réponse `ask_clarification` ET un outil
d'action (`generate_test_data`, `add_test_row`…). La boucle de sélection appliquait
une priorité debug > data_batch > première action sans réserver de place à la
clarification : selon l'ordre des tool_calls, l'action gagnait → un test était
généré alors que l'agent venait de poser une question. L'agent « n'attendait pas »
la réponse de l'utilisateur.

Design attendu : poser une question est TERMINAL — si `ask_clarification` est
présent, il gagne, aucun autre outil n'est exécuté, le tour route vers
history_saver et attend la réponse.
"""

import json

import pytest
from langchain_core.messages import AIMessage

from build_query.conversational_agent import conversational_agent
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


def _state_with_one_test():
    results_content = json.dumps(
        [
            {
                "test_uid": "a3f9",
                "test_index": "1",
                "test_name": "MEG",
                "unit_test_description": "classification MEG",
                "data": {"photo_m": [{"nb_contrats_new": 1}]},
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
        "query": "SELECT * FROM photo_m WHERE nb_contrats_new = 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "teste le cas MEG même s'il a plus d'un contrat actif",
        "gen_retries": 2,
    }


def _clarification_call():
    return {
        "name": "ask_clarification",
        "args": {
            "question": "Le SQL exige nb_contrats_new = 1 ; veux-tu le comportement actuel ?"
        },
        "id": "call_clarif",
    }


@pytest.mark.asyncio
async def test_clarification_wins_over_generate_when_listed_after(monkeypatch):
    """`ask_clarification` listé APRÈS une action : il doit quand même gagner."""
    response = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "generate_test_data",
                "args": {"scenario": "client avec plusieurs contrats actifs"},
                "id": "call_gen",
            },
            _clarification_call(),
        ],
    )
    fake = FakeLLM([response])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert update["agent_tool_call"] == "ask_clarification"
    # Aucun test ne doit être généré : pas de message scénario, l'input n'est pas
    # remplacé par le scénario de génération.
    msgs = update.get("messages", [])
    assert all(
        m.additional_kwargs.get("type") != MsgType.GENERATE_TEST_SCENARIO for m in msgs
    )
    # La question est bien émise avec son breadcrumb de reprise.
    clarif_msgs = [m for m in msgs if m.additional_kwargs.get("pending_intent")]
    assert len(clarif_msgs) == 1


@pytest.mark.asyncio
async def test_clarification_wins_over_data_patch(monkeypatch):
    """`ask_clarification` combiné à un `add_test_row` : il gagne, pas de data_batch."""
    response = AIMessage(
        content="",
        tool_calls=[
            {
                "name": "add_test_row",
                "args": {
                    "test_uid": "a3f9",
                    "tables": ["photo_m"],
                    "instruction": "2e contrat",
                },
                "id": "call_add",
            },
            _clarification_call(),
        ],
    )
    fake = FakeLLM([response])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert update["agent_tool_call"] == "ask_clarification"


@pytest.mark.asyncio
async def test_clarification_alone_still_works(monkeypatch):
    """Cas nominal : `ask_clarification` seul reste terminal et émet la question."""
    response = AIMessage(content="", tool_calls=[_clarification_call()])
    fake = FakeLLM([response])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert update["agent_tool_call"] == "ask_clarification"
    clarif_msgs = [
        m
        for m in update.get("messages", [])
        if m.additional_kwargs.get("pending_intent")
    ]
    assert len(clarif_msgs) == 1


@pytest.mark.asyncio
async def test_prompt_instructs_clarify_on_add_vs_modify(monkeypatch):
    """Le prompt système doit dire à l'agent de demander clarification quand la
    demande n'indique pas clairement s'il faut CRÉER un nouveau test ou MODIFIER
    un test existant — il ne doit jamais choisir à l'aveugle entre les deux."""
    # Réponse texte libre : on ne s'intéresse qu'au prompt envoyé au LLM.
    response = AIMessage(content="ok")
    fake = FakeLLM([response])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_state_with_one_test())

    # Le SystemMessage est le premier message du premier (et unique) appel LLM.
    system_prompt = fake.calls[0][0].content
    assert "CRÉER UN NOUVEAU TEST" in system_prompt
    assert "MODIFIER UN TEST EXISTANT" in system_prompt
    # La consigne doit pointer vers ask_clarification, pas vers une action.
    add_vs_modify_idx = system_prompt.index("CRÉER UN NOUVEAU TEST")
    assert (
        "ask_clarification"
        in system_prompt[add_vs_modify_idx - 400 : add_vs_modify_idx + 400]
    )
