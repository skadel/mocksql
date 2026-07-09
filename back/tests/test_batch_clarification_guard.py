"""Régression — pas de `ask_clarification` dans la boucle multi-tests.

Incident : l'utilisateur demande 2 tests. Le nominal passe, puis la boucle batch
produit une suggestion du type « un client PRO→PART n'apparaît pas dans le résultat
final ». L'agent la lit comme une attente utilisateur contredite par le SQL et
appelle `ask_clarification` — outil terminal → history_saver. Résultat : 1 test au
lieu de 2 + une question orpheline dans le chat (personne n'est censé y répondre :
la « suggestion » vient de MockSQL lui-même, pas d'un humain).

Depuis le rebranch, les tours de suggestion de la boucle vont DIRECTEMENT au
generator (cf. test_multi_test_loop) : l'agent n'est plus sur ce chemin. Il reste
en revanche dans la boucle de CORRECTION bad_data d'un test du lot
(bad_data_to_agent → conversational_agent), où `suggestion_intent` +
`auto_tests_built` traînent encore dans le state — ces gardes protègent ce
sous-chemin (posés uniquement par le backend : un clic utilisateur sur le panneau
ne pose jamais `auto_tests_built`) :
- le prompt interdit `ask_clarification` et ordonne de tester le comportement
  observé (si le SQL exclut le cas, le test AFFIRME l'exclusion) ;
- si l'agent l'appelle quand même, l'appel est ignoré : action co-émise conservée,
  sinon retry capé, sinon break sans outil → fallback generator ;
- backstop de routage : `ask_clarification` en batch route vers generator, jamais
  history_saver.

Le comportement TERMINAL de `ask_clarification` sur un tour utilisateur réel
(chat, clic suggestion du panneau) est préservé — cf.
test_conversational_agent_clarification.py.
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


def _batch_state():
    """State tel que produit par generate_single_suggestion : suggestion machine
    injectée en input + suggestion_intent + compteur de boucle."""
    results_content = json.dumps(
        [
            {
                "test_uid": "a3f9",
                "test_index": "1",
                "test_name": "nominal",
                "unit_test_description": "cas nominal des mouvements",
                "data": {"photo_m": [{"segment_new": "PRO", "segment_old": "PRO"}]},
                "results_json": "[]",
                "assertion_results": [],
            }
        ]
    )
    results_msg = AIMessage(
        content=results_content, additional_kwargs={"type": MsgType.RESULTS}
    )
    return {
        "session": "sess-batch",
        "messages": [results_msg],
        "dialect": "duckdb",
        "query": "SELECT * FROM photo_m WHERE segment_new = segment_old",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": (
            "Un client qui change de regroupement, passant d'un segment 'PRO' à un "
            "segment 'PART', n'apparaît pas dans le résultat final."
        ),
        "suggestion_intent": True,
        "auto_tests_built": 1,
        "tests_target": 2,
        "gen_retries": 2,
    }


def _clarification_call():
    return {
        "name": "ask_clarification",
        "args": {
            "question": "Souhaitez-vous que ce changement de segment apparaisse ?"
        },
        "id": "call_clarif",
    }


def _generate_call():
    return {
        "name": "generate_test_data",
        "args": {"scenario": "client PRO→PART exclu du résultat final"},
        "id": "call_gen",
    }


# --- Routage : backstop generator, jamais history_saver en batch ---------------


def test_route_ask_clarification_in_batch_falls_back_to_generator():
    state = {
        "agent_tool_call": "ask_clarification",
        "suggestion_intent": True,
        "auto_tests_built": 1,
    }
    assert route_agent_output(state) == "generator"


def test_route_ask_clarification_on_user_click_stays_terminal():
    """Clic utilisateur sur une suggestion du panneau (pas de compteur de boucle) :
    la question est légitime, le tour reste terminal."""
    state = {"agent_tool_call": "ask_clarification", "suggestion_intent": True}
    assert route_agent_output(state) == "history_saver"


def test_route_ask_clarification_plain_chat_stays_terminal():
    state = {"agent_tool_call": "ask_clarification"}
    assert route_agent_output(state) == "history_saver"


# --- Agent : la clarification est ignorée en boucle batch -----------------------


@pytest.mark.asyncio
async def test_agent_retries_then_generates_after_clarification_in_batch(monkeypatch):
    """Clarification seule en batch → feedback + retry ; le 2ᵉ tour agit."""
    responses = [
        AIMessage(content="", tool_calls=[_clarification_call()]),
        AIMessage(content="", tool_calls=[_generate_call()]),
    ]
    fake = FakeLLM(responses)
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_batch_state())

    assert update["agent_tool_call"] == "generate_test_data"
    # Aucune question orpheline dans le chat.
    assert all(
        not m.additional_kwargs.get("pending_intent")
        for m in update.get("messages", [])
    )


@pytest.mark.asyncio
async def test_agent_keeps_co_emitted_action_in_batch(monkeypatch):
    """Clarification + action dans la MÊME réponse en batch : l'action gagne
    (inverse du comportement terminal sur tour utilisateur)."""
    response = AIMessage(
        content="", tool_calls=[_clarification_call(), _generate_call()]
    )
    fake = FakeLLM([response])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_batch_state())

    assert update["agent_tool_call"] == "generate_test_data"
    assert len(fake.calls) == 1  # pas de retry : l'action co-émise suffit


@pytest.mark.asyncio
async def test_agent_clarification_exhausted_ends_without_question(monkeypatch):
    """L'agent insiste sur la clarification malgré les retries → tour sans outil
    (route_agent_output retombe alors sur generator via suggestion_intent), et
    surtout AUCUNE question n'est émise dans le chat."""
    responses = [AIMessage(content="", tool_calls=[_clarification_call()])] * 3
    fake = FakeLLM(responses)
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _batch_state()
    update = await conversational_agent(state)

    assert update["agent_tool_call"] is None
    assert all(
        not m.additional_kwargs.get("pending_intent")
        for m in update.get("messages", [])
    )
    # Le fallback de routage garantit qu'un test sort quand même.
    assert route_agent_output({**state, **update}) == "generator"


# --- Prompt : interdiction en batch, échappatoire conservée au clic user --------


@pytest.mark.asyncio
async def test_batch_prompt_forbids_clarification(monkeypatch):
    fake = FakeLLM([AIMessage(content="", tool_calls=[_generate_call()])])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_batch_state())

    system_prompt = fake.calls[0][0].content
    assert "n'appelle JAMAIS `ask_clarification`" in system_prompt
    # La consigne « comportement exclu = cas à tester » doit être présente.
    assert "AFFIRME" in system_prompt


@pytest.mark.asyncio
async def test_user_click_prompt_keeps_clarification_escape(monkeypatch):
    """Clic utilisateur (suggestion_intent sans auto_tests_built) : l'échappatoire
    ask_clarification reste offerte dans la note de suggestion."""
    fake = FakeLLM([AIMessage(content="", tool_calls=[_generate_call()])])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _batch_state()
    del state["auto_tests_built"]
    await conversational_agent(state)

    system_prompt = fake.calls[0][0].content
    assert "Seulement si la suggestion suppose un comportement" in system_prompt
