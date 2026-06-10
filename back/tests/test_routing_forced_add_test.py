"""Regression tests for the "click a coverage suggestion" flow.

Bug initial : cliquer sur une suggestion soumettait un message texte. Comme des tests
existaient déjà (has_existing_tests), `routing` l'envoyait au conversational_agent, qui
pouvait alors "décider" de répondre en texte libre ("c'est déjà vérifié, j'ai regardé le
code") au lieu de générer le test demandé.

Design retenu (hybride) :
  - le clic passe TOUJOURS par l'agent (flag `suggestion_intent`) pour qu'il puisse
    détecter qu'une suggestion recoupe un test existant et l'étendre plutôt qu'en créer
    un doublon ;
  - filet de sécurité : si l'agent ne produit aucune action (texte libre seul),
    `route_agent_output` retombe sur le `generator` → un test sort toujours.

Ces tests épinglent les deux garanties : l'agent est atteint, et le no-op est rattrapé.
"""

import json

import pytest
from langchain_core.messages import AIMessage

from build_query.routing import routing
from build_query.query_chain import route_agent_output
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


# --- routing : le clic sur suggestion atteint l'agent -------------------------


@pytest.mark.asyncio
async def test_suggestion_click_routes_to_conversational_agent():
    """Suggestion (input + tests existants) → conversational_agent (dédup possible)."""
    state = {
        "input": "Vérifie que le total annuel correspond à la somme des mois",
        "has_existing_tests": True,
        "rerun_all_tests": False,
        "suggestion_intent": True,
        "user_message_id": "umsg-1",
        "parent_message_id": "pmsg-1",
        "request_id": "req-1",
    }
    result = await routing(state)
    assert result["route"] == "conversational_agent"


# --- route_agent_output : filet de sécurité anti no-op ------------------------


def test_suggestion_intent_no_tool_falls_back_to_generator():
    """suggestion_intent + aucun outil actionnable → generator (pas history_saver)."""
    state = {"agent_tool_call": None, "suggestion_intent": True}
    assert route_agent_output(state) == "generator"


def test_plain_no_tool_still_goes_to_history_saver():
    """Sans suggestion_intent ni bad_data, un no-op reste un history_saver (inchangé)."""
    state = {"agent_tool_call": None}
    assert route_agent_output(state) == "history_saver"


def test_suggestion_intent_add_test_row_goes_to_data_patcher():
    """Si l'agent étend un test existant (add_test_row), on route vers le data_patcher."""
    state = {"agent_tool_call": "add_test_row", "suggestion_intent": True}
    assert route_agent_output(state) == "data_patcher"


# --- conversational_agent : le mandat est injecté -----------------------------


def _state_suggestion_click():
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
        "input": "Vérifie que le total annuel correspond à la somme des mois",
        "suggestion_intent": True,
        "gen_retries": 2,
    }


def _generate_test_call(scenario):
    return AIMessage(
        content="",
        tool_calls=[
            {"name": "generate_test_data", "args": {"scenario": scenario}, "id": "c1"}
        ],
    )


@pytest.mark.asyncio
async def test_suggestion_intent_injects_mandate_and_acts(monkeypatch):
    """Avec suggestion_intent : le prompt système porte le mandat « produire une action »
    et un appel generate_test_data est bien capté comme action de test."""
    fake = FakeLLM([_generate_test_call("total annuel == somme des mois")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_suggestion_click())

    system_msg = fake.calls[0][0]
    assert "SUGGESTION" in system_msg.content
    assert "JAMAIS" in system_msg.content  # interdiction de la non-action
    assert update["agent_tool_call"] == "generate_test_data"
