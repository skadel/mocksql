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


def _state_auto_correct():
    """État de retry bad_data automatique : flag `auto_correct` posé, un `input`
    PÉRIMÉ qui traîne, un test en échec + un verdict EVALUATION."""
    test = {
        "test_uid": "a3f9",
        "test_index": "1",
        "test_name": "Référentiel banques",
        "unit_test_description": "partition_date à jour",
        "data": {"MARKETING_Referentiels_banques": [{"partition_date": "2025-01-01"}]},
        "status": "empty_results",
        "failing_cte": "tmp_final_bp",
        "cte_trace": {
            "tmp_final_bp": {
                "row_count": 0,
                "steps": [
                    {"label": "rcomp", "count": 1},
                    {"label": "+ WHERE onus.no_siret IS NULL", "count": 0},
                ],
            }
        },
        "results_json": "[]",
        "assertion_results": [],
    }
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS},
    )
    eval_msg = AIMessage(
        content="**Insuffisant** — La CTE `tmp_final_bp` est vide",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": "1",
            "parent": "r1",
        },
    )
    return {
        "session": "sess1",
        "messages": [results_msg, eval_msg],
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "STALE_INPUT: génère un test pour le réseau BP",  # périmé → doit être ignoré
        "evaluation_feedback": "bad_data",
        "auto_correct": True,
        "gen_retries": 2,
    }


@pytest.mark.asyncio
async def test_auto_correct_branch_ignores_stale_input(monkeypatch):
    """Sur retry bad_data (auto_correct), l'agent envoie son trigger de correction
    CIBLÉE — pas l'input périmé — et consomme le flag.

    Pin du rebranch `bad_data → conversational_agent` : si l'agent prenait la branche
    `if user_input`, il traiterait la requête de génération initiale comme une consigne
    chat au lieu de corriger le test en échec."""
    fake = FakeLLM([_patch_call("a3f9")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct())

    last_human = fake.calls[0][-1]
    assert isinstance(last_human, HumanMessage)
    # Le trigger de correction ciblée est envoyé, PAS l'input périmé
    assert "STALE_INPUT" not in last_human.content
    assert "patch_test_field" in last_human.content or "CIBLÉE" in last_human.content
    # Le flag est consommé pour ne pas fuiter au tour suivant
    assert update["auto_correct"] is False
    # L'agent a agi (patch incrémental) → data_batch, pas un no-op qui tuerait le retry
    assert update["agent_tool_call"] == "data_batch"


@pytest.mark.asyncio
async def test_persistently_invalid_uid_does_not_loop(monkeypatch):
    """If the LLM never supplies a valid uid, retries are bounded and we don't hang."""
    fake = FakeLLM([_patch_call("bad1"), _patch_call("bad2"), _patch_call("bad3")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    # _UID_RETRY_MAX == 2 → at most 3 invocations (initial + 2 retries), then give up.
    assert len(fake.calls) <= 3
    assert update["agent_tool_call"] is None


def _update_data_call(test_uid, instruction="ajoute une raison sociale"):
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "update_test_data",
                "args": {"test_uid": test_uid, "instruction": instruction},
                "id": "call_u1",
            }
        ],
    )


def _generate_data_call(scenario="cas plage vide"):
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "generate_test_data",
                "args": {"scenario": scenario},
                "id": "call_g1",
            }
        ],
    )


@pytest.mark.asyncio
async def test_update_test_data_propagates_test_index(monkeypatch):
    """update_test_data doit propager le test_uid ciblé dans le state, sinon le
    generator ne sait pas quel test corriger et crée un doublon."""
    fake = FakeLLM([_update_data_call("a3f9")])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_with_one_test())

    assert update["agent_tool_call"] == "update_test_data"
    # L'identité stable (test_uid) doit être propagée, pas le rang.
    assert update["test_uid"] == "a3f9"


@pytest.mark.asyncio
async def test_generate_test_data_clears_stale_target(monkeypatch):
    """generate_test_data crée un NOUVEAU test → tout ciblage périmé (test_uid /
    test_index) doit être effacé, sinon _resolve_target_key écraserait un test."""
    fake = FakeLLM([_generate_data_call()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _state_with_one_test()
    state["test_uid"] = "a3f9"  # ciblage périmé qui traîne dans le state
    state["test_index"] = "1"
    update = await conversational_agent(state)

    assert update["agent_tool_call"] == "generate_test_data"
    assert update["test_uid"] is None
    assert update["test_index"] is None


@pytest.mark.asyncio
async def test_resume_after_clarification_allows_text_answer(monkeypatch):
    """Reprise après clarification : la consigne ne doit PLUS forcer « génère ou
    corrige le test ». Une question explicative doit pouvoir recevoir une réponse
    texte (aucun outil)."""
    clarif_msg = AIMessage(
        content="Dans quel contexte raison_sociale est null ?",
        id="c1",
        additional_kwargs={
            "type": MsgType.OTHER,
            "pending_intent": "Dans quel contexte raison_sociale est null ?",
        },
    )
    # L'agent répond en texte libre, sans appeler d'outil.
    fake = FakeLLM(
        [
            AIMessage(
                content="raison_sociale est null car le LEFT JOIN coface ne matche pas."
            )
        ]
    )
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    state = _state_with_one_test()
    state["history"] = [clarif_msg]  # la reprise s'appuie sur state["history"]
    state["input"] = "oui dans le resultat des tests"
    update = await conversational_agent(state)

    # La reprise ne force plus une action de test.
    system_msg = fake.calls[0][0]
    assert "génère ou corrige le test approprié" not in system_msg.content
    # Réponse texte conservée, aucun outil déclenché.
    assert update["agent_tool_call"] is None
