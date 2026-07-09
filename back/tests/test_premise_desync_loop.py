"""Boucle de correction auto pour une désync prémisse↔données d'ENTRÉE.

Extension de TICKET-1/TICKET-2. Quand le juge détecte `bad_input_description` ET que
le test porte une `user_premise` EXPLICITE (« on injecte 10 et 20 TiB »), la cible de
correction est connue : la prémisse. Plutôt que d'émettre d'emblée un VALIDATION_PROMPT
(Valider / Corriger), on tente d'abord une correction automatique — aligner les données
injectées SUR la prémisse — via la boucle `bad_data` → `conversational_agent`, et on ne
retombe sur le VALIDATION_PROMPT qu'à épuisement des retries.

Contrat préservé : on ne réécrit JAMAIS la prémisse en silence. La correction attendue
est de ramener les DONNÉES vers la prémisse, pas l'inverse.
"""

import json
import uuid

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.conversational_agent import conversational_agent
from build_query.query_chain import route_evaluator
from build_query.test_evaluator import evaluate_tests
from utils.msg_types import MsgType


def _results_msg(test: dict) -> AIMessage:
    return AIMessage(
        content=json.dumps([test]),
        id=str(uuid.uuid4()),
        additional_kwargs={"type": MsgType.RESULTS},
    )


def _bad_input_test(**overrides) -> dict:
    test = {
        "test_index": 4,
        "test_uid": "a3f9",
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "bad_input_description",
        "evaluation_explanation": (
            "La description annonce 10 et 20 TiB en entrée mais les lignes injectées "
            "portent 28.08 et 3479.61."
        ),
        "corrected_description": (
            "Deux claims vérifiés de 28.08 et 3479.61 TiB sont agrégés sur la journée."
        ),
        "results_json": json.dumps([{"total_tib": 3507.69}]),
        "unit_test_description": "On injecte deux claims de 10 et 20 TiB sur la journée.",
        "data": {"claims": [{"tib": 28.08}, {"tib": 3479.61}]},
    }
    test.update(overrides)
    return test


# ── Évaluateur : bascule vers la boucle bad_data quand une prémisse est en jeu ──


@pytest.mark.asyncio
async def test_evaluator_routes_to_bad_data_loop_when_premise_and_retries():
    """bad_input_description + user_premise + retries > 0 → boucle de correction auto
    (pas de VALIDATION_PROMPT), sans décrémenter gen_retries (l'agent le fait)."""
    test = _bad_input_test(user_premise="on injecte 10 et 20 TiB")
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 4, "gen_retries": 2}
    )

    assert update["evaluation_feedback"] == "bad_data"
    assert update["status"] == "empty_results"
    assert "gen_retries" not in update  # décrément délégué à l'agent

    types = [m.additional_kwargs.get("type") for m in update["messages"]]
    assert MsgType.VALIDATION_PROMPT not in types
    assert MsgType.BAD_DATA_DIAGNOSTIC in types

    # Le diagnostic synthétique porte le marqueur `kind` + les 6 clés lues par
    # _build_agent_eval_context (accès par clé directe → KeyError si absente).
    diag_msg = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.BAD_DATA_DIAGNOSTIC
    )
    diag = json.loads(diag_msg.content)
    assert diag["kind"] == "premise_desync"
    for key in (
        "root_cause",
        "sql_pattern",
        "data_issue",
        "fix_recipe",
        "affected_tables",
        "affected_ctes",
    ):
        assert key in diag
    assert "10 et 20 TiB" in diag["fix_recipe"]
    assert diag["affected_tables"] == ["claims"]


def test_route_evaluator_bad_data_goes_to_agent_loop():
    """Non-régression du routage : bad_data + retries > 0 → bad_data_to_agent."""
    assert (
        route_evaluator({"evaluation_feedback": "bad_data", "gen_retries": 2})
        == "bad_data_to_agent"
    )


# ── Fallback : sans prémisse, ou retries épuisés → VALIDATION_PROMPT inchangé ──


@pytest.mark.asyncio
async def test_evaluator_falls_back_to_validation_prompt_when_retries_exhausted():
    """bad_input_description + user_premise mais gen_retries == 0 → on ne boucle plus,
    on retombe sur le VALIDATION_PROMPT (question pointant la prémisse)."""
    test = _bad_input_test(user_premise="on injecte 10 et 20 TiB")
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 4, "gen_retries": 0}
    )

    assert update["evaluation_feedback"] == "bad_input_description"
    types = [m.additional_kwargs.get("type") for m in update["messages"]]
    assert MsgType.VALIDATION_PROMPT in types
    prompt = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.VALIDATION_PROMPT
    )
    assert "prémisse" in prompt.content.lower()


@pytest.mark.asyncio
async def test_evaluator_keeps_validation_prompt_without_premise():
    """Sans user_premise : comportement historique — VALIDATION_PROMPT, pas de boucle
    (c'est MockSQL qui a écrit l'attente → on demande à l'utilisateur)."""
    test = _bad_input_test()  # pas de user_premise
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 4, "gen_retries": 5}
    )
    assert update["evaluation_feedback"] == "bad_input_description"
    types = [m.additional_kwargs.get("type") for m in update["messages"]]
    assert MsgType.VALIDATION_PROMPT in types
    assert MsgType.BAD_DATA_DIAGNOSTIC not in types


# ── Agent : trigger dédié « aligner les données sur la prémisse » ──────────────


class FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _patch_batch() -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": "a3f9",
                    "table": "claims",
                    "row_index": 0,
                    "field": "tib",
                    "value_json": json.dumps(10),
                },
                "id": "call_0",
            }
        ],
    )


def _premise_desync_state():
    premise = "on injecte 10 et 20 TiB"
    test = _bad_input_test(user_premise=premise, status="empty_results")
    test["test_index"] = "1"
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS},
    )
    diagnostic = {
        "root_cause": "Les données d'entrée ne respectent pas la prémisse énoncée.",
        "sql_pattern": "premise_desync",
        "data_issue": "injecté 28.08 / 3479.61 vs prémisse 10 et 20 TiB",
        "fix_recipe": f"Aligner les données d'entrée sur la prémisse « {premise} ».",
        "affected_tables": ["claims"],
        "affected_ctes": [],
        "kind": "premise_desync",
    }
    eval_msg = AIMessage(
        content="**Insuffisant** — désync prémisse↔entrée",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": "1",
            "parent": "r1",
            "diagnostic": diagnostic,
        },
    )
    return {
        "session": "sess1",
        "messages": [results_msg, eval_msg],
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "",
        "evaluation_feedback": "bad_data",
        "auto_correct": True,
        "gen_retries": 2,
    }


def _trigger_text(messages) -> str:
    return "\n".join(str(m.content) for m in messages if isinstance(m, HumanMessage))


@pytest.mark.asyncio
async def test_agent_trigger_aligns_data_on_premise(monkeypatch):
    """Diagnostic kind=premise_desync : le trigger ordonne d'aligner les DONNÉES
    d'entrée sur la prémisse — il nomme la prémisse et n'invite pas à la déléguer."""
    fake = FakeLLM([_patch_batch()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_premise_desync_state())

    trigger = _trigger_text(fake.calls[0])
    assert "10 et 20 TiB" in trigger  # la prémisse est nommée
    assert "prémisse" in trigger.lower()
    # Direction sans ambiguïté : on corrige les données, on ne délègue pas.
    assert "request_reevaluation" not in trigger
