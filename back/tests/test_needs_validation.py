"""Régression — désync description↔cardinalité (`needs_validation`).

Scénario : « pour un client avec 2 cartes, je m'attends à 1 ligne » mais la requête en
produit 2, AVEC des données d'entrée valides. On ne doit PAS boucler en auto-correction
(bad_data) : on sauve l'état et on émet un VALIDATION_PROMPT actionnable, puis on attend
la décision de l'utilisateur. Cf. test_evaluator, route_evaluator, accept_validation.
"""

import json
import uuid
from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage

from build_query.accept_validation import accept_validation
from build_query.query_chain import route_after_accept, route_evaluator
from build_query.test_evaluator import evaluate_tests
from utils.msg_types import MsgType


def _results_msg(test: dict) -> AIMessage:
    return AIMessage(
        content=json.dumps([test]),
        id=str(uuid.uuid4()),
        additional_kwargs={"type": MsgType.RESULTS},
    )


def test_route_evaluator_needs_validation_goes_to_history_saver():
    """needs_validation est terminal : pas de boucle de retry."""
    assert (
        route_evaluator({"evaluation_feedback": "needs_validation", "gen_retries": 5})
        == "history_saver"
    )


@pytest.mark.asyncio
async def test_evaluator_emits_validation_prompt():
    """Un test needs_validation émet le verdict + un VALIDATION_PROMPT ancré, sans toucher aux retries."""
    test = {
        "test_index": 0,
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "needs_validation",
        "evaluation_explanation": "Le scénario suppose 1 ligne mais le calcul en produit 2 — à confirmer.",
        "expected_row_count": 1,
        "results_json": json.dumps([{"client": "A"}, {"client": "A"}]),
        "unit_test_description": "Pour un client avec 2 cartes, une seule ligne attendue.",
    }
    state = {
        "messages": [_results_msg(test)],
        "test_index": 0,
        "gen_retries": 5,
        "request_id": "req-1",
    }

    update = await evaluate_tests(state)

    assert update["evaluation_feedback"] == "needs_validation"
    assert update["status"] == "complete"
    # Pas de décrément de retries : on ne corrige pas.
    assert "gen_retries" not in update

    types = [m.additional_kwargs.get("type") for m in update["messages"]]
    assert MsgType.EVALUATION in types
    assert MsgType.VALIDATION_PROMPT in types

    prompt = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.VALIDATION_PROMPT
    )
    assert prompt.additional_kwargs["test_index"] == 0
    assert prompt.additional_kwargs["expected_row_count"] == 1
    assert prompt.additional_kwargs["actual_row_count"] == 2
    assert "2" in prompt.content and "1" in prompt.content


@pytest.mark.asyncio
async def test_evaluator_does_not_loop_on_needs_validation():
    """Le feedback needs_validation ne déclenche ni bad_data ni bad_assertions."""
    test = {
        "test_index": 3,
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "needs_validation",
        "evaluation_explanation": "Cardinalité différente.",
        "expected_row_count": 1,
        "results_json": json.dumps([{"x": 1}, {"x": 2}]),
    }
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 3, "gen_retries": 2}
    )
    assert update["evaluation_feedback"] not in ("bad_data", "bad_assertions")
    assert route_evaluator({**update, "gen_retries": 2}) == "history_saver"


# ── bad_description : désync description ↔ valeur de sortie concrète (même flux Valider/Corriger) ──


def test_route_evaluator_bad_description_goes_to_history_saver():
    """bad_description est terminal comme needs_validation : pas de boucle de retry."""
    assert (
        route_evaluator({"evaluation_feedback": "bad_description", "gen_retries": 5})
        == "history_saver"
    )


@pytest.mark.asyncio
async def test_evaluator_emits_validation_prompt_for_bad_description():
    """Un test bad_description émet le verdict + un VALIDATION_PROMPT ancré (Valider / Corriger)."""
    test = {
        "test_index": 7,
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "bad_description",
        "evaluation_explanation": "La description annonce un total de 2.0M que le calcul ne produit pas.",
        "corrected_description": "Le total agrégé vaut 1.4M sur la période.",
        "results_json": json.dumps([{"total": 1_400_000}]),
        "unit_test_description": "Le total agrégé vaut 2.0M sur la période.",
    }
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 7, "gen_retries": 5}
    )

    assert update["evaluation_feedback"] == "bad_description"
    assert update["status"] == "complete"
    assert "gen_retries" not in update  # pas de décrément : on ne corrige pas

    types = [m.additional_kwargs.get("type") for m in update["messages"]]
    assert MsgType.EVALUATION in types
    assert MsgType.VALIDATION_PROMPT in types

    prompt = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.VALIDATION_PROMPT
    )
    assert prompt.additional_kwargs["reason_type"] == "bad_description"
    assert prompt.additional_kwargs["test_index"] == 7


@pytest.mark.asyncio
async def test_accept_validation_applies_corrected_description_without_llm():
    """Au clic « Je valide », accept_validation applique la corrected_description pré-calculée
    (pas de 2ᵉ appel LLM) et flippe le verdict à Bon."""
    target = {
        "test_index": "7",
        "verdict": "Insuffisant",
        "reason_type": "bad_description",
        "unit_test_description": "Le total agrégé vaut 2.0M sur la période.",
        "corrected_description": "Le total agrégé vaut 1.4M sur la période.",
        "corrected_name": "Total agrégé période",
        "results_json": json.dumps([{"total": 1_400_000}]),
    }
    stored = {"test_cases": [target]}
    captured: dict = {}

    def _capture(_sid, updates):
        captured.update(updates)

    with (
        patch("build_query.accept_validation.get_test", return_value=stored),
        patch("build_query.accept_validation.update_test", side_effect=_capture),
        patch(
            "build_query.accept_validation._realign_description",
            side_effect=AssertionError(
                "ne doit PAS appeler le LLM quand corrected_description existe"
            ),
        ),
    ):
        result = await accept_validation(
            {"session": "sess-1", "test_index": "7", "parent_message_id": "p-1"}
        )

    saved = captured["test_cases"][0]
    assert saved["verdict"] == "Bon"
    assert saved["reason_type"] is None
    assert saved["unit_test_description"] == "Le total agrégé vaut 1.4M sur la période."
    assert "corrected_description" not in saved

    update_msg = next(
        m
        for m in result["messages"]
        if m.additional_kwargs.get("type") == MsgType.UPDATE_TEST
    )
    payload = json.loads(update_msg.content)
    assert payload["new_description"] == "Le total agrégé vaut 1.4M sur la période."


# ── Reprise post-validation : suggestions régénérées, sans re-run DuckDB ──


def test_route_after_accept_revalidated_goes_to_suggestions():
    """Validation réussie → on reprend sur suggestions_generator (comme après l'éval)."""
    assert route_after_accept({"revalidated": True}) == "suggestions_generator"


def test_route_after_accept_noop_goes_to_history_saver():
    """No-op (test introuvable / test_index absent) → clôture directe, pas de suggestions."""
    assert route_after_accept({}) == "history_saver"


@pytest.mark.asyncio
async def test_accept_validation_triggers_suggestions_reprise():
    """accept_validation pose `revalidated` et hydrate le SQL/colonnes pour que
    suggestions_generator contextualise — sans ré-exécuter le test (pas de message RESULTS)."""
    target = {
        "test_index": "2",
        "verdict": "Insuffisant",
        "reason_type": "needs_validation",
        "unit_test_description": "Pour un client avec 2 cartes, une seule ligne attendue.",
        "corrected_description": "Pour un client avec 2 cartes, deux lignes sont produites.",
        "corrected_name": "Deux cartes, deux lignes",
        "results_json": json.dumps([{"client": "A"}, {"client": "A"}]),
    }
    stored = {
        "test_cases": [target],
        "optimized_sql": "SELECT client FROM cards",
        "sql": "SELECT client FROM cards",
        "used_columns": ['{"table": "cards", "used_columns": ["client"]}'],
        "query_decomposed": "[]",
    }

    with (
        patch("build_query.accept_validation.get_test", return_value=stored),
        patch("build_query.accept_validation.update_test"),
    ):
        result = await accept_validation(
            {"session": "sess-2", "test_index": "2", "parent_message_id": "p-2"}
        )

    assert result["revalidated"] is True
    assert result["optimized_sql"] == "SELECT client FROM cards"
    assert result["used_columns"] == stored["used_columns"]
    # Aucun message RESULTS : la validation ne ré-exécute pas (résultats stockés inchangés).
    types = [m.additional_kwargs.get("type") for m in result["messages"]]
    assert MsgType.RESULTS not in types
    assert route_after_accept(result) == "suggestions_generator"
