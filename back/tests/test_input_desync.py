"""Régression — désync description ↔ données d'ENTRÉE injectées (`bad_input_description`).

TICKET-2. La desync description↔SORTIE est déjà gérée (`bad_description` /
`needs_validation`). Ce module couvre l'autre axe : la description raconte des valeurs
d'entrée (« on injecte 10 et 20 TiB ») qui ne correspondent pas aux lignes réellement
injectées (28.08 / 3479.61) — cas observé sur fdp `daily_verified_claims`.

Contrat (hérité de TICKET-1) : on NE réécrit JAMAIS le narratif en silence. On passe par
la MÊME délégation que bad_description — verdict Insuffisant + VALIDATION_PROMPT
actionnable (Valider → réaligne / Corriger). Pas de boucle d'auto-correction.
"""

import json
import uuid
from unittest.mock import patch

import pytest
from langchain_core.messages import AIMessage

from build_query.accept_validation import accept_validation
from build_query.query_chain import route_evaluator
from build_query.test_evaluator import evaluate_tests
from utils.msg_types import MsgType


def _results_msg(test: dict) -> AIMessage:
    return AIMessage(
        content=json.dumps([test]),
        id=str(uuid.uuid4()),
        additional_kwargs={"type": MsgType.RESULTS},
    )


def test_route_evaluator_bad_input_description_goes_to_history_saver():
    """bad_input_description est terminal comme bad_description : pas de boucle de retry."""
    assert (
        route_evaluator(
            {"evaluation_feedback": "bad_input_description", "gen_retries": 5}
        )
        == "history_saver"
    )


@pytest.mark.asyncio
async def test_evaluator_emits_validation_prompt_for_bad_input_description():
    """Un test bad_input_description émet le verdict + un VALIDATION_PROMPT ancré
    (Valider / Corriger), sans toucher aux retries — exactement comme bad_description."""
    test = {
        "test_index": 4,
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
    }
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 4, "gen_retries": 5}
    )

    assert update["evaluation_feedback"] == "bad_input_description"
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
    assert prompt.additional_kwargs["reason_type"] == "bad_input_description"
    assert prompt.additional_kwargs["test_index"] == 4


@pytest.mark.asyncio
async def test_evaluator_does_not_loop_on_bad_input_description():
    """Le feedback bad_input_description ne déclenche ni bad_data ni bad_assertions."""
    test = {
        "test_index": 2,
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "bad_input_description",
        "evaluation_explanation": "Valeurs d'entrée décrites ≠ injectées.",
        "corrected_description": "Réalignée sur les valeurs réelles.",
        "results_json": json.dumps([{"x": 1}]),
        "unit_test_description": "On injecte 10 et 20.",
    }
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 2, "gen_retries": 2}
    )
    assert update["evaluation_feedback"] not in ("bad_data", "bad_assertions")
    assert route_evaluator({**update, "gen_retries": 2}) == "history_saver"


@pytest.mark.asyncio
async def test_evaluator_question_points_to_premise_when_user_authored():
    """Si le test porte une prémisse utilisateur (TICKET-1), la question de validation
    pointe l'attente énoncée plutôt qu'un simple réalignement cosmétique."""
    test = {
        "test_index": 5,
        "status": "complete",
        "verdict": "Insuffisant",
        "reason_type": "bad_input_description",
        "evaluation_explanation": "Entrées décrites ≠ injectées.",
        "corrected_description": "Réalignée.",
        "results_json": json.dumps([{"x": 1}]),
        "unit_test_description": "On injecte 10 et 20.",
        "user_premise": "on injecte 10 et 20 TiB",
    }
    update = await evaluate_tests(
        {"messages": [_results_msg(test)], "test_index": 5, "gen_retries": 3}
    )
    prompt = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.VALIDATION_PROMPT
    )
    assert "prémisse" in prompt.content.lower()


# ── accept_validation : valider une desync d'entrée retire la prémisse (T1↔T2) ──


@pytest.mark.asyncio
async def test_accept_validation_drops_premise_on_input_desync():
    """Valider une desync d'ENTRÉE = l'utilisateur accepte les données réelles → sa
    prémisse était fausse, on la retire (sinon le garde bad_data protégerait une
    prémisse abandonnée)."""
    target = {
        "test_index": "5",
        "verdict": "Insuffisant",
        "reason_type": "bad_input_description",
        "unit_test_description": "On injecte 10 et 20 TiB.",
        "corrected_description": "On injecte 28.08 et 3479.61 TiB.",
        "corrected_name": "Deux claims agrégés",
        "user_premise": "on injecte 10 et 20 TiB",
        "results_json": json.dumps([{"total": 3507.69}]),
    }
    captured: dict = {}
    with (
        patch(
            "build_query.accept_validation.get_test",
            return_value={"test_cases": [target]},
        ),
        patch(
            "build_query.accept_validation.update_test",
            side_effect=lambda _sid, updates: captured.update(updates),
        ),
    ):
        await accept_validation(
            {"session": "s1", "test_index": "5", "parent_message_id": "p1"}
        )

    saved = captured["test_cases"][0]
    assert saved["verdict"] == "Bon"
    assert "user_premise" not in saved  # prémisse abandonnée retirée


@pytest.mark.asyncio
async def test_accept_validation_keeps_premise_on_output_desync():
    """Valider une desync de SORTIE (bad_description) ne touche PAS à la prémisse
    d'entrée, qui reste pertinente."""
    target = {
        "test_index": "5",
        "verdict": "Insuffisant",
        "reason_type": "bad_description",
        "unit_test_description": "Le total vaut 2.0M.",
        "corrected_description": "Le total vaut 1.4M.",
        "corrected_name": "Total période",
        "user_premise": "un client avec 2 cartes",
        "results_json": json.dumps([{"total": 1_400_000}]),
    }
    captured: dict = {}
    with (
        patch(
            "build_query.accept_validation.get_test",
            return_value={"test_cases": [target]},
        ),
        patch(
            "build_query.accept_validation.update_test",
            side_effect=lambda _sid, updates: captured.update(updates),
        ),
    ):
        await accept_validation(
            {"session": "s1", "test_index": "5", "parent_message_id": "p1"}
        )

    saved = captured["test_cases"][0]
    assert saved["user_premise"] == "un client avec 2 cartes"  # préservée
