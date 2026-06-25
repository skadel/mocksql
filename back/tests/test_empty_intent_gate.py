"""Régression — verdict LLM des résultats vides (garde d'intention).

Un résultat à 0 ligne n'est PAS automatiquement `bad_data` : c'est parfois le
comportement *voulu* du test (axe de couverture `empty`, branche d'un UNION ALL
volontairement vide…). On laisse le LLM juger l'intention UNE SEULE FOIS, à la
première occurrence du vide (`empty_results_regen` falsy). Sur les retries de
régénération (`empty_results_regen` True), on reste sur la boucle déterministe
sans rappeler le LLM — c'est le compromis « hybride ».

Cf. test_evaluator.evaluate_tests (fast path empty_results) et _classify_empty_intent.
"""

import json
import uuid
from unittest.mock import AsyncMock, patch

import pytest
from langchain_core.messages import AIMessage

from build_query.test_evaluator import evaluate_tests
from utils.msg_types import MsgType


def _results_msg(test: dict) -> AIMessage:
    return AIMessage(
        content=json.dumps([test]),
        id=str(uuid.uuid4()),
        additional_kwargs={"type": MsgType.RESULTS},
    )


def _empty_test(idx: int = 0) -> dict:
    return {
        "test_index": idx,
        "status": "empty_results",
        "unit_test_description": (
            "Filtre sur une date future : la requête ne doit retourner aucune ligne."
        ),
        "data": {"orders": [{"id": 1, "d": "2020-01-01"}]},
        "cte_trace": {"filtered": {"row_count": 0}},
        "failing_cte": "filtered",
    }


@pytest.mark.asyncio
async def test_empty_intended_is_pass_not_bad_data():
    """Quand le LLM juge que 0 ligne est le comportement voulu → verdict PASS,
    pas de bad_data, pas de décrément de retries."""
    state = {
        "messages": [_results_msg(_empty_test())],
        "test_index": 0,
        "gen_retries": 3,
        "request_id": "req-1",
    }

    with patch(
        "build_query.test_evaluator._classify_empty_intent",
        new=AsyncMock(
            return_value=(
                "Bon",
                "0 ligne est bien le résultat attendu : la date est future.",
            )
        ),
    ) as mock_intent:
        update = await evaluate_tests(state)

    mock_intent.assert_awaited_once()
    assert update.get("evaluation_feedback") in (None,)
    assert update["status"] == "complete"
    assert "gen_retries" not in update  # pas de correction → pas de décrément
    assert not update.get("empty_results_regen")

    # Deux messages : RESULTS (avec assertion table vide) puis EVALUATION
    assert len(update["messages"]) == 2
    results_msg, eval_msg = update["messages"]

    assert results_msg.additional_kwargs.get("type") == MsgType.RESULTS
    updated_tests = json.loads(results_msg.content)
    assert len(updated_tests) == 1
    test = updated_tests[0]
    assert test["verdict"] == "Bon"
    assert (
        test["evaluation_explanation"]
        == "0 ligne est bien le résultat attendu : la date est future."
    )
    assert len(test["assertion_results"]) == 1
    assertion = test["assertion_results"][0]
    assert assertion["passed"] is True
    assert assertion["sql"] == "SELECT * FROM __result__"

    assert eval_msg.additional_kwargs.get("type") == MsgType.EVALUATION
    assert eval_msg.additional_kwargs.get("parent") == results_msg.id
    assert eval_msg.content.startswith("**Bon**")
    # Le message utilisateur porte l'explication LLM, pas le jargon structurel.
    assert "contraintes" not in eval_msg.content


@pytest.mark.asyncio
async def test_empty_unexpected_falls_to_bad_data_with_llm_wording():
    """Quand le LLM juge le vide inattendu → bad_data + boucle de régénération.
    Le message utilisateur porte l'explication LLM (claire), le diag structurel
    reste interne (additional_kwargs['diag'])."""
    state = {
        "messages": [_results_msg(_empty_test())],
        "test_index": 0,
        "gen_retries": 3,
        "request_id": "req-1",
    }

    with patch(
        "build_query.test_evaluator._classify_empty_intent",
        new=AsyncMock(
            return_value=(
                "Insuffisant",
                "Le scénario attend des lignes mais le filtre de date les exclut toutes.",
            )
        ),
    ):
        update = await evaluate_tests(state)

    assert update["evaluation_feedback"] == "bad_data"
    assert update["status"] == "empty_results"
    assert update["empty_results_regen"] is True
    assert update["gen_retries"] == 2

    msg = update["messages"][0]
    assert msg.additional_kwargs.get("type") == MsgType.EVALUATION
    assert "**Insuffisant**" in msg.content
    # Explication LLM côté user…
    assert "filtre de date" in msg.content
    # …et le diag structurel reste disponible pour le générateur (interne).
    assert msg.additional_kwargs.get("diag")


@pytest.mark.asyncio
async def test_regen_retry_skips_llm_intent():
    """Sur un retry de régénération (empty_results_regen déjà True), on ne rappelle
    PAS le LLM d'intention : boucle déterministe directe."""
    state = {
        "messages": [_results_msg(_empty_test())],
        "test_index": 0,
        "gen_retries": 2,
        "request_id": "req-1",
        "empty_results_regen": True,
    }

    with patch(
        "build_query.test_evaluator._classify_empty_intent",
        new=AsyncMock(side_effect=AssertionError("ne doit pas être appelé en retry")),
    ) as mock_intent:
        update = await evaluate_tests(state)

    mock_intent.assert_not_called()
    assert update["evaluation_feedback"] == "bad_data"
    assert update["status"] == "empty_results"
    assert update["gen_retries"] == 1
