"""Phase 2.1 — le ``coherence_check`` PRÉPARE la revue humaine (il ne juge pas l'output).

Vérifie que le nœud, sur une sortie non vide : produit ``coherence`` + ``review_hint``,
choisit les ``expect.columns`` porteuses, flague le non-déterminisme, écrit le tout sur le
test en ``review.status = draft``, et dégrade proprement si le LLM échoue. LLM mocké — zéro
appel réseau, zéro budget.
"""

import json

import pytest
from langchain_core.messages import AIMessage

from build_query import coherence_check as cc
from build_query.coherence_check import CoherenceResult, coherence_check
from utils.msg_types import MsgType


@pytest.fixture(autouse=True)
def _enable_coherence(monkeypatch):
    # Le nœud est gardé par un flag (défaut OFF) : on l'active pour tester sa logique.
    monkeypatch.setattr(
        "storage.config.is_coherence_check_enabled", lambda: True, raising=True
    )


ROWS = [
    {"order_id": 1, "amount": 50.0, "is_high_value": False},
    {"order_id": 3, "amount": 100.0, "is_high_value": True},
]


def _state(status="complete", rows=ROWS, test_index=0):
    test = {
        "test_index": test_index,
        "test_name": "Seuil de high value",
        "unit_test_description": "amount >= 100 → is_high_value",
        "tags": ["Cas limites"],
        "status": status,
        "data": {"orders": [{"order_id": 1, "amount": 50.0}]},
        "results_json": json.dumps(rows),
        "assertion_results": [],
    }
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="results-1",
        additional_kwargs={"type": MsgType.RESULTS, "parent": "p0"},
    )
    return {
        "messages": [results_msg],
        "test_index": test_index,
        "optimized_sql": "SELECT order_id, amount, amount >= 100 AS is_high_value FROM orders",
        "dialect": "bigquery",
        "request_id": "req-1",
    }


def _mock_llm(monkeypatch, result):
    class _Structured:
        async def ainvoke(self, _msgs):
            if isinstance(result, Exception):
                raise result
            return result

    class _LLM:
        def with_structured_output(self, _model):
            return _Structured()

    monkeypatch.setattr(cc, "make_llm", lambda *a, **k: _LLM())


def _results_test(update):
    msg = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.RESULTS
    )
    return json.loads(msg.content)[0]


@pytest.mark.asyncio
async def test_produces_hint_coherence_columns(monkeypatch):
    _mock_llm(
        monkeypatch,
        CoherenceResult(
            coherence="ok",
            review_hint="ligne order_id=3 : amount=100 est le cas limite du seuil",
            key_columns=["order_id", "is_high_value"],
            non_deterministic=False,
        ),
    )
    update = await coherence_check(_state())
    test = _results_test(update)
    assert test["review"]["status"] == "draft"
    assert test["review"]["coherence"] == "ok"
    assert "order_id=3" in test["review"]["hint"]
    assert test["review"]["non_deterministic"] is False
    # expect restreint aux colonnes porteuses choisies par le coherence_check.
    assert test["expect"]["columns"] == ["order_id", "is_high_value"]
    assert test["expect"]["rows"] == [
        {"order_id": 1, "is_high_value": False},
        {"order_id": 3, "is_high_value": True},
    ]
    # Un message COHERENCE est émis pour le panneau (hint + coherence).
    coh = next(
        m
        for m in update["messages"]
        if m.additional_kwargs.get("type") == MsgType.COHERENCE
    )
    assert coh.additional_kwargs["coherence"] == "ok"


@pytest.mark.asyncio
async def test_warn_is_recorded_but_non_blocking(monkeypatch):
    _mock_llm(
        monkeypatch,
        CoherenceResult(
            coherence="warn",
            review_hint="aucune ligne NULL alors que le scénario prétend tester les NULL",
            key_columns=[],
            non_deterministic=True,
        ),
    )
    update = await coherence_check(_state())
    test = _results_test(update)
    assert test["review"]["coherence"] == "warn"
    assert test["review"]["non_deterministic"] is True
    # warn ne pose PAS d'evaluation_feedback (aucune boucle de retry déclenchée).
    assert "evaluation_feedback" not in update
    # key_columns vide → expect sur toutes les colonnes (repli).
    assert test["expect"]["columns"] == ["order_id", "amount", "is_high_value"]


@pytest.mark.asyncio
async def test_llm_failure_degrades_gracefully(monkeypatch):
    _mock_llm(monkeypatch, RuntimeError("LLM down"))
    update = await coherence_check(_state())
    test = _results_test(update)
    # Toujours un contrat expect + review draft, mais pas de hint/coherence.
    assert test["review"]["status"] == "draft"
    assert "hint" not in test["review"]
    assert test["expect"]["columns"] == ["order_id", "amount", "is_high_value"]
    # Aucun message COHERENCE quand le LLM a échoué.
    assert all(
        m.additional_kwargs.get("type") != MsgType.COHERENCE for m in update["messages"]
    )


@pytest.mark.asyncio
async def test_no_op_on_empty_output(monkeypatch):
    _mock_llm(monkeypatch, RuntimeError("must not be called"))
    update = await coherence_check(_state(rows=[]))
    # Sortie vide → circuit empty_results déterministe, coherence_check ne touche à rien.
    assert update == {}


@pytest.mark.asyncio
async def test_no_op_when_not_complete(monkeypatch):
    _mock_llm(monkeypatch, RuntimeError("must not be called"))
    update = await coherence_check(_state(status="empty_results"))
    assert update == {}


@pytest.mark.asyncio
async def test_disabled_flag_is_pass_through(monkeypatch):
    # Flag OFF (défaut) → aucun appel LLM, aucune modification : pass-through immédiat.
    monkeypatch.setattr(
        "storage.config.is_coherence_check_enabled", lambda: False, raising=True
    )
    _mock_llm(monkeypatch, RuntimeError("must not be called"))
    update = await coherence_check(_state())
    assert update == {}
