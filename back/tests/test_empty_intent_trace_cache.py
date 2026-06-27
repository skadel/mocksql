"""Régression — juge d'intention vide : injection de la trace CTE + cache de verdict.

Deux améliorations couplées sur `_classify_empty_intent` :

1. **Trace d'exécution injectée** — le juge ne doit plus simuler de tête une requête
   à 15 CTE pour deviner d'où viennent les 0 lignes. On lui donne l'échelle de lignes
   par étape (`cte_trace`) + la transition bloquante + la décomposition de prédicat,
   neutralement (ni « corrige les données » ni « c'est voulu » — il décide).

2. **Cache de verdict (fingerprint)** — si SQL + données + scénario + CTE bloquante
   sont inchangés, on réutilise le verdict stocké sans rappeler le LLM.

Cf. test_evaluator._classify_empty_intent / _format_trace_for_intent_judge /
_empty_intent_fingerprint.
"""

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.messages import AIMessage

from build_query.test_evaluator import (
    _classify_empty_intent,
    _empty_intent_fingerprint,
    _format_trace_for_intent_judge,
    _ReevalResult,
    evaluate_tests,
)
from utils.msg_types import MsgType


def _cte_trace() -> dict:
    return {
        "PHOTO_M_1": {"row_count": 1},
        "PHOTO_M": {"row_count": 2},
        "TEMP_VIEW_MEG": {
            "row_count": 1,
            "sample": [{"REGPMENT_Old": "PREMIUM", "REGPMENT_New": None}],
        },
        "MEG": {
            "row_count": 0,
            "blocking": True,
            "steps": [
                {"label": "FROM TEMP_VIEW_MEG", "count": 1},
                {"label": "WHERE REGPMENT_New <> REGPMENT_Old", "count": 0},
            ],
            "join_breakdown": [
                "prédicat `REGPMENT_New <> REGPMENT_Old` : REGPMENT_New=NULL ← BLOQUANT"
            ],
        },
        "TEMP_RESULTS": {"row_count": 0},
    }


def _empty_test(idx: int = 0) -> dict:
    return {
        "test_index": idx,
        "status": "empty_results",
        "unit_test_description": "Client MEG : changement de regroupement attendu.",
        "data": {"DS_REF_PORTEUR": [{"no_carte": "001", "cd_iban": "FR76"}]},
        "cte_trace": _cte_trace(),
        "failing_cte": "MEG",
    }


def _capturing_llm(captured: dict, verdict: str = "Bon", explanation: str = "ok"):
    """make_llm() factice qui capture le prompt HUMAN passé au juge."""
    structured = MagicMock()

    async def _ainvoke(messages):
        captured["prompt"] = messages[-1].content
        return _ReevalResult(verdict=verdict, explanation=explanation)

    structured.ainvoke = AsyncMock(side_effect=_ainvoke)
    llm = MagicMock()
    llm.with_structured_output.return_value = structured
    return llm


# ─────────────────────────── 1. Trace injectée ────────────────────────────


@pytest.mark.asyncio
async def test_trace_injected_into_judge_prompt():
    captured: dict = {}
    test = _empty_test()
    with patch(
        "build_query.test_evaluator.make_llm",
        return_value=_capturing_llm(captured),
    ):
        await _classify_empty_intent(
            {"dialect": "bigquery"}, test, "WITH x AS (...) ..."
        )

    prompt = captured["prompt"]
    # L'échelle de lignes par étape doit être présente…
    assert "PHOTO_M" in prompt
    assert "MEG" in prompt
    # …avec la CTE bloquante marquée comme première étape vide…
    assert "première étape vide" in prompt
    # …la transition qui élimine les lignes…
    assert "REGPMENT_New <> REGPMENT_Old" in prompt
    # …et la décomposition du prédicat.
    assert "BLOQUANT" in prompt


def test_format_trace_is_neutral_and_ordered():
    block = _format_trace_for_intent_judge("MEG", _cte_trace())
    # Ordre du flux préservé.
    assert block.index("PHOTO_M_1") < block.index("MEG")
    # Pas de consigne « corrige les données » (biais générateur) dans le bloc juge.
    assert "ajoute" not in block.lower()
    assert "patch" not in block.lower()
    # Étape bloquante nommée.
    assert "MEG" in block
    assert "première étape vide" in block


def test_format_trace_empty_when_no_trace():
    assert _format_trace_for_intent_judge("", {}) == ""


# ─────────────────────────── 2. Fingerprint ───────────────────────────────


def test_fingerprint_stable_and_sensitive():
    sql = "SELECT * FROM t WHERE x = 1"
    data = {"t": [{"x": 1}]}
    base = _empty_intent_fingerprint(sql, data, "scénario A", "MEG")

    # Stable : mêmes entrées → même empreinte.
    assert base == _empty_intent_fingerprint(sql, data, "scénario A", "MEG")
    # Insensible au reformatage cosmétique (espaces / sauts de ligne).
    assert base == _empty_intent_fingerprint(
        "SELECT *\n  FROM t\nWHERE x = 1", data, "scénario A", "MEG"
    )
    # Sensible aux 4 dimensions.
    assert base != _empty_intent_fingerprint(
        sql, {"t": [{"x": 2}]}, "scénario A", "MEG"
    )
    assert base != _empty_intent_fingerprint(sql, data, "scénario B", "MEG")
    assert base != _empty_intent_fingerprint(sql, data, "scénario A", "TEMP_RESULTS")


# ─────────────────────────── 3. Cache de verdict ──────────────────────────


@pytest.mark.asyncio
async def test_cache_hit_skips_llm():
    sql = "WITH x AS (...) ..."
    test = _empty_test()
    fp = _empty_intent_fingerprint(
        sql, test["data"], test["unit_test_description"], "MEG"
    )
    test["empty_intent_cache"] = {
        "fingerprint": fp,
        "verdict": "Bon",
        "explanation": "déjà jugé : vide voulu",
    }

    llm = MagicMock()
    llm.with_structured_output.side_effect = AssertionError(
        "LLM ne doit pas être appelé"
    )
    with patch("build_query.test_evaluator.make_llm", return_value=llm):
        verdict, explanation = await _classify_empty_intent(
            {"dialect": "bigquery"}, test, sql
        )

    assert verdict == "Bon"
    assert explanation == "déjà jugé : vide voulu"


@pytest.mark.asyncio
async def test_cache_miss_on_changed_scenario_calls_llm():
    sql = "WITH x AS (...) ..."
    test = _empty_test()
    # Empreinte d'un AUTRE scénario → cache périmé → re-éval.
    test["empty_intent_cache"] = {
        "fingerprint": _empty_intent_fingerprint(sql, test["data"], "autre", "MEG"),
        "verdict": "Bon",
        "explanation": "périmé",
    }
    captured: dict = {}
    with patch(
        "build_query.test_evaluator.make_llm",
        return_value=_capturing_llm(
            captured, verdict="Insuffisant", explanation="frais"
        ),
    ):
        verdict, explanation = await _classify_empty_intent(
            {"dialect": "bigquery"}, test, sql
        )

    assert "prompt" in captured  # LLM bien rappelé
    assert verdict == "Insuffisant"
    assert explanation == "frais"


@pytest.mark.asyncio
async def test_caller_persists_cache_on_pass():
    """Sur verdict PASS, le test mis à jour porte `empty_intent_cache` avec l'empreinte
    courante — pour court-circuiter le LLM au prochain run inchangé."""
    test = _empty_test()
    state = {
        "messages": [
            AIMessage(
                content=json.dumps([test]),
                id=str(uuid.uuid4()),
                additional_kwargs={"type": MsgType.RESULTS},
            )
        ],
        "test_index": 0,
        "gen_retries": 3,
        "request_id": "req-1",
        "query": "WITH x AS (...) ...",
    }
    captured: dict = {}
    with patch(
        "build_query.test_evaluator.make_llm",
        return_value=_capturing_llm(captured, verdict="Bon", explanation="vide voulu"),
    ):
        update = await evaluate_tests(state)

    results_msg = update["messages"][0]
    persisted = json.loads(results_msg.content)[0]
    cache = persisted.get("empty_intent_cache")
    assert cache is not None
    assert cache["verdict"] == "Bon"
    expected_fp = _empty_intent_fingerprint(
        state["query"], test["data"], test["unit_test_description"], "MEG"
    )
    assert cache["fingerprint"] == expected_fp
