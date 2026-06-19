"""Boucle multi-tests (génération de N tests en chaîne, 1–3).

À la 1ʳᵉ génération, l'utilisateur peut demander N tests au total (``tests_target``).
Le nominal compte pour 1 ; on auto-construit ``N-1`` tests supplémentaires en générant
UNE suggestion à la fois (``generate_single_suggestion``) puis en l'enchaînant sur le
``conversational_agent`` (chemin clic-suggestion). ``route_evaluator`` décide de reboucler
ou de clore via le ``suggestions_generator`` (qui remplit le panneau, sans boucler).
"""

import pytest
from langchain_core.runnables import RunnableLambda

from build_query.query_chain import route_evaluator
from build_query import suggestions_node
from build_query.suggestions_node import generate_single_suggestion


# --- route_evaluator : décision de boucle vs clôture --------------------------


def _state(target, built):
    return {
        "evaluation_feedback": None,
        "has_existing_tests": False,
        "gen_retries": 2,
        "tests_target": target,
        "auto_tests_built": built,
    }


def test_target_one_closes_immediately():
    """N=1 (défaut) → pas de boucle, on va droit au panneau de suggestions."""
    assert route_evaluator(_state(1, 0)) == "suggestions_generator"


def test_target_three_loops_then_closes():
    """N=3 → 2 tests auto : boucle tant que built < 2, puis clôture."""
    assert route_evaluator(_state(3, 0)) == "generate_single_suggestion"
    assert route_evaluator(_state(3, 1)) == "generate_single_suggestion"
    assert route_evaluator(_state(3, 2)) == "suggestions_generator"


def test_target_two_loops_once():
    assert route_evaluator(_state(2, 0)) == "generate_single_suggestion"
    assert route_evaluator(_state(2, 1)) == "suggestions_generator"


def test_missing_target_defaults_to_no_loop():
    """Sans tests_target ni compteur (comportement historique) → suggestions direct."""
    state = {"evaluation_feedback": None, "has_existing_tests": False, "gen_retries": 2}
    assert route_evaluator(state) == "suggestions_generator"


def test_bad_data_priority_over_loop():
    """La correction bad_data reste prioritaire sur la boucle multi-tests."""
    state = _state(3, 0)
    state["evaluation_feedback"] = "bad_data"
    assert route_evaluator(state) == "bad_data_to_agent"


# --- generate_single_suggestion : pose l'intention + incrémente le compteur ----


def _patch_llm(monkeypatch, text="Vérifie le cas NULL"):
    fake = RunnableLambda(
        lambda _pv: suggestions_node.TestSuggestionsOutput(
            analyse_des_manques="x",
            suggestions=[suggestions_node.TestSuggestion(text=text)],
        )
    )

    class _FakeLLM:
        def with_structured_output(self, _schema):
            return fake

    monkeypatch.setattr(suggestions_node, "make_llm", lambda: _FakeLLM())


@pytest.mark.asyncio
async def test_single_suggestion_sets_intent_and_counter(monkeypatch):
    async def _no_tests(_session, _state):
        return []

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _no_tests)
    # Le panneau ne doit PAS être touché par la boucle (persistance réservée à la clôture).
    monkeypatch.setattr(
        suggestions_node,
        "update_test",
        lambda *a, **k: pytest.fail("update_test ne doit pas être appelé en boucle"),
    )
    _patch_llm(monkeypatch)

    state = {
        "session": "s1",
        "query": "SELECT 1",
        "dialect": "bigquery",
        "messages": [],
        "auto_tests_built": 1,
    }
    out = await generate_single_suggestion(state)
    assert out["suggestion_intent"] is True
    assert out["auto_tests_built"] == 2
    assert out["input"] == "Vérifie le cas NULL"


@pytest.mark.asyncio
async def test_single_suggestion_fallback_on_llm_error(monkeypatch):
    async def _no_tests(_session, _state):
        return []

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _no_tests)

    def _boom():
        raise RuntimeError("pas de credentials")

    monkeypatch.setattr(suggestions_node, "make_llm", _boom)

    state = {
        "session": "s1",
        "query": "SELECT 1",
        "dialect": "bigquery",
        "messages": [],
    }
    out = await generate_single_suggestion(state)
    # Échec LLM : on pose tout de même l'intention + le compteur, avec une consigne générique.
    assert out["suggestion_intent"] is True
    assert out["auto_tests_built"] == 1
    assert "cas limite" in out["input"]
