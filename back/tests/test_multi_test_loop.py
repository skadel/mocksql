"""Boucle multi-tests (génération de N tests en chaîne, 1–3).

À la 1ʳᵉ génération, l'utilisateur peut demander N tests au total (``tests_target``).
Le nominal compte pour 1 ; on auto-construit ``N-1`` tests supplémentaires en générant
UNE suggestion à la fois (``generate_single_suggestion``) puis en l'enchaînant sur le
``conversational_agent`` (chemin clic-suggestion). ``route_evaluator`` décide de reboucler
ou de clore via le ``suggestions_generator`` (qui remplit le panneau, sans boucler).
"""

import json

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


# --- Priorité de couverture UNION ALL : branches d'abord, puis assemblage --------
#
# Quand l'utilisateur demande N tests sur une requête UNION ALL, la boucle multi-tests
# doit d'abord couvrir CHAQUE branche (priorité 1, test focalisé déterministe), puis un
# test nominal sur l'assemblage complet (priorité 2, target_path="all"), et SEULEMENT
# ensuite retomber sur la suggestion LLM contextuelle (cas limites). Le test 1 est déjà
# la 1ʳᵉ branche (défaut du generator) ; ces cas concernent les tests auto suivants.


def _paths_state(test_cases, last_target_path):
    """State de boucle batch avec un catalogue de paths à 2 branches + assemblage."""
    return {
        "session": "s1",
        "query": "SELECT 1",
        "dialect": "bigquery",
        "messages": [],
        "auto_tests_built": 1,
        "tests_target": 4,
        "target_path": last_target_path,
        "path_plans": json.dumps(
            {
                "ouverture": {"branch_index": 0, "host_cte": "final_query"},
                "fermeture": {"branch_index": 1, "host_cte": "final_query"},
                "all": {"branch_index": None, "host_cte": None},
            }
        ),
    }


def _no_llm_guard(monkeypatch):
    monkeypatch.setattr(
        suggestions_node,
        "make_llm",
        lambda: pytest.fail(
            "le LLM ne doit pas être appelé pour une couverture de branche déterministe"
        ),
    )


@pytest.mark.asyncio
async def test_batch_prioritizes_next_uncovered_union_branch(monkeypatch):
    """1ʳᵉ branche déjà couverte → la boucle cible la branche suivante, SANS LLM."""
    test_cases = [{"test_index": "1", "target_path": "ouverture"}]

    async def _tests(_session, _state):
        return test_cases

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _tests)
    _no_llm_guard(monkeypatch)

    out = await generate_single_suggestion(_paths_state(test_cases, "ouverture"))
    assert out["target_path"] == "fermeture"
    assert out["suggestion_intent"] is True
    assert out["auto_tests_built"] == 2
    assert "fermeture" in out["input"]


@pytest.mark.asyncio
async def test_batch_falls_to_full_assembly_after_all_branches(monkeypatch):
    """Toutes les branches couvertes → test nominal sur l'assemblage complet (all)."""
    test_cases = [
        {"test_index": "1", "target_path": "ouverture"},
        {"test_index": "2", "target_path": "fermeture"},
    ]

    async def _tests(_session, _state):
        return test_cases

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _tests)
    _no_llm_guard(monkeypatch)

    out = await generate_single_suggestion(_paths_state(test_cases, "fermeture"))
    assert out["target_path"] == "all"
    assert out["suggestion_intent"] is True


@pytest.mark.asyncio
async def test_batch_falls_to_llm_once_branches_and_assembly_covered(monkeypatch):
    """Branches + assemblage couverts → suggestion LLM contextuelle (cas limites)."""
    test_cases = [
        {"test_index": "1", "target_path": "ouverture"},
        {"test_index": "2", "target_path": "fermeture"},
        {"test_index": "3", "target_path": "all"},
    ]

    async def _tests(_session, _state):
        return test_cases

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _tests)
    _patch_llm(monkeypatch, text="Vérifie le format de date en sortie")

    out = await generate_single_suggestion(_paths_state(test_cases, "all"))
    assert out.get("target_path") is None  # pas de focalisation déterministe imposée
    assert out["input"] == "Vérifie le format de date en sortie"
