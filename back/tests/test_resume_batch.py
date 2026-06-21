"""Reprise d'une boucle multi-tests interrompue.

Scénario utilisateur : l'ingénieur demande N tests, une coupure (réseau, crash LLM) survient
après K < N tests déjà checkpointés sur disque. Un simple re-run de la MÊME requête doit
construire les N-K tests manquants — sans repartir de zéro (ce qui dupliquerait le nominal)
ni clore direct via final_response (ce que faisait l'ancien chemin has_existing_tests).

Pièces couvertes :
- ``_should_resume_batch`` : décide si ce run est une reprise (pur, sur le dict du test).
- ``route_evaluator`` : continue la boucle sous resume_batch malgré has_existing_tests=True.
- ``routing`` : court-circuite vers le route "resume_batch".
- ``pre_routing`` : persiste tests_target tôt + seede les champs de reprise (intégration).
"""

import pytest

from build_query.query_chain import _should_resume_batch, route_evaluator
from build_query.routing import routing


# --- _should_resume_batch -----------------------------------------------------


def _test_file(target=3, n_cases=1):
    return {
        "tests_target": target,
        "test_cases": [{"test_index": i} for i in range(n_cases)],
        "sql": "SELECT 1",
    }


def test_resume_when_batch_incomplete():
    resume, target, existing = _should_resume_batch({}, _test_file(target=3, n_cases=1))
    assert (resume, target, existing) == (True, 3, 1)


def test_no_resume_when_batch_complete():
    resume, *_ = _should_resume_batch({}, _test_file(target=3, n_cases=3))
    assert resume is False


def test_no_resume_without_target():
    resume, *_ = _should_resume_batch({}, {"test_cases": [{"test_index": 0}]})
    assert resume is False  # target défaut 1, existing 1 → pas < 1


def test_no_resume_when_no_tests_yet():
    """1ʳᵉ génération (aucun test sur disque) n'est pas une reprise."""
    resume, *_ = _should_resume_batch({}, {"tests_target": 3, "test_cases": []})
    assert resume is False


@pytest.mark.parametrize(
    "flag",
    [
        "input",
        "user_tables",
        "suggestion_intent",
        "assertion_only",
        "rerun_only",
        "regenerate_suggestions",
        "validate_intent",
        "rerun_all_tests",
    ],
)
def test_no_resume_when_competing_intent(flag):
    """Toute intention concurrente (chat, suggestion, validation, rerun) prime la reprise."""
    resume, *_ = _should_resume_batch({flag: "x"}, _test_file(target=3, n_cases=1))
    assert resume is False


# --- route_evaluator : la boucle continue sous resume_batch -------------------


def _eval_state(target, built, resume):
    return {
        "evaluation_feedback": None,
        "has_existing_tests": True,  # des tests existent déjà sur disque (reprise)
        "gen_retries": 2,
        "tests_target": target,
        "auto_tests_built": built,
        "resume_batch": resume,
    }


def test_resume_loops_until_target():
    # 3 tests voulus, 1 extra déjà construit (auto_tests_built=1) → encore 1 à faire.
    assert route_evaluator(_eval_state(3, 1, True)) == "generate_single_suggestion"
    # auto_tests_built atteint target-1 → on clôt via le panneau de suggestions.
    assert route_evaluator(_eval_state(3, 2, True)) == "suggestions_generator"


def test_without_resume_existing_tests_close_directly():
    """Sans resume_batch, des tests existants → final_response (comportement édition)."""
    assert route_evaluator(_eval_state(3, 1, False)) == "final_response"


def test_resume_bad_data_still_prioritized():
    state = _eval_state(3, 1, True)
    state["evaluation_feedback"] = "bad_data"
    assert route_evaluator(state) == "bad_data_to_agent"


# --- routing : court-circuit resume_batch -------------------------------------


@pytest.mark.asyncio
async def test_routing_short_circuits_on_resume_batch():
    out = await routing({"resume_batch": True})
    assert out == {"route": "resume_batch"}


# --- pre_routing : persistance précoce + seeding (intégration) ----------------


@pytest.mark.asyncio
async def test_pre_routing_seeds_resume_fields(monkeypatch):
    """SQL inchangé + tests existants en nombre insuffisant → pre_routing pose resume_batch,
    tests_target et auto_tests_built (= existing-1) pour reprendre la boucle."""
    from build_query import query_chain

    stored = {
        "model_name": "m",
        "tests_target": 3,
        "test_cases": [{"test_index": 0}, {"test_index": 1}],  # 2 sur 3
        "sql": "SELECT 1",
        "optimized_sql": "SELECT 1",
        "used_columns": [{"project": "p", "table": "t", "used_columns": ["a"]}],
        "query_decomposed": "{}",
        "path_plans": None,
    }
    monkeypatch.setattr(query_chain, "get_test", lambda _s: stored)
    monkeypatch.setattr(query_chain, "update_test", lambda *a, **k: None)
    monkeypatch.setattr(query_chain, "load_model_context", lambda _n: None)

    async def _no_history(**_k):
        return []

    monkeypatch.setattr(query_chain, "get_messages_history", _no_history)
    monkeypatch.setattr(query_chain, "_normalize_profile", lambda _p: {})
    import models.schemas as schemas

    monkeypatch.setattr(schemas, "get_profile", lambda: {})

    state = {
        "session": "s1",
        "query": "SELECT 1",
        "dialect": "bigquery",
        "parent_message_id": "",
        "tests_target": 3,
    }
    out = await query_chain.pre_routing(state)

    assert out["resume_batch"] is True
    assert out["tests_target"] == 3
    assert out["auto_tests_built"] == 1  # 2 tests sur disque → 1 extra déjà construit


@pytest.mark.asyncio
async def test_pre_routing_persists_target_on_first_gen(monkeypatch):
    """1ʳᵉ génération (aucun test) avec N>1 → tests_target persisté AVANT toute construction."""
    from build_query import query_chain

    persisted = {}
    stored = {"model_name": "m", "test_cases": [], "sql": "", "query_decomposed": "{}"}
    monkeypatch.setattr(query_chain, "get_test", lambda _s: stored)
    monkeypatch.setattr(
        query_chain, "update_test", lambda _s, fields: persisted.update(fields)
    )
    monkeypatch.setattr(query_chain, "load_model_context", lambda _n: None)

    state = {
        "session": "s1",
        "query": "SELECT new",
        "dialect": "bigquery",
        "parent_message_id": "",
        "tests_target": 3,
    }
    out = await query_chain.pre_routing(state)

    assert persisted.get("tests_target") == 3
    # SQL entrant ≠ stocké (vide) → re-validation, pas de reprise sur ce 1ᵉʳ tour.
    assert not out.get("resume_batch")
