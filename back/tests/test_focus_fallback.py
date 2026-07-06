"""Fallback runtime focus → all (test-before-fix).

Un test FOCALISÉ (target_path != "all") qui ne converge pas — désync
description↔sortie (bad_description / needs_validation / bad_input_description)
ou bad_data à retries épuisés — est régénéré UNE fois en target_path="all"
(chemin structurellement sûr : toutes les tables sources peuplées).

Principe (décision produit, incident sf_bq012 spider2-snow) : le critère de
validité d'un test focalisé — « la sortie du script complet colle à la
description » — est MESURÉ par l'executor+évaluateur ; on ne le prédit pas
statiquement (toute heuristique SQL sur-apprend le corpus d'éval, cf. régression
sf_bq091 de la règle « agrégat en aval »). Le focus garde sa valeur partout où il
converge ; on ne dégrade que sur échec constaté.
"""

import asyncio

from build_query.examples_generator import (
    _should_regenerate,
    create_appropriate_prompt,
)
from build_query.query_chain import _focus_fallback, route_evaluator


# ---------------------------------------------------------------------------
# route_evaluator : sorties non-convergentes d'un test focalisé → focus_fallback
# ---------------------------------------------------------------------------


def test_bad_description_focused_routes_to_fallback():
    """Désync valeur de sortie sur un test focalisé (signature sf_bq012 : la
    compensation inter-branches fausse le scénario mono-branche) → fallback."""
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "focus_fallback"


def test_needs_validation_focused_routes_to_fallback():
    state = {
        "evaluation_feedback": "needs_validation",
        "target_path": "trace_inflows",
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "focus_fallback"


def test_bad_input_description_focused_routes_to_fallback():
    state = {
        "evaluation_feedback": "bad_input_description",
        "target_path": "trace_inflows",
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "focus_fallback"


def test_bad_data_exhausted_focused_routes_to_fallback():
    """Retries incrémentaux épuisés sur un test focalisé → dernier recours = regen all
    (plutôt que bad_data_exhausted → sauvegarde d'un test KO)."""
    state = {
        "evaluation_feedback": "bad_data",
        "target_path": "trace_inflows",
        "gen_retries": 0,
    }
    assert route_evaluator(state) == "focus_fallback"


def test_bad_data_with_retries_keeps_agent_loop():
    """Tant qu'il reste des retries, la boucle de correction incrémentale garde la
    priorité (le fallback n'est qu'un dernier recours)."""
    state = {
        "evaluation_feedback": "bad_data",
        "target_path": "trace_inflows",
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "bad_data_to_agent"


# ---------------------------------------------------------------------------
# Gardes : une seule fois, jamais sans focus, jamais sur relance lecture-seule
# ---------------------------------------------------------------------------


def test_fallback_fires_only_once_bad_description():
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "focus_fallback_used": True,
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "history_saver"


def test_fallback_fires_only_once_bad_data():
    state = {
        "evaluation_feedback": "bad_data",
        "target_path": "trace_inflows",
        "focus_fallback_used": True,
        "gen_retries": 0,
    }
    assert route_evaluator(state) == "bad_data_exhausted"


def test_all_target_path_keeps_legacy_route():
    """Test non focalisé (all) : comportement historique inchangé."""
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "all",
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "history_saver"


def test_absent_target_path_keeps_legacy_route():
    state = {"evaluation_feedback": "bad_description", "gen_retries": 2}
    assert route_evaluator(state) == "history_saver"


def test_rerun_only_stays_readonly_even_focused():
    """Relance lecture-seule : jamais de régénération, même focalisée non convergente
    (cf. project_rerun_empty_sentinel : user_rerun = lecture seule)."""
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "rerun_only": True,
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "history_saver"


def test_verdict_absent_never_triggers_fallback():
    """Aucun feedback explicite (évaluateur silencieux / circuit breaker) → flux nominal,
    pas de fallback : ne jamais régénérer un test que rien ne condamne (sf_bq091)."""
    state = {
        "evaluation_feedback": None,
        "target_path": "apps_with_meta",
        "gen_retries": 2,
        "has_existing_tests": True,
    }
    assert route_evaluator(state) == "final_response"


# ---------------------------------------------------------------------------
# Nœud focus_fallback : pose l'état de régénération all
# ---------------------------------------------------------------------------


def test_node_resets_focus_to_all():
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "gen_retries": 0,
    }
    out = asyncio.run(_focus_fallback(state))
    assert out["target_path"] == "all"
    assert out["focus_fallback_used"] is True
    assert out["focus_fallback"] is True  # trigger one-shot du régénérateur
    assert out["gen_retries"] >= 1  # budget de retry restauré pour le test all


def test_node_targets_evaluated_test_for_replacement():
    """Le regen doit REMPLACER le test focalisé non convergent, pas s'ajouter à côté :
    le nœud relaie le test_index du dernier message EVALUATION (seul endroit où
    l'évaluateur consigne l'identité du test évalué) vers _resolve_target_key."""
    from langchain_core.messages import AIMessage

    from utils.msg_types import MsgType

    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "gen_retries": 0,
        "messages": [
            AIMessage(
                content="**Insuffisant** — désync",
                additional_kwargs={"type": MsgType.EVALUATION, "test_index": "1"},
            )
        ],
    }
    out = asyncio.run(_focus_fallback(state))
    assert out["test_index"] == "1"


def test_node_without_evaluation_message_sets_no_target():
    state = {
        "evaluation_feedback": "bad_description",
        "target_path": "trace_inflows",
        "gen_retries": 0,
        "messages": [],
    }
    out = asyncio.run(_focus_fallback(state))
    assert "test_index" not in out


# ---------------------------------------------------------------------------
# _should_regenerate : honore le trigger du fallback
# ---------------------------------------------------------------------------


def test_should_regenerate_honors_fallback_flag():
    """Sans input ni changement de colonnes, avec des tests existants,
    _should_regenerate rend False — sauf si le fallback vient d'être posé."""
    existing = [{"test_index": "1", "test_uid": "abcd"}]
    assert _should_regenerate({}, existing) is False
    assert _should_regenerate({"focus_fallback": True}, existing) is True


# ---------------------------------------------------------------------------
# create_appropriate_prompt : le fallback doit produire un prompt de régénération
# (état fallback = tests existants + pas d'input + status complete → tombait dans
# le `else: return None` → l'executor rejouait l'ancien test focalisé à l'identique)
# ---------------------------------------------------------------------------


def test_prompt_router_regenerates_on_fallback():
    state = {
        "optimized_sql": "SELECT 1 AS x",
        "dialect": "duckdb",
        "focus_fallback": True,
        "target_path": "all",
        "status": "complete",
    }
    prompt = asyncio.run(
        create_appropriate_prompt(state, [{"test_index": "1"}], [], [], "{}")
    )
    assert prompt is not None


def test_prompt_router_still_none_without_trigger():
    """Hors fallback, l'état « tests existants sans input ni empty_results » reste
    un no-op de génération (l'executor recharge les tests existants)."""
    state = {
        "optimized_sql": "SELECT 1 AS x",
        "dialect": "duckdb",
        "status": "complete",
    }
    prompt = asyncio.run(
        create_appropriate_prompt(state, [{"test_index": "1"}], [], [], "{}")
    )
    assert prompt is None
