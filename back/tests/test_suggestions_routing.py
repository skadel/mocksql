"""Tests de routage des suggestions après le passage à un panneau dédié.

Comportement visé :
  - les suggestions ne sont auto-générées qu'à la **1ʳᵉ génération** (0 → N tests),
    plus à chaque édition/ajout (sinon : appel LLM inutile + pollution du fil) ;
  - un bouton « Régénérer » du panneau régénère à la demande via le flag
    ``regenerate_suggestions`` → ``routing`` court-circuite l'agent et va droit au
    générateur de suggestions, sans message de clôture trompeur dans le fil.
"""

import pytest

from build_query.routing import routing
from build_query.query_chain import route_evaluator, route_after_suggestions


# --- route_evaluator : auto-suggestions seulement à la 1ʳᵉ génération ----------


def test_first_generation_routes_to_suggestions():
    """Aucun test existant → on suggère une fois."""
    state = {"evaluation_feedback": None, "has_existing_tests": False, "gen_retries": 2}
    assert route_evaluator(state) == "suggestions_generator"


def test_existing_tests_skip_suggestions():
    """Des tests existent déjà → pas de suggestions auto, on clôture directement."""
    state = {"evaluation_feedback": None, "has_existing_tests": True, "gen_retries": 2}
    assert route_evaluator(state) == "final_response"


def test_bad_data_still_takes_priority_over_gating():
    """Le gating ne court-circuite pas la boucle de correction bad_data."""
    state = {
        "evaluation_feedback": "bad_data",
        "has_existing_tests": False,
        "gen_retries": 2,
    }
    assert route_evaluator(state) == "bad_data_to_agent"


# --- route_after_suggestions : régénération = pas de message de clôture --------


def test_regenerate_skips_final_response():
    assert route_after_suggestions({"regenerate_suggestions": True}) == "history_saver"


def test_normal_generation_keeps_final_response():
    assert route_after_suggestions({}) == "final_response"


# --- routing : le bouton « Régénérer » court-circuite l'agent ------------------


@pytest.mark.asyncio
async def test_regenerate_flag_routes_to_suggestions():
    """regenerate_suggestions a priorité, même avec des tests existants + input."""
    state = {
        "regenerate_suggestions": True,
        "has_existing_tests": True,
        "input": "",
        "user_message_id": "umsg-1",
        "parent_message_id": "pmsg-1",
        "request_id": "req-1",
    }
    result = await routing(state)
    assert result["route"] == "suggestions"
