"""Mise à jour de description = TOUJOURS une proposition, jamais appliquée d'office.

Scénario produit : l'utilisateur clique sur une suggestion ; l'agent conversationnel
détecte qu'un test existant ressemble à sa demande. Il ne doit PAS réécrire la description
en silence — il la PROPOSE (propose_description_node), l'utilisateur valide
(apply_description) ou refuse (reject_description) depuis le panneau.

Cf. build_query/description_proposal.py, query_chain.route_agent_output, routing.routing.
"""

import json
from unittest.mock import patch

import pytest

from build_query.description_proposal import (
    apply_description,
    propose_description_node,
    reject_description,
)
from build_query.query_chain import route_agent_output
from build_query.routing import routing
from utils.msg_types import MsgType


def test_route_agent_output_update_description_goes_to_proposal():
    """update_test_description ne route JAMAIS vers une application directe."""
    assert (
        route_agent_output({"agent_tool_call": "update_test_description"})
        == "propose_description_node"
    )


@pytest.mark.asyncio
async def test_routing_apply_intent():
    assert (await routing({"apply_description_intent": True}))[
        "route"
    ] == "apply_description"


@pytest.mark.asyncio
async def test_routing_reject_intent():
    assert (await routing({"reject_description_intent": True}))[
        "route"
    ] == "reject_description"


@pytest.mark.asyncio
async def test_propose_stores_proposal_without_mutating_description():
    """propose_description_node enregistre proposed_* SANS toucher la vraie description."""
    target = {
        "test_index": "3",
        "test_name": "Titre actuel",
        "unit_test_description": "Description actuelle.",
    }
    stored = {"test_cases": [target]}
    captured: dict = {}

    with (
        patch("build_query.description_proposal.get_test", return_value=stored),
        patch(
            "build_query.description_proposal.update_test",
            side_effect=lambda _s, u: captured.update(u),
        ),
    ):
        result = await propose_description_node(
            {
                "session": "s",
                "parent_message_id": "p",
                "agent_tool_args": {
                    "test_index": "3",
                    "new_name": "Nouveau titre",
                    "new_description": "Nouvelle description.",
                    "reason": "Le test n°3 couvre déjà ce cas.",
                },
            }
        )

    saved = captured["test_cases"][0]
    # La proposition est posée…
    assert saved["proposed_name"] == "Nouveau titre"
    assert saved["proposed_description"] == "Nouvelle description."
    # …mais la description/titre réels NE changent pas tant que l'utilisateur n'a pas validé.
    assert saved["test_name"] == "Titre actuel"
    assert saved["unit_test_description"] == "Description actuelle."

    proposal = result["messages"][0]
    assert proposal.additional_kwargs["type"] == MsgType.UPDATE_TEST_PROPOSAL
    payload = json.loads(proposal.content)
    assert payload["new_description"] == "Nouvelle description."
    assert payload["reason"] == "Le test n°3 couvre déjà ce cas."


@pytest.mark.asyncio
async def test_apply_description_applies_and_clears_proposal():
    """apply_description écrit la proposition sur le test et la consomme."""
    target = {
        "test_index": "3",
        "test_name": "Titre actuel",
        "unit_test_description": "Description actuelle.",
        "proposed_name": "Nouveau titre",
        "proposed_description": "Nouvelle description.",
    }
    stored = {"test_cases": [target]}
    captured: dict = {}

    with (
        patch("build_query.description_proposal.get_test", return_value=stored),
        patch(
            "build_query.description_proposal.update_test",
            side_effect=lambda _s, u: captured.update(u),
        ),
    ):
        result = await apply_description(
            {"session": "s", "test_index": "3", "parent_message_id": "p"}
        )

    saved = captured["test_cases"][0]
    assert saved["test_name"] == "Nouveau titre"
    assert saved["unit_test_description"] == "Nouvelle description."
    assert "proposed_name" not in saved
    assert "proposed_description" not in saved

    msg = result["messages"][0]
    assert msg.additional_kwargs["type"] == MsgType.UPDATE_TEST


@pytest.mark.asyncio
async def test_apply_description_noop_without_pending_proposal():
    """Sans proposition en attente, apply_description ne fait rien (pas de message)."""
    stored = {"test_cases": [{"test_index": "3", "unit_test_description": "x"}]}
    with (
        patch("build_query.description_proposal.get_test", return_value=stored),
        patch("build_query.description_proposal.update_test"),
    ):
        result = await apply_description({"session": "s", "test_index": "3"})
    assert result == {}


@pytest.mark.asyncio
async def test_reject_description_clears_proposal():
    """reject_description efface la proposition (sans toucher la description réelle)."""
    target = {
        "test_index": "3",
        "unit_test_description": "Description actuelle.",
        "proposed_description": "Nouvelle description.",
    }
    stored = {"test_cases": [target]}
    captured: dict = {}

    with (
        patch("build_query.description_proposal.get_test", return_value=stored),
        patch(
            "build_query.description_proposal.update_test",
            side_effect=lambda _s, u: captured.update(u),
        ),
    ):
        await reject_description({"session": "s", "test_index": "3"})

    saved = captured["test_cases"][0]
    assert "proposed_description" not in saved
    assert saved["unit_test_description"] == "Description actuelle."
