"""Proposition de mise à jour de la description d'un test (jamais appliquée d'office).

Le `conversational_agent` ne modifie JAMAIS directement le nom/la description d'un test :
quand il appelle `update_test_description`, on passe par ``propose_description_node``, qui
- stocke la description/le nom PROPOSÉS sur le test (champs ``proposed_*``, cachés/gitignorés),
- émet un message ``UPDATE_TEST_PROPOSAL`` qui dit à l'utilisateur quel test ressemble et ce
  qui est proposé.

L'utilisateur tranche ensuite depuis le panneau :
- « Appliquer » → ``apply_description`` (déterministe) écrit les champs proposés sur le test
  et émet un ``UPDATE_TEST`` (la carte affiche « Description mise à jour ») ;
- « Garder l'actuelle » → ``reject_description`` efface simplement la proposition.

Ce flux miroite ``accept_validation`` : une action déclenchée par un clic bouton est
déterministe (pas de l'agent), et survit au rechargement car l'état vit sur le test.
"""

import json
import logging
import uuid

from langchain_core.messages import AIMessage

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from storage.test_repository import get_test, update_test
from utils.msg_types import MsgType

logger = logging.getLogger(__name__)


def _parent_id(state: QueryState):
    return (
        state["messages"][-1].id
        if state.get("messages")
        else state.get("parent_message_id")
    )


async def propose_description_node(state: QueryState):
    """Enregistre une proposition de mise à jour de description (sans rien appliquer)."""
    args = state.get("agent_tool_args") or {}
    test_index = args.get(
        "test_index"
    )  # résolu depuis test_uid par conversational_agent
    new_name = (args.get("new_name") or "").strip()
    new_description = (args.get("new_description") or "").strip()
    reason = (args.get("reason") or "").strip()

    if test_index is None or (not new_name and not new_description):
        logger.diag("[propose_description] no-op (test_index/desc manquants)")
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    found = False
    updated_cases = []
    for c in test.get("test_cases") or []:
        if str(c.get("test_index")) == str(test_index):
            c = dict(c)
            # On ne touche PAS test_name / unit_test_description : seuls les champs
            # proposed_* portent la proposition tant que l'utilisateur n'a pas validé.
            if new_name:
                c["proposed_name"] = new_name
            if new_description:
                c["proposed_description"] = new_description
            found = True
        updated_cases.append(c)

    if not found:
        logger.diag("[propose_description] test_index=%s introuvable", test_index)
        return {}

    update_test(state["session"], {"test_cases": updated_cases})
    logger.diag(
        "[propose_description] proposition enregistrée pour test=%s", test_index
    )

    proposal_msg = AIMessage(
        content=json.dumps(
            {
                "test_index": test_index,
                "new_name": new_name,
                "new_description": new_description,
                "reason": reason,
            }
        ),
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.UPDATE_TEST_PROPOSAL,
            "parent": _parent_id(state),
            "request_id": state.get("request_id"),
            "test_index": test_index,
        },
    )
    return {"messages": [proposal_msg]}


async def apply_description(state: QueryState):
    """Applique la description proposée (clic « Appliquer » de l'utilisateur)."""
    test_index = state.get("test_index")
    if test_index is None:
        logger.warning("[apply_description] test_index absent — no-op")
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    target = next(
        (
            c
            for c in test.get("test_cases") or []
            if str(c.get("test_index")) == str(test_index)
        ),
        None,
    )
    if target is None:
        logger.warning("[apply_description] test_index=%s introuvable", test_index)
        return {}

    new_name = (target.get("proposed_name") or "").strip()
    new_description = (target.get("proposed_description") or "").strip()
    if not new_name and not new_description:
        logger.diag("[apply_description] aucune proposition en attente — no-op")
        return {}

    updated_cases = []
    for c in test.get("test_cases") or []:
        if str(c.get("test_index")) == str(test_index):
            c = dict(c)
            if new_name:
                c["test_name"] = new_name
            if new_description:
                c["unit_test_description"] = new_description
            c.pop("proposed_name", None)
            c.pop("proposed_description", None)
        updated_cases.append(c)

    update_test(state["session"], {"test_cases": updated_cases})
    logger.diag("[apply_description] description appliquée pour test=%s", test_index)

    return {
        "messages": [
            AIMessage(
                content=json.dumps(
                    {
                        "test_index": test_index,
                        "new_name": new_name,
                        "new_description": new_description,
                    }
                ),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.UPDATE_TEST,
                    "parent": _parent_id(state),
                    "request_id": state.get("request_id"),
                    "test_index": test_index,
                },
            )
        ]
    }


async def reject_description(state: QueryState):
    """Écarte la proposition de description (clic « Garder l'actuelle »)."""
    test_index = state.get("test_index")
    if test_index is None:
        logger.warning("[reject_description] test_index absent — no-op")
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    changed = False
    updated_cases = []
    for c in test.get("test_cases") or []:
        if str(c.get("test_index")) == str(test_index):
            c = dict(c)
            if c.pop("proposed_name", None) is not None:
                changed = True
            if c.pop("proposed_description", None) is not None:
                changed = True
        updated_cases.append(c)

    if changed:
        update_test(state["session"], {"test_cases": updated_cases})
        logger.diag("[reject_description] proposition écartée pour test=%s", test_index)

    return {}
