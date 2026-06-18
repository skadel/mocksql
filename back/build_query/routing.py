import logging

from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import JsonOutputParser

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.prompt_tools import make_routing_prompt
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state, common_history_retriever

logger = logging.getLogger(__name__)

_llm_cache = None


def _get_llm():
    """Instancie le LLM à la demande (pas à l'import) — évite d'exiger des
    credentials Google au simple chargement du module (collecte pytest en CI)."""
    global _llm_cache
    if _llm_cache is None:
        _llm_cache = make_llm()
    return _llm_cache


async def routing(state: QueryState):
    """
    Simplified routing:
    - profile_result provided → profile_checker (store and check coverage)
    - user_tables provided → executor (re-run tests with custom data)
    - query provided → parser → generator (parse SQL, then generate tests)
    - chat input only → classify intent; if off-topic → other, else → generator
    """
    # Clic « Je valide l'état actuel » sur un test needs_validation : action déterministe,
    # court-circuite le routage LLM → réalignement description + verdict Bon (accept_validation).
    if state.get("validate_intent"):
        logger.diag("[routing] → accept_validation (validate_intent présent)")
        return {"route": "accept_validation"}

    profile_result = state.get("profile_result")
    if profile_result:
        logger.diag("[routing] → profile_checker (profile_result présent)")
        return {"route": "profile_checker"}

    # Régénération explicite des suggestions (bouton du panneau) : court-circuite l'agent
    # conversationnel pour aller directement régénérer, sans appel LLM de routage.
    if state.get("regenerate_suggestions"):
        logger.diag("[routing] → suggestions (regenerate_suggestions présent)")
        return {"route": "suggestions"}

    user_tables = state.get("user_tables")
    if user_tables:
        logger.diag("[routing] → executor (user_tables présent)")
        return {
            "route": "executor",
            "examples": [
                HumanMessage(
                    content=user_tables,
                    id=state["user_message_id"],
                    additional_kwargs={
                        "type": MsgType.USER_EXAMPLES,
                        "parent": state["parent_message_id"],
                        "request_id": state.get("request_id"),
                    },
                )
            ],
        }

    # Assertion-only mode: user wants to modify assertion metadata without regenerating data
    if state.get("assertion_only"):
        logger.diag("[routing] → assertion_modifier (assertion_only)")
        input_text = state.get("input", "").strip()
        messages = []
        if input_text:
            messages.append(
                HumanMessage(
                    content=input_text,
                    id=state["user_message_id"],
                    additional_kwargs={
                        "type": MsgType.EXAMPLES_INSTRUCTION,
                        "parent": state["parent_message_id"],
                        "request_id": state.get("request_id"),
                    },
                )
            )
        return {"route": "assertion_modifier", "messages": messages}

    input_text = state.get("input", "").strip()
    messages = []

    # Demande manuelle de correction d'erreur : charger l'historique pour le fixer
    if input_text == "__fix_error__":
        logger.diag("[routing] → fixer (__fix_error__ reçu)")
        error_history = await common_history_retriever(
            session_id=state["session"],
            last_message_id=state.get("parent_message_id") or None,
            msg_type=[MsgType.ERROR_SQL, MsgType.ERROR, MsgType.SQL],
        )
        return {"route": "fixer", "messages": error_history}

    # When existing tests are present, route all natural language input to the conversational agent.
    # Inclut le clic sur une suggestion (suggestion_intent) : l'agent peut alors détecter qu'elle
    # recoupe un test existant et l'étendre plutôt que créer un doublon. Le garde-fou anti-no-op
    # (cf. route_agent_output) garantit qu'un test sort quand même si l'agent ne produit rien.
    if (
        input_text
        and state.get("has_existing_tests")
        and not state.get("rerun_all_tests")
    ):
        logger.diag("[routing] → conversational_agent (tests existants + input texte)")
        messages.append(
            HumanMessage(
                content=input_text,
                id=state["user_message_id"],
                additional_kwargs={
                    "type": MsgType.QUERY,
                    "parent": state["parent_message_id"],
                    "request_id": state.get("request_id"),
                },
            )
        )
        return {"route": "conversational_agent", "messages": messages}

    # When only user text is provided (no new SQL), classify intent with LLM
    # Skip classification when test_index is set: it's always a test modification
    if input_text:
        if state.get("route") == "generator":
            detected_route = "generator"
        else:
            detected_route = await _classify_intent(state, input_text)
        if detected_route == "other":
            messages.append(
                HumanMessage(
                    content=input_text,
                    id=state["user_message_id"],
                    additional_kwargs={
                        "type": MsgType.QUERY,
                        "parent": state["parent_message_id"],
                        "request_id": state.get("request_id"),
                    },
                )
            )
            return {"route": "other", "messages": messages}
        else:
            messages.append(
                HumanMessage(
                    content=input_text,
                    id=state["user_message_id"],
                    additional_kwargs={
                        "type": MsgType.EXAMPLES_INSTRUCTION,
                        "parent": state["parent_message_id"],
                        "request_id": state.get("request_id"),
                    },
                )
            )

    return {
        "route": "generator",
        "messages": messages,
    }


async def _classify_intent(state: QueryState, input_text: str) -> str:
    """Use LLM fine-grained router to detect off-topic messages."""
    history = get_history_from_state(
        state,
        msg_type=[
            MsgType.SQL,
            MsgType.QUERY,
            MsgType.EXAMPLES,
            MsgType.RESULTS,
            MsgType.REASONING,
        ],
    )
    prompt = make_routing_prompt(
        dialect=state.get("dialect", ""),
        history=history,
    )
    chain = prompt | _get_llm() | JsonOutputParser()
    try:
        result = await chain.ainvoke({"input": input_text})
        detected = result.get("route", "generator")
        logger.diag(
            "[routing] _classify_intent → %s (input=%r)", detected, input_text[:80]
        )
        return detected
    except Exception as exc:
        logger.diag(
            "[routing] _classify_intent exception → fallback generator: %s", exc
        )
        return "generator"
