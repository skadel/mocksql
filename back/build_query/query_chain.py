import json
import logging
import uuid

import utils.logger  # noqa: F401 — registers DIAG level (15)
from langchain_core.messages import AIMessage
from build_query.assertion_corrector import correct_assertions
from build_query.assertion_modifier import modify_assertions
from build_query.accept_validation import accept_validation
from build_query.conversational_agent import conversational_agent
from build_query.data_patcher import data_patcher_node
from build_query.debug_node import debug_test_node
from build_query.delete_test_node import delete_test_node
from build_query.update_test_node import update_test_node
from build_query.assertion_generator import generate_assertions
from build_query.final_response_node import final_response
from build_query.examples_executor import run_on_examples
from build_query.examples_generator import generate_examples
from build_query.suggestions_node import generate_suggestions
from build_query.test_evaluator import evaluate_tests
from build_query.profile_checker import _normalize_profile
from build_query.routing import routing
from build_query.state import QueryState
from models.message_service import get_messages_history
from utils.llm_factory import make_llm
from storage.config import get_llm_model
from storage.context_loader import load_model_context
from storage.test_repository import get_test, update_test
from utils.msg_types import MsgType
from utils.saver import history_saver, get_history_from_state

logger = logging.getLogger(__name__)


def _lightweight_query_decomposed(sql: str, dialect: str) -> str:
    """Derive query_decomposed from SQL (sqlglot only, no BigQuery call).
    Produces {name, code} entries sufficient for debug_executor.
    Used as a fallback for old tests that predate query_decomposed persistence."""
    try:
        import sqlglot

        statements = sqlglot.parse(sql, read=dialect)
        final_ast = next(
            (
                s
                for s in statements
                if isinstance(s, (sqlglot.exp.Query, sqlglot.exp.With))
            ),
            None,
        )
        if final_ast is None:
            return "[]"
        ctes = []
        with_clause = final_ast.ctes
        if with_clause:
            for cte in with_clause:
                ctes.append(
                    {
                        "name": cte.alias_or_name,
                        "code": cte.this.sql(dialect=dialect, pretty=True),
                        "dependencies": [],
                        "sources": [],
                    }
                )
        final_ast.ctes.clear()
        final_code = final_ast.sql(dialect=dialect, pretty=True)

        # Extract inline subqueries (FROM (SELECT ...) AS alias) as inspectable steps
        existing_names = {c["name"] for c in ctes}
        for node in final_ast.walk():
            if (
                isinstance(node, sqlglot.exp.Subquery)
                and node.alias
                and node.alias not in existing_names
            ):
                existing_names.add(node.alias)
                ctes.append(
                    {
                        "name": node.alias,
                        "code": node.this.sql(dialect=dialect, pretty=True),
                        "dependencies": [],
                        "sources": [],
                    }
                )

        ctes.append(
            {
                "name": "final_query",
                "code": final_code,
                "dependencies": [],
                "sources": [],
            }
        )
        return json.dumps(ctes)
    except Exception as exc:
        print(f"[pre_routing] _lightweight_query_decomposed failed: {exc}")
        return "[]"


async def _bad_data_to_agent(state: QueryState):
    """Entry into the automatic bad_data correction loop, routed to the
    conversational_agent.

    Sets ``auto_correct`` so the agent takes its auto-correction branch even if a
    stale ``input`` lingers in state. Does NOT decrement ``gen_retries`` — the agent
    decrements it itself on the bad_data path (see conversational_agent).

    Also completes the ``outcome`` of the last ``correction_attempts`` entry from
    the fresh diagnostic (one-line digest: blocking step), so the agent's next
    round sees « tentative N → sans effet » instead of rediscovering the problem."""
    update: dict = {"auto_correct": True}
    attempts = list(state.get("correction_attempts") or [])
    if attempts and attempts[-1].get("outcome") is None:
        from build_query.examples_generator import _get_failing_cte_from_results

        failing_cte, _trace = _get_failing_cte_from_results(state.get("messages", []))
        digest = (
            f"toujours 0 ligne — étape bloquante inchangée ({failing_cte})"
            if failing_cte
            else "verdict toujours Insuffisant (bad_data)"
        )
        attempts[-1] = {
            **attempts[-1],
            "outcome": {"blocking_cte": failing_cte, "digest": digest},
        }
        update["correction_attempts"] = attempts
    return update


async def _bad_data_exhausted(state: QueryState):
    """Signal the frontend that bad_data retries are exhausted — show retry button."""
    return {
        "messages": [
            AIMessage(
                content="",
                id=str(uuid.uuid4()),
                additional_kwargs={
                    # Chaîner sous le dernier message du tour (évaluation/résultats) plutôt
                    # que sous parent_message_id, pour ne pas créer de branche sœur parasite
                    # avec un éventuel QUERY de ce tour. L'ancrage métier se fait par test_index.
                    "type": MsgType.RETRY_PROMPT,
                    "parent": state["messages"][-1].id
                    if state.get("messages")
                    else state.get("parent_message_id"),
                    "request_id": state.get("request_id"),
                    "test_index": state.get("test_index"),
                },
            )
        ]
    }


async def pre_routing(state: QueryState):
    """
    Load stored sql + used_columns from the test file.
    If the incoming query matches the stored one and the profile is complete,
    pre-populate state so that parser and/or profile_checker can be skipped.
    """
    incoming_query = state.get("query", "").strip()
    if not incoming_query:
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    model_context = load_model_context(test.get("model_name", ""))
    has_existing_tests = len(test.get("test_cases") or []) > 0
    stored_sql = (test.get("sql") or "").strip()
    stored_optimised_sql = (test.get("optimized_sql") or "").strip()
    stored_used_columns = test.get("used_columns") or []
    stored_query_decomposed = test.get("query_decomposed") or ""
    if not stored_query_decomposed and (stored_optimised_sql or stored_sql):
        stored_query_decomposed = _lightweight_query_decomposed(
            stored_optimised_sql or stored_sql, state.get("dialect", "bigquery")
        )
        update_test(state["session"], {"query_decomposed": stored_query_decomposed})

    if incoming_query and stored_sql != incoming_query:
        logger.diag("[pre_routing] SQL entrant ≠ SQL stocké → re-validation requise")
        return {
            "has_existing_tests": has_existing_tests,
            "model_context": model_context,
            "query_decomposed": stored_query_decomposed,
        }

    if not stored_used_columns:
        return {
            "validated_sql": stored_sql,
            "optimized_sql": stored_optimised_sql,
            "has_existing_tests": has_existing_tests,
            "model_context": model_context,
            "query_decomposed": stored_query_decomposed,
        }

    from models.schemas import get_profile

    profile = _normalize_profile(get_profile())
    history = await get_messages_history(
        session_id=state["session"], message_data_id=state["parent_message_id"]
    )

    return {
        "used_columns": stored_used_columns,
        "profile_complete": True,
        "profile": profile or {},
        "validated_sql": stored_sql,
        "optimized_sql": stored_optimised_sql,
        "history": history,
        "has_existing_tests": has_existing_tests,
        "model_context": model_context,
        "query_decomposed": stored_query_decomposed,
    }


async def _handle_other(state: QueryState):
    """Respond to off-topic user questions using the data analyst prompt."""
    from build_query.prompt_tools import build_other_prompt
    from utils.llm_errors import (
        is_vertex_permission_error,
        format_vertex_permission_message,
    )

    llm = make_llm()
    history = get_history_from_state(
        state,
        msg_type=[
            MsgType.QUERY,
            MsgType.SQL,
            MsgType.REASONING,
            MsgType.RESULTS,
            MsgType.OTHER,
        ],
    )
    user_input = state.get("input", "")
    dialect = state.get("dialect", "bigquery")
    schemas = state.get("schemas") or []
    descriptions = json.dumps(schemas)

    prompt = build_other_prompt(user_input, dialect, history)
    chain = prompt | llm
    try:
        result = await chain.ainvoke({"descriptions": descriptions})
    except Exception as exc:
        if is_vertex_permission_error(exc):
            error_msg = format_vertex_permission_message(get_llm_model())
            return {
                "messages": [
                    AIMessage(
                        content=error_msg,
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            # Même raison que la réponse OTHER ci-dessous : chaîner sous
                            # la question (user_message_id), pas en frère du QUERY.
                            "type": MsgType.ERROR,
                            "parent": state.get("user_message_id"),
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "error": "llm_permission_denied",
            }
        raise

    return {
        "messages": [
            AIMessage(
                content=result.content,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    # Chaîner SOUS la question (user_message_id), pas en frère :
                    # le message QUERY de l'utilisateur a déjà parent=parent_message_id
                    # (cf. routing.py). Si la réponse partageait ce même parent, question
                    # et réponse deviendraient des branches sœurs et la réponse tomberait
                    # sur une branche morte, invisible à get_messages_history (qui remonte
                    # la chaîne de parents) → le conversational_agent ne la verrait plus.
                    "type": MsgType.OTHER,
                    "parent": state.get("user_message_id"),
                    "request_id": state.get("request_id"),
                },
            )
        ]
    }


def route_agent_output(state: QueryState):
    """Route la sortie du conversational_agent selon l'outil qu'il a appelé.

    Défini au niveau module (et non imbriqué) pour rester testable : ne dépend que de
    ``state``. Le garde-fou final garantit qu'aucune intention actionnable (boucle bad_data
    OU clic sur suggestion) ne se termine en no-op `history_saver` — on retombe alors sur
    le `generator` pour qu'un test sorte quand même."""
    tool_call = state.get("agent_tool_call")
    logger.diag("[route_agent_output] agent_tool_call=%s", tool_call)
    if tool_call in (
        "patch_test_field",
        "remove_test_row",
        "add_test_row",
        "data_batch",
    ):
        return "data_patcher"
    if tool_call in ("generate_test_data", "update_test_data"):
        return "generator"
    if tool_call == "delete_test":
        return "delete_test_node"
    if tool_call == "update_test_description":
        return "update_test_node"
    if tool_call == "generate_suggestions":
        return "suggestions_generator"
    if tool_call in ("run_cte", "count_cte_steps", "debug_batch"):
        return "debug_node"
    if tool_call == "request_reevaluation":
        return "test_evaluator"
    if tool_call == "ask_clarification":
        logger.diag("[route_agent_output] → history_saver (ask_clarification)")
        return "history_saver"
    # Auto-correction loop (bad_data) : si l'agent n'a émis aucun outil actionnable,
    # ne pas tuer le retry par un history_saver — retomber sur la régénération
    # complète du generator. Garantit que le rebranch n'est jamais pire que le regen.
    # Idem pour un clic sur suggestion (suggestion_intent) : on garantit qu'un test
    # sort toujours, même si l'agent a répondu en texte libre au lieu d'agir.
    if (
        state.get("auto_correct")
        or state.get("evaluation_feedback") == "bad_data"
        or state.get("suggestion_intent")
    ):
        logger.diag(
            "[route_agent_output] aucun tool_call (bad_data/suggestion) → generator (fallback)"
        )
        return "generator"
    logger.diag("[route_agent_output] aucun tool_call actionnable → history_saver")
    return "history_saver"


def route_evaluator(state: QueryState):
    """Route la sortie de ``test_evaluator``. Défini au niveau module (comme
    ``route_agent_output``) pour rester testable : ne dépend que de ``state``.

    Les suggestions ne sont auto-générées qu'à la 1ʳᵉ génération (``has_existing_tests``
    falsy) ; sur éditions/ajouts suivants on va directement à ``final_response`` (cf. bouton
    « Régénérer » côté panneau)."""
    feedback = state.get("evaluation_feedback")
    retries = state.get("gen_retries", 0)
    logger.diag(
        "[route_evaluator] evaluation_feedback=%s gen_retries=%s assertion_only=%s",
        feedback,
        retries,
        state.get("assertion_only"),
    )
    # Skip retries and suggestions for assertion-only edits or simple reruns
    if state.get("assertion_only") or state.get("rerun_only"):
        logger.diag(
            "[route_evaluator] → history_saver (assertion_only=%s rerun_only=%s)",
            state.get("assertion_only"),
            state.get("rerun_only"),
        )
        return "history_saver"
    # SQL structurally requires too many rows — no retry can fix this
    if feedback == "too_many_rows":
        logger.diag("[route_evaluator] → history_saver (too_many_rows)")
        return "history_saver"
    # Désync description↔cardinalité (données valides) : pas de boucle — l'état est sauvé
    # et un VALIDATION_PROMPT a été émis ; on attend la décision de l'utilisateur.
    if feedback == "needs_validation":
        logger.diag("[route_evaluator] → history_saver (needs_validation)")
        return "history_saver"
    if feedback == "bad_data":
        if retries > 0:
            logger.diag(
                "[route_evaluator] → bad_data_to_agent (bad_data retries=%d)",
                retries,
            )
            return "bad_data_to_agent"
        logger.diag("[route_evaluator] → bad_data_exhausted (bad_data retries épuisés)")
        return "bad_data_exhausted"
    if feedback == "bad_assertions":
        if retries > 0:
            logger.diag(
                "[route_evaluator] → assertion_corrector (bad_assertions retries=%d)",
                retries,
            )
            return "assertion_corrector"
    # Suggestions auto-générées une seule fois : à la 1ʳᵉ génération (0 → N tests).
    if not state.get("has_existing_tests"):
        logger.diag(
            "[route_evaluator] → suggestions_generator (1ʳᵉ génération, feedback=%s)",
            feedback,
        )
        return "suggestions_generator"
    logger.diag(
        "[route_evaluator] → final_response (tests existants, pas de suggestions auto)"
    )
    return "final_response"


def route_after_suggestions(state: QueryState):
    """Sortie de ``suggestions_generator``. Régénération à la demande → ``history_saver``
    (pas de message de clôture « j'ai généré des tests », qui serait faux) ; flux normal de
    1ʳᵉ génération → ``final_response``. Module-level pour rester testable."""
    # Régénération à la demande : bouton du panneau (regenerate_suggestions) OU
    # l'agent conversationnel qui a appelé generate_suggestions. Dans les deux cas,
    # pas de message de clôture « j'ai généré un test » (faux : seules les
    # suggestions ont changé) — le panneau se rafraîchit via le message SSE.
    if (
        state.get("regenerate_suggestions")
        or state.get("agent_tool_call") == "generate_suggestions"
    ):
        logger.diag("[route_after_suggestions] → history_saver (régénération)")
        return "history_saver"
    logger.diag("[route_after_suggestions] → final_response")
    return "final_response"


def build_query_graph():
    from langgraph.graph import END, StateGraph, START
    from utils.timing import timed_node

    builder = StateGraph(QueryState)

    def add_timed_node(name, fn):
        """Enregistre un nœud en chronométrant son exécution (niveau DIAG)."""
        builder.add_node(name, timed_node(name, fn))

    add_timed_node("pre_routing", pre_routing)
    add_timed_node("routing", routing)
    add_timed_node("conversational_agent", conversational_agent)
    add_timed_node("data_patcher", data_patcher_node)
    add_timed_node("debug_node", debug_test_node)
    add_timed_node("delete_test_node", delete_test_node)
    add_timed_node("update_test_node", update_test_node)
    add_timed_node("accept_validation", accept_validation)
    add_timed_node("generator", generate_examples)
    add_timed_node("assertion_modifier", modify_assertions)
    add_timed_node("executor", run_on_examples)
    add_timed_node("assertion_generator", generate_assertions)
    add_timed_node("assertion_corrector", correct_assertions)
    add_timed_node("test_evaluator", evaluate_tests)
    add_timed_node("bad_data_to_agent", _bad_data_to_agent)
    add_timed_node("bad_data_exhausted", _bad_data_exhausted)
    add_timed_node("suggestions_generator", generate_suggestions)
    add_timed_node("final_response", final_response)
    add_timed_node("history_saver", history_saver)
    add_timed_node("other", _handle_other)

    def route_input(state: QueryState):
        if state.get("error"):
            logger.diag("[route_input] → history_saver (error=%s)", state.get("error"))
            return "history_saver"
        route = state.get("route", "").lower()
        if route == "accept_validation":
            logger.diag("[route_input] → accept_validation")
            return "accept_validation"
        if route == "conversational_agent":
            logger.diag("[route_input] → conversational_agent")
            return "conversational_agent"
        if route == "assertion_modifier":
            logger.diag("[route_input] → assertion_modifier")
            return "assertion_modifier"
        if "executor" in route:
            logger.diag("[route_input] → executor (route=%s)", route)
            return "executor"
        if route == "other":
            logger.diag("[route_input] → other")
            return "other"
        if route == "suggestions":
            logger.diag(
                "[route_input] → suggestions_generator (régénération à la demande)"
            )
            return "suggestions_generator"
        if len(state.get("used_columns", [])) == 0:
            logger.diag("[route_input] → executor (used_columns vides)")
            return "executor"
        logger.diag(
            "[route_input] → generator (%d used_columns)",
            len(state.get("used_columns", [])),
        )
        return "generator"

    def route_executor(state: QueryState):
        if state.get("error") or state.get("status") == "error":
            logger.diag(
                "[route_executor] → history_saver (error=%s status=%s)",
                state.get("error"),
                state.get("status"),
            )
            return "history_saver"
        status = state.get("status")
        # No results to evaluate — evaluator handles routing (retry or error)
        if status in ("empty_results", "bad_data_error"):
            logger.diag("[route_executor] → test_evaluator (status=%s)", status)
            return "test_evaluator"
        # rerun_all: verdicts already computed in executor (no LLM needed)
        if state.get("rerun_all_tests"):
            logger.diag("[route_executor] → test_evaluator (rerun_all)")
            return "test_evaluator"
        logger.diag("[route_executor] → assertion_generator (status=%s)", status)
        return "assertion_generator"

    builder.add_edge(START, "pre_routing")
    builder.add_edge("pre_routing", "routing")
    builder.add_conditional_edges("routing", route_input)
    builder.add_conditional_edges("conversational_agent", route_agent_output)
    # After debug, always let the agent decide: ask user or regenerate.
    # Debug tools are removed from the agent's toolset when debug_retries == 0 (safety).
    builder.add_edge("debug_node", "conversational_agent")
    builder.add_edge("delete_test_node", "history_saver")
    builder.add_edge("update_test_node", "history_saver")
    builder.add_edge("accept_validation", "history_saver")
    builder.add_edge("data_patcher", "executor")
    builder.add_edge("generator", "executor")
    builder.add_edge("assertion_modifier", "executor")
    builder.add_conditional_edges("executor", route_executor)
    builder.add_edge("assertion_generator", "test_evaluator")
    builder.add_conditional_edges("test_evaluator", route_evaluator)
    builder.add_edge("bad_data_to_agent", "conversational_agent")
    builder.add_edge("bad_data_exhausted", "history_saver")
    builder.add_edge("assertion_corrector", "test_evaluator")
    builder.add_conditional_edges("suggestions_generator", route_after_suggestions)
    builder.add_edge("final_response", "history_saver")
    builder.add_edge("other", "history_saver")
    builder.add_edge("history_saver", END)

    graph = builder.compile()
    return graph
