import json
import uuid

from langchain_core.messages import AIMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from build_query.examples_executor import run_on_examples
from build_query.examples_generator import generate_examples
from build_query.test_evaluator import evaluate_tests
from build_query.profile_checker import _normalize_profile
from build_query.routing import routing
from build_query.state import QueryState
from models.env_variables import GENERATOR_MODEL
from models.message_service import get_messages_history
from storage.test_repository import get_test
from utils.msg_types import MsgType
from utils.saver import history_saver, get_history_from_state


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

    stored_sql = (test.get("sql") or "").strip()
    stored_optimised_sql = (test.get("optimized_sql") or "").strip()
    stored_used_columns = test.get("used_columns") or []

    if incoming_query and stored_sql != incoming_query:
        print("not validated query")
        return {}

    if not stored_used_columns:
        return {"validated_sql": stored_sql, "optimized_sql": stored_optimised_sql}

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
    }


async def _handle_other(state: QueryState):
    """Respond to off-topic user questions using the data analyst prompt."""
    from build_query.prompt_tools import build_other_prompt
    from utils.llm_errors import (
        is_vertex_permission_error,
        format_vertex_permission_message,
    )

    llm = ChatGoogleGenerativeAI(model=GENERATOR_MODEL, vertexai=True, temperature=0)
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
            error_msg = format_vertex_permission_message(GENERATOR_MODEL)
            return {
                "messages": [
                    AIMessage(
                        content=error_msg,
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.ERROR,
                            "parent": state.get("parent_message_id"),
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
                    "type": MsgType.OTHER,
                    "parent": state.get("parent_message_id"),
                    "request_id": state.get("request_id"),
                },
            )
        ]
    }


def build_query_graph():
    from langgraph.graph import END, StateGraph, START

    builder = StateGraph(QueryState)

    builder.add_node("pre_routing", pre_routing)
    builder.add_node("routing", routing)
    builder.add_node("generator", generate_examples)
    builder.add_node("executor", run_on_examples)
    builder.add_node("test_evaluator", evaluate_tests)
    builder.add_node("history_saver", history_saver)
    builder.add_node("other", _handle_other)

    def route_input(state: QueryState):
        if state.get("error"):
            return "history_saver"
        route = state.get("route", "").lower()
        if "executor" in route:
            return "executor"
        if route == "other":
            return "other"
        if len(state.get("used_columns", [])) == 0:
            return "executor"
        return "generator"

    def route_executor(state: QueryState):
        if state.get("error"):
            return "history_saver"
        return "test_evaluator"

    def route_evaluator(state: QueryState):
        if (
            state.get("status") == "empty_results"
            and (state.get("gen_retries") or 0) > 0
        ):
            return "generator"
        return "history_saver"

    builder.add_edge(START, "pre_routing")
    builder.add_edge("pre_routing", "routing")
    builder.add_conditional_edges("routing", route_input)
    builder.add_edge("generator", "executor")
    builder.add_conditional_edges("executor", route_executor)
    builder.add_conditional_edges("test_evaluator", route_evaluator)
    builder.add_edge("other", "history_saver")
    builder.add_edge("history_saver", END)

    graph = builder.compile()
    return graph
