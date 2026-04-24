from langchain_core.messages import HumanMessage
from langchain_core.output_parsers import JsonOutputParser
from langchain_google_genai import ChatGoogleGenerativeAI

from build_query.prompt_tools import make_routing_prompt
from build_query.state import QueryState
from models.env_variables import GENERATOR_MODEL
from utils.msg_types import MsgType
from utils.saver import get_history_from_state, common_history_retriever

_llm = ChatGoogleGenerativeAI(model=GENERATOR_MODEL, vertexai=True, temperature=0)


async def routing(state: QueryState):
    """
    Simplified routing:
    - profile_result provided → profile_checker (store and check coverage)
    - user_tables provided → executor (re-run tests with custom data)
    - query provided → parser → generator (parse SQL, then generate tests)
    - chat input only → classify intent; if off-topic → other, else → generator
    """
    profile_result = state.get("profile_result")
    if profile_result:
        return {"route": "profile_checker"}

    user_tables = state.get("user_tables")
    if user_tables:
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

    input_text = state.get("input", "").strip()
    messages = []

    # Demande manuelle de correction d'erreur : charger l'historique pour le fixer
    if input_text == "__fix_error__":
        error_history = await common_history_retriever(
            session_id=state["session"],
            last_message_id=state.get("parent_message_id") or None,
            msg_type=[MsgType.ERROR_SQL, MsgType.ERROR, MsgType.SQL],
        )
        return {"route": "fixer", "messages": error_history}

    # When only user text is provided (no new SQL), classify intent with LLM
    # Skip classification when test_index is set: it's always a test modification
    if input_text:
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
        granularity="fine",
        dialect=state.get("dialect", ""),
        history=history,
    )
    chain = prompt | _llm | JsonOutputParser()
    try:
        result = await chain.ainvoke({"input": input_text})
        return result.get("route", "generator")
    except Exception:
        return "generator"
