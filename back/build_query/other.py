import uuid

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate

from build_query.prompt_tools import build_other_prompt
from build_query.state import QueryState
from common_vars import get_table_details
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state


async def other(state: QueryState):
    """Solves the query using the given input"""
    history = get_history_from_state(
        state,
        msg_type=[
            MsgType.SQL,
            MsgType.QUERY,
            MsgType.REASONING,
            MsgType.DATA_RESULTS,
            MsgType.RESULTS,
            MsgType.PROVIDED_SQL,
            MsgType.OTHER,
        ],
    )

    template: ChatPromptTemplate = build_other_prompt(
        history=history, user_input=state["input"], dialect=state["dialect"]
    )
    llm = make_llm()

    runnable = template | llm | StrOutputParser()

    llm_response: str = await runnable.ainvoke(
        {"descriptions": await get_table_details(state["project"])}
    )
    parent_message_id = (
        state["parent_message_id"] if state["parent_message_id"] != "" else None
    )

    messages = [
        HumanMessage(
            state["input"],
            id=state["user_message_id"],
            additional_kwargs={"type": MsgType.QUERY, "parent": parent_message_id},
        ),
        AIMessage(
            content=llm_response,
            id=str(uuid.uuid4()),
            additional_kwargs={
                "type": MsgType.OTHER,
                "parent": state["user_message_id"],
            },
        ),
    ]

    return {"messages": messages}
