from langchain_core.messages import HumanMessage, SystemMessage

from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state


async def conv_reasoning_node(state: QueryState):
    """Generates reasoning about what conversational action to take. No tool calls — pure text stream."""
    existing_tests = await retrieve_existing_tests(state["session"], state)
    tests_summary = (
        "\n".join(
            f"Test {t.get('test_index')}: {t.get('test_name', '')} — {t.get('unit_test_description', '')}"
            for t in existing_tests
        )
        or "Aucun test pour l'instant."
    )

    system_content = f"""Tu es un assistant expert en tests SQL pour MockSQL.

SQL testé (dialecte {state.get("dialect", "bigquery")}):
{state.get("optimized_sql") or state.get("query", "")}

Tests existants :
{tests_summary}

L'utilisateur fait une demande. Analyse-la brièvement (2-3 phrases) :
- Ce qu'il veut faire
- Pourquoi ce scénario est pertinent ou manquant dans la couverture actuelle
- L'action que tu vas prendre (générer un test, supprimer, suggérer, répondre…)

Sois concis et direct. Réponds en français."""

    llm = make_llm()
    history = get_history_from_state(
        state,
        msg_type=[MsgType.QUERY, MsgType.OTHER, MsgType.RESULTS, MsgType.EXAMPLES],
    )
    user_input = state.get("input", "")
    messages = [SystemMessage(content=system_content)] + history
    if user_input:
        messages = messages + [HumanMessage(content=user_input)]

    result = await llm.ainvoke(messages)

    raw_content = result.content
    if isinstance(raw_content, list):
        raw_content = "".join(
            part.get("text", "") if isinstance(part, dict) else ""
            for part in raw_content
            if isinstance(part, dict) and part.get("type") == "text"
        )

    return {"conv_reasoning": raw_content}
