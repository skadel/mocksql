import uuid

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool

from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state


async def conversational_agent(state: QueryState):
    """Conversational LLM agent: responds naturally and can call generate_test or delete_test."""
    existing_tests = await retrieve_existing_tests(state["session"], state)
    tests_summary = "\n".join(
        f"Test {t.get('test_index')}: {t.get('test_name', '')} — {t.get('unit_test_description', '')}"
        for t in existing_tests
    ) or "Aucun test pour l'instant."

    system_content = f"""Tu es un assistant expert en tests SQL pour MockSQL.

SQL testé (dialecte {state.get('dialect', 'bigquery')}):
{state.get('optimized_sql') or state.get('query', '')}

Tests existants :
{tests_summary}

Tu peux répondre aux questions sur la couverture, analyser les redondances,
et utiliser les outils disponibles pour générer ou supprimer des tests.
Pour toute suppression, demande toujours confirmation dans ta réponse AVANT d'appeler delete_test.
Réponds en français, de manière concise et naturelle."""

    @tool
    def generate_test(scenario: str) -> str:
        """Génère un nouveau test pour le scénario décrit en langage naturel."""
        return scenario

    @tool
    def delete_test(test_index: int) -> str:
        """Supprime le test à l'index donné (valeur du champ test_index du test)."""
        return str(test_index)

    @tool
    def generate_suggestions(instructions: str = "") -> str:
        """Génère des suggestions de cas de tests non encore couverts. Appelle cet outil pour proposer
        des scénarios à l'utilisateur, notamment après une génération de tests ou quand il demande
        quoi tester ensuite. Le paramètre instructions est optionnel : tu peux y préciser un axe
        particulier (ex : 'focus sur les cas NULL', 'insiste sur les valeurs limites')."""
        return instructions

    llm = make_llm().bind_tools([generate_test, delete_test, generate_suggestions])
    history = get_history_from_state(
        state,
        msg_type=[MsgType.QUERY, MsgType.OTHER, MsgType.RESULTS, MsgType.EXAMPLES],
    )
    user_input = state.get("input", "")
    messages = [SystemMessage(content=system_content)] + history
    if user_input:
        messages = messages + [HumanMessage(content=user_input)]

    result = await llm.ainvoke(messages)
    tool_calls = getattr(result, "tool_calls", [])

    agent_tool_call = None
    agent_tool_args: dict = {}
    new_input = state.get("input", "")

    if tool_calls:
        tc = tool_calls[0]
        agent_tool_call = tc["name"]
        agent_tool_args = tc["args"]
        if agent_tool_call == "generate_test":
            new_input = agent_tool_args.get("scenario", new_input)
        elif agent_tool_call == "generate_suggestions":
            # instructions stored in agent_tool_args, picked up by suggestions_node
            pass

    update: dict = {
        "agent_tool_call": agent_tool_call,
        "agent_tool_args": agent_tool_args,
        "input": new_input,
    }

    if agent_tool_call == "generate_test":
        scenario = agent_tool_args.get("scenario", new_input)
        scenario_msg = AIMessage(
            content=scenario,
            id=str(uuid.uuid4()),
            additional_kwargs={
                "type": MsgType.GENERATE_TEST_SCENARIO,
                "parent": state.get("user_message_id"),
                "request_id": state.get("request_id"),
            },
        )
        update["messages"] = update.get("messages", []) + [scenario_msg]

    # Gemini with bind_tools may return content as a list of parts instead of a plain string
    raw_content = result.content
    if isinstance(raw_content, list):
        raw_content = "".join(
            part.get("text", "") if isinstance(part, dict) else ""
            for part in raw_content
            if isinstance(part, dict) and part.get("type") == "text"
        )

    if raw_content:
        update["messages"] = [
            AIMessage(
                content=raw_content,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.OTHER,
                    "parent": state.get("user_message_id"),
                    "request_id": state.get("request_id"),
                },
            )
        ]

    return update
