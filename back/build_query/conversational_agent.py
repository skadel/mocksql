import json
import uuid

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool

from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state, get_message_type


async def conversational_agent(state: QueryState):
    """Conversational LLM agent: responds naturally and can call generate_test or delete_test."""
    existing_tests = await retrieve_existing_tests(state["session"], state)
    tests_summary = (
        "\n".join(
            f"Test {t.get('test_index')}: {t.get('test_name', '')} — {t.get('unit_test_description', '')}"
            for t in existing_tests
        )
        or "Aucun test pour l'instant."
    )

    # Contexte injecté quand l'agent est appelé après un verdict "bad_data" de l'évaluateur
    evaluation_feedback = state.get("evaluation_feedback")
    eval_context = ""
    eval_test_idx = None
    if evaluation_feedback == "bad_data":
        eval_msgs = [
            m
            for m in state.get("messages", [])
            if get_message_type(m) == MsgType.EVALUATION
        ]
        if eval_msgs:
            latest_eval = eval_msgs[-1]
            eval_test_idx = latest_eval.additional_kwargs.get("test_index")
            eval_verdict_text = latest_eval.content
            retries_left = state.get("gen_retries", 0)

            # Find the failing test case to expose its data to the agent
            failing_test = next(
                (t for t in existing_tests if str(t.get("test_index")) == str(eval_test_idx)),
                None,
            )
            test_data_block = ""
            if failing_test:
                input_data = failing_test.get("data", {})
                results_json = failing_test.get("results_json", "[]")
                assertion_results = failing_test.get("assertion_results", [])

                if input_data:
                    try:
                        input_summary = json.dumps(input_data, ensure_ascii=False, indent=2)
                    except Exception:
                        input_summary = str(input_data)
                    test_data_block += f"\n\nDonnées d'entrée du test {eval_test_idx} :\n```json\n{input_summary}\n```"

                if results_json and results_json != "[]":
                    try:
                        parsed_results = json.loads(results_json) if isinstance(results_json, str) else results_json
                        results_summary = json.dumps(parsed_results[:10], ensure_ascii=False, indent=2)
                    except Exception:
                        results_summary = str(results_json)[:500]
                    test_data_block += f"\n\nSortie DuckDB obtenue :\n```json\n{results_summary}\n```"
                else:
                    test_data_block += f"\n\nSortie DuckDB obtenue : **vide (0 lignes)**"

                if assertion_results:
                    failing_assertions = [a for a in assertion_results if a.get("status") != "pass"]
                    if failing_assertions:
                        try:
                            assertions_summary = json.dumps(failing_assertions, ensure_ascii=False, indent=2)
                        except Exception:
                            assertions_summary = str(failing_assertions)[:500]
                        test_data_block += f"\n\nAssertions en échec :\n```json\n{assertions_summary}\n```"

            eval_context = f"""

⚠️ CONTEXTE AUTOMATIQUE — Le test {eval_test_idx} a été jugé **Insuffisant à cause des données d'entrée**.
Verdict de l'évaluateur : {eval_verdict_text}
Tentatives de correction restantes : {retries_left}{test_data_block}

Ta mission : analyser pourquoi les données d'entrée posent problème, puis les corriger.
Tu as à ta disposition :
- `count_cte_steps` pour diagnostiquer où les données bloquent dans les CTEs
- `run_cte` pour inspecter le contenu d'une CTE intermédiaire
- `generate_test` pour regénérer le test avec des données corrigées et une instruction précise

Choisis librement l'outil le plus adapté à ce que tu observes dans les données du test."""

    ctes = json.loads(state.get("query_decomposed") or "[]")
    cte_names = [c["name"] for c in ctes]
    cte_names_str = ", ".join(f'"{n}"' for n in cte_names) if cte_names else "aucune"

    system_content = f"""Tu es un assistant expert en tests SQL pour MockSQL.

SQL testé (dialecte {state.get("dialect", "bigquery")}):
{state.get("optimized_sql") or state.get("query", "")}

Étapes inspectables avec run_cte / count_cte_steps : {cte_names_str}

Tests existants :
{tests_summary}

Tu peux répondre aux questions sur la couverture, analyser les redondances,
et utiliser les outils disponibles pour générer ou supprimer des tests.
Pour toute suppression, demande toujours confirmation dans ta réponse AVANT d'appeler delete_test.
Réponds en français, de manière concise et naturelle.{eval_context}"""

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

    @tool
    def run_cte(test_index: int, cte_name: str, column: str = "") -> str:
        """Exécute la requête SQL jusqu'à la CTE nommée avec les données du test et retourne les lignes.
        Utilise cet outil pour voir ce que contient une CTE intermédiaire ou finale.
        column est optionnel : si fourni, ne sélectionne que cette colonne (ex : 'revenue')."""
        return f"{test_index}:{cte_name}:{column}"

    @tool
    def count_cte_steps(test_index: int, cte_name: str) -> str:
        """Analyse pas à pas le nombre de lignes survivant à chaque JOIN et chaque condition WHERE
        d'une CTE, via une seule requête DuckDB avec des CASE WHEN cumulatifs.
        Utilise cet outil pour diagnostiquer pourquoi une CTE retourne 0 ligne."""
        return f"{test_index}:{cte_name}"

    llm = make_llm().bind_tools(
        [generate_test, delete_test, generate_suggestions, run_cte, count_cte_steps]
    )
    history = get_history_from_state(
        state,
        msg_type=[
            MsgType.QUERY,
            MsgType.OTHER,
            MsgType.RESULTS,
            MsgType.EXAMPLES,
            MsgType.DEBUG_RUN_CTE,
            MsgType.DEBUG_COUNT_STEPS,
        ],
    )
    user_input = state.get("input", "")
    messages = [SystemMessage(content=system_content)] + history
    if user_input:
        messages = messages + [HumanMessage(content=user_input)]
    elif evaluation_feedback == "bad_data":
        # Déclenchement automatique : aucune saisie utilisateur, l'agent vient de l'évaluateur
        trigger = (
            f"Le test {eval_test_idx} a été jugé Insuffisant à cause des données d'entrée. "
            "Analyse le problème et prends les mesures nécessaires pour le corriger."
        )
        messages = messages + [HumanMessage(content=trigger)]

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
        elif agent_tool_call == "run_cte":
            # test_index, cte_name, column passed to debug_node via agent_tool_args
            pass
        elif agent_tool_call == "count_cte_steps":
            # test_index, cte_name passed to debug_node via agent_tool_args
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
