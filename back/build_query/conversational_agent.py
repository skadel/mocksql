import json
import logging
import uuid

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_history_from_state, get_message_type

logger = logging.getLogger(__name__)


def _format_debug_message(msg: BaseMessage) -> BaseMessage:
    """Return a copy of a DEBUG_RUN_CTE message with human-readable content."""
    if get_message_type(msg) != MsgType.DEBUG_RUN_CTE:
        return msg
    try:
        data = json.loads(msg.content)
    except Exception:
        return msg

    cte = data.get("cte_name", "?")
    if data.get("error"):
        formatted = f"[run_cte] {cte} → erreur : {data['error']}"
    else:
        rows = data.get("rows", [])
        row_count = data.get("row_count", 0)
        col_filter = data.get("column")
        header = f'[run_cte] CTE "{cte}"'
        if col_filter:
            header += f" (colonne : {col_filter})"
        header += f" — {row_count} ligne(s)"
        if not rows:
            formatted = header + " : vide"
        else:
            headers = list(rows[0].keys())
            sep = " | "
            col_line = sep.join(headers)
            row_lines = [
                sep.join(str(r.get(h, "")) for h in headers) for r in rows[:15]
            ]
            formatted = "\n".join([header, col_line, "-" * len(col_line)] + row_lines)
            if row_count > 15:
                formatted += f"\n  … {row_count - 15} lignes supplémentaires"

    return AIMessage(
        content=formatted,
        id=msg.id,
        additional_kwargs=msg.additional_kwargs,
    )


async def conversational_agent(state: QueryState):
    """Conversational LLM agent: responds naturally and can call generate_test or delete_test."""
    logger.diag(
        "[conv_agent] entrée — evaluation_feedback=%s gen_retries=%s input=%r",
        state.get("evaluation_feedback"),
        state.get("gen_retries"),
        (state.get("input") or "")[:60],
    )
    existing_tests = await retrieve_existing_tests(state["session"], state)
    tests_summary = (
        "\n".join(
            f"[{t.get('test_uid', '?')}] {t.get('test_name', '')} — {t.get('unit_test_description', '')}"
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
            eval_verdict_text = (
                latest_eval.additional_kwargs.get("diag") or latest_eval.content
            )
            retries_left = state.get("gen_retries", 0)

            # Find the failing test case to expose its data to the agent
            failing_test = next(
                (
                    t
                    for t in existing_tests
                    if str(t.get("test_index")) == str(eval_test_idx)
                ),
                None,
            )
            test_data_block = ""
            if failing_test:
                input_data = failing_test.get("data", {})
                results_json = failing_test.get("results_json", "[]")
                assertion_results = failing_test.get("assertion_results", [])

                if input_data:
                    try:
                        input_summary = json.dumps(
                            input_data, ensure_ascii=False, indent=2
                        )
                    except Exception:
                        input_summary = str(input_data)
                    test_data_block += f"\n\nDonnées d'entrée du test {eval_test_idx} :\n```json\n{input_summary}\n```"

                if results_json and results_json != "[]":
                    try:
                        parsed_results = (
                            json.loads(results_json)
                            if isinstance(results_json, str)
                            else results_json
                        )
                        results_summary = json.dumps(
                            parsed_results[:10], ensure_ascii=False, indent=2
                        )
                    except Exception:
                        results_summary = str(results_json)[:500]
                    test_data_block += (
                        f"\n\nSortie DuckDB obtenue :\n```json\n{results_summary}\n```"
                    )
                else:
                    test_data_block += "\n\nSortie DuckDB obtenue : **vide (0 lignes)**"

                if assertion_results:
                    failing_assertions = [
                        a for a in assertion_results if a.get("status") != "pass"
                    ]
                    if failing_assertions:
                        try:
                            assertions_summary = json.dumps(
                                failing_assertions, ensure_ascii=False, indent=2
                            )
                        except Exception:
                            assertions_summary = str(failing_assertions)[:500]
                        test_data_block += f"\n\nAssertions en échec :\n```json\n{assertions_summary}\n```"

            failing_uid = (failing_test or {}).get("test_uid", str(eval_test_idx))
            test_name = (failing_test or {}).get("unit_test_description", "")
            test_name_line = f"\nScénario : {test_name}" if test_name else ""
            eval_context = f"""

⚠️ CONTEXTE AUTOMATIQUE — Correction du test [{failing_uid}]{test_name_line}

**Ce qui a été généré (données d'entrée injectées dans DuckDB) :**{test_data_block}

**Ce que l'évaluateur a conclu :**
{eval_verdict_text}

Tentatives de correction restantes : {retries_left}

**Outils disponibles :**
- `run_cte` — inspecter le contenu réel (valeurs) d'une CTE intermédiaire
- `update_test_data` — corriger les données d'entrée avec une instruction précise
- `request_reevaluation` — demander une réévaluation LLM si les données sont correctes (ex : 0 ligne est le comportement attendu pour ce scénario)

⚠️ Règle impérative : si la cause est évidente d'après le diagnostic ci-dessus, appelle directement `update_test_data`. Utilise `run_cte` uniquement pour inspecter les valeurs d'une CTE si nécessaire. Appelle `request_reevaluation` si le comportement est intentionnel. Ne demande pas de confirmation."""

    ctes = json.loads(state.get("query_decomposed") or "[]")
    cte_names = [c["name"] for c in ctes]
    cte_names_str = ", ".join(f'"{n}"' for n in cte_names) if cte_names else "aucune"

    debug_retries = state.get("debug_retries") or 0
    debug_budget_note = f"\nRounds de debug restants : {debug_retries}." + (
        " Tu ne peux plus appeler run_cte — prends une décision (demander une précision à l'utilisateur ou regénérer le test)."
        if debug_retries == 0
        else ""
    )

    system_content = f"""Tu es un assistant expert en tests SQL pour MockSQL.

SQL testé (dialecte {state.get("dialect", "bigquery")}):
{state.get("optimized_sql") or state.get("query", "")}

Étapes inspectables avec run_cte / count_cte_steps : {cte_names_str}

Tests existants :
{tests_summary}

Tu peux répondre aux questions sur la couverture, analyser les redondances,
et utiliser les outils disponibles pour générer ou supprimer des tests.
Pour toute suppression, demande toujours confirmation dans ta réponse AVANT d'appeler delete_test.
Réponds en français, de manière concise et naturelle.{debug_budget_note}{eval_context}"""

    # Build uid→test lookup (test_uid is assigned by retrieve_existing_tests above)
    uid_to_test: dict = {t["test_uid"]: t for t in existing_tests if t.get("test_uid")}

    # Tools that reference a specific test by test_uid and need uid validation
    _UID_TOOLS = {
        "delete_test",
        "update_test_description",
        "update_test_data",
        "run_cte",
        "request_reevaluation",
    }

    @tool
    def generate_test_data(scenario: str) -> str:
        """Génère un nouveau test pour le scénario décrit en langage naturel."""
        return scenario

    @tool
    def delete_test(test_uid: str) -> str:
        """Supprime le test identifié par test_uid (ex: 'a3f9c2').
        Utilise l'identifiant court visible dans la liste des tests existants."""
        return test_uid

    @tool
    def update_test_data(test_uid: str, instruction: str) -> str:
        """Corrige les données d'entrée d'un test existant identifié par test_uid.
        instruction : décrit la correction à apporter aux données (ex: 'les montants doivent être positifs').
        Utilise cet outil quand les données d'entrée sont incorrectes ou insuffisantes."""
        return f"{test_uid}:{instruction}"

    @tool
    def update_test_description(
        test_uid: str, new_name: str = "", new_description: str = ""
    ) -> str:
        """Met à jour le nom et/ou la description d'un test existant identifié par test_uid.
        new_name : nouveau titre du test (laisser vide pour ne pas modifier).
        new_description : nouvelle description (laisser vide pour ne pas modifier)."""
        return f"{test_uid}:{new_name}:{new_description}"

    @tool
    def generate_suggestions(instructions: str = "") -> str:
        """Génère des suggestions de cas de tests non encore couverts. Appelle cet outil pour proposer
        des scénarios à l'utilisateur, notamment après une génération de tests ou quand il demande
        quoi tester ensuite. Le paramètre instructions est optionnel : tu peux y préciser un axe
        particulier (ex : 'focus sur les cas NULL', 'insiste sur les valeurs limites')."""
        return instructions

    @tool
    def run_cte(test_uid: str, cte_name: str, column: str = "") -> str:
        """Exécute la requête SQL jusqu'à la CTE nommée avec les données du test et retourne les lignes réelles.
        Utilise cet outil pour inspecter les valeurs d'une CTE intermédiaire ou finale.
        column est optionnel : si fourni, ne sélectionne que cette colonne (ex : 'revenue')."""
        return f"{test_uid}:{cte_name}:{column}"

    @tool
    def request_reevaluation(test_uid: str, reason: str) -> str:
        """Demande une réévaluation LLM du test quand le diagnostic montre que les données
        d'entrée sont correctes et que l'évaluation initiale était erronée.
        Utilise cet outil quand le comportement observé (ex : 0 ligne retournée) est
        intentionnel et cohérent avec le scénario décrit (ex : cas plage vide, jointure sans résultat attendu).
        reason : justification courte expliquant pourquoi le comportement est correct."""
        return f"{test_uid}:{reason}"

    base_tools = [
        generate_test_data,
        delete_test,
        update_test_data,
        update_test_description,
        generate_suggestions,
        request_reevaluation,
    ]
    debug_tools = [run_cte] if debug_retries > 0 else []
    llm = make_llm().bind_tools(base_tools + debug_tools)
    history = get_history_from_state(
        state,
        msg_type=[
            MsgType.QUERY,
            MsgType.OTHER,
            MsgType.RESULTS,
            MsgType.EXAMPLES,
            MsgType.DEBUG_RUN_CTE,
        ],
    )
    user_input = state.get("input", "")
    formatted_history = [_format_debug_message(m) for m in history]
    messages_for_llm = [SystemMessage(content=system_content)] + formatted_history
    if user_input:
        messages_for_llm = messages_for_llm + [HumanMessage(content=user_input)]
    elif evaluation_feedback == "bad_data":
        # Déclenchement automatique : aucune saisie utilisateur, l'agent vient de l'évaluateur
        failing_uid_trigger = next(
            (
                t.get("test_uid", str(eval_test_idx))
                for t in existing_tests
                if str(t.get("test_index")) == str(eval_test_idx)
            ),
            str(eval_test_idx),
        )
        has_debug_results = any(
            get_message_type(m) in (MsgType.DEBUG_RUN_CTE, MsgType.DEBUG_COUNT_STEPS)
            for m in history
        )
        if has_debug_results:
            trigger = (
                f"Le diagnostic est terminé — les résultats sont visibles ci-dessus. "
                f"Analyse : si les données d'entrée sont incorrectes, appelle `update_test_data` sur [{failing_uid_trigger}]. "
                f"Si au contraire le comportement observé (ex : 0 ligne) est le comportement attendu pour ce scénario, "
                f"appelle `request_reevaluation` sur [{failing_uid_trigger}] avec la justification."
            )
        else:
            trigger = (
                f"Le test [{failing_uid_trigger}] a été jugé Insuffisant à cause des données d'entrée. "
                f"Le diagnostic CTE est disponible dans le contexte ci-dessus. "
                f"Si la cause est évidente, appelle directement `update_test_data`. "
                f"Si le comportement observé est en réalité attendu pour ce scénario, appelle `request_reevaluation`. "
                f"Utilise `run_cte` uniquement si tu as besoin d'inspecter les valeurs réelles d'une CTE spécifique."
            )
        messages_for_llm = messages_for_llm + [HumanMessage(content=trigger)]

    _DEBUG_TOOLS = {"run_cte"}

    agent_tool_call: str | None = None
    agent_tool_args: dict = {}
    new_input = state.get("input", "")
    result = None
    _UID_RETRY_MAX = 2
    uid_retries = 0

    logger.diag("[conv_agent] PROMPT SYSTEM (extrait):\n%s", system_content[:2000])
    logger.diag(
        "[conv_agent] messages_for_llm: %d msgs — dernier:\n%s",
        len(messages_for_llm),
        messages_for_llm[-1].content[:500] if messages_for_llm else "(vide)",
    )

    while True:
        result = await llm.ainvoke(messages_for_llm)
        tool_calls = getattr(result, "tool_calls", [])
        logger.diag(
            "[conv_agent] LLM → tool_calls=%s content=%r",
            [f"{tc['name']}({list(tc.get('args', {}).keys())})" for tc in tool_calls]
            or "(aucun)",
            (result.content or "")[:200],
        )

        if not tool_calls:
            logger.diag("[conv_agent] LLM n'a appelé aucun outil → réponse texte libre")
            break

        # Separate debug calls (can be batched) from action calls (take only first)
        pending_debug_calls = []
        first_action_tc = None

        for tc in tool_calls:
            if tc["name"] in _DEBUG_TOOLS:
                args = dict(tc["args"])
                uid = args.get("test_uid", "")
                if uid and uid in uid_to_test:
                    args["test_index"] = uid_to_test[uid]["test_index"]
                elif uid:
                    continue  # silently skip invalid uid in batch mode
                pending_debug_calls.append({"tool": tc["name"], "args": args})
            elif first_action_tc is None and tc["name"] not in _DEBUG_TOOLS:
                first_action_tc = tc

        # If there are debug calls, batch them and skip action processing this round
        if pending_debug_calls:
            agent_tool_call = "debug_batch"
            agent_tool_args = {"calls": pending_debug_calls}
            break

        if first_action_tc is None:
            break

        tc_name: str = first_action_tc["name"]
        tc_args: dict = dict(first_action_tc["args"])

        # Validate test_uid for tools that target a specific test
        if tc_name in _UID_TOOLS:
            uid = tc_args.get("test_uid", "")
            if uid and uid not in uid_to_test:
                if uid_retries < _UID_RETRY_MAX:
                    uid_retries += 1
                    logger.diag(
                        "[conv_agent] uid=%r inconnu — retry %d/%d",
                        uid,
                        uid_retries,
                        _UID_RETRY_MAX,
                    )
                    available = (
                        ", ".join(
                            f"{t['test_uid']} ({t.get('test_name', '?')})"
                            for t in existing_tests
                            if t.get("test_uid")
                        )
                        or "aucun"
                    )
                    error_feedback = (
                        f"L'identifiant de test '{uid}' n'existe pas. "
                        f"IDs disponibles : {available}"
                    )
                    messages_for_llm = messages_for_llm + [
                        result,
                        HumanMessage(content=error_feedback),
                    ]
                    continue
                # Exhausted retries → treat as no-op
                break

        # Resolve test_uid → test_index so downstream nodes need no change
        if tc_name in _UID_TOOLS:
            uid = tc_args.get("test_uid", "")
            if uid and uid in uid_to_test:
                tc_args["test_index"] = uid_to_test[uid]["test_index"]

        agent_tool_call = tc_name
        agent_tool_args = tc_args
        logger.diag("[conv_agent] outil sélectionné: %s args=%s", tc_name, tc_args)
        if tc_name == "generate_test_data":
            new_input = tc_args.get("scenario", new_input)
        elif tc_name == "update_test_data":
            new_input = tc_args.get("instruction", new_input)
        break

    # When triggered automatically after executor (bad_data), parent is the last message
    if evaluation_feedback == "bad_data" and state.get("messages"):
        parent = state["messages"][-1].id
    else:
        parent = state.get("user_message_id")

    update: dict = {
        "agent_tool_call": agent_tool_call,
        "agent_tool_args": agent_tool_args,
        "input": new_input,
    }
    if agent_tool_call == "request_reevaluation":
        update["gen_retries"] = -1
        update["reevaluation_context"] = agent_tool_args.get("reason", "")
        if "test_index" in agent_tool_args:
            update["test_index"] = agent_tool_args["test_index"]

    msgs_to_add = []
    last_msg_id = parent

    if agent_tool_call in ("generate_test_data", "update_test_data"):
        scenario = (
            agent_tool_args.get("scenario")
            or agent_tool_args.get("instruction")
            or new_input
        )
        scenario_msg = AIMessage(
            content=scenario,
            id=str(uuid.uuid4()),
            additional_kwargs={
                "type": MsgType.GENERATE_TEST_SCENARIO,
                "parent": last_msg_id,
                "request_id": state.get("request_id"),
            },
        )
        msgs_to_add.append(scenario_msg)
        last_msg_id = scenario_msg.id
        update["agent_message_id"] = scenario_msg.id

    # Gemini with bind_tools may return content as a list of parts instead of a plain string
    raw_content = result.content
    if isinstance(raw_content, list):
        raw_content = "".join(
            part.get("text", "") if isinstance(part, dict) else ""
            for part in raw_content
            if isinstance(part, dict) and part.get("type") == "text"
        )

    if raw_content:
        msgs_to_add.append(
            AIMessage(
                content=raw_content,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.OTHER,
                    "parent": last_msg_id,
                    "request_id": state.get("request_id"),
                },
            )
        )

    if msgs_to_add:
        update["messages"] = msgs_to_add

    return update
