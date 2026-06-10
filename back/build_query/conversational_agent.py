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


def _format_data_indexed(data: dict) -> str:
    """Affiche les données d'un test avec les indices de lignes pour référencer [table][i]."""
    lines = []
    for table, rows in (data or {}).items():
        lines.append(f"Table {table}:")
        for i, row in enumerate(rows or []):
            try:
                lines.append(f"  [{i}] {json.dumps(row, ensure_ascii=False)}")
            except Exception:
                lines.append(f"  [{i}] {row!r}")
    return "\n".join(lines) if lines else "(aucune donnée)"


def _build_agent_eval_context(state: QueryState, existing_tests: list) -> tuple:
    """Construit le bloc de contexte injecté dans le prompt système du conversational_agent
    après un verdict ``bad_data``.

    Rassemble, pour la tentative échouée en cours (la dernière EVALUATION) :
    - le diagnostic structuré (cause racine, pattern SQL, problème dans les données,
      recette de correction, tables/CTEs concernées) s'il est présent, sinon le verdict brut ;
    - les données d'entrée injectées (indexées par ligne) ;
    - la sortie DuckDB obtenue (ou « vide ») ;
    - les assertions en échec.

    Retourne ``(eval_context, eval_test_idx)``. Si le feedback n'est pas ``bad_data`` ou
    qu'aucun message EVALUATION n'est présent, retourne ``("", None)``.
    """
    if state.get("evaluation_feedback") != "bad_data":
        return "", None

    eval_msgs = [
        m
        for m in state.get("messages", [])
        if get_message_type(m) == MsgType.EVALUATION
    ]
    if not eval_msgs:
        return "", None

    latest_eval = eval_msgs[-1]
    eval_test_idx = latest_eval.additional_kwargs.get("test_index")
    diag_struct = latest_eval.additional_kwargs.get("diagnostic")
    if diag_struct:
        eval_verdict_text = (
            f"**Cause racine :** {diag_struct['root_cause']}\n"
            f"**Pattern SQL :** {diag_struct['sql_pattern']}\n"
            f"**Problème dans les données :** {diag_struct['data_issue']}\n"
            f"**Correction attendue :** {diag_struct['fix_recipe']}\n"
            f"**Tables concernées :** {', '.join(diag_struct['affected_tables'])}\n"
            f"**CTEs concernées :** {', '.join(diag_struct['affected_ctes'])}"
        )
    else:
        eval_verdict_text = (
            latest_eval.additional_kwargs.get("diag") or latest_eval.content
        )
    retries_left = state.get("gen_retries", 0)

    # Find the failing test case to expose its data to the agent
    failing_test = next(
        (t for t in existing_tests if str(t.get("test_index")) == str(eval_test_idx)),
        None,
    )
    # Référence le test par son uid partout (c'est l'identifiant que l'agent
    # doit passer aux outils) — pas par test_index, pour éviter qu'il confonde
    # les deux et invente un identifiant.
    failing_uid = (failing_test or {}).get("test_uid", str(eval_test_idx))
    test_data_block = ""
    if failing_test:
        input_data = failing_test.get("data", {})
        results_json = failing_test.get("results_json", "[]")
        assertion_results = failing_test.get("assertion_results", [])

        if input_data:
            test_data_block += f"\n\nDonnées d'entrée du test [{failing_uid}] :\n{_format_data_indexed(input_data)}"

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
                test_data_block += (
                    f"\n\nAssertions en échec :\n```json\n{assertions_summary}\n```"
                )

    test_name = (failing_test or {}).get("unit_test_description", "")
    test_name_line = f"\nScénario : {test_name}" if test_name else ""
    eval_context = f"""

⚠️ CONTEXTE AUTOMATIQUE — Correction du test [{failing_uid}]{test_name_line}

**Ce qui a été généré (données d'entrée injectées dans DuckDB) :**{test_data_block}

**Ce que l'évaluateur a conclu :**
{eval_verdict_text}

Tentatives de correction restantes : {retries_left}

**Outils disponibles :**
- `run_cte` — inspecter une CTE intermédiaire (debug)
- `patch_test_field` — modifier un champ précis sur une ligne existante
- `remove_test_row` — supprimer une ligne par son indice
- `add_test_row` — ajouter une nouvelle ligne (génération LLM scopée)

⚡ Tu peux combiner `patch_test_field`, `remove_test_row` et `add_test_row` dans une même réponse :
   toutes les opérations seront appliquées dans l'ordre avant la ré-exécution.
   Exemple pour dupliquer une ligne : appelle `patch_test_field` sur les lignes [1] et [2]
   pour leur donner la même date que [0], sans appeler `add_test_row`.
   Préfère les patches sur des lignes existantes aux ajouts LLM quand c'est possible.
- `update_test_data` — régénérer complètement les données (si la correction est trop complexe)
- `request_reevaluation` — si le comportement observé est intentionnel

⚠️ Règle impérative : préfère les corrections chirurgicales groupées à la régénération complète. Utilise `update_test_data` seulement si la logique du scénario doit être refondée. Ne demande pas de confirmation."""

    return eval_context, eval_test_idx


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
    eval_context, eval_test_idx = _build_agent_eval_context(state, existing_tests)

    ctes = json.loads(state.get("query_decomposed") or "[]")
    cte_names = [c["name"] for c in ctes]
    cte_names_str = ", ".join(f'"{n}"' for n in cte_names) if cte_names else "aucune"

    debug_retries = state.get("debug_retries") or 0
    debug_budget_note = f"\nRounds de debug restants : {debug_retries}." + (
        " Tu ne peux plus appeler run_cte — prends une décision (demander une précision à l'utilisateur ou regénérer le test)."
        if debug_retries == 0
        else ""
    )

    # Clic sur une suggestion de couverture : l'utilisateur veut un test concret, pas une
    # réponse conversationnelle. On laisse à l'agent la latitude de dédupliquer (étendre un test
    # proche au lieu d'en créer un quasi-identique), mais on lui interdit la non-action.
    suggestion_note = ""
    if state.get("suggestion_intent"):
        suggestion_note = (
            "\n\n⚠️ L'utilisateur a cliqué sur une SUGGESTION de couverture pour en faire un test. "
            "Tu DOIS produire une action de test, jamais une simple réponse texte :\n"
            "- Si le scénario n'est pas couvert → `generate_test_data` (nouveau test).\n"
            "- S'il recoupe largement un test existant → étends/ajuste ce test "
            "(`add_test_row` ou `update_test_data`) plutôt que de créer un doublon, et explique-le.\n"
            "- Seulement si la suggestion suppose un comportement que le SQL ne fait pas → "
            "`ask_clarification`.\n"
            "Ne réponds JAMAIS que « c'est déjà vérifié » sans agir : si c'est déjà couvert, "
            "renforce le test existant via `add_test_row`."
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
Réponds en français, de manière concise et naturelle.

Si la demande suppose un comportement SQL que tu n'observes pas dans la requête
(ex : l'utilisateur attend un tri par volume mais la requête utilise MAX() alphabétique,
ou une notion de "plus pertinent" qui est en réalité arbitraire ou alphabétique),
utilise `ask_clarification` pour signaler l'incohérence et demander confirmation avant d'agir.

Ne produis pas de texte de réflexion quand tu appelles un outil — l'outil parle pour toi.
Réserve le texte libre aux réponses purement conversationnelles (sans outil).{debug_budget_note}{suggestion_note}{eval_context}"""

    # Build uid→test lookup (test_uid is assigned by retrieve_existing_tests above)
    uid_to_test: dict = {t["test_uid"]: t for t in existing_tests if t.get("test_uid")}

    # Tools that reference a specific test by test_uid and need uid validation
    _UID_TOOLS = {
        "delete_test",
        "update_test_description",
        "update_test_data",
        "run_cte",
        "request_reevaluation",
        "patch_test_field",
        "remove_test_row",
        "add_test_row",
    }

    @tool
    def generate_test_data(scenario: str) -> str:
        """Génère un nouveau test pour le scénario décrit en langage naturel."""
        return scenario

    @tool
    def delete_test(test_uid: str) -> str:
        """Supprime le test identifié par test_uid (ex: 'a3f9').
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
    def patch_test_field(
        test_uid: str, table: str, row_index: int, field: str, value_json: str
    ) -> str:
        """Modifie la valeur d'un champ dans une ligne existante des données d'entrée d'un test.
        table: nom de la table tel qu'affiché dans les données (ex: 'chicago_taxi_trips_taxi_trips')
        row_index: indice 0-based de la ligne à modifier (visible dans l'affichage [0], [1]…)
        field: nom du champ à modifier
        value_json: valeur JSON encodée à affecter (ex: "null" pour NULL, "42" pour entier, '"texte"' pour chaîne, '"2024-01-01"' pour date)"""
        return f"{test_uid}:{table}:{row_index}:{field}:{value_json}"

    @tool
    def remove_test_row(test_uid: str, table: str, row_index: int) -> str:
        """Supprime une ligne des données d'entrée d'un test.
        table: nom de la table
        row_index: indice 0-based de la ligne à supprimer"""
        return f"{test_uid}:{table}:{row_index}"

    @tool
    def add_test_row(test_uid: str, tables: list[str], instruction: str = "") -> str:
        """Ajoute une nouvelle ligne dans les tables spécifiées pour un test existant.
        tables: liste des noms de tables qui ont besoin d'une nouvelle ligne
                (plusieurs tables si le scénario nécessite des lignes cohérentes sur un JOIN)
        instruction: contexte court sur ce que doit représenter la nouvelle ligne
                     (ex: 'Regular tier driver', 'client sans commande')"""
        return f"{test_uid}:{','.join(tables)}:{instruction}"

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

    @tool
    def ask_clarification(question: str) -> str:
        """Pose une question de clarification à l'utilisateur avant d'agir.
        Utilise cet outil quand la demande est ambiguë ou quand tu détectes une incohérence
        entre l'intention exprimée et le comportement réel de la requête SQL.
        Exemple : l'utilisateur demande de tester "le domaine le plus pertinent" mais la requête
        utilise MAX() alphabétique — signale-le et demande si c'est intentionnel.
        question : la question à poser à l'utilisateur (claire, concise, en français)."""
        return question

    base_tools = [
        ask_clarification,
        generate_test_data,
        delete_test,
        update_test_data,
        update_test_description,
        generate_suggestions,
        request_reevaluation,
        patch_test_field,
        remove_test_row,
        add_test_row,
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

    # Reprise stateless après ask_clarification : si la dernière action de l'agent
    # (dans l'historique) était une question de clarification non résolue, l'input
    # utilisateur courant en est la réponse → on ré-injecte l'intention pour que
    # l'agent agisse au lieu de reposer la même question.
    resume_context = ""
    if user_input:
        for m in reversed(history):
            mtype = get_message_type(m)
            if mtype == MsgType.OTHER and (m.additional_kwargs or {}).get(
                "pending_intent"
            ):
                pending_intent = m.additional_kwargs["pending_intent"]
                resume_context = f"""

⚠️ REPRISE APRÈS CLARIFICATION
Tu avais demandé : "{pending_intent}"
L'utilisateur vient de répondre : "{user_input}"
Agis maintenant en conséquence (génère ou corrige le test approprié). Ne repose pas la même question."""
                break
            # L'agent a déjà agi après avoir demandé (test généré/exécuté) → pas de reprise
            if mtype in (MsgType.RESULTS, MsgType.EXAMPLES):
                break

    formatted_history = [_format_debug_message(m) for m in history]
    messages_for_llm = [
        SystemMessage(content=system_content + resume_context)
    ] + formatted_history
    # Déclenchement automatique (retry bad_data) : prioritaire sur un `input` périmé
    # qui pourrait traîner dans le state. `auto_correct` est posé par le nœud
    # bad_data_to_agent ; le repli `not user_input and feedback == bad_data` couvre
    # les entrées sans flag (ex. CLI generate).
    is_auto_correct = bool(state.get("auto_correct")) or (
        not user_input and evaluation_feedback == "bad_data"
    )
    if is_auto_correct:
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
                f"Corrige de façon CIBLÉE le test [{failing_uid_trigger}] : utilise "
                f"`patch_test_field` / `add_test_row` / `remove_test_row` pour ajuster "
                f"précisément les données qui alimentent l'étape bloquante. N'emploie "
                f"`update_test_data` (régénération complète) que si une correction ciblée "
                f"est impossible. Si le comportement observé (ex : 0 ligne) est en réalité "
                f"attendu pour ce scénario, appelle `request_reevaluation` avec la justification."
            )
        else:
            trigger = (
                f"Le test [{failing_uid_trigger}] a été jugé Insuffisant : ses données "
                f"d'entrée ne satisfont pas ses contraintes (diagnostic CTE ci-dessus, avec "
                f"l'étape bloquante). Corrige de façon CIBLÉE plutôt que tout régénérer : "
                f"utilise `patch_test_field` / `add_test_row` / `remove_test_row` pour ajuster "
                f"précisément les données de l'étape bloquante (ex. pour un anti-join "
                f"`… IS NULL`, fais en sorte que la clé NE matche PAS la table anti-jointe). "
                f"Utilise `run_cte` d'abord si tu dois inspecter les valeurs réelles d'une CTE. "
                f"Ne recours à `update_test_data` (régénération complète) que si une correction "
                f"ciblée est impossible. Si le comportement observé est en réalité attendu, "
                f"appelle `request_reevaluation`."
            )
        messages_for_llm = messages_for_llm + [HumanMessage(content=trigger)]
    elif user_input:
        messages_for_llm = messages_for_llm + [HumanMessage(content=user_input)]

    _DEBUG_TOOLS = {"run_cte"}
    _DATA_PATCH_TOOLS = {"patch_test_field", "remove_test_row", "add_test_row"}

    agent_tool_call: str | None = None
    agent_tool_args: dict = {}
    new_input = state.get("input", "")
    result = None
    _UID_RETRY_MAX = 2
    uid_retries = 0

    logger.diag("[conv_agent] PROMPT SYSTEM (extrait):\n%s", system_content[:2000])
    if eval_context:
        logger.diag("[conv_agent] EVAL_CONTEXT (bloc complet):\n%s", eval_context)
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
            [f"{tc['name']}({tc.get('args', {})})" for tc in tool_calls] or "(aucun)",
            (result.content or "")[:1000],
        )

        if not tool_calls:
            logger.diag("[conv_agent] LLM n'a appelé aucun outil → réponse texte libre")
            break

        # Collect debug calls (batch), data patch calls (batch), and first other action
        pending_debug_calls = []
        pending_data_calls = []
        invalid_data_uids: list[str] = []
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
            elif tc["name"] in _DATA_PATCH_TOOLS:
                args = dict(tc["args"])
                uid = args.get("test_uid", "")
                if uid and uid in uid_to_test:
                    args["test_index"] = uid_to_test[uid]["test_index"]
                elif uid and uid not in uid_to_test:
                    invalid_data_uids.append(uid)
                    continue
                pending_data_calls.append({"tool": tc["name"], "args": args})
            elif first_action_tc is None:
                first_action_tc = tc

        # A data-patch batch that references unknown uids would otherwise be
        # silently dropped → the turn becomes a no-op (agent_tool_call=None →
        # history_saver). Mirror the single-action path: feed the valid ids back
        # to the LLM and retry instead of swallowing the request.
        if (
            invalid_data_uids
            and not pending_debug_calls
            and first_action_tc is None
            and uid_retries < _UID_RETRY_MAX
        ):
            uid_retries += 1
            logger.diag(
                "[conv_agent] data_batch uid(s) inconnu(s)=%s — retry %d/%d",
                invalid_data_uids,
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
                f"Les identifiants de test {invalid_data_uids} n'existent pas. "
                f"IDs disponibles : {available}. "
                f"Ré-applique tes modifications avec les bons identifiants."
            )
            messages_for_llm = messages_for_llm + [
                result,
                HumanMessage(content=error_feedback),
            ]
            continue

        # Priority: debug > data_batch > single action
        if pending_debug_calls:
            agent_tool_call = "debug_batch"
            agent_tool_args = {"calls": pending_debug_calls}
            break

        if pending_data_calls:
            agent_tool_call = "data_batch"
            agent_tool_args = {"calls": pending_data_calls}
            logger.diag(
                "[conv_agent] data_batch: %d opération(s) — %s",
                len(pending_data_calls),
                [op["tool"] for op in pending_data_calls],
            )
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
        # Flag consommé : éviter qu'il ne fuite sur un tour suivant (ex. chat user).
        "auto_correct": False,
    }
    if evaluation_feedback == "bad_data":
        current_retries = state.get("gen_retries")
        if current_retries is not None and current_retries > 0:
            update["gen_retries"] = current_retries - 1
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
                "action": "add"
                if agent_tool_call == "generate_test_data"
                else "update",
                "parent": last_msg_id,
                "request_id": state.get("request_id"),
            },
        )
        msgs_to_add.append(scenario_msg)
        last_msg_id = scenario_msg.id
        update["agent_message_id"] = scenario_msg.id
    elif agent_tool_call == "ask_clarification":
        question = agent_tool_args.get("question", "")
        if question:
            msgs_to_add.append(
                AIMessage(
                    content=question,
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.OTHER,
                        # Breadcrumb pour la reprise stateless : au tour suivant, l'agent
                        # détecte que sa dernière question était une clarification non résolue
                        # et traite l'input utilisateur comme la réponse.
                        "pending_intent": question,
                        "parent": last_msg_id,
                        "request_id": state.get("request_id"),
                    },
                )
            )

    # Gemini with bind_tools may return content as a list of parts instead of a plain string
    raw_content = result.content
    if isinstance(raw_content, list):
        raw_content = "".join(
            part.get("text", "") if isinstance(part, dict) else ""
            for part in raw_content
            if isinstance(part, dict) and part.get("type") == "text"
        )

    # Only display raw LLM text when no tool was called (pure conversational response).
    # When a tool is called, the raw_content is internal reasoning — not user-facing.
    # ask_clarification already emits its question above; action tools speak for themselves.
    if raw_content and agent_tool_call is None:
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
