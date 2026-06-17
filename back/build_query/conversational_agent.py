import json
import logging
import uuid

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage, SystemMessage
from langchain_core.tools import tool

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from storage.test_repository import get_test
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE
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


def _validate_data_patch_calls(calls: list, uid_to_test: dict) -> tuple[list, list]:
    """Valide les opérations de patch contre les données réelles des tests ciblés.

    Sans cette validation, un `patch_test_field` sur un champ inexistant créerait un
    champ fantôme dans la ligne (corruption silencieuse), et une table ou un indice
    de ligne inexistants seraient ignorés sans bruit par le data_patcher — le tour
    deviendrait un no-op sans que l'agent n'apprenne que sa demande était invalide.

    Simule l'application en ordre (un `add_test_row` ajoute 1 ligne par table) pour
    autoriser un patch sur une ligne ajoutée plus tôt dans le même lot.
    Retourne ``(calls_valides, erreurs)`` — les erreurs sont prêtes à être renvoyées
    au LLM pour qu'il ré-émette une demande corrigée."""
    valid: list = []
    errors: list = []
    row_counts: dict = {}  # (uid, table) → nb de lignes simulé (None = table absente)
    known_fields: dict = {}  # (uid, table) → union des champs des lignes existantes

    def _init(uid: str, table: str, data: dict) -> tuple:
        key = (uid, table)
        if key not in row_counts:
            rows = (data or {}).get(table)
            row_counts[key] = len(rows) if rows is not None else None
            known_fields[key] = (
                set().union(*(r.keys() for r in rows)) if rows else set()
            )
        return key

    for op in calls:
        tool, args = op["tool"], op["args"]
        uid = args.get("test_uid", "")
        data = (uid_to_test.get(uid) or {}).get("data") or {}

        if tool == "add_test_row":
            for table in args.get("tables") or []:
                key = _init(uid, table, data)
                row_counts[key] = (row_counts[key] or 0) + 1
            valid.append(op)
            continue

        table = args.get("table", "")
        key = _init(uid, table, data)
        if row_counts[key] is None:
            errors.append(
                f"{tool} : la table '{table}' n'existe pas dans les données du test "
                f"[{uid}]. Tables disponibles : {', '.join(data) or 'aucune'}."
            )
            continue
        try:
            row_idx = int(args.get("row_index", 0))
        except (TypeError, ValueError):
            errors.append(
                f"{tool} : row_index invalide ({args.get('row_index')!r}) pour {table}."
            )
            continue
        if not 0 <= row_idx < row_counts[key]:
            errors.append(
                f"{tool} : l'indice de ligne {row_idx} est hors limites pour {table} "
                f"({row_counts[key]} ligne(s), indices 0..{row_counts[key] - 1})."
            )
            continue
        if tool == "patch_test_field":
            field = args.get("field", "")
            fields = known_fields[key]
            if fields and field not in fields:
                errors.append(
                    f"patch_test_field : le champ '{field}' n'existe pas dans {table}. "
                    f"Champs disponibles : {', '.join(sorted(fields))}."
                )
                continue
        elif tool == "remove_test_row":
            row_counts[key] -= 1
        valid.append(op)

    return valid, errors


def _format_ledger_op(op: dict) -> str:
    """Rend une op du ledger en une clause courte et lisible par l'agent."""
    tool = op.get("tool", "?")
    if tool == "patch_test_field":
        return (
            f"patch_test_field {op.get('table', '?')}[{op.get('row_index', '?')}]"
            f".{op.get('field', '?')} = {op.get('value_json', '?')}"
        )
    if tool == "remove_test_row":
        return f"remove_test_row {op.get('table', '?')}[{op.get('row_index', '?')}]"
    if tool == "add_test_row":
        tables = op.get("tables") or []
        instr = op.get("instruction", "")
        suffix = f" ({instr})" if instr else ""
        return f"add_test_row {', '.join(tables)}{suffix}"
    if tool == "regen":
        return "update_test_data (régénération complète des données)"
    return tool


def _format_outcome_trace(outcome: dict) -> str:
    """Rend le trace structuré d'une tentative (profil row_count + valeurs des CTE
    pivots + mismatch jointure), pour que la *comparaison* d'une tentative à l'autre
    révèle ce qui a réellement bougé (ex. une valeur de jointure non-déterministe).

    Retourne une chaîne (lignes indentées) ou "" si aucun trace n'est attaché.
    """
    trace = outcome.get("cte_trace") or {}
    profile = trace.get("profile") or []
    if not profile:
        return ""
    lines: list[str] = []
    profile_txt = " → ".join(f"{e.get('name')}={e.get('rows')}" for e in profile)
    lines.append(f"  Profil CTE : {profile_txt}")
    # Valeurs des CTE pivots (faible cardinalité) : c'est là qu'une valeur de
    # jointure qui change d'un tour à l'autre devient visible.
    for e in profile:
        if e.get("sample"):
            lines.append(
                f"  {e.get('name')} = {json.dumps(e['sample'], ensure_ascii=False, default=str)}"
            )
    for bl in trace.get("mismatch") or []:
        lines.append(f"  Blocage : {bl}")
    return "\n".join(lines)


def _render_attempt_messages(attempts: list) -> list[BaseMessage]:
    """Rend le ledger des tentatives en conversation alternée AI/HUMAN.

    Le ledger est la source (pas de persistance de vrais messages LangChain) ;
    le rendu est reconstruit à chaque round et inséré avant le trigger
    ``auto_correct``, pour que l'agent raisonne « ce levier a déjà été actionné
    sans effet → le bloqueur est ailleurs » au lieu de redécouvrir le problème.

    Au-delà du digest une-ligne, chaque tentative porte son **trace d'exécution**
    (profil row_count de toutes les CTE + valeurs des pivots + mismatch) : la lecture
    croisée tentative N vs N+1 expose la vraie cause (ex. valeur de jointure qui bouge)
    plutôt que de faire confiance à la seule CTE « bloquante » désignée.
    """
    msgs: list[BaseMessage] = []
    for a in attempts or []:
        rnd = a.get("round", "?")
        ops_txt = " ; ".join(_format_ledger_op(op) for op in a.get("ops") or [])
        msgs.append(AIMessage(content=f"Tentative {rnd} — {ops_txt}"))
        outcome = a.get("outcome")
        if outcome:
            trace_txt = _format_outcome_trace(outcome)
            body = f"Résultat tentative {rnd} : {outcome.get('digest', '?')}."
            if trace_txt:
                body += "\n" + trace_txt
            body += "\nNe répète pas une tentative équivalente."
            msgs.append(HumanMessage(content=body))
    return msgs


def _normalized_ops(ops: list) -> tuple:
    """Forme canonique d'un lot d'ops pour comparaison avec le ledger.

    Accepte les deux formats : ``[{tool, args}]`` (lot en cours) et les entrées
    plates du ledger (``{tool, table, row_index, …}``).
    """
    norm = []
    for op in ops or []:
        src = op.get("args") if isinstance(op.get("args"), dict) else op
        norm.append(
            (
                op.get("tool", "?"),
                str(src.get("table", "")),
                str(src.get("row_index", "")),
                str(src.get("field", "")),
                str(src.get("value_json", "")),
                tuple(src.get("tables") or []),
                str(src.get("instruction", "")),
            )
        )
    return tuple(sorted(norm))


def _noop_batch_reason(
    pending_calls: list, uid_to_test: dict, attempts: list
) -> str | None:
    """Motif de rejet d'un lot de patches inopérant, ou None si le lot est valable.

    Deux gardes (incident 2026-06-11 : l'échange PROD1↔PROD2 entre deux lignes —
    identiques après SUBSTR — a consommé un round executor+evaluator complet sans
    pouvoir changer le résultat) :
    1. lot identique à une tentative passée du ledger → rejet avec le round ;
    2. lot de patches qui ne change le multiset d'AUCUNE colonne touchée → rejet.
    """
    batch_norm = _normalized_ops(pending_calls)
    for a in attempts or []:
        if a.get("ops") and _normalized_ops(a["ops"]) == batch_norm:
            digest = (a.get("outcome") or {}).get("digest", "sans effet")
            return (
                f"⛔ Lot rejeté sans ré-exécution : il est identique à la tentative "
                f"{a.get('round')} ({digest}). Propose une correction DIFFÉRENTE — "
                "le bloqueur est ailleurs."
            )

    import copy

    datas: dict = {}
    touched: set = set()
    for call in pending_calls:
        tool = call.get("tool")
        args = call.get("args") or {}
        uid = args.get("test_uid", "")
        test = uid_to_test.get(uid)
        if test is None or tool == "add_test_row":
            return None  # injugeable / ajout de ligne → toujours effectif
        data = datas.setdefault(uid, copy.deepcopy(test.get("data") or {}))
        table = args.get("table", "")
        try:
            row_idx = int(args.get("row_index", 0))
        except (TypeError, ValueError):
            return None
        rows = data.get(table)
        if rows is None or row_idx >= len(rows):
            return None  # op invalide : data_patcher la loguera, pas un no-op
        if tool == "remove_test_row":
            rows.pop(row_idx)
            return None  # une suppression change toujours les multisets
        if tool == "patch_test_field":
            field = args.get("field", "")
            raw = args.get("value_json", "null")
            try:
                value = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                value = raw
            rows[row_idx][field] = value
            touched.add((uid, table, field))

    if not touched:
        return None
    for uid, table, field in touched:
        before = [
            repr(r.get(field))
            for r in (uid_to_test[uid].get("data") or {}).get(table, [])
        ]
        after = [repr(r.get(field)) for r in datas[uid].get(table, [])]
        if sorted(before) != sorted(after):
            return None
    return (
        "⛔ Lot rejeté sans ré-exécution : ces patches ne modifient le multiset "
        "d'aucune colonne touchée (ex. échange de valeurs entre deux lignes — le "
        "résultat de la requête serait identique). Change la VALEUR d'au moins une "
        "colonne impliquée dans l'étape bloquante, en suivant les recettes de "
        "jointure si la clé est dérivée."
    )


def _plain_text(content) -> str:
    """Contenu texte d'un message LLM — Gemini avec bind_tools peut renvoyer
    une liste de parts au lieu d'une chaîne."""
    if isinstance(content, list):
        return "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return content or ""


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
                    parsed_results, ensure_ascii=False, indent=2
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

⚠️ Règle impérative : préfère les corrections chirurgicales groupées à la régénération complète. Utilise `update_test_data` seulement si la logique du scénario doit être refondée. Applique ces corrections directement, sans demander de confirmation (la confirmation préalable n'est exigée que pour `delete_test`)."""

    return eval_context, eval_test_idx


def _build_tool_ack(agent_tool_call: str | None, agent_tool_args: dict) -> str | None:
    """Phrase courte (1-2 lignes) annonçant à l'utilisateur l'action que l'agent
    engage via ses outils, pour qu'il voie que sa demande est prise en compte.

    Couvre les outils dont l'utilisateur attend une confirmation conversationnelle :
    - `generate_test_data` / `update_test_data` : courte phrase au-dessus de la carte
      « Nouveau test / Test modifié » (le scénario détaillé reste dans la carte) ;
    - `data_batch` (patch/ajout/suppression de lignes) : n'affiche que le tableau
      patché → on verbalise l'action ;
    - `generate_suggestions` : ne rafraîchit que le panneau dédié → on l'annonce.
    `update_test_description` / `delete_test` ont déjà leur propre confirmation →
    retourne None (comme pour tout outil de debug/réévaluation/clarification).
    """
    if agent_tool_call == "generate_test_data":
        return "C'est noté — je te prépare un nouveau test pour ce scénario."
    if agent_tool_call == "update_test_data":
        return "C'est noté — je corrige les données de ce test, puis je le relance."
    if agent_tool_call == "data_batch":
        adds = patches = removes = 0
        for op in agent_tool_args.get("calls") or []:
            tool = op.get("tool")
            if tool == "add_test_row":
                adds += 1  # une op = une ligne logique (cohérente sur le JOIN)
            elif tool == "patch_test_field":
                patches += 1
            elif tool == "remove_test_row":
                removes += 1
        parts = []
        if adds:
            parts.append(f"j'ajoute {adds} ligne{'s' if adds > 1 else ''}")
        if patches:
            parts.append(f"j'ajuste {patches} valeur{'s' if patches > 1 else ''}")
        if removes:
            parts.append(f"je retire {removes} ligne{'s' if removes > 1 else ''}")
        detail = ", ".join(parts) if parts else "j'ajuste les données"
        return (
            f"Je modifie les données de test pour prendre en compte ta demande "
            f"({detail}), puis je relance l'exécution."
        )
    if agent_tool_call == "generate_suggestions":
        return (
            "Je te prépare de nouvelles suggestions de cas à tester — "
            "elles apparaîtront dans le panneau des tests."
        )
    return None


def _format_branch_plan_hint(test_obj: dict | None) -> str:
    """Rappel du contrat de branche (UNION ALL) déclaré à la génération.

    Donne à l'agent de correction une cible explicite : diffuser le contrat
    déclaré (must_hold / must_not_hold) contre la trace CTE, plutôt que de
    repartir du seul signal « CTE vide ».
    """
    bp = (test_obj or {}).get("branch_plan")
    if not bp:
        return ""
    branch = bp.get("branch", "?")
    must_hold = bp.get("must_hold") or []
    must_not_hold = bp.get("must_not_hold") or []
    if not must_hold and not must_not_hold:
        return ""
    lines = [
        f"\n\nContrat de branche déclaré à la génération (branche « {branch} ») — "
        "confronte-le à la trace CTE et cible la condition NON respectée :"
    ]
    if must_hold:
        lines.append("  Doivent être VRAIES (sinon la ligne ne survit pas) :")
        lines += [f"    - {c}" for c in must_hold]
    if must_not_hold:
        lines.append("  Doivent rester FAUSSES (sinon la ligne est supprimée) :")
        lines += [f"    - {c}" for c in must_not_hold]
    return "\n".join(lines)


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

    # Suggestions de couverture actives (état du modèle, panneau dédié) : on les
    # expose à l'agent pour qu'il comprenne « fais un test de la suggestion 2 » ou
    # « régénère les suggestions en insistant sur X » — sinon il ne sait pas de quoi
    # l'utilisateur parle. Numérotées 1-based pour matcher l'affichage du panneau.
    from build_query.suggestions_node import SUGGESTIONS_CAP

    stored = get_test(state["session"]) or {}
    current_suggestions = [s for s in (stored.get("suggestions") or []) if s]
    suggestions_count = len(current_suggestions)
    suggestions_summary = (
        "\n".join(f"{i}. {s}" for i, s in enumerate(current_suggestions, 1))
        if current_suggestions
        else "Aucune suggestion active pour l'instant."
    )

    # Contexte injecté quand l'agent est appelé après un verdict "bad_data" de l'évaluateur
    evaluation_feedback = state.get("evaluation_feedback")
    eval_context, eval_test_idx = _build_agent_eval_context(state, existing_tests)

    ctes = json.loads(state.get("query_decomposed") or "[]")
    cte_names = [c["name"] for c in ctes]
    cte_names_str = ", ".join(f'"{n}"' for n in cte_names) if cte_names else "aucune"

    debug_retries = state.get("debug_retries") or 0
    debug_budget_note = f"\nRounds de debug (run_cte) restants : {debug_retries}." + (
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

    # Canal de raisonnement : quand le thinking natif Gemini est actif (flash/pro),
    # le raisonnement se fait hors du contenu → interdire le texte de réflexion
    # garde les réponses propres. Quand il est INACTIF (flash-lite, budget 0), le
    # contenu est le seul canal de raisonnement : l'interdire force le modèle à
    # émettre un appel d'outil complexe « à froid » — c'est un facteur direct de
    # MALFORMED_FUNCTION_CALL. Le texte émis avec un outil n'est de toute façon
    # jamais montré à l'utilisateur (cf. filtrage de raw_content plus bas).
    from storage.config import is_native_thinking_active

    if is_native_thinking_active():
        reasoning_note = (
            "Ne produis pas de texte de réflexion quand tu appelles un outil — "
            "l'outil parle pour toi.\n"
            "Réserve le texte libre aux réponses purement conversationnelles (sans outil)."
        )
    else:
        reasoning_note = (
            "Avant d'appeler un outil, tu peux poser ton raisonnement en 1 à 2 phrases "
            "de texte (jamais montrées à l'utilisateur), puis émets l'appel d'outil "
            "avec des arguments JSON valides.\n"
            "Réserve les réponses développées aux réponses purement conversationnelles "
            "(sans outil)."
        )

    # Recettes de jointure (clés dérivées) : sans elles, face à un écart du type
    # `code_produit = ROD` vs données `PROD1`, l'agent interprète l'écart comme une
    # inversion de lignes et patche à côté jusqu'à épuisement des retries (incident
    # du 2026-06-11). Mises en cache par (sql, dialect) — coût nul après le 1ᵉʳ appel.
    from build_query.join_recipes import build_join_recipes_block

    join_recipes_block = build_join_recipes_block(
        state.get("optimized_sql") or state.get("query", ""),
        dialect=state.get("dialect", "bigquery"),
        schema=state.get("schemas") or None,
    )

    system_content = f"""{MOCKSQL_PRODUCT_PREAMBLE}

Tu es l'assistant conversationnel de MockSQL : tu réponds aux questions de l'utilisateur sur ses tests et tu les modifies via tes outils.

SQL testé (dialecte {state.get("dialect", "bigquery")}):
{state.get("optimized_sql") or state.get("query", "")}
{join_recipes_block}
Étapes inspectables avec run_cte : {cte_names_str}

Tests existants :
{tests_summary}

Suggestions de couverture actives ({suggestions_count}/{SUGGESTIONS_CAP} — maximum {SUGGESTIONS_CAP}, proposées à l'utilisateur dans le panneau dédié, numérotées comme à l'écran) :
{suggestions_summary}

Si l'utilisateur fait référence à une suggestion (« la suggestion 2 », « le cas que tu as proposé sur les NULL »…), appuie-toi sur cette liste pour savoir de quoi il parle, puis agis : `generate_test_data` pour en faire un nouveau test, ou étends un test existant si le scénario recoupe largement.
Tu peux générer des suggestions toi-même avec `generate_suggestions` ; passe dans `instructions` le commentaire de l'utilisateur (ex : « focus sur les cas limites », « des cas qui existent en prod ») pour orienter les propositions. Règles du plafond de {SUGGESTIONS_CAP} :
- Par défaut, les nouvelles suggestions s'AJOUTENT aux existantes (appel sans `replace`).
- Si l'utilisateur demande de « remplacer » / « refaire » les suggestions → appelle `generate_suggestions` avec `replace=True`.
- Si les {SUGGESTIONS_CAP} suggestions sont déjà présentes et qu'il en veut plus SANS remplacer → n'appelle pas l'outil : explique-lui qu'il a atteint le maximum de {SUGGESTIONS_CAP} et qu'il peut en supprimer dans le panneau ou demander un remplacement.

Tu peux répondre aux questions sur la couverture, analyser les redondances,
et utiliser les outils disponibles pour générer ou supprimer des tests.
Pour toute suppression, demande toujours confirmation dans ta réponse AVANT d'appeler delete_test.
Réponds en français, de manière concise et naturelle.

⚠️ Ne réponds JAMAIS à l'aveugle sur ce que produit la requête. Dès qu'une question
porte sur le comportement réel du SQL (« qu'est-ce que renvoie cette CTE ? », « pourquoi
ce test ne retourne rien ? », « ce cas est-il bien couvert ? », « quelle valeur sort
de … ? »), inspecte d'abord avec `run_cte` sur les données du test concerné, PUIS réponds
en t'appuyant sur les lignes réellement observées. Ne devine pas le résultat d'une
exécution que tu peux vérifier. `run_cte` est un outil d'inspection (ni génération ni
correction) : l'utiliser n'enfreint pas la règle « réponds en texte » ci-dessous.

Si l'utilisateur pose une simple question (explication d'un résultat, analyse de
couverture/redondance) sans demander de modification, réponds en texte — n'appelle
aucun outil de génération ou de correction (mais `run_cte` reste autorisé pour vérifier
ta réponse avant de l'écrire).

Quand le contexte s'y prête, TERMINE ta réponse en proposant une action concrète et
utile : ajuster un test ou sa description, ajouter un test pour un cas voisin non couvert,
etc. N'hésite pas à le proposer — mais n'agis pas sans l'accord de l'utilisateur, c'est à
lui de valider. Ne propose rien si aucune action n'a de sens dans le contexte (ne force
pas une proposition artificielle).

Si la demande suppose un comportement SQL que tu n'observes pas dans la requête
(ex : l'utilisateur attend un tri par volume mais la requête utilise MAX() alphabétique,
ou une notion de "plus pertinent" qui est en réalité arbitraire ou alphabétique),
utilise `ask_clarification` pour signaler l'incohérence et demander confirmation avant d'agir.

Si la demande n'indique pas clairement s'il faut CRÉER UN NOUVEAU TEST ou
MODIFIER UN TEST EXISTANT, et que le contexte (test ancré, formulation) ne lève pas
le doute, n'agis PAS à l'aveugle : appelle `ask_clarification` pour demander lequel
des deux tu dois faire AVANT de choisir entre `generate_test_data` (nouveau test) et
`add_test_row` / `update_test_data` / `update_test_description` (test existant).

{reasoning_note}{debug_budget_note}{suggestion_note}{eval_context}"""

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
    def generate_suggestions(instructions: str = "", replace: bool = False) -> str:
        """Génère des suggestions de cas de tests non encore couverts. Appelle cet outil pour proposer
        des scénarios à l'utilisateur, notamment après une génération de tests ou quand il demande
        quoi tester ensuite.
        instructions (optionnel) : axe à privilégier (ex : 'focus sur les cas NULL',
        'insiste sur les valeurs limites', 'des cas qui existent en prod').
        replace : False (défaut) → les nouvelles suggestions S'AJOUTENT aux existantes
        (plafond de 5 ; au-delà, les plus anciennes sont écartées). True → REMPLACE toute la
        liste actuelle par de nouvelles (à utiliser quand l'utilisateur demande de « remplacer »
        ou « refaire » les suggestions)."""
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
            # Verdicts de l'évaluateur : l'agent doit voir TOUT l'historique des
            # évaluations (pas seulement la dernière, re-injectée dans le SYSTEM sur
            # bad_data) pour répondre aux questions « pourquoi ce test est warn ? » et
            # garder le contexte des verdicts passés sur l'ensemble des tests.
            MsgType.EVALUATION,
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
Traite maintenant la demande initiale à la lumière de cette réponse, sans reposer la même question.
- Si elle appelle une action sur un test (créer, corriger, supprimer), utilise l'outil approprié.
- Si c'est une simple question (explication, analyse de couverture/redondance), réponds en texte, sans appeler d'outil."""
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
        failing_test_obj = next(
            (
                t
                for t in existing_tests
                if str(t.get("test_index")) == str(eval_test_idx)
            ),
            None,
        )
        failing_uid_trigger = (failing_test_obj or {}).get(
            "test_uid", str(eval_test_idx)
        )
        branch_plan_hint = _format_branch_plan_hint(failing_test_obj)
        # TICKET-1 : protection d'une prémisse utilisateur. Quand le test a été créé
        # sur une affirmation EXPLICITE de l'utilisateur (marqueur `user_premise`),
        # la boucle ne doit pas muter en silence la valeur énoncée pour rendre le test
        # vert — ce serait blanchir l'attente de l'user en tautologie. On oriente alors
        # vers la délégation (request_reevaluation / ask_clarification → VALIDATION_PROMPT)
        # plutôt que vers un patch muet. Détection par authorship explicite (pas
        # d'heuristique sur le texte) ; enforcement par instruction de prompt.
        user_premise = (failing_test_obj or {}).get("user_premise")
        premise_guard = (
            (
                f"\n\n⚠️ PRÉMISSE UTILISATEUR à protéger : ce test a été créé sur "
                f"l'affirmation explicite de l'utilisateur — « {user_premise} ». NE mute "
                f"JAMAIS en silence une valeur d'entrée qui porte cette prémisse pour "
                f"faire passer le test. Si la correction nécessaire la contredirait, "
                f"appelle `request_reevaluation` (si le comportement observé — ex : 0 "
                f"ligne — est en fait correct pour ce scénario) ou `ask_clarification` "
                f"(pour que l'utilisateur tranche : son attente est-elle fausse, ou son "
                f"SQL ?). Ne patche pas la valeur énoncée."
            )
            if user_premise
            else ""
        )
        if any(get_message_type(m) == MsgType.DEBUG_RUN_CTE for m in history):
            trigger = (
                (
                    f"Le diagnostic est terminé — les résultats sont visibles ci-dessus. "
                    f"Corrige de façon CIBLÉE le test [{failing_uid_trigger}] : utilise "
                    f"`patch_test_field` / `add_test_row` / `remove_test_row` pour ajuster "
                    f"précisément les données qui alimentent l'étape bloquante. N'emploie "
                    f"`update_test_data` (régénération complète) que si une correction ciblée "
                    f"est impossible. Si le comportement observé (ex : 0 ligne) est en réalité "
                    f"attendu pour ce scénario, appelle `request_reevaluation` avec la justification."
                )
                + branch_plan_hint
                + premise_guard
            )
        else:
            # Ne pas répéter ici les règles d'usage des outils : elles sont déjà
            # dans le contexte automatique du SYSTEM — la duplication allonge le
            # prompt sans gain et crée des risques d'incohérence entre les deux.
            trigger = (
                (
                    f"Le test [{failing_uid_trigger}] a été jugé Insuffisant : ses données "
                    f"d'entrée ne satisfont pas ses contraintes (diagnostic CTE et règles "
                    f"d'usage des outils dans le contexte automatique ci-dessus). Applique "
                    f"maintenant une correction CIBLÉE de l'étape bloquante — `run_cte` "
                    f"d'abord si tu dois inspecter les valeurs réelles d'une CTE."
                )
                + branch_plan_hint
                + premise_guard
            )
        # Mémoire des tentatives : rendu du ledger en conversation alternée
        # AI/HUMAN, inséré entre l'historique et le trigger courant.
        attempt_msgs = _render_attempt_messages(state.get("correction_attempts") or [])
        messages_for_llm = (
            messages_for_llm + attempt_msgs + [HumanMessage(content=trigger)]
        )
    elif user_input:
        messages_for_llm = messages_for_llm + [HumanMessage(content=user_input)]

    # Instructions supplémentaires saisies par l'utilisateur pendant la génération :
    # consultées à chaud ici et CONSOMMÉES (marquées appliquées) pour qu'elles ne soient
    # pas rejouées par le flush de fin de run. Cf. build_query/pending_instructions.
    from build_query.pending_instructions import (
        consume_instructions,
        peek_instructions,
    )

    extra_instructions = peek_instructions(state["session"])
    if extra_instructions:
        instructions_block = (
            "Instructions supplémentaires ajoutées par l'utilisateur pendant la "
            "génération (à prendre en compte, dans l'ordre) :\n"
            + "\n".join(f"{i + 1}. {t}" for i, t in enumerate(extra_instructions))
        )
        messages_for_llm = messages_for_llm + [HumanMessage(content=instructions_block)]
        consume_instructions(state["session"])

    _DEBUG_TOOLS = {"run_cte"}
    _DATA_PATCH_TOOLS = {"patch_test_field", "remove_test_row", "add_test_row"}

    agent_tool_call: str | None = None
    agent_tool_args: dict = {}
    new_input = state.get("input", "")
    result = None
    _UID_RETRY_MAX = 2
    uid_retries = 0
    _NOOP_RETRY_MAX = 2
    noop_retries = 0
    _MALFORMED_RETRY_MAX = 2
    malformed_retries = 0

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
            # Réponse totalement vide : typiquement `finish_reason:
            # MALFORMED_FUNCTION_CALL` sur Gemini (flash-lite surtout) — l'appel
            # d'outil n'a pas pu être parsé et est perdu. Sans retry, le tour
            # brûle un gen_retry et retombe sur le generator (régénération
            # complète) alors qu'une ré-émission suffit le plus souvent.
            if (
                not _plain_text(result.content).strip()
                and malformed_retries < _MALFORMED_RETRY_MAX
            ):
                malformed_retries += 1
                finish_reason = (getattr(result, "response_metadata", None) or {}).get(
                    "finish_reason", "?"
                )
                logger.diag(
                    "[conv_agent] réponse vide (finish_reason=%s) — retry %d/%d",
                    finish_reason,
                    malformed_retries,
                    _MALFORMED_RETRY_MAX,
                )
                messages_for_llm = messages_for_llm + [
                    HumanMessage(
                        content=(
                            "Ta réponse précédente était vide (appel d'outil malformé "
                            "ou interrompu). Ré-émets-la proprement : soit UN appel "
                            "d'outil avec des arguments JSON valides, soit une réponse "
                            "texte."
                        )
                    )
                ]
                continue
            logger.diag("[conv_agent] LLM n'a appelé aucun outil → réponse texte libre")
            break

        # `ask_clarification` est EXCLUSIF et TERMINAL : poser une question doit
        # mettre le tour en pause et attendre la réponse de l'utilisateur (cf. reprise
        # stateless plus bas). Le modèle a tendance à le combiner avec une action
        # (generate_test_data, add_test_row…) dans la même réponse — il signale
        # l'incohérence ET tente de satisfaire la demande. Sans ce court-circuit, la
        # priorité debug > data_batch > première action exécutait l'action selon
        # l'ordre des tool_calls : l'agent « n'attendait pas » et générait un test.
        clarif_tc = next(
            (tc for tc in tool_calls if tc["name"] == "ask_clarification"), None
        )
        if clarif_tc:
            agent_tool_call = "ask_clarification"
            agent_tool_args = dict(clarif_tc["args"])
            logger.diag(
                "[conv_agent] ask_clarification prioritaire → tour terminal "
                "(autres outils ignorés : %s)",
                [tc["name"] for tc in tool_calls if tc["name"] != "ask_clarification"]
                or "aucun",
            )
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
            # Validation des cibles (table / indice de ligne / champ) contre les
            # données réelles : une cible inexistante est renvoyée à l'agent pour
            # qu'il refasse sa demande — sinon le data_patcher l'ignorerait en
            # silence (ou créerait un champ fantôme) et le tour serait perdu.
            pending_data_calls, patch_errors = _validate_data_patch_calls(
                pending_data_calls, uid_to_test
            )
            if patch_errors:
                if uid_retries < _UID_RETRY_MAX:
                    uid_retries += 1
                    logger.diag(
                        "[conv_agent] cible(s) de patch invalide(s) — retry %d/%d :\n%s",
                        uid_retries,
                        _UID_RETRY_MAX,
                        "\n".join(patch_errors),
                    )
                    error_feedback = (
                        "Certaines opérations ciblent des éléments inexistants "
                        "dans les données du test :\n- "
                        + "\n- ".join(patch_errors)
                        + "\nRé-émets tes corrections avec des cibles valides "
                        "(les données indexées sont visibles dans le contexte)."
                    )
                    messages_for_llm = messages_for_llm + [
                        result,
                        HumanMessage(content=error_feedback),
                    ]
                    continue
                # Retries épuisés : les ops invalides sont abandonnées ; s'il ne
                # reste rien, aucun outil actionnable → fallback route_agent_output.
                logger.diag(
                    "[conv_agent] cibles invalides persistantes — %d op(s) abandonnée(s)",
                    len(patch_errors),
                )
                if not pending_data_calls:
                    break
            # Garde anti-no-op (boucle bad_data uniquement) : un lot identique à
            # une tentative passée, ou qui ne change le multiset d'aucune colonne
            # touchée, est renvoyé à l'agent SANS relancer l'executor ni consommer
            # de retry supplémentaire.
            if is_auto_correct:
                noop_reason = _noop_batch_reason(
                    pending_data_calls,
                    uid_to_test,
                    state.get("correction_attempts") or [],
                )
                if noop_reason:
                    if noop_retries < _NOOP_RETRY_MAX:
                        noop_retries += 1
                        logger.diag(
                            "[conv_agent] lot no-op rejeté (retry %d/%d) : %s",
                            noop_retries,
                            _NOOP_RETRY_MAX,
                            noop_reason[:200],
                        )
                        messages_for_llm = messages_for_llm + [
                            result,
                            HumanMessage(content=noop_reason),
                        ]
                        continue
                    # Épuisé : aucun outil actionnable → route_agent_output
                    # retombe sur le generator (régénération complète).
                    logger.diag("[conv_agent] lots no-op répétés — fallback generator")
                    break
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
    # update_test_data corrige un test EXISTANT via le generator : propager le
    # test_uid (identité stable) pour que _resolve_target_key cible le bon test.
    # generate_test_data crée un NOUVEAU test → effacer tout ciblage périmé qui
    # traînerait dans le state (sinon il écraserait un test existant).
    elif agent_tool_call == "update_test_data" and agent_tool_args.get("test_uid"):
        update["test_uid"] = agent_tool_args["test_uid"]
    elif agent_tool_call == "generate_test_data":
        update["test_uid"] = None
        update["test_index"] = None

    msgs_to_add = []
    last_msg_id = parent

    if agent_tool_call in ("generate_test_data", "update_test_data"):
        # Accusé conversationnel au-dessus de la carte scénario : le scénario détaillé
        # reste dans la carte, mais cette phrase confirme la prise en compte. Pas
        # pendant la correction auto (update_test_data en fallback regen) → bruit.
        ack_text = _build_tool_ack(agent_tool_call, agent_tool_args)
        if ack_text and not is_auto_correct:
            ack_msg = AIMessage(
                content=ack_text,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.OTHER,
                    "parent": last_msg_id,
                    "request_id": state.get("request_id"),
                },
            )
            msgs_to_add.append(ack_msg)
            last_msg_id = ack_msg.id
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
    elif agent_tool_call in ("data_batch", "generate_suggestions"):
        # Accusé en langage naturel : data_batch n'affiche que le tableau patché et
        # generate_suggestions ne rafraîchit que le panneau dédié — sans cette phrase
        # l'utilisateur ne voit pas que sa demande a été prise en compte.
        # Pendant la boucle de correction automatique (bad_data), data_batch tourne
        # sans demande directe de l'utilisateur → pas d'accusé (ce serait du bruit à
        # chaque retry) ; on n'accuse que les actions déclenchées par un input user.
        ack_text = _build_tool_ack(agent_tool_call, agent_tool_args)
        if ack_text and not (is_auto_correct and agent_tool_call == "data_batch"):
            ack_msg = AIMessage(
                content=ack_text,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.OTHER,
                    "parent": last_msg_id,
                    "request_id": state.get("request_id"),
                },
            )
            msgs_to_add.append(ack_msg)
            last_msg_id = ack_msg.id
            # Le tableau patché (data_patcher) chaîne sous l'accusé via agent_message_id.
            update["agent_message_id"] = ack_msg.id

    raw_content = _plain_text(result.content)

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
