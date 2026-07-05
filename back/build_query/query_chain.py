import json
import logging
import uuid
from typing import Any, Dict

import utils.logger  # noqa: F401 — registers DIAG level (15)
from langchain_core.messages import AIMessage
from utils.sqlglot_ast import pop_with
from build_query.assertion_corrector import correct_assertions
from build_query.assertion_modifier import modify_assertions
from build_query.accept_validation import accept_validation
from build_query.description_proposal import (
    apply_description,
    propose_description_node,
    reject_description,
)
from build_query.conversational_agent import conversational_agent
from build_query.data_patcher import data_patcher_node
from build_query.debug_node import debug_test_node
from build_query.delete_test_node import delete_test_node
from build_query.assertion_generator import generate_assertions
from build_query.final_response_node import final_response
from build_query.examples_executor import run_on_examples
from build_query.examples_generator import generate_examples
from build_query.suggestions_node import (
    generate_single_suggestion,
    generate_suggestions,
)
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
from utils.saver import history_saver, get_history_from_state, get_message_type

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
        # Retire la clause WITH entière : `ctes.clear()` vide la liste mais laisse le
        # nœud `With` (→ `WITH ` orphelin non re-parsable) et `set("with", None)` est un
        # no-op quand la clé est `with_` (sqlglot ≥ 30). pop_with gère les deux clés.
        pop_with(final_ast)
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
        from build_query.examples_generator import (
            _compact_cte_trace,
            _get_failing_cte_from_results,
        )

        failing_cte, trace = _get_failing_cte_from_results(state.get("messages", []))
        # Désync prémisse↔entrée : pas de CTE en échec (le résultat n'est pas vide, ce
        # sont les valeurs injectées qui ≠ prémisse) → digest dédié plutôt que le fallback
        # générique qui laisserait croire à un blocage structurel inexistant.
        latest_eval = next(
            (
                m
                for m in reversed(state.get("messages", []))
                if get_message_type(m) == MsgType.EVALUATION
            ),
            None,
        )
        is_premise_desync = (
            (latest_eval.additional_kwargs.get("diagnostic") or {}).get("kind")
            == "premise_desync"
            if latest_eval
            else False
        )
        if failing_cte:
            digest = f"toujours 0 ligne — étape bloquante inchangée ({failing_cte})"
        elif is_premise_desync:
            digest = "données d'entrée toujours ≠ prémisse utilisateur"
        else:
            digest = "verdict toujours Insuffisant (bad_data)"
        outcome: dict = {"blocking_cte": failing_cte, "digest": digest}
        # Trace structuré complet (profil row_count de TOUTES les CTE + valeurs des
        # pivots + mismatch jointure) : conservé par tentative pour que l'agent lise
        # l'ÉVOLUTION (erreur 1 → erreur 2) plutôt qu'un symptôme isolé.
        if trace:
            outcome["cte_trace"] = _compact_cte_trace(failing_cte, trace)
        attempts[-1] = {**attempts[-1], "outcome": outcome}
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


def _should_resume_batch(state: QueryState, test: dict) -> tuple[bool, int, int]:
    """Décide si ce run reprend une boucle multi-tests interrompue.

    Scénario : l'utilisateur a demandé N tests (``tests_target``, persisté sur le modèle dès
    le début de la boucle), une coupure (réseau, crash LLM) est survenue après K<N tests
    déjà checkpointés sur disque. Un simple re-run de la MÊME requête (sans saisie chat, sans
    intention concurrente) doit alors construire les N-K tests restants au lieu de repartir de
    zéro (ce qui dupliquerait le nominal) ou de clore direct via ``final_response``.

    Retourne ``(resume, target, existing)`` — ``resume`` vrai seulement si la requête est
    inchangée, des tests existent mais en nombre insuffisant, et aucun autre flux n'est
    demandé (chat, suggestion, assertion, validation, rerun_all)."""
    if state.get("input") or state.get("user_tables"):
        return False, 0, 0
    if any(
        state.get(flag)
        for flag in (
            "suggestion_intent",
            "assertion_only",
            "rerun_only",
            "regenerate_suggestions",
            "validate_intent",
            "rerun_all_tests",
        )
    ):
        return False, 0, 0
    # target = objectif persisté du batch (source de vérité), à défaut celui de la requête.
    target = test.get("tests_target") or state.get("tests_target") or 1
    existing = len(test.get("test_cases") or [])
    resume = 0 < existing < target
    return resume, target, existing


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

    # Persiste l'objectif du batch dès le DÉBUT de la 1ʳᵉ génération (avant qu'un seul test
    # ne soit construit) : ainsi, une coupure en cours de boucle laisse sur disque le N
    # demandé, que la reprise lit pour savoir combien de tests il reste à construire.
    incoming_target = state.get("tests_target") or 1
    if not has_existing_tests and incoming_target > 1:
        update_test(state["session"], {"tests_target": incoming_target})

    # Reprise d'une boucle multi-tests interrompue : tests sur disque mais en nombre < N.
    resume_batch, resume_target, resume_existing = _should_resume_batch(state, test)
    resume_fields: Dict[str, Any] = {}
    if resume_batch:
        resume_fields = {
            "resume_batch": True,
            "tests_target": resume_target,
            # auto_tests_built compte les tests EXTRA déjà construits (hors nominal) : E tests
            # sur disque ⇒ E-1 extras. generate_single_suggestion reprendra à partir de là.
            "auto_tests_built": max(resume_existing - 1, 0),
        }
        logger.diag(
            "[pre_routing] reprise batch : %d/%d tests sur disque → construction des manquants",
            resume_existing,
            resume_target,
        )

    stored_sql = (test.get("sql") or "").strip()
    stored_optimised_sql = (test.get("optimized_sql") or "").strip()
    stored_used_columns = test.get("used_columns") or []
    stored_query_decomposed = test.get("query_decomposed") or ""
    # Catalogue des paths UNION ALL : seedé dans le state sur les tours SANS
    # re-validation (chat / agent set_target_path) pour que le générateur, l'agent et
    # les suggestions le retrouvent. Quand le SQL change, le validateur le recalcule.
    stored_path_plans = test.get("path_plans")
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
            "path_plans": stored_path_plans,
            **resume_fields,
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
        "path_plans": stored_path_plans,
        **resume_fields,
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
    if tool_call in ("generate_test_data", "update_test_data", "set_target_path"):
        return "generator"
    if tool_call == "delete_test":
        return "delete_test_node"
    if tool_call == "update_test_description":
        # Jamais d'application directe : on PROPOSE la nouvelle description (l'utilisateur
        # valide depuis le panneau via apply_description / reject_description).
        return "propose_description_node"
    if tool_call == "generate_suggestions":
        return "suggestions_generator"
    if tool_call in ("run_cte", "debug_batch"):
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
    # Désync description↔réel (données valides) : pas de boucle — l'état est sauvé et un
    # VALIDATION_PROMPT a été émis ; on attend la décision de l'utilisateur (Valider / Corriger).
    # needs_validation = écart de cardinalité ; bad_description = écart de valeur de SORTIE ;
    # bad_input_description = écart description ↔ valeurs d'ENTRÉE injectées (TICKET-2).
    if feedback in ("needs_validation", "bad_description", "bad_input_description"):
        logger.diag("[route_evaluator] → history_saver (%s)", feedback)
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
    # 1ʳᵉ génération : boucle multi-tests puis suggestions de couverture.
    # L'utilisateur a demandé N tests au total (tests_target) ; le nominal compte pour 1,
    # on auto-construit N-1 tests supplémentaires depuis des suggestions uniques. Tant qu'il
    # reste des tests à construire → generate_single_suggestion (qui enchaîne sur le
    # conversational_agent). Sinon, dernière étape : suggestions_generator (panneau, pas de
    # boucle). Ce point n'est atteint qu'une fois le test courant RÉGLÉ (les branches
    # bad_data / needs_validation / etc. ci-dessus sont prioritaires).
    # Boucle active à la 1ʳᵉ génération (pas de tests préexistants) OU lors de la reprise d'un
    # batch interrompu (resume_batch) : dans les deux cas on construit/complète jusqu'à N.
    if not state.get("has_existing_tests") or state.get("resume_batch"):
        target = state.get("tests_target") or 1
        built = state.get("auto_tests_built") or 0
        if built < target - 1:
            logger.diag(
                "[route_evaluator] → generate_single_suggestion (boucle %d/%d)",
                built + 1,
                target - 1,
            )
            return "generate_single_suggestion"
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
    # l'agent conversationnel qui a appelé generate_suggestions OU reprise post-validation
    # (revalidated). Dans tous ces cas, pas de message de clôture « j'ai généré un test »
    # (faux : seules les suggestions ont changé, ou un test a été validé via son propre
    # message EVALUATION) — le panneau se rafraîchit via le message SSE.
    if (
        state.get("regenerate_suggestions")
        or state.get("agent_tool_call") == "generate_suggestions"
        or state.get("revalidated")
    ):
        logger.diag("[route_after_suggestions] → history_saver (régénération)")
        return "history_saver"
    logger.diag("[route_after_suggestions] → final_response")
    return "final_response"


def route_after_accept(state: QueryState):
    """Sortie de ``accept_validation``. Sur validation réussie (``revalidated``), on reprend
    le pipeline comme après une évaluation : régénération des suggestions de couverture, puis
    clôture. Sur no-op (test introuvable, test_index absent), rien à faire → ``history_saver``.
    Module-level pour rester testable (ne dépend que de ``state``)."""
    if state.get("revalidated"):
        logger.diag(
            "[route_after_accept] → suggestions_generator (reprise post-validation)"
        )
        return "suggestions_generator"
    logger.diag("[route_after_accept] → history_saver (no-op)")
    return "history_saver"


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
    add_timed_node("propose_description_node", propose_description_node)
    add_timed_node("apply_description", apply_description)
    add_timed_node("reject_description", reject_description)
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
    add_timed_node("generate_single_suggestion", generate_single_suggestion)
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
        if route == "apply_description":
            logger.diag("[route_input] → apply_description")
            return "apply_description"
        if route == "reject_description":
            logger.diag("[route_input] → reject_description")
            return "reject_description"
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
        if route == "resume_batch":
            logger.diag(
                "[route_input] → generate_single_suggestion (reprise boucle multi-tests)"
            )
            return "generate_single_suggestion"
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
    builder.add_edge("propose_description_node", "history_saver")
    builder.add_edge("apply_description", "history_saver")
    builder.add_edge("reject_description", "history_saver")
    builder.add_conditional_edges("accept_validation", route_after_accept)
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
    # Boucle multi-tests : la suggestion unique enchaîne sur le conversational_agent, qui
    # construit le test (generate_test_data → generator → executor → test_evaluator), puis
    # route_evaluator décide de reboucler ou de clore via suggestions_generator.
    builder.add_edge("generate_single_suggestion", "conversational_agent")
    builder.add_edge("final_response", "history_saver")
    builder.add_edge("other", "history_saver")
    builder.add_edge("history_saver", END)

    graph = builder.compile()
    return graph
