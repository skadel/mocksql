import json
import logging
import uuid
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)


class _ReevalResult(BaseModel):
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    explanation: str


async def _reevaluate_empty_result(
    state: QueryState, current_test: dict, last_results_msg: AIMessage
) -> dict:
    """LLM re-evaluation when the conversational agent suspects bad_data was a false positive."""
    test_desc = current_test.get("unit_test_description", "")
    input_data = current_test.get("data", {})
    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    reason = state.get("reevaluation_context", "")
    eval_test_index = current_test.get("test_index")

    try:
        input_summary = json.dumps(input_data, ensure_ascii=False, indent=2)[:800]
    except Exception:
        input_summary = str(input_data)[:800]

    prompt = f"""SQL testé (dialecte {state.get("dialect", "bigquery")}) :
{sql}

Scénario du test : {test_desc}

Données d'entrée injectées dans DuckDB :
{input_summary}

Résultat DuckDB : 0 lignes retournées.

Justification de l'agent de diagnostic (pourquoi 0 lignes serait correct) :
{reason}

Évalue la qualité de ce test. Le fait que la requête retourne 0 lignes est-il cohérent avec le scénario décrit et les données fournies ?
- "Excellent" ou "Bon" si 0 lignes est bien le comportement attendu pour ce scénario.
- "Insuffisant" si les données d'entrée ne permettent pas de valider le scénario malgré la justification."""

    llm = make_llm().with_structured_output(_ReevalResult)
    try:
        logger.diag("[evaluator] PROMPT LLM (réévaluation):\n%s", prompt[:3000])
        result = await llm.ainvoke(
            [
                SystemMessage(
                    content=MOCKSQL_PRODUCT_PREAMBLE
                    + "\n\nTu réévalues ici la qualité d'un test (verdict argumenté) pour l'utilisateur."
                ),
                HumanMessage(content=prompt),
            ]
        )
        verdict = result.verdict
        explanation = result.explanation
    except Exception as exc:
        logger.warning("[evaluator] _reevaluate_empty_result failed: %s", exc)
        verdict = "Insuffisant"
        explanation = "Réévaluation impossible — erreur LLM."

    logger.diag(
        "[evaluator] réévaluation après request_reevaluation : verdict=%s — %s",
        verdict,
        explanation,
    )
    evaluation_feedback = "bad_data" if verdict == "Insuffisant" else None
    return {
        "messages": [
            AIMessage(
                content=f"**{verdict}** — {explanation}",
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EVALUATION,
                    "parent": last_results_msg.id,
                    "request_id": state.get("request_id"),
                    "test_index": eval_test_index,
                },
            )
        ],
        "evaluation_feedback": evaluation_feedback,
        "status": "complete",
        "reevaluation_context": None,
    }


async def evaluate_tests(state: QueryState):
    """
    Lit le verdict pré-calculé par l'executor (embedded dans le résultat du test),
    émet le message EVALUATION et gère le routing (bad_data / bad_assertions / too_many_rows).

    Le verdict et les assertions sont produits en un seul appel LLM dans _generate_assertions_and_evaluate
    (examples_executor.py). Ce nœud ne fait plus d'appel LLM.
    """
    if state.get("error"):
        return {}

    results_msgs = [
        m for m in state.get("messages", []) if get_message_type(m) == MsgType.RESULTS
    ]
    if not results_msgs:
        return {}

    last_results = results_msgs[-1]
    try:
        all_tests = json.loads(last_results.content)
    except Exception:
        return {}

    if not isinstance(all_tests, list):
        all_tests = [all_tests]

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    current_test = find_current_test(all_tests, state.get("test_index"))
    if current_test is None:
        return {}

    # Re-evaluation requested by conversational_agent: skip automatic bad_data classification.
    if current_test.get("status") == "empty_results" and state.get(
        "reevaluation_context"
    ):
        return await _reevaluate_empty_result(state, current_test, last_results)

    # DuckDB data error (Invalid Input Error, Conversion Error): wrong format/type in generated data.
    if current_test.get("status") == "bad_data_error":
        exec_error = current_test.get("exec_error", "")
        display_reason = (
            "Les données générées contiennent des valeurs au mauvais format."
        )
        diag = f"Erreur DuckDB lors de l'exécution :\n{exec_error}"
        gen_retries = (
            state.get("gen_retries") if state.get("gen_retries") is not None else 3
        )
        if gen_retries == 0:
            stub_test = dict(current_test)
            for table_name in stub_test.get("data", {}):
                stub_test["data"][table_name] = []
            stub_test["tags"] = list(
                set(
                    stub_test.get("tags", [])
                    + ["FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED"]
                )
            )
            return {
                "examples": [
                    AIMessage(
                        content=json.dumps(stub_test),
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EXAMPLES,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "evaluation_feedback": "bad_data",
                "status": "complete",
            }
        return {
            "messages": [
                AIMessage(
                    content=f"**Insuffisant** — {display_reason}",
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": current_test.get("test_index"),
                        "diag": diag,
                        "intermediate": True,
                    },
                )
            ],
            "evaluation_feedback": "bad_data",
            "status": "empty_results",
        }

    # Fast path: empty_results due to structural SQL constraint (no LLM needed).
    if current_test.get("status") == "empty_results":
        from build_query.constraint_simplifier import (
            check_correlated_aggregate_cardinality,
            check_having_cardinality,
        )

        dialect = state.get("dialect", "bigquery")
        cardinality_error: str | None = None
        for _check in (
            check_having_cardinality,
            check_correlated_aggregate_cardinality,
        ):
            try:
                _check(sql, dialect)
            except ValueError as exc:
                cardinality_error = str(exc)
                break

        if cardinality_error:
            logger.diag("[evaluator] too_many_rows détecté: %s", cardinality_error)
            return {
                "messages": [
                    AIMessage(
                        content=f"**Insuffisant** — {cardinality_error}",
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EVALUATION,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                            "test_index": current_test.get("test_index"),
                        },
                    )
                ],
                "evaluation_feedback": "too_many_rows",
                "status": "complete",
            }

        gen_retries = (
            state.get("gen_retries") if state.get("gen_retries") is not None else 3
        )

        cte_trace = current_test.get("cte_trace", {})
        failing_cte = current_test.get("failing_cte", "")
        if cte_trace:
            # Source unique de format de trace (compressée, centrée sur l'étape
            # bloquante) — partagée avec le feedback du générateur via _build_eval_messages.
            from build_query.examples_generator import _format_cte_trace_hint

            diag = _format_cte_trace_hint(failing_cte, cte_trace)
            display_reason = (
                f"La CTE `{failing_cte}` est vide — les données ne satisfont pas ses contraintes."
                if failing_cte
                else "Les données d'entrée ne produisent aucun résultat."
            )
        elif failing_cte:
            diag = f"La requête retourne 0 ligne — la CTE `{failing_cte}` est vide. Les données d'entrée ne satisfont pas les contraintes de jointure ou de filtre."
            display_reason = f"La CTE `{failing_cte}` est vide — les données ne satisfont pas ses contraintes."
        else:
            diag = "La requête retourne 0 ligne. Les données d'entrée ne produisent aucun résultat."
            display_reason = "Les données d'entrée ne produisent aucun résultat."

        logger.diag(
            "[evaluator] empty_results sans contrainte structurelle → bad_data, retries=%d",
            gen_retries,
        )

        if gen_retries == 0:
            logger.warning(
                "[evaluator] Circuit breaker déclenché pour le test %s",
                current_test.get("test_index"),
            )
            stub_test = dict(current_test)
            for table_name in stub_test.get("data", {}):
                stub_test["data"][table_name] = []
            stub_test["tags"] = list(
                set(
                    stub_test.get("tags", [])
                    + ["FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED"]
                )
            )

            return {
                "examples": [
                    AIMessage(
                        content=json.dumps(stub_test),
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EXAMPLES,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "evaluation_feedback": "bad_data",
                "status": "complete",
            }

        state_update: dict = {
            "messages": [
                AIMessage(
                    content=f"**Insuffisant** — {display_reason}",
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": current_test.get("test_index"),
                        "diag": diag,
                        "intermediate": True,
                    },
                )
            ],
            "evaluation_feedback": "bad_data",
            "status": "empty_results",
            # Route straight to the generator for a holistic regeneration targeting
            # the failing CTE (cte_trace travels in the RESULTS message). Bypasses the
            # conversational_agent, whose single-field patches cannot fix a 0-row query.
            # Decrement gen_retries here since we skip the agent (which normally does it).
            "empty_results_regen": True,
            "gen_retries": gen_retries - 1,
        }
        return state_update

    verdict = current_test.get("verdict")
    reason_type = current_test.get("reason_type")
    explanation = current_test.get("evaluation_explanation", "")

    if not verdict:
        return {}

    logger.diag(
        "[evaluator] verdict=%s reason_type=%s — %s", verdict, reason_type, explanation
    )

    eval_test_index = current_test.get("test_index")
    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 1
    )
    debug_retries = (
        state.get("debug_retries") if state.get("debug_retries") is not None else 2
    )

    evaluation_feedback = (
        reason_type if verdict == "Insuffisant" and reason_type else None
    )
    triggers_agent_retry = evaluation_feedback == "bad_data" and not state.get(
        "assertion_only"
    )
    triggers_assertion_retry = evaluation_feedback == "bad_assertions"

    new_status = "complete"
    if triggers_agent_retry and gen_retries > 0:
        new_status = "empty_results"
    elif triggers_assertion_retry and debug_retries > 0:
        new_status = "bad_assertions"

    diagnostic = current_test.get("diagnostic")
    eval_msg_id = str(uuid.uuid4())
    eval_msg_kwargs: dict = {
        "type": MsgType.EVALUATION,
        "parent": last_results.id,
        "request_id": state.get("request_id"),
        "test_index": eval_test_index,
    }
    if diagnostic and evaluation_feedback == "bad_data":
        eval_msg_kwargs["diagnostic"] = diagnostic

    messages: list = [
        AIMessage(
            content=f"**{verdict}** — {explanation}",
            id=eval_msg_id,
            additional_kwargs=eval_msg_kwargs,
        )
    ]

    if diagnostic and evaluation_feedback == "bad_data":
        messages.append(
            AIMessage(
                content=json.dumps(diagnostic),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.BAD_DATA_DIAGNOSTIC,
                    "parent": eval_msg_id,
                    "test_index": eval_test_index,
                    "request_id": state.get("request_id"),
                },
            )
        )

    state_update: dict = {
        "messages": messages,
        "evaluation_feedback": evaluation_feedback,
        "status": new_status,
        "empty_results_regen": False,
    }

    if triggers_assertion_retry:
        state_update["debug_retries"] = debug_retries - 1

    return state_update
