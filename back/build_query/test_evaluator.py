import json
import logging
import uuid

from langchain_core.messages import AIMessage

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)


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

        # No structural constraint → données incorrectes, déclenche le retry conversational_agent
        gen_retries = state.get("gen_retries") if state.get("gen_retries") is not None else 1
        failing_cte = current_test.get("failing_cte", "")
        if failing_cte:
            explanation = f"La requête retourne 0 ligne — la CTE `{failing_cte}` est vide. Les données d'entrée ne satisfont pas les contraintes de jointure ou de filtre."
        else:
            explanation = "La requête retourne 0 ligne. Les données d'entrée ne produisent aucun résultat."
        logger.diag("[evaluator] empty_results sans contrainte structurelle → bad_data, retries=%d", gen_retries)
        state_update: dict = {
            "messages": [
                AIMessage(
                    content=f"**Insuffisant** — {explanation}",
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": current_test.get("test_index"),
                    },
                )
            ],
            "evaluation_feedback": "bad_data",
            "status": "empty_results",
        }
        if gen_retries > 0:
            state_update["gen_retries"] = gen_retries - 1
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

    evaluation_feedback = (
        reason_type if verdict == "Insuffisant" and reason_type else None
    )
    triggers_agent_retry = evaluation_feedback == "bad_data" and not state.get(
        "assertion_only"
    )
    triggers_assertion_fix = (
        evaluation_feedback == "bad_assertions"
        and not state.get("assertion_only")
        and current_test.get("assertion_fix") is not None
    )

    state_update: dict = {
        "messages": [
            AIMessage(
                content=f"**{verdict}** — {explanation}",
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EVALUATION,
                    "parent": last_results.id,
                    "request_id": state.get("request_id"),
                    "test_index": eval_test_index,
                },
            )
        ],
        "evaluation_feedback": evaluation_feedback,
        "status": "empty_results"
        if (triggers_agent_retry and gen_retries > 0)
        else "complete",
    }

    if triggers_agent_retry or triggers_assertion_fix:
        state_update["gen_retries"] = gen_retries - 1

    if triggers_assertion_fix:
        fix = current_test["assertion_fix"]
        updated_test = {
            **current_test,
            "test_name": fix["test_name"],
            "unit_test_description": fix["unit_test_description"],
            "unit_test_build_reasoning": fix["unit_test_build_reasoning"],
            "tags": fix["tags"],
            "suggestions": fix["suggestions"],
        }
        parent = state.get("user_message_id") or state.get("parent_message_id")
        state_update["examples"] = [
            AIMessage(
                content=json.dumps(updated_test),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EXAMPLES,
                    "parent": parent,
                    "request_id": state.get("request_id"),
                    "generated_test_index": updated_test.get("test_index"),
                    "assertion_only": True,
                },
            )
        ]
        logger.diag(
            "[evaluator] assertion_fix inline: test_name=%r", fix["test_name"]
        )

    return state_update
