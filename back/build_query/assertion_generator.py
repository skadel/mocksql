import asyncio
import io
import json
import logging
import uuid
from typing import Any, Dict, Optional

import pandas as pd
from langchain_core.messages import AIMessage

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.examples import DB_PATH, initialize_duckdb
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)


async def generate_assertions(state: QueryState) -> Dict[str, Any]:
    """
    LangGraph node: generates assertions and evaluates test quality via a single LLM call.

    Runs after executor whenever DuckDB produced non-empty results (status=complete).
    For empty_results or bad_data_error the executor routes directly to test_evaluator,
    bypassing this node entirely — no LLM work is done on data that produced nothing.
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

    current_test = find_current_test(all_tests, state.get("test_index"))
    if current_test is None or current_test.get("status") != "complete":
        return {}

    results_json = current_test.get("results_json", "[]")
    try:
        result_df = pd.read_json(io.StringIO(results_json), orient="records")
    except Exception:
        result_df = pd.DataFrame()

    if result_df.empty:
        return {}

    session_id = state["session"].replace("-", "_")
    test_index = current_test.get("test_index", "1")
    suffix = f"{session_id}{test_index}"
    view_name = f"__result__{suffix}"

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    test_data = current_test.get("data", {})
    test_description = current_test.get("unit_test_description", "")

    from build_query.examples_executor import (
        _assertion_to_executable,
        _evaluate_assertions_with_retry,
        _fix_logically_failing_assertions,
        _generate_assertions_and_evaluate,
        _generate_diagnostic,
    )
    from utils.timing import atimed

    updated_test: Optional[Dict[str, Any]] = None

    with initialize_duckdb(DB_PATH) as con:
        con.register(view_name, result_df)
        try:
            retry_kwargs = dict(
                view_name=view_name,
                con=con,
                duckdb_sql=sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
            )

            eval_result = await _generate_assertions_and_evaluate(
                duckdb_sql=sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
            )

            async with atimed("assertion_gen:eval+fix"):
                # _Assertion n'expose que description/expected_condition ; le SQL dbt-style
                # exécutable est dérivé ici via _assertion_to_executable. Passer model_dump()
                # brut laisserait `sql` vide → con.execute("") → None → crash .fetchdf().
                assertion_results = await _evaluate_assertions_with_retry(
                    [_assertion_to_executable(a) for a in eval_result.assertions],
                    **retry_kwargs,
                )
                try:
                    assertion_results = await _fix_logically_failing_assertions(
                        assertion_results, **retry_kwargs
                    )
                except asyncio.CancelledError:
                    logger.warning(
                        "[assertion_generator] fixer interrompu (CancelledError) — résultats partiels conservés"
                    )

        finally:
            try:
                con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
            except Exception:
                pass

    has_failing = any(not a.get("passed") for a in assertion_results)
    if has_failing:
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": "Insuffisant",
            "reason_type": "bad_assertions",
            "evaluation_explanation": "Les assertions générées ne correspondent pas au résultat de la requête.",
        }
        updated_test.pop("assertion_fix", None)
    else:
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": eval_result.verdict,
            "reason_type": eval_result.reason_type,
            "evaluation_explanation": eval_result.explanation,
        }
        if eval_result.assertion_fix is not None:
            updated_test["assertion_fix"] = eval_result.assertion_fix.model_dump()
        if (
            eval_result.diagnostic is not None
            and eval_result.diagnostic.root_cause
            != "Données d'entrée insuffisantes ou incohérentes avec la logique SQL"
        ):
            updated_test["diagnostic"] = eval_result.diagnostic.model_dump()
        elif updated_test.get("reason_type") == "bad_data":
            diag = await _generate_diagnostic(
                duckdb_sql=sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
                eval_reasoning=eval_result.reasoning,
            )
            if diag:
                updated_test["diagnostic"] = diag.model_dump()

    updated_all_tests = [
        updated_test if t.get("test_index") == current_test.get("test_index") else t
        for t in all_tests
    ]

    parent = last_results.additional_kwargs.get("parent") or state.get(
        "parent_message_id"
    )
    sql_kw = state.get("query", "").strip()
    optimized_kw = state.get("optimized_sql", "").strip()

    return {
        "messages": [
            AIMessage(
                content=json.dumps(
                    updated_all_tests, ensure_ascii=False, indent=2, default=str
                ),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    **last_results.additional_kwargs,
                    "type": MsgType.RESULTS,
                    "parent": parent,
                    "request_id": state.get("request_id"),
                    **({"sql": sql_kw} if sql_kw else {}),
                    **({"optimized_sql": optimized_kw} if optimized_kw else {}),
                },
            )
        ],
    }
