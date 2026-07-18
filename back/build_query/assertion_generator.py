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
        # dtype=False : NE PAS ré-inférer les types depuis le JSON. Sans lui, une colonne
        # VARCHAR de chiffres (`'001'`) est coercée en int64 (`1`) et une colonne NULL en
        # float64/NaN → le juge, qui lit le schéma et l'échantillon, épingle des artefacts
        # de sérialisation (`CD = 1` au lieu de `'001'`). Cf. incident c2 / P1-1.
        result_df = pd.read_json(
            io.StringIO(results_json), orient="records", dtype=False
        )
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
    # Focus de génération (branche UNION ALL) : transmis au juge en CONTEXTE seulement.
    # L'exécution et l'évaluation portent sur le script complet (`sql` = optimized_sql complet,
    # non slicé) — le focus n'a servi qu'à cibler les données d'entrée.
    focus_path = current_test.get("target_path", "")

    from build_query.examples_executor import (
        _assertion_to_executable,
        _autoscope_failing_assertions,
        _cardinality_pin,
        _evaluate_assertions_with_retry,
        _fix_logically_failing_assertions,
        _generate_assertions_and_evaluate,
        _generate_diagnostic,
        _is_bare_rowcount_pin,
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
                focus_path=focus_path,
                con=con,
                view_name=view_name,
            )

            async with atimed("assertion_gen:eval+fix"):
                # _Assertion n'expose que description/expected_condition ; le SQL dbt-style
                # exécutable est dérivé ici via _assertion_to_executable. Passer model_dump()
                # brut laisserait `sql` vide → con.execute("") → None → crash .fetchdf().
                # Pin de cardinalité déterministe (COUNT(*) = N, hors LLM) appendé en fin de
                # suite ; les pins bruts émis par le LLM malgré la consigne sont dédoublonnés.
                executables = []
                for a in eval_result.assertions:
                    ex = _assertion_to_executable(a)
                    if not _is_bare_rowcount_pin(ex):
                        executables.append(ex)
                executables.append(_cardinality_pin(len(result_df)))
                assertion_results = await _evaluate_assertions_with_retry(
                    executables,
                    **retry_kwargs,
                )
                # Rattrapage déterministe (sans LLM) du pattern « format long » : une
                # expected_condition conjonctive (ex. `indicateur='nb_cartes' AND valeur=2974`)
                # échoue à tort sur les autres lignes → on relève le sélecteur en `scope`.
                # Exécuté avant le fixer LLM, qui ne traite ensuite que le résidu.
                assertion_results = _autoscope_failing_assertions(
                    assertion_results, view_name, con
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

    # Fix 3 — garde-fou « CTE amont vide ». Un verdict Insuffisant sur un résultat NON
    # vide peut masquer une CTE amont REQUISE vide (agrégat d'un ensemble vide → 1 ligne
    # NULL, puis LEFT JOIN + COALESCE d'échafaudage) : le circuit empty_results ne se
    # déclenche jamais, et les assertions blanchies sur les lignes dégénérées laissent le
    # test en Insuffisant mort (reason_type None → aucune boucle). On sonde À LA DEMANDE
    # (chemin Insuffisant, rare — zéro coût happy path), et si une CTE requise est vide sur
    # un test happy-path on reclasse en bad_data → boucle de correction ciblée (sf_bq093).
    # On ne double-traite PAS les cas où le LLM route déjà ailleurs (validation, bad_data).
    empty_cte_diag = None
    if eval_result.reason_type not in (
        "needs_validation",
        "bad_description",
        "bad_input_description",
        "bad_data",
    ) and (eval_result.verdict == "Insuffisant" or has_failing):
        from build_query.examples_executor import (
            probe_empty_upstream_cte,
        )

        with initialize_duckdb(DB_PATH) as probe_con:
            empty_cte_diag = await probe_empty_upstream_cte(
                state, current_test, probe_con
            )

    # Désync description↔cardinalité : prioritaire sur `has_failing`. Les assertions sont
    # au niveau ligne (conditions positives sur __result__) et passent généralement sur la
    # vraie sortie ; l'écart est entre le NOMBRE de lignes annoncé et le réel. On ne corrige
    # pas en boucle : on sauve l'état et on demande validation à l'utilisateur (cf.
    # test_evaluator → VALIDATION_PROMPT, accept_validation). Voir aussi state.py.
    if empty_cte_diag is not None:
        # CTE amont requise vide sur un test happy-path : la vraie cause est des données
        # d'entrée qui ne franchissent pas un filtre/jointure, pas les assertions. On route
        # vers la boucle bad_data avec le diagnostic prédicat↔valeurs (cf. Fix 3).
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": "Insuffisant",
            "reason_type": "bad_data",
            "evaluation_explanation": (
                "Une étape intermédiaire de la requête ne produit aucune ligne : "
                "les données d'entrée ne franchissent pas un filtre ou une jointure."
            ),
            "diagnostic": empty_cte_diag,
        }
        updated_test.pop("assertion_fix", None)
        updated_test.pop("expected_row_count", None)
    elif eval_result.reason_type == "needs_validation":
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": "Insuffisant",
            "reason_type": "needs_validation",
            "evaluation_explanation": eval_result.explanation,
            "expected_row_count": eval_result.expected_row_count,
            "corrected_description": eval_result.corrected_description,
            "corrected_name": eval_result.corrected_name,
        }
        updated_test.pop("assertion_fix", None)
        updated_test.pop("diagnostic", None)
    elif eval_result.reason_type == "bad_description":
        # Désync description↔valeur de sortie concrète : données valides, narratif faux. Comme
        # needs_validation, on délègue à l'humain (Valider / Corriger) plutôt que de boucler —
        # on porte la description corrigée pour qu'accept_validation l'applique au clic.
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": "Insuffisant",
            "reason_type": "bad_description",
            "evaluation_explanation": eval_result.explanation,
            "corrected_description": eval_result.corrected_description,
            "corrected_name": eval_result.corrected_name,
        }
        updated_test.pop("assertion_fix", None)
        updated_test.pop("diagnostic", None)
        updated_test.pop("expected_row_count", None)
    elif eval_result.reason_type == "bad_input_description":
        # Désync description↔valeurs d'ENTRÉE injectées (TICKET-2) : données valides,
        # narratif d'entrée faux. Même délégation que bad_description — on porte la
        # description corrigée pour qu'accept_validation l'applique au clic. `user_premise`
        # (TICKET-1), s'il est présent, est conservé via `**current_test`.
        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": "Insuffisant",
            "reason_type": "bad_input_description",
            "evaluation_explanation": eval_result.explanation,
            "corrected_description": eval_result.corrected_description,
            "corrected_name": eval_result.corrected_name,
        }
        updated_test.pop("assertion_fix", None)
        updated_test.pop("diagnostic", None)
        updated_test.pop("expected_row_count", None)
    elif has_failing:
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
