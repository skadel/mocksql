import io
import logging
from typing import List, Any, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from models.message_service import delete_all_messages
from storage.config import is_initialized
from storage.test_repository import get_test, update_test
from utils.saver import common_history_retriever

logger = logging.getLogger(__name__)

router = APIRouter()


class MessageRequest(BaseModel):
    modelId: str


class ClearHistoryRequest(BaseModel):
    sessionId: str


class PatchTestsRequest(BaseModel):
    sessionId: str
    tests: List[Any]


class ApplyAssertionsRequest(BaseModel):
    sessionId: str
    testIndex: Any  # str | int — comparé via str()
    assertions: List[dict]  # [{description, expected_condition}]


class PatchSqlRequest(BaseModel):
    sessionId: str
    sql: str
    optimized_sql: str = ""
    tests: Optional[List[Any]] = None
    test_results: Optional[List[Any]] = None
    restored_message_id: Optional[str] = None
    last_error: Optional[str] = None


class DismissSuggestionRequest(BaseModel):
    sessionId: str
    suggestion: str


class QueueInstructionRequest(BaseModel):
    sessionId: str
    text: str
    dialect: str = "bigquery"
    parentMessageId: Optional[str] = None


class FlushInstructionsRequest(BaseModel):
    sessionId: str


@router.post("/getMessages")
async def get_messages(body: MessageRequest):
    if not is_initialized():
        raise HTTPException(
            status_code=400,
            detail="Projet non initialisé. Lancez 'mocksql init' dans votre répertoire de travail pour commencer.",
        )
    try:
        history = await common_history_retriever(body.modelId, filtered_types=[])
        if history is None:
            raise HTTPException(status_code=404, detail="Session not found")

        test = get_test(body.modelId)
        sql = test.get("sql") if test else None
        optimized_sql = test.get("optimized_sql") if test else None
        last_error = test.get("last_error") if test else ""
        test_results = test.get("test_cases", []) if test else []
        suggestions = test.get("suggestions", []) if test else []
        suggestion_rationales = test.get("suggestion_rationales", {}) if test else {}
        restored_message_id = test.get("restored_message_id") if test else None
        # Objectif du batch multi-tests (N demandé). Sert au front à détecter une boucle
        # interrompue (len(test_results) < tests_target) et à proposer la reprise.
        tests_target = test.get("tests_target") if test else None

        # Fallback: extract from last results message in history
        if not test_results:
            import json

            for msg in reversed(history):
                if msg.additional_kwargs.get("type") == "results":
                    try:
                        test_results = json.loads(msg.content)
                    except Exception:
                        pass
                    break

        return {
            "messages": history,
            "sql": sql,
            "optimized_sql": optimized_sql,
            "test_results": test_results,
            "suggestions": suggestions,
            "suggestion_rationales": suggestion_rationales,
            "restored_message_id": restored_message_id,
            "tests_target": tests_target,
            "last_error": last_error or "",
            "sql_history": [],
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(
            "Erreur lors du chargement des messages pour la session %s", body.modelId
        )
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/models/sql")
async def patch_model_sql(body: PatchSqlRequest):
    try:
        updates: dict = {
            "sql": body.sql,
            "optimized_sql": body.optimized_sql,
        }
        if body.tests is not None:
            updates["test_cases"] = body.tests
        if body.test_results is not None:
            updates["test_cases"] = body.test_results
        if body.restored_message_id is not None:
            updates["restored_message_id"] = body.restored_message_id or None
        if body.last_error is not None:
            updates["last_error"] = body.last_error

        update_test(body.sessionId, updates)
        return {"ok": True}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.patch("/models/tests")
async def patch_model_tests(body: PatchTestsRequest):
    try:
        update_test(body.sessionId, {"test_cases": body.tests})
        return {"ok": True}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/tests/apply_assertions")
async def apply_assertions(body: ApplyAssertionsRequest):
    """Ré-exécute une liste d'assertions fournie par l'utilisateur sur les données
    inchangées du test (modif / suppression / ajout assertion par assertion), recalcule
    un verdict déterministe, persiste et renvoie les résultats.

    Aucun appel LLM : on réutilise l'exécuteur d'assertions dbt-style existant.
    """
    from build_query.examples_executor import (
        _assertion_sql_from_condition,
        _evaluate_assertions,
    )
    from utils.examples import DB_PATH, initialize_duckdb

    try:
        test = get_test(body.sessionId)
        test_cases: List[dict] = (test or {}).get("test_cases", [])
        current = next(
            (t for t in test_cases if str(t.get("test_index")) == str(body.testIndex)),
            None,
        )
        if current is None:
            raise HTTPException(status_code=404, detail="Test introuvable")

        try:
            result_df = pd.read_json(
                io.StringIO(current.get("results_json", "[]")), orient="records"
            )
        except Exception:
            result_df = pd.DataFrame()

        session_id = body.sessionId.replace("-", "_")
        view_name = f"__result__{session_id}{current.get('test_index', '1')}"

        execs = [
            {
                "description": a.get("description", ""),
                "expected_condition": a.get("expected_condition", ""),
                "sql": _assertion_sql_from_condition(a.get("expected_condition", "")),
            }
            for a in body.assertions
        ]

        with initialize_duckdb(DB_PATH) as con:
            con.register(view_name, result_df)
            try:
                assertion_results = _evaluate_assertions(execs, view_name, con)
            finally:
                try:
                    con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                except Exception:
                    pass

        has_error = any(a.get("error") for a in assertion_results)
        violations = sum(
            1 for a in assertion_results if not a.get("passed") and not a.get("error")
        )

        if has_error:
            verdict = "Insuffisant"
            first_err = next(
                (a.get("error") for a in assertion_results if a.get("error")), ""
            )
            evaluation = (
                f"Insuffisant — une assertion contient une erreur SQL : {first_err}"
            )
        elif violations:
            verdict = "Insuffisant"
            evaluation = f"Insuffisant — {violations} ligne(s) violent une assertion."
        else:
            verdict = "Bon"
            evaluation = "Bon — Toutes les assertions passent sur ce résultat."

        updated = {
            **current,
            "assertion_results": assertion_results,
            "verdict": verdict,
            "evaluation": evaluation,
            "evaluation_explanation": evaluation,
        }
        new_cases = [
            updated if str(t.get("test_index")) == str(body.testIndex) else t
            for t in test_cases
        ]
        update_test(body.sessionId, {"test_cases": new_cases})

        return {
            "ok": True,
            "test_index": current.get("test_index"),
            "assertion_results": assertion_results,
            "evaluation": evaluation,
        }
    except HTTPException:
        raise
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/suggestions/dismiss")
async def dismiss_suggestion(body: DismissSuggestionRequest):
    try:
        test = get_test(body.sessionId)
        if not test:
            return {"ok": True}

        suggestion = body.suggestion.strip()

        suggestions = [
            s for s in (test.get("suggestions") or []) if s.strip() != suggestion
        ]

        dismissed: list = list(test.get("dismissed_suggestions") or [])
        if suggestion not in [d.strip() for d in dismissed]:
            dismissed.append(suggestion)

        rationales = {
            k: v
            for k, v in (test.get("suggestion_rationales") or {}).items()
            if k.strip() != suggestion
        }

        update_test(
            body.sessionId,
            {
                "suggestions": suggestions,
                "dismissed_suggestions": dismissed,
                "suggestion_rationales": rationales,
            },
        )
        return {"ok": True}
    except Exception as e:
        print(e)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post("/query/instruction")
async def queue_instruction(body: QueueInstructionRequest):
    """Traite un message saisi pendant qu'une génération est déjà en cours.

    Classe l'intention (cf. build_query/inflight_message) :
    - **instruction** (l'utilisateur veut influencer la génération) → mise en file,
      consultée à chaud par le run en vol (peek), rejouée par le flush si non consommée.
    - **question** (« pourquoi ce résultat ? ») → répondue en direct par un appel LLM
      indépendant (read-only), persistée dans le fil et renvoyée pour affichage immédiat,
      sans toucher la génération en cours.
    """
    from build_query.inflight_message import (
        answer_inflight_question,
        classify_inflight_message,
    )
    from build_query.pending_instructions import add_instruction

    kind = await classify_inflight_message(body.sessionId, body.text, body.dialect)

    if kind == "question":
        try:
            qa = await answer_inflight_question(
                body.sessionId, body.text, body.dialect, body.parentMessageId
            )
            return {"kind": "question", **qa}
        except Exception as exc:
            logger.warning("inflight question answering failed: %s", exc)
            # Repli : on dégrade en instruction plutôt que de perdre le message.
            kind = "instruction"

    queued = add_instruction(body.sessionId, body.text)
    return {"kind": "instruction", "queued": queued}


@router.post("/query/instruction/flush")
async def flush_instructions_route(body: FlushInstructionsRequest):
    """Renvoie les instructions non consommées en vol et vide la session — appelé en
    fin de run par le front pour le replay."""
    from build_query.pending_instructions import flush_instructions

    return {"instructions": flush_instructions(body.sessionId)}


@router.post("/clearHistory")
async def clear_history(body: ClearHistoryRequest):
    result = await delete_all_messages(body.sessionId)
    if not result.get("success"):
        raise HTTPException(
            status_code=500, detail=result.get("error", "Failed to clear history")
        )
    return {"ok": True}
