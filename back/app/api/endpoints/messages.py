import logging
from typing import List, Any, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from storage.config import is_initialized
from storage.test_repository import get_test, update_test
from utils.saver import common_history_retriever

logger = logging.getLogger(__name__)

router = APIRouter()


class MessageRequest(BaseModel):
    modelId: str


class PatchTestsRequest(BaseModel):
    sessionId: str
    tests: List[Any]


class PatchSqlRequest(BaseModel):
    sessionId: str
    sql: str
    optimized_sql: str = ""
    tests: Optional[List[Any]] = None
    test_results: Optional[List[Any]] = None
    restored_message_id: Optional[str] = None
    last_error: Optional[str] = None


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
        restored_message_id = test.get("restored_message_id") if test else None

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
            "restored_message_id": restored_message_id,
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
