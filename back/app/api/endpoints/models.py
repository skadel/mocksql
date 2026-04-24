from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from storage.test_repository import (
    list_models,
    list_tests,
    get_test,
    create_test,
    delete_test,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# SQL Models (fichiers physiques dans models_path)
# ---------------------------------------------------------------------------


@router.get("/models")
async def get_models():
    """Liste les fichiers .sql disponibles dans models_path."""
    try:
        return list_models()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Tests (fichiers dans .mocksql/tests/)
# ---------------------------------------------------------------------------


class CreateTestRequest(BaseModel):
    model_name: str


@router.get("/tests")
async def get_tests(model_name: str):
    """Liste tous les tests pour un model donné."""
    try:
        return list_tests(model_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/test/{session_id}")
async def get_test_route(session_id: str, model_name: str = None):
    try:
        test = get_test(session_id, model_name)
        if test is None:
            raise HTTPException(status_code=404, detail="Test not found")
        return test
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/tests")
async def create_test_route(body: CreateTestRequest):
    """Crée un nouveau test pour un model. Retourne le test_id (= session_id)."""
    try:
        test = create_test(body.model_name)
        return JSONResponse(content=test, status_code=201)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/tests/{session_id}")
async def delete_test_route(session_id: str, model_name: str):
    try:
        ok = delete_test(session_id, model_name)
        if not ok:
            raise HTTPException(status_code=404, detail="Test not found")
        return JSONResponse(status_code=204, content={"message": "Test deleted"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
