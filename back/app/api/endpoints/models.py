from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from storage.test_repository import (
    list_models,
    list_all_tests,
    list_tests,
    get_test,
    create_test,
    delete_test,
    read_model_sql,
    _test_path,
    _read_json,
)

router = APIRouter()


# ---------------------------------------------------------------------------
# SQL Models (fichiers physiques dans models_path)
# ---------------------------------------------------------------------------


@router.get("/models")
async def get_models():
    """Liste les fichiers .sql disponibles dans models_path.

    Each entry includes session_id / updated_at / test_name when a test already
    exists for that model, so the frontend can redirect instead of regenerating.
    Tested models are returned first (sorted by updated_at desc), then untested.
    """
    try:
        sql_files = list_models()
        tested = []
        untested = []
        for f in sql_files:
            p = _test_path(f["name"])
            if p.exists():
                data = _read_json(p)
                if data:
                    tested.append(
                        {
                            **f,
                            "session_id": data.get("test_id"),
                            "updated_at": data.get("updated_at"),
                            "test_name": data.get("test_name"),
                        }
                    )
                    continue
            untested.append(f)
        tested.sort(key=lambda x: x.get("updated_at") or "", reverse=True)
        return tested + untested
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tests/all")
async def get_all_tests():
    """Liste toutes les sessions de test (tous models confondus), triées par date décroissante."""
    try:
        return list_all_tests()
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


@router.get("/models/sql")
async def get_model_sql(name: str):
    """Retourne le contenu SQL preprocessé d'un fichier .sql."""
    sql = read_model_sql(name)
    if sql is None:
        raise HTTPException(status_code=404, detail=f"Model '{name}' not found")
    return {"sql": sql, "name": name}


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
