from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from build_query.complexity_scorer import compute_complexity_score, compute_priority_score
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
    get_model_file_hash,
    get_commits_since_sha,
    get_recent_commits,
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
            model_name = f["name"]
            p = _test_path(model_name)
            if p.exists():
                data = _read_json(p)
                if data:
                    current_hash = get_model_file_hash(model_name)
                    stored_hash: str | None = data.get("source_hash")
                    stored_sha: str | None = data.get("source_sha")

                    # Staleness: prefer content-hash comparison (works without git);
                    # fall back to git SHA if hash not yet stored.
                    if stored_hash and current_hash:
                        is_stale = current_hash != stored_hash
                    elif stored_sha:
                        is_stale = False  # unknown without fresh git call
                    else:
                        is_stale = False

                    commits_since = 0
                    if is_stale and stored_sha:
                        commits_since = get_commits_since_sha(model_name, stored_sha)
                    elif not is_stale and stored_sha and stored_hash is None:
                        # legacy: no hash stored yet — check via git
                        from storage.test_repository import get_model_file_git_sha

                        current_git_sha = get_model_file_git_sha(model_name)
                        if current_git_sha and current_git_sha != stored_sha:
                            is_stale = True
                            commits_since = get_commits_since_sha(
                                model_name, stored_sha
                            )

                    tested.append(
                        {
                            **f,
                            "session_id": data.get("test_id"),
                            "updated_at": data.get("updated_at"),
                            "test_name": data.get("test_name"),
                            "model_name": model_name,
                            "is_stale": is_stale,
                            "commits_since": commits_since,
                        }
                    )
                    continue
            untested.append(
                {**f, "model_name": model_name, "is_stale": False, "commits_since": 0}
            )
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


@router.get("/models/explore")
async def explore_models():
    """Return all SQL models with complexity + git activity scores, sorted by priority desc.

    Includes both tested and untested models so engineers can identify what most
    needs test coverage.
    """
    try:
        sql_files = list_models()
        results = []
        for f in sql_files:
            model_name = f["name"]
            sql = read_model_sql(model_name) or ""
            complexity = compute_complexity_score(sql)
            recent_commits = get_recent_commits(model_name)
            priority = compute_priority_score(complexity["total"], recent_commits)

            tested = _test_path(model_name).exists()
            session_id: str | None = None
            if tested:
                data = _read_json(_test_path(model_name))
                session_id = data.get("test_id") if data else None

            results.append(
                {
                    "name": model_name,
                    "model_name": model_name,
                    "is_tested": tested,
                    "session_id": session_id,
                    "priority_score": priority,
                    "complexity_score": complexity["total"],
                    "recent_commits": recent_commits,
                    "complexity_breakdown": complexity["breakdown"],
                }
            )

        results.sort(key=lambda x: x["priority_score"], reverse=True)
        return results
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
