import json
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common_vars import PROJECTS_TABLE_NAME, USERS_TABLE_NAME
from models.database import execute, query
from models.permissions import grant_role

router = APIRouter()


class ProjectRequest(BaseModel):
    project_id: Optional[str] = None
    name: str
    dialect: str
    description: Optional[str] = None
    service_account_key: Optional[str] = None
    auto_import: Optional[bool] = False
    user_sub: Optional[str] = ""


class ShareProjectRequest(BaseModel):
    project: str
    target: str


def _row_to_project(row: dict) -> dict:
    return {
        "project_id": row["project_id"],
        "name": row["name"],
        "dialect": row["dialect"],
        "description": row.get("description") or "",
        "service_account_key": row.get("service_account_key"),
        "auto_import": row.get("auto_import") or False,
        "schema": json.loads(row["json_schema"]) if row.get("json_schema") else [],
    }


@router.get("/projects")
async def get_projects():
    rows = await query(f"SELECT * FROM {PROJECTS_TABLE_NAME}")
    return [_row_to_project(r) for r in (rows or [])]


@router.get("/project/{project_id}")
async def get_project(project_id: str):
    rows = await query(
        f"SELECT * FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        (project_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Project not found")
    return _row_to_project(rows[0])


@router.post("/projects")
async def create_project(body: ProjectRequest):
    project_id = body.project_id or str(uuid.uuid4())
    now = datetime.now().isoformat()
    await execute(
        f"""
        INSERT INTO {PROJECTS_TABLE_NAME}
            (project_id, name, dialect, description, service_account_key, auto_import, user_sub, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (project_id) DO UPDATE SET
            name = EXCLUDED.name,
            dialect = EXCLUDED.dialect,
            description = EXCLUDED.description,
            service_account_key = EXCLUDED.service_account_key,
            auto_import = EXCLUDED.auto_import,
            updated_at = EXCLUDED.updated_at
        """,
        project_id,
        body.name,
        body.dialect,
        body.description or "",
        body.service_account_key,
        body.auto_import or False,
        body.user_sub or "",
        now,
        now,
    )
    rows = await query(
        f"SELECT * FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        (project_id,),
    )
    return JSONResponse(content=_row_to_project(rows[0]), status_code=201)


@router.delete("/projects/{project_id}")
async def delete_project(project_id: str):
    rows = await query(
        f"SELECT project_id FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        (project_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Project not found")
    await execute(
        f"DELETE FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        project_id,
    )
    return JSONResponse(status_code=204, content={"message": "Project deleted"})


@router.delete("/projects/{project_id}/table/{table_name}")
async def delete_project_table(project_id: str, table_name: str):
    rows = await query(
        f"SELECT json_schema FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        (project_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Project not found")

    schema = json.loads(rows[0].get("json_schema") or "[]")
    updated = [t for t in schema if t.get("table_name") != table_name]

    await execute(
        f"UPDATE {PROJECTS_TABLE_NAME} SET json_schema = $1, updated_at = $2 WHERE project_id = $3",
        json.dumps(updated, ensure_ascii=False),
        datetime.now().isoformat(),
        project_id,
    )
    return {"removed": table_name, "remaining": len(updated)}


@router.post("/projects/share")
async def share_project(body: ShareProjectRequest):
    target_rows = await query(
        f"SELECT user_id FROM {USERS_TABLE_NAME} WHERE email = $1 OR user_id = $1",
        (body.target,),
    )
    if not target_rows:
        raise HTTPException(status_code=404, detail=f"User not found: {body.target}")
    target_user_id = target_rows[0]["user_id"]
    await grant_role(target_user_id, body.project, role="user")
    return {"shared": True, "user_id": target_user_id}
