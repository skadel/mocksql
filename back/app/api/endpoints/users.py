from fastapi import APIRouter
from pydantic import BaseModel

from common_vars import USER_SETTINGS_TABLE_NAME
from models.database import execute, query

router = APIRouter()


class UserPreferencesRequest(BaseModel):
    user_id: str
    auto_import_always: bool


@router.get("/user/preferences")
async def get_user_preferences(user_id: str):
    rows = await query(
        f"SELECT auto_import_always FROM {USER_SETTINGS_TABLE_NAME} WHERE user_id = $1",
        (user_id,),
    )
    if rows:
        return {"auto_import_always": rows[0]["auto_import_always"]}
    return {"auto_import_always": False}


@router.patch("/user/preferences")
async def update_user_preferences(body: UserPreferencesRequest):
    existing = await query(
        f"SELECT user_id FROM {USER_SETTINGS_TABLE_NAME} WHERE user_id = $1",
        (body.user_id,),
    )
    if existing:
        await execute(
            f"UPDATE {USER_SETTINGS_TABLE_NAME} SET auto_import_always = $1 WHERE user_id = $2",
            body.auto_import_always,
            body.user_id,
        )
    else:
        await execute(
            f"INSERT INTO {USER_SETTINGS_TABLE_NAME} (user_id, auto_import_always) VALUES ($1, $2)",
            body.user_id,
            body.auto_import_always,
        )
    return {"success": True}
