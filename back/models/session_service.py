import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from fastapi import HTTPException
from starlette import status

from common_vars import SESSIONS_TABLE_NAME
from models.database import db_pool


class SessionData:
    @staticmethod
    async def get_existing_user(user_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an existing user based on user_id.
        """
        async with db_pool.connection() as conn:
            get_user_query = (
                f"SELECT * FROM {SESSIONS_TABLE_NAME} WHERE user_info->>'id' = $1"
            )
            result = await conn.fetchrow(get_user_query, user_id)
            return dict(result) if result else None

    @staticmethod
    async def store_session_data(db, token: Dict, user_info: Dict) -> str:
        """
        Stores or updates session data for a user.
        """
        try:
            user_id = user_info.get("id")
            existing_user = await SessionData.get_existing_user(user_id)

            token_expiry_datetime = (
                datetime.fromtimestamp(token["expires_at"])
                if "expires_at" in token
                else None
            )

            async with db.transaction():
                if existing_user:
                    # Update existing session for the user
                    update_query = f"""
                    UPDATE {SESSIONS_TABLE_NAME}
                    SET access_token = $1, refresh_token = $2, token_expiry = $3, user_info = $4
                    WHERE user_info->>'id' = $5
                    """
                    await db.execute(
                        update_query,
                        token["access_token"],
                        token.get("refresh_token"),
                        token_expiry_datetime,
                        json.dumps(user_info),
                        user_id,
                    )
                    return existing_user["session_id"]
                else:
                    # Insert new session for the user
                    insert_query = f"""
                    INSERT INTO {SESSIONS_TABLE_NAME} (access_token, refresh_token, token_expiry, user_info, created_at)
                    VALUES ($1, $2, $3, $4, $5) RETURNING session_id
                    """
                    result = await db.fetchrow(
                        insert_query,
                        token["access_token"],
                        token.get("refresh_token"),
                        token_expiry_datetime,
                        json.dumps(user_info),
                        datetime.utcnow(),
                    )
                    return result["session_id"]
        except Exception as e:
            print(e)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Error processing user information in session storage.",
            )


async def store_session(token: Dict[str, Any], user_info: Dict[str, Any]) -> str:
    """
    High-level function to handle session storage, creating or updating as needed.
    """
    async with db_pool.connection() as conn:
        async with conn.transaction():
            session_id = await SessionData.store_session_data(conn, token, user_info)
            return session_id


async def search_emails(query: str, limit: int = 50) -> List[str]:
    """
    Search for users whose name or email contains the query substring (case-insensitive).
    Returns up to `limit` User objects.
    """
    sql = """
        SELECT user_id, name, email, role
        FROM users
        WHERE name ILIKE $1 OR email ILIKE $1
        ORDER BY name
        LIMIT $2
    """
    pattern = f"%{query}%"
    async with db_pool.connection() as conn:
        rows = await conn.fetch(sql, pattern, limit)
        return [row["email"] for row in rows]
