from datetime import datetime
from typing import Any, Dict, Optional

from common_vars import USERS_TABLE_NAME
from models.database import execute, query


async def store_user(user_info: Dict[str, Any]) -> None:
    """
    Stores or updates the user information in the users table.

    user_info should contain:
    - id: str
    - name: str
    - email: str
    - picture: Optional[str]
    - role: Optional[str]
    """
    # Determine role, default 'user'
    role = user_info.get("role", "user")
    created_at = datetime.utcnow().isoformat()

    # Check if user exists
    existing = await query(
        f"""
        SELECT user_id
        FROM {USERS_TABLE_NAME}
        WHERE user_id = $1
        """,
        (user_info["id"],),
    )

    if existing:
        # Update existing user
        sql = f"""
        UPDATE {USERS_TABLE_NAME}
        SET name       = $1,
            email      = $2,
            picture    = $3,
            role       = $4,
            created_at = $5
        WHERE user_id = $6
        """
        await execute(
            sql,
            user_info["name"],
            user_info["email"],
            user_info.get("picture"),
            role,
            created_at,
            user_info["id"],
        )
    else:
        # Insert new user
        sql = f"""
        INSERT INTO {USERS_TABLE_NAME}
            (user_id, name, email, picture, role, created_at)
        VALUES
            ($1, $2, $3, $4, $5, $6)
        """
        await execute(
            sql,
            user_info["id"],
            user_info["name"],
            user_info["email"],
            user_info.get("picture"),
            role,
            created_at,
        )


async def get_user(user_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves a user by user_id.
    """
    rows = await query(
        f"""
        SELECT *
        FROM {USERS_TABLE_NAME}
        WHERE user_id = $1
        """,
        (user_id,),
    )
    return rows[0] if rows else None


async def get_user_by_email(email: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves a user by email.
    """
    rows = await query(
        f"""
        SELECT *
        FROM {USERS_TABLE_NAME}
        WHERE email = $1
        """,
        (email,),
    )
    return rows[0] if rows else None
