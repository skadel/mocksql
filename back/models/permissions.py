from typing import Optional

from models.database import db_pool


async def grant_role(
    user_id: str,
    project_id: str,
    role: str = "user",
) -> None:
    """
    Ajoute ou met à jour le rôle de l’utilisateur pour ce projet.
    """
    upsert = """
    INSERT INTO user_projects (user_id, project_id, role)
    VALUES ($1, $2, $3)
    ON CONFLICT (user_id, project_id)
    DO UPDATE SET
      role       = EXCLUDED.role,
      granted_at = CURRENT_TIMESTAMP;
    """
    async with db_pool.connection() as conn:
        await conn.execute(upsert, user_id, project_id, role)


async def revoke_access(user_id: str, project_id: str) -> None:
    """
    Supprime l’accès de l’utilisateur au projet.
    """
    delete = """
    DELETE FROM user_projects
      WHERE user_id = $1
        AND project_id = $2;
    """
    async with db_pool.connection() as conn:
        await conn.execute(delete, user_id, project_id)


async def get_user_role(user_id: str, project_id: str) -> Optional[str]:
    """
    Renvoie le rôle (string) de l’utilisateur sur ce projet, ou None si pas d’accès.
    """
    query = """
    SELECT role
      FROM user_projects
     WHERE user_id    = $1
       AND project_id = $2;
    """
    async with db_pool.connection() as conn:
        row = await conn.fetchrow(query, user_id, project_id)
        return row["role"] if row else None


async def is_admin(user_id: str, project_id: str) -> bool:
    """
    True si le rôle est exactement 'admin'.
    """
    role = await get_user_role(user_id, project_id)
    return role == "admin"


async def has_access(user_id: str, project_id: str) -> bool:
    """
    True si un mapping existe, quel que soit le rôle.
    """
    return (await get_user_role(user_id, project_id)) is not None
