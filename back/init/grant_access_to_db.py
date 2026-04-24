import asyncio

from common_vars import (
    MODELS_TABLE_NAME,
    SESSIONS_TABLE_NAME,
    EXAMPLES_TABLE_NAME,
    USERS_TABLE_NAME,
    PROJECTS_TABLE_NAME,
    COMMON_HISTORY_TABLE_NAME,
)
from models.database import execute


def main():
    asyncio.run(grant_access_to_postgres_user("postgres"))


if __name__ == "__main__":
    main()


async def grant_access_to_postgres_user(user: str) -> None:
    q = f"""
    GRANT ALL PRIVILEGES ON TABLE {MODELS_TABLE_NAME}       TO {user};
    GRANT ALL PRIVILEGES ON TABLE {SESSIONS_TABLE_NAME}     TO {user};
    GRANT ALL PRIVILEGES ON TABLE {EXAMPLES_TABLE_NAME}     TO {user};
    GRANT ALL PRIVILEGES ON TABLE {USERS_TABLE_NAME}        TO {user};
    GRANT ALL PRIVILEGES ON TABLE {PROJECTS_TABLE_NAME}     TO {user};
    GRANT ALL PRIVILEGES ON TABLE {COMMON_HISTORY_TABLE_NAME} TO {user};
    """
    await execute(q)
