import asyncio

from common_vars import (
    SESSIONS_TABLE_NAME,
    USER_PROJECTS_TABLE_NAME,
    USERS_TABLE_NAME,
    COMMON_HISTORY_TABLE_NAME,
    USER_SETTINGS_TABLE_NAME,
)
from models.database import execute
from models.env_variables import DB_MODE


async def main():
    await asyncio.gather(
        initiate_sessions_table(),
        initiate_user_projects_table(),
        initiate_users_table(),
        init_chat_history_table(),
        init_user_settings_table(),
    )
    await run_migrations()


def cli_main():
    asyncio.run(main())


async def run_migrations() -> None:
    pass


async def init_user_settings_table() -> None:
    if DB_MODE in ("postgres", "cloudsql"):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USER_SETTINGS_TABLE_NAME} (
            user_id            VARCHAR(255) PRIMARY KEY,
            auto_import_always BOOLEAN NOT NULL DEFAULT FALSE
        );
        """
    else:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USER_SETTINGS_TABLE_NAME} (
            user_id            VARCHAR PRIMARY KEY,
            auto_import_always BOOLEAN NOT NULL DEFAULT FALSE
        );
        """
    await execute(sql)


async def initiate_sessions_table() -> None:
    if DB_MODE == "duckdb":
        sql = f"""
        CREATE TABLE IF NOT EXISTS {SESSIONS_TABLE_NAME} (
            session_id   INTEGER PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            token_expiry TIMESTAMP,
            user_info    JSON,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    else:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {SESSIONS_TABLE_NAME} (
            session_id SERIAL PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            token_expiry TIMESTAMP,
            user_info JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    await execute(sql)


async def initiate_user_projects_table() -> None:
    if DB_MODE == "duckdb":
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USER_PROJECTS_TABLE_NAME} (
            user_id    VARCHAR NOT NULL,
            project_id VARCHAR NOT NULL,
            granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, project_id)
        );
        """
    else:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USER_PROJECTS_TABLE_NAME} (
            user_id    VARCHAR(255) NOT NULL
                         REFERENCES {USERS_TABLE_NAME}(user_id) ON DELETE CASCADE,
            project_id VARCHAR(255) NOT NULL,
            granted_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (user_id, project_id)
        );
        """
    await execute(sql)


async def initiate_users_table() -> None:
    if DB_MODE == "duckdb":
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USERS_TABLE_NAME} (
            user_id    VARCHAR PRIMARY KEY,
            password   VARCHAR,
            name       VARCHAR,
            email      VARCHAR UNIQUE,
            picture    TEXT,
            role       VARCHAR NOT NULL DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    else:
        sql = f"""
        CREATE TABLE IF NOT EXISTS {USERS_TABLE_NAME} (
            user_id    VARCHAR(255) PRIMARY KEY,
            password   VARCHAR(255),
            name       VARCHAR(255),
            email      VARCHAR(255) UNIQUE,
            picture    TEXT,
            role       VARCHAR(50) NOT NULL DEFAULT 'user',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    await execute(sql)


async def init_chat_history_table() -> None:
    json_type = "JSONB" if DB_MODE in ("postgres", "cloudsql") else "JSON"

    if DB_MODE in ("postgres", "cloudsql"):
        sql = f"""
        CREATE TABLE IF NOT EXISTS {COMMON_HISTORY_TABLE_NAME} (
            id           SERIAL PRIMARY KEY,
            session_id   VARCHAR(255) NOT NULL,
            data         {json_type} NOT NULL,
            type         VARCHAR(255),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    elif DB_MODE == "duckdb":
        sql = f"""
        CREATE SEQUENCE IF NOT EXISTS common_history_id_seq START 1;
        CREATE TABLE IF NOT EXISTS {COMMON_HISTORY_TABLE_NAME} (
            id INTEGER PRIMARY KEY DEFAULT nextval('common_history_id_seq'),
            session_id   VARCHAR(255) NOT NULL,
            data         {json_type} NOT NULL,
            type         VARCHAR(255),
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    else:
        raise ValueError(f"{DB_MODE} is not handled")
    await execute(sql)
