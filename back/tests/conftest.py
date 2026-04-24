import os

# Set before any module import so env_variables._validate_envs() passes
os.environ.setdefault("DB_CONNECTION_TYPE", "duckdb")
os.environ.setdefault("DUCKDB_PATH", ":memory:")

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport


def make_executor(responses: dict):
    """SQL executor stub: returns the first result whose keyword appears in the query string."""

    def executor(query_str: str) -> list[dict]:
        for keyword, result in responses.items():
            if keyword in query_str:
                return result
        return []

    return executor


@pytest.fixture(scope="session")
def test_app():
    from fastapi import FastAPI, APIRouter
    from app.api.endpoints import query, messages, models, users

    app = FastAPI()
    api_router = APIRouter(prefix="/api")
    api_router.include_router(query.router)
    api_router.include_router(messages.router)
    api_router.include_router(models.router)
    api_router.include_router(users.router)
    app.include_router(api_router)
    return app


@pytest_asyncio.fixture
async def client(test_app):
    async with AsyncClient(
        transport=ASGITransport(app=test_app), base_url="http://test"
    ) as c:
        yield c
