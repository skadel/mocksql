from unittest.mock import AsyncMock, patch


class TestGetMessages:
    async def test_session_not_found_returns_404(self, client):
        with (
            patch("app.api.endpoints.messages.is_initialized", return_value=True),
            patch(
                "app.api.endpoints.messages.common_history_retriever",
                new=AsyncMock(return_value=None),
            ),
        ):
            resp = await client.post("/api/getMessages", json={"modelId": "unknown"})
        assert resp.status_code == 404

    async def test_returns_messages_and_metadata(self, client):
        with (
            patch("app.api.endpoints.messages.is_initialized", return_value=True),
            patch(
                "app.api.endpoints.messages.common_history_retriever",
                new=AsyncMock(return_value=[]),
            ),
            patch(
                "app.api.endpoints.messages.get_test",
                return_value={
                    "sql": "SELECT 1",
                    "optimized_sql": "SELECT 1",
                    "test_cases": [],
                    "restored_message_id": None,
                    "last_error": None,
                },
            ),
        ):
            resp = await client.post("/api/getMessages", json={"modelId": "sess-1"})

        assert resp.status_code == 200
        data = resp.json()
        assert "messages" in data
        assert "sql" in data
        assert "sql_history" in data
        assert data["sql"] == "SELECT 1"

    async def test_missing_model_id_returns_422(self, client):
        resp = await client.post("/api/getMessages", json={})
        assert resp.status_code == 422


class TestPatchModelSql:
    async def test_updates_sql(self, client):
        with patch("app.api.endpoints.messages.update_test", return_value=None):
            resp = await client.patch(
                "/api/models/sql",
                json={
                    "sessionId": "sess-1",
                    "sql": "SELECT id FROM orders",
                    "optimized_sql": "SELECT orders.id FROM orders",
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    async def test_updates_sql_with_optional_fields(self, client):
        with patch("app.api.endpoints.messages.update_test", return_value=None):
            resp = await client.patch(
                "/api/models/sql",
                json={
                    "sessionId": "sess-1",
                    "sql": "SELECT 1",
                    "optimized_sql": "",
                    "tests": [{"name": "test1"}],
                    "test_results": [{"status": "pass"}],
                    "last_error": "",
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    async def test_missing_session_id_returns_422(self, client):
        resp = await client.patch("/api/models/sql", json={"sql": "SELECT 1"})
        assert resp.status_code == 422


class TestPatchModelTests:
    async def test_updates_test_results(self, client):
        with patch("app.api.endpoints.messages.update_test", return_value=None):
            resp = await client.patch(
                "/api/models/tests",
                json={
                    "sessionId": "sess-1",
                    "tests": [{"status": "pass"}],
                },
            )
        assert resp.status_code == 200
        assert resp.json() == {"ok": True}

    async def test_missing_fields_returns_422(self, client):
        resp = await client.patch("/api/models/tests", json={"sessionId": "sess-1"})
        assert resp.status_code == 422
