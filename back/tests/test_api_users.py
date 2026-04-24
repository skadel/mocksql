from unittest.mock import AsyncMock, patch


class TestGetUserPreferences:
    async def test_no_row_returns_false(self, client):
        with patch("app.api.endpoints.users.query", new=AsyncMock(return_value=[])):
            resp = await client.get("/api/user/preferences?user_id=u1")
        assert resp.status_code == 200
        assert resp.json() == {"auto_import_always": False}

    async def test_existing_row_returns_stored_value(self, client):
        row = [{"auto_import_always": True}]
        with patch("app.api.endpoints.users.query", new=AsyncMock(return_value=row)):
            resp = await client.get("/api/user/preferences?user_id=u1")
        assert resp.status_code == 200
        assert resp.json()["auto_import_always"] is True


class TestUpdateUserPreferences:
    async def test_insert_new_user(self, client):
        with (
            patch("app.api.endpoints.users.query", new=AsyncMock(return_value=[])),
            patch("app.api.endpoints.users.execute", new=AsyncMock(return_value=None)),
        ):
            resp = await client.patch(
                "/api/user/preferences",
                json={"user_id": "u1", "auto_import_always": True},
            )
        assert resp.status_code == 200
        assert resp.json() == {"success": True}

    async def test_update_existing_user(self, client):
        with (
            patch(
                "app.api.endpoints.users.query",
                new=AsyncMock(return_value=[{"user_id": "u1"}]),
            ),
            patch("app.api.endpoints.users.execute", new=AsyncMock(return_value=None)),
        ):
            resp = await client.patch(
                "/api/user/preferences",
                json={"user_id": "u1", "auto_import_always": False},
            )
        assert resp.status_code == 200
        assert resp.json() == {"success": True}

    async def test_missing_user_id_returns_422(self, client):
        resp = await client.patch(
            "/api/user/preferences", json={"auto_import_always": True}
        )
        assert resp.status_code == 422

    async def test_missing_auto_import_returns_422(self, client):
        resp = await client.patch("/api/user/preferences", json={"user_id": "u1"})
        assert resp.status_code == 422
