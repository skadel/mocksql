from unittest.mock import AsyncMock, MagicMock, patch

VALID_SQL = "SELECT id FROM orders"

BASE_VALIDATE_BODY = {
    "sql": VALID_SQL,
    "project": "proj1",
    "dialect": "bigquery",
    "session": "",
    "parent_message_id": "",
}


def _fake_table(db="dataset", name="orders", catalog=None):
    t = MagicMock()
    t.db = db
    t.name = name
    t.catalog = catalog
    return t


class TestValidateQuery:
    async def test_missing_tables_returns_invalid(self, client):
        with (
            patch(
                "app.api.endpoints.query.extract_real_table_refs",
                return_value=[_fake_table()],
            ),
            patch(
                "app.api.endpoints.query.get_tables_mapping",
                new=AsyncMock(return_value={}),
            ),
        ):
            resp = await client.post("/api/validate-query", json=BASE_VALIDATE_BODY)

        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert "missing_tables" in data

    async def test_valid_query_returns_success(self, client):
        validator_result = {
            "status": "success",
            "used_columns": [{"table": "orders", "used_columns": ["id"]}],
            "optimized_sql": VALID_SQL,
            "query_decomposed": "{}",
        }
        with (
            patch("app.api.endpoints.query.extract_real_table_refs", return_value=[]),
            patch(
                "build_query.validator.validate_query",
                new=AsyncMock(return_value=validator_result),
            ),
        ):
            resp = await client.post("/api/validate-query", json=BASE_VALIDATE_BODY)

        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        assert "used_columns" in data

    async def test_validator_error_returns_invalid(self, client):
        validator_result = {"status": "error", "error": "Syntax error near SELECT"}
        with (
            patch("app.api.endpoints.query.extract_real_table_refs", return_value=[]),
            patch(
                "build_query.validator.validate_query",
                new=AsyncMock(return_value=validator_result),
            ),
        ):
            resp = await client.post("/api/validate-query", json=BASE_VALIDATE_BODY)

        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False
        assert "error" in data

    async def test_session_triggers_db_writes(self, client):
        body = {**BASE_VALIDATE_BODY, "session": "sess-123"}
        validator_result = {
            "status": "success",
            "used_columns": [],
            "optimized_sql": VALID_SQL,
            "query_decomposed": "{}",
        }
        with (
            patch("app.api.endpoints.query.extract_real_table_refs", return_value=[]),
            patch(
                "build_query.validator.validate_query",
                new=AsyncMock(return_value=validator_result),
            ),
            patch(
                "app.api.endpoints.query.update_test", return_value=None
            ) as mock_update,
        ):
            resp = await client.post("/api/validate-query", json=body)

        assert resp.status_code == 200
        assert resp.json()["valid"] is True
        assert mock_update.call_count == 1

    async def test_missing_sql_returns_422(self, client):
        resp = await client.post(
            "/api/validate-query",
            json={"project": "p1", "dialect": "bigquery", "session": ""},
        )
        assert resp.status_code == 422

    async def test_auto_import_flag_included_in_missing_tables(self, client):
        with (
            patch(
                "app.api.endpoints.query.extract_real_table_refs",
                return_value=[_fake_table()],
            ),
            patch(
                "app.api.endpoints.query.get_tables_mapping",
                new=AsyncMock(return_value={}),
            ),
        ):
            resp = await client.post("/api/validate-query", json=BASE_VALIDATE_BODY)

        data = resp.json()
        # AUTO_SCHEMA_IMPORT is True by default → response should include auto_import_available
        assert "auto_import_available" in data
        assert data["auto_import_available"] is True


class TestSkipProfile:
    async def test_with_session_returns_skipped(self, client):
        with patch("app.api.endpoints.query.update_test", return_value=None):
            resp = await client.post("/api/skip-profile", json={"session": "sess-123"})
        assert resp.status_code == 200
        assert resp.json() == {"skipped": True}

    async def test_empty_session_skips_db_write(self, client):
        resp = await client.post("/api/skip-profile", json={"session": ""})
        assert resp.status_code == 200
        assert resp.json()["skipped"] is True

    async def test_missing_session_returns_422(self, client):
        resp = await client.post("/api/skip-profile", json={})
        assert resp.status_code == 422


class TestCheckProfile:
    async def test_profile_complete(self, client):
        check_result = {"profile_complete": True, "missing_columns": []}
        with patch(
            "build_query.profile_checker.check_profile",
            new=AsyncMock(return_value=check_result),
        ):
            resp = await client.post(
                "/api/check-profile",
                json={
                    "sql": VALID_SQL,
                    "project": "p1",
                    "dialect": "bigquery",
                    "session": "s1",
                    "used_columns": [],
                },
            )
        assert resp.status_code == 200
        assert resp.json()["profile_complete"] is True

    async def test_profile_incomplete_returns_request(self, client):
        check_result = {"profile_complete": False, "missing_columns": ["col1"]}
        profile_req = {
            "profile_sql": "SELECT col1 FROM orders",
            "missing_columns": ["col1"],
            "expected_joins": [],
        }
        with (
            patch(
                "build_query.profile_checker.check_profile",
                new=AsyncMock(return_value=check_result),
            ),
            patch(
                "build_query.profile_checker.build_profile_request",
                new=AsyncMock(return_value=profile_req),
            ),
        ):
            resp = await client.post(
                "/api/check-profile",
                json={
                    "sql": VALID_SQL,
                    "project": "p1",
                    "dialect": "bigquery",
                    "session": "s1",
                    "used_columns": [],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["profile_complete"] is False
        assert "profile_request" in data
        assert data["profile_request"]["profile_query"] == "SELECT col1 FROM orders"

    async def test_missing_fields_returns_422(self, client):
        resp = await client.post("/api/check-profile", json={"sql": VALID_SQL})
        assert resp.status_code == 422


class TestImportMissingTables:
    async def test_unqualified_table_returns_422(self, client):
        resp = await client.post(
            "/api/import-missing-tables",
            json={
                "tables_to_import": ["orders"],  # no dataset.table qualification
                "project": "p1",
            },
        )
        assert resp.status_code == 422
        data = resp.json()
        assert data["detail"]["needs_manual_config"] is True
