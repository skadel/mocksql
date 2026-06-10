import json
from unittest.mock import patch

# results_json simule la sortie persistée de la requête (table __result__ reconstruite).
_RESULTS_JSON = json.dumps([{"id": 1, "amount": 10}, {"id": 2, "amount": 20}])


def _fake_test():
    return {
        "test_id": "sess-1",
        "model_name": "demo",
        "test_cases": [
            {
                "test_index": "1",
                "unit_test_description": "Montants positifs",
                "results_json": _RESULTS_JSON,
                "assertion_results": [],
            }
        ],
    }


class TestApplyAssertions:
    async def test_all_pass_gives_bon(self, client):
        captured = {}

        def _capture(_sid, updates):
            captured.update(updates)
            return None

        with (
            patch("app.api.endpoints.messages.get_test", return_value=_fake_test()),
            patch("app.api.endpoints.messages.update_test", side_effect=_capture),
        ):
            resp = await client.post(
                "/api/tests/apply_assertions",
                json={
                    "sessionId": "sess-1",
                    "testIndex": "1",
                    "assertions": [
                        {
                            "description": "amount positif",
                            "expected_condition": "amount > 0",
                        }
                    ],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["assertion_results"][0]["passed"] is True
        assert data["evaluation"].startswith("Bon")
        # persistance : verdict + assertion_results écrits sur le bon test
        assert captured["test_cases"][0]["verdict"] == "Bon"

    async def test_violation_gives_insuffisant(self, client):
        with (
            patch("app.api.endpoints.messages.get_test", return_value=_fake_test()),
            patch("app.api.endpoints.messages.update_test", return_value=None),
        ):
            resp = await client.post(
                "/api/tests/apply_assertions",
                json={
                    "sessionId": "sess-1",
                    "testIndex": "1",
                    "assertions": [
                        {
                            "description": "amount > 15",
                            "expected_condition": "amount > 15",
                        }
                    ],
                },
            )

        assert resp.status_code == 200
        data = resp.json()
        # la ligne id=1 (amount=10) viole la condition → assertion échoue
        assert data["assertion_results"][0]["passed"] is False
        assert data["evaluation"].startswith("Insuffisant")
        assert "Bon" not in data["evaluation"]  # front mappe /Bon/ avant /Insuffisant/

    async def test_unknown_test_index_returns_404(self, client):
        with patch("app.api.endpoints.messages.get_test", return_value=_fake_test()):
            resp = await client.post(
                "/api/tests/apply_assertions",
                json={"sessionId": "sess-1", "testIndex": "99", "assertions": []},
            )
        assert resp.status_code == 404
