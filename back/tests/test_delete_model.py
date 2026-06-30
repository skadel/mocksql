"""Régression : supprimer un modèle doit retirer le fichier de définition commité,
son cache sidecar gitignoré, ET (au niveau endpoint) purger les conversations
(`common_history`). Les données de test synthétiques vivent dans ces fichiers — il
n'y a pas de table DuckDB persistante à purger (exécution en `:memory:`)."""

from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    import storage.config as config

    config.load_config.cache_clear()
    import storage.test_repository as tr

    return tr


def _write_model_with_cache(tr, model_name: str, test_id: str):
    """Écrit un modèle dont un test_case porte un champ runtime (`results_json`) →
    force la création d'un cache sidecar à côté de la définition."""
    path = tr._test_path(model_name)
    tr.write_test_doc(
        path,
        {
            "test_id": test_id,
            "model_name": model_name,
            "sql": "SELECT 1 AS x",
            "test_cases": [
                {
                    "test_index": "1",
                    "test_name": "nominal",
                    "results_json": '[{"x": 1}]',
                }
            ],
        },
    )
    return path


def test_delete_model_removes_definition_and_cache(repo):
    path = _write_model_with_cache(repo, "finance/revenue", "uuid-rev")
    cache = repo.cache_path_for(path)
    assert path.exists()
    assert cache and cache.exists()  # le results_json a bien été sorti dans le cache

    name = repo.delete_model("uuid-rev")

    assert name == "finance/revenue"
    assert not path.exists()
    assert not cache.exists()  # le sidecar ne survit pas en orphelin


def test_delete_model_resolves_by_session_id_alone(repo):
    # Modèle imbriqué supprimé sans connaître le model_name (le front n'envoie que le id).
    repo._write_json(
        repo._test_path("spider/bq006"),
        {
            "test_id": "uuid-bq",
            "model_name": "spider/bq006",
            "sql": "SELECT 1",
            "test_cases": [],
        },
    )
    assert repo.delete_model("uuid-bq") == "spider/bq006"
    assert not repo._test_path("spider/bq006").exists()


def test_delete_model_unknown_session_returns_none(repo):
    assert repo.delete_model("does-not-exist") is None


def test_delete_test_also_drops_cache_sidecar(repo):
    """delete_test (DELETE /tests/{id}) laissait jusqu'ici le cache sidecar orphelin."""
    path = _write_model_with_cache(repo, "finance/revenue", "uuid-rev")
    cache = repo.cache_path_for(path)
    assert cache and cache.exists()

    assert repo.delete_test("uuid-rev", "finance/revenue") is True
    assert not path.exists()
    assert not cache.exists()


class TestDeleteModelEndpoint:
    async def test_deletes_files_and_conversations(self, client):
        with (
            patch(
                "app.api.endpoints.models.delete_model",
                return_value="finance/revenue",
            ) as del_model,
            patch(
                "app.api.endpoints.models.delete_all_messages",
                new=AsyncMock(return_value={"success": True}),
            ) as del_msgs,
        ):
            resp = await client.delete("/api/models/uuid-rev")

        assert resp.status_code == 200
        body = resp.json()
        assert body["ok"] is True
        assert body["model_name"] == "finance/revenue"
        del_model.assert_called_once_with("uuid-rev")
        del_msgs.assert_awaited_once_with("uuid-rev")

    async def test_unknown_model_returns_404_without_touching_conversations(
        self, client
    ):
        with (
            patch("app.api.endpoints.models.delete_model", return_value=None),
            patch(
                "app.api.endpoints.models.delete_all_messages", new=AsyncMock()
            ) as del_msgs,
        ):
            resp = await client.delete("/api/models/nope")

        assert resp.status_code == 404
        del_msgs.assert_not_called()
