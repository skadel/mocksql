"""Régression : un fichier de test produit par la CLI (`mocksql generate`) ou écrit à
la main ne porte QUE la définition (`sql`, `used_columns`, `test_cases`, `suggestions`)
— sans `test_id` ni `model_name`. Or le serveur indexe TOUTE recherche sur `test_id`
(`GET /models` renvoie `session_id = test_id`, `get_test` matche par `test_id`).

Sans dérivation, un modèle généré uniquement en CLI est invisible dans l'UI (0 test
affiché) alors que le fichier JSON existe bien. Le chemin du fichier EST l'identité du
modèle (`tests/<model_name>.json`), donc on dérive `model_name` + un `test_id`
déterministe à la lecture.
"""

from pathlib import Path

import pytest


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    import storage.config as config

    config.load_config.cache_clear()
    import storage.test_repository as tr

    return tr


def _write_minimal(tr, model_name: str) -> Path:
    """Écrit le fichier minimal qu'émet la CLI : ni test_id ni model_name."""
    path = tr._tests_root() / f"{model_name}.json"
    tr.write_test_doc(
        path,
        {
            "sql": "SELECT 1 AS x",
            "used_columns": [],
            "test_cases": [
                {"test_index": "1", "test_name": "nominal", "data": {"t": [{"x": 1}]}}
            ],
            "suggestions": ["cas limite NULL"],
        },
    )
    return path


def test_minimal_file_gets_identity_derived_from_path(repo):
    _write_minimal(repo, "complex_examples/c3")

    [doc] = repo.list_tests("complex_examples/c3")
    assert doc["model_name"] == "complex_examples/c3"
    assert doc["test_id"]  # un id non vide est dérivé
    assert len(doc["test_cases"]) == 1


def test_derived_test_id_is_stable_and_resolvable(repo):
    """GET /models renvoie le test_id dérivé ; get_test(ce test_id) doit retrouver
    le fichier — sinon l'UI navigue vers /models/<id> et charge 0 test."""
    _write_minimal(repo, "complex_examples/c3")

    # Ce que renvoie GET /models (cf. endpoints/models.py).
    listed = repo._read_json(repo._test_path("complex_examples/c3"))
    session_id = listed["test_id"]
    assert session_id

    # Ce que fait getMessages → get_test(session_id) (model_name inconnu).
    found = repo.get_test(session_id)
    assert found is not None
    assert found["model_name"] == "complex_examples/c3"
    assert len(found["test_cases"]) == 1


def test_existing_identity_is_not_overwritten(repo):
    """Un fichier déjà complet (flux serveur) garde son test_id/model_name."""
    path = repo._tests_root() / "spider" / "bq006.json"
    repo.write_test_doc(
        path,
        {
            "test_id": "real-uuid-xyz",
            "model_name": "spider/bq006",
            "sql": "SELECT 1",
            "test_cases": [],
        },
    )
    doc = repo._read_json(path)
    assert doc["test_id"] == "real-uuid-xyz"
    assert doc["model_name"] == "spider/bq006"
