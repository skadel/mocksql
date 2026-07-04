"""CLI de gestion de suite — les trous UI ↔ CLI comblés :

- `mocksql remove-test`  : suppression d'un cas par test_uid (équivalent delete_test_node).
- `mocksql validate`     : bouton « Je valide l'état actuel » (équivalent accept_validation).
- `mocksql suggest ...`  : panneau de suggestions (list / use / dismiss / regenerate).

Tout est basé fichiers (.mocksql/tests/{model}.json) — le même stockage que le serveur.
Le ciblage se fait par test_uid (jamais par position) ; les suggestions par numéro
1-based ou texte exact.
"""

import json
from pathlib import Path

import pytest

from cli.doc_io import TestDocError
from cli.manage_cmd import (
    _resolve_suggestion,
    consume_suggestion_updates,
    run_remove_test,
    run_suggest_dismiss,
    run_suggest_list,
    run_suggest_regenerate,
    run_suggest_use,
    run_validate,
)
from storage.test_files import read_test_doc, write_test_doc


@pytest.fixture(autouse=True)
def _reset_storage_config():
    """_point_storage_at mute MOCKSQL_BASE_DIR + le cache lru de load_config —
    on purge le cache après chaque test pour ne pas contaminer les suivants."""
    yield
    import storage.config as storage_config

    storage_config.load_config.cache_clear()


def _suite() -> dict:
    return {
        "sql": "SELECT 1",
        "test_cases": [
            {"test_uid": "aaaa", "test_index": "0", "test_name": "nominal"},
            {
                "test_uid": "bbbb",
                "test_index": "1",
                "test_name": "nulls",
                "assertion_results": [
                    {
                        "assertion_uid": "spec0001",
                        "description": "spec",
                        "sql": "SELECT * FROM __result__ WHERE x IS NULL",
                    }
                ],
            },
        ],
    }


def _write_doc(tmp_path: Path, doc: dict, model: str = "orders") -> Path:
    """Écrit le doc du modèle et retourne le chemin de config (mocksql.yml) attendu
    par les runtimes (seul son parent compte pour les opérations fichier)."""
    write_test_doc(tmp_path / ".mocksql" / "tests" / f"{model}.json", doc)
    return tmp_path / "mocksql.yml"


def _read_saved(tmp_path: Path, model: str = "orders") -> dict:
    return read_test_doc(tmp_path / ".mocksql" / "tests" / f"{model}.json")


# ── remove-test ────────────────────────────────────────────────────────────────


def test_remove_test_by_uid(tmp_path):
    config = _write_doc(tmp_path, _suite())
    result = run_remove_test(config, "orders", "bbbb")
    assert result["removed"]["test_uid"] == "bbbb"
    assert result["remaining"] == 1
    saved = _read_saved(tmp_path)
    assert [c["test_uid"] for c in saved["test_cases"]] == ["aaaa"]


def test_remove_test_unknown_uid_raises(tmp_path):
    config = _write_doc(tmp_path, _suite())
    with pytest.raises(TestDocError, match="introuvable"):
        run_remove_test(config, "orders", "zzzz")


def test_remove_test_missing_model_raises(tmp_path):
    with pytest.raises(TestDocError, match="Aucun test"):
        run_remove_test(tmp_path / "mocksql.yml", "ghost", "aaaa")


# ── validate ───────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_validate_applies_corrected_description_without_llm(tmp_path):
    """Chemin nominal : la corrected_description pré-calculée par l'évaluateur est
    appliquée telle quelle (aucun appel LLM) et le verdict flippe à Bon."""
    doc = {
        "sql": "SELECT 1",
        "test_cases": [
            {
                "test_uid": "aaaa",
                "test_index": "0",
                "verdict": "Insuffisant",
                "reason_type": "bad_description",
                "unit_test_description": "Le total vaut 2.0M.",
                "corrected_description": "Le total vaut 1.4M.",
                "corrected_name": "Total période",
                "results_json": json.dumps([{"total": 1_400_000}]),
            }
        ],
    }
    config = _write_doc(tmp_path, doc)

    result = await run_validate(config, "orders", "aaaa")

    assert result["verdict"] == "Bon"
    case = _read_saved(tmp_path)["test_cases"][0]
    assert case["verdict"] == "Bon"
    assert case["reason_type"] is None
    assert case["unit_test_description"] == "Le total vaut 1.4M."
    assert case["test_name"] == "Total période"
    assert "corrected_description" not in case
    assert "corrected_name" not in case


@pytest.mark.asyncio
async def test_validate_input_desync_drops_user_premise(tmp_path):
    """Valider une desync d'ENTRÉE = accepter les données réelles → la prémisse
    utilisateur abandonnée est retirée (le garde bad_data ne la protège plus)."""
    doc = {
        "sql": "SELECT 1",
        "test_cases": [
            {
                "test_uid": "aaaa",
                "test_index": "0",
                "verdict": "Insuffisant",
                "reason_type": "bad_input_description",
                "user_premise": "un client avec 2 cartes",
                "unit_test_description": "Un client avec 2 cartes.",
                "corrected_description": "Un client avec 1 carte.",
                "results_json": "[]",
            }
        ],
    }
    config = _write_doc(tmp_path, doc)

    await run_validate(config, "orders", "aaaa")

    case = _read_saved(tmp_path)["test_cases"][0]
    assert case["verdict"] == "Bon"
    assert "user_premise" not in case


@pytest.mark.asyncio
async def test_validate_nothing_pending_raises(tmp_path):
    """Un test sans désync en attente (ni reason_type validable, ni corrected_description)
    n'a rien à valider — erreur claire plutôt qu'un flip silencieux du verdict."""
    doc = {
        "sql": "SELECT 1",
        "test_cases": [{"test_uid": "aaaa", "test_index": "0", "verdict": "Bon"}],
    }
    config = _write_doc(tmp_path, doc)
    with pytest.raises(TestDocError, match="Rien à valider"):
        await run_validate(config, "orders", "aaaa")


# ── suggest list / resolution / consommation ──────────────────────────────────


def test_suggest_list_numbers_and_metadata(tmp_path):
    doc = {
        **_suite(),
        "suggestions": ["s1", "s2"],
        "suggestion_rationales": {"s2": "preuve chiffrée"},
        "suggestion_paths": {"s1": "branch_a"},
    }
    config = _write_doc(tmp_path, doc)
    result = run_suggest_list(config, "orders")
    assert [s["number"] for s in result["suggestions"]] == [1, 2]
    assert result["suggestions"][0]["text"] == "s1"
    assert result["suggestions"][0]["target_path"] == "branch_a"
    assert result["suggestions"][0]["rationale"] is None
    assert result["suggestions"][1]["rationale"] == "preuve chiffrée"


def test_resolve_suggestion_by_number_and_text():
    doc = {"suggestions": ["s1", "s2"]}
    assert _resolve_suggestion(doc, 2, None) == "s2"
    assert _resolve_suggestion(doc, None, " s1 ") == "s1"


def test_resolve_suggestion_errors():
    doc = {"suggestions": ["s1"]}
    with pytest.raises(TestDocError, match="hors limites"):
        _resolve_suggestion(doc, 4, None)
    with pytest.raises(TestDocError, match="introuvable"):
        _resolve_suggestion(doc, None, "inconnu")
    with pytest.raises(TestDocError, match="exactement un"):
        _resolve_suggestion(doc, 1, "s1")
    with pytest.raises(TestDocError, match="exactement un"):
        _resolve_suggestion(doc, None, None)
    with pytest.raises(TestDocError, match="Aucune suggestion"):
        _resolve_suggestion({"suggestions": []}, 1, None)


def test_consume_suggestion_updates_mirrors_server():
    """Miroir de la consommation serveur (saver / POST dismiss) : la suggestion sort du
    panneau, entre dans la liste de son sort (dédupliquée), et ses métadonnées
    (rationale, focus) sont nettoyées."""
    stored = {
        "suggestions": ["s1", "s2"],
        "accepted_suggestions": ["s1"],
        "suggestion_rationales": {"s1": "r1", "s2": "r2"},
        "suggestion_paths": {"s1": "branch_a"},
    }
    updates = consume_suggestion_updates(stored, "s1", fate="accepted")
    assert updates["suggestions"] == ["s2"]
    assert updates["accepted_suggestions"] == ["s1"]  # dédupliqué
    assert updates["suggestion_rationales"] == {"s2": "r2"}
    assert updates["suggestion_paths"] == {}


def test_suggest_dismiss_moves_to_dismissed(tmp_path):
    doc = {
        **_suite(),
        "suggestions": ["s1", "s2"],
        "suggestion_rationales": {"s1": "r1"},
    }
    config = _write_doc(tmp_path, doc)
    result = run_suggest_dismiss(config, "orders", 1, None)
    assert result["dismissed"] == "s1"
    saved = _read_saved(tmp_path)
    assert saved["suggestions"] == ["s2"]
    assert saved["dismissed_suggestions"] == ["s1"]
    assert saved["suggestion_rationales"] == {}


# ── suggest use : génération + consommation ────────────────────────────────────


def _project(tmp_path: Path, doc: dict) -> Path:
    """Projet minimal : mocksql.yml + models/orders.sql + doc de test."""
    config = _write_doc(tmp_path, doc)
    config.write_text("dialect: bigquery\nmodels_path: ./models\n", encoding="utf-8")
    (tmp_path / "models").mkdir(exist_ok=True)
    (tmp_path / "models" / "orders.sql").write_text("SELECT 1", encoding="utf-8")
    return config


@pytest.mark.asyncio
async def test_suggest_use_generates_then_consumes(tmp_path, monkeypatch):
    """use : lance la génération additive avec le texte de la suggestion + son focus de
    branche (suggestion_paths → target_path), puis la consomme (accepted_suggestions)."""
    doc = {
        **_suite(),
        "suggestions": ["un cas limite"],
        "suggestion_paths": {"un cas limite": "branch_a"},
    }
    config = _project(tmp_path, doc)

    calls: dict = {}

    async def _fake_run_generate(model, config, output_dir, **kwargs):
        calls["model"] = model
        calls.update(kwargs)

    monkeypatch.setattr("cli.generate.run_generate", _fake_run_generate)

    result = await run_suggest_use(
        config, "orders", 1, None, tmp_path / ".mocksql" / "tests"
    )

    assert calls["model"] == tmp_path / "models" / "orders.sql"
    assert calls["instruction"] == "un cas limite"
    assert calls["target_path"] == "branch_a"
    assert result["used"] == "un cas limite"
    saved = _read_saved(tmp_path)
    assert saved["suggestions"] == []
    assert saved["accepted_suggestions"] == ["un cas limite"]
    assert "un cas limite" not in (saved.get("suggestion_paths") or {})


@pytest.mark.asyncio
async def test_suggest_use_does_not_consume_on_failure(tmp_path, monkeypatch):
    """Une génération en échec ne consomme PAS la suggestion : le besoin de couverture
    reste ouvert dans le panneau."""
    config = _project(tmp_path, {**_suite(), "suggestions": ["un cas limite"]})

    async def _boom(*_a, **_k):
        raise RuntimeError("generation failed")

    monkeypatch.setattr("cli.generate.run_generate", _boom)

    with pytest.raises(RuntimeError):
        await run_suggest_use(
            config, "orders", 1, None, tmp_path / ".mocksql" / "tests"
        )
    assert _read_saved(tmp_path)["suggestions"] == ["un cas limite"]


# ── suggest regenerate : câblage du nœud serveur ───────────────────────────────


@pytest.mark.asyncio
async def test_suggest_regenerate_calls_node_in_replace_mode(tmp_path, monkeypatch):
    """regenerate : state minimal correct (session = test_id backfillé du fichier,
    regenerate_suggestions posé → mode replace) ; le nœud persiste lui-même via
    storage.test_repository — le MÊME fichier que la CLI."""
    config = _project(tmp_path, _suite())

    # Pré-posé via monkeypatch pour que la mutation de _point_storage_at soit restaurée.
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    monkeypatch.setattr("models.env_variables.validate_required_env", lambda: None)

    captured: dict = {}

    async def _fake_node(state):
        captured.update(state)
        from storage.test_repository import update_test

        update_test(state["session"], {"suggestions": ["nouvelle suggestion"]})
        return {}

    monkeypatch.setattr("build_query.suggestions_node.generate_suggestions", _fake_node)

    result = await run_suggest_regenerate(config, "orders")

    assert captured["regenerate_suggestions"] is True
    assert captured["session"]  # identité dérivée du chemin (uuid5 déterministe)
    assert captured["query"] == "SELECT 1"
    assert result["suggestions"] == ["nouvelle suggestion"]


@pytest.mark.asyncio
async def test_suggest_regenerate_requires_existing_tests(tmp_path, monkeypatch):
    """Sans cas de test, les suggestions n'ont rien à contextualiser — erreur claire."""
    config = _project(tmp_path, {"sql": "SELECT 1", "test_cases": []})
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    monkeypatch.setattr("models.env_variables.validate_required_env", lambda: None)

    with pytest.raises(TestDocError, match="Aucun cas de test"):
        await run_suggest_regenerate(config, "orders")
