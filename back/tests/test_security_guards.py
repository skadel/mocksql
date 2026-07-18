"""Régression sécurité (audit 2026-07) : traversée de chemin via `model_name` et
injection de commande via le CLI `bq`.

1. Traversal — `_test_path` / `read_model_sql` / `load_model_context` joignaient
   `model_name` (contrôlé par l'appelant HTTP) au filesystem sans vérifier que le
   résultat reste sous la racine : `../../` lisait/écrivait/supprimait hors de
   `.mocksql/tests` et de `models_path`. Idem pour le catch-all SPA de server.py
   (couvert ici au niveau du helper `safe_join`).

2. Commande bq — `_run_bq_cli` passait `shell=True` : sous Windows, les
   métacaractères cmd.exe (`&`, `|`, `%`) dans un argument non validé exécutent
   une commande arbitraire. Le fix retire `shell=True` (résolution de `bq` via
   `shutil.which`) et valide chaque composant (proj/dataset/table/billing_project)
   au point de construction de la commande — défense en profondeur, l'API valide
   déjà en amont via `validate_bq_ref`.
"""

import asyncio
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    import storage.config as config

    config.load_config.cache_clear()
    import storage.test_repository as tr

    return tr


# ---------------------------------------------------------------------------
# safe_join (helper partagé — utilisé aussi par le catch-all SPA de server.py)
# ---------------------------------------------------------------------------


def test_safe_join_accepts_nested_relative(tmp_path):
    from utils.path_guard import safe_join

    p = safe_join(tmp_path, "finance/revenue", suffix=".json")
    assert p is not None
    assert p == tmp_path / "finance" / "revenue.json"


def test_safe_join_rejects_parent_escape(tmp_path):
    from utils.path_guard import safe_join

    assert safe_join(tmp_path, "../evil", suffix=".json") is None
    assert safe_join(tmp_path, "a/../../evil") is None


def test_safe_join_rejects_absolute_path(tmp_path):
    from utils.path_guard import safe_join

    outside = tmp_path.parent / "evil.sql"
    assert safe_join(tmp_path, str(outside)) is None


# ---------------------------------------------------------------------------
# test_repository : _test_path / read_model_sql
# ---------------------------------------------------------------------------


def test_test_path_rejects_traversal(repo, tmp_path):
    with pytest.raises(ValueError):
        repo._test_path("../../evil")
    # Un nom imbriqué légitime continue de fonctionner.
    p = repo._test_path("finance/revenue")
    assert p.name == "revenue.json"


def test_read_model_sql_rejects_traversal(repo, tmp_path):
    # Un fichier .sql existe HORS de models_path — il ne doit pas être lisible.
    secret = tmp_path / "secret.sql"
    secret.write_text("SELECT 'secret'", encoding="utf-8")
    models_dir = Path(repo.get_models_path())
    models_dir.mkdir(parents=True, exist_ok=True)
    rel_escape = "../secret"
    assert repo.read_model_sql(rel_escape) is None


def test_model_file_helpers_reject_traversal(repo, tmp_path):
    secret = tmp_path / "secret.sql"
    secret.write_text("SELECT 'secret'", encoding="utf-8")
    assert repo.get_model_file_hash("../secret") is None
    assert repo.get_model_file_git_sha("../secret") is None
    assert repo.get_commits_since_sha("../secret", "deadbeef") == 0


def test_load_model_context_rejects_traversal(repo, tmp_path):
    from storage.context_loader import load_model_context

    (tmp_path / "secret.md").write_text("contexte secret", encoding="utf-8")
    assert load_model_context("../secret") == ""


# ---------------------------------------------------------------------------
# schema_fetcher : _run_bq_cli sans shell, composants validés
# ---------------------------------------------------------------------------


def _fake_completed(stdout="{}"):
    return SimpleNamespace(stdout=stdout, stderr="", returncode=0)


def test_run_bq_cli_does_not_use_shell(monkeypatch):
    from build_query import schema_fetcher

    captured = {}

    def fake_run(cmd, **kwargs):
        captured["cmd"] = cmd
        captured.update(kwargs)
        return _fake_completed()

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.setattr(
        "shutil.which", lambda name: r"C:\fake\google-cloud-sdk\bin\bq.cmd"
    )

    out = asyncio.run(
        schema_fetcher._run_bq_cli(["bq", "show", "--format=prettyjson"], "proj")
    )
    assert out == "{}"
    assert not captured.get("shell"), "shell=True réintroduit dans _run_bq_cli"
    # L'exécutable est résolu explicitement (shutil.which), pas par cmd.exe.
    assert captured["cmd"][0].endswith(("bq.cmd", "bq.exe", "bq"))


def test_run_bq_cli_fails_fast_when_bq_missing(monkeypatch):
    from build_query import schema_fetcher

    # Filet : si le code (pré-fix) tente quand même un subprocess, ne jamais
    # exécuter le vrai `bq` (hang interactif possible) — échouer immédiatement.
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: _fake_completed("should not run")
    )
    monkeypatch.setattr("shutil.which", lambda name: None)
    with pytest.raises(RuntimeError, match="bq"):
        asyncio.run(schema_fetcher._run_bq_cli(["bq", "show"], "proj"))


def test_fetch_table_via_cli_rejects_metacharacters(monkeypatch):
    """Défense en profondeur : même si un appelant oublie validate_bq_ref, aucun
    subprocess ne doit être lancé avec un composant contenant un métacaractère."""
    from build_query import schema_fetcher

    def boom(*args, **kwargs):  # pragma: no cover — ne doit jamais être atteint
        raise AssertionError("subprocess lancé avec un ref non validé")

    monkeypatch.setattr(subprocess, "run", boom)

    with pytest.raises(ValueError):
        asyncio.run(
            schema_fetcher._fetch_table_via_cli("proj.data&calc.table", "billing")
        )
    with pytest.raises(ValueError):
        asyncio.run(
            schema_fetcher._fetch_partition_values_cli("proj.dataset.tab|le", "billing")
        )
    with pytest.raises(ValueError):
        # billing_project entre aussi dans la ligne de commande (--project_id=…).
        asyncio.run(
            schema_fetcher._fetch_table_via_cli("proj.dataset.table", "bad&billing")
        )
