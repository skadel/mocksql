"""Régression : `utils.errors` doit s'importer sans l'extra `bigquery`.

Le connecteur BigQuery (`google-cloud-bigquery` + `google-api-core`) est une
dépendance optionnelle (extra `bigquery`). La CI installe sans extras, donc
`google.api_core` est absent. `utils.errors` ne doit donc PAS importer
`google.api_core.exceptions` de façon inconditionnelle, sinon toute la chaîne
`validator` -> `errors` casse à la collecte des tests.
"""

import builtins
import importlib
import sys

import pytest


@pytest.fixture
def without_google_api_core(monkeypatch):
    """Simule l'absence de `google.api_core` (extra bigquery non installé)."""
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "google.api_core.exceptions" or name.startswith("google.api_core"):
            raise ImportError("No module named 'google.api_core'")
        return real_import(name, *args, **kwargs)

    # Purge les modules google.api_core déjà chargés pour forcer le ré-import.
    for mod in list(sys.modules):
        if mod.startswith("google.api_core"):
            monkeypatch.delitem(sys.modules, mod, raising=False)
    monkeypatch.delitem(sys.modules, "utils.errors", raising=False)
    monkeypatch.setattr(builtins, "__import__", fake_import)
    yield


@pytest.mark.usefixtures("without_google_api_core")
def test_errors_imports_without_bigquery_extra():
    """Le module s'importe et expose ses helpers même sans `google.api_core`."""
    errors = importlib.import_module("utils.errors")

    # Les fonctions publiques sont disponibles.
    assert callable(errors.handle_compile_phase_exceptions)
    assert callable(errors.handle_post_compile_exceptions)
    assert callable(errors.handle_execution_exceptions)

    # Les classes d'exception BigQuery existent en fallback (jamais matchées
    # par une vraie exception puisque BigQuery n'est pas installé).
    for cls in (errors.BadRequest, errors.Forbidden, errors.NotFound):
        assert isinstance(cls, type)
        assert issubclass(cls, Exception)


@pytest.mark.usefixtures("without_google_api_core")
def test_fallback_classes_do_not_match_real_exceptions():
    """Une exception standard ne doit pas être prise pour une erreur BigQuery."""
    errors = importlib.import_module("utils.errors")

    assert not isinstance(ValueError("boom"), errors.BadRequest)
    assert not isinstance(RuntimeError("boom"), errors.NotFound)
    assert not isinstance(Exception("boom"), errors.Forbidden)
