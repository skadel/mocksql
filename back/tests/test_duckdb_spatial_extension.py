"""Option `duckdb.extensions` dans mocksql.yml + message d'erreur actionnable.

Incident (éval thelook_ecommerce 2026-06-15) : les modèles staging utilisant des
fonctions géo BigQuery (`ST_GEOGPOINT` → transpilé `st_point`) échouaient à
l'exécution locale : `Catalog Error: Scalar Function with name "st_point" is not
in the catalog, but it exists in the spatial extension.` — l'extension spatial de
DuckDB n'était jamais chargée au boot.

Fix : `duckdb.extensions: [spatial]` dans mocksql.yml charge l'extension sur chaque
connexion (`apply_duckdb_extensions`), et toute requête qui touche une fonction
d'extension non chargée remonte un message expliquant comment l'activer
(`_missing_extension_hint`). Cf. [[project_duckdb_spatial_extension_gap]].
"""

import duckdb
import pytest

from storage import config
from utils.examples import _missing_extension_hint


def test_spatial_fails_without_extension():
    """Sans extension chargée, st_point est introuvable (reproduit l'incident)."""
    con = duckdb.connect(":memory:")
    with pytest.raises(Exception) as exc:
        con.execute("SELECT st_point(1, 2)")
    assert "spatial extension" in str(exc.value)


def test_apply_duckdb_extensions_loads_spatial(monkeypatch):
    """Avec `duckdb.extensions: [spatial]`, st_point fonctionne."""
    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: ["spatial"])
    con = duckdb.connect(":memory:")
    config.apply_duckdb_extensions(con)
    assert (
        con.execute("SELECT ST_AsText(st_point(1, 2))").fetchone()[0] == "POINT (1 2)"
    )


def test_apply_duckdb_extensions_noop_when_empty(monkeypatch):
    """Aucune extension configurée → aucun INSTALL/LOAD, pas d'erreur."""
    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: [])
    con = duckdb.connect(":memory:")
    config.apply_duckdb_extensions(con)  # ne doit pas lever


def test_apply_duckdb_extensions_bad_ext_warns_not_raises(monkeypatch):
    """Une extension invalide est journalisée sans interrompre la connexion."""
    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: ["__nope__"])
    con = duckdb.connect(":memory:")
    config.apply_duckdb_extensions(con)  # warning, pas d'exception
    assert con.execute("SELECT 1").fetchone()[0] == 1


def test_missing_extension_hint_actionable():
    err = (
        'Catalog Error: Scalar Function with name "st_point" is not in the '
        "catalog, but it exists in the spatial extension."
    )
    hint = _missing_extension_hint(err)
    assert hint is not None
    assert "spatial" in hint
    assert "mocksql.yml" in hint
    assert "extensions:" in hint


def test_missing_extension_hint_none_for_unrelated_error():
    assert _missing_extension_hint("Binder Error: column foo not found") is None
