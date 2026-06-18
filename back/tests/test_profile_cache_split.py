"""Le profil (PII : top_values, left_key_sample, sample_values) vit dans un cache
SÉPARÉ et gitignoré (`profile.json`), jamais dans le `schema_cache.json` commité.

Comportement visé :
  - save_profile / save_sample_values écrivent dans profile.json, jamais dans schema_cache.json ;
  - un schema_cache.json legacy (profil inline) est migré au chargement : profil → profile.json,
    schema_cache.json réécrit sans `profile`/`sample_values` ; la migration est idempotente ;
  - ensure_mocksql_dir gitignore profile.json.
"""

import json
from pathlib import Path

import pytest

from models import schemas


@pytest.fixture
def caches(tmp_path, monkeypatch):
    """Redirige les deux caches vers tmp et réinitialise l'état module."""
    schema_path = tmp_path / ".mocksql" / "schema_cache.json"
    profile_path = tmp_path / ".mocksql" / "profile.json"
    monkeypatch.setattr(schemas, "SCHEMA_CACHE_PATH", str(schema_path))
    monkeypatch.setattr(schemas, "PROFILE_CACHE_PATH", str(profile_path))
    schemas._profile_cache = None
    schemas._cache = None
    schemas._cache_by_name = None
    schemas._cache_time = None
    yield schema_path, profile_path
    schemas._profile_cache = None


def _read(p: Path) -> dict:
    return json.loads(p.read_text(encoding="utf-8"))


# --- round-trip : le profil va dans profile.json, pas dans schema_cache ------


def test_save_profile_writes_to_profile_cache_only(caches):
    schema_path, profile_path = caches
    profile = {"tables": {"orders": {"columns": {"email": {"top_values": ["a@b.co"]}}}}}

    schemas.save_profile(profile)

    assert schemas.get_profile() == profile
    assert profile_path.exists()
    assert _read(profile_path)["profile"] == profile
    # schema_cache.json ne doit PAS contenir le profil (ni les emails bruts).
    if schema_path.exists():
        sc = _read(schema_path)
        assert "profile" not in sc
        assert "sample_values" not in sc


def test_save_sample_values_writes_to_profile_cache_only(caches):
    schema_path, profile_path = caches

    schemas.save_sample_values("orders.email", ["a@b.co", "c@d.co"])

    assert schemas.get_sample_values() == {"orders.email": ["a@b.co", "c@d.co"]}
    assert _read(profile_path)["sample_values"] == {
        "orders.email": ["a@b.co", "c@d.co"]
    }
    if schema_path.exists():
        assert "sample_values" not in _read(schema_path)


def test_saving_schema_does_not_carry_profile(caches):
    schema_path, _ = caches
    schemas.save_profile({"tables": {"t": {}}})
    schemas.save_schemas([{"table_name": "proj.ds.orders", "columns": []}])

    sc = _read(schema_path)
    assert sc["tables"][0]["table_name"] == "proj.ds.orders"
    assert "profile" not in sc
    assert "sample_values" not in sc


# --- migration des schema_cache.json legacy ----------------------------------


def test_legacy_inline_profile_is_migrated_out_of_schema_cache(caches):
    schema_path, profile_path = caches
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    legacy = {
        "tables": [{"table_name": "proj.ds.orders", "columns": []}],
        "profile": {
            "tables": {"orders": {"columns": {"email": {"top_values": ["x@y.co"]}}}}
        },
        "sample_values": {"orders.email": ["x@y.co"]},
    }
    schema_path.write_text(json.dumps(legacy), encoding="utf-8")

    # Le chargement du profil déclenche la migration.
    assert schemas.get_profile() == legacy["profile"]

    sc = _read(schema_path)
    assert "profile" not in sc
    assert "sample_values" not in sc
    assert sc["tables"] == legacy["tables"]

    prof = _read(profile_path)
    assert prof["profile"] == legacy["profile"]
    assert prof["sample_values"] == legacy["sample_values"]


def test_migration_is_idempotent(caches):
    schema_path, profile_path = caches
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    legacy = {
        "tables": [{"table_name": "t", "columns": []}],
        "profile": {"tables": {"t": {}}},
    }
    schema_path.write_text(json.dumps(legacy), encoding="utf-8")

    schemas.get_profile()
    schemas._profile_cache = None  # force un 2e chargement réel
    first = _read(profile_path)
    schemas.get_profile()
    assert _read(profile_path) == first
    assert "profile" not in _read(schema_path)


# --- gitignore ---------------------------------------------------------------


def test_ensure_mocksql_dir_gitignores_profile_cache(tmp_path):
    from storage.config import ensure_mocksql_dir

    mocksql_dir = tmp_path / ".mocksql"
    ensure_mocksql_dir(mocksql_dir)
    content = (mocksql_dir / ".gitignore").read_text(encoding="utf-8")
    assert "profile.json" in content
    assert "data/" in content


def test_ensure_mocksql_dir_appends_missing_entry_to_existing_gitignore(tmp_path):
    """Projet existant avec un .gitignore legacy (data/ seul) → profile.json ajouté."""
    from storage.config import ensure_mocksql_dir

    mocksql_dir = tmp_path / ".mocksql"
    mocksql_dir.mkdir()
    (mocksql_dir / ".gitignore").write_text("data/\n", encoding="utf-8")
    ensure_mocksql_dir(mocksql_dir)
    content = (mocksql_dir / ".gitignore").read_text(encoding="utf-8")
    assert "profile.json" in content
    assert content.count("data/") == 1  # pas de doublon


# --- config getter (parité avec la clé schema_cache du CLI) ------------------


def test_get_profile_cache_path_reads_mocksql_yml(tmp_path, monkeypatch):
    import storage.config as config

    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    (tmp_path / "mocksql.yml").write_text(
        "profile_cache: .mocksql/custom_profile.json\n", encoding="utf-8"
    )
    config.load_config.cache_clear()
    try:
        resolved = config.get_profile_cache_path()
    finally:
        config.load_config.cache_clear()
    assert resolved == str(tmp_path / ".mocksql" / "custom_profile.json")
