"""Regression tests for cli/schema_cache.py — the new on-disk format is
{"tables": [...], "profile": {...}} but the shared CLI loader used to return
the raw JSON, crashing `generate`/`profile` with
`TypeError: string indices must be integers`.
"""

import json

from cli.schema_cache import load_schema_cache, save_schema_cache


def _write(path, data):
    path.write_text(json.dumps(data), encoding="utf-8")


def test_load_unwraps_dict_format(tmp_path):
    cache = tmp_path / "schema_cache.json"
    _write(
        cache,
        {
            "tables": [{"table_name": "p.d.t", "columns": []}],
            "profile": {"tables": {}},
        },
    )
    loaded = load_schema_cache(str(cache))
    assert isinstance(loaded, list)
    assert loaded[0]["table_name"] == "p.d.t"


def test_load_handles_legacy_list_format(tmp_path):
    cache = tmp_path / "schema_cache.json"
    _write(cache, [{"table_name": "p.d.t", "columns": []}])
    loaded = load_schema_cache(str(cache))
    assert isinstance(loaded, list)
    assert loaded[0]["table_name"] == "p.d.t"


def test_load_missing_file_returns_empty_list(tmp_path):
    loaded = load_schema_cache(str(tmp_path / "nope.json"))
    assert loaded == []


def test_save_preserves_profile_and_sample_values(tmp_path):
    cache = tmp_path / "schema_cache.json"
    _write(
        cache,
        {
            "tables": [{"table_name": "p.d.old", "columns": []}],
            "profile": {"tables": {"x": 1}},
            "sample_values": {"p.d.old.c": ["a"]},
        },
    )
    save_schema_cache(str(cache), [{"table_name": "p.d.new", "columns": []}])

    raw = json.loads(cache.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    assert raw["profile"] == {"tables": {"x": 1}}
    assert raw["sample_values"] == {"p.d.old.c": ["a"]}
    assert raw["tables"] == [{"table_name": "p.d.new", "columns": []}]
