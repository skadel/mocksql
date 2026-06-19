"""Régression : split définition (commitée, lisible) / cache (gitignoré) du fichier de test.

Garantit que :
  - le fichier `.mocksql/tests/{model}.json` commité ne porte QUE la définition,
  - le dérivé/runtime part dans `.mocksql/cache/{model}.json`,
  - `used_columns` est stocké en JSON imbriqué lisible sur le disque,
  - la lecture recompose un dict identique à la forme mémoire (round-trip),
  - le réplay sans cache (clone/CI) reste correct.
"""

import json
from pathlib import Path

from storage.test_files import cache_path_for, read_test_doc, write_test_doc


def _doc() -> dict:
    return {
        "test_id": "abc",
        "model_name": "demo/payment_summary",
        "sql": "SELECT 1",
        "optimized_sql": "SELECT 1 AS one",
        "used_columns": [
            '{"project": "p", "database": "d", "table": "t", "used_columns": ["a"]}'
        ],
        "query_decomposed": '[{"name": "final", "code": "SELECT 1"}]',
        "last_error": "",
        "suggestions": ["cas limite NULL"],
        "test_cases": [
            {
                "test_index": "1",
                "test_uid": "9a0c",
                "test_name": "happy",
                "unit_test_description": "cas nominal",
                "unit_test_build_reasoning": "long raisonnement LLM…",
                "tags": ["Logique métier"],
                "data": {"t": [{"a": 1}]},
                "status": "complete",
                "results_json": '[{"a": 1}]',
                "assertion_results": [
                    {
                        "description": "a vaut 1",
                        "sql": "SELECT * FROM __result__ WHERE a != 1",
                    }
                ],
                "verdict": "Excellent",
                "reason_type": None,
                "evaluation_explanation": "ok",
            }
        ],
    }


def test_definition_file_is_clean_and_readable(tmp_path: Path):
    tests_dir = tmp_path / ".mocksql" / "tests"
    path = tests_dir / "demo" / "payment_summary.json"
    write_test_doc(path, _doc())

    definition = json.loads(path.read_text(encoding="utf-8"))
    # Dérivé/runtime absents du fichier commité.
    for k in ("optimized_sql", "query_decomposed", "last_error"):
        assert k not in definition
    case = definition["test_cases"][0]
    for k in ("unit_test_build_reasoning", "status", "results_json", "reason_type"):
        assert k not in case
    # La définition garde le test lisible.
    assert definition["sql"] == "SELECT 1"
    assert case["assertion_results"][0]["description"] == "a vaut 1"
    # used_columns stocké en JSON imbriqué (pas une string échappée).
    assert isinstance(definition["used_columns"][0], dict)
    assert definition["used_columns"][0]["table"] == "t"


def test_cache_sidecar_holds_derived_and_runtime(tmp_path: Path):
    path = tmp_path / ".mocksql" / "tests" / "demo" / "payment_summary.json"
    write_test_doc(path, _doc())

    cache_path = cache_path_for(path)
    assert cache_path is not None and cache_path.exists()
    assert "cache" in cache_path.parts and "tests" not in cache_path.parts
    cache = json.loads(cache_path.read_text(encoding="utf-8"))
    assert cache["optimized_sql"] == "SELECT 1 AS one"
    assert cache["test_cases"]["1"]["results_json"] == '[{"a": 1}]'


def test_roundtrip_preserves_memory_shape(tmp_path: Path):
    path = tmp_path / ".mocksql" / "tests" / "demo" / "payment_summary.json"
    original = _doc()
    write_test_doc(path, original)

    loaded = read_test_doc(path)
    assert (
        loaded == original
    )  # forme mémoire strictement identique (used_columns = list[str])


def test_replay_without_cache_keeps_definition(tmp_path: Path):
    path = tmp_path / ".mocksql" / "tests" / "demo" / "payment_summary.json"
    write_test_doc(path, _doc())
    cache_path_for(path).unlink()  # simule un clone / CI sans cache gitignoré

    loaded = read_test_doc(path)
    assert loaded["sql"] == "SELECT 1"
    assert loaded["test_cases"][0]["data"] == {"t": [{"a": 1}]}
    assert loaded["test_cases"][0]["assertion_results"][0]["sql"]
    # used_columns reste exploitable (re-stringifié en list[str]).
    assert json.loads(loaded["used_columns"][0])["table"] == "t"
    # Champs dérivés absents (pas de cache) — les consommateurs ont des defaults.
    assert "optimized_sql" not in loaded


def test_legacy_inline_file_still_reads(tmp_path: Path):
    """Un vieux fichier tout-en-un (used_columns en strings, sans cache) se lit tel quel."""
    path = tmp_path / ".mocksql" / "tests" / "legacy.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    legacy = _doc()
    path.write_text(json.dumps(legacy, default=str), encoding="utf-8")

    loaded = read_test_doc(path)
    assert loaded["optimized_sql"] == "SELECT 1 AS one"
    assert loaded["test_cases"][0]["status"] == "complete"
