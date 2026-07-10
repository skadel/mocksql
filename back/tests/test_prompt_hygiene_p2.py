"""Régression (audit c6.sql, P2 + params) — hygiène des prompts et garde-fous.

- P2-2 : `<input_data>` rendu en JSON (guillemets doubles, `null`/`true`) plutôt qu'en repr
  Python — cohérent avec les autres blocs.
- Garde-fou taille : un prompt géant (>200K tokens estimés) émet un warning — il aurait
  surfacé tôt le `<result_sample>` non plafonné à l'origine du 429.
- Params : `get_llm_max_retries` explicite et surchargeable (backoff sur 429 quota TPM).
"""

import logging
from types import SimpleNamespace

from build_query.examples_executor import _format_input_data
from storage.config import get_llm_max_retries
from utils.timing import LLMTimingCallback, _estimate_prompt_tokens


# ─────────────────────────── P2-2 · input_data en JSON ───────────────────────────


def test_format_input_data_is_json_not_python_repr():
    out = _format_input_data([{"a": None, "b": True, "c": "x"}])
    assert out == '[{"a": null, "b": true, "c": "x"}]'
    assert "'" not in out  # pas de guillemets simples (repr Python)


def test_format_input_data_falls_back_on_unserializable():
    class Weird:
        pass

    # default=str encaisse ; jamais d'exception qui casserait la génération du prompt.
    out = _format_input_data([{"x": Weird()}])
    assert isinstance(out, str) and out


# ─────────────────────────── garde-fou taille de prompt ───────────────────────────


def test_estimate_prompt_tokens_roughly_chars_over_4():
    msg = SimpleNamespace(content="x" * 4000)
    assert _estimate_prompt_tokens([[msg]]) == 1000


def test_large_prompt_emits_warning(caplog):
    cb = LLMTimingCallback()
    big = SimpleNamespace(content="x" * (4 * 201_000))  # ~201k tokens estimés
    with caplog.at_level(logging.WARNING, logger="timing"):
        cb.on_chat_model_start({"name": "m"}, [[big]], run_id="r-big")
    assert any("volumineux" in r.getMessage() for r in caplog.records)


def test_small_prompt_no_warning(caplog):
    cb = LLMTimingCallback()
    small = SimpleNamespace(content="hello")
    with caplog.at_level(logging.WARNING, logger="timing"):
        cb.on_chat_model_start({"name": "m"}, [[small]], run_id="r-small")
    assert not any("volumineux" in r.getMessage() for r in caplog.records)


# ─────────────────────────── params · max_retries ───────────────────────────


def test_max_retries_env_override(monkeypatch):
    monkeypatch.setenv("LLM_MAX_RETRIES", "9")
    assert get_llm_max_retries() == 9


def test_max_retries_invalid_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("LLM_MAX_RETRIES", "not-an-int")
    assert get_llm_max_retries() == 6
