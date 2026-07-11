"""Timeout LLM borné + visibilité du thinking (rumination).

Contexte (investigation 2026-07-10, bq001/spider) : un appel `generateContent` sur
gemini-3-flash-preview a tenu 302 s (200 OK, ~62 800 tokens de thinking pour 230 tokens
utiles) pendant lesquels la CLI attendait en silence — `make_llm` ne fixait aucun
timeout (google-genai → httpx `timeout=None` = attente infinie).

- `get_llm_timeout` : borne par appel, surchargeable (`llm.timeout` / `LLM_TIMEOUT`),
  `0` = désactivé. Un timeout client n'est PAS retenté par tenacity (le predicate ne
  matche que les APIError HTTP) → échec rapide et actionnable.
- `LLMTimingCallback` : la ligne `[timing] llm:` porte in/out/thinking, et un WARNING
  signale une rumination massive (thinking ≥ seuil) au niveau de log par défaut.
- `prompt_dump` : tokens de thinking dans l'en-tête + section `### reasoning` quand
  `include_thoughts` est actif (résumés de pensée Gemini).
"""

import logging
from types import SimpleNamespace
from uuid import uuid4

import pytest

import utils.llm_factory as llm_factory
from storage import config
from utils.prompt_dump import _render_output
from utils.timing import LLMTimingCallback


@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Pointe la config sur un dossier temporaire et vide le cache lru."""
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    for var in (
        "LLM_TIMEOUT",
        "LLM_INCLUDE_THOUGHTS",
        "DEFAULT_MODEL_NAME",
        "LLM_THINKING_BUDGET",
        "LLM_THINKING_LEVEL",
        "LLM_THINKING_SAFETY_BUDGET",
    ):
        monkeypatch.delenv(var, raising=False)
    config.load_config.cache_clear()
    yield tmp_path
    config.load_config.cache_clear()


def _write_yml(project_dir, body: str) -> None:
    (project_dir / "mocksql.yml").write_text(body, encoding="utf-8")


# ─────────────────────────────── get_llm_timeout ───────────────────────────────


def test_timeout_default_is_bounded(project_dir):
    # Le défaut DOIT être borné : timeout absent = hang infini silencieux (cf. module doc).
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    assert config.get_llm_timeout() == 300


def test_timeout_yml_override(project_dir):
    _write_yml(project_dir, "llm:\n  timeout: 120\n")
    assert config.get_llm_timeout() == 120


def test_timeout_env_fallback(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    monkeypatch.setenv("LLM_TIMEOUT", "60")
    assert config.get_llm_timeout() == 60


def test_timeout_yml_primes_over_env(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  timeout: 120\n")
    monkeypatch.setenv("LLM_TIMEOUT", "60")
    assert config.get_llm_timeout() == 120


def test_timeout_zero_disables(project_dir):
    _write_yml(project_dir, "llm:\n  timeout: 0\n")
    assert config.get_llm_timeout() is None


def test_timeout_invalid_falls_back_to_default(project_dir, monkeypatch):
    monkeypatch.setenv("LLM_TIMEOUT", "abc")
    assert config.get_llm_timeout() == 300


# ─────────────────────────── get_llm_include_thoughts ───────────────────────────


def test_include_thoughts_default_off(project_dir):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    assert config.get_llm_include_thoughts() is False


def test_include_thoughts_yml(project_dir):
    _write_yml(project_dir, "llm:\n  include_thoughts: true\n")
    assert config.get_llm_include_thoughts() is True


def test_include_thoughts_env_fallback(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    monkeypatch.setenv("LLM_INCLUDE_THOUGHTS", "true")
    assert config.get_llm_include_thoughts() is True


# ───────────────────── get_llm_thinking_safety_budget (dérivé) ─────────────────────


def test_safety_budget_default_from_timeout_is_clamped(project_dir):
    # Timeout défaut 300 s → 300*200*0.6 = 36000, borné au plafond API Gemini 2.5.
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    assert config.get_llm_thinking_safety_budget() == 24576


def test_safety_budget_scales_with_timeout(project_dir):
    # « les deux correspondent » : un timeout plus court réduit le budget dérivé.
    _write_yml(project_dir, "llm:\n  timeout: 100\n")
    assert config.get_llm_thinking_safety_budget() == 12000  # 100*200*0.6


def test_safety_budget_none_when_timeout_disabled(project_dir):
    # Timeout illimité + aucune surcharge → pas de plafond dérivé.
    _write_yml(project_dir, "llm:\n  timeout: 0\n")
    assert config.get_llm_thinking_safety_budget() is None


def test_safety_budget_yml_override(project_dir):
    _write_yml(project_dir, "llm:\n  thinking_safety_budget: 4096\n")
    assert config.get_llm_thinking_safety_budget() == 4096


def test_safety_budget_override_wins_even_when_timeout_disabled(project_dir):
    # La surcharge explicite prime sur la dérivation (ici neutralisée par timeout: 0).
    _write_yml(project_dir, "llm:\n  timeout: 0\n  thinking_safety_budget: 8192\n")
    assert config.get_llm_thinking_safety_budget() == 8192


def test_safety_budget_zero_disables(project_dir):
    _write_yml(project_dir, "llm:\n  thinking_safety_budget: 0\n")
    assert config.get_llm_thinking_safety_budget() is None


def test_safety_budget_env_fallback(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    monkeypatch.setenv("LLM_THINKING_SAFETY_BUDGET", "2048")
    assert config.get_llm_thinking_safety_budget() == 2048


def test_safety_budget_yml_primes_over_env(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  thinking_safety_budget: 4096\n")
    monkeypatch.setenv("LLM_THINKING_SAFETY_BUDGET", "2048")
    assert config.get_llm_thinking_safety_budget() == 4096


# ─────────────────────────────────── make_llm ───────────────────────────────────


@pytest.fixture
def captured_llm_kwargs(monkeypatch):
    captured = {}

    class FakeLLM:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(llm_factory, "ChatGoogleGenerativeAI", FakeLLM)
    return captured


def test_make_llm_passes_default_timeout(project_dir, captured_llm_kwargs):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs["timeout"] == 300


def test_make_llm_timeout_disabled(project_dir, captured_llm_kwargs):
    _write_yml(project_dir, "llm:\n  timeout: 0\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs.get("timeout") is None


def test_make_llm_include_thoughts_opt_in(project_dir, captured_llm_kwargs):
    _write_yml(project_dir, "llm:\n  include_thoughts: true\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs.get("include_thoughts") is True


def test_make_llm_include_thoughts_absent_by_default(project_dir, captured_llm_kwargs):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    llm_factory.make_llm()
    assert "include_thoughts" not in captured_llm_kwargs


def test_make_llm_injects_safety_budget_by_default(project_dir, captured_llm_kwargs):
    # Ni budget ni niveau explicites → plafond de sécurité dérivé du timeout est envoyé.
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs["thinking_budget"] == 24576


def test_make_llm_explicit_budget_wins_over_safety(project_dir, captured_llm_kwargs):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n  thinking_budget: 512\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs["thinking_budget"] == 512


def test_make_llm_explicit_level_skips_safety(project_dir, captured_llm_kwargs):
    # thinking_level explicite → on n'injecte PAS de budget (sinon il l'écraserait).
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n  thinking_level: low\n")
    llm_factory.make_llm()
    assert captured_llm_kwargs.get("thinking_level") == "low"
    assert "thinking_budget" not in captured_llm_kwargs


def test_make_llm_safety_disabled_sends_no_budget(project_dir, captured_llm_kwargs):
    _write_yml(
        project_dir, "llm:\n  model: gemini-2.5-flash\n  thinking_safety_budget: 0\n"
    )
    llm_factory.make_llm()
    assert "thinking_budget" not in captured_llm_kwargs


# ──────────────────────── LLMTimingCallback · tokens/thinking ────────────────────────


def _fake_response(input_tokens=3971, output_tokens=63088, reasoning=62858):
    usage = {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "output_token_details": {"reasoning": reasoning},
    }
    msg = SimpleNamespace(usage_metadata=usage, content="ok")
    gen = SimpleNamespace(message=msg, text="ok")
    return SimpleNamespace(generations=[[gen]], llm_output=None)


def test_timing_logs_thinking_tokens(caplog):
    cb = LLMTimingCallback()
    run_id = uuid4()
    with caplog.at_level(15, logger="timing"):  # DIAG
        cb.on_chat_model_start(
            {}, [[]], run_id=run_id, metadata={"ls_model_name": "gemini-x"}
        )
        cb.on_llm_end(_fake_response(), run_id=run_id)
    joined = " ".join(r.getMessage() for r in caplog.records)
    assert "in=3971" in joined
    assert "out=63088" in joined
    assert "thinking=62858" in joined


def test_timing_warns_on_massive_thinking(caplog):
    # Rumination (thinking ≥ seuil) → WARNING visible au niveau de log par défaut,
    # pas seulement en DIAG — c'est le signal « pourquoi c'était si lent ».
    cb = LLMTimingCallback()
    run_id = uuid4()
    with caplog.at_level(logging.WARNING, logger="timing"):
        cb.on_chat_model_start(
            {}, [[]], run_id=run_id, metadata={"ls_model_name": "gemini-x"}
        )
        cb.on_llm_end(_fake_response(reasoning=62858), run_id=run_id)
    assert any(r.levelno == logging.WARNING for r in caplog.records)


def test_timing_no_warning_below_threshold(caplog):
    cb = LLMTimingCallback()
    run_id = uuid4()
    with caplog.at_level(logging.WARNING, logger="timing"):
        cb.on_chat_model_start(
            {}, [[]], run_id=run_id, metadata={"ls_model_name": "gemini-x"}
        )
        cb.on_llm_end(_fake_response(output_tokens=1200, reasoning=900), run_id=run_id)
    assert not any(r.levelno == logging.WARNING for r in caplog.records)


def test_timing_survives_missing_usage(caplog):
    # Réponse sans usage_metadata (proto exotique) : la ligne timing sort quand même.
    cb = LLMTimingCallback()
    run_id = uuid4()
    resp = SimpleNamespace(generations=[[SimpleNamespace(message=None, text="")]])
    with caplog.at_level(15, logger="timing"):
        cb.on_chat_model_start(
            {}, [[]], run_id=run_id, metadata={"ls_model_name": "gemini-x"}
        )
        cb.on_llm_end(resp, run_id=run_id)
    assert any("[timing] llm:gemini-x" in r.getMessage() for r in caplog.records)


# ───────────────────────── prompt_dump · reasoning visible ─────────────────────────


def test_render_output_includes_reasoning_blocks():
    # include_thoughts=True → blocs {"type": "reasoning"} dans le content ;
    # le dump doit les rendre pour voir SUR QUOI le modèle rumine.
    msg = SimpleNamespace(
        content=[
            {"type": "reasoning", "reasoning": "Je considère les doublons par device…"},
            {"type": "text", "text": '{"suggestions": []}'},
        ],
        tool_calls=None,
        usage_metadata={"input_tokens": 10, "output_tokens": 20},
    )
    gen = SimpleNamespace(message=msg, text='{"suggestions": []}')
    resp = SimpleNamespace(generations=[[gen]], llm_output=None)
    output_md, usage = _render_output(resp)
    assert "### reasoning" in output_md
    assert "doublons par device" in output_md
    assert usage["input_tokens"] == 10
