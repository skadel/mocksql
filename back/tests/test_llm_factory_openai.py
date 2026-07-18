"""Routage provider OpenAI dans make_llm + config associée.

Objectif : pouvoir lancer une éval (génération + juge) avec un modèle OpenAI
(`gpt-*`, `o3…`) sans toucher au chemin Gemini/Vertex existant. La clé est lue
depuis l'env `OPENAI_API_KEY` (back/.env). Le routage se fait par NOM de modèle
— les `llm.provider: vertexai` déjà présents dans les mocksql.yml des projets
d'éval ne doivent PAS envoyer un modèle gpt-* vers Vertex (404 garanti).
"""

import pytest

import utils.llm_factory as llm_factory
from storage import config


@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Pointe la config sur un dossier temporaire et vide le cache lru."""
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    for var in (
        "LLM_TIMEOUT",
        "DEFAULT_MODEL_NAME",
        "LLM_THINKING_BUDGET",
        "LLM_THINKING_LEVEL",
        "LLM_THINKING_SAFETY_BUDGET",
        "LLM_PROVIDER",
        "LLM_MAX_RETRIES",
    ):
        monkeypatch.delenv(var, raising=False)
    config.load_config.cache_clear()
    yield tmp_path
    config.load_config.cache_clear()


def _write_yml(project_dir, body: str) -> None:
    (project_dir / "mocksql.yml").write_text(body, encoding="utf-8")


@pytest.fixture
def captured_openai_kwargs(monkeypatch):
    captured = {}

    class FakeLLM:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(llm_factory, "ChatOpenAI", FakeLLM)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    return captured


@pytest.fixture
def captured_gemini_kwargs(monkeypatch):
    captured = {}

    class FakeLLM:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(llm_factory, "ChatGoogleGenerativeAI", FakeLLM)
    return captured


# ─────────────────────────────── get_llm_provider ───────────────────────────────


def test_provider_gpt_name_detected(project_dir):
    assert config.get_llm_provider("gpt-5-mini") == "openai"


def test_provider_o_series_detected(project_dir):
    assert config.get_llm_provider("o4-mini") == "openai"


def test_provider_gemini_name_detected(project_dir):
    assert config.get_llm_provider("gemini-2.5-flash") == "vertexai"


def test_provider_yml_vertexai_does_not_hijack_gpt(project_dir):
    # Les mocksql.yml des projets d'éval portent déjà `provider: vertexai` :
    # le nom de modèle prime, sinon un gpt-* partirait vers Vertex (404).
    _write_yml(project_dir, "llm:\n  provider: vertexai\n")
    assert config.get_llm_provider("gpt-5-mini") == "openai"


def test_provider_yml_decides_for_unknown_name(project_dir):
    _write_yml(project_dir, "llm:\n  provider: openai\n")
    assert config.get_llm_provider("my-proxy-model") == "openai"


def test_provider_env_decides_for_unknown_name(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  streaming: false\n")
    monkeypatch.setenv("LLM_PROVIDER", "openai")
    assert config.get_llm_provider("my-proxy-model") == "openai"


def test_provider_unknown_name_defaults_to_vertexai(project_dir):
    assert config.get_llm_provider("my-proxy-model") == "vertexai"


# ──────────────────────── is_openai_reasoning_model ────────────────────────


def test_gpt5_is_reasoning(project_dir):
    assert config.is_openai_reasoning_model("gpt-5-mini") is True


def test_o_series_is_reasoning(project_dir):
    assert config.is_openai_reasoning_model("o3") is True


def test_gpt41_is_not_reasoning(project_dir):
    assert config.is_openai_reasoning_model("gpt-4.1-mini") is False


def test_gpt5_chat_is_not_reasoning(project_dir):
    assert config.is_openai_reasoning_model("gpt-5-chat-latest") is False


# ─────────────────────── is_native_thinking_active (openai) ───────────────────────


def test_native_thinking_active_for_gpt5(project_dir):
    _write_yml(project_dir, "llm:\n  model: gpt-5-mini\n")
    assert config.is_native_thinking_active() is True


def test_native_thinking_inactive_for_gpt41(project_dir):
    _write_yml(project_dir, "llm:\n  model: gpt-4.1-mini\n")
    assert config.is_native_thinking_active() is False


# ─────────────────────────────── make_llm (openai) ───────────────────────────────


def test_make_llm_routes_gpt_to_openai(project_dir, captured_openai_kwargs):
    llm_factory.make_llm(model="gpt-5-mini")
    assert captured_openai_kwargs["model"] == "gpt-5-mini"
    # Les kwargs Gemini ne doivent PAS fuir vers ChatOpenAI.
    for gemini_only in ("vertexai", "thinking_budget", "thinking_level", "location"):
        assert gemini_only not in captured_openai_kwargs


def test_make_llm_default_still_gemini(
    project_dir, captured_openai_kwargs, captured_gemini_kwargs
):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    llm_factory.make_llm()
    assert captured_gemini_kwargs["model"] == "gemini-2.5-flash"
    assert not captured_openai_kwargs


def test_make_llm_openai_propagates_timeout_retries_callbacks(
    project_dir, captured_openai_kwargs
):
    llm_factory.make_llm(model="gpt-5-mini")
    assert captured_openai_kwargs["timeout"] == 300
    assert captured_openai_kwargs["max_retries"] == 6
    assert captured_openai_kwargs["callbacks"]


def test_make_llm_reasoning_model_omits_temperature(
    project_dir, captured_openai_kwargs
):
    # gpt-5* / o-série ne supportent que la température par défaut → ne pas l'envoyer.
    llm_factory.make_llm(model="gpt-5-mini", temperature=0)
    assert "temperature" not in captured_openai_kwargs


def test_make_llm_non_reasoning_keeps_temperature(project_dir, captured_openai_kwargs):
    llm_factory.make_llm(model="gpt-4.1-mini", temperature=0.1)
    assert captured_openai_kwargs["temperature"] == 0.1


def test_make_llm_thinking_level_maps_to_reasoning_effort(
    project_dir, captured_openai_kwargs
):
    _write_yml(project_dir, "llm:\n  thinking_level: low\n")
    llm_factory.make_llm(model="gpt-5-mini")
    assert captured_openai_kwargs["reasoning_effort"] == "low"
    assert "thinking_level" not in captured_openai_kwargs


def test_make_llm_no_reasoning_effort_by_default(project_dir, captured_openai_kwargs):
    llm_factory.make_llm(model="gpt-5-mini")
    assert "reasoning_effort" not in captured_openai_kwargs


def test_make_llm_open_api_key_fallback(
    project_dir, captured_openai_kwargs, monkeypatch
):
    # Repli sur la variante OPEN_API_KEY présente dans back/.env.
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("OPEN_API_KEY", "sk-fallback")
    llm_factory.make_llm(model="gpt-5-mini")
    assert captured_openai_kwargs["api_key"] == "sk-fallback"


def test_make_llm_missing_api_key_fails_fast(
    project_dir, captured_openai_kwargs, monkeypatch
):
    # Fail-fast actionnable plutôt qu'une 401 tardive au premier appel.
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("OPEN_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        llm_factory.make_llm(model="gpt-5-mini")


def test_make_llm_missing_package_actionable(project_dir, monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test")
    monkeypatch.setattr(llm_factory, "ChatOpenAI", None)
    with pytest.raises(RuntimeError, match="langchain-openai"):
        llm_factory.make_llm(model="gpt-5-mini")
