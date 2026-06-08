import pytest

from storage import config


@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Pointe la config sur un dossier temporaire et vide le cache lru."""
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    monkeypatch.delenv("LLM_THINKING_BUDGET", raising=False)
    monkeypatch.delenv("LLM_THINKING_LEVEL", raising=False)
    monkeypatch.delenv("DEFAULT_MODEL_NAME", raising=False)
    config.load_config.cache_clear()
    yield tmp_path
    config.load_config.cache_clear()


def _write_yml(project_dir, body: str) -> None:
    (project_dir / "mocksql.yml").write_text(body, encoding="utf-8")


def test_thinking_budget_zero_is_preserved(project_dir):
    # Régression : `thinking_budget: 0` désactive le thinking Gemini, mais
    # l'ancien `cfg.get(...) or os.getenv(...)` traitait 0 comme falsy et
    # retournait None → le thinking n'était jamais désactivé.
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n  thinking_budget: 0\n")
    assert config.get_llm_thinking_budget() == 0


def test_thinking_budget_positive_value(project_dir):
    _write_yml(project_dir, "llm:\n  thinking_budget: 1024\n")
    assert config.get_llm_thinking_budget() == 1024


def test_thinking_budget_absent_returns_none(project_dir):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    assert config.get_llm_thinking_budget() is None


def test_thinking_budget_env_fallback(project_dir, monkeypatch):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    monkeypatch.setenv("LLM_THINKING_BUDGET", "0")
    assert config.get_llm_thinking_budget() == 0


def test_thinking_budget_invalid_returns_none(project_dir):
    _write_yml(project_dir, "llm:\n  thinking_budget: not_an_int\n")
    assert config.get_llm_thinking_budget() is None


# ---------------------------------------------------------------------------
# is_native_thinking_active : combine le réglage explicite et le défaut modèle.
# ---------------------------------------------------------------------------


def test_native_thinking_flash_default_on(project_dir):
    # flash : thinking ON par défaut (aucun budget passé).
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n")
    assert config.is_native_thinking_active() is True


def test_native_thinking_pro_default_on(project_dir):
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-pro\n")
    assert config.is_native_thinking_active() is True


def test_native_thinking_flash_lite_default_off(project_dir):
    # flash-lite : thinking OFF par défaut — et 'lite' contient 'flash',
    # donc l'ordre des tests dans le helper compte (lite testé en premier).
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash-lite\n")
    assert config.is_native_thinking_active() is False


def test_native_thinking_budget_zero_disables(project_dir):
    # thinking_budget: 0 désactive le thinking même sur flash.
    _write_yml(project_dir, "llm:\n  model: gemini-2.5-flash\n  thinking_budget: 0\n")
    assert config.is_native_thinking_active() is False


def test_native_thinking_explicit_budget_enables_on_lite(project_dir):
    # budget>0 explicite réactive le thinking, même sur flash-lite.
    _write_yml(
        project_dir, "llm:\n  model: gemini-2.5-flash-lite\n  thinking_budget: 512\n"
    )
    assert config.is_native_thinking_active() is True


def test_native_thinking_unknown_model_defaults_off(project_dir):
    # Modèle inconnu : on suppose thinking inactif → fallback CoT in-schema (sûr).
    _write_yml(project_dir, "llm:\n  model: some-other-model\n")
    assert config.is_native_thinking_active() is False
