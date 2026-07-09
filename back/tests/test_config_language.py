import pytest

from storage import config


@pytest.fixture
def project_dir(tmp_path, monkeypatch):
    """Pointe la config sur un dossier temporaire et vide le cache lru."""
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    monkeypatch.delenv("MOCKSQL_LANGUAGE", raising=False)
    config.load_config.cache_clear()
    yield tmp_path
    config.load_config.cache_clear()


def _write_yml(project_dir, body: str) -> None:
    (project_dir / "mocksql.yml").write_text(body, encoding="utf-8")


def test_language_default_is_english(project_dir):
    # Aucune config, aucun env : l'anglais est le défaut produit.
    _write_yml(project_dir, "dialect: bigquery\n")
    assert config.get_language() == "en"
    assert config.output_language_name() == "English"


def test_language_from_config_french(project_dir):
    _write_yml(project_dir, "language: fr\n")
    assert config.get_language() == "fr"
    assert config.output_language_name() == "French"


def test_language_config_takes_precedence_over_env(project_dir, monkeypatch):
    # mocksql.yml prime sur l'env (parité avec les autres réglages).
    _write_yml(project_dir, "language: fr\n")
    monkeypatch.setenv("MOCKSQL_LANGUAGE", "en")
    assert config.get_language() == "fr"


def test_language_env_fallback_when_config_absent(project_dir, monkeypatch):
    _write_yml(project_dir, "dialect: bigquery\n")
    monkeypatch.setenv("MOCKSQL_LANGUAGE", "fr")
    assert config.get_language() == "fr"


def test_language_unknown_falls_back_to_english(project_dir):
    # Langue non supportée / faute de frappe : jamais d'erreur, repli anglais.
    _write_yml(project_dir, "language: klingon\n")
    assert config.get_language() == "en"


def test_language_regional_code_keeps_prefix(project_dir):
    _write_yml(project_dir, "language: fr-FR\n")
    assert config.get_language() == "fr"


def test_language_directive_names_configured_language(project_dir):
    _write_yml(project_dir, "language: fr\n")
    directive = config.output_language_directive()
    assert "French" in directive
    # La directive doit sortir sans accolades : elle est injectée dans des
    # ChatPromptTemplate LangChain qui interprètent `{}` comme des placeholders.
    assert "{" not in directive and "}" not in directive
