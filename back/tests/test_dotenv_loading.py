import os
from pathlib import Path

from dotenv import load_dotenv


def _load(monkeypatch, tmp_path):
    """Reproduit l'appel exact de cli/main.py : load_dotenv(dotenv_path=Path('.env'))."""
    monkeypatch.chdir(tmp_path)
    load_dotenv(dotenv_path=Path(".env"))


class TestDotenvLoading:
    def test_loads_env_from_cwd(self, tmp_path, monkeypatch):
        """load_dotenv trouve le .env dans le répertoire courant du client."""
        (tmp_path / ".env").write_text(
            "VERTEX_PROJECT=my-test-project\nGOOGLE_CLOUD_LOCATION=us-central1\n"
        )
        monkeypatch.delenv("VERTEX_PROJECT", raising=False)
        monkeypatch.delenv("GOOGLE_CLOUD_LOCATION", raising=False)

        _load(monkeypatch, tmp_path)

        assert os.getenv("VERTEX_PROJECT") == "my-test-project"
        assert os.getenv("GOOGLE_CLOUD_LOCATION") == "us-central1"

    def test_does_not_override_existing_env_vars(self, tmp_path, monkeypatch):
        """Une variable déjà définie dans l'environnement système n'est pas écrasée."""
        (tmp_path / ".env").write_text("VERTEX_PROJECT=from-dotenv\n")
        monkeypatch.setenv("VERTEX_PROJECT", "from-system")

        _load(monkeypatch, tmp_path)

        assert os.getenv("VERTEX_PROJECT") == "from-system"

    def test_no_env_file_leaves_vars_unset(self, tmp_path, monkeypatch):
        """Sans .env dans le CWD, les variables restent non définies."""
        monkeypatch.delenv("VERTEX_PROJECT", raising=False)

        _load(monkeypatch, tmp_path)

        assert os.getenv("VERTEX_PROJECT") is None
