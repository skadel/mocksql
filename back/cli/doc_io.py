"""I/O partagée des fichiers de test pour les CLIs qui mutent un document de modèle
(`remove-test`, `validate`, `confirm`, `suggest`).

Chaque modèle a un document unique `.mocksql/tests/{model}.json` (définition commitée)
+ son cache sidecar gitignoré, fusionnés de façon transparente par `read_test_doc`.
Le ciblage d'un cas se fait toujours par `test_uid` — jamais par position (cf.
l'invariant d'identité test_uid vs test_index).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from storage.test_files import read_test_doc, write_test_doc


def find_test_case(doc: dict[str, Any], test_uid: str) -> dict[str, Any] | None:
    """Retourne le test_case dont le test_uid correspond, ou None."""
    for tc in doc.get("test_cases", []):
        if tc.get("test_uid") == test_uid:
            return tc
    return None


class TestDocError(Exception):
    """Erreur métier CLI : modèle, test ou assertion introuvable."""

    __test__ = False  # préfixe Test* : ne pas collecter comme classe de test pytest


def model_file(config_path: Path, model: str) -> Path:
    return config_path.parent / ".mocksql" / "tests" / f"{model}.json"


def load_doc(config_path: Path, model: str) -> tuple[Path, dict[str, Any]]:
    path = model_file(config_path, model)
    if not path.exists():
        raise TestDocError(
            f"Aucun test pour le modèle '{model}'. Lance `mocksql generate {model}.sql`."
        )
    doc = read_test_doc(path)
    if doc is None:
        raise TestDocError(f"Fichier de test illisible : {path}")
    return path, doc


def save_doc(path: Path, doc: dict[str, Any]) -> None:
    write_test_doc(path, doc)


def require_test_case(doc: dict[str, Any], test_uid: str) -> dict[str, Any]:
    tc = find_test_case(doc, test_uid)
    if tc is None:
        known = [t.get("test_uid") for t in doc.get("test_cases", [])]
        raise TestDocError(f"test_uid '{test_uid}' introuvable. Connus : {known}")
    return tc
