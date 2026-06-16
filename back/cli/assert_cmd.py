"""Runtime de la CLI `assert` : pose/édite une spec sur un test puis confirme son
état (rouge/vert) en rejouant le cas contre le SQL source du disque.

Permet la boucle TDD pilotée par un agent de code :
  1. `assert add`    → pose la cible, confirme qu'elle est ROUGE,
  2. l'agent édite le `.sql` source,
  3. `assert update`/`mocksql test` → relit le disque, confirme le passage au VERT.

La logique de mutation pure est dans `cli/assertions.py` ; ce module gère l'I/O
JSON et la ré-exécution.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from cli.assertions import (
    add_assertion,
    ensure_assertion_uids,
    find_test_case,
    remove_assertion,
    update_assertion,
)
from cli.test_runner import run_tests


class AssertError(Exception):
    """Erreur métier de la CLI assert (modèle/test/assertion introuvable)."""


def _model_file(config_path: Path, model: str) -> Path:
    return config_path.parent / ".mocksql" / "tests" / f"{model}.json"


def _load_doc(config_path: Path, model: str) -> tuple[Path, dict[str, Any]]:
    path = _model_file(config_path, model)
    if not path.exists():
        raise AssertError(
            f"Aucun test pour le modèle '{model}'. Lance `mocksql generate {model}.sql`."
        )
    return path, json.loads(path.read_text(encoding="utf-8"))


def _save_doc(path: Path, doc: dict[str, Any]) -> None:
    path.write_text(
        json.dumps(doc, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )


def _require_test_case(doc: dict[str, Any], test_uid: str) -> dict[str, Any]:
    tc = find_test_case(doc, test_uid)
    if tc is None:
        known = [t.get("test_uid") for t in doc.get("test_cases", [])]
        raise AssertError(f"test_uid '{test_uid}' introuvable. Connus : {known}")
    return tc


async def _reexec_assertion_status(
    config_path: Path, model: str, test_case: dict[str, Any], description: str
) -> dict[str, Any]:
    """Rejoue le modèle (SQL disque) et retourne l'état de l'assertion ciblée.

    Le résultat d'exécution ne porte pas l'assertion_uid (l'évaluateur reconstruit
    ses dicts) — on rapproche par `test_index` (cas) puis `description` (assertion).
    """
    _, results = await run_tests(config_path, [model], frozen=False)
    target_index = str(test_case.get("test_index", "0"))
    for mr in results:
        if mr["model"] != model:
            continue
        for case in mr["cases"]:
            if case.get("index") != target_index:
                continue
            for a in case.get("assertions", []):
                if a.get("description") == description:
                    return {
                        "passed": a.get("passed"),
                        "failing_rows": a.get("failing_rows", []),
                        "error": a.get("error"),
                    }
    return {"passed": None, "failing_rows": [], "error": "cas non rejoué"}


# ── Verbes ─────────────────────────────────────────────────────────────────────


def run_list(config_path: Path, model: str, test_uid: str) -> dict[str, Any]:
    path, doc = _load_doc(config_path, model)
    tc = _require_test_case(doc, test_uid)
    assertions = ensure_assertion_uids(tc)
    _save_doc(path, doc)  # persiste les uids rétro-remplis
    return {
        "model": model,
        "test_uid": test_uid,
        "assertions": [
            {
                "assertion_uid": a.get("assertion_uid"),
                "description": a.get("description"),
                "sql": a.get("sql"),
                "passed": a.get("passed"),
            }
            for a in assertions
        ],
    }


async def run_add(
    config_path: Path, model: str, test_uid: str, description: str, sql: str
) -> dict[str, Any]:
    path, doc = _load_doc(config_path, model)
    tc = _require_test_case(doc, test_uid)
    assertion = add_assertion(tc, description, sql)
    _save_doc(path, doc)
    status = await _reexec_assertion_status(config_path, model, tc, description)
    assertion.update(status)
    _save_doc(path, doc)  # persiste le verdict frais
    return {"model": model, "test_uid": test_uid, "assertion": assertion}


async def run_update(
    config_path: Path,
    model: str,
    test_uid: str,
    assertion_uid: str,
    description: str | None,
    sql: str | None,
) -> dict[str, Any]:
    path, doc = _load_doc(config_path, model)
    tc = _require_test_case(doc, test_uid)
    try:
        assertion = update_assertion(
            tc, assertion_uid, description=description, sql=sql
        )
    except KeyError:
        raise AssertError(f"assertion_uid '{assertion_uid}' introuvable.")
    _save_doc(path, doc)
    status = await _reexec_assertion_status(
        config_path, model, tc, assertion["description"]
    )
    assertion.update(status)
    _save_doc(path, doc)
    return {"model": model, "test_uid": test_uid, "assertion": assertion}


def run_remove(
    config_path: Path, model: str, test_uid: str, assertion_uid: str
) -> dict[str, Any]:
    path, doc = _load_doc(config_path, model)
    tc = _require_test_case(doc, test_uid)
    try:
        removed = remove_assertion(tc, assertion_uid)
    except KeyError:
        raise AssertError(f"assertion_uid '{assertion_uid}' introuvable.")
    _save_doc(path, doc)
    return {"model": model, "test_uid": test_uid, "removed": removed}
