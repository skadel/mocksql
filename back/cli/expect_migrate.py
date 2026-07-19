"""mocksql migrate-expect — backfill du contrat ``expect`` sur les tests existants.

Migration spec validation-humaine §5 : pour chaque cas sauvegardé,
``verdict ∈ {Excellent, Bon}`` (et non mort-né) → ``expect`` pré-rempli depuis
``results_json`` (restreint aux colonnes des assertions quand identifiables) +
``review.status = "confirmed"`` avec ``confirmed_by = "verdict-llm-legacy"`` — le
replay CI continue de fonctionner sans intervention, l'UI peut badger distinctement
les tests jamais confirmés par un humain. ``Insuffisant`` / mort-nés → ``draft``.

Idempotent : un cas déjà migré (``review.status`` posé) n'est pas retouché sans
``--overwrite``. Zéro LLM.
"""

from __future__ import annotations

import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from build_query.expect_contract import build_expect
from storage.test_files import is_deadborn_case, read_test_doc, write_test_doc

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)

_SUCCESS_VERDICTS = ("Excellent", "Bon")


def migrate_case(case: Dict[str, Any], sql: str, dialect: str | None) -> str:
    """Migre UN cas en place. Retourne l'issue : ``confirmed`` / ``draft`` /
    ``no_results`` (non exprimable en lignes — pas de sortie observée exploitable)."""
    expect = (
        None
        if is_deadborn_case(case)
        else build_expect(
            case.get("results_json"), case.get("assertion_results"), sql, dialect
        )
    )
    review = case.get("review") if isinstance(case.get("review"), dict) else {}
    if expect is None:
        case["review"] = {**review, "status": "draft"}
        return "no_results"
    case["expect"] = expect
    if case.get("verdict") in _SUCCESS_VERDICTS:
        case["review"] = {
            **review,
            "status": "confirmed",
            "confirmed_by": "verdict-llm-legacy",
            "confirmed_at": datetime.now().isoformat(),
        }
        return "confirmed"
    case["review"] = {**review, "status": "draft"}
    return "draft"


def migrate_expect(
    config_path: Path, dry_run: bool = False, overwrite: bool = False
) -> Dict[str, Any]:
    """Backfill ``expect`` + ``review`` sur tous les tests sous ``.mocksql/tests``.

    Retourne les stats : ``{"models": N, "confirmed": …, "draft": …,
    "no_results": …, "already": …, "details": [{model, outcome_par_cas}]}``.
    """
    import yaml

    cfg: Dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    dialect = cfg.get("dialect")

    tests_root = config_path.parent / ".mocksql" / "tests"
    stats: Dict[str, Any] = {
        "models": 0,
        "confirmed": 0,
        "draft": 0,
        "no_results": 0,
        "already": 0,
        "details": [],
    }
    if not tests_root.exists():
        return stats

    for f in sorted(tests_root.rglob("*.json")):
        if _UUID_RE.match(f.stem):
            continue
        doc = read_test_doc(f)
        if not doc:
            continue
        sql = doc.get("sql") or ""
        outcomes: List[str] = []
        changed = False
        for case in doc.get("test_cases") or []:
            review = case.get("review") if isinstance(case.get("review"), dict) else {}
            if review.get("status") and not overwrite:
                outcomes.append("already")
                stats["already"] += 1
                continue
            outcome = migrate_case(case, sql, dialect)
            outcomes.append(outcome)
            stats[outcome] += 1
            changed = True
        if changed and not dry_run:
            write_test_doc(f, doc)
        stats["models"] += 1
        model_name = f.relative_to(tests_root).with_suffix("").as_posix()
        stats["details"].append({"model": model_name, "outcomes": outcomes})
    return stats
