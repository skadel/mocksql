"""Helpers purs de manipulation d'assertions pour la CLI `assert`.

Sous-tendent `mocksql assert list/add/update/remove` : un agent de code pose une
spec (assertion cible), confirme qu'elle est rouge, édite le `.sql` source, puis
rejoue jusqu'au vert. Ciblage par `test_uid` (test) + `assertion_uid` (assertion),
jamais par position — cf. l'invariant d'identité des tests.

Ce module ne fait QUE muter des dicts en mémoire. La lecture/écriture du JSON et la
ré-exécution DuckDB vivent dans la couche commande (main.py / test_runner.py).
"""

from __future__ import annotations

import uuid
from typing import Any


def _short_uid() -> str:
    return uuid.uuid4().hex[:8]


def find_test_case(doc: dict[str, Any], test_uid: str) -> dict[str, Any] | None:
    """Retourne le test_case dont le test_uid correspond, ou None."""
    for tc in doc.get("test_cases", []):
        if tc.get("test_uid") == test_uid:
            return tc
    return None


def ensure_assertion_uids(test_case: dict[str, Any]) -> list[dict[str, Any]]:
    """Rétro-remplit un assertion_uid court sur les assertions qui n'en ont pas.

    Idempotent : une assertion qui a déjà un uid le conserve. Mute et retourne la
    liste des assertions du test_case.
    """
    assertions = test_case.setdefault("assertion_results", [])
    for a in assertions:
        if not a.get("assertion_uid"):
            a["assertion_uid"] = _short_uid()
    return assertions


def add_assertion(
    test_case: dict[str, Any], description: str, sql: str
) -> dict[str, Any]:
    """Ajoute une assertion (dbt-style : le SQL retourne les lignes ÉCHOUANTES).

    `passed` reste None jusqu'à la première ré-exécution par la couche commande.
    """
    assertions = test_case.setdefault("assertion_results", [])
    assertion = {
        "assertion_uid": _short_uid(),
        "description": description,
        "sql": sql,
        "passed": None,
        "failing_rows": [],
    }
    assertions.append(assertion)
    return assertion


def update_assertion(
    test_case: dict[str, Any],
    assertion_uid: str,
    *,
    description: str | None = None,
    sql: str | None = None,
) -> dict[str, Any]:
    """Modifie une assertion ciblée par son uid. Lève KeyError si introuvable."""
    for a in test_case.get("assertion_results", []):
        if a.get("assertion_uid") == assertion_uid:
            if description is not None:
                a["description"] = description
            if sql is not None:
                a["sql"] = sql
            return a
    raise KeyError(assertion_uid)


def remove_assertion(test_case: dict[str, Any], assertion_uid: str) -> dict[str, Any]:
    """Supprime et retourne l'assertion ciblée par son uid. Lève KeyError sinon."""
    assertions = test_case.get("assertion_results", [])
    for i, a in enumerate(assertions):
        if a.get("assertion_uid") == assertion_uid:
            return assertions.pop(i)
    raise KeyError(assertion_uid)
