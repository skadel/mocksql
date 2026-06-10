"""Régression : correct_assertions ne doit pas planter à la sérialisation quand
les failing_rows (issues de DuckDB) contiennent des pandas.Timestamp, et doit
remonter une erreur visible plutôt que crasher le stream."""

import json
from unittest.mock import patch

import pandas as pd
from langchain_core.messages import AIMessage

from build_query.assertion_corrector import (
    _Assertion,
    _ImprovedAssertions,
    correct_assertions,
)
from utils.msg_types import MsgType

# results_json avec une colonne datetime → DuckDB renvoie des Timestamp dans failing_rows
_RESULTS_JSON = json.dumps([{"id": 1, "ts": "2026-01-01T00:00:00"}])


def _state_with_results():
    test = {
        "test_index": "1",
        "unit_test_description": "Montants positifs",
        "data": {},
        "results_json": _RESULTS_JSON,
        "assertion_results": [],
        "correction_attempts": [],
    }
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS, "parent": "p1"},
    )
    return {
        "session": "sess-1",
        "test_index": "1",
        "query": "SELECT 1",
        "optimized_sql": "",
        "messages": [results_msg],
    }


# Une assertion échouée dont les failing_rows portent un Timestamp non sérialisable
# en JSON par défaut — exactement ce que DuckDB.to_dict(orient="records") produit.
_TS_ASSERTION_RESULTS = [
    {
        "description": "ts cohérent",
        "expected_condition": "id > 0",
        "sql": "SELECT * FROM __result__ WHERE NOT (id > 0)",
        "passed": False,
        "failing_rows": [{"id": 1, "ts": pd.Timestamp("2026-01-01")}],
    }
]


async def test_correct_assertions_serializes_timestamps():
    improved = _ImprovedAssertions(
        reasoning="r",
        assertions=[_Assertion(description="ts cohérent", expected_condition="id > 0")],
        verdict="Bon",
        explanation="ok",
    )

    async def _fake_improved(*args, **kwargs):
        return improved

    async def _fake_eval(*args, **kwargs):
        return _TS_ASSERTION_RESULTS

    async def _fake_fix(results, *args, **kwargs):
        return results

    with (
        patch(
            "build_query.assertion_corrector._generate_improved_assertions",
            side_effect=_fake_improved,
        ),
        patch(
            "build_query.examples_executor._evaluate_assertions_with_retry",
            side_effect=_fake_eval,
        ),
        patch(
            "build_query.examples_executor._fix_logically_failing_assertions",
            side_effect=_fake_fix,
        ),
    ):
        out = await correct_assertions(_state_with_results())

    # Ne doit PAS planter, et le message émis doit être du JSON valide (Timestamps sérialisés).
    assert out["messages"], "correct_assertions doit émettre un message"
    msg = out["messages"][0]
    # Si le message est une erreur, le test échoue : la sérialisation aurait dû réussir.
    assert msg.additional_kwargs.get("type") == MsgType.RESULTS
    parsed = json.loads(msg.content)  # lève si le contenu n'est pas du JSON valide
    assert parsed[0]["assertion_results"][0]["failing_rows"][0]["ts"]


async def test_correct_assertions_surfaces_error_instead_of_crashing():
    """Un souci sur une assertion → message ERROR visible + état error, pas de crash."""

    async def _boom(*args, **kwargs):
        raise RuntimeError("boom assertion")

    with patch(
        "build_query.assertion_corrector._generate_improved_assertions",
        side_effect=_boom,
    ):
        out = await correct_assertions(_state_with_results())

    msg = out["messages"][0]
    assert msg.additional_kwargs.get("type") == MsgType.ERROR
    assert "boom assertion" in msg.content
    assert out["error"] == "assertion_correction_failed"
    # gen_retries forcé à 0 → la boucle de correction s'arrête (même si limite pas atteinte)
    assert out["gen_retries"] == 0
