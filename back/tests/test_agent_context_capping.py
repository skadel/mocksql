"""Plafonnement du contexte du conversational_agent (incident c6_v3_2).

Le message RESULTS embarque la sortie COMPLÈTE de chaque test (``results_json``) + le
``result_json`` de CHAQUE CTE (``step_by_step_results``) + les ``failing_rows`` des
assertions ; ``_build_agent_eval_context`` en ré-injecte une copie. Sur c6 (1344 lignes ×
~45 colonnes + colonnes ARRAY de 14 éléments), un seul prompt de correction ``bad_data``
dépassait la limite modèle (400 INVALID_ARGUMENT, ~1,23M tokens).

Garde : rendu compacté À LA FRONTIÈRE AGENT uniquement (le message original reste intact
dans le state — le front continue de recevoir le résultat complet) : échantillon par
LIGNES ENTIÈRES (jamais de troncature caractère), colonnes ARRAY tronquées avec marqueur,
``sql_code`` des CTE omis (l'agent a déjà la requête), ``failing_rows`` plafonnées.
"""

import json

from langchain_core.messages import AIMessage

from build_query.conversational_agent import (
    _build_agent_eval_context,
    _compact_results_message,
    _truncate_long_arrays,
)
from utils.msg_types import MsgType

# ---------------------------------------------------------------------------
# Fabriques : un résultat c6-like (lignes nombreuses + colonnes ARRAY)
# ---------------------------------------------------------------------------

_ARRAY_14 = list(range(100, 114))  # 100..113 — 113 ne doit JAMAIS survivre au cap


def _rows(n):
    return [{"id": f"ROW{i}", "valeur": i, "lags": list(_ARRAY_14)} for i in range(n)]


def _test_result(n_rows=50):
    return {
        "test_index": 0,
        "unit_test_description": "desc",
        "status": "complete",
        "test_data": {"orders": [{"amount": 10}]},
        "results_json": json.dumps(_rows(n_rows)),
        "step_by_step_results": [
            {
                "cte_name": "MonthlyData",
                "sql_code": "SELECT tres_long_sql_duplique FROM datamart",
                "row_count": 40,
                "result_json": json.dumps(_rows(40)),
            },
            {
                "cte_name": "res",
                "sql_code": "SELECT encore_du_sql FROM res",
                "row_count": 40,
                "result_json": json.dumps(_rows(40)),
            },
        ],
        "assertion_results": [
            {
                "description": "d",
                "expected_condition": "valeur = 1",
                "sql": "SELECT * FROM __result__ WHERE (valeur = 1) IS NOT TRUE",
                "passed": False,
                "failing_rows": _rows(30),
            }
        ],
    }


def _results_msg(test_result, msg_id="r1"):
    return AIMessage(
        content=json.dumps([test_result]),
        id=msg_id,
        additional_kwargs={"type": MsgType.RESULTS},
    )


# ---------------------------------------------------------------------------
# _truncate_long_arrays
# ---------------------------------------------------------------------------


def test_truncate_long_arrays_caps_with_marker():
    out = _truncate_long_arrays({"lags": list(_ARRAY_14), "x": 1})
    assert out["x"] == 1
    assert 113 not in out["lags"]
    assert out["lags"][-1] == "… (+8)"
    assert out["lags"][:3] == [100, 101, 102]


def test_truncate_long_arrays_short_lists_untouched():
    assert _truncate_long_arrays([1, 2, 3]) == [1, 2, 3]


# ---------------------------------------------------------------------------
# _compact_results_message — frontière agent
# ---------------------------------------------------------------------------


def test_compact_results_caps_rows_and_arrays():
    out = _compact_results_message(_results_msg(_test_result()))
    parsed = json.loads(out.content)[0]

    assert "results_json" not in parsed
    assert parsed["results_row_count"] == 50
    sample = parsed["results_sample"]
    assert sample[-1] == "… (+30 autres lignes)"
    assert len(sample) == 21  # 20 lignes + marqueur
    assert sample[0]["id"] == "ROW0"
    assert "ROW49" not in out.content
    assert "113" not in out.content  # arrays tronqués partout


def test_compact_results_drops_cte_sql_and_caps_cte_rows():
    out = _compact_results_message(_results_msg(_test_result()))
    parsed = json.loads(out.content)[0]

    assert "sql_code" not in out.content
    steps = parsed["step_by_step_results"]
    assert [s["cte_name"] for s in steps] == ["MonthlyData", "res"]
    for s in steps:
        assert s["row_count"] == 40
        assert s["sample"][-1] == "… (+37 autres lignes)"
        assert len(s["sample"]) == 4  # 3 lignes + marqueur


def test_compact_results_caps_failing_rows_and_drops_assertion_sql():
    out = _compact_results_message(_results_msg(_test_result()))
    parsed = json.loads(out.content)[0]

    a = parsed["assertion_results"][0]
    assert "sql" not in a
    assert a["expected_condition"] == "valeur = 1"
    assert a["failing_rows"][-1] == "… (+27 autres lignes)"
    assert len(a["failing_rows"]) == 4


def test_compact_results_preserves_identity_and_input_data():
    msg = _results_msg(_test_result())
    out = _compact_results_message(msg)
    parsed = json.loads(out.content)[0]

    assert out.id == msg.id
    assert out.additional_kwargs.get("type") == MsgType.RESULTS
    assert parsed["test_data"] == {"orders": [{"amount": 10}]}
    assert parsed["status"] == "complete"


def test_compact_results_passthrough_non_results_and_unparsable():
    other = AIMessage(content="bonjour", additional_kwargs={"type": MsgType.OTHER})
    assert _compact_results_message(other) is other
    broken = AIMessage(
        content="pas du json", additional_kwargs={"type": MsgType.RESULTS}
    )
    assert _compact_results_message(broken) is broken


# ---------------------------------------------------------------------------
# _build_agent_eval_context — copie « Sortie DuckDB obtenue » plafonnée
# ---------------------------------------------------------------------------


def _eval_state():
    eval_msg = AIMessage(
        content="**Insuffisant** — données incohérentes",
        additional_kwargs={"type": MsgType.EVALUATION, "test_index": 0},
    )
    return {
        "evaluation_feedback": "bad_data",
        "messages": [eval_msg],
        "gen_retries": 2,
    }


def _existing_tests():
    tr = _test_result()
    return [
        {
            "test_index": 0,
            "test_uid": "uid-1",
            "unit_test_description": "desc",
            "data": {"orders": [{"amount": 10}]},
            "results_json": tr["results_json"],
            "assertion_results": tr["assertion_results"],
        }
    ]


def test_eval_context_result_sample_is_capped():
    ctx, _ = _build_agent_eval_context(_eval_state(), _existing_tests())
    assert "50 ligne(s)" in ctx  # la cardinalité réelle reste visible
    assert "ROW0" in ctx
    assert "ROW49" not in ctx
    assert "(+30 autres lignes)" in ctx
    assert "113" not in ctx  # arrays tronqués


def test_eval_context_failing_assertions_capped_without_sql():
    ctx, _ = _build_agent_eval_context(_eval_state(), _existing_tests())
    assert "valeur = 1" in ctx  # expected_condition conservée
    assert "__result__ WHERE" not in ctx  # champ sql omis
    assert "(+27 autres lignes)" in ctx
