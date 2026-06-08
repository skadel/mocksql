"""Tests du few-shot d'historique injecté dans le prompt du générateur.

Vérifie que, lors d'une boucle de retry `bad_data`, `_build_eval_messages` retourne
des paires [AIMessage(tentative), HumanMessage(feedback)] — une paire par itération.

Le LLM reçoit ainsi l'historique en format natif human/ai :
  - AIMessage  : le JSON exact qu'il a généré (unit_test_build_reasoning + data)
  - HumanMessage : le résultat DuckDB réel + le verdict
"""

import json

from langchain_core.messages import AIMessage, HumanMessage

from build_query.examples_generator import _build_eval_messages
from utils.msg_types import MsgType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _results_msg(
    test_index,
    data,
    msg_id,
    results_json=None,
    reasoning="",
    status="complete",
    failing_cte=None,
):
    test_case = {
        "test_index": test_index,
        "test_name": "test",
        "unit_test_description": "desc",
        "unit_test_build_reasoning": reasoning,
        "tags": [],
        "data": data,
        "status": status,
        "results_json": results_json if results_json is not None else "[]",
    }
    if failing_cte:
        test_case["failing_cte"] = failing_cte
    return AIMessage(
        content=json.dumps([test_case]),
        id=msg_id,
        additional_kwargs={"type": MsgType.RESULTS},
    )


def _eval_msg(test_index, verdict_text, parent_id, verdict="Insuffisant"):
    return AIMessage(
        content=f"**{verdict}** — {verdict_text}",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": test_index,
            "parent": parent_id,
        },
    )


def _state(messages, feedback="bad_data"):
    return {"evaluation_feedback": feedback, "messages": messages}


_EXISTING = [{"test_index": 0}]


# ---------------------------------------------------------------------------
# Format : retourne des paires [AIMessage, HumanMessage]
# ---------------------------------------------------------------------------


def test_returns_list_of_messages():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "le filtre amount>100 n'est pas satisfait", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)

    assert len(msgs) == 2
    assert isinstance(msgs[0], AIMessage)
    assert isinstance(msgs[1], HumanMessage)


def test_ai_message_contains_generated_data():
    """L'AIMessage doit reproduire le JSON que le générateur a produit."""
    data = {"orders": [{"amount": 10, "status": "open"}]}
    reasoning = "J'ai choisi amount=10 car le filtre est amount>5."
    state = _state(
        [
            _results_msg(0, data, "r1", reasoning=reasoning),
            _eval_msg(0, "la colonne status est filtrée", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)
    ai_content = json.loads(msgs[0].content)

    assert ai_content["data"] == data
    assert ai_content["unit_test_build_reasoning"] == reasoning
    assert "test_name" in ai_content
    assert "tags" in ai_content


def test_human_message_contains_duckdb_output():
    """L'HumanMessage doit contenir le résultat DuckDB réel."""
    output = [{"total": 42}]
    state = _state(
        [
            _results_msg(
                0,
                {"orders": [{"amount": 200}]},
                "r1",
                results_json=json.dumps(output),
            ),
            _eval_msg(0, "le total devrait être 100", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)
    feedback = msgs[1].content

    assert '"total": 42' in feedback
    assert "Résultat DuckDB" in feedback


def test_human_message_contains_verdict():
    """L'HumanMessage doit contenir le verdict et le diagnostic."""
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "le filtre amount>100 n'est pas satisfait", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)
    feedback = msgs[1].content

    assert "Insuffisant" in feedback
    assert "le filtre amount>100 n'est pas satisfait" in feedback
    assert "Génère une nouvelle version" in feedback


def test_multiple_iterations_produce_multiple_pairs():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "échec A", "r1"),
            _results_msg(0, {"orders": [{"amount": 50}]}, "r2"),
            _eval_msg(0, "échec B", "r2"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)

    # 2 iterations → 4 messages : AI, Human, AI, Human
    assert len(msgs) == 4
    assert isinstance(msgs[0], AIMessage)
    assert isinstance(msgs[1], HumanMessage)
    assert isinstance(msgs[2], AIMessage)
    assert isinstance(msgs[3], HumanMessage)

    # Les données sont dans le bon ordre
    assert '"amount": 10' in msgs[0].content
    assert '"amount": 50' in msgs[2].content
    assert "échec A" in msgs[1].content
    assert "échec B" in msgs[3].content


# ---------------------------------------------------------------------------
# Données complètes — pas de troncature
# ---------------------------------------------------------------------------


def test_full_input_data_without_truncation():
    big_rows = [{"amount": i, "label": "x" * 20} for i in range(50)]
    state = _state(
        [
            _results_msg(0, {"orders": big_rows}, "r1"),
            _eval_msg(0, "trop de lignes vides", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)
    ai_content = json.loads(msgs[0].content)

    # Toutes les 50 lignes présentes, aucune troncature
    assert len(ai_content["data"]["orders"]) == 50
    assert ai_content["data"]["orders"][0]["amount"] == 0
    assert ai_content["data"]["orders"][49]["amount"] == 49


def test_empty_results_shows_failing_cte():
    state = _state(
        [
            _results_msg(
                0,
                {"orders": [{"amount": 10}]},
                "r1",
                status="empty_results",
                failing_cte="filtered_orders",
            ),
            _eval_msg(0, "aucune ligne après filtrage", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)
    feedback = msgs[1].content

    assert "filtered_orders" in feedback
    assert "CTE bloquante" in feedback
    assert "0 lignes" in feedback


def test_verdict_bon_preserved():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "données correctes", "r1", verdict="Bon"),
            _results_msg(0, {"orders": [{"amount": 20}]}, "r2"),
            _eval_msg(0, "insuffisant", "r2", verdict="Insuffisant"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)

    assert "Bon" in msgs[1].content
    assert "Insuffisant" in msgs[3].content


# ---------------------------------------------------------------------------
# Garde-fous
# ---------------------------------------------------------------------------


def test_returns_empty_when_not_bad_data():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "raison", "r1"),
        ],
        feedback="bad_assertions",
    )
    assert _build_eval_messages(state, _EXISTING) == []


def test_returns_empty_when_no_eval_messages():
    state = _state([_results_msg(0, {"orders": [{"amount": 10}]}, "r1")])
    assert _build_eval_messages(state, _EXISTING) == []


def test_filters_other_test_indices():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r0"),
            _eval_msg(0, "echec test zero", "r0"),
            _results_msg(1, {"orders": [{"amount": 999}]}, "r1"),
            _eval_msg(1, "echec test un", "r1"),
        ]
    )

    msgs = _build_eval_messages(state, [{"test_index": 0}, {"test_index": 1}])

    # Seul le test_index 1 (dernière EVALUATION) est inclus → 1 paire
    assert len(msgs) == 2
    ai_content = json.loads(msgs[0].content)
    assert ai_content["data"]["orders"][0]["amount"] == 999
    assert "echec test un" in msgs[1].content
    assert "echec test zero" not in msgs[1].content


def test_orphan_evaluation_is_skipped():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "échec relié", "r1"),
            _eval_msg(0, "orphelin", "parent-inexistant"),
        ]
    )

    msgs = _build_eval_messages(state, _EXISTING)

    # Seulement la paire pour l'évaluation reliée
    assert len(msgs) == 2
    assert "échec relié" in msgs[1].content


def test_all_orphans_returns_empty():
    state = _state(
        [
            _eval_msg(0, "orphelin A", "inexistant-1"),
            _eval_msg(0, "orphelin B", "inexistant-2"),
        ]
    )
    assert _build_eval_messages(state, _EXISTING) == []
