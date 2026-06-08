"""Tests de la mémoire des tentatives injectée dans le prompt du générateur.

Vérifie que, lors d'une boucle de retry `bad_data` (evaluate → conversational_agent
→ generator), le LLM reçoit un historique few-shot complet : pour chaque itération,
le raisonnement du générateur, les données injectées SANS troncature, le résultat
DuckDB réel, et le diagnostic d'échec.

C'est `_build_eval_context` qui porte cette logique : il relit `state["messages"]`
(où s'accumulent les RESULTS/EVALUATION du tour courant) et apparie chaque message
EVALUATION avec son RESULTS parent via `additional_kwargs["parent"]`.
"""

import json

from langchain_core.messages import AIMessage

from build_query.examples_generator import _build_eval_context
from utils.msg_types import MsgType


# ---------------------------------------------------------------------------
# Helpers — reconstituent les messages tels que l'executor / l'evaluator les émettent
# ---------------------------------------------------------------------------


def _results_msg(test_index, data, msg_id, results_json=None, reasoning="", status="complete", failing_cte=None):
    """Message RESULTS contenant la liste des tests (un seul ici), avec un id stable."""
    test_case = {
        "test_index": test_index,
        "data": data,
        "unit_test_build_reasoning": reasoning,
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
    """Message EVALUATION lié à son RESULTS parent."""
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
# Cas nominal : plusieurs itérations doivent toutes remonter
# ---------------------------------------------------------------------------


def test_accumulates_all_iterations_with_verdicts():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "le filtre amount>100 n'est pas satisfait", "r1"),
            _results_msg(0, {"orders": [{"amount": 50}]}, "r2"),
            _eval_msg(0, "toujours sous le seuil", "r2"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "Itération 1" in ctx
    assert "Itération 2" in ctx
    assert '"amount": 10' in ctx
    assert '"amount": 50' in ctx
    assert "le filtre amount>100 n'est pas satisfait" in ctx
    assert "toujours sous le seuil" in ctx
    assert "NE REPRODUISEZ PAS" in ctx
    assert "Historique des tentatives précédentes" in ctx


def test_iterations_are_ordered():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "échec A", "r1"),
            _results_msg(0, {"orders": [{"amount": 50}]}, "r2"),
            _eval_msg(0, "échec B", "r2"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert ctx.index("Itération 1") < ctx.index("Itération 2")
    assert ctx.index("échec A") < ctx.index("échec B")


# ---------------------------------------------------------------------------
# Few-shot : données complètes, résultat DuckDB, raisonnement du générateur
# ---------------------------------------------------------------------------


def test_includes_full_input_without_truncation():
    """Les données d'entrée doivent apparaître intégralement, sans troncature."""
    big_rows = [{"amount": i, "label": "x" * 20} for i in range(50)]
    state = _state(
        [
            _results_msg(0, {"orders": big_rows}, "r1"),
            _eval_msg(0, "trop de lignes vides", "r1"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "trop de lignes vides" in ctx
    # Toutes les lignes doivent être présentes — pas de marqueur de troncature
    assert "...}" not in ctx
    # Le premier et le dernier enregistrement doivent apparaître
    assert '"amount": 0' in ctx
    assert '"amount": 49' in ctx


def test_includes_actual_duckdb_output():
    """Le résultat DuckDB réel doit apparaître dans le bloc de l'itération."""
    output_rows = [{"total": 42, "category": "A"}]
    state = _state(
        [
            _results_msg(
                0,
                {"orders": [{"amount": 200}]},
                "r1",
                results_json=json.dumps(output_rows),
            ),
            _eval_msg(0, "la colonne category est absente", "r1"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert '"total": 42' in ctx
    assert '"category": "A"' in ctx
    assert "Résultat DuckDB" in ctx


def test_includes_generator_reasoning():
    """Le raisonnement du générateur doit être inclus dans le bloc de l'itération."""
    state = _state(
        [
            _results_msg(
                0,
                {"orders": [{"amount": 10}]},
                "r1",
                reasoning="J'ai choisi amount=10 car le filtre est amount>5, mais le JOIN échoue.",
            ),
            _eval_msg(0, "le JOIN ne produit rien", "r1"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "J'ai choisi amount=10" in ctx
    assert "Raisonnement du générateur" in ctx


def test_empty_results_shows_failing_cte():
    """Pour un status empty_results, le bloc doit mentionner la CTE bloquante."""
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

    ctx = _build_eval_context(state, _EXISTING)

    assert "filtered_orders" in ctx
    assert "CTE bloquante" in ctx
    assert "0 lignes" in ctx


def test_verdict_label_bon_is_preserved():
    """Un verdict Bon doit apparaître tel quel sans être remplacé par Insuffisant."""
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "les données sont correctes", "r1", verdict="Bon"),
            _results_msg(0, {"orders": [{"amount": 20}]}, "r2"),
            _eval_msg(0, "toujours insuffisant", "r2", verdict="Insuffisant"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "Verdict : Bon" in ctx
    assert "Verdict : Insuffisant" in ctx


# ---------------------------------------------------------------------------
# Garde-fous : pas de bruit hors du cas bad_data
# ---------------------------------------------------------------------------


def test_empty_when_feedback_is_not_bad_data():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "raison", "r1"),
        ],
        feedback="bad_assertions",
    )
    assert _build_eval_context(state, _EXISTING) == ""


def test_empty_when_no_evaluation_messages():
    state = _state([_results_msg(0, {"orders": [{"amount": 10}]}, "r1")])
    assert _build_eval_context(state, _EXISTING) == ""


# ---------------------------------------------------------------------------
# Ciblage : seules les itérations du test en cours de régénération remontent
# ---------------------------------------------------------------------------


def test_filters_out_other_test_indices():
    # target = test 1 (dernière EVALUATION). Les tentatives du test 0 ne doivent pas fuiter.
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r0"),
            _eval_msg(0, "echec du test zero", "r0"),
            _results_msg(1, {"orders": [{"amount": 999}]}, "r1"),
            _eval_msg(1, "echec du test un", "r1"),
        ]
    )

    ctx = _build_eval_context(state, [{"test_index": 0}, {"test_index": 1}])

    assert "echec du test un" in ctx
    assert "echec du test zero" not in ctx
    assert '"amount": 999' in ctx
    assert '"amount": 10' not in ctx
    assert "Itération 1" in ctx
    assert "Itération 2" not in ctx


# ---------------------------------------------------------------------------
# Robustesse : lien parent cassé → l'itération est ignorée silencieusement
# ---------------------------------------------------------------------------


def test_orphan_evaluation_without_parent_results_is_skipped():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "échec relié", "r1"),
            _eval_msg(0, "échec orphelin", "parent-inexistant"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "échec relié" in ctx
    assert "échec orphelin" not in ctx


def test_returns_empty_when_all_orphans():
    """Si toutes les évaluations sont orphelines, retourner chaîne vide."""
    state = _state(
        [
            _eval_msg(0, "orphelin A", "inexistant-1"),
            _eval_msg(0, "orphelin B", "inexistant-2"),
        ]
    )

    assert _build_eval_context(state, _EXISTING) == ""
