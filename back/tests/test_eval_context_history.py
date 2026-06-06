"""Tests de la mémoire des tentatives échouées injectée dans le prompt du générateur.

Vérifie que, lors d'une boucle de retry `bad_data` (evaluate → conversational_agent
→ generator), le LLM reçoit bien l'historique cumulé des exécutions passées AVEC
leur verdict : chaque itération échouée doit apparaître avec les données essayées
et la raison de l'échec, accompagnée de la consigne anti-répétition.

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


def _results_msg(test_index, data, msg_id):
    """Message RESULTS contenant la liste des tests (un seul ici), avec un id stable."""
    return AIMessage(
        content=json.dumps([{"test_index": test_index, "data": data}]),
        id=msg_id,
        additional_kwargs={"type": MsgType.RESULTS},
    )


def _eval_msg(test_index, verdict_text, parent_id):
    """Message EVALUATION 'Insuffisant' lié à son RESULTS parent."""
    return AIMessage(
        content=f"**Insuffisant** — {verdict_text}",
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
# Cas nominal : plusieurs itérations échouées doivent toutes remonter
# ---------------------------------------------------------------------------


def test_accumulates_all_failed_iterations_with_verdicts():
    state = _state(
        [
            _results_msg(0, {"orders": [{"amount": 10}]}, "r1"),
            _eval_msg(0, "le filtre amount>100 n'est pas satisfait", "r1"),
            _results_msg(0, {"orders": [{"amount": 50}]}, "r2"),
            _eval_msg(0, "toujours sous le seuil", "r2"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    # Les deux échecs sont numérotés et présents
    assert "Itération 1" in ctx
    assert "Itération 2" in ctx
    # Les données essayées à chaque tour
    assert '"amount": 10' in ctx
    assert '"amount": 50' in ctx
    # Le verdict de chaque itération (sans le préfixe "**Insuffisant** — ")
    assert "le filtre amount>100 n'est pas satisfait" in ctx
    assert "toujours sous le seuil" in ctx
    assert "**Insuffisant**" not in ctx  # le préfixe est bien retiré
    # La consigne anti-répétition encadre le bloc
    assert "NE REPRODUISEZ PAS" in ctx
    assert "Historique des tentatives échouées" in ctx


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
    # target = test 1 (dernière EVALUATION). Les échecs du test 0 ne doivent pas fuiter.
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
    # Une seule itération conservée → numérotation repart à 1
    assert "Itération 1" in ctx
    assert "Itération 2" not in ctx


# ---------------------------------------------------------------------------
# Robustesse : lien parent cassé → l'itération est ignorée silencieusement
# (documente le comportement actuel, cf. except Exception: pass)
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

    # L'itération reliée remonte, l'orpheline est ignorée (pas de RESULTS parent)
    assert "échec relié" in ctx
    assert "échec orphelin" not in ctx


# ---------------------------------------------------------------------------
# Troncature : les grosses données d'entrée sont coupées à 200 caractères
# ---------------------------------------------------------------------------


def test_large_input_data_is_truncated():
    big_rows = [{"amount": i, "label": "x" * 20} for i in range(50)]
    state = _state(
        [
            _results_msg(0, {"orders": big_rows}, "r1"),
            _eval_msg(0, "trop de lignes", "r1"),
        ]
    )

    ctx = _build_eval_context(state, _EXISTING)

    assert "trop de lignes" in ctx
    assert "...}" in ctx  # marqueur de troncature
