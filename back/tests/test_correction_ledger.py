"""
P1c — Garde anti-no-op et mémoire des tentatives (boucle bad_data).

Incident du 2026-06-11 : l'échange PROD1↔PROD2 entre deux lignes (identiques
après SUBSTR) a consommé un round executor+evaluator complet sans pouvoir
changer le résultat, et le round suivant — amnésique — pouvait répéter ou
défaire la tentative. Les deux gardes partagent le même ledger
(``QueryState.correction_attempts``) :
1. un lot de patches qui ne change le multiset d'aucune colonne touchée est
   renvoyé à l'agent sans relancer l'executor ;
2. un lot identique à une tentative passée est rejeté avec le motif
   « déjà tenté au round N » ;
3. chaque round voit les tentatives précédentes en conversation alternée.
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.conversational_agent import (
    _noop_batch_reason,
    _render_attempt_messages,
    conversational_agent,
)
from build_query.data_patcher import append_correction_attempt
from build_query.query_chain import _bad_data_to_agent
from utils.msg_types import MsgType


class FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _patch_batch(*ops):
    """ops: tuples (row_index, field, value)."""
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": "a3f9",
                    "table": "ds_correspondance",
                    "row_index": row,
                    "field": field,
                    "value_json": json.dumps(value),
                },
                "id": f"call_{i}",
            }
            for i, (row, field, value) in enumerate(ops)
        ],
    )


def _state_auto_correct(correction_attempts=None):
    test = {
        "test_uid": "a3f9",
        "test_index": "1",
        "test_name": "Cartes",
        "unit_test_description": "ouverture",
        "data": {
            "ds_correspondance": [
                {"code_produit": "PROD1"},
                {"code_produit": "PROD2"},
            ]
        },
        "status": "empty_results",
        "failing_cte": "temp_carte",
        "results_json": "[]",
        "assertion_results": [],
    }
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS},
    )
    eval_msg = AIMessage(
        content="**Insuffisant** — temp_carte vide",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": "1",
            "parent": "r1",
        },
    )
    state = {
        "session": "sess1",
        "messages": [results_msg, eval_msg],
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "",
        "evaluation_feedback": "bad_data",
        "auto_correct": True,
        "gen_retries": 2,
    }
    if correction_attempts is not None:
        state["correction_attempts"] = correction_attempts
    return state


# ── Garde no-op : échange de valeurs (multiset inchangé) ─────────────────────


@pytest.mark.asyncio
async def test_swap_batch_rejected_then_real_patch_accepted(monkeypatch):
    swap = _patch_batch((0, "code_produit", "PROD2"), (1, "code_produit", "PROD1"))
    real = _patch_batch((0, "code_produit", "'PROD1'"))
    fake = FakeLLM([swap, real])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct())

    # le lot no-op est renvoyé à l'agent avec le motif, sans ré-exécution
    assert len(fake.calls) == 2
    feedback = fake.calls[1][-1]
    assert isinstance(feedback, HumanMessage)
    assert "⛔" in feedback.content and "multiset" in feedback.content
    # le second lot (valeur réellement changée) passe
    assert update["agent_tool_call"] == "data_batch"
    value = update["agent_tool_args"]["calls"][0]["args"]["value_json"]
    assert json.loads(value) == "'PROD1'"


@pytest.mark.asyncio
async def test_exhausted_noop_batches_fall_back_without_action(monkeypatch):
    swap = lambda: _patch_batch(  # noqa: E731
        (0, "code_produit", "PROD2"), (1, "code_produit", "PROD1")
    )
    fake = FakeLLM([swap(), swap(), swap()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct())

    # _NOOP_RETRY_MAX == 2 → 3 invocations max, puis aucun outil actionnable
    # (route_agent_output retombera sur le generator).
    assert len(fake.calls) == 3
    assert update["agent_tool_call"] is None


# ── Garde no-op : lot identique à une tentative passée ───────────────────────


@pytest.mark.asyncio
async def test_batch_identical_to_past_attempt_rejected(monkeypatch):
    past = [
        {
            "round": 1,
            "test_uid": "a3f9",
            "ops": [
                {
                    "tool": "patch_test_field",
                    "table": "ds_correspondance",
                    "row_index": 0,
                    "field": "code_produit",
                    "value_json": json.dumps("ROD"),
                }
            ],
            "outcome": {
                "blocking_cte": "temp_carte",
                "digest": "toujours 0 ligne — étape bloquante inchangée (temp_carte)",
            },
        }
    ]
    same_as_past = _patch_batch((0, "code_produit", "ROD"))
    different = _patch_batch((0, "code_produit", "'PROD1'"))
    fake = FakeLLM([same_as_past, different])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    update = await conversational_agent(_state_auto_correct(correction_attempts=past))

    feedback = fake.calls[1][-1]
    assert "tentative 1" in feedback.content.lower()
    assert update["agent_tool_call"] == "data_batch"


# ── Mémoire des tentatives : rendu en conversation alternée ──────────────────


@pytest.mark.asyncio
async def test_ledger_rendered_before_trigger(monkeypatch):
    past = [
        {
            "round": 1,
            "test_uid": "a3f9",
            "ops": [
                {
                    "tool": "patch_test_field",
                    "table": "ds_ref_porteur",
                    "row_index": 0,
                    "field": "cd_chef_file",
                    "value_json": '"1"',
                }
            ],
            "outcome": {
                "blocking_cte": "temp_carte",
                "digest": "toujours 0 ligne — étape bloquante inchangée (temp_carte)",
            },
        }
    ]
    fake = FakeLLM([_patch_batch((0, "code_produit", "'PROD1'"))])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_state_auto_correct(correction_attempts=past))

    msgs = fake.calls[0]
    ai_attempts = [
        m for m in msgs if isinstance(m, AIMessage) and "Tentative 1" in str(m.content)
    ]
    human_outcomes = [
        m
        for m in msgs
        if isinstance(m, HumanMessage) and "Résultat tentative 1" in str(m.content)
    ]
    assert ai_attempts and human_outcomes
    assert "cd_chef_file" in ai_attempts[0].content
    # l'outcome précède le trigger final (dernier message)
    assert msgs.index(human_outcomes[0]) < len(msgs) - 1


def test_render_attempt_messages_alternates_ai_human():
    msgs = _render_attempt_messages(
        [
            {
                "round": 1,
                "ops": [{"tool": "regen"}],
                "outcome": {"digest": "toujours 0 ligne"},
            },
            {
                "round": 2,
                "ops": [{"tool": "remove_test_row", "table": "t", "row_index": 1}],
                "outcome": None,
            },
        ]
    )
    assert isinstance(msgs[0], AIMessage) and "régénération complète" in msgs[0].content
    assert isinstance(msgs[1], HumanMessage) and "toujours 0 ligne" in msgs[1].content
    assert isinstance(msgs[2], AIMessage) and "remove_test_row t[1]" in msgs[2].content
    assert len(msgs) == 3  # pas d'outcome au round 2 → pas de message HUMAN


# ── _noop_batch_reason : unités ──────────────────────────────────────────────


def _uid_map():
    return {
        "a3f9": {
            "test_uid": "a3f9",
            "data": {"t": [{"f": "A"}, {"f": "B"}]},
        }
    }


def _call(row, value, field="f", tool="patch_test_field"):
    return {
        "tool": tool,
        "args": {
            "test_uid": "a3f9",
            "table": "t",
            "row_index": row,
            "field": field,
            "value_json": json.dumps(value),
        },
    }


def test_noop_reason_for_swap():
    assert (
        _noop_batch_reason([_call(0, "B"), _call(1, "A")], _uid_map(), []) is not None
    )


def test_noop_reason_for_identical_value():
    assert _noop_batch_reason([_call(0, "A")], _uid_map(), []) is not None


def test_no_reason_when_value_changes():
    assert _noop_batch_reason([_call(0, "C")], _uid_map(), []) is None


def test_no_reason_for_add_or_remove():
    add = {"tool": "add_test_row", "args": {"test_uid": "a3f9", "tables": ["t"]}}
    assert _noop_batch_reason([add], _uid_map(), []) is None
    rm = {
        "tool": "remove_test_row",
        "args": {"test_uid": "a3f9", "table": "t", "row_index": 0},
    }
    assert _noop_batch_reason([rm], _uid_map(), []) is None


# ── append_correction_attempt + complétion de l'outcome ──────────────────────


def test_append_correction_attempt_increments_round_and_flattens_ops():
    state = {
        "correction_attempts": [
            {"round": 1, "ops": [{"tool": "regen"}], "outcome": None}
        ]
    }
    ops = [
        {
            "tool": "patch_test_field",
            "args": {
                "test_index": "1",  # détail interne → exclu du ledger
                "table": "t",
                "row_index": 0,
                "field": "f",
                "value_json": '"x"',
            },
        }
    ]
    attempts = append_correction_attempt(state, "a3f9", ops)
    assert len(attempts) == 2
    entry = attempts[-1]
    assert entry["round"] == 2
    assert entry["test_uid"] == "a3f9"
    assert entry["outcome"] is None
    assert entry["ops"] == [
        {
            "tool": "patch_test_field",
            "table": "t",
            "row_index": 0,
            "field": "f",
            "value_json": '"x"',
        }
    ]


@pytest.mark.asyncio
async def test_bad_data_to_agent_completes_last_outcome():
    results_msg = AIMessage(
        content=json.dumps(
            [{"status": "empty_results", "failing_cte": "temp_carte", "cte_trace": {}}]
        ),
        additional_kwargs={"type": MsgType.RESULTS},
    )
    state = {
        "messages": [results_msg],
        "correction_attempts": [
            {
                "round": 1,
                "test_uid": "a3f9",
                "ops": [{"tool": "regen"}],
                "outcome": None,
            }
        ],
    }
    update = await _bad_data_to_agent(state)
    assert update["auto_correct"] is True
    outcome = update["correction_attempts"][-1]["outcome"]
    assert outcome["blocking_cte"] == "temp_carte"
    assert "temp_carte" in outcome["digest"]


# ── Trace d'exécution structuré conservé par tentative (évolution) ───────────


def test_compact_cte_trace_keeps_profile_samples_and_mismatch():
    from build_query.examples_generator import _compact_cte_trace

    trace = {
        "StateCases": {"row_count": 8},
        "TopStates": {"row_count": 4},
        "FourthState": {"row_count": 1, "sample": [{"state_name": "StateD"}]},
        "CountyCases": {
            "row_count": 0,
            "join_breakdown": [
                "state_name : veut 'StateD', présent {StateB} ← BLOQUANT"
            ],
        },
    }
    compact = _compact_cte_trace("CountyCases", trace)
    # profil ordonné de TOUTES les CTE
    names = [e["name"] for e in compact["profile"]]
    assert names == ["StateCases", "TopStates", "FourthState", "CountyCases"]
    # le sample du pivot faible-cardinalité est conservé
    fourth = next(e for e in compact["profile"] if e["name"] == "FourthState")
    assert fourth["sample"] == [{"state_name": "StateD"}]
    # le row_count seul (sans sample) reste nu
    assert "sample" not in compact["profile"][0]
    # le mismatch de la CTE bloquante est remonté
    assert compact["mismatch"] == [
        "state_name : veut 'StateD', présent {StateB} ← BLOQUANT"
    ]


def test_render_attempt_messages_surfaces_trace_evolution():
    """Deux tentatives où la valeur du pivot FourthState BOUGE alors que les états
    n'ont pas changé : le rendu doit exposer les deux valeurs pour que l'agent voie
    la cible mouvante (vide non-déterministe)."""
    attempts = [
        {
            "round": 1,
            "ops": [
                {
                    "tool": "patch_test_field",
                    "table": "us_counties",
                    "row_index": 0,
                    "field": "state_name",
                    "value_json": '"StateB"',
                }
            ],
            "outcome": {
                "digest": "toujours 0 ligne — étape bloquante inchangée (CountyCases)",
                "cte_trace": {
                    "profile": [
                        {"name": "TopStates", "rows": 4},
                        {
                            "name": "FourthState",
                            "rows": 1,
                            "sample": [{"state_name": "StateD"}],
                        },
                        {"name": "CountyCases", "rows": 0},
                    ],
                    "mismatch": [
                        "state_name : veut 'StateD', présent {StateB} ← BLOQUANT"
                    ],
                },
            },
        },
        {
            "round": 2,
            "ops": [
                {
                    "tool": "patch_test_field",
                    "table": "us_counties",
                    "row_index": 0,
                    "field": "state_name",
                    "value_json": '"TargetState"',
                }
            ],
            "outcome": {
                "digest": "toujours 0 ligne — étape bloquante inchangée (CountyCases)",
                "cte_trace": {
                    "profile": [
                        {"name": "TopStates", "rows": 4},
                        {
                            "name": "FourthState",
                            "rows": 1,
                            "sample": [{"state_name": "StateB"}],
                        },
                        {"name": "CountyCases", "rows": 0},
                    ],
                    "mismatch": [
                        "state_name : veut 'StateB', présent {TargetState} ← BLOQUANT"
                    ],
                },
            },
        },
    ]
    msgs = _render_attempt_messages(attempts)
    humans = [m for m in msgs if isinstance(m, HumanMessage)]
    assert len(humans) == 2
    # profil row_count rendu
    assert "FourthState=1" in humans[0].content and "CountyCases=0" in humans[0].content
    # la valeur du pivot DIFFÈRE d'une tentative à l'autre → cible mouvante visible
    assert "StateD" in humans[0].content
    assert "StateB" in humans[1].content
    # mismatch « veut X, présent Y » conservé
    assert "BLOQUANT" in humans[0].content


@pytest.mark.asyncio
async def test_bad_data_to_agent_persists_structured_trace():
    results_msg = AIMessage(
        content=json.dumps(
            [
                {
                    "status": "empty_results",
                    "failing_cte": "CountyCases",
                    "cte_trace": {
                        "FourthState": {
                            "row_count": 1,
                            "sample": [{"state_name": "StateD"}],
                        },
                        "CountyCases": {
                            "row_count": 0,
                            "join_breakdown": [
                                "state_name : veut 'StateD', présent {StateB} ← BLOQUANT"
                            ],
                        },
                    },
                }
            ]
        ),
        additional_kwargs={"type": MsgType.RESULTS},
    )
    state = {
        "messages": [results_msg],
        "correction_attempts": [
            {
                "round": 1,
                "test_uid": "a3f9",
                "ops": [{"tool": "regen"}],
                "outcome": None,
            }
        ],
    }
    update = await _bad_data_to_agent(state)
    outcome = update["correction_attempts"][-1]["outcome"]
    assert "cte_trace" in outcome
    fourth = next(
        e for e in outcome["cte_trace"]["profile"] if e["name"] == "FourthState"
    )
    assert fourth["sample"] == [{"state_name": "StateD"}]
    assert outcome["cte_trace"]["mismatch"]
