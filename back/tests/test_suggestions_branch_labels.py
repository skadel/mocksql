"""Nommage des branches UNION ALL (cf. docs/spec-suggestions-robustesse.md) :

- W4 — nommage une seule fois : ``_label_branches`` (appel LLM) ne doit tourner que pour les
  branches encore sans label. Toutes labellisées (labels rechargés dans ``path_plans`` tant
  que le SQL ne change pas) → zéro appel LLM.
- W5 — labels distincts : les branches d'un UNION ALL partagent un long tronc CTE commun
  (> 2500 car) et ne diffèrent que par leur SELECT final (``'<indicator>' AS indicator``).
  Tronquer en tête masquait ce discriminant → labels identiques. Le labeleur doit recevoir le
  SELECT discriminant ; un filet déterministe garantit des labels distincts en dernier ressort.
"""

import json

import pytest
from langchain_core.runnables import RunnableLambda

from build_query import suggestions_node
from build_query.suggestions_node import (
    _BranchLabelsOutput,
    TestSuggestionsOutput as SuggestionsOutput,  # alias : évite la collecte pytest du nom Test*
)


# --- Constructeurs de scénarios ------------------------------------------------------------

# Préfixe CTE commun volontairement > 2500 car : reproduit le cas réel où l'ancienne
# troncature en tête masquait le SELECT final discriminant.
_COMMON_PREFIX = (
    "WITH base AS (\n  SELECT "
    + ", ".join(f"col_{i} AS col_{i}" for i in range(160))
    + " FROM warehouse.accounts\n)"
)
assert len(_COMMON_PREFIX) > 2500  # garde-fou : le scénario perd son sens sinon


def _union_plans(indicators: list[str]) -> dict:
    """``path_plans`` d'un UNION ALL dont les branches partagent ``_COMMON_PREFIX`` et ne
    diffèrent que par le littéral ``'<indicator>' AS indicator`` de leur SELECT final."""
    plans: dict = {}
    for i, ind in enumerate(indicators):
        sliced = f"{_COMMON_PREFIX}\nSELECT '{ind}' AS indicator, account_id FROM base"
        plans[f"b{i}"] = {
            "sliced_sql": sliced,
            "used_columns": [],
            "branch_index": i,
            "host_cte": "final_query",
        }
    plans["all"] = {
        "sliced_sql": _COMMON_PREFIX,
        "used_columns": [],
        "branch_index": None,
        "host_cte": None,
    }
    return plans


def _install_main_llm(monkeypatch, *, suggestions=None):
    """Mock du LLM principal de ``generate_suggestions`` (les 3 suggestions du panneau)."""

    class _FakeLLM:
        def with_structured_output(self, _model):
            return RunnableLambda(
                lambda _pv: SuggestionsOutput(
                    analyse_des_manques="ok", suggestions=suggestions or []
                )
            )

    monkeypatch.setattr(suggestions_node, "make_llm", lambda *a, **k: _FakeLLM())
    monkeypatch.setattr(suggestions_node, "get_test", lambda *a, **k: {})
    monkeypatch.setattr(suggestions_node, "update_test", lambda *a, **k: None)
    monkeypatch.setattr(suggestions_node, "is_native_thinking_active", lambda: True)
    monkeypatch.setattr(
        "utils.saver.persist_completed_tests", lambda *a, **k: 0, raising=False
    )

    async def _fake_retrieve(session, state):
        return [
            {
                "test_index": 0,
                "test_name": "t",
                "unit_test_description": "desc",
                "status": "pass",
                "results_json": '[{"x": 1}]',
                "data": {"t": [{"x": 1}]},
            }
        ]

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _fake_retrieve)


def _state(plans: dict) -> dict:
    return {
        "session": "s1",
        "query": "SELECT 1",
        "messages": [],
        "agent_tool_args": {},
        "path_plans": json.dumps(plans),
        "query_decomposed": json.dumps(
            [
                {
                    "name": "final_query",
                    "code": "SELECT 1",
                    "dependencies": [],
                    "sources": [],
                }
            ]
        ),
    }


# --- W4 : nommage une seule fois -----------------------------------------------------------


@pytest.mark.asyncio
async def test_label_branches_skipped_when_all_branches_cached(monkeypatch):
    """Toutes les branches non couvertes ont déjà un label (cache ``path_plans``) →
    ``_label_branches`` (appel LLM) n'est pas appelé."""
    from unittest.mock import AsyncMock

    _install_main_llm(monkeypatch)
    spy = AsyncMock(return_value={})
    monkeypatch.setattr(suggestions_node, "_label_branches", spy)

    plans = _union_plans(["nb_ope", "ouvertures"])
    plans["b0"]["label"] = "les opérations"
    plans["b1"]["label"] = "les ouvertures de compte"

    await suggestions_node.generate_suggestions(_state(plans))

    spy.assert_not_called()


@pytest.mark.asyncio
async def test_label_branches_called_only_for_unlabeled(monkeypatch):
    """Une branche labellisée, une sans label → le LLM n'est appelé que pour la branche
    encore sans label."""
    from unittest.mock import AsyncMock

    _install_main_llm(monkeypatch)
    spy = AsyncMock(return_value={"b1": "les ouvertures de compte"})
    monkeypatch.setattr(suggestions_node, "_label_branches", spy)

    plans = _union_plans(["nb_ope", "ouvertures"])
    plans["b0"]["label"] = "les opérations"  # b1 sans label

    await suggestions_node.generate_suggestions(_state(plans))

    spy.assert_called_once()
    assert spy.call_args.args[0] == ["b1"]


# --- W5 : labels distincts -----------------------------------------------------------------


@pytest.mark.asyncio
async def test_label_branches_sends_discriminant_not_common_prefix(monkeypatch):
    """Le labeleur reçoit le SELECT final discriminant de chaque branche (avec son littéral
    ``indicator``), pas seulement le tronc CTE commun tronqué en tête."""
    captured: dict = {}

    def _capture(prompt_value):
        captured["branches"] = prompt_value.to_messages()[1].content
        return _BranchLabelsOutput(labels=[])

    class _FakeLLM:
        def with_structured_output(self, _model):
            return RunnableLambda(_capture)

    monkeypatch.setattr(suggestions_node, "make_llm", lambda *a, **k: _FakeLLM())

    indicators = [
        "nb_ope",
        "ouvertures",
        "fermetures",
        "encours",
        "incidents",
        "virements",
    ]
    plans = _union_plans(indicators)
    uncovered = [f"b{i}" for i in range(len(indicators))]

    await suggestions_node._label_branches(uncovered, plans, "bigquery")

    blob = captured["branches"]
    for ind in indicators:
        assert ind in blob, f"discriminant {ind!r} absent du prompt de nommage"


def test_disambiguate_branch_labels_yields_distinct_labels():
    """Filet déterministe : des labels LLM identiques sont désambiguïsés via le littéral
    ``indicator`` de chaque branche → labels finaux tous distincts."""
    plans = _union_plans(["nb_ope", "ouvertures", "fermetures"])
    collided = {
        "b0": "les anomalies du nombre d'opérations",
        "b1": "les anomalies du nombre d'opérations",
        "b2": "les anomalies du nombre d'opérations",
    }
    out = suggestions_node._disambiguate_branch_labels(collided, plans, "bigquery")

    assert len(set(out.values())) == 3
    assert "nb_ope" in out["b0"]
    assert "ouvertures" in out["b1"]
    assert "fermetures" in out["b2"]


def test_disambiguate_branch_labels_is_idempotent_on_distinct_input():
    """Des labels déjà distincts ne sont pas modifiés (pas de churn entre deux générations)."""
    plans = _union_plans(["nb_ope", "ouvertures"])
    distinct = {"b0": "les opérations", "b1": "les ouvertures"}
    out = suggestions_node._disambiguate_branch_labels(distinct, plans, "bigquery")
    assert out == distinct


@pytest.mark.asyncio
async def test_generate_suggestions_persists_distinct_labels_on_collision(monkeypatch):
    """e2e : si le LLM rend 6 labels identiques sur 6 branches, les labels persistés dans
    ``path_plans`` sont tous distincts (validation W5)."""
    from unittest.mock import AsyncMock

    _install_main_llm(monkeypatch)

    indicators = [
        "nb_ope",
        "ouvertures",
        "fermetures",
        "encours",
        "incidents",
        "virements",
    ]
    uncovered = [f"b{i}" for i in range(len(indicators))]
    # Le LLM rend le MÊME label pour toutes les branches.
    monkeypatch.setattr(
        suggestions_node,
        "_label_branches",
        AsyncMock(return_value={n: "les anomalies" for n in uncovered}),
    )

    persisted: dict = {}

    def _capture_update(_session, payload):
        if "path_plans" in payload:
            persisted["path_plans"] = json.loads(payload["path_plans"])

    monkeypatch.setattr(suggestions_node, "update_test", _capture_update)

    plans = _union_plans(indicators)
    await suggestions_node.generate_suggestions(_state(plans))

    labels = [persisted["path_plans"][n]["label"] for n in uncovered]
    assert len(set(labels)) == len(indicators), labels
