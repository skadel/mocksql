"""Câblage du prompt de `generate_suggestions` :

- #1 : la directive de raisonnement s'adapte au thinking natif (conclusion brève dans
  `coverage_gap_analysis` si actif ; chain-of-thought in-schema sinon). Plus aucune
  demande de CoT *dans le JSON* quand le thinking natif porte déjà le raisonnement.
- #6 : la consigne « contextualisée » ne prétend s'appuyer sur les résultats d'exécution
  que si au moins un test a réellement retourné des lignes ; sinon elle recentre sur la
  structure du SQL.

Le test capture les messages réellement formatés (placeholders résolus) — ce qui valide
aussi qu'aucun placeholder n'est manquant/orphelin (#6 a introduit `results_grounding`).
"""

import json

import pytest
from langchain_core.runnables import RunnableLambda

from build_query import suggestions_node
from build_query.suggestions_node import (
    TestSuggestionsOutput as SuggestionsOutput,  # alias : évite la collecte pytest du nom Test*
)


def _install_capture(monkeypatch, captured: dict):
    """Mock make_llm pour capturer le prompt formaté au lieu d'appeler un vrai LLM."""

    def _capture(prompt_value):
        captured["system"] = prompt_value.to_messages()[0].content
        captured["user"] = prompt_value.to_messages()[1].content
        return SuggestionsOutput(coverage_gap_analysis="ok", suggestions=[])

    class _FakeLLM:
        def with_structured_output(self, _model):
            return RunnableLambda(_capture)

    monkeypatch.setattr(suggestions_node, "make_llm", lambda *a, **k: _FakeLLM())
    monkeypatch.setattr(suggestions_node, "get_test", lambda *a, **k: {})
    monkeypatch.setattr(suggestions_node, "update_test", lambda *a, **k: None)


def _test_case(*, with_result: bool) -> dict:
    return {
        "test_index": 0,
        "test_name": "t",
        "unit_test_description": "desc",
        "status": "pass" if with_result else "empty_results",
        "results_json": '[{"x": 1}]' if with_result else "[]",
        "data": {"t": [{"x": 1}]},
    }


async def _run(
    monkeypatch, *, native: bool, with_result: bool, instructions: str | None = None
) -> dict:
    captured: dict = {}
    _install_capture(monkeypatch, captured)
    monkeypatch.setattr(suggestions_node, "is_native_thinking_active", lambda: native)

    async def fake_retrieve(session, state):
        return [_test_case(with_result=with_result)]

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", fake_retrieve)

    state = {
        "session": "s1",
        "query": "SELECT region, SUM(x) FROM t GROUP BY region",
        "messages": [],
        "agent_tool_args": {"instructions": instructions} if instructions else {},
        "query_decomposed": json.dumps(
            [
                {
                    "name": "agg",
                    "code": "SELECT region, SUM(x) AS s FROM t GROUP BY region",
                    "dependencies": [],
                    "sources": [{"table": "t"}],
                },
                {
                    "name": "final_query",
                    "code": "SELECT region, s FROM agg WHERE s > 0",
                    "dependencies": ["agg"],
                    "sources": [],
                },
            ]
        ),
    }
    await suggestions_node.generate_suggestions(state)
    return captured


@pytest.mark.asyncio
async def test_native_thinking_directive_keeps_cot_out_of_json(monkeypatch):
    captured = await _run(monkeypatch, native=True, with_result=True)
    system = captured["system"]
    # Le raisonnement se fait dans le canal de réflexion ; conclusion brève dans le champ.
    assert "canal de réflexion" in system
    assert "que la conclusion" in system
    # Plus de demande explicite de chain-of-thought *dans le JSON*.
    assert "chain-of-thought directement dans" not in system


@pytest.mark.asyncio
async def test_non_native_directive_requests_in_schema_cot(monkeypatch):
    captured = await _run(monkeypatch, native=False, with_result=True)
    system = captured["system"]
    assert "chain-of-thought directement dans `coverage_gap_analysis`" in system


@pytest.mark.asyncio
async def test_results_grounding_claims_results_when_a_test_succeeded(monkeypatch):
    captured = await _run(monkeypatch, native=True, with_result=True)
    user = captured["user"]
    assert "résultats d'exécution réels" in user
    assert "ne prétends pas t'appuyer sur des résultats" not in user


@pytest.mark.asyncio
async def test_results_grounding_falls_back_when_no_test_succeeded(monkeypatch):
    captured = await _run(monkeypatch, native=True, with_result=False)
    user = captured["user"]
    assert "ne prétends pas t'appuyer sur des résultats" in user


@pytest.mark.asyncio
async def test_instructions_specifiques_injected_when_present(monkeypatch):
    """Régression : les consignes spécifiques (agent_tool_args.instructions) doivent être
    réellement injectées dans le prompt — pas un littéral `{}` orphelin. Cf. bug où
    `instruction_block` portait un `{}` non-interpolé : les instructions étaient perdues."""
    instructions = "Concentre-toi sur la fenêtre glissante de winsorization"
    captured = await _run(
        monkeypatch, native=True, with_result=True, instructions=instructions
    )
    user = captured["user"]
    assert "<instructions_specifiques>" in user
    assert instructions in user
    # Le placeholder littéral ne doit jamais survivre au rendu.
    assert "<instructions_specifiques>\n{}\n</instructions_specifiques>" not in user


@pytest.mark.asyncio
async def test_instructions_specifiques_absent_when_empty(monkeypatch):
    """Sans consigne spécifique, aucun bloc <instructions_specifiques> ne doit apparaître."""
    captured = await _run(monkeypatch, native=True, with_result=True, instructions=None)
    assert "<instructions_specifiques>" not in captured["user"]


@pytest.mark.asyncio
async def test_sql_digest_injected_in_prompt(monkeypatch):
    """#5 — la pré-digestion structurelle (pipeline de CTEs) est injectée à côté du SQL."""
    captured = await _run(monkeypatch, native=True, with_result=True)
    user = captured["user"]
    assert "Structure de la requête" in user
    assert "`agg`" in user and "`final_query`" in user
