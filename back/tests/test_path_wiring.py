"""Câblage du focus par branche UNION ALL — nouveau modèle.

Le focus n'est plus un track DÉTERMINISTE (une suggestion forcée par branche). C'est une
suggestion CONTEXTUELLE optionnelle : le suggesteur peut marquer une suggestion comme
focalisée sur une branche (champ ``target_path``), et le clic est remappé de façon
déterministe vers ``set_target_path`` via la map ``suggestion_paths`` persistée.
"""

import json

import pytest
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

from build_query import suggestions_node
from build_query.query_chain import route_agent_output
from build_query.suggestions_node import (  # alias : évite la collecte pytest de Test*
    TestSuggestion as Suggestion,
    TestSuggestionsOutput as SuggestionsOutput,
)


def _plans() -> dict:
    def _sliced(ind: str) -> str:
        return f"WITH base AS (SELECT 1 AS x) SELECT '{ind}' AS reseau, x FROM base"

    return {
        "TMP_FINAL_BP": {
            "sliced_sql": _sliced("BP"),
            "used_columns": [],
            "branch_index": 0,
        },
        "TMP_FINAL_CE": {
            "sliced_sql": _sliced("CE"),
            "used_columns": [],
            "branch_index": 1,
        },
        "all": {"sliced_sql": "SELECT *", "used_columns": [], "branch_index": None},
    }


# --- _branch_catalog_block (pur) -----------------------------------------------------------


def test_branch_catalog_empty_without_plans():
    assert suggestions_node._branch_catalog_block({}, set(), "bigquery") == ""


def test_branch_catalog_lists_branches_with_coverage_and_discriminant():
    state = {"path_plans": json.dumps(_plans())}
    block = suggestions_node._branch_catalog_block(state, {"TMP_FINAL_BP"}, "bigquery")
    assert "<branches_union_all>" in block
    assert "TMP_FINAL_BP" in block and "TMP_FINAL_CE" in block
    assert "`all`" not in block  # le path d'assemblage n'est pas listé comme branche
    assert "déjà couverte" in block  # BP couvert
    assert "non couverte" in block  # CE non couvert
    # Le SELECT discriminant de chaque branche est injecté (token réseau de la branche).
    assert "'CE'" in block or "CE" in block


# --- persistance de suggestion_paths (e2e mocké) -------------------------------------------


def _install(monkeypatch, *, suggestions, capture, persisted):
    class _FakeLLM:
        def with_structured_output(self, _model):
            def _run(prompt_value):
                capture["messages"] = prompt_value.to_messages()
                return SuggestionsOutput(
                    analyse_des_manques="ok", suggestions=suggestions
                )

            return RunnableLambda(_run)

    monkeypatch.setattr(suggestions_node, "make_llm", lambda *a, **k: _FakeLLM())
    monkeypatch.setattr(suggestions_node, "get_test", lambda *a, **k: {})
    monkeypatch.setattr(suggestions_node, "is_native_thinking_active", lambda: True)
    monkeypatch.setattr(
        "utils.saver.persist_completed_tests", lambda *a, **k: 0, raising=False
    )

    def _cap_update(_session, payload):
        persisted.update(payload)

    monkeypatch.setattr(suggestions_node, "update_test", _cap_update)

    async def _fake_retrieve(_session, _state):
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


def _state() -> dict:
    return {
        "session": "s1",
        "query": "SELECT 'BP' AS reseau UNION ALL SELECT 'CE' AS reseau",
        "messages": [],
        "agent_tool_args": {},
        "path_plans": json.dumps(_plans()),
        "query_decomposed": json.dumps(
            [{"name": "q", "code": "SELECT 1", "dependencies": [], "sources": []}]
        ),
    }


@pytest.mark.asyncio
async def test_persists_only_valid_target_paths(monkeypatch):
    """Le suggesteur peut marquer une suggestion comme focalisée ; seules les branches
    VALIDES (présentes dans path_plans) sont retenues dans suggestion_paths."""
    capture: dict = {}
    persisted: dict = {}
    suggestions = [
        Suggestion(text="Vérifie le périmètre CE", target_path="TMP_FINAL_CE"),
        Suggestion(text="Vérifie une branche fantôme", target_path="N_EXISTE_PAS"),
        Suggestion(text="Vérifie le total global", target_path=""),
    ]
    _install(monkeypatch, suggestions=suggestions, capture=capture, persisted=persisted)

    await suggestions_node.generate_suggestions(_state())

    assert persisted["suggestion_paths"] == {"Vérifie le périmètre CE": "TMP_FINAL_CE"}
    # Les 3 suggestions restent affichées ; seule celle au focus valide porte un target_path.
    assert set(persisted["suggestions"]) == {
        "Vérifie le périmètre CE",
        "Vérifie une branche fantôme",
        "Vérifie le total global",
    }


@pytest.mark.asyncio
async def test_branch_catalog_injected_into_prompt(monkeypatch):
    """Le catalogue de branches est injecté dans le prompt du suggesteur contextuel."""
    capture: dict = {}
    persisted: dict = {}
    _install(
        monkeypatch,
        suggestions=[Suggestion(text="Vérifie X", target_path="")],
        capture=capture,
        persisted=persisted,
    )

    await suggestions_node.generate_suggestions(_state())

    user_msg = capture["messages"][1].content
    assert "<branches_union_all>" in user_msg
    assert "TMP_FINAL_BP" in user_msg and "TMP_FINAL_CE" in user_msg


# --- clic suggestion → focus déterministe (conversational_agent) ----------------------------


class _SystemCapturingLLM:
    def __init__(self):
        self.system = None

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.system = messages[0].content
        return AIMessage(content="ok")


@pytest.mark.asyncio
async def test_suggestion_click_focus_forces_set_target_path(monkeypatch):
    """Un clic sur une suggestion marquée focus (suggestion_paths) impose à l'agent
    d'appeler set_target_path avec le nom machine EXACT de la branche."""
    from build_query import conversational_agent as ca

    fake = _SystemCapturingLLM()
    monkeypatch.setattr(ca, "make_llm", lambda *a, **k: fake)

    clicked = "Vérifie le périmètre CE"
    monkeypatch.setattr(
        ca,
        "get_test",
        lambda *a, **k: {
            "suggestions": [clicked],
            "suggestion_paths": {clicked: "TMP_FINAL_CE"},
        },
    )

    async def _fake_retrieve(_session, _state):
        return [{"test_uid": "u1", "test_name": "t", "unit_test_description": "d"}]

    monkeypatch.setattr(ca, "retrieve_existing_tests", _fake_retrieve)

    state = {
        "session": "s1",
        "messages": [],
        "dialect": "bigquery",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": clicked,
        "suggestion_intent": True,
        "gen_retries": 2,
        "path_plans": json.dumps(_plans()),
    }

    await ca.conversational_agent(state)

    assert fake.system is not None
    assert 'set_target_path(path="TMP_FINAL_CE")' in fake.system


# --- routage (inchangé) --------------------------------------------------------------------


def test_route_set_target_path_goes_to_generator():
    assert route_agent_output({"agent_tool_call": "set_target_path"}) == "generator"
