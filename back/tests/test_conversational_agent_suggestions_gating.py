"""Regression: the conversational agent must not expose `generate_suggestions`
as a tool when there is no test yet.

Suggestions are *coverage gaps relative to existing tests* — with zero tests the
tool has nothing to compare against. Offering it invites the model to call a
no-op (the suggestions node early-returns on an empty test list) or to confuse
the user. The tool must appear only once at least one test exists, mirroring the
existing conditional gating of `run_cte` (debug) and `set_target_path` (paths).
"""

import json

import pytest
from langchain_core.messages import AIMessage

from build_query.conversational_agent import conversational_agent
from utils.msg_types import MsgType


class ToolRecordingLLM:
    """Captures the tool names passed to bind_tools, then answers in plain text."""

    def __init__(self):
        self.bound_tool_names: list[str] | None = None

    def bind_tools(self, tools):
        self.bound_tool_names = [
            getattr(t, "name", getattr(t, "__name__", str(t))) for t in tools
        ]
        return self

    async def ainvoke(self, _messages):
        return AIMessage(content="Voici ma réponse.")


def _base_state(messages):
    return {
        "session": "sess-suggestions-gating-xyz",
        "messages": messages,
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "Qu'est-ce que je pourrais tester ensuite ?",
        "gen_retries": 2,
    }


def _results_message_with_one_test():
    content = json.dumps(
        [
            {
                "test_uid": "b7c2",
                "test_index": "1",
                "test_name": "Chemin nominal",
                "unit_test_description": "cas standard",
                "data": {"t": [{"x": 1}]},
                "results_json": "[]",
                "assertion_results": [],
            }
        ]
    )
    return AIMessage(content=content, additional_kwargs={"type": MsgType.RESULTS})


# Outils qui n'ont de sens que face à un test déjà généré (suggestions = manque de
# couverture ; le reste opère sur un test_uid existant).
TEST_DEPENDENT_TOOLS = {
    "generate_suggestions",
    "delete_test",
    "update_test_data",
    "update_test_description",
    "request_reevaluation",
    "patch_test_field",
    "remove_test_row",
    "add_test_row",
}

# Outils toujours pertinents, y compris sans aucun test (démarrer / clarifier / règle métier).
ALWAYS_TOOLS = {"ask_clarification", "generate_test_data", "note_lesson"}


@pytest.mark.asyncio
async def test_test_dependent_tools_hidden_without_tests(monkeypatch):
    fake = ToolRecordingLLM()
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_base_state([]))

    assert fake.bound_tool_names is not None
    bound = set(fake.bound_tool_names)
    assert TEST_DEPENDENT_TOOLS.isdisjoint(bound), (
        f"outils dépendants d'un test exposés à vide : {TEST_DEPENDENT_TOOLS & bound}"
    )
    # Les outils de démarrage restent disponibles.
    assert ALWAYS_TOOLS <= bound


@pytest.mark.asyncio
async def test_test_dependent_tools_exposed_with_a_test(monkeypatch):
    fake = ToolRecordingLLM()
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_base_state([_results_message_with_one_test()]))

    assert fake.bound_tool_names is not None
    bound = set(fake.bound_tool_names)
    assert TEST_DEPENDENT_TOOLS <= bound, (
        f"outils dépendants d'un test manquants : {TEST_DEPENDENT_TOOLS - bound}"
    )
