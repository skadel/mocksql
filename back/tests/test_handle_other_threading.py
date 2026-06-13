"""Régression : le nœud `_handle_other` doit chaîner sa réponse SOUS la question.

Bug : la réponse hors-sujet (`MsgType.OTHER`) était attachée à `parent_message_id`
— le MÊME parent que le message QUERY de l'utilisateur (créé dans `routing.py` avec
`id=user_message_id, parent=parent_message_id`). Question et réponse devenaient donc
FRÈRES au lieu d'être chaînées question→réponse.

Conséquence : `get_messages_history` reconstruit l'historique en remontant la chaîne
de parents ; comme la réponse OTHER et la question partagent un parent, elles sont sur
deux branches alternatives. Selon la branche active suivie ensuite, la réponse OTHER
tombe sur une branche morte et DISPARAÎT de l'historique vu par le conversational_agent.

Tous les autres producteurs de bulles bot qui suivent un message utilisateur chaînent
sous `user_message_id` (cf. `other.py`, `conversational_agent.py`). Ce test épingle la
cohérence : la réponse hors-sujet chaîne sous la question.
"""

import pytest
from langchain_core.messages import AIMessage

from build_query.query_chain import _handle_other, _bad_data_exhausted
from build_query.delete_test_node import delete_test_node
from utils.msg_types import MsgType


class _Result:
    def __init__(self, content):
        self.content = content


class _FakeChain:
    def __init__(self, content, exc=None):
        self._content = content
        self._exc = exc

    async def ainvoke(self, _vars):
        if self._exc is not None:
            raise self._exc
        return _Result(self._content)


class _FakePrompt:
    """`prompt | llm` → renvoie une chaîne factice quel que soit le llm."""

    def __init__(self, content, exc=None):
        self._content = content
        self._exc = exc

    def __or__(self, _other):
        return _FakeChain(self._content, self._exc)


@pytest.mark.asyncio
async def test_handle_other_threads_answer_under_user_message(monkeypatch):
    monkeypatch.setattr("build_query.query_chain.make_llm", lambda: object())
    monkeypatch.setattr(
        "build_query.prompt_tools.build_other_prompt",
        lambda user_input, dialect, history: _FakePrompt("DuckDB est un moteur SQL."),
    )

    state = {
        "input": "C'est quoi DuckDB ?",
        "dialect": "duckdb",
        "schemas": [],
        "history": [],
        "user_message_id": "umsg-1",
        "parent_message_id": "pmsg-0",
        "request_id": "req-1",
    }

    update = await _handle_other(state)
    [msg] = update["messages"]

    assert msg.additional_kwargs["type"] == MsgType.OTHER
    # La réponse chaîne SOUS la question (user_message_id) — pas en frère (parent_message_id).
    assert msg.additional_kwargs["parent"] == "umsg-1"


@pytest.mark.asyncio
async def test_handle_other_error_also_threads_under_user_message(monkeypatch):
    """Le repli erreur (permission Vertex) chaîne lui aussi sous la question, par cohérence."""
    monkeypatch.setattr("build_query.query_chain.make_llm", lambda: object())
    monkeypatch.setattr(
        "build_query.prompt_tools.build_other_prompt",
        lambda user_input, dialect, history: _FakePrompt(
            "", exc=RuntimeError("denied")
        ),
    )
    monkeypatch.setattr(
        "utils.llm_errors.is_vertex_permission_error", lambda _exc: True
    )
    monkeypatch.setattr(
        "utils.llm_errors.format_vertex_permission_message",
        lambda _model: "Accès refusé.",
    )

    state = {
        "input": "C'est quoi DuckDB ?",
        "dialect": "duckdb",
        "schemas": [],
        "history": [],
        "user_message_id": "umsg-1",
        "parent_message_id": "pmsg-0",
        "request_id": "req-1",
    }

    update = await _handle_other(state)
    [msg] = update["messages"]

    assert msg.additional_kwargs["type"] == MsgType.ERROR
    assert msg.additional_kwargs["parent"] == "umsg-1"


def _bot_msg(mid):
    return AIMessage(content="x", id=mid, additional_kwargs={"type": MsgType.RESULTS})


@pytest.mark.asyncio
async def test_delete_test_threads_under_last_message(monkeypatch):
    """delete_test chaîne sous le dernier message du tour (pas en frère du QUERY)."""
    monkeypatch.setattr(
        "build_query.delete_test_node.get_test",
        lambda _sess: {"test_cases": [{"test_index": "1"}, {"test_index": "2"}]},
    )
    monkeypatch.setattr(
        "build_query.delete_test_node.update_test", lambda *a, **k: None
    )

    state = {
        "session": "sess1",
        "agent_tool_args": {"test_index": "1"},
        "messages": [_bot_msg("last-msg")],
        "parent_message_id": "pmsg-0",
        "request_id": "req-1",
    }
    update = await delete_test_node(state)
    [msg] = update["messages"]
    assert msg.additional_kwargs["type"] == MsgType.DELETE_TEST
    assert msg.additional_kwargs["parent"] == "last-msg"


@pytest.mark.asyncio
async def test_retry_prompt_threads_under_last_message():
    """Le RETRY_PROMPT (boucle bad_data) chaîne sous le dernier message du tour."""
    state = {
        "messages": [_bot_msg("eval-msg")],
        "parent_message_id": "pmsg-0",
        "request_id": "req-1",
        "test_index": "1",
    }
    update = await _bad_data_exhausted(state)
    [msg] = update["messages"]
    assert msg.additional_kwargs["type"] == MsgType.RETRY_PROMPT
    assert msg.additional_kwargs["parent"] == "eval-msg"
