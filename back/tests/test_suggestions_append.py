"""Tests du mode append des suggestions (l'agent « rajoute des suggestions »).

Comportement visé :
  - les nouvelles suggestions S'AJOUTENT aux existantes, les plus récentes en tête,
    dédupliquées et plafonnées à SUGGESTIONS_CAP ;
  - quand le plafond est déjà atteint, on n'ajoute pas en silence : pas d'appel LLM,
    un message d'avertissement est renvoyé à l'utilisateur ;
  - le bouton « Régénérer » (regenerate_suggestions) et `replace=True` gardent le
    comportement de remplacement complet.
"""

import pytest

from build_query import suggestions_node
from build_query.suggestions_node import _merge_suggestions, SUGGESTIONS_CAP
from utils.msg_types import MsgType
from utils.saver import get_message_type


# --- _merge_suggestions : fusion pure ----------------------------------------


def test_merge_newest_first_and_capped():
    new = ["new1", "new2", "new3"]
    pending = ["old1", "old2", "old3"]
    merged = _merge_suggestions(new, pending)
    # Les nouvelles en tête, plafonné : on écarte les plus anciennes.
    assert merged == ["new1", "new2", "new3", "old1", "old2"]
    assert len(merged) == SUGGESTIONS_CAP


def test_merge_dedup_keeps_first_occurrence():
    merged = _merge_suggestions(["a", "b"], ["b", "c"])
    assert merged == ["a", "b", "c"]


def test_merge_below_cap_keeps_all():
    assert _merge_suggestions(["a"], ["b"]) == ["a", "b"]


def test_merge_strips_and_drops_blanks():
    assert _merge_suggestions([" a ", ""], ["a", " "]) == ["a"]


# --- garde-fou plafond : pas d'ajout silencieux ------------------------------


@pytest.mark.asyncio
async def test_append_at_cap_blocks_and_warns(monkeypatch):
    """Append demandé alors que CAP suggestions sont déjà en attente → pas de
    génération LLM, message d'avertissement renvoyé à l'utilisateur."""
    pending = [f"suggestion {i}" for i in range(SUGGESTIONS_CAP)]

    async def fake_retrieve(session, state):
        return [{"test_index": 0, "test_name": "t", "test_cases": []}]

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", fake_retrieve)
    monkeypatch.setattr(
        suggestions_node, "get_test", lambda *a, **k: {"suggestions": pending}
    )

    def _boom(*a, **k):
        raise AssertionError(
            "make_llm ne doit pas être appelé quand le plafond est atteint"
        )

    monkeypatch.setattr(suggestions_node, "make_llm", _boom)

    state = {
        "session": "s1",
        "agent_tool_call": "generate_suggestions",
        "agent_tool_args": {"instructions": "rajoute des cas prod"},
        "messages": [],
    }
    result = await suggestions_node.generate_suggestions(state)
    msgs = result["messages"]
    assert len(msgs) == 1
    assert get_message_type(msgs[0]) == MsgType.OTHER
    assert str(SUGGESTIONS_CAP) in msgs[0].content


@pytest.mark.asyncio
async def test_replace_true_does_not_block_at_cap(monkeypatch):
    """`replace=True` au plafond ne déclenche PAS le garde-fou : on régénère."""
    pending = [f"suggestion {i}" for i in range(SUGGESTIONS_CAP)]

    async def fake_retrieve(session, state):
        return [{"test_index": 0, "test_name": "t", "test_cases": []}]

    called = {"llm": False}

    def _mark(*a, **k):
        called["llm"] = True
        raise RuntimeError("stop after make_llm")  # on coupe juste après le garde-fou

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", fake_retrieve)
    monkeypatch.setattr(
        suggestions_node, "get_test", lambda *a, **k: {"suggestions": pending}
    )
    monkeypatch.setattr(suggestions_node, "make_llm", _mark)

    state = {
        "session": "s1",
        "agent_tool_call": "generate_suggestions",
        "agent_tool_args": {"instructions": "", "replace": True},
        "messages": [],
    }
    # make_llm() est hors du try/except du nœud → l'exception remonte. L'important :
    # on a bien ATTEINT make_llm (le garde-fou plafond n'a pas court-circuité).
    with pytest.raises(RuntimeError):
        await suggestions_node.generate_suggestions(state)
    assert called["llm"] is True
