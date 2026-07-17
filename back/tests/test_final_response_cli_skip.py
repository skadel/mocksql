"""En mode CLI, ``final_response`` ne fait AUCUN appel LLM.

Le message de clôture n'est jamais affiché par la CLI — l'appel coûtait ~24 s
par modèle, soit ~44 min sur un batch de 110 (spider2-snow). Le nœud sert
directement le fallback templaté (gratuit) : l'historique garde ainsi un
message de clôture persisté si le modèle est ensuite ouvert en UI.

Le chemin serveur est INCHANGÉ — y compris la directive de langue doublée
(bookend recency), intentionnelle (cf. final_response_node).
"""

import pytest

from build_query import final_response_node
from build_query.final_response_node import final_response
from utils.msg_types import MsgType


def _counting_llm(calls):
    class _LLM:
        async def ainvoke(self, _messages):
            calls["n"] += 1

            class _R:
                content = "message de clôture rédigé par le LLM"

            return _R()

    return _LLM


def _base_state(**extra):
    state = {
        "request_id": "req-1",
        "parent_message_id": "pmsg-1",
        "status": "complete",
        "evaluation_feedback": None,
        "messages": [],
    }
    state.update(extra)
    return state


@pytest.mark.asyncio
async def test_cli_mode_skips_llm_and_emits_templated_closing(monkeypatch):
    calls = {"n": 0}
    monkeypatch.setattr(final_response_node, "make_llm", _counting_llm(calls))

    out = await final_response(_base_state(cli_mode=True))

    assert calls["n"] == 0, "final_response ne doit pas appeler le LLM en mode CLI"
    msg = out["messages"][0]
    assert msg.content.strip(), "le message templaté doit rester persisté"
    assert msg.additional_kwargs["type"] == MsgType.FINAL_RESPONSE


@pytest.mark.asyncio
async def test_cli_mode_keeps_panel_pointer_with_gap_analysis(monkeypatch):
    """L'analyse de couverture (1ʳᵉ génération) garde son pointeur panneau."""
    calls = {"n": 0}
    monkeypatch.setattr(final_response_node, "make_llm", _counting_llm(calls))

    out = await final_response(
        _base_state(cli_mode=True, coverage_gap_analysis="Cas vide non couvert.")
    )

    assert calls["n"] == 0
    assert "Suggestions" in out["messages"][0].content


@pytest.mark.asyncio
async def test_server_mode_still_calls_llm(monkeypatch):
    """Sans le flag (mode serveur), le comportement LLM est inchangé."""
    calls = {"n": 0}
    monkeypatch.setattr(final_response_node, "make_llm", _counting_llm(calls))

    out = await final_response(_base_state())

    assert calls["n"] == 1
    assert out["messages"][0].content == "message de clôture rédigé par le LLM"
