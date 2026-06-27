"""Le message de clôture (final_response) ne doit pas annoncer « généré » quand
l'opération est une **réévaluation** : SQL mis à jour ou bannière « fichier modifié »
qui re-exécute les tests existants (``rerun_all_tests``). Les tests existaient déjà —
dire « j'ai généré le test » est faux et trompeur.
"""

import pytest

from build_query import final_response_node
from build_query.final_response_node import _collect_run_context, final_response


class _BoomLLM:
    async def ainvoke(self, _messages):
        raise RuntimeError("llm down")


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


def test_rerun_all_action_is_reevalue_not_genere():
    """rerun_all_tests sans agent_tool_call → action « réévalué », jamais « généré »."""
    ctx = _collect_run_context(_base_state(rerun_all_tests=True))
    assert ctx["action"] == "réévalué"


def test_agent_call_still_wins_over_rerun():
    """Un add/modify explicite de l'agent garde la priorité sur le flag rerun."""
    ctx = _collect_run_context(
        _base_state(rerun_all_tests=True, agent_tool_call="update_test_data")
    )
    assert ctx["action"] == "modifié"


def test_fresh_generation_still_genere():
    """Sans rerun_all_tests ni agent_tool_call → 1ʳᵉ génération → « généré »."""
    ctx = _collect_run_context(_base_state())
    assert ctx["action"] == "généré"


@pytest.mark.asyncio
async def test_facts_say_reevalue_to_llm(monkeypatch):
    """Les faits transmis au LLM annoncent « réévalué », pas « généré »."""
    captured = {}

    class _CapturingLLM:
        async def ainvoke(self, messages):
            captured["human"] = messages[-1].content

            class _R:
                content = "J'ai réévalué ton test, tout passe."

            return _R()

    monkeypatch.setattr(final_response_node, "make_llm", lambda: _CapturingLLM())
    await final_response(_base_state(rerun_all_tests=True))

    assert "réévalué" in captured["human"]
    assert "généré" not in captured["human"]


@pytest.mark.asyncio
async def test_fallback_message_uses_reevalue(monkeypatch):
    """LLM indisponible → le fallback templaté dit « réévalué », pas « généré »."""
    monkeypatch.setattr(final_response_node, "make_llm", lambda: _BoomLLM())
    out = await final_response(_base_state(rerun_all_tests=True))

    text = out["messages"][0].content
    assert "réévalué" in text
    assert "généré" not in text
