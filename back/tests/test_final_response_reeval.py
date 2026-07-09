"""Le message de clôture (final_response) ne doit pas annoncer « généré » quand
l'opération est une **réévaluation** : SQL mis à jour ou bannière « fichier modifié »
qui re-exécute les tests existants (``rerun_all_tests``). Les tests existaient déjà —
dire « j'ai généré le test » est faux et trompeur.

Les mots d'action sont localisés (défaut produit : anglais ; ``language: fr`` /
env ``MOCKSQL_LANGUAGE`` pour le français) — les assertions portent sur le défaut
anglais, plus un test dédié au chemin français.
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


def test_rerun_all_action_is_reevaluate_not_generate():
    """rerun_all_tests sans agent_tool_call → action « re-evaluated », jamais « generated »."""
    ctx = _collect_run_context(_base_state(rerun_all_tests=True))
    assert ctx["action"] == "re-evaluated"


def test_rerun_all_action_localized_french(monkeypatch):
    """Avec la langue fr, le même chemin produit « réévalué »."""
    monkeypatch.setenv("MOCKSQL_LANGUAGE", "fr")
    ctx = _collect_run_context(_base_state(rerun_all_tests=True))
    assert ctx["action"] == "réévalué"


def test_agent_call_still_wins_over_rerun():
    """Un add/modify explicite de l'agent garde la priorité sur le flag rerun."""
    ctx = _collect_run_context(
        _base_state(rerun_all_tests=True, agent_tool_call="update_test_data")
    )
    assert ctx["action"] == "updated"


def test_fresh_generation_still_generate():
    """Sans rerun_all_tests ni agent_tool_call → 1ʳᵉ génération → « generated »."""
    ctx = _collect_run_context(_base_state())
    assert ctx["action"] == "generated"


@pytest.mark.asyncio
async def test_facts_say_reevaluate_to_llm(monkeypatch):
    """Les faits transmis au LLM annoncent « re-evaluated », pas « generated »."""
    captured = {}

    class _CapturingLLM:
        async def ainvoke(self, messages):
            captured["human"] = messages[-1].content

            class _R:
                content = "I re-evaluated your test, everything passes."

            return _R()

    monkeypatch.setattr(final_response_node, "make_llm", lambda: _CapturingLLM())
    await final_response(_base_state(rerun_all_tests=True))

    assert "re-evaluated" in captured["human"]
    assert "generated" not in captured["human"]


@pytest.mark.asyncio
async def test_fallback_message_uses_reevaluate(monkeypatch):
    """LLM indisponible → le fallback templaté dit « re-evaluated », pas « generated »."""
    monkeypatch.setattr(final_response_node, "make_llm", lambda: _BoomLLM())
    out = await final_response(_base_state(rerun_all_tests=True))

    text = out["messages"][0].content
    assert "re-evaluated" in text
    assert "generated" not in text
