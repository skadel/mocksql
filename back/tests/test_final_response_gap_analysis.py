"""Le message de clôture (final_response) doit, à la 1ʳᵉ génération, exposer
l'analyse des manques produite par ``suggestions_generator`` et renvoyer
l'utilisateur vers le panneau Suggestions.

``coverage_gap_analysis`` est posé dans le state par ``generate_suggestions``
(cf. suggestions_node). final_response le tisse dans le message de clôture :
1–2 phrases sur ce qui n'est pas couvert + pointeur vers le panneau. Sur une
édition (pas de suggestions auto), le champ est absent → aucun pointeur.
"""

import pytest

from build_query import final_response_node
from build_query.final_response_node import final_response


class _FakeLLM:
    """LLM factice : renvoie un texte fixe pour vérifier le chemin nominal."""

    def __init__(self, text):
        self._text = text

    async def ainvoke(self, _messages):
        class _R:
            content = self._text

        return _R()


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


@pytest.mark.asyncio
async def test_gap_analysis_passed_to_llm_facts(monkeypatch):
    """L'analyse des manques doit être transmise au LLM dans les faits."""
    captured = {}

    class _CapturingLLM:
        async def ainvoke(self, messages):
            captured["human"] = messages[-1].content
            captured["system"] = messages[0].content

            class _R:
                content = "J'ai généré ton test. Reste à couvrir le cas vide."

            return _R()

    monkeypatch.setattr(final_response_node, "make_llm", lambda: _CapturingLLM())

    state = _base_state(
        coverage_gap_analysis="Le cas d'une plage vide n'est pas couvert."
    )
    await final_response(state)

    assert "plage vide n'est pas couvert" in captured["human"]
    assert "panneau Suggestions" in captured["system"]


@pytest.mark.asyncio
async def test_no_gap_analysis_no_panel_in_system(monkeypatch):
    """Sans analyse des manques (édition) → pas de consigne panneau dans le system."""
    captured = {}

    class _CapturingLLM:
        async def ainvoke(self, messages):
            captured["system"] = messages[0].content

            class _R:
                content = "J'ai modifié ton test, tout passe."

            return _R()

    monkeypatch.setattr(final_response_node, "make_llm", lambda: _CapturingLLM())

    state = _base_state(agent_tool_call="update_test_data")
    await final_response(state)

    assert "panneau Suggestions" not in captured["system"]


@pytest.mark.asyncio
async def test_fallback_appends_panel_pointer_on_llm_error(monkeypatch):
    """LLM indisponible + analyse des manques présente → le fallback templaté
    doit quand même pointer vers le panneau Suggestions."""
    monkeypatch.setattr(final_response_node, "make_llm", lambda: _BoomLLM())

    state = _base_state(coverage_gap_analysis="Les valeurs NULL ne sont pas testées.")
    out = await final_response(state)

    text = out["messages"][0].content
    assert "Suggestions panel" in text


@pytest.mark.asyncio
async def test_fallback_appends_panel_pointer_on_llm_error_fr(monkeypatch):
    """Même chose en langue de sortie française : pointeur localisé."""
    monkeypatch.setenv("MOCKSQL_LANGUAGE", "fr")
    monkeypatch.setattr(final_response_node, "make_llm", lambda: _BoomLLM())

    state = _base_state(coverage_gap_analysis="Les valeurs NULL ne sont pas testées.")
    out = await final_response(state)

    text = out["messages"][0].content
    assert "panneau Suggestions" in text


@pytest.mark.asyncio
async def test_fallback_no_pointer_without_gap_analysis(monkeypatch):
    """LLM indisponible sans analyse des manques → fallback sans pointeur panneau."""
    monkeypatch.setattr(final_response_node, "make_llm", lambda: _BoomLLM())

    state = _base_state()
    out = await final_response(state)

    text = out["messages"][0].content
    assert "Suggestions panel" not in text
    assert "panneau Suggestions" not in text
