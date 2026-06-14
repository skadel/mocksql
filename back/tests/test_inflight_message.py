"""Classification d'un message saisi PENDANT une génération en cours.

Garantit le mapping route→kind du routeur d'intention réutilisé :
``generator`` → ``instruction`` (MockSQL doit agir sur la génération),
``other`` → ``question`` (explication / réflexion, répondue en direct).
Cf. build_query/inflight_message.
"""

import pytest
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

import build_query.inflight_message as inflight


@pytest.fixture
def fake_history(monkeypatch):
    async def _empty(*_args, **_kwargs):
        return []

    monkeypatch.setattr(inflight, "common_history_retriever", _empty)


def _patch_llm_route(monkeypatch, route: str):
    """Remplace le LLM module-level par un faux qui renvoie le JSON de route voulu."""
    fake = RunnableLambda(lambda _: AIMessage(content=f'{{"route": "{route}"}}'))
    monkeypatch.setattr(inflight, "_llm", fake)


@pytest.mark.asyncio
@pytest.mark.usefixtures("fake_history")
async def test_route_other_maps_to_question(monkeypatch):
    _patch_llm_route(monkeypatch, "other")
    kind = await inflight.classify_inflight_message(
        "sess", "pourquoi j'ai eu ce résultat ?", "bigquery"
    )
    assert kind == "question"


@pytest.mark.asyncio
@pytest.mark.usefixtures("fake_history")
async def test_route_generator_maps_to_instruction(monkeypatch):
    _patch_llm_route(monkeypatch, "generator")
    kind = await inflight.classify_inflight_message(
        "sess", "ajoute un cas avec une date NULL", "bigquery"
    )
    assert kind == "instruction"


@pytest.mark.asyncio
@pytest.mark.usefixtures("fake_history")
async def test_empty_text_defaults_to_instruction():
    # Pas d'appel LLM : court-circuit pour un texte vide.
    assert (
        await inflight.classify_inflight_message("sess", "   ", "bigquery")
        == "instruction"
    )


@pytest.mark.asyncio
@pytest.mark.usefixtures("fake_history")
async def test_llm_failure_falls_back_to_instruction(monkeypatch):
    def _boom(_):
        raise RuntimeError("llm down")

    monkeypatch.setattr(inflight, "_llm", RunnableLambda(_boom))
    kind = await inflight.classify_inflight_message(
        "sess", "n'importe quoi", "bigquery"
    )
    assert kind == "instruction"
