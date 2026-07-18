import os

from langchain_core.language_models import BaseChatModel
from langchain_google_genai import ChatGoogleGenerativeAI

# Dépendance opt-in (éval avec un modèle OpenAI) : import gardé pour ne pas casser
# une installation sans l'extra — même convention que les connecteurs warehouse.
try:
    from langchain_openai import ChatOpenAI
except ImportError:  # pragma: no cover
    ChatOpenAI = None

from storage.config import (
    get_llm_include_thoughts,
    get_llm_location,
    get_llm_max_retries,
    get_llm_model,
    get_llm_provider,
    get_llm_streaming,
    get_llm_thinking_budget,
    get_llm_thinking_level,
    get_llm_thinking_safety_budget,
    get_llm_timeout,
    is_openai_reasoning_model,
)
from utils.prompt_dump import PromptDumpCallback
from utils.timing import LLMTimingCallback

# Callbacks partagés, sans état entre appels (clé par run_id) → réutilisables sur
# toutes les instances. timing : chronomètre chaque appel (niveau DIAG). dump :
# écrit prompt+output sur disque si MOCKSQL_DUMP_PROMPTS est défini (sinon no-op).
_timing_callback = LLMTimingCallback()
_dump_callback = PromptDumpCallback()


def _make_openai_llm(
    model: str, temperature: float, streaming: bool | None
) -> "ChatOpenAI":
    """Branche OpenAI (modèles gpt-* / o-série). Les réglages Gemini (location,
    thinking_budget, thinking_cap, include_thoughts) ne s'appliquent pas ; seul
    `thinking_level` est traduit en `reasoning_effort` sur les modèles raisonnants."""
    if ChatOpenAI is None:
        raise RuntimeError(
            f"Modèle OpenAI demandé ({model}) mais le paquet langchain-openai est absent. "
            "Installer avec : poetry add langchain-openai"
        )
    # Nom standard OPENAI_API_KEY (lu nativement par le client), avec repli sur
    # OPEN_API_KEY (variante présente dans back/.env).
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPEN_API_KEY")
    if not api_key:
        raise RuntimeError(
            f"Modèle OpenAI demandé ({model}) mais OPENAI_API_KEY est absente de "
            "l'environnement — l'ajouter dans back/.env (OPENAI_API_KEY=sk-...)."
        )
    kwargs: dict = dict(
        model=model,
        api_key=api_key,
        streaming=streaming if streaming is not None else get_llm_streaming(),
        max_retries=get_llm_max_retries(),
        timeout=get_llm_timeout(),
        callbacks=[_timing_callback, _dump_callback],
    )
    if is_openai_reasoning_model(model):
        # gpt-5* / o-série : température par défaut uniquement (400 sinon).
        thinking_level = get_llm_thinking_level()
        if thinking_level is not None:
            kwargs["reasoning_effort"] = thinking_level
    else:
        kwargs["temperature"] = temperature
    return ChatOpenAI(**kwargs)


def make_llm(
    *,
    temperature: float = 0,
    model: str | None = None,
    streaming: bool | None = None,
    location: str | None = None,
    thinking_cap: int | None = None,
) -> BaseChatModel:
    resolved_model = model or get_llm_model()
    if get_llm_provider(resolved_model) == "openai":
        return _make_openai_llm(resolved_model, temperature, streaming)

    resolved_location = location or get_llm_location()
    kwargs: dict = dict(
        model=resolved_model,
        vertexai=True,
        temperature=temperature,
        streaming=streaming if streaming is not None else get_llm_streaming(),
        # Retry/backoff explicite sur erreur transitoire (429 quota TPM notamment) : rendu
        # déterministe et surchargeable plutôt que de dépendre du défaut implicite du client.
        max_retries=get_llm_max_retries(),
        # Sans timeout, httpx attend indéfiniment une requête que le serveur ne clôt
        # jamais (hang muet, cf. get_llm_timeout). None (llm.timeout: 0) = illimité.
        timeout=get_llm_timeout(),
        callbacks=[_timing_callback, _dump_callback],
    )
    if resolved_location:
        kwargs["location"] = resolved_location
    if get_llm_include_thoughts():
        kwargs["include_thoughts"] = True

    # Gemini thinking mode — supported on gemini-2.5-flash and gemini-2.5-pro.
    # thinking_budget takes precedence over thinking_level when both are set.
    thinking_budget = get_llm_thinking_budget()
    thinking_level = get_llm_thinking_level()
    if thinking_budget is not None:
        kwargs["thinking_budget"] = thinking_budget
    elif thinking_level is not None:
        kwargs["thinking_level"] = thinking_level
    else:
        # Ni budget ni niveau explicites : plafond de sécurité dérivé du timeout, pour que
        # la rumination s'épuise DANS la fenêtre (le budget mord avant le kill dur du
        # timeout). Anti-rumination proactif, cf. incident bq001. None = pas de plafond.
        safety_budget = get_llm_thinking_safety_budget()
        # `thinking_cap` : borne PAR APPEL posée par un nœud à sortie courte
        # (cf. suggestions_generator) — le plafond dérivé est anti-hang (24 576 tok)
        # et ne mord jamais pour quelques phrases. Le cap ne s'applique QUE dans
        # cette branche : un thinking_budget/thinking_level explicite de
        # l'utilisateur prime toujours.
        if thinking_cap is not None:
            safety_budget = (
                thinking_cap
                if safety_budget is None
                else min(safety_budget, thinking_cap)
            )
        if safety_budget is not None:
            kwargs["thinking_budget"] = safety_budget

    return ChatGoogleGenerativeAI(**kwargs)
