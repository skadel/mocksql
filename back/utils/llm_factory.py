from langchain_google_genai import ChatGoogleGenerativeAI

from storage.config import (
    get_llm_include_thoughts,
    get_llm_location,
    get_llm_max_retries,
    get_llm_model,
    get_llm_streaming,
    get_llm_thinking_budget,
    get_llm_thinking_level,
    get_llm_thinking_safety_budget,
    get_llm_timeout,
)
from utils.prompt_dump import PromptDumpCallback
from utils.timing import LLMTimingCallback

# Callbacks partagés, sans état entre appels (clé par run_id) → réutilisables sur
# toutes les instances. timing : chronomètre chaque appel (niveau DIAG). dump :
# écrit prompt+output sur disque si MOCKSQL_DUMP_PROMPTS est défini (sinon no-op).
_timing_callback = LLMTimingCallback()
_dump_callback = PromptDumpCallback()


def make_llm(
    *,
    temperature: float = 0,
    model: str | None = None,
    streaming: bool | None = None,
    location: str | None = None,
) -> ChatGoogleGenerativeAI:
    resolved_location = location or get_llm_location()
    kwargs: dict = dict(
        model=model or get_llm_model(),
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
        if safety_budget is not None:
            kwargs["thinking_budget"] = safety_budget

    return ChatGoogleGenerativeAI(**kwargs)
