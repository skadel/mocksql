from langchain_google_genai import ChatGoogleGenerativeAI

from storage.config import (
    get_llm_location,
    get_llm_model,
    get_llm_streaming,
    get_llm_thinking_budget,
    get_llm_thinking_level,
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
        callbacks=[_timing_callback, _dump_callback],
    )
    if resolved_location:
        kwargs["location"] = resolved_location

    # Gemini thinking mode — supported on gemini-2.5-flash and gemini-2.5-pro.
    # thinking_budget takes precedence over thinking_level when both are set.
    thinking_budget = get_llm_thinking_budget()
    thinking_level = get_llm_thinking_level()
    if thinking_budget is not None:
        kwargs["thinking_budget"] = thinking_budget
    elif thinking_level is not None:
        kwargs["thinking_level"] = thinking_level

    return ChatGoogleGenerativeAI(**kwargs)
