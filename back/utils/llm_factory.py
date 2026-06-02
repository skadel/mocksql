from langchain_google_genai import ChatGoogleGenerativeAI

from storage.config import get_llm_location, get_llm_model, get_llm_streaming


def make_llm(
    *,
    temperature: float = 0,
    model: str | None = None,
    streaming: bool | None = None,
    location: str | None = None,
) -> ChatGoogleGenerativeAI:
    resolved_location = location or get_llm_location()
    kwargs = dict(
        model=model or get_llm_model(),
        vertexai=True,
        temperature=temperature,
        streaming=streaming if streaming is not None else get_llm_streaming(),
    )
    if resolved_location:
        kwargs["location"] = resolved_location
    return ChatGoogleGenerativeAI(**kwargs)
