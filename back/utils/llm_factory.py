from langchain_google_genai import ChatGoogleGenerativeAI

from storage.config import get_llm_model, get_llm_streaming


def make_llm(
    *,
    temperature: float = 0,
    model: str | None = None,
    streaming: bool | None = None,
) -> ChatGoogleGenerativeAI:
    return ChatGoogleGenerativeAI(
        model=model or get_llm_model(),
        vertexai=True,
        temperature=temperature,
        streaming=streaming if streaming is not None else get_llm_streaming(),
    )
