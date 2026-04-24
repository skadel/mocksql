import re

from langchain_classic.output_parsers import OutputFixingParser
from langchain_google_genai import ChatGoogleGenerativeAI

from models.env_variables import DEFAULT_MODEL_NAME


def escape_unescaped_placeholders(text):
    # Match {content} but not {{content}}
    return re.sub(r"(?<!\{)\{([^{}]*)\}(?!\})", r"{{\1}}", text)


_llm = None


def _get_llm():
    global _llm
    if _llm is None:
        _llm = ChatGoogleGenerativeAI(
            model=DEFAULT_MODEL_NAME, vertexai=True, temperature=0
        )
    return _llm


def create_output_fixing_parser(parser):
    return OutputFixingParser.from_llm(parser=parser, llm=_get_llm(), max_retries=2)
