import re

from langchain_classic.output_parsers import OutputFixingParser
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

from utils.llm_factory import make_llm


def escape_unescaped_placeholders(text):
    # Match {content} but not {{content}}
    return re.sub(r"(?<!\{)\{([^{}]*)\}(?!\})", r"{{\1}}", text)


def _strip_code_fences(msg) -> AIMessage:
    text = msg.content if hasattr(msg, "content") else str(msg)
    text = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
    text = re.sub(r"\n?```\s*$", "", text.strip())
    return AIMessage(content=text.strip())


def create_output_fixing_parser(parser):
    fixing_parser = OutputFixingParser.from_llm(
        parser=parser, llm=make_llm(), max_retries=2
    )
    return RunnableLambda(_strip_code_fences) | fixing_parser
