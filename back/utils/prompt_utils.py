import re

from langchain_classic.output_parsers import OutputFixingParser

from utils.llm_factory import make_llm


def escape_unescaped_placeholders(text):
    # Match {content} but not {{content}}
    return re.sub(r"(?<!\{)\{([^{}]*)\}(?!\})", r"{{\1}}", text)


def create_output_fixing_parser(parser):
    return OutputFixingParser.from_llm(parser=parser, llm=make_llm(), max_retries=2)
