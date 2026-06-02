import logging
import re

from langchain_classic.output_parsers import OutputFixingParser
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

from utils.llm_factory import make_llm

import utils.logger  # noqa: F401 — registers DIAG level

logger = logging.getLogger(__name__)


def escape_unescaped_placeholders(text):
    # Match {content} but not {{content}}
    return re.sub(r"(?<!\{)\{([^{}]*)\}(?!\})", r"{{\1}}", text)


def _strip_code_fences(msg) -> AIMessage:
    content = msg.content if hasattr(msg, "content") else str(msg)
    if isinstance(content, list):
        text = "".join(
            block.get("text", "") if isinstance(block, dict) else str(block)
            for block in content
        )
    else:
        text = content
    before = text[:80].replace("\n", "\\n")
    text = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
    text = re.sub(r"\n?```\s*$", "", text.strip())
    after = text[:80].replace("\n", "\\n")
    logger.diag("[strip_code_fences] avant=%r → après=%r", before, after)
    return AIMessage(content=text.strip())


class _FenceStrippingParser:
    """Enveloppe le parser sous-jacent pour stripper les fences markdown avant chaque tentative.

    L'OutputFixingParser a un chemin de retry où son LLM interne retourne du JSON
    enveloppé en ```json ... ``` sans repasser par _strip_code_fences. Ce wrapper
    garantit que TOUTES les tentatives (initiale + retry) strippent les fences,
    et logue l'erreur originale au niveau DIAG pour faciliter le debug.
    """

    def __init__(self, inner):
        self._inner = inner

    def _strip(self, text: str) -> str:
        text = re.sub(r"^```(?:json)?\s*\n?", "", text.strip())
        text = re.sub(r"\n?```\s*$", "", text.strip())
        return text.strip()

    def parse(self, text: str):
        stripped = self._strip(text)
        logger.diag(
            "[FenceStrippingParser] parse, input[:60]=%r stripped[:60]=%r",
            text[:60],
            stripped[:60],
        )
        try:
            return self._inner.parse(stripped)
        except Exception as exc:
            logger.diag(
                "[FenceStrippingParser] parse FAILED type=%s msg=%s",
                type(exc).__name__,
                str(exc)[:300],
            )
            raise

    def get_format_instructions(self):
        return self._inner.get_format_instructions()

    def __getattr__(self, name):
        return getattr(self._inner, name)


def create_output_fixing_parser(parser):
    fence_parser = _FenceStrippingParser(parser)
    fixing_parser = OutputFixingParser.from_llm(
        parser=fence_parser, llm=make_llm(), max_retries=2
    )
    # _strip_code_fences sur le chemin initial (LLM → AIMessage → fixing_parser)
    return RunnableLambda(_strip_code_fences) | fixing_parser
