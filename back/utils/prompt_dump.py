"""Dump du prompt entièrement rendu + output de chaque appel LLM, pour debug.

Activé par l'env ``MOCKSQL_DUMP_PROMPTS`` (ex. ``generator,suggestions_generator``
ou ``*``). Absent/vide = aucun coût. Écrit un fichier markdown par appel dans
``logs/prompts/{label}/`` (gitignoré, cf. ``storage/config.get_prompt_dump_dir``).

Le **label** identifie l'agent. Il vient de ``metadata["mocksql_label"]`` (sous-label
opt-in posé via ``.with_config({"metadata": {"mocksql_label": "generator:update"}})``
sur un call-site précis), sinon de ``metadata["langgraph_node"]`` (injecté
automatiquement par LangGraph dans les runs enfants), sinon ``"llm"``.

Branché automatiquement dans ``make_llm``, à côté de ``LLMTimingCallback`` —
capture donc le prompt rendu quel que soit le call-site (``with_structured_output``,
``prompt | llm | parser`` ou ``llm.ainvoke`` direct).
"""

import json
import logging
import time
from datetime import datetime

from langchain_core.callbacks import BaseCallbackHandler

from storage.config import get_prompt_dump_dir, get_prompt_dump_filter
from utils.timing import _model_name

logger = logging.getLogger("prompt_dump")


class PromptDumpCallback(BaseCallbackHandler):
    """Écrit prompt+output sur disque pour les appels LLM dont le label matche."""

    def __init__(self) -> None:
        # run_id -> (start, label, model, prompt_md)
        self._pending: dict = {}

    def on_chat_model_start(
        self, serialized, messages, *, run_id, metadata=None, **kwargs
    ):
        flt = get_prompt_dump_filter()
        if flt is None:
            return
        label = _resolve_label(metadata)
        if not _matches(label, flt):
            return
        self._pending[run_id] = (
            time.perf_counter(),
            label,
            _model_name(serialized, kwargs),
            _render_prompt(messages),
        )

    def on_llm_end(self, response, *, run_id, **kwargs):
        entry = self._pending.pop(run_id, None)
        if entry is None:
            return
        start, label, model, prompt_md = entry
        output_md, usage = _render_output(response)
        _write_dump(
            label, model, _elapsed_ms(start), usage, prompt_md, output_md, run_id
        )

    def on_llm_error(self, error, *, run_id, **kwargs):
        entry = self._pending.pop(run_id, None)
        if entry is None:
            return
        start, label, model, prompt_md = entry
        output_md = f"### erreur\n\n```\n{error}\n```"
        _write_dump(
            label,
            model,
            _elapsed_ms(start),
            None,
            prompt_md,
            output_md,
            run_id,
            suffix="-ERROR",
        )


def _elapsed_ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


def _resolve_label(metadata) -> str:
    meta = metadata or {}
    return meta.get("mocksql_label") or meta.get("langgraph_node") or "llm"


def _matches(label: str, flt: set[str]) -> bool:
    if "*" in flt:
        return True
    base = label.split(":", 1)[0]
    return label in flt or base in flt or any(label.startswith(tok) for tok in flt)


def _content_to_text(content) -> str:
    if isinstance(content, str):
        return content
    # Contenu multimodal (liste de blocs) : sérialise lisiblement.
    return json.dumps(content, ensure_ascii=False, indent=2, default=str)


def _render_prompt(messages) -> str:
    # messages : List[List[BaseMessage]] (un sous-tableau par prompt du batch).
    parts: list[str] = []
    for batch in messages:
        for m in batch:
            role = getattr(m, "type", m.__class__.__name__)
            parts.append(f"### [{role}]\n\n{_content_to_text(m.content)}")
    return "\n\n".join(parts) or "(vide)"


def _render_output(response):
    """Rend l'output en markdown + extrait l'usage tokens (best-effort).

    Pour ``with_structured_output``, la réponse arrive en tool call → le JSON
    structuré apparaît sous la section ``tool_calls``.
    """
    parts: list[str] = []
    usage = None
    try:
        for gen_list in response.generations:
            for gen in gen_list:
                text = getattr(gen, "text", "") or ""
                if text:
                    parts.append(f"### text\n\n{text}")
                msg = getattr(gen, "message", None)
                if msg is None:
                    continue
                content = getattr(msg, "content", None)
                if isinstance(content, list):
                    # include_thoughts=True → blocs {"type": "reasoning"} : les rendre
                    # pour voir sur quoi le modèle rumine (gen.text les exclut).
                    thoughts = [
                        b["reasoning"]
                        for b in content
                        if isinstance(b, dict)
                        and b.get("type") == "reasoning"
                        and b.get("reasoning")
                    ]
                    if thoughts:
                        parts.insert(0, "### reasoning\n\n" + "\n\n".join(thoughts))
                tcs = getattr(msg, "tool_calls", None)
                if tcs:
                    rendered = json.dumps(
                        tcs, ensure_ascii=False, indent=2, default=str
                    )
                    parts.append(f"### tool_calls\n\n```json\n{rendered}\n```")
                usage = usage or getattr(msg, "usage_metadata", None)
    except Exception as e:  # pragma: no cover — le dump ne doit jamais casser un run
        parts.append(f"(échec rendu output: {e})")
    if usage is None and getattr(response, "llm_output", None):
        usage = response.llm_output.get("usage_metadata")
    return ("\n\n".join(parts) or "(vide)"), usage


def _write_dump(
    label, model, elapsed_ms, usage, prompt_md, output_md, run_id, suffix=""
):
    try:
        dump_dir = get_prompt_dump_dir() / label.replace(":", "__")
        dump_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now()
        fname = ts.strftime("%Y%m%d-%H%M%S-%f")[:-3] + f"-{str(run_id)[:8]}{suffix}.md"
        usage_line = ""
        if usage:
            reasoning = (usage.get("output_token_details") or {}).get("reasoning")
            thinking = f" (dont thinking={reasoning})" if reasoning else ""
            usage_line = f"- tokens: in={usage.get('input_tokens')} out={usage.get('output_tokens')}{thinking}\n"
        body = (
            f"# Prompt dump — {label}\n\n"
            f"- timestamp: {ts.isoformat(timespec='seconds')}\n"
            f"- label: {label}\n"
            f"- model: {model}\n"
            f"- latency_ms: {elapsed_ms:.0f}\n"
            f"{usage_line}"
            f"\n## PROMPT\n\n{prompt_md}\n\n## OUTPUT\n\n{output_md}\n"
        )
        path = dump_dir / fname
        path.write_text(body, encoding="utf-8")
        logger.diag("[dump] %s → %s", label, path)
    except Exception as e:  # pragma: no cover — best-effort, ne casse jamais le run
        logger.warning("[dump] échec écriture pour %s: %s", label, e)
