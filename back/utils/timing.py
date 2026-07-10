"""Instrumentation de performance : chronomètre les phases du workflow.

Toutes les durées sont loggées au niveau DIAG (15) sous le préfixe ``[timing]``
pour pouvoir être activées/désactivées via le niveau de log sans bruiter la prod.

Trois points d'entrée :
- ``timed`` / ``atimed`` : context managers autour d'un bloc arbitraire
  (ex. dry-run BigQuery, exécution DuckDB).
- ``timed_node`` : wrappe un nœud LangGraph async pour chronométrer son exécution.
- ``LLMTimingCallback`` : callback LangChain qui chronomètre chaque appel LLM
  (branché automatiquement dans ``make_llm``), y compris le temps de streaming.
"""

import logging
import time
from contextlib import asynccontextmanager, contextmanager

import utils.logger  # noqa: F401 — enregistre le niveau DIAG (15)
from langchain_core.callbacks import BaseCallbackHandler

logger = logging.getLogger("timing")


@contextmanager
def timed(label: str):
    """Chronomètre un bloc synchrone et logge la durée en ms (niveau DIAG)."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.diag("[timing] %s: %.0f ms", label, elapsed_ms)


@asynccontextmanager
async def atimed(label: str):
    """Chronomètre un bloc asynchrone et logge la durée en ms (niveau DIAG)."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.diag("[timing] %s: %.0f ms", label, elapsed_ms)


def timed_node(name: str, fn):
    """Wrappe un nœud LangGraph async pour logger sa durée d'exécution."""

    async def wrapper(state):
        start = time.perf_counter()
        try:
            return await fn(state)
        finally:
            elapsed_ms = (time.perf_counter() - start) * 1000
            logger.diag("[timing] node:%s: %.0f ms", name, elapsed_ms)

    # Préserve le nom pour le débogage / introspection LangGraph
    wrapper.__name__ = getattr(fn, "__name__", name)
    return wrapper


class LLMTimingCallback(BaseCallbackHandler):
    """Chronomètre chaque appel LLM (chat models), streaming inclus.

    ``on_chat_model_start`` mémorise l'instant de départ par ``run_id`` ;
    ``on_llm_end`` / ``on_llm_error`` logguent la durée écoulée.
    """

    def __init__(self) -> None:
        self._starts: dict = {}

    def on_chat_model_start(self, serialized, messages, *, run_id, **kwargs):
        model = _model_name(serialized, kwargs)
        self._starts[run_id] = (time.perf_counter(), model)
        approx_tokens = _estimate_prompt_tokens(messages)
        if approx_tokens >= _PROMPT_SIZE_WARN_TOKENS:
            # Un prompt géant explose la latence et risque un 429 (quota TPM Vertex) —
            # p. ex. un <result_sample> non plafonné (cf. audit c6.sql). Surface le tôt.
            logger.warning(
                "[llm:%s] prompt volumineux (~%d k tokens estimés) — risque de latence/429 TPM",
                model,
                approx_tokens // 1000,
            )

    def on_llm_end(self, response, *, run_id, **kwargs):
        self._log(run_id)

    def on_llm_error(self, error, *, run_id, **kwargs):
        self._log(run_id, suffix=" (erreur)")

    def _log(self, run_id, suffix: str = "") -> None:
        entry = self._starts.pop(run_id, None)
        if entry is None:
            return
        start, model = entry
        elapsed_ms = (time.perf_counter() - start) * 1000
        logger.diag("[timing] llm:%s: %.0f ms%s", model, elapsed_ms, suffix)


# Seuil d'alerte : au-delà, un prompt risque la latence et le 429 (quota TPM Vertex).
_PROMPT_SIZE_WARN_TOKENS = 200_000


def _estimate_prompt_tokens(messages) -> int:
    """Estimation grossière (≈ 4 caractères/token) de la taille d'un prompt de chat, à partir
    des messages passés à ``on_chat_model_start`` (liste de listes de messages). Sert au seul
    garde-fou de taille — pas de comptage exact requis."""
    total_chars = 0
    for group in messages or []:
        for m in group:
            content = getattr(m, "content", m)
            if isinstance(content, str):
                total_chars += len(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, str):
                        total_chars += len(part)
                    elif isinstance(part, dict):
                        total_chars += len(str(part.get("text", "")))
    return total_chars // 4


def _model_name(serialized, kwargs) -> str:
    """Best-effort extraction du nom de modèle pour étiqueter la durée LLM."""
    meta = kwargs.get("metadata") or {}
    name = meta.get("ls_model_name")
    if name:
        return name
    if isinstance(serialized, dict):
        params = serialized.get("kwargs") or {}
        return params.get("model") or serialized.get("name") or "llm"
    return "llm"
