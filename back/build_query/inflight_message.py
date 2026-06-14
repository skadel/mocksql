"""Traitement d'un message saisi PENDANT qu'une génération est déjà en cours.

Distingue deux intentions (réutilise le routeur d'intention `make_routing_prompt`) :

- **instruction** : l'utilisateur veut influencer la génération en cours
  (« couvre aussi le cas NULL », « ajoute un client sans commande ») → mise en file
  (cf. ``pending_instructions``), consultée à chaud par le run en vol.
- **question** : l'utilisateur pose une question ou réfléchit à voix haute
  (« pourquoi j'ai eu ce résultat ? ») → répondue **en direct** par un appel LLM
  indépendant, **read-only** sur l'état persisté (SQL + résultats déjà en base). Elle
  ne touche pas le run en vol (pas de mutation du graph → pas de race) et est persistée
  dans le fil pour être réaffichée au rechargement.

Le routeur renvoie ``generator`` (l'utilisateur demande à MockSQL d'agir → instruction)
ou ``other`` (question / réflexion → question). On mappe directement ces deux routes.
"""

import json
import logging
import uuid

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.output_parsers import JsonOutputParser

from build_query.prompt_tools import build_other_prompt, make_routing_prompt
from common_vars import COMMON_HISTORY_TABLE_NAME
from models.database import execute
from models.env_variables import DB_MODE
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import common_history_retriever

logger = logging.getLogger(__name__)

_llm = make_llm()

# Types d'historique pertinents pour classer et pour répondre à une question :
# le SQL testé, les questions/réponses passées et les résultats d'exécution.
_HISTORY_TYPES = [
    MsgType.SQL,
    MsgType.QUERY,
    MsgType.RESULTS,
    MsgType.OTHER,
    MsgType.REASONING,
]


async def classify_inflight_message(session: str, text: str, dialect: str) -> str:
    """Classe un message en vol → ``"instruction"`` ou ``"question"``.

    Réutilise le routeur d'intention : ``generator`` → instruction (MockSQL doit agir
    sur la génération), ``other`` → question (explication / réflexion). En cas d'échec
    LLM on retombe sur ``"instruction"`` — c'est le comportement historique, on ne perd
    jamais une consigne dans le vide.
    """
    text = (text or "").strip()
    if not text:
        return "instruction"
    try:
        history = await common_history_retriever(session, msg_type=_HISTORY_TYPES)
    except Exception as exc:
        logger.diag("[inflight] history retrieval failed → instruction: %s", exc)
        history = []
    prompt = make_routing_prompt(dialect=dialect, history=history)
    chain = prompt | _llm | JsonOutputParser()
    try:
        result = await chain.ainvoke({"input": text})
        route = result.get("route", "generator")
    except Exception as exc:
        logger.diag("[inflight] classify exception → instruction: %s", exc)
        return "instruction"
    kind = "question" if route == "other" else "instruction"
    logger.diag("[inflight] classify route=%s → %s (text=%r)", route, kind, text[:80])
    return kind


async def answer_inflight_question(
    session: str,
    text: str,
    dialect: str,
    parent_message_id: str | None,
) -> dict:
    """Répond en direct à une question posée pendant la génération.

    Appel LLM indépendant (read-only) sur l'historique persisté ; persiste la question
    (USER/QUERY) puis la réponse (BOT/OTHER) dans le fil et renvoie les deux messages
    (format LangChain ``.dict()``) pour insertion immédiate côté front.
    """
    try:
        history = await common_history_retriever(session, msg_type=_HISTORY_TYPES)
    except Exception:
        history = []

    prompt = build_other_prompt(text, dialect, history)
    chain = prompt | _llm
    result = await chain.ainvoke({"descriptions": "[]"})
    answer_text = getattr(result, "content", "") or ""

    request_id = str(uuid.uuid4())
    user_msg = HumanMessage(
        content=text,
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.QUERY,
            "parent": parent_message_id,
            "request_id": request_id,
        },
    )
    # La réponse chaîne SOUS la question (pas en frère) — même raison que le nœud
    # `other` : get_messages_history remonte la chaîne de parents.
    answer_msg = AIMessage(
        content=answer_text,
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.OTHER,
            "parent": user_msg.id,
            "request_id": request_id,
        },
    )

    await _persist_messages(session, [user_msg, answer_msg])

    return {
        "question": user_msg.dict(),
        "answer": answer_msg.dict(),
    }


async def _persist_messages(session: str, messages: list[BaseMessage]) -> None:
    """Insère des messages dans l'historique commun (même schéma que history_saver)."""
    use_jsonb = DB_MODE in ("postgres", "cloudsql")
    cast_clause = "::jsonb" if use_jsonb else ""
    sql_stmt = f"""
    INSERT INTO {COMMON_HISTORY_TABLE_NAME} (session_id, data, type)
    VALUES ($1, $2{cast_clause}, $3)
    """
    for msg in messages:
        data_json = json.dumps(msg.dict())
        await execute(sql_stmt, session, data_json, msg.type)
