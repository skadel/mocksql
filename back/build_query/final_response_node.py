"""Nœud de clôture du flux : l'agent répond une dernière fois en langage naturel.

À la fin d'une requête (génération initiale, ajout ou modification d'un test),
ce nœud émet un court message — la « voix » de l'agent conversationnel — qui
résume ce qui vient d'être fait (« J'ai créé / modifié ton test … tout passe »).
Côté frontend, ce message est la partie *visible* de la bulle unique regroupant
toute la requête ; les étapes intermédiaires sont repliées.
"""

import logging
import uuid

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type

logger = logging.getLogger(__name__)


def _coerce_text(content) -> str:
    """Gemini bind_tools peut renvoyer le contenu sous forme de liste de parts."""
    if isinstance(content, list):
        return "".join(
            part.get("text", "")
            for part in content
            if isinstance(part, dict) and part.get("type") == "text"
        )
    return content if isinstance(content, str) else ""


def _collect_run_context(state: QueryState) -> dict:
    """Reconstitue ce qui s'est passé pendant la requête à partir des messages émis."""
    request_id = state.get("request_id")
    messages = state.get("messages", []) or []

    scenario = ""
    verdict_line = ""
    n_tests = 0
    for msg in messages:
        # On ne garde que les messages de la requête courante quand l'info existe.
        msg_request = (getattr(msg, "additional_kwargs", {}) or {}).get("request_id")
        if request_id and msg_request and msg_request != request_id:
            continue
        mtype = get_message_type(msg)
        if mtype == MsgType.GENERATE_TEST_SCENARIO:
            scenario = _coerce_text(msg.content)
        elif mtype == MsgType.EVALUATION:
            verdict_line = _coerce_text(msg.content)
        elif mtype == MsgType.EXAMPLES:
            n_tests += 1

    agent_call = state.get("agent_tool_call")
    if agent_call == "update_test_data":
        action = "modifié"
    elif agent_call == "generate_test_data":
        action = "ajouté"
    else:
        action = "généré"

    status = state.get("status")
    feedback = state.get("evaluation_feedback")
    exec_ok = status == "complete" and not feedback

    return {
        "action": action,
        "scenario": scenario.strip(),
        "verdict_line": verdict_line.strip(),
        "n_tests": n_tests,
        "exec_ok": exec_ok,
        "feedback": feedback,
    }


def _fallback_message(ctx: dict) -> str:
    """Message templaté utilisé si l'appel LLM échoue."""
    action = ctx["action"]
    if ctx["action"] == "généré":
        n = ctx["n_tests"] or 1
        head = f"J'ai généré {n} test{'s' if n > 1 else ''} pour ta requête."
    else:
        head = f"J'ai {action} ton test."
    tail = (
        " Tout s'exécute correctement."
        if ctx["exec_ok"]
        else " Jette un œil au détail des étapes ci-dessous."
    )
    return head + tail


# Types émis pendant la requête mais NON persistés dans l'historique
# (cf. history_saver) : si final_response s'y rattache, le parent est orphelin
# au rechargement → chaîne cassée. On les saute pour viser un message persisté.
_NON_PERSISTED_TYPES = {MsgType.SUGGESTIONS, MsgType.EXAMPLES}


def _parent_for_summary(state: QueryState) -> str | None:
    """Rattache le résumé au dernier message bot *persisté* de la requête courante."""
    request_id = state.get("request_id")
    messages = state.get("messages", []) or []
    for msg in reversed(messages):
        kwargs = getattr(msg, "additional_kwargs", {}) or {}
        if kwargs.get("type") in _NON_PERSISTED_TYPES:
            continue
        msg_request = kwargs.get("request_id")
        if request_id and msg_request == request_id:
            return msg.id
    return state.get("parent_message_id")


async def final_response(state: QueryState):
    """Émet un court message de clôture en langage naturel."""
    ctx = _collect_run_context(state)
    parent_id = _parent_for_summary(state)

    # Analyse des manques produite par suggestions_generator (1ʳᵉ génération) : on la tisse
    # dans le message de clôture pour expliquer ce qui n'est pas couvert et renvoyer au
    # panneau Suggestions. Absente sur les éditions (pas de suggestions auto) → bloc ignoré.
    gap_analysis = (state.get("coverage_gap_analysis") or "").strip()

    system_lines = [
        "Tu es l'assistant MockSQL qui aide à tester des requêtes SQL. "
        "Tu viens de finir une opération sur les tests d'un utilisateur. "
        "Réponds-lui directement, en français, ton chaleureux et professionnel. "
        "Dis ce que tu as fait (créé / modifié / généré le(s) test(s)) et si tout "
        "s'exécute bien. Ne répète pas le SQL, ne mets pas de titre.",
    ]
    if gap_analysis:
        system_lines.append(
            "Une analyse de couverture est fournie : après avoir annoncé les tests, "
            "ajoute 1 à 2 phrases qui résument en langage métier ce qui n'est pas encore "
            "couvert (reformule l'analyse, ne la recopie pas mot pour mot), puis précise "
            "que tu as déposé des suggestions de tests pour renforcer la couverture dans "
            "le panneau Suggestions. Reste sous 4 phrases au total. Ne liste pas les "
            "suggestions une par une."
        )
    else:
        system_lines.append(
            "Réponds en 1 à 2 phrases courtes. Ne liste pas de suggestions."
        )
    system = SystemMessage(content=" ".join(system_lines))
    facts = [
        f"Action réalisée : test {ctx['action']}.",
        f"Nombre de tests concernés : {ctx['n_tests'] or 1}.",
        f"Exécution DuckDB : {'OK' if ctx['exec_ok'] else 'à vérifier'}.",
    ]
    if ctx["scenario"]:
        facts.append(f"Scénario visé : {ctx['scenario']}")
    if ctx["verdict_line"]:
        facts.append(f"Verdict d'évaluation : {ctx['verdict_line']}")
    if gap_analysis:
        facts.append(f"Analyse de couverture (manques identifiés) : {gap_analysis}")
    human = HumanMessage(content="\n".join(facts))

    panel_pointer = (
        " J'ai aussi déposé des suggestions pour renforcer la couverture dans le panneau Suggestions."
        if gap_analysis
        else ""
    )
    try:
        llm = make_llm()
        result = await llm.ainvoke([system, human])
        text = _coerce_text(result.content).strip()
        if not text:
            text = _fallback_message(ctx) + panel_pointer
    except Exception as exc:  # noqa: BLE001 — clôture best-effort, jamais bloquante
        logger.diag("[final_response] LLM indisponible, fallback templaté : %s", exc)
        text = _fallback_message(ctx) + panel_pointer

    return {
        "messages": [
            AIMessage(
                content=text,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.FINAL_RESPONSE,
                    "parent": parent_id,
                    "request_id": state.get("request_id"),
                },
            )
        ]
    }
