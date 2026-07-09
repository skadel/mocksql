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
from storage.config import get_language, output_language_directive
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


# Les mots d'action des « facts » et des fallbacks, par langue de sortie.
# L'anglais place l'action APRÈS le nom sans accord (« 2 tests generated ») :
# le suffixe pluriel ne s'applique qu'au français.
_ACTION_WORDS = {
    "fr": {
        "update": "modifié",
        "add": "ajouté",
        "rerun": "réévalué",
        "generate": "généré",
    },
    "en": {
        "update": "updated",
        "add": "added",
        "rerun": "re-evaluated",
        "generate": "generated",
    },
}


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

    # Mot d'action localisé : il part dans les « facts » donnés au LLM, qui recopie
    # leur langue — un fact français ferait basculer toute la réponse en français.
    words = _ACTION_WORDS[get_language()]
    agent_call = state.get("agent_tool_call")
    if agent_call == "update_test_data":
        action = words["update"]
    elif agent_call == "generate_test_data":
        action = words["add"]
    elif state.get("rerun_all_tests"):
        # SQL mis à jour ou réévaluation déclenchée depuis la bannière « fichier modifié » :
        # les tests existaient déjà, on les re-exécute / réévalue — surtout pas « généré ».
        action = words["rerun"]
    else:
        action = words["generate"]

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
    n = ctx["n_tests"] or 1
    plural = "s" if n > 1 else ""
    if get_language() == "en":
        if action == "generated":
            head = f"I generated {n} test{plural} for your query."
        elif action == "re-evaluated":
            head = f"I re-evaluated your test{plural}."
        else:
            head = f"I {action} your test."
        tail = (
            " Everything runs correctly."
            if ctx["exec_ok"]
            else " Take a look at the step details below."
        )
        return head + tail
    if action == "généré":
        head = f"J'ai généré {n} test{plural} pour ta requête."
    elif action == "réévalué":
        head = f"J'ai réévalué {'tes' if n > 1 else 'ton'} test{plural}."
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
        output_language_directive() + " ",
        "Tu es l'assistant MockSQL qui aide à tester des requêtes SQL. "
        "Tu viens de terminer une opération sur les tests d'un utilisateur. "
        "Réponds-lui directement, sur un ton sobre et factuel. "
        "Reprends EXACTEMENT l'action indiquée dans les faits ci-dessous "
        "(généré / ajouté / modifié / réévalué) — n'en invente pas une autre, "
        "et ne dis pas « généré » si l'action est une réévaluation. Annonce "
        "l'état d'exécution sans emphase : pas de « ravi de confirmer », de "
        "« parfaitement » ni d'autres superlatifs. Ne répète pas le SQL, ne "
        "mets pas de titre.",
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
    # Bookend de langue : le corps de ce prompt est rédigé en français, avec des
    # ancres d'action FR — « (généré / ajouté / modifié / réévalué) » — qui faisaient
    # parfois dériver la clôture vers le français MÊME en config anglaise (la directive
    # de tête perdait face aux ancres + aux faits). On répète la directive en DERNIER
    # (recency) pour que l'ultime signal avant génération soit la langue de sortie.
    system_lines.append(output_language_directive())
    system = SystemMessage(content=" ".join(system_lines))
    n_tests = ctx["n_tests"] or 1
    plural = "s" if n_tests > 1 else ""
    # Facts localisés : le LLM recopie la langue des facts plus sûrement que celle
    # de la directive — des facts français font basculer la réponse en français.
    en = get_language() == "en"
    if en:
        facts = [
            f"Action performed: {n_tests} test{plural} {ctx['action']}.",
            f"DuckDB execution: {'OK' if ctx['exec_ok'] else 'needs review'}.",
        ]
        if ctx["scenario"]:
            facts.append(f"Target scenario: {ctx['scenario']}")
        if ctx["verdict_line"]:
            facts.append(f"Evaluation verdict: {ctx['verdict_line']}")
        if gap_analysis:
            facts.append(f"Coverage analysis (identified gaps): {gap_analysis}")
    else:
        facts = [
            f"Action réalisée : {n_tests} test{plural} {ctx['action']}{plural}.",
            f"Exécution DuckDB : {'OK' if ctx['exec_ok'] else 'à vérifier'}.",
        ]
        if ctx["scenario"]:
            facts.append(f"Scénario visé : {ctx['scenario']}")
        if ctx["verdict_line"]:
            facts.append(f"Verdict d'évaluation : {ctx['verdict_line']}")
        if gap_analysis:
            facts.append(f"Analyse de couverture (manques identifiés) : {gap_analysis}")
    human = HumanMessage(content="\n".join(facts))

    if not gap_analysis:
        panel_pointer = ""
    elif en:
        panel_pointer = " I also dropped test suggestions in the Suggestions panel to strengthen coverage."
    else:
        panel_pointer = " J'ai aussi déposé des suggestions pour renforcer la couverture dans le panneau Suggestions."
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
