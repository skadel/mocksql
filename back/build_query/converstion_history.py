import json
from typing import List, Tuple, Optional, Union

from langchain_core.messages import BaseMessage, AIMessage, HumanMessage

from utils.msg_types import MsgType
from utils.saver import get_message_type

# ----------------------------------------------------------------------------
# 1. Configuration
# ----------------------------------------------------------------------------

AGENT_NAMES = {
    MsgType.PROVIDED_SQL: "Code SQL fourni par l'utilisateur",
    MsgType.SQL: "Code SQL généré et compilé.",
    MsgType.SQL_UPDATE: "Code SQL modifié par l'utilisateur",
    MsgType.EXAMPLES: "Tests unitaires pour tester le code SQL",
    MsgType.EXAMPLES_INSTRUCTION: "Demande de modification des tests unitaires",
    MsgType.RESULTS: "Résultat de l'exécution des tests unitaires",
    MsgType.ROUTE: "Routage de la question de l'utilisateur",
    MsgType.OTHER: "\n",
    MsgType.ERROR: "Erreur rencontrée",
    MsgType.ERROR_SQL: "Code SQL causant une erreur",
}

# Balises (pseudo‑XML) à entourer autour du contenu pour certains types.
TAG_BY_TYPE = {
    MsgType.SQL: "sql",
    MsgType.PROVIDED_SQL: "sql",
    MsgType.REASONING: "reasoning",
    MsgType.RESULTS: "results",
    MsgType.EXAMPLES: "examples",
    MsgType.EXAMPLES_INSTRUCTION: "examples_update",
}

# Constants for generator output format
_UNIT_TEST_KEYS = (
    "unit_test_description",
    "unit_test_build_reasoning",
    "tags",
    "suggestions",
    "data",
)
_GENERATOR_INSTRUCTION_TYPES = frozenset({MsgType.QUERY, MsgType.EXAMPLES_INSTRUCTION})
_SYNTHETIC_NOMINAL_HUMAN = (
    "Génère des données de test qui répondent à un cas standard "
    "et donnent un résultat non vide pour vérifier le cas nominal."
)


# ----------------------------------------------------------------------------
# 2. Helpers
# ----------------------------------------------------------------------------


def wrap_with_tag(tag: str, body: str) -> str:
    """Encapsule le corps dans <tag> ... </tag>."""
    return f"<{tag}>\n{body}\n</{tag}>"


def safe_process_sql(sql_text: str, dialect: str, remove_db: bool = True) -> str:
    """Stub pour le nettoyage/formatage du SQL : à adapter."""
    return sql_text


# ----------------------------------------------------------------------------
# 3. Formatage d'un seul message (sans notion de numérotation)
# ----------------------------------------------------------------------------


def format_single_message(
    message,
    dialect: str,
    process_sql: bool = True,
    prettify_reasoning: bool = True,
) -> Tuple[str, str, str]:
    """Retourne (type, rendu_complet, corps_seul)."""
    msg_type = get_message_type(message)
    if msg_type not in AGENT_NAMES:
        raise ValueError(f"Type d'agent non pris en charge : {msg_type}")

    if msg_type == MsgType.RESULTS:
        content_body = json.dumps(
            json.loads(message.content), indent=2, ensure_ascii=False
        )
    elif msg_type == MsgType.EXAMPLES:
        content_body = json.dumps(
            {"unit_tests": json.loads(message.content)}, indent=2, ensure_ascii=False
        )
    elif msg_type in (MsgType.SQL, MsgType.PROVIDED_SQL):
        raw = (
            safe_process_sql(message.content, dialect, remove_db=True)
            if process_sql
            else message.content
        )
        content_body = prettify_sql_steps(raw)
    else:
        content_body = message.content

    tag = TAG_BY_TYPE.get(msg_type)
    if tag:
        content_body = wrap_with_tag(tag, content_body)

    if msg_type == MsgType.QUERY:
        formatted = content_body
    else:
        formatted = (
            f"<{AGENT_NAMES[msg_type]}>\n{content_body}\n</{AGENT_NAMES[msg_type]}>"
        )

    return msg_type, formatted, content_body


# ----------------------------------------------------------------------------
# 4. Formatage de tout l'historique, avec numérotation des questions
# ----------------------------------------------------------------------------


def format_history(
    history: List[BaseMessage],
    dialect: str,
    current_agent: Optional[str] = None,
    output_format: str = "message",
    process_sql: bool = True,
    excluded_agents: Optional[List[str]] = None,
    prettify_reasoning: bool = True,
) -> Union[List, str]:
    """Formate l'historique sous forme LangChain ou en texte linéaire.

    output_format:
      'message'   — alternance HumanMessage/AIMessage par agent courant.
      'text'      — texte linéaire numéroté.
      'generator' — alternance instruction/génération pour le générateur de tests unitaires ;
                    les résultats d'exécution sont préfixés au HumanMessage suivant.
    """
    if output_format not in ("message", "text", "generator"):
        raise ValueError("output_format doit être 'message', 'text' ou 'generator'")
    if current_agent and current_agent not in AGENT_NAMES:
        raise ValueError(f"Type d'agent courant non reconnu : {current_agent}")

    excluded_agents = excluded_agents or []

    # ------------------------------------------------------------------
    # 4.a — Sortie « message » (objets LangChain)
    # ------------------------------------------------------------------
    if output_format == "message":
        formatted_history: List[Union[AIMessage, HumanMessage]] = []
        non_current_buffer: List[str] = []

        for msg in history:
            msg_type, formatted_content, _ = format_single_message(
                msg, dialect, process_sql, prettify_reasoning
            )
            if msg_type in excluded_agents:
                continue

            if msg_type == current_agent:
                if non_current_buffer:
                    formatted_history.append(
                        HumanMessage(content="".join(non_current_buffer))
                    )
                    non_current_buffer = []
                formatted_history.append(AIMessage(content=formatted_content))
            else:
                non_current_buffer.append(formatted_content)

        if non_current_buffer:
            formatted_history.append(
                HumanMessage(
                    content="voici les échanges que j'ai eu avec plusieurs professionnels :\n\n"
                    + "\n\n".join(non_current_buffer)
                )
            )
        return formatted_history

    # ------------------------------------------------------------------
    # 4.b — Sortie texte linéaire
    # ------------------------------------------------------------------
    if output_format == "text":
        question_index = 0
        conversation_lines: List[str] = []
        for msg in history:
            msg_type, formatted_content, _ = format_single_message(
                msg, dialect, process_sql, prettify_reasoning
            )
            if msg_type in excluded_agents:
                continue

            if msg_type == MsgType.QUERY:
                question_index += 1
                formatted_content = f"Question {question_index} :\n{msg.content}"

            conversation_lines.append(formatted_content)

        return "\n\n".join(conversation_lines)

    # ------------------------------------------------------------------
    # 4.c — Sortie orientée générateur de tests unitaires
    # ------------------------------------------------------------------
    result: List[Union[AIMessage, HumanMessage]] = []
    pending_results_text: Optional[str] = None
    examples_emitted = False

    for msg in history:
        msg_type = get_message_type(msg)
        if msg_type in excluded_agents:
            continue

        if msg_type in _GENERATOR_INSTRUCTION_TYPES:
            parts: List[str] = []
            if pending_results_text:
                parts.append(
                    "Voici ce que j'ai obtenu en exécutant ma requête SQL sur les données générées :\n"
                    + pending_results_text
                )
                pending_results_text = None
            instruction = (
                wrap_with_tag("demande de modification/rajout de test", msg.content)
                if msg_type == MsgType.EXAMPLES_INSTRUCTION
                else msg.content
            )
            parts.append(instruction)
            result.append(HumanMessage(content="\n\n".join(parts)))
            examples_emitted = False

        elif msg_type == MsgType.EXAMPLES:
            if not result:
                result.append(HumanMessage(content=_SYNTHETIC_NOMINAL_HUMAN))
            tests = _filter_to_generated(json.loads(msg.content), msg)
            result.append(AIMessage(content=_format_unit_tests_for_generator(tests)))
            examples_emitted = True

        elif msg_type == MsgType.RESULTS:
            results_data = _filter_to_generated(json.loads(msg.content), msg)
            if not examples_emitted:
                if not result:
                    result.append(HumanMessage(content=_SYNTHETIC_NOMINAL_HUMAN))
                result.append(
                    AIMessage(content=_format_unit_tests_for_generator(results_data))
                )
                examples_emitted = True
            pending_results_text = _format_execution_results(results_data)

    return result


# ----------------------------------------------------------------------------
# 5. Helpers du format générateur
# ----------------------------------------------------------------------------


def _filter_to_generated(tests: list, msg) -> list:
    """Filters the test list to the single test linked to this message via generated_test_index."""
    idx = msg.additional_kwargs.get("generated_test_index")
    if idx is None:
        return tests
    filtered = [t for t in tests if str(t.get("test_index")) == str(idx)]
    return filtered if filtered else tests


def _format_unit_tests_for_generator(tests: list) -> str:
    """Sérialise les tests en JSON ne conservant que les champs de génération.

    Un seul test → objet JSON ; plusieurs → tableau JSON.
    Les champs runtime (results_json, status, test_index) sont exclus.
    """
    cleaned = [{k: t[k] for k in _UNIT_TEST_KEYS if k in t} for t in tests]
    if len(cleaned) == 1:
        return json.dumps(cleaned[0], indent=2, ensure_ascii=False)
    return json.dumps(cleaned, indent=2, ensure_ascii=False)


def _format_execution_results(results_data: list) -> str:
    """Formate les résultats d'exécution pour le préfixe du prochain HumanMessage."""
    parts = []
    for r in results_data:
        idx = r.get("test_index", 0)
        desc = r.get("unit_test_description", "")
        raw_res = r.get("results_json", "[]")
        try:
            res_str = json.dumps(json.loads(raw_res), indent=2, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            res_str = raw_res
        parts.append(f"Test {idx} ({desc}) :\n{res_str}")
    return "\n\n".join(parts)


# ----------------------------------------------------------------------------
# 6. Utilitaires SQL
# ----------------------------------------------------------------------------


def prettify_sql_steps(raw: str) -> str:
    """Transforme un JSON de steps SQL en texte lisible pour le LLM.

    Entrée : [{"name": "...", "code": "...", "sources": [...], ...}, ...]
    Retourne le texte brut si ce n'est pas du JSON de steps valide.
    """
    try:
        steps = json.loads(raw)
        if not (isinstance(steps, list) and steps and "code" in steps[0]):
            return raw
    except Exception:
        return raw

    ctes = [s for s in steps if s.get("name") != "final_query"]
    final = next((s for s in steps if s.get("name") == "final_query"), None)

    parts = []

    for step in ctes:
        name = step.get("name", "")
        code = step.get("code", "").strip()
        sources = step.get("sources", [])
        header = f"-- CTE : {name}"
        if sources:
            tables = ", ".join(
                f"{s.get('database', '')}.{s.get('table', '')}" for s in sources
            )
            header += f"\n-- Tables utilisées : {tables}"
        parts.append(f"{header}\n{name} AS (\n{code}\n)")

    if final:
        code = final.get("code", "").strip()
        sources = final.get("sources", [])
        header = "-- Requête finale"
        if sources:
            tables = ", ".join(
                f"{s.get('database', '')}.{s.get('table', '')}" for s in sources
            )
            header += f"\n-- Tables utilisées : {tables}"
        if parts:
            parts.insert(0, "WITH")
        parts.append(f"{header}\n{code}")

    return "\n\n".join(parts)
