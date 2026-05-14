import json
import uuid
from pydantic import BaseModel, Field

from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

from build_query.examples_generator import retrieve_existing_tests
from build_query.prompt_tools import _format_profile_block
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type


# 1. Structure Pydantic (avec Chain of Thought)
class TestSuggestionsOutput(BaseModel):
    analyse_des_manques: str = Field(
        description="Brève analyse des cas limites (edge-cases) ou règles métier qui manquent dans les tests actuels."
    )
    suggestions: list[str] = Field(
        description="Liste exacte de 3 suggestions de cas de tests d'une phrase commençant par un verbe.",
        min_length=1,
        max_length=3,
    )


async def generate_suggestions(state: QueryState):
    """Génère des suggestions de cas de tests non encore couverts et les émet comme message SUGGESTIONS."""

    # --- 1. Préparation des données ---
    test_cases = await retrieve_existing_tests(state["session"], state)
    if not test_cases:
        return {}

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    dialect = state.get("dialect", "bigquery")
    profile = state.get("profile")
    used_columns = state.get("used_columns") or []
    profile_block = _format_profile_block(profile, used_columns) if profile else ""

    raw_instructions = (state.get("agent_tool_args") or {}).get(
        "instructions", ""
    ) or ""
    if isinstance(raw_instructions, list):
        raw_instructions = " ".join(str(x) for x in raw_instructions if x)
    instructions = raw_instructions.strip()

    existing = "\n".join(
        f"- {tc.get('test_name', '')}: {tc.get('unit_test_description', '')}"
        for tc in test_cases
    )

    # Formatage propre avec balises XML pour le prompt
    instruction_block = (
        "<instructions_specifiques>\n{}\n</instructions_specifiques>"
        if instructions
        else ""
    )
    existing_tests_block = (
        existing if existing else "Aucun test existant pour le moment."
    )

    # --- 2. Construction du Prompt ---
    prompt_template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """Tu es un expert en assurance qualité et en tests unitaires SQL (dialecte: {dialect}).
Ton objectif est de fournir des idées de cas de tests pertinents et non couverts pour garantir la robustesse de la requête.""",
            ),
            (
                "user",
                """Voici la requête SQL à analyser :
<sql>
{sql}
</sql>

{instruction_block}

Voici les tests déjà générés (à ne pas reproduire) :
<tests_existants>
{existing_tests_block}
</tests_existants>

{profile_section}

Génère exactement 3 nouvelles suggestions de cas de tests non encore couverts.
Chaque suggestion doit être une assertion actionnable courte commençant par un verbe (ex : "Vérifie que...", "S'assure que...", "Teste le comportement...").
Si un profil statistique est fourni, au moins une suggestion doit cibler un cas qui existe réellement dans les données — formule-la ainsi : "[PROD] Vérifie que..." pour la distinguer des suggestions génériques.""",
            ),
        ]
    )

    # --- 3. Exécution avec LangChain (Structured Output) ---
    llm = make_llm()
    structured_llm = llm.with_structured_output(TestSuggestionsOutput)
    chain = prompt_template | structured_llm

    profile_section = (
        f"Profil statistique réel des données (distributions mesurées en production) :\n{profile_block}"
        if profile_block
        else ""
    )

    try:
        result = await chain.ainvoke(
            {
                "dialect": dialect,
                "sql": sql,
                "instruction_block": instruction_block,
                "existing_tests_block": existing_tests_block,
                "profile_section": profile_section,
            }
        )
        suggestions = result.suggestions[:3]

    except Exception as e:
        print(f"Erreur LLM lors de la génération des suggestions: {e}")
        # Fallback: collect from existing test.suggestions fields
        seen = set()
        suggestions = []
        for tc in test_cases:
            for s in tc.get("suggestions") or []:
                if s and s not in seen:
                    seen.add(s)
                    suggestions.append(s)
        suggestions = suggestions[:3]

    if not suggestions:
        return {}

    # --- 4. Détermination du parent_id ---
    messages = state.get("messages", [])
    parent_id = state.get("parent_message_id") or state.get("user_message_id")
    for m in reversed(messages):
        if get_message_type(m) == MsgType.EVALUATION:
            parent_id = m.id
            break
        if get_message_type(m) == MsgType.RESULTS:
            parent_id = m.id

    # --- 5. Retour au state LangGraph ---
    return {
        "messages": [
            AIMessage(
                content=json.dumps(suggestions, ensure_ascii=False),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.SUGGESTIONS,
                    "parent": parent_id,
                    "request_id": state.get("request_id"),
                    "profile_available": bool(profile_block),
                },
            )
        ]
    }
