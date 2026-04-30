"""
Assertion modifier agent.

Handles user requests to modify or refine the *assertion* of an existing test
(description, name, tags, suggestions) WITHOUT regenerating the input data tables.

Graph position:
  routing → assertion_modifier → executor → test_evaluator → history_saver

The executor re-runs the query on the unchanged data so the evaluator produces
a fresh verdict based on the updated assertion text.
"""

import json
import uuid

from langchain_core.messages import AIMessage

from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_errors import is_vertex_permission_error, normalize_llm_content
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.test_utils import find_current_test


async def modify_assertions(state: QueryState):
    """
    Assertion-only modification node.

    Keeps input data (data tables) unchanged.
    Updates: test_name, unit_test_description, unit_test_build_reasoning, tags, suggestions.
    Emits an EXAMPLES message so the executor can re-run and the evaluator
    produces a fresh verdict on the updated assertion.
    """
    from utils.llm_errors import format_vertex_permission_message
    from storage.config import get_llm_model

    if state.get("error"):
        return {}

    session_id = state["session"]
    existing_tests = await retrieve_existing_tests(session_id, state)
    if not existing_tests:
        return {}

    current_test = find_current_test(existing_tests, state.get("test_index"))
    if not current_test:
        return {}

    user_instruction = state.get("input", "").strip()
    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    parent = state.get("user_message_id") or state.get("parent_message_id")

    prompt = _build_prompt(sql, current_test, user_instruction)
    llm = make_llm()

    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
        # Strip markdown code fences if present
        if "```" in content:
            for part in content.split("```"):
                stripped = part.lstrip("json").strip()
                if stripped.startswith("{"):
                    content = stripped
                    break
        updated_fields = json.loads(content.strip())
    except Exception as exc:
        if is_vertex_permission_error(exc):
            error_msg = format_vertex_permission_message(get_llm_model())
            return {
                "messages": [
                    AIMessage(
                        content=error_msg,
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.ERROR,
                            "parent": parent,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "error": "llm_permission_denied",
            }
        return {}

    # Merge updated assertion metadata; preserve input data exactly
    updated_test = {
        **current_test,
        "test_name": updated_fields.get("test_name", current_test.get("test_name", "")),
        "unit_test_description": updated_fields.get(
            "unit_test_description", current_test.get("unit_test_description", "")
        ),
        "unit_test_build_reasoning": updated_fields.get(
            "unit_test_build_reasoning",
            current_test.get("unit_test_build_reasoning", ""),
        ),
        "tags": updated_fields.get("tags", current_test.get("tags", [])),
        "suggestions": updated_fields.get(
            "suggestions", current_test.get("suggestions", [])
        ),
    }

    return {
        "examples": [
            AIMessage(
                content=json.dumps(updated_test),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EXAMPLES,
                    "parent": parent,
                    "request_id": state.get("request_id"),
                    "generated_test_index": updated_test.get("test_index"),
                    "assertion_only": True,
                },
            )
        ]
    }


def _build_prompt(sql: str, test: dict, user_instruction: str) -> str:
    test_info = {
        "test_name": test.get("test_name", ""),
        "unit_test_description": test.get("unit_test_description", ""),
        "tags": test.get("tags", []),
        "suggestions": test.get("suggestions", []),
        "input_tables": list(test.get("data", {}).keys()),
    }

    return f"""Tu es un expert en qualité de tests SQL unitaires.

**Requête SQL testée :**
```sql
{sql}
```

**Test actuel :**
```json
{json.dumps(test_info, ensure_ascii=False, indent=2)}
```

**Instruction utilisateur :**
{user_instruction}

**Mission :**
Modifie uniquement les métadonnées d'assertion du test selon l'instruction.
Ne touche PAS aux données d'entrée (`data`) — elles restent inchangées.

Réponds avec un objet JSON strict contenant exactement ces champs :
```json
{{
  "test_name": "Nom court du scénario (3–6 mots)",
  "unit_test_description": "Vérifie que … (assertion courte et actionnable)",
  "unit_test_build_reasoning": "Explication de la modification appliquée",
  "tags": ["Logique métier"],
  "suggestions": ["Vérifie que ...", "S'assure que ..."]
}}
```

Tags disponibles : Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance.
JSON uniquement, sans texte ni explication autour."""
