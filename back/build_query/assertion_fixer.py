"""
Assertion fixer — auto-correction triggered after a bad_assertions verdict.

Reads the evaluator's explanation and rewrites unit_test_description so the
executor regenerates correct assertion SQL from the improved description.

Graph position:
  test_evaluator (bad_assertions) → assertion_fixer → executor → test_evaluator
"""

import json
import uuid

from langchain_core.messages import AIMessage

from build_query.examples_generator import retrieve_existing_tests
from build_query.state import QueryState
from utils.llm_errors import (
    format_vertex_permission_message,
    is_vertex_permission_error,
    normalize_llm_content,
)
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import find_current_test


async def fix_assertions(state: QueryState):
    """
    Auto-fix node for bad_assertions.

    Updates test_name, unit_test_description, and suggestions so that when
    the executor re-runs it regenerates assertion SQL that correctly validates
    the test scenario. Input data tables are preserved unchanged.
    """
    from storage.config import get_llm_model

    if state.get("error"):
        return {}

    eval_msgs = [
        m
        for m in state.get("messages", [])
        if get_message_type(m) == MsgType.EVALUATION
    ]
    if not eval_msgs:
        return {}

    latest_eval = eval_msgs[-1]
    evaluator_explanation = latest_eval.content
    eval_test_idx = latest_eval.additional_kwargs.get("test_index")

    session_id = state["session"]
    existing_tests = await retrieve_existing_tests(session_id, state)
    if not existing_tests:
        return {}

    current_test = find_current_test(
        existing_tests,
        eval_test_idx if eval_test_idx is not None else state.get("test_index"),
    )
    if not current_test:
        return {}

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    parent = state.get("user_message_id") or state.get("parent_message_id")

    llm = make_llm()
    prompt = _build_prompt(sql, current_test, evaluator_explanation)

    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
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


def _build_prompt(sql: str, test: dict, evaluator_explanation: str) -> str:
    test_info = {
        "test_name": test.get("test_name", ""),
        "unit_test_description": test.get("unit_test_description", ""),
        "tags": test.get("tags", []),
        "suggestions": test.get("suggestions", []),
        "assertion_results": [
            {
                "description": a.get("description"),
                "sql": a.get("sql"),
                "passed": a.get("passed"),
            }
            for a in (test.get("assertion_results") or [])
        ],
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

**Verdict de l'évaluateur :**
{evaluator_explanation}

**Mission :**
L'évaluateur a jugé ce test Insuffisant à cause des assertions (logique incorrecte,
conditions inversées, valeurs attendues erronées, etc.).

Corrige la description du test pour qu'elle décrive **précisément et correctement**
ce que le test doit vérifier. Cette description sera utilisée pour générer de nouvelles
assertions SQL — elle doit être suffisamment précise pour éviter les ambiguïtés.

Ne touche PAS aux données d'entrée (`data`).

Réponds avec un objet JSON strict :
```json
{{
  "test_name": "Nom court du scénario (3–6 mots)",
  "unit_test_description": "Vérifie que … (description précise, sans ambiguïté, actionnable)",
  "unit_test_build_reasoning": "Explication de la correction appliquée",
  "tags": ["Logique métier"],
  "suggestions": ["Vérifie que ...", "S'assure que ..."]
}}
```

Tags disponibles : Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance.
JSON uniquement, sans texte ni explication autour."""
