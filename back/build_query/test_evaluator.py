import json
import uuid

from langchain_core.messages import AIMessage
from langchain_google_genai import ChatGoogleGenerativeAI

from build_query.state import QueryState
from models.env_variables import GENERATOR_MODEL
from utils.llm_errors import is_vertex_permission_error, normalize_llm_content
from utils.msg_types import MsgType
from utils.saver import get_message_type


async def evaluate_tests(state: QueryState):
    """
    Évalue la qualité de la suite de tests unitaires après exécution et produit
    un verdict + commentaire via le LLM.
    """
    if state.get("error"):
        return {}

    results_msgs = [
        m for m in state.get("messages", []) if get_message_type(m) == MsgType.RESULTS
    ]
    if not results_msgs:
        return {}

    last_results = results_msgs[-1]
    try:
        all_tests = json.loads(last_results.content)
    except Exception:
        return {}

    if not isinstance(all_tests, list):
        all_tests = [all_tests]

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()

    # Identify the current test: prefer the one matching state["test_index"],
    # otherwise fall back to the last element in the list.
    current_test_index = state.get("test_index")
    current_test = None
    if current_test_index is not None:
        current_test = next(
            (t for t in all_tests if t.get("test_index") == current_test_index),
            None,
        )
    if current_test is None:
        if not all_tests:
            return {}
        current_test = all_tests[-1]

    try:
        result_rows = json.loads(current_test.get("results_json") or "[]")
    except Exception:
        result_rows = []

    input_data = current_test.get("data", {})

    test_detail = {
        "description": current_test.get("unit_test_description", ""),
        "reasoning": current_test.get("unit_test_build_reasoning", ""),
        "tags": current_test.get("tags", []),
        "input_data": input_data,
        "status": current_test.get("status"),
        "row_count": len(result_rows),
        "result_rows": result_rows[:20],  # cap to avoid bloating the prompt
    }
    if current_test.get("error"):
        test_detail["error"] = current_test["error"]
    if current_test.get("failing_cte"):
        test_detail["failing_cte"] = current_test["failing_cte"]

    prompt = f"""Tu es un expert en qualité de tests SQL unitaires.

Tu reçois une requête SQL et les détails d'un test unitaire qui vient d'être généré et exécuté.

**Requête SQL :**
```sql
{sql}
```

**Test évalué :**
{json.dumps(test_detail, ensure_ascii=False, indent=2)}

**Mission :**
Réponds en une seule phrase ultra-concise (max 20 mots) en français.
Format strict : `**Verdict** — <ce que le test couvre> + <données/résultat valides ou problème>.`
Verdicts possibles : **Excellent**, **Bon**, **Insuffisant**.
Exemple : `**Bon** — Couvre la jointure sur clé manquante. Données et résultat valides.`"""

    llm = ChatGoogleGenerativeAI(model=GENERATOR_MODEL, vertexai=True, temperature=0)

    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
    except Exception as exc:
        if is_vertex_permission_error(exc):
            return {}
        raise

    eval_test_index = current_test.get("test_index")
    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 2
    )
    is_insuffisant = "Insuffisant" in content
    should_retry = is_insuffisant and gen_retries > 0

    return {
        "messages": [
            AIMessage(
                content=content,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EVALUATION,
                    "parent": last_results.id,
                    "request_id": state.get("request_id"),
                    "test_index": eval_test_index,
                },
            )
        ],
        "status": "empty_results" if should_retry else "complete",
        **({"gen_retries": gen_retries - 1} if should_retry else {}),
    }
