import json
import uuid

from langchain_core.messages import AIMessage

from build_query.state import QueryState
from utils.llm_errors import is_vertex_permission_error, normalize_llm_content
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import build_test_detail, find_current_test


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

    current_test = find_current_test(all_tests, state.get("test_index"))
    if current_test is None:
        return {}

    test_detail = build_test_detail(current_test)

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

    llm = make_llm()

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
    # Skip auto-retry when in assertion-only mode — the input data hasn't changed
    should_retry = (
        is_insuffisant and gen_retries > 0 and not state.get("assertion_only")
    )

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
