import json
import logging
import uuid
from typing import Literal, Optional

from langchain_core.messages import AIMessage
from pydantic import BaseModel

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.llm_errors import is_vertex_permission_error
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import build_test_detail, find_current_test

logger = logging.getLogger(__name__)


class _EvaluationOutput(BaseModel):
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    reason_type: Optional[Literal["bad_data", "bad_assertions"]] = None
    explanation: str


async def evaluate_tests(state: QueryState):
    """
    Évalue la qualité de la suite de tests unitaires après exécution et produit
    un verdict + commentaire via le LLM.

    Quand le verdict est Insuffisant, classifie la cause :
    - bad_data       : données d'entrée incorrectes → déclenche une relance via l'agent conversationnel
    - bad_assertions : assertions/résultats attendus mal définis
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

    # Fast path: if the test produced 0 rows, check whether the SQL itself requires
    # more rows than MockSQL can generate (e.g. COUNT(*) threshold in a CTE WHERE).
    # This avoids wasting an LLM call and avoids the bad_data retry loop.
    if test_detail.get("status") == "empty_results":
        from build_query.constraint_simplifier import (
            check_correlated_aggregate_cardinality,
            check_having_cardinality,
        )
        dialect = state.get("dialect", "bigquery")
        cardinality_error: str | None = None
        for _check in (check_having_cardinality, check_correlated_aggregate_cardinality):
            try:
                _check(sql, dialect)
            except ValueError as exc:
                cardinality_error = str(exc)
                break

        if cardinality_error:
            logger.diag("[evaluator] too_many_rows détecté: %s", cardinality_error)
            content = f"**Insuffisant** — {cardinality_error}"
            return {
                "messages": [
                    AIMessage(
                        content=content,
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EVALUATION,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                            "test_index": current_test.get("test_index"),
                        },
                    )
                ],
                "evaluation_feedback": "too_many_rows",
                "status": "complete",
            }

    prompt = f"""Tu es un expert en qualité de tests SQL unitaires.

Tu reçois une requête SQL et les détails d'un test unitaire qui vient d'être généré et exécuté.

**Requête SQL :**
```sql
{sql}
```

**Test évalué :**
{json.dumps(test_detail, ensure_ascii=False, indent=2)}

**Mission :**
Évalue la qualité du test et retourne un objet JSON structuré avec :
- `verdict` : "Excellent", "Bon", ou "Insuffisant"
- `reason_type` (uniquement si Insuffisant) :
  - "bad_data" : les données d'entrée sont incorrectes (mauvais types, contraintes non respectées,
    valeurs incohérentes, jointures impossibles, résultat inattendu dû aux données générées)
  - "bad_assertions" : les assertions ou résultats attendus sont trop permissifs, incorrects ou
    mal définis — les données elles-mêmes sont valides
- `explanation` : une phrase ultra-concise (max 20 mots) en français expliquant le verdict

**Cas particulier — résultat vide intentionnel** : si la description du test indique explicitement que le résultat attendu est vide (ex : "plage vide", "aucune ligne", "filtre qui exclut tout", "0 ligne attendue"), alors le résultat vide est correct. Dans ce cas, évalue si les données d'entrée sont bien construites pour produire ce vide (Bon/Excellent), ou si les données ne semblent pas configurées pour ce scénario (Insuffisant + bad_data).

Exemple de sortie : {{"verdict": "Bon", "reason_type": null, "explanation": "Couvre la jointure sur clé manquante. Données et résultat valides."}}"""

    logger.diag("[evaluator] test_detail:\n%s", json.dumps(test_detail, ensure_ascii=False, indent=2))
    logger.diag("[evaluator] prompt:\n%s", prompt)

    llm = make_llm()
    structured_llm = llm.with_structured_output(_EvaluationOutput)

    try:
        result: _EvaluationOutput = await structured_llm.ainvoke(prompt)
        logger.diag("[evaluator] verdict=%s reason_type=%s — %s", result.verdict, result.reason_type, result.explanation)
    except Exception as exc:
        if is_vertex_permission_error(exc):
            return {}
        raise

    content = f"**{result.verdict}** — {result.explanation}"

    eval_test_index = current_test.get("test_index")
    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 2
    )

    evaluation_feedback = (
        result.reason_type
        if result.verdict == "Insuffisant" and result.reason_type
        else None
    )
    triggers_agent_retry = (
        evaluation_feedback == "bad_data"
        and gen_retries > 0
        and not state.get("assertion_only")
    )
    triggers_assertion_fix = (
        evaluation_feedback == "bad_assertions"
        and gen_retries > 0
        and not state.get("assertion_only")
    )

    state_update: dict = {
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
        "evaluation_feedback": evaluation_feedback,
        "status": "empty_results" if triggers_agent_retry else "complete",
    }
    if triggers_agent_retry or triggers_assertion_fix:
        state_update["gen_retries"] = gen_retries - 1

    return state_update
