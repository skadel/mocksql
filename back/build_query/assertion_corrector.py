import io
import json
import logging
import uuid
from typing import Any, Dict, List, Literal, Optional

import pandas as pd
from langchain_core.messages import AIMessage
from pydantic import BaseModel

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.examples import DB_PATH, initialize_duckdb
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)


class _Assertion(BaseModel):
    description: str
    sql: str


class _ImprovedAssertions(BaseModel):
    reasoning: str
    assertions: List[_Assertion]
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    reason_type: Optional[Literal["bad_data", "bad_assertions"]] = None
    explanation: str


async def _generate_improved_assertions(
    duckdb_sql: str,
    test_data: Any,
    result_df: pd.DataFrame,
    test_description: str,
    evaluation_explanation: str,
    assertion_fix: Optional[Dict],
) -> _ImprovedAssertions:
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    suggestions_block = ""
    if assertion_fix:
        suggestions = assertion_fix.get("suggestions", [])
        if suggestions:
            lines = "\n".join(f"- {s}" for s in suggestions)
            suggestions_block = (
                f"\n\nVérifications suggérées par l'évaluateur :\n{lines}"
            )

    prompt = f"""Tu es un expert en tests SQL dbt-style avec DuckDB.

Le test suivant a été évalué "Insuffisant" car les assertions générées ne vérifient pas réellement la logique métier.
Problème identifié : {evaluation_explanation}

Description du test : {test_description}{suggestions_block}

Données d'entrée :
{json.dumps(test_data, ensure_ascii=False, default=str)}

Requête SQL testée :
```sql
{duckdb_sql}
```

Résultat après exécution sur DuckDB — {row_count} ligne(s).

Schéma exact de `__result__` :
{schema_str}

Exemples de lignes :
{json.dumps(sample, ensure_ascii=False, default=str)}

Commence par raisonner à voix haute (`reasoning`, 3–5 phrases) :
- Pourquoi les anciennes assertions étaient-elles insuffisantes ?
- Quel comportement SQL cette requête doit-elle vraiment vérifier ?
- Quelles invariants métier peuvent être testés sur ce résultat ?

Génère 2 à 3 nouvelles assertions SQL qui VÉRIFIENT RÉELLEMENT la logique métier.

Règles strictes :
- Convention dbt-style : 0 ligne si OK, des lignes si KO
- Évite les tautologies : "WHERE col = valeur_unique" passe toujours pour un résultat à 1 ligne
- Teste le COMPORTEMENT de la requête (MAX, MIN, agrégation, jointure, classement, filtrage)
- INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`. Toutes tes assertions doivent uniquement lire `__result__`. Pas de sous-requête vers une table source, même pour vérifier un MAX. Exemple interdit : `SELECT ... FROM ma_table_source ...`
- Pour vérifier un MAX : compare les colonnes de `__result__` entre elles via des sous-requêtes sur `__result__` uniquement, ex. : `SELECT * FROM __result__ WHERE val != (SELECT MAX(val) FROM __result__)`
- Utilise UNIQUEMENT les colonnes du schéma ci-dessus (noms exacts, sensibles à la casse)
- Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête :
  `SELECT * FROM (SELECT *, expr AS col FROM __result__) WHERE col ...`

Puis évalue la qualité de ces nouvelles assertions :
- `verdict` : "Excellent", "Bon", ou "Insuffisant"
- `reason_type` : uniquement si Insuffisant → "bad_data" ou "bad_assertions"
- `explanation` : une phrase ultra-concise (max 20 mots) en français"""

    llm = make_llm()
    structured_llm = llm.with_structured_output(_ImprovedAssertions)
    try:
        result: _ImprovedAssertions = await structured_llm.ainvoke(prompt)
        logger.diag(
            "[assertion_corrector] verdict=%s reason_type=%s assertions=%s",
            result.verdict,
            result.reason_type,
            len(result.assertions),
        )
        return result
    except Exception as e:
        logger.diag("[assertion_corrector] ERREUR LLM: %s", e)
        return _ImprovedAssertions(
            reasoning="Correction indisponible.",
            assertions=[],
            verdict="Bon",
            explanation="Correction des assertions indisponible.",
        )


async def correct_assertions(state: QueryState) -> Dict[str, Any]:
    """
    Corrige les assertions faibles (bad_assertions) sans ré-exécuter le SQL.

    Lit results_json depuis le dernier message RESULTS, génère de meilleures
    assertions via LLM en exploitant assertion_fix, les évalue en DuckDB,
    et émet un nouveau message RESULTS avec le verdict mis à jour.
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

    current_test = find_current_test(all_tests, state.get("test_index"))
    if current_test is None:
        return {}

    results_json = current_test.get("results_json", "[]")
    try:
        result_df = pd.read_json(io.StringIO(results_json), orient="records")
    except Exception:
        result_df = pd.DataFrame()

    if result_df.empty:
        logger.diag("[assertion_corrector] results_json vide, abandon")
        return {}

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    assertion_fix = current_test.get("assertion_fix")
    evaluation_explanation = current_test.get(
        "evaluation_explanation", "assertions insuffisantes"
    )

    improved = await _generate_improved_assertions(
        duckdb_sql=sql,
        test_data=current_test.get("data", {}),
        result_df=result_df,
        test_description=current_test.get("unit_test_description", ""),
        evaluation_explanation=evaluation_explanation,
        assertion_fix=assertion_fix,
    )

    if not improved.assertions:
        return {}

    # Evaluate new assertions against __result__ rebuilt from results_json
    from build_query.examples_executor import (
        _evaluate_assertions_with_retry,
        _fix_logically_failing_assertions,
    )

    session_id = state["session"].replace("-", "_")
    test_index = current_test.get("test_index", "1")
    suffix = f"{session_id}{test_index}"
    view_name = f"__result__{suffix}"

    assertion_results = []
    with initialize_duckdb(DB_PATH) as con:
        con.register(view_name, result_df)
        try:
            retry_kwargs = dict(
                view_name=view_name,
                con=con,
                duckdb_sql=sql,
                test_data=current_test.get("data", {}),
                result_df=result_df,
                test_description=current_test.get("unit_test_description", ""),
            )
            assertion_results = await _evaluate_assertions_with_retry(
                [a.model_dump() for a in improved.assertions], **retry_kwargs
            )
            assertion_results = await _fix_logically_failing_assertions(
                assertion_results, **retry_kwargs
            )
        finally:
            try:
                con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
            except Exception:
                pass

    has_failing = any(not a.get("passed") for a in assertion_results)

    updated_test = {
        **current_test,
        "assertion_results": assertion_results,
        "verdict": "Insuffisant" if has_failing else improved.verdict,
        "reason_type": "bad_assertions" if has_failing else improved.reason_type,
        "evaluation_explanation": (
            "Les assertions générées ne correspondent pas au résultat de la requête."
            if has_failing
            else improved.explanation
        ),
        "assertion_fix": None,
    }
    updated_all_tests = [
        updated_test if t.get("test_index") == current_test.get("test_index") else t
        for t in all_tests
    ]

    parent = last_results.additional_kwargs.get("parent") or state.get(
        "parent_message_id"
    )
    sql_kw = state.get("query", "").strip()
    optimized_kw = state.get("optimized_sql", "").strip()

    return {
        "messages": [
            AIMessage(
                content=json.dumps(updated_all_tests, ensure_ascii=False),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.RESULTS,
                    "parent": parent,
                    "request_id": state.get("request_id"),
                    **({"sql": sql_kw} if sql_kw else {}),
                    **({"optimized_sql": optimized_kw} if optimized_kw else {}),
                },
            )
        ]
    }
