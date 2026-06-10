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
    expected_condition: str


class _ImprovedAssertions(BaseModel):
    reasoning: str
    assertions: List[_Assertion]
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    reason_type: Optional[Literal["bad_data", "bad_assertions"]] = None
    explanation: str


def _format_correction_history(attempts: List[Dict]) -> str:
    if not attempts:
        return ""
    lines = ["\n\nTentatives précédentes — à ne PAS reproduire :\n"]
    for i, attempt in enumerate(attempts, 1):
        lines.append(f"Tentative {i} :")
        for a in attempt.get("assertions", []):
            outcome = a.get("outcome", "?")
            lines.append(f'  • "{a["description"]}"')
            lines.append(f"    SQL : {a['sql']}")
            lines.append(f"    → {outcome}")
        if attempt.get("explanation"):
            lines.append(f"  Bilan : {attempt['explanation']}")
        lines.append("")
    return "\n".join(lines)


def _build_attempt_record(assertion_results: List[Dict], explanation: str) -> Dict:
    """Build a history entry from the current correction attempt."""
    assertions_log = []
    for a in assertion_results:
        if a.get("error"):
            outcome = f"ERREUR SQL : {a['error']}"
        elif not a.get("passed"):
            rows = a.get("failing_rows", [])
            sample = (
                json.dumps(rows[:2], ensure_ascii=False, default=str) if rows else ""
            )
            outcome = f"VIOLATION DE DONNÉES{' — ex : ' + sample if sample else ''}"
        else:
            outcome = "PASSÉ ✓"
        assertions_log.append(
            {
                "description": a.get("description", ""),
                "sql": a.get("sql", ""),
                "outcome": outcome,
            }
        )
    return {"assertions": assertions_log, "explanation": explanation}


async def _generate_improved_assertions(
    duckdb_sql: str,
    test_data: Any,
    result_df: pd.DataFrame,
    test_description: str,
    evaluation_explanation: str,
    assertion_fix: Optional[Dict],
    correction_attempts: Optional[List[Dict]] = None,
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

    history_block = _format_correction_history(correction_attempts or [])

    prompt = f"""Tu es un expert en tests SQL dbt-style avec DuckDB.

Le test suivant a été évalué "Insuffisant" car les assertions générées ne vérifient pas réellement la logique métier.
Problème identifié : {evaluation_explanation}

Description du test : {test_description}{suggestions_block}{history_block}

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

Génère 2 à 3 nouvelles assertions qui VÉRIFIENT RÉELLEMENT la logique métier.

Chaque assertion fournit une `expected_condition` : une **condition booléenne POSITIVE** qui doit
être VRAIE pour chaque ligne quand le test réussit. **Tu n'écris PAS de SQL `SELECT`/`WHERE` et tu
n'écris JAMAIS la négation** — MockSQL négocie ta condition lui-même pour produire la requête de
validation (0 ligne = OK). Tu exprimes seulement la vérité métier attendue, à l'affirmative.

Règles strictes :
- Exprime l'AFFIRMATION, jamais sa négation : pour pincer la valeur retournée `2026-01-02`, écris
  `expected_condition: "date = '2026-01-02'"` — surtout PAS `date != '2026-01-02'`. "Vérifier ce qui
  ne doit pas être là" est INTERDIT : reformule toujours en ce qui DOIT être là.
- N'écris que l'expression booléenne (pas de `SELECT`, pas de `WHERE`, pas de `FROM`).
- Évite les tautologies : une condition que toute ligne satisfait forcément ne teste rien.
- Teste le COMPORTEMENT de la requête (MAX, MIN, agrégation, jointure, classement, filtrage)
- INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`. Pas de sous-requête vers une table source.
- Pour vérifier un MAX : compare les colonnes de `__result__` entre elles via une sous-requête sur
  `__result__` uniquement, ex. : `expected_condition: "val = (SELECT MAX(val) FROM __result__)"`
- Utilise UNIQUEMENT les colonnes du schéma ci-dessus (noms exacts, sensibles à la casse)

Puis évalue la qualité de ces nouvelles assertions :
- `verdict` : "Excellent", "Bon", ou "Insuffisant"
- `reason_type` : uniquement si Insuffisant → "bad_data" ou "bad_assertions"
- `explanation` : une phrase ultra-concise (max 20 mots) en français"""

    llm = make_llm()
    structured_llm = llm.with_structured_output(_ImprovedAssertions)
    try:
        logger.diag("[assertion_corrector] PROMPT LLM:\n%s", prompt[:3000])
        result: _ImprovedAssertions = await structured_llm.ainvoke(prompt)
        logger.diag("[assertion_corrector] reasoning:\n%s", result.reasoning[:1000])
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
    assertions via LLM en exploitant assertion_fix et correction_attempts,
    les évalue en DuckDB, et émet un nouveau message RESULTS avec le verdict mis à jour.
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

    parent = last_results.additional_kwargs.get("parent") or state.get(
        "parent_message_id"
    )

    # Tout ce qui suit (LLM, exécution DuckDB, sérialisation) peut lever : un souci sur
    # une assertion (ex. Timestamp non sérialisable, SQL invalide) doit être remonté en
    # erreur visible plutôt que de crasher le stream silencieusement — même si la limite
    # de corrections est atteinte. On pose `error` + `gen_retries=0` pour arrêter la boucle.
    try:
        sql = (state.get("optimized_sql") or state.get("query", "")).strip()
        assertion_fix = current_test.get("assertion_fix")
        evaluation_explanation = current_test.get(
            "evaluation_explanation", "assertions insuffisantes"
        )
        correction_attempts: List[Dict] = current_test.get("correction_attempts") or []

        improved = await _generate_improved_assertions(
            duckdb_sql=sql,
            test_data=current_test.get("data", {}),
            result_df=result_df,
            test_description=current_test.get("unit_test_description", ""),
            evaluation_explanation=evaluation_explanation,
            assertion_fix=assertion_fix,
            correction_attempts=correction_attempts,
        )

        if not improved.assertions:
            return {}

        # Evaluate new assertions against __result__ rebuilt from results_json
        from build_query.examples_executor import (
            _assertion_to_executable,
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
                    [_assertion_to_executable(a) for a in improved.assertions],
                    **retry_kwargs,
                )
                assertion_results = await _fix_logically_failing_assertions(
                    assertion_results, **retry_kwargs
                )
            finally:
                try:
                    con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
                except Exception:
                    pass

        # In dbt-style testing, passed=False with no error means the assertion runs
        # correctly and finds a data violation — that's bad_data, not bad_assertions.
        # Only SQL syntax errors (error field set) indicate a bad assertion.
        has_sql_error = any(a.get("error") for a in assertion_results)
        has_data_violation = any(
            not a.get("passed") and not a.get("error") for a in assertion_results
        )

        if has_sql_error:
            verdict_final = "Insuffisant"
            reason_final = "bad_assertions"
            explanation_final = "Les assertions générées ne correspondent pas au résultat de la requête."
        elif has_data_violation:
            # Assertions are logically correct but the test data violates the rule
            verdict_final = "Insuffisant"
            reason_final = "bad_data"
            explanation_final = improved.explanation
        else:
            verdict_final = improved.verdict
            reason_final = improved.reason_type
            explanation_final = improved.explanation

        updated_attempts = correction_attempts + [
            _build_attempt_record(assertion_results, explanation_final)
        ]
        logger.diag(
            "[assertion_corrector] correction_attempts: %d → %d",
            len(correction_attempts),
            len(updated_attempts),
        )

        updated_test = {
            **current_test,
            "assertion_results": assertion_results,
            "verdict": verdict_final,
            "reason_type": reason_final,
            "evaluation_explanation": explanation_final,
            "assertion_fix": None,
            "correction_attempts": updated_attempts,
        }
        updated_all_tests = [
            updated_test if t.get("test_index") == current_test.get("test_index") else t
            for t in all_tests
        ]

        sql_kw = state.get("query", "").strip()
        optimized_kw = state.get("optimized_sql", "").strip()

        return {
            "messages": [
                AIMessage(
                    content=json.dumps(
                        updated_all_tests, ensure_ascii=False, default=str
                    ),
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.RESULTS,
                        "parent": parent,
                        "request_id": state.get("request_id"),
                        **({"sql": sql_kw} if sql_kw else {}),
                        **({"optimized_sql": optimized_kw} if optimized_kw else {}),
                    },
                )
            ],
            "gen_retries": max(0, state.get("gen_retries", 0) - 1),
        }
    except Exception as exc:
        logger.warning(
            "[assertion_corrector] échec de la correction des assertions "
            "(test_index=%s) : %s",
            current_test.get("test_index"),
            exc,
            exc_info=True,
        )
        return {
            "messages": [
                AIMessage(
                    content=(
                        "La correction automatique des assertions a échoué "
                        f"({type(exc).__name__} : {exc}). Le test est conservé en l'état "
                        "— vérifie ou édite ses assertions manuellement."
                    ),
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.ERROR,
                        "parent": parent,
                        "request_id": state.get("request_id"),
                    },
                )
            ],
            "error": "assertion_correction_failed",
            "gen_retries": 0,
        }
