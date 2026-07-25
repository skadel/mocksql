"""``coherence_check`` — le LLM PRÉPARE la revue humaine (il ne juge plus l'output).

Spec ``docs/spec-validation-humaine.md`` §4 (repositionné). Sur un test dont l'exécution
a produit des lignes, un appel LLM **léger** produit :

1. ``coherence`` (``ok`` | ``warn``) : le scénario annoncé (nom/description/tags) est-il
   réellement EXERCÉ par les données injectées ? (un test « cas NULL » sans NULL en jeu →
   ``warn``). C'est de la COHÉRENCE narratif↔données↔SQL, **jamais du réalisme** ni du
   jugement de correction de l'output — l'humain reste l'oracle.
2. ``review_hint`` : LA chose à vérifier, une phrase, ancrée sur une ligne concrète
   (« ligne order_id=3 : amount=100 est le cas limite du seuil »).
3. ``key_columns`` : les colonnes PORTEUSES de la logique du scénario → deviennent
   ``expect.columns`` (comparées au replay), éditables par l'humain à la revue.
4. ``non_deterministic`` : la sortie peut varier d'un run à l'autre (LIMIT sans ORDER BY
   total, ex-æquo) → flag de revue.

Le résultat est écrit sur le test : ``review = {status: draft, hint, coherence,
non_deterministic}`` + ``expect`` reconstruit sur les colonnes porteuses. ``warn`` est
**non bloquant** (spec Q3) : aucune boucle de retry — la boucle ``bad_data`` reste
déterministe (0 ligne / tout-NULL). Best-effort : un échec LLM dégrade proprement
(pas de hint, colonnes = toutes) sans jamais casser la génération.
"""

import json
import logging
import uuid
from typing import List, Literal, Optional

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel, Field

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.expect_contract import build_expect
from build_query.state import QueryState
from storage.config import output_language_directive
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)

# Échantillon de lignes injecté dans le prompt (les mocks sont petits ; on reste généreux
# pour ne jamais tronquer un objet JSON au milieu — cf. feedback juge/troncature).
_SAMPLE_ROWS = 30


class CoherenceResult(BaseModel):
    """Sortie structurée du coherence_check — PRÉPARE la revue, ne juge pas l'output."""

    coherence: Literal["ok", "warn"] = Field(
        description=(
            "ok si le scénario annoncé est réellement exercé par les données injectées ; "
            "warn si le cas prétendu n'est pas atteint (ex. test 'valeurs NULL' sans NULL "
            "dans les données, 'plage vide' qui renvoie des lignes). Cohérence "
            "narratif↔données↔SQL, jamais le réalisme."
        )
    )
    review_hint: str = Field(
        description=(
            "UNE phrase : la chose à vérifier par l'humain, ancrée sur une ligne concrète "
            "de la sortie (ex. « ligne order_id=3 : amount=100 est le cas limite du seuil »)."
        )
    )
    key_columns: List[str] = Field(
        default_factory=list,
        description=(
            "Colonnes de la SORTIE porteuses de la logique du scénario (celles qu'un "
            "humain regarde pour valider). Vide = toutes les colonnes."
        ),
    )
    non_deterministic: bool = Field(
        default=False,
        description=(
            "True si la sortie peut varier d'un run à l'autre : LIMIT/QUALIFY sans tri "
            "total, ex-æquo sur la clé de tri."
        ),
    )


def _result_rows(current_test: dict) -> Optional[List[dict]]:
    try:
        rows = json.loads(current_test.get("results_json") or "[]")
    except Exception:
        return None
    return rows if isinstance(rows, list) else None


def _build_prompt(current_test: dict, sql: str, dialect: str, rows: List[dict]) -> str:
    name = current_test.get("test_name") or ""
    description = current_test.get("unit_test_description", "")
    tags = current_test.get("tags", [])
    data = current_test.get("data", {})
    columns = list(rows[0].keys()) if rows else []
    return f"""SQL testé (dialecte {dialect}) :
{sql}

Scénario annoncé du test :
- Titre : {name}
- Description : {description}
- Tags : {json.dumps(tags, ensure_ascii=False)}

Données d'entrée injectées dans DuckDB :
{json.dumps(data, ensure_ascii=False, indent=2, default=str)}

Sortie produite (colonnes : {json.dumps(columns, ensure_ascii=False)}) :
{json.dumps(rows[:_SAMPLE_ROWS], ensure_ascii=False, indent=2, default=str)}

Tu NE juges PAS si cette sortie est correcte — c'est l'humain (l'ingénieur) qui le décidera.
Tu PRÉPARES sa revue :
1. `coherence` : le scénario annoncé est-il réellement EXERCÉ par ces données ? (un cas
   « NULL » sans NULL, une « plage vide » qui renvoie des lignes → `warn`).
2. `review_hint` : la SEULE chose qu'il doit vérifier, en une phrase, ancrée sur une ligne
   précise de la sortie (nomme une valeur de clé et la colonne porteuse).
3. `key_columns` : les colonnes de la sortie qui portent la logique du scénario.
4. `non_deterministic` : la sortie peut-elle varier d'un run à l'autre (LIMIT/QUALIFY sans
   tri total, ex-æquo) ?"""


async def _run_llm(prompt: str) -> Optional[CoherenceResult]:
    llm = make_llm().with_structured_output(CoherenceResult)
    try:
        logger.diag("[coherence_check] PROMPT LLM:\n%s", prompt[:3000])
        return await llm.ainvoke(
            [
                SystemMessage(
                    content=output_language_directive()
                    + "\n\n"
                    + MOCKSQL_PRODUCT_PREAMBLE
                    + "\n\nTu prépares la revue humaine d'un test (focalisation + point "
                    "d'attention). Tu ne juges jamais si la sortie est correcte."
                ),
                HumanMessage(content=prompt),
            ]
        )
    except Exception as exc:
        logger.warning("[coherence_check] appel LLM échoué: %s", exc)
        return None


async def coherence_check(state: QueryState):
    """Nœud LangGraph : prépare la revue humaine d'un test complet (spec §4).

    Best-effort et non bloquant : sur test vide / sans lignes / échec LLM, on n'émet rien
    (la boucle ``bad_data`` déterministe et l'évaluateur restent seuls maîtres du routage).

    Gardé par ``is_coherence_check_enabled()`` (défaut OFF, réglage transitoire Phase 2) :
    désactivé → pass-through immédiat (aucun coût LLM), le pipeline d'assertions reste seul.
    """
    from storage.config import is_coherence_check_enabled

    if state.get("error") or not is_coherence_check_enabled():
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
    if current_test is None or current_test.get("status") != "complete":
        return {}

    rows = _result_rows(current_test)
    if not rows:
        # Sortie vide (voulue ou non) : traitée par le circuit empty_results déterministe,
        # pas ici — le coherence_check ne parle que d'une sortie NON vide à revoir.
        return {}

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    dialect = state.get("dialect", "bigquery")
    result = await _run_llm(_build_prompt(current_test, sql, dialect, rows))

    key_columns = list(result.key_columns) if result else []
    expect = build_expect(
        current_test.get("results_json"),
        current_test.get("assertion_results"),
        sql,
        dialect,
        columns=key_columns or None,
    )

    prev_review = (
        current_test.get("review")
        if isinstance(current_test.get("review"), dict)
        else {}
    )
    review = {**prev_review, "status": prev_review.get("status") or "draft"}
    if result is not None:
        review["coherence"] = result.coherence
        review["hint"] = result.review_hint
        review["non_deterministic"] = result.non_deterministic

    updated_test = {**current_test, "review": review}
    if expect is not None:
        updated_test["expect"] = expect

    updated_all_tests = [
        updated_test if t.get("test_index") == current_test.get("test_index") else t
        for t in all_tests
    ]

    eval_test_index = current_test.get("test_index")
    parent = last_results.additional_kwargs.get("parent") or state.get(
        "parent_message_id"
    )
    sql_kw = state.get("query", "").strip()
    optimized_kw = state.get("optimized_sql", "").strip()
    new_results_id = str(uuid.uuid4())

    messages: list = [
        AIMessage(
            content=json.dumps(
                updated_all_tests, ensure_ascii=False, indent=2, default=str
            ),
            id=new_results_id,
            additional_kwargs={
                **last_results.additional_kwargs,
                "type": MsgType.RESULTS,
                "parent": parent,
                "request_id": state.get("request_id"),
                **({"sql": sql_kw} if sql_kw else {}),
                **({"optimized_sql": optimized_kw} if optimized_kw else {}),
            },
        )
    ]
    if result is not None:
        # Message de revue pour le panneau (hint + coherence). N'entre PAS dans le verdict
        # d'exécution — c'est une aide à la revue humaine.
        messages.append(
            AIMessage(
                content=result.review_hint,
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.COHERENCE,
                    "parent": new_results_id,
                    "request_id": state.get("request_id"),
                    "test_index": eval_test_index,
                    "coherence": result.coherence,
                    "non_deterministic": result.non_deterministic,
                },
            )
        )
        logger.diag(
            "[coherence_check] test=%s coherence=%s non_det=%s cols=%s",
            eval_test_index,
            result.coherence,
            result.non_deterministic,
            key_columns,
        )

    return {"messages": messages}
