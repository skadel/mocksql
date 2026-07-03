"""
Régression : relance (« Relancer ») d'un test dont le résultat vide est INTENTIONNEL.

Un test jugé « vide voulu » porte l'assertion sentinelle `SELECT * FROM __result__`
(posée par le PASS d'intention vide de test_evaluator : 0 ligne → passe, ≥1 ligne → échoue).
À la relance (`rerun_all`), l'executor doit rejouer cette assertion de façon déterministe
au lieu de repartir dans le circuit « vide inattendu » (statut empty_results → juge LLM
d'intention). Sans sentinelle, rejouer des assertions failing-rows sur 0 ligne passerait
par vacuité → on garde le chemin empty_results (diagnostic CTE + juge).

Couvre aussi le transport de `empty_intent_cache` à travers l'executor : sans lui,
l'évaluateur ne retrouve jamais l'empreinte memoïsée → rappel LLM à chaque relance.
"""

import duckdb
import pytest

from build_query.examples_executor import _run_single_test_case
from utils.test_utils import EMPTY_RESULT_SENTINEL_SQL, is_empty_result_sentinel

SCHEMAS = [
    {
        "table_name": "proj.ds.orders",
        "description": "",
        "columns": [
            {"name": "id", "type": "STRING"},
            {"name": "amount", "type": "FLOAT64"},
        ],
        "primary_keys": ["id"],
    }
]
USED_COLUMNS = [
    {
        "project": "proj",
        "database": "ds",
        "table": "orders",
        "used_columns": ["id", "amount"],
    }
]
# amount <= 100 sur la ligne injectée → le filtre exclut tout → 0 ligne.
QUERY = "SELECT id FROM `proj.ds.orders` WHERE amount > 100"

SENTINEL_ASSERTION = {
    "description": "La requête doit retourner 0 ligne (table vide intentionnelle)",
    "sql": EMPTY_RESULT_SENTINEL_SQL,
    "passed": True,
}
FAILING_ROWS_ASSERTION = {
    "description": "id vaut A",
    "sql": "SELECT * FROM __result__ WHERE (id = 'A') IS NOT TRUE",
    "passed": True,
}
INTENT_CACHE = {
    "fingerprint": "abc123",
    "verdict": "Excellent",
    "explanation": "Le vide est le comportement attendu.",
}


def _state():
    return {
        "session": "sess",
        "project": "proj",
        "dialect": "bigquery",
        "messages": [],
        "query_decomposed": "[]",
        "status": "",
        "gen_retries": 1,
        "request_id": None,
    }


def _test_case(assertions):
    return {
        "test_index": "4",
        "test_name": "Filtre excluant tout",
        "unit_test_description": "Aucune ligne attendue : le filtre exclut la ligne injectée.",
        "data": {"proj.ds.orders": [{"id": "A", "amount": 50.0}]},
        "assertion_results": assertions,
        "empty_intent_cache": INTENT_CACHE,
    }


async def _run(test_case, rerun_all):
    con = duckdb.connect(":memory:")
    try:
        return await _run_single_test_case(
            state=_state(),
            test_case=test_case,
            loop_index=0,
            session_id="s1",
            query=QUERY,
            schemas=SCHEMAS,
            used_columns=USED_COLUMNS,
            con=con,
            dialect="bigquery",
            rerun_all=rerun_all,
        )
    finally:
        con.close()


class TestIsEmptyResultSentinel:
    def test_exact_match(self):
        assert is_empty_result_sentinel({"sql": "SELECT * FROM __result__"})

    def test_tolerates_case_spaces_and_semicolon(self):
        assert is_empty_result_sentinel({"sql": "  select *  from __result__ ; "})

    def test_rejects_filtered_assertion(self):
        assert not is_empty_result_sentinel(FAILING_ROWS_ASSERTION)

    def test_rejects_empty_or_missing_sql(self):
        assert not is_empty_result_sentinel({"sql": ""})
        assert not is_empty_result_sentinel({})


@pytest.mark.asyncio
async def test_rerun_empty_with_sentinel_replays_assertions():
    """Relance d'un test « vide intentionnel » : la sentinelle est rejouée (0 ligne → passe),
    le test sort en `complete`/`Bon` sans repasser par le circuit empty_results (juge LLM)."""
    result = await _run(_test_case([SENTINEL_ASSERTION]), rerun_all=True)
    assert result["status"] == "complete"
    assert result["verdict"] == "Bon"
    assert result["assertion_results"][0]["passed"] is True
    assert result.get("failing_cte") is None


@pytest.mark.asyncio
async def test_rerun_empty_sentinel_fails_when_rows_appear():
    """La sentinelle n'est pas une tautologie : si la relance produit des lignes alors que
    le test attend 0 ligne, elle échoue → Insuffisant / bad_assertions."""
    tc = _test_case([SENTINEL_ASSERTION])
    tc["data"] = {"proj.ds.orders": [{"id": "A", "amount": 150.0}]}  # passe le filtre
    result = await _run(tc, rerun_all=True)
    assert result["status"] == "complete"
    assert result["verdict"] == "Insuffisant"
    assert result["reason_type"] == "bad_assertions"
    assert result["assertion_results"][0]["passed"] is False


@pytest.mark.asyncio
async def test_rerun_empty_without_sentinel_keeps_empty_results_path():
    """Sans sentinelle, un résultat vide à la relance reste `empty_results` : rejouer des
    assertions failing-rows sur 0 ligne les ferait toutes passer par vacuité."""
    result = await _run(_test_case([FAILING_ROWS_ASSERTION]), rerun_all=True)
    assert result["status"] == "empty_results"
    assert result["assertion_results"] == []


@pytest.mark.asyncio
async def test_generation_empty_ignores_sentinel():
    """Hors relance (génération/retry), le chemin empty_results reste inchangé même si une
    sentinelle traîne dans le test : le diagnostic CTE + juge d'intention font foi."""
    result = await _run(_test_case([SENTINEL_ASSERTION]), rerun_all=False)
    assert result["status"] == "empty_results"


@pytest.mark.asyncio
async def test_executor_carries_empty_intent_cache():
    """`empty_intent_cache` traverse l'executor : sans lui dans le message RESULTS,
    l'évaluateur ne retrouve pas l'empreinte memoïsée → rappel LLM à chaque relance."""
    result = await _run(_test_case([FAILING_ROWS_ASSERTION]), rerun_all=True)
    assert result["empty_intent_cache"] == INTENT_CACHE
