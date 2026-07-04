"""Disjoncteur anti-thrash de la boucle de régénération d'assertions.

Incident c6 : une cause STRUCTURELLE partagée (colonne `offset` mot réservé) faisait
échouer toutes les assertions avec la même erreur ; chaque round de régénération LLM
reproduisait l'erreur à l'identique → REGEN_ASSERTION_LIMIT × N appels brûlés pour rien
(aucun modèle ne peut réparer une erreur dont il ignore la cause). Règle : si un round
complet ne fait bouger AUCUNE signature d'erreur (indices + première ligne du message),
la cause n'est pas une erreur de formulation → arrêt de la boucle. Tant que les erreurs
CHANGENT d'un round à l'autre, la boucle continue comme avant.
"""

import duckdb
import pandas as pd

import build_query.examples_executor as ee


def _con():
    con = duckdb.connect()
    con.register("v", pd.DataFrame([{"a": 1}]))
    return con


_BROKEN = {
    "description": "d",
    "expected_condition": "missing_col = 1",
    "sql": "SELECT * FROM __result__ WHERE (missing_col = 1) IS NOT TRUE",
}


async def test_breaker_stops_after_one_fruitless_round(monkeypatch):
    """Erreurs inchangées après un round complet → arrêt (1 round, pas 3)."""
    calls = []

    async def _stub(original, error, **kwargs):
        calls.append(error)
        return dict(_BROKEN)  # même SQL cassé → même signature d'erreur

    monkeypatch.setattr(ee, "_regenerate_assertion", _stub)
    results = await ee._evaluate_assertions_with_retry(
        [dict(_BROKEN), dict(_BROKEN)],
        view_name="v",
        con=_con(),
        duckdb_sql="SELECT 1",
        test_data=[],
        result_df=pd.DataFrame([{"a": 1}]),
        test_description="t",
    )
    # 1 round de régénération (2 assertions) — pas REGEN_ASSERTION_LIMIT (3) rounds.
    assert len(calls) == 2
    assert all(r.get("error") for r in results)


async def test_regen_none_does_not_rehammer(monkeypatch):
    """Régénération qui ne produit rien (LLM en échec) → erreur inchangée → arrêt."""
    calls = []

    async def _stub(original, error, **kwargs):
        calls.append(error)
        return None

    monkeypatch.setattr(ee, "_regenerate_assertion", _stub)
    await ee._evaluate_assertions_with_retry(
        [dict(_BROKEN)],
        view_name="v",
        con=_con(),
        duckdb_sql="SELECT 1",
        test_data=[],
        result_df=pd.DataFrame([{"a": 1}]),
        test_description="t",
    )
    assert len(calls) == 1


async def test_loop_continues_while_errors_change(monkeypatch):
    """Contrôle : erreur différente au round suivant (progrès) → la boucle continue
    et le fix finit par passer."""
    other_broken = {
        "description": "d",
        "expected_condition": "other_missing = 1",
        "sql": "SELECT * FROM __result__ WHERE (other_missing = 1) IS NOT TRUE",
    }
    fixed = {
        "description": "d",
        "expected_condition": "a = 1",
        "sql": "SELECT * FROM __result__ WHERE (a = 1) IS NOT TRUE",
    }
    seq = [dict(other_broken), dict(fixed)]

    async def _stub(original, error, **kwargs):
        return seq.pop(0)

    monkeypatch.setattr(ee, "_regenerate_assertion", _stub)
    results = await ee._evaluate_assertions_with_retry(
        [dict(_BROKEN)],
        view_name="v",
        con=_con(),
        duckdb_sql="SELECT 1",
        test_data=[],
        result_df=pd.DataFrame([{"a": 1}]),
        test_description="t",
    )
    assert not seq  # les deux régénérations ont bien été consommées
    assert results[0].get("error") is None
    assert results[0]["passed"] is True
