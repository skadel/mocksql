"""Régression filecoin-data-portal / warm_storage_datasets — assertion SCOPÉE.

Incident (éval fdp 2026-06-14) : pour `SELECT ... FROM warm_storage_datasets ORDER BY
billing_started_at` (résultat à 2 lignes), le générateur a produit l'assertion
`dataset_id = 'DS_001' AND billing_started_at = (SELECT MIN(billing_started_at) FROM __result__)`.
Comme une `expected_condition` est testée sur CHAQUE ligne, la ligne DS_002 (qui n'est
pas le min) est toujours remontée comme violante → le test échoue alors que le tri est
correct. Et la règle anti-vacuité interdit la correction par implication (`<>`/`OR NOT`).

Fix : le champ `scope` restreint l'univers d'une assertion. On peut alors affirmer un
fait sur UNE ligne précise (le min/max) en restant POSITIF, sans fausse violation — et
ça pince bien une régression de tri/association. Un scope qui ne couvre aucune ligne fait
échouer l'assertion (anti-vacuité). Cf. [[project_assertion_fixer_vacuity]].
"""

import duckdb
import pandas as pd

from build_query.examples_executor import (
    _assertion_sql_from_condition,
    _evaluate_assertions,
)

# Sortie correcte : triée par billing_started_at → la ligne la plus ancienne est DS_001.
_CORRECT = [
    {"dataset_id": "DS_001", "billing_started_at": "2026-01-02"},
    {"dataset_id": "DS_002", "billing_started_at": "2026-01-06"},
]
# Sortie buguée : la ligne la plus ancienne n'est plus DS_001 (mauvais tri/association).
_BUGGY = [
    {"dataset_id": "DS_002", "billing_started_at": "2026-01-02"},
    {"dataset_id": "DS_001", "billing_started_at": "2026-01-06"},
]

_MIN_SCOPE = "billing_started_at = (SELECT MIN(billing_started_at) FROM __result__)"


def _eval(rows, assertion):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return _evaluate_assertions([assertion], "v", con)[0]


def test_unscoped_single_row_fact_fails_on_correct_multirow_output():
    """L'ancienne forme (sans scope) échoue à tort sur un résultat multi-lignes correct."""
    res = _eval(
        _CORRECT,
        {
            "expected_condition": "dataset_id = 'DS_001'",
            "sql": _assertion_sql_from_condition(
                "dataset_id = 'DS_001' AND " + _MIN_SCOPE
            ),
        },
    )
    assert res["passed"] is False  # DS_002 est remontée → faux échec


def test_scoped_assertion_passes_on_correct_output():
    """Scopée sur la ligne min : positive, ne teste que la bonne ligne → passe."""
    res = _eval(
        _CORRECT,
        {
            "expected_condition": "dataset_id = 'DS_001'",
            "scope": _MIN_SCOPE,
            "sql": _assertion_sql_from_condition("dataset_id = 'DS_001'", _MIN_SCOPE),
        },
    )
    assert res["passed"] is True


def test_scoped_assertion_catches_regression():
    """Non-vacuité : la même assertion scopée ÉCHOUE sur une sortie buguée (tri cassé)."""
    res = _eval(
        _BUGGY,
        {
            "expected_condition": "dataset_id = 'DS_001'",
            "scope": _MIN_SCOPE,
            "sql": _assertion_sql_from_condition("dataset_id = 'DS_001'", _MIN_SCOPE),
        },
    )
    assert res["passed"] is False
    assert any(r["dataset_id"] == "DS_002" for r in res["failing_rows"])


def test_empty_scope_is_rejected_as_vacuous():
    """Un scope qui ne couvre aucune ligne → assertion vacante → échec explicite."""
    res = _eval(
        _CORRECT,
        {
            "expected_condition": "dataset_id = 'DS_001'",
            "scope": "dataset_id = 'NOPE'",
            "sql": _assertion_sql_from_condition(
                "dataset_id = 'DS_001'", "dataset_id = 'NOPE'"
            ),
        },
    )
    assert res["passed"] is False
    assert "vacante" in res.get("error", "")


def test_sql_builder_injects_scope():
    """La requête générée restreint bien l'univers avant le test IS NOT TRUE."""
    sql = _assertion_sql_from_condition("dataset_id = 'DS_001'", _MIN_SCOPE)
    assert f"WHERE ({_MIN_SCOPE}) AND ((dataset_id = 'DS_001') IS NOT TRUE)" in sql
    # Sans scope : forme historique inchangée (rétrocompat).
    assert (
        _assertion_sql_from_condition("amount > 0")
        == "SELECT * FROM __result__ WHERE (amount > 0) IS NOT TRUE"
    )
