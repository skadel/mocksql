"""Mode d'assertion EXISTS (« il existe au moins une ligne telle que … »).

Complément du mode universel (`all` : la condition tient sur CHAQUE ligne). Pour affirmer
la PRÉSENCE d'une ligne précise dans un résultat multi-lignes — typiquement le FORMAT LONG
(une ligne par métrique : colonne label `indicateur` + colonne `valeur`) — le mode `exists`
laisse écrire `indicateur = 'nb_cartes' AND valeur = 2974` sans piéger les autres lignes :
l'assertion passe dès qu'une ligne matche, et n'expose pas les autres comme violantes.
"""

import duckdb
import pandas as pd

from build_query.examples_executor import (
    _Assertion,
    _assertion_sql_from_condition,
    _assertion_to_executable,
    _evaluate_assertions,
)

_LONG = [
    {"indicateur": "nb_cartes", "valeur": 2974},
    {"indicateur": "nb_clients", "valeur": 1200},
    {"indicateur": "nb_comptes", "valeur": 3050},
]


def _con(rows):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return con


def test_exists_sql_shape():
    """Le mode exists produit une requête NOT EXISTS (0 ligne = au moins un match)."""
    sql = _assertion_sql_from_condition(
        "indicateur = 'nb_cartes' AND valeur = 2974", quantifier="exists"
    )
    assert "NOT EXISTS" in sql
    assert "FROM __result__ WHERE (indicateur = 'nb_cartes' AND valeur = 2974)" in sql


def test_exists_sql_folds_scope():
    """Un scope éventuel est fondu dans le filtre EXISTS."""
    sql = _assertion_sql_from_condition(
        "valeur = 2974", scope="indicateur = 'nb_cartes'", quantifier="exists"
    )
    assert "NOT EXISTS" in sql
    assert "(indicateur = 'nb_cartes') AND (valeur = 2974)" in sql


def test_exists_passes_when_a_row_matches_without_flagging_others():
    """Card-count : la condition AND passe en exists, sans remonter les autres lignes."""
    cond = "indicateur = 'nb_cartes' AND valeur = 2974"
    res = _evaluate_assertions(
        [
            {
                "expected_condition": cond,
                "quantifier": "exists",
                "sql": _assertion_sql_from_condition(cond, quantifier="exists"),
            }
        ],
        "v",
        _con(_LONG),
    )[0]
    assert res["passed"] is True
    assert res["failing_rows"] == []
    assert res["quantifier"] == "exists"


def test_exists_fails_when_no_row_matches_without_synthetic_row():
    """Si aucune ligne ne matche → échec, et pas de ligne sentinelle exposée."""
    cond = "indicateur = 'nb_cartes' AND valeur = 9999"
    res = _evaluate_assertions(
        [
            {
                "expected_condition": cond,
                "quantifier": "exists",
                "sql": _assertion_sql_from_condition(cond, quantifier="exists"),
            }
        ],
        "v",
        _con(_LONG),
    )[0]
    assert res["passed"] is False
    assert res["failing_rows"] == []  # l'absence n'est pas un contre-exemple ligne


def test_assertion_to_executable_threads_quantifier():
    """`_assertion_to_executable` propage le quantifier dans le dict et le SQL."""
    a = _Assertion(
        description="Le nombre de cartes vaut 2974.",
        expected_condition="indicateur = 'nb_cartes' AND valeur = 2974",
        quantifier="exists",
    )
    ex = _assertion_to_executable(a)
    assert ex["quantifier"] == "exists"
    assert "NOT EXISTS" in ex["sql"]


def test_all_mode_default_unchanged():
    """Rétrocompat : sans quantifier, le mode all (IS NOT TRUE) reste inchangé."""
    a = _Assertion(
        description="Toutes les valeurs sont positives.",
        expected_condition="valeur > 0",
    )
    ex = _assertion_to_executable(a)
    assert "quantifier" not in ex
    assert ex["sql"] == "SELECT * FROM __result__ WHERE (valeur > 0) IS NOT TRUE"
