"""Régression « format long » (card-count) — relevage automatique du sélecteur en `scope`.

Incident : pour un résultat en format long (une ligne par métrique : colonne label
`indicateur` + colonne `valeur`), le générateur produit l'assertion conjonctive
`indicateur = 'nb_cartes' AND valeur = 2974`. Comme une `expected_condition` est testée
sur CHAQUE ligne via `(cond) IS NOT TRUE`, les lignes des AUTRES indicateurs (où le `AND`
vaut FALSE) remontent à tort comme violantes → 4 lignes sur 5 « plantent » alors que la
métrique nb_cartes est correcte.

Fix : `_autoscope_failing_assertions` relève mécaniquement le sélecteur (`indicateur =
'nb_cartes'`) dans `scope` et garde `valeur = 2974` comme condition — sans LLM, validé
contre les données réelles. La régression de valeur reste détectée (l'assertion scopée
échoue si la métrique a la mauvaise valeur).
"""

import duckdb
import pandas as pd

from build_query.examples_executor import (
    _assertion_sql_from_condition,
    _autoscope_conjunction,
    _autoscope_failing_assertions,
    _evaluate_assertions,
)

# Résultat format long : 5 métriques, une par ligne. nb_cartes vaut bien 2974.
_LONG = [
    {"indicateur": "nb_cartes", "valeur": 2974},
    {"indicateur": "nb_clients", "valeur": 1200},
    {"indicateur": "nb_comptes", "valeur": 3050},
    {"indicateur": "nb_agences", "valeur": 42},
    {"indicateur": "nb_pays", "valeur": 7},
]
# Même résultat mais nb_cartes a une mauvaise valeur (régression à détecter).
_LONG_BUGGY = [
    {"indicateur": "nb_cartes", "valeur": 9999},
    {"indicateur": "nb_clients", "valeur": 1200},
]


def _con(rows):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return con


def test_unscoped_and_form_fails_on_other_rows():
    """Reproduit le bug : la forme AND remonte les 4 lignes des autres indicateurs."""
    cond = "indicateur = 'nb_cartes' AND valeur = 2974"
    res = _evaluate_assertions(
        [{"expected_condition": cond, "sql": _assertion_sql_from_condition(cond)}],
        "v",
        _con(_LONG),
    )[0]
    assert res["passed"] is False
    assert len(res["failing_rows"]) == 4
    assert all(r["indicateur"] != "nb_cartes" for r in res["failing_rows"])


def test_autoscope_conjunction_splits_selector_from_value():
    """Le sélecteur de label part en scope, le test de valeur reste en condition."""
    split = _autoscope_conjunction(
        "indicateur = 'nb_cartes' AND valeur = 2974", "v", _con(_LONG)
    )
    assert split is not None
    scope_sql, cond_sql = split
    assert "indicateur" in scope_sql and "nb_cartes" in scope_sql
    assert "valeur" in cond_sql and "2974" in cond_sql
    assert "indicateur" not in cond_sql


def test_autoscope_repairs_failing_assertion():
    """`_autoscope_failing_assertions` transforme l'assertion rouge en assertion verte scopée."""
    cond = "indicateur = 'nb_cartes' AND valeur = 2974"
    failing = _evaluate_assertions(
        [
            {
                "description": "Le nombre de cartes vaut 2974.",
                "expected_condition": cond,
                "sql": _assertion_sql_from_condition(cond),
            }
        ],
        "v",
        _con(_LONG),
    )
    repaired = _autoscope_failing_assertions(failing, "v", _con(_LONG))[0]
    assert repaired["passed"] is True
    assert "nb_cartes" in repaired.get("scope", "")
    assert repaired["description"] == "Le nombre de cartes vaut 2974."


def test_autoscope_preserves_regression_detection():
    """Non-vacuité : sur une sortie buguée (nb_cartes ≠ 2974), l'assertion reste rouge."""
    cond = "indicateur = 'nb_cartes' AND valeur = 2974"
    failing = _evaluate_assertions(
        [{"expected_condition": cond, "sql": _assertion_sql_from_condition(cond)}],
        "v",
        _con(_LONG_BUGGY),
    )
    repaired = _autoscope_failing_assertions(failing, "v", _con(_LONG_BUGGY))[0]
    # La valeur réelle (9999) ne satisfait pas la condition dans le scope nb_cartes :
    # aucune partition ne rend l'assertion verte → laissée intacte (en échec).
    assert repaired["passed"] is False


def test_autoscope_leaves_universal_invariant_untouched():
    """Un invariant vrai sur TOUTES les lignes n'est pas une cible (il passe déjà)."""
    cond = "valeur > 0"
    passing = _evaluate_assertions(
        [{"expected_condition": cond, "sql": _assertion_sql_from_condition(cond)}],
        "v",
        _con(_LONG),
    )
    assert passing[0]["passed"] is True
    out = _autoscope_failing_assertions(passing, "v", _con(_LONG))
    assert out[0]["passed"] is True
    assert not out[0].get("scope")


def test_autoscope_no_split_for_single_predicate():
    """Pas d'AND → rien à relever : `_autoscope_conjunction` renvoie None."""
    assert _autoscope_conjunction("valeur = 999", "v", _con(_LONG)) is None


def test_autoscope_skips_assertion_with_existing_scope():
    """Une assertion déjà scopée n'est pas re-traitée."""
    cond = "indicateur = 'nb_cartes' AND valeur = 2974"
    failing = [
        {
            "expected_condition": cond,
            "scope": "indicateur IS NOT NULL",
            "sql": _assertion_sql_from_condition(cond, "indicateur IS NOT NULL"),
            "passed": False,
            "failing_rows": [],
        }
    ]
    out = _autoscope_failing_assertions(failing, "v", _con(_LONG))
    # Inchangée : on ne touche pas à un scope déjà posé par le générateur.
    assert out[0]["scope"] == "indicateur IS NOT NULL"
