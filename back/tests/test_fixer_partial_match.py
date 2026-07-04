"""Régression (audit c6.sql, P1-3) — signal utile au fixer d'assertions.

Pour une assertion `exists` sans match (ou `all` scopée sur un périmètre vide), le fixer ne
voyait que « Lignes remontées : [] » — signal vide, il devait deviner le conjoint fautif (sur
c6 : 4 assertions `exists` cassées par un seul conjoint `partition_date`). `_closest_partial_match`
trouve le plus grand sous-ensemble de conjoints satisfiable et renvoie les lignes les plus
proches + le(s) conjoint(s) retiré(s) — le conjoint fautif saute alors aux yeux.
"""

import duckdb
import pandas as pd

from build_query.examples_executor import _closest_partial_match


def _con(rows):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return con


def test_isolates_culprit_conjunct():
    # `valeur = 999` ne matche rien ; `indicateur = 'nb_cartes'` oui → on droppe `valeur`.
    con = _con(
        [
            {"indicateur": "nb_cartes", "valeur": 2974},
            {"indicateur": "nb_ope", "valeur": 100},
        ]
    )
    out = _closest_partial_match("indicateur = 'nb_cartes' AND valeur = 999", "v", con)
    assert out is not None
    near, dropped = out
    assert "valeur = 999" in dropped
    assert len(near) == 1
    assert near[0]["indicateur"] == "nb_cartes"


def test_returns_none_for_single_conjunct():
    con = _con([{"x": 1}])
    assert _closest_partial_match("x = 999", "v", con) is None


def test_returns_none_when_nothing_partially_matches():
    con = _con([{"a": 1, "b": 2}])
    # Ni `a = 9` ni `b = 9` ne matchent → aucun sous-ensemble non vide satisfiable.
    assert _closest_partial_match("a = 9 AND b = 9", "v", con) is None


def test_respects_scope():
    con = _con(
        [
            {"grp": "A", "indicateur": "x", "valeur": 5},
            {"grp": "B", "indicateur": "x", "valeur": 999},
        ]
    )
    # Scopé sur grp=A : la ligne B (qui matcherait valeur=999) est hors périmètre.
    out = _closest_partial_match(
        "indicateur = 'x' AND valeur = 999", "v", con, scope="grp = 'A'"
    )
    assert out is not None
    near, dropped = out
    assert all(r["grp"] == "A" for r in near)
