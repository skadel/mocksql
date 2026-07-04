"""Colonnes mots réservés DuckDB (`offset`, `end`, …) dans le SQL d'assertion.

Incident c6 : la requête produit une colonne `offset` (UNNEST(...) WITH OFFSET AS offset).
Le LLM référence la colonne nue dans chaque assertion → `Parser Error: syntax error at or
near "offset"` sur TOUTE la suite → boucles de régénération/correction qui thrashent sans
converger (aucun modèle ne sait que le mot est réservé). Garde déterministe : le SQL est
re-rendu par sqlglot au point d'exécution unique (`_evaluate_assertions`), ce qui quote
mécaniquement les mots réservés — et eux seuls. Couvre tous les chemins : génération
initiale, fixer, SQL brut régénéré, autoscope, et assertions déjà persistées (rerun).
"""

import duckdb
import pandas as pd

from build_query.assertion_eval import _evaluate_assertions

_ROWS = [
    {"indicator": "nb_ope", "offset": 13, "end": "x", "valeur": 500},
    {"indicator": "mt_ope", "offset": 7, "end": "y", "valeur": 100},
]


def _con(rows=_ROWS):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return con


def test_all_mode_reserved_column_executes():
    """Mode all : `offset` nu ne doit plus produire de Parser Error."""
    res = _evaluate_assertions(
        [
            {
                "expected_condition": "offset > 0",
                "sql": "SELECT * FROM __result__ WHERE (offset > 0) IS NOT TRUE",
            }
        ],
        "v",
        _con(),
    )[0]
    assert res.get("error") is None
    assert res["passed"] is True
    # Le SQL restitué (persisté/affiché) est la forme quotée, valide en DuckDB.
    assert '"offset"' in res["sql"]


def test_exists_mode_reserved_column_executes():
    """Mode exists (forme du builder) : condition avec `offset` nu exécutable."""
    res = _evaluate_assertions(
        [
            {
                "expected_condition": "indicator = 'nb_ope' AND offset = 13",
                "quantifier": "exists",
                "sql": (
                    "SELECT 1 AS _no_match WHERE NOT EXISTS "
                    "(SELECT 1 FROM __result__ WHERE (indicator = 'nb_ope' AND offset = 13))"
                ),
            }
        ],
        "v",
        _con(),
    )[0]
    assert res.get("error") is None
    assert res["passed"] is True


def test_scoped_assertion_with_reserved_scope():
    """Le scope (exécuté à part pour la garde anti-vacuité) est aussi normalisé."""
    res = _evaluate_assertions(
        [
            {
                "expected_condition": "valeur = 500",
                "scope": "offset = 13",
                "sql": (
                    "SELECT * FROM __result__ "
                    "WHERE (offset = 13) AND ((valeur = 500) IS NOT TRUE)"
                ),
            }
        ],
        "v",
        _con(),
    )[0]
    assert res.get("error") is None
    assert res["passed"] is True


def test_other_reserved_words_covered():
    """La garde est générique (ex. `end`), pas spécifique à `offset`."""
    res = _evaluate_assertions(
        [
            {
                "expected_condition": "end = 'x'",
                "quantifier": "exists",
                "sql": (
                    "SELECT 1 AS _no_match WHERE NOT EXISTS "
                    "(SELECT 1 FROM __result__ WHERE (end = 'x'))"
                ),
            }
        ],
        "v",
        _con(),
    )[0]
    assert res.get("error") is None
    assert res["passed"] is True


def test_helper_quotes_only_reserved_identifiers():
    """Re-rendu sqlglot : quote les mots réservés et EUX SEULS (pas de sur-quoting)."""
    from build_query.assertion_eval import _quote_reserved_identifiers

    out = _quote_reserved_identifiers("offset = 13 AND indicator = 'nb_ope'")
    assert '"offset" = 13' in out
    assert "indicator = 'nb_ope'" in out


def test_helper_leaves_unparsable_sql_verbatim():
    """Best-effort : SQL non parsable → chaîne inchangée, jamais bloquant."""
    from build_query.assertion_eval import _quote_reserved_identifiers

    broken = "SELECT FROM WHERE (("
    assert _quote_reserved_identifiers(broken) == broken
