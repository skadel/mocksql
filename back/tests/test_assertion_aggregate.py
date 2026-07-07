"""Mode d'assertion AGGREGATE (« une propriété globale du résultat est vraie »).

Complément des modes `all` (invariant sur chaque ligne) et `exists` (présence d'une
ligne) : la condition porte sur des AGRÉGATS de `__result__` (`SUM(revenue) = 40`,
`COUNT(*) = 3`) — c'est la forme qui attrape les régressions de CARDINALITÉ (lignes en
trop qui fuient un filtre, lignes manquantes), invisibles pour une assertion scopée sur
une ligne précise (le scope filtre justement les lignes parasites).

Non-vacuité naturelle sur résultat vide : un agrégat sans GROUP BY produit toujours une
ligne scalaire — `SUM → NULL → IS NOT TRUE → violation`, `COUNT(*) = 0 → pass`. La garde
anti-vacuité « résultat vide » ne doit donc PAS force-fail les assertions aggregate.
"""

import json

import duckdb
import pandas as pd
from langchain_core.messages import AIMessage

from build_query.examples_executor import (
    _Assertion,
    _assertion_sql_from_condition,
    _assertion_to_executable,
    _autoscope_failing_assertions,
    _evaluate_assertions,
    _fix_logically_failing_assertions,
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


def _empty_con():
    con = duckdb.connect()
    con.register(
        "v",
        pd.DataFrame(
            {
                "indicateur": pd.Series(dtype="object"),
                "valeur": pd.Series(dtype="int64"),
            }
        ),
    )
    return con


# ---------------------------------------------------------------------------
# Forme SQL
# ---------------------------------------------------------------------------


def test_aggregate_sql_shape():
    """Le mode aggregate wrappe la condition en sous-requête scalaire IS NOT TRUE."""
    sql = _assertion_sql_from_condition("SUM(valeur) = 7224", quantifier="aggregate")
    assert sql == (
        "SELECT 1 AS _agg_violation WHERE "
        "(SELECT (SUM(valeur) = 7224) FROM __result__) IS NOT TRUE"
    )


def test_aggregate_sql_folds_scope_into_from():
    """Un scope éventuel restreint le périmètre de l'agrégat (WHERE dans le FROM)."""
    sql = _assertion_sql_from_condition(
        "SUM(valeur) = 2974", scope="indicateur = 'nb_cartes'", quantifier="aggregate"
    )
    assert "FROM __result__ WHERE (indicateur = 'nb_cartes')" in sql
    assert "(SELECT (SUM(valeur) = 2974) FROM __result__ WHERE" in sql
    assert "IS NOT TRUE" in sql


def test_assertion_to_executable_threads_aggregate():
    """`_assertion_to_executable` propage le quantifier aggregate dans le dict et le SQL."""
    a = _Assertion(
        description="La somme totale vaut 7224.",
        expected_condition="SUM(valeur) = 7224",
        quantifier="aggregate",
    )
    ex = _assertion_to_executable(a)
    assert ex["quantifier"] == "aggregate"
    assert "_agg_violation" in ex["sql"]


# ---------------------------------------------------------------------------
# Exécution (et replay CI : le dict stocké est rejoué tel quel, sql compris)
# ---------------------------------------------------------------------------


def _eval_one(cond, rows, scope=None):
    return _evaluate_assertions(
        [
            {
                "expected_condition": cond,
                **({"scope": scope} if scope else {}),
                "quantifier": "aggregate",
                "sql": _assertion_sql_from_condition(cond, scope, "aggregate"),
            }
        ],
        "v",
        _con(rows) if rows else _empty_con(),
    )[0]


def test_aggregate_passes_on_correct_sum():
    res = _eval_one("SUM(valeur) = 7224", _LONG)
    assert res["passed"] is True
    assert res["quantifier"] == "aggregate"


def test_aggregate_fails_on_wrong_sum():
    res = _eval_one("SUM(valeur) = 9999", _LONG)
    assert res["passed"] is False
    assert not res.get("error")


def test_aggregate_count_passes():
    res = _eval_one("COUNT(*) = 3", _LONG)
    assert res["passed"] is True


def test_aggregate_scoped_count():
    """L'agrégat scopé porte sur les seules lignes du scope."""
    res = _eval_one("COUNT(*) = 1", _LONG, scope="indicateur = 'nb_cartes'")
    assert res["passed"] is True


def test_aggregate_failing_exposes_no_synthetic_row():
    """La ligne sentinelle `_agg_violation` n'est pas un contre-exemple lisible."""
    res = _eval_one("SUM(valeur) = 9999", _LONG)
    assert res["passed"] is False
    assert res["failing_rows"] == []


# ---------------------------------------------------------------------------
# Résultat vide : non-vacuité naturelle, pas de force-fail par la garde
# ---------------------------------------------------------------------------


def test_aggregate_count_zero_passes_on_empty_result():
    """`COUNT(*) = 0` en aggregate est une vraie affirmation de vide → passe, sans
    être force-failée « vacante » (l'agrégat produit toujours une ligne scalaire)."""
    res = _eval_one("COUNT(*) = 0", None)
    assert res["passed"] is True
    assert not res.get("error")


def test_aggregate_sum_fails_naturally_on_empty_result():
    """`SUM(x) = 40` sur vide → NULL → IS NOT TRUE → échec NATUREL (sans `error`) :
    c'est le signal honnête d'une sortie vidée par une régression."""
    res = _eval_one("SUM(valeur) = 40", None)
    assert res["passed"] is False
    assert not res.get("error")


def test_aggregate_scope_empty_still_guarded():
    """Non-régression : un scope qui ne sélectionne aucune ligne reste rejeté comme
    vacant, même en aggregate (assertion d'absence déguisée — interdite)."""
    res = _eval_one("COUNT(*) = 0", _LONG, scope="indicateur = 'inexistant'")
    assert res["passed"] is False
    assert "vacante" in (res.get("error") or "")


# ---------------------------------------------------------------------------
# Autoscope : ne doit relever en scope QUE le mode all
# ---------------------------------------------------------------------------


def _failing(cond, quantifier):
    return {
        "description": "d",
        "expected_condition": cond,
        "quantifier": quantifier,
        "sql": _assertion_sql_from_condition(cond, None, quantifier),
        "passed": False,
        "failing_rows": [],
    }


def test_autoscope_skips_aggregate():
    """Une assertion aggregate échouante ne doit pas être mangle-ée en all scopé
    (le rebuild de l'autoscope hard-wire le mode all et perdrait le quantifier)."""
    a = _failing("indicateur = 'nb_cartes' AND valeur = 2974", "aggregate")
    out = _autoscope_failing_assertions([dict(a)], "v", _con(_LONG))[0]
    assert out["quantifier"] == "aggregate"
    assert out["sql"] == a["sql"]
    assert not out.get("scope")


def test_autoscope_skips_exists():
    """Même garde pour exists (trou latent) : le quantifier doit survivre."""
    a = _failing("indicateur = 'nb_cartes' AND valeur = 2974", "exists")
    out = _autoscope_failing_assertions([dict(a)], "v", _con(_LONG))[0]
    assert out["quantifier"] == "exists"
    assert out["sql"] == a["sql"]
    assert not out.get("scope")


# ---------------------------------------------------------------------------
# Fixer LLM : une décision aggregate ne doit pas être coercée en all
# ---------------------------------------------------------------------------


class _FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)

    async def ainvoke(self, messages):
        return self._responses.pop(0)


async def test_fixer_accepts_aggregate_quantifier(monkeypatch):
    """Le fixer peut réparer une assertion en mode aggregate (allow-list élargie) :
    la coercition silencieuse en all produirait un agrégat en WHERE → erreur DuckDB."""
    response = AIMessage(
        content=json.dumps(
            {
                "decisions": [
                    {
                        "id": 0,
                        "correct": False,
                        "description": "La somme totale vaut 7224.",
                        "expected_condition": "SUM(valeur) = 7224",
                        "quantifier": "aggregate",
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(
        "build_query.examples_executor.make_llm", lambda: _FakeLLM([response])
    )

    result_df = pd.DataFrame(_LONG)
    con = duckdb.connect()
    con.register("v", result_df)

    failing = [
        {
            "description": "La somme totale vaut 9999.",
            "expected_condition": "SUM(valeur) = 9999",
            "quantifier": "aggregate",
            "sql": _assertion_sql_from_condition(
                "SUM(valeur) = 9999", None, "aggregate"
            ),
            "passed": False,
            "failing_rows": [],
        }
    ]

    fixed = await _fix_logically_failing_assertions(
        failing,
        view_name="v",
        con=con,
        duckdb_sql="SELECT ...",
        test_data=[],
        result_df=result_df,
        test_description="Somme totale des indicateurs",
    )

    a = fixed[0]
    assert a["passed"] is True
    assert a["quantifier"] == "aggregate"
    assert "_agg_violation" in a["sql"]
