"""Pin de cardinalité déterministe — la réponse au faux positif « NULL qui fuit ».

Incident (critique démo) : sur un test « payment_type NULL est exclu », les assertions
générées ne couvraient que la ligne Credit Card. Une régression SQL laissant fuir les
NULL restait verte : les assertions scopées ignorent mécaniquement les lignes en trop.

Parade déterministe (hors LLM) : `assertion_generator` appende mécaniquement une
assertion aggregate `COUNT(*) = N` (N = row_count exact du résultat, jamais tronqué).
Elle passe par construction à la génération ; sa valeur est au replay CI, où toute
dérive de cardinalité (ligne fuyante OU manquante) la fait échouer. Le LLM a interdiction
d'émettre lui-même un pin brut (règle prompt) ; s'il en émet un quand même, la dédup
`_is_bare_rowcount_pin` le retire au profit du pin mécanique.
"""

import json
import uuid

import duckdb
import pandas as pd
from langchain_core.messages import AIMessage

from build_query.assertion_generator import generate_assertions
from build_query.examples_executor import (
    _Assertion,
    _AssertionsAndEvaluation,
    _assertion_sql_from_condition,
    _cardinality_pin,
    _evaluate_assertions,
    _is_bare_rowcount_pin,
    _is_valid_positive_condition,
)
from utils.msg_types import MsgType

_LONG = [
    {"indicateur": "nb_cartes", "valeur": 2974},
    {"indicateur": "nb_clients", "valeur": 1200},
    {"indicateur": "nb_comptes", "valeur": 3050},
]


# ---------------------------------------------------------------------------
# Le pin lui-même
# ---------------------------------------------------------------------------


def test_cardinality_pin_shape():
    pin = _cardinality_pin(3)
    assert pin["description"] == "Le résultat contient exactement 3 ligne(s)."
    assert pin["expected_condition"] == "COUNT(*) = 3"
    assert pin["quantifier"] == "aggregate"
    assert pin["sql"] == _assertion_sql_from_condition(
        "COUNT(*) = 3", None, "aggregate"
    )


def test_cardinality_pin_catches_extra_row():
    """Le pin échoue quand une ligne fuit (régression), passe sur la cardinalité exacte."""
    con = duckdb.connect()
    con.register("v", pd.DataFrame(_LONG))
    ok = _evaluate_assertions([_cardinality_pin(3)], "v", con)[0]
    assert ok["passed"] is True
    ko = _evaluate_assertions([_cardinality_pin(2)], "v", con)[0]
    assert ko["passed"] is False


# ---------------------------------------------------------------------------
# Dédup : pins bruts émis par le LLM malgré la consigne
# ---------------------------------------------------------------------------


def test_bare_rowcount_pin_detected():
    for cond, quantifier in [
        ("COUNT(*) = 3", "aggregate"),
        ("count(*)=3", "aggregate"),
        ("  COUNT( * )  =  3 ", "aggregate"),
        # En mode all, COUNT(*) en WHERE est une erreur DuckDB → assertion morte, à dropper aussi.
        ("COUNT(*) = 3", "all"),
    ]:
        assert _is_bare_rowcount_pin(
            {"expected_condition": cond, "quantifier": quantifier}
        ), cond


def test_non_bare_rowcount_kept():
    for ex in [
        # Combiné à une autre contrainte : porte plus que la cardinalité.
        {
            "expected_condition": "COUNT(*) = 3 AND SUM(valeur) = 7224",
            "quantifier": "aggregate",
        },
        # Scopé : cardinalité d'un sous-ensemble, pas le pin global.
        {
            "expected_condition": "COUNT(*) = 1",
            "quantifier": "aggregate",
            "scope": "indicateur = 'nb_cartes'",
        },
        # COUNT non-star : compte autre chose que les lignes.
        {
            "expected_condition": "COUNT(DISTINCT indicateur) = 3",
            "quantifier": "aggregate",
        },
        {"expected_condition": "SUM(valeur) = 7224", "quantifier": "aggregate"},
    ]:
        assert not _is_bare_rowcount_pin(ex), ex["expected_condition"]


# ---------------------------------------------------------------------------
# Tautologies d'agrégat (garde du fixer)
# ---------------------------------------------------------------------------


def test_aggregate_tautologies_rejected():
    assert _is_valid_positive_condition("COUNT(*) >= 0") is False
    assert _is_valid_positive_condition("COUNT(*) > -1") is False
    assert _is_valid_positive_condition("0 <= COUNT(*)") is False


def test_legitimate_count_conditions_accepted():
    """`COUNT(*) >= 1` (non-vacuité) et `COUNT(*) = N` (pin) contraignent vraiment."""
    assert _is_valid_positive_condition("COUNT(*) >= 1") is True
    assert _is_valid_positive_condition("COUNT(*) = 3") is True


# ---------------------------------------------------------------------------
# Scénario de la critique démo : le pin attrape le NULL qui fuit
# ---------------------------------------------------------------------------

_DEMO_OK = [
    {"payment_type": "Credit Card", "nb": 2, "total": 30},
    {"payment_type": "Cash", "nb": 1, "total": 10},
]
_DEMO_LEAK = _DEMO_OK + [{"payment_type": None, "nb": 1, "total": 5}]


def _demo_suite():
    scoped = {
        "description": "Deux courses par carte de crédit.",
        "expected_condition": "nb = 2",
        "scope": "payment_type = 'Credit Card'",
        "sql": _assertion_sql_from_condition("nb = 2", "payment_type = 'Credit Card'"),
    }
    anchor = {
        "description": "La ligne des paiements en espèces est présente.",
        "expected_condition": "payment_type = 'Cash' AND total = 10",
        "quantifier": "exists",
        "sql": _assertion_sql_from_condition(
            "payment_type = 'Cash' AND total = 10", None, "exists"
        ),
    }
    return [scoped, anchor, _cardinality_pin(2)]


def test_demo_suite_green_on_correct_result():
    con = duckdb.connect()
    con.register("v", pd.DataFrame(_DEMO_OK))
    results = _evaluate_assertions(_demo_suite(), "v", con)
    assert all(r["passed"] for r in results)


def test_demo_null_leak_caught_only_by_pin():
    """Régression « le WHERE payment_type IS NOT NULL saute » : les assertions ciblées
    restent vertes (le scope ignore la ligne NULL) — SEUL le pin échoue. C'est lui qui
    transforme le faux positif de la démo en vrai rouge."""
    con = duckdb.connect()
    con.register("v", pd.DataFrame(_DEMO_LEAK))
    scoped, anchor, pin = _evaluate_assertions(_demo_suite(), "v", con)
    assert scoped["passed"] is True
    assert anchor["passed"] is True
    assert pin["passed"] is False


# ---------------------------------------------------------------------------
# Intégration : le nœud generate_assertions appende le pin et dédoublonne
# ---------------------------------------------------------------------------


async def test_generate_assertions_appends_pin_and_dedupes(monkeypatch):
    fake_eval = _AssertionsAndEvaluation(
        reasoning="ok",
        assertions=[
            _Assertion(
                description="Le nombre de cartes vaut 2974.",
                expected_condition="valeur = 2974",
                scope="indicateur = 'nb_cartes'",
            ),
            # Doublon interdit par le prompt : doit être retiré au profit du pin mécanique.
            _Assertion(
                description="Le résultat a trois lignes.",
                expected_condition="COUNT(*) = 3",
                quantifier="aggregate",
            ),
        ],
        verdict="Bon",
        reason_type=None,
        explanation="Les données couvrent le scénario.",
    )

    async def _fake_generate(*args, **kwargs):
        return fake_eval

    monkeypatch.setattr(
        "build_query.examples_executor._generate_assertions_and_evaluate",
        _fake_generate,
    )

    test = {
        "test_index": "1",
        "status": "complete",
        "results_json": json.dumps(_LONG),
        "data": {},
        "unit_test_description": "Trois indicateurs agrégés.",
    }
    state = {
        "messages": [
            AIMessage(
                content=json.dumps([test]),
                id=str(uuid.uuid4()),
                additional_kwargs={"type": MsgType.RESULTS},
            )
        ],
        "session": "sess-pin-1",
        "test_index": "1",
        "query": "SELECT indicateur, valeur FROM t",
        "optimized_sql": "SELECT indicateur, valeur FROM t",
        "request_id": "req-pin",
    }

    update = await generate_assertions(state)

    updated_test = json.loads(update["messages"][0].content)[0]
    results = updated_test["assertion_results"]
    conditions = [r["expected_condition"] for r in results]

    # Le doublon LLM est retiré, le pin mécanique est appendé en dernier.
    assert conditions.count("COUNT(*) = 3") == 1
    pin = results[-1]
    assert pin["description"] == "Le résultat contient exactement 3 ligne(s)."
    assert pin["quantifier"] == "aggregate"
    assert pin["passed"] is True
    # L'assertion métier du LLM est conservée et passe.
    assert results[0]["expected_condition"] == "valeur = 2974"
    assert results[0]["passed"] is True
    assert updated_test["verdict"] == "Bon"
