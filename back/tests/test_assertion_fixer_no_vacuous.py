"""Régression bq002 — le fixer d'assertions ne doit JAMAIS blanchir une assertion
qui échoue légitimement en une tautologie qui « passe » sans rien tester.

Incident : pour l'hebdo attendu 2.0M, le LLM avait généré `expected_condition: "max_revenue = 2"`.
Le résultat réel valait 1.0 partout → l'assertion échoue (correct). Le fixer a alors accepté
une réécriture en SQL libre :
    SELECT * FROM __result__ WHERE max_revenue = 2 AND (SELECT COUNT(*) FROM __result__ WHERE max_revenue = 2) = 0
clause qui ne peut JAMAIS remonter de ligne → « passe » → le test ne valide plus rien.

Invariant attendu : après passage du fixer, une assertion ne doit pas se retrouver
`passed=True` par vacuité. Si la réécriture ne tient pas sur le résultat réel, on garde
l'assertion d'origine en échec (verdict bad_assertions honnête).
"""

import json

import duckdb
import pandas as pd
import pytest
from langchain_core.messages import AIMessage

from build_query.examples_executor import (
    _fix_logically_failing_assertions,
    _is_valid_positive_condition,
)

# Résultat réel : la requête produit 1.0 partout (pas 2.0 comme la description l'annonçait).
_RESULT_ROWS = [{"max_revenue": 1.0}, {"max_revenue": 1.0}, {"max_revenue": 1.0}]


class _FakeLLM:
    """Renvoie une réponse en file ; enregistre les messages reçus."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _laundering_response() -> AIMessage:
    """Le fixer LLM tente de blanchir l'assertion échouée en tautologie auto-contradictoire."""
    return AIMessage(
        content=json.dumps(
            {
                "decisions": [
                    {
                        "id": 0,
                        "correct": False,
                        "description": "Le revenu hebdomadaire est présent.",
                        "sql": (
                            "SELECT * FROM __result__ WHERE max_revenue = 2 "
                            "AND (SELECT COUNT(*) FROM __result__ WHERE max_revenue = 2) = 0"
                        ),
                    }
                ]
            }
        )
    )


# ---------------------------------------------------------------------------
# Garde 1 — _is_valid_positive_condition : filtrage via l'AST sqlglot (pas une regex).
# Un littéral chaîne ne doit pas être confondu avec une clause négative, et `IS NOT NULL`
# (présence positive) reste autorisé.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "cond",
    [
        "max_revenue = 1",
        "amount > 0",
        "date = '2016-01-02'",
        "z_score = (SELECT MAX(z_score) FROM __result__)",
        "x IS NOT NULL",  # présence positive — autorisée
        "x IS NOT DISTINCT FROM 2",  # égalité NULL-safe = affirmation positive
        "status = 'is null'",  # littéral chaîne, PAS une clause IS NULL (régression regex)
        "label = 'not in stock'",  # littéral chaîne, PAS un NOT IN (régression regex)
        "  amount > 0 ;  ",  # tolère espaces et point-virgule de tête/queue
        "TRUE AND amount > 0",  # simplify → amount > 0 (positive)
        "FALSE OR amount > 0",  # simplify → amount > 0 (positive)
        "amount > 0 AND status = 'active'",  # composé positif — pas une tautologie
        "x = x AND y = 2",  # AND avec une vraie contrainte (y = 2) → teste qqch
    ],
)
def test_condition_positive_acceptee(cond):
    assert _is_valid_positive_condition(cond) is True


@pytest.mark.parametrize(
    "cond",
    [
        "",
        "   ",
        "x != 2",
        "x <> 2",
        "x IS DISTINCT FROM 2",  # != NULL-safe → forme négative déguisée
        "x NOT IN (1, 2)",
        "x NOT IN (SELECT y FROM __result__)",  # NOT IN avec sous-requête
        "NOT (x = 2)",
        "x NOT LIKE 'a%'",
        "x NOT ILIKE 'a%'",
        "x NOT BETWEEN 1 AND 5",
        "x IS NULL",
        "SELECT * FROM __result__ WHERE x = 2",  # requête, pas une expression
        "x = 2 AND (SELECT COUNT(*) FROM __result__ WHERE x = 2) = ",  # non parsable
    ],
)
def test_condition_negative_ou_invalide_rejetee(cond):
    assert _is_valid_positive_condition(cond) is False


@pytest.mark.parametrize(
    "cond",
    [
        "1 = 1",  # constante booléenne après simplify
        "TRUE",
        "1 < 2",
        "x = x",  # opérandes identiques → toujours vraie
        "x >= x",
        "x > x",  # toujours fausse → (faux) IS NOT TRUE toujours vrai → passe sans tester
        "(x = x)",  # parenthèses de tête
        "lower(label) = lower(label)",
        "x = x AND y = y",  # AND tautologique : tous les opérandes vacuité
        "x = x OR y > 5",  # OR tautologique : un opérande vacuité suffit
        "x >= x AND y <= y",  # composé sur plusieurs familles same-operand
    ],
)
def test_tautologie_rejetee(cond):
    """Garde anti-tautologie : seule classe de vacuité que la ré-exécution (Garde 2)
    laisse passer — une tautologie « passe » sur n'importe quelles données."""
    assert _is_valid_positive_condition(cond) is False


async def test_fixer_rejette_blanchiment_en_tautologie(monkeypatch):
    fake = _FakeLLM([_laundering_response()])
    monkeypatch.setattr("build_query.examples_executor.make_llm", lambda: fake)

    result_df = pd.DataFrame(_RESULT_ROWS)
    view_name = "__result__sess1"
    con = duckdb.connect()
    con.register(view_name, result_df)

    # Assertion d'origine : forme positive correcte, mais elle échoue car le résultat
    # réel ne contient pas max_revenue = 2.
    failing = [
        {
            "description": "Le revenu hebdomadaire est de deux millions.",
            "expected_condition": "max_revenue = 2",
            "sql": "SELECT * FROM __result__ WHERE (max_revenue = 2) IS NOT TRUE",
            "passed": False,
            "failing_rows": _RESULT_ROWS,
        }
    ]

    fixed = await _fix_logically_failing_assertions(
        failing,
        view_name=view_name,
        con=con,
        duckdb_sql="SELECT max_revenue FROM ...",
        test_data=[],
        result_df=result_df,
        test_description="Revenu hebdo attendu 2.0M",
    )

    a = fixed[0]
    # L'assertion ne doit pas avoir été blanchie : elle reste en échec (honnête).
    assert a["passed"] is False, (
        "Le fixer a blanchi une assertion échouée en tautologie qui « passe »"
    )
    # Et elle ne doit pas exposer une expected_condition vide + SQL brut auto-contradictoire.
    assert "COUNT(*)" not in (a.get("sql") or ""), (
        "Le fixer a accepté du SQL libre auto-référent au lieu d'une condition positive"
    )


async def test_fixer_applique_un_vrai_fix_de_logique(monkeypatch):
    """Chemin positif : si le LLM corrige une vraie erreur de logique avec une
    condition positive qui passe sur le résultat réel, le fix est bien appliqué
    (la garde anti-vacuité ne doit pas tout bloquer)."""
    response = AIMessage(
        content=json.dumps(
            {
                "decisions": [
                    {
                        "id": 0,
                        "correct": False,
                        "description": "Le revenu hebdomadaire est d'un million.",
                        "expected_condition": "max_revenue = 1",
                    }
                ]
            }
        )
    )
    fake = _FakeLLM([response])
    monkeypatch.setattr("build_query.examples_executor.make_llm", lambda: fake)

    result_df = pd.DataFrame(_RESULT_ROWS)
    view_name = "__result__sess2"
    con = duckdb.connect()
    con.register(view_name, result_df)

    failing = [
        {
            "description": "Le revenu hebdomadaire est de deux millions.",
            "expected_condition": "max_revenue = 2",
            "sql": "SELECT * FROM __result__ WHERE (max_revenue = 2) IS NOT TRUE",
            "passed": False,
            "failing_rows": _RESULT_ROWS,
        }
    ]

    fixed = await _fix_logically_failing_assertions(
        failing,
        view_name=view_name,
        con=con,
        duckdb_sql="SELECT max_revenue FROM ...",
        test_data=[],
        result_df=result_df,
        test_description="Revenu hebdo",
    )

    a = fixed[0]
    assert a["passed"] is True
    assert a["expected_condition"] == "max_revenue = 1"
    # Le SQL exécuté est dérivé mécaniquement de la condition (forme IS NOT TRUE).
    assert "IS NOT TRUE" in a["sql"]
