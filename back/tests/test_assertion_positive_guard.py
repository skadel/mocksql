"""Régression (audit c6.sql, P1-2) — garde structurelle des conditions positives à la
GÉNÉRATION initiale.

Le générateur est censé n'émettre que des `expected_condition` POSITIVES, mais il lui arrive
d'émettre une forme négative (`IS NULL` nu, `!=`, `NOT IN`…) malgré l'interdit du prompt (1
assertion sur le run c6). Une fois enveloppée en `(cond) IS NOT TRUE`, une telle condition
peut « passer » à vide (ex. `x IS NULL` sur une colonne entièrement NULL → 0 ligne violante).

Jusqu'ici seules les voies fixer (`_fix_logically_failing_assertions`) et relevage
(`_autoscope_conjunction`) validaient la positivité via `_is_valid_positive_condition` ;
`_assertion_to_executable` (chemin de génération) l'ignorait. La garde échoue désormais
explicitement une condition non-positive → l'assertion entre dans la boucle du fixer (qui la
régénère en forme positive) au lieu d'un faux vert.
"""

import duckdb
import pandas as pd

from build_query.assertion_eval import _evaluate_assertions
from build_query.examples_executor import _Assertion, _assertion_to_executable


def _eval(cond: str, rows: list, quantifier: str = "all", scope=None):
    a = _Assertion(
        description="d", expected_condition=cond, quantifier=quantifier, scope=scope
    )
    executable = _assertion_to_executable(a)
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return executable, _evaluate_assertions([executable], "v", con)[0]


def test_negative_is_null_does_not_pass_vacuously():
    # `x IS NULL` sur une colonne entièrement NULL passerait à vide sans la garde.
    _, res = _eval("x IS NULL", [{"x": None}, {"x": None}])
    assert res["passed"] is False


def test_negative_neq_does_not_pass_vacuously():
    # `x != 5` (négation) sur des données où aucune ligne ne vaut 5.
    _, res = _eval("x != 5", [{"x": 1}, {"x": 2}])
    assert res["passed"] is False


def test_tautology_does_not_pass_vacuously():
    _, res = _eval("1 = 1", [{"x": 1}])
    assert res["passed"] is False


def test_valid_positive_condition_passes_normally():
    # Une condition positive vraie sur toutes les lignes passe (comportement inchangé).
    executable, res = _eval("x = 1", [{"x": 1}, {"x": 1}])
    assert res["passed"] is True
    # Le SQL reste la forme dbt-style normale (pas la sentinelle de rejet).
    assert "IS NOT TRUE" in executable["sql"]


def test_valid_positive_condition_fails_when_violated():
    _, res = _eval("x = 1", [{"x": 1}, {"x": 2}])
    assert res["passed"] is False
