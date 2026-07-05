"""Garde anti-vacuité sur RÉSULTAT VIDE (P0-1, incident c2 BPCE).

Une assertion `all` non scopée est de la forme ``SELECT * FROM __result__ WHERE (cond)
IS NOT TRUE`` : sur une vue résultat VIDE elle retourne 0 ligne violante → « passe » à
tort. Conséquence : une régression SQL qui vide la sortie (la plus courante) shipperait
au vert. On la fait échouer explicitement — SAUF si le scénario attend un vide (présence
de la sentinelle ``SELECT * FROM __result__`` émise par test_evaluator pour les tests à
plage vide intentionnelle).

Cf. assertion_eval._evaluate_assertions / _is_empty_intent_sentinel et
test_evaluator (empty_assertion, ligne « table vide intentionnelle »).
"""

import duckdb

from build_query.assertion_eval import _evaluate_assertions


def _empty_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE __result__ (total INTEGER, payment_type TEXT)")
    return con


def _nonempty_con():
    con = duckdb.connect()
    con.execute("CREATE TABLE __result__ (total INTEGER, payment_type TEXT)")
    con.execute("INSERT INTO __result__ VALUES (150, 'Credit Card')")
    return con


def test_all_assertion_on_empty_result_is_vacuous_and_fails():
    """Le cœur de P0-1 : `all` non scopé sur vue vide → échec « vacante », pas un pass."""
    res = _evaluate_assertions(
        [
            {
                "description": "le total carte vaut 150",
                "expected_condition": "total = 150",
                "sql": "SELECT * FROM __result__ WHERE (total = 150) IS NOT TRUE",
            }
        ],
        "__result__",
        _empty_con(),
    )[0]
    assert res["passed"] is False
    assert "vacante" in res["error"]


def test_empty_intent_sentinel_passes_on_empty_result():
    """La sentinelle « plage vide intentionnelle » (SELECT * FROM __result__ nu) doit
    PASSER sur un résultat vide — c'est exactement ce qu'elle affirme, pas une vacuité."""
    res = _evaluate_assertions(
        [
            {
                "description": "La requête doit retourner 0 ligne (table vide intentionnelle)",
                "sql": "SELECT * FROM __result__",
            }
        ],
        "__result__",
        _empty_con(),
    )[0]
    assert res["passed"] is True


def test_empty_intent_sentinel_disables_vacancy_guard_for_whole_suite():
    """Quand le scénario attend un vide (sentinelle présente), les autres assertions `all`
    ne doivent PAS être requalifiées vacantes — le vide est voulu pour toute la suite."""
    res = _evaluate_assertions(
        [
            {
                "description": "table vide intentionnelle",
                "sql": "SELECT * FROM __result__",
            },
            {
                "description": "aucun paiement carte",
                "expected_condition": "payment_type <> 'Credit Card'",
                "sql": (
                    "SELECT * FROM __result__ "
                    "WHERE (payment_type <> 'Credit Card') IS NOT TRUE"
                ),
            },
        ],
        "__result__",
        _empty_con(),
    )
    assert all(a["passed"] for a in res)


def test_non_empty_result_unaffected_by_guard():
    """Rétrocompat : sur un résultat NON vide, la garde est un no-op — l'assertion
    valide passe normalement."""
    res = _evaluate_assertions(
        [
            {
                "description": "le total carte vaut 150",
                "expected_condition": "total = 150",
                "sql": "SELECT * FROM __result__ WHERE (total = 150) IS NOT TRUE",
            }
        ],
        "__result__",
        _nonempty_con(),
    )[0]
    assert res["passed"] is True


def test_scoped_assertion_on_empty_uses_scope_guard_not_vacancy():
    """Une assertion SCOPÉE sur vue vide était déjà couverte par la garde de scope
    (le périmètre ne sélectionne aucune ligne) — on vérifie qu'elle échoue toujours,
    peu importe le libellé exact, sans régression introduite par la garde de vacuité."""
    res = _evaluate_assertions(
        [
            {
                "description": "le total carte vaut 150",
                "expected_condition": "total = 150",
                "scope": "payment_type = 'Credit Card'",
                "sql": (
                    "SELECT * FROM __result__ "
                    "WHERE (payment_type = 'Credit Card') AND ((total = 150) IS NOT TRUE)"
                ),
            }
        ],
        "__result__",
        _empty_con(),
    )[0]
    assert res["passed"] is False
    assert "error" in res
