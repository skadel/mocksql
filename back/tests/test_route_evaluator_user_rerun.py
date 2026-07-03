"""
Régression : le bouton « Relancer » (`user_rerun`) est en LECTURE SEULE dans route_evaluator.

`rerun_all_tests` est partagé avec la mise à jour SQL (front : `context === 'sql_update'
|| rerunAll`) — le backend ne peut pas en dériver l'intention. Le flag dédié `user_rerun`
(posé uniquement par le bouton « Relancer ») garantit qu'une simple relance ré-émet les
verdicts recalculés puis clôt via final_response, sans jamais entrer dans les boucles de
correction (bad_data_to_agent / assertion_corrector / bad_data_exhausted) qui réécriraient
données ou assertions. La mise à jour SQL, elle, garde ses boucles de réparation.
"""

from build_query.query_chain import route_evaluator


def _state(**overrides):
    state = {
        "evaluation_feedback": None,
        "gen_retries": 5,
        "debug_retries": 2,
        "has_existing_tests": True,
        "rerun_all_tests": True,
    }
    state.update(overrides)
    return state


class TestUserRerunReadOnly:
    def test_bad_data_does_not_enter_correction_loop(self):
        s = _state(user_rerun=True, evaluation_feedback="bad_data")
        assert route_evaluator(s) == "final_response"

    def test_bad_data_retries_exhausted_still_closes(self):
        s = _state(user_rerun=True, evaluation_feedback="bad_data", gen_retries=0)
        assert route_evaluator(s) == "final_response"

    def test_bad_assertions_does_not_enter_corrector(self):
        s = _state(user_rerun=True, evaluation_feedback="bad_assertions")
        assert route_evaluator(s) == "final_response"

    def test_clean_rerun_closes_via_final_response(self):
        s = _state(user_rerun=True)
        assert route_evaluator(s) == "final_response"


class TestSqlUpdateKeepsRepairLoops:
    """rerun_all_tests SANS user_rerun = mise à jour SQL : boucles de réparation inchangées."""

    def test_bad_data_still_routes_to_agent(self):
        s = _state(evaluation_feedback="bad_data")
        assert route_evaluator(s) == "bad_data_to_agent"

    def test_bad_assertions_still_routes_to_corrector(self):
        s = _state(evaluation_feedback="bad_assertions")
        assert route_evaluator(s) == "assertion_corrector"
