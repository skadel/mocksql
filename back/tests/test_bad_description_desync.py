"""#2 — Désync description ↔ sortie réelle : quand la `unit_test_description` annonce une
valeur de sortie concrète contredite par le résultat réel (bq002 : « hebdo 2.0M » alors que
DuckDB produit 1.0 ; bq143 : « corrélation 0.2 » alors que le réel vaut 0.0), le test doit
être FLAGUÉ (verdict Insuffisant, `reason_type="bad_description"`) — pas blanchi en « Excellent ».

Le flag ne déclenche AUCUNE boucle de correction : la donnée est valide, c'est la 3ᵉ voix
(le narratif) qui ment. Comme `needs_validation`, il sauve l'état et émet un VALIDATION_PROMPT
actionnable (Valider / Corriger), puis route vers `history_saver` en attendant la décision
de l'utilisateur — jamais vers une boucle de correction de données.
"""

import pytest

from build_query.examples_executor import _AssertionsAndEvaluation, _Assertion
from build_query.query_chain import route_evaluator


def test_modele_accepte_reason_type_bad_description():
    """Le schéma d'éval doit accepter le nouveau motif `bad_description`."""
    ev = _AssertionsAndEvaluation(
        reasoning="La description annonce 2.0M mais le résultat réel vaut 1.0.",
        assertions=[
            _Assertion(
                description="Le revenu maximal vaut un million.",
                expected_condition="max_revenue = 1",
            )
        ],
        verdict="Insuffisant",
        reason_type="bad_description",
        explanation="La description annonce une valeur que la requête ne produit pas.",
    )
    assert ev.reason_type == "bad_description"
    # Pas de diagnostic exigé (ce n'est pas bad_data) ni d'assertion_fix.
    assert ev.diagnostic is None
    assert ev.assertion_fix is None


@pytest.mark.parametrize("has_existing", [False, True])
def test_bad_description_flague_sans_boucle_de_retry(has_existing):
    """`bad_description` ne matche aucune branche de retry (ni bad_data ni bad_assertions).
    Comme `needs_validation`, l'état est sauvé et un VALIDATION_PROMPT est émis → route vers
    `history_saver` (attente de la décision utilisateur), jamais vers une boucle de correction
    de données (qui serait inutile : la donnée est bonne)."""
    state = {
        "evaluation_feedback": "bad_description",
        "has_existing_tests": has_existing,
        "gen_retries": 2,
    }
    route = route_evaluator(state)
    assert route == "history_saver"
    assert route not in (
        "bad_data_to_agent",
        "bad_data_exhausted",
        "assertion_corrector",
    )
