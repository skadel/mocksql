"""TICKET-3 — Assertions de TRI robustes (anti-épinglage de clé technique).

Cas observé sur fdp (`warm_storage_datasets`) : pour valider un ORDER BY, le modèle
épingle l'ID exact attendu à une position (« la 1ʳᵉ ligne a l'id X ») au lieu de tester
la RELATION d'ordre. C'est fragile : ça casse au moindre changement de données et ça ne
teste pas la logique de tri.

Le pattern n'est pas détectable de façon déterministe fiable (qu'est-ce qu'une « clé
technique » ? l'assertion vise-t-elle un tri ?) → on guide à la GÉNÉRATION via la
description du champ `expected_condition` de `_Assertion`. Ce test est un garde-fou
anti-régression de cette consigne (la génération elle-même se valide à l'éval).
"""

from build_query.examples_executor import _Assertion


def _expected_condition_help() -> str:
    return _Assertion.model_fields["expected_condition"].description.lower()


def test_expected_condition_guidance_warns_against_pinning_keys_for_ordering():
    """La consigne doit dissuader d'épingler une clé technique pour valider un tri et
    pousser vers l'affirmation de la relation d'ordre."""
    help_text = _expected_condition_help()
    # Mentionne le tri / l'ordre…
    assert "tri" in help_text or "ordre" in help_text
    # …et l'idée d'épingler une clé / valeur exacte comme à proscrire.
    assert "épingl" in help_text or "fragile" in help_text
