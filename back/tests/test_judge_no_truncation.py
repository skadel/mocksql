"""Régression : les données d'entrée montrées au juge LLM ne sont JAMAIS tronquées.

Bug d'origine : `_classify_empty_intent` / `_reevaluate_empty_result` sérialisaient
l'input avec un slice `[:800]` caractères. Sur une table au nom long et à colonnes
nombreuses, la coupe tombait au milieu d'un objet JSON (ex. `"cd_type"` coupé en deux).
Le juge lisait ce JSON malformé et concluait à tort que les données étaient
« incomplètes et malformées », hallucinant un défaut de données comme cause du 0-ligne
alors que DuckDB avait reçu les données complètes.
"""

import json

from build_query.test_evaluator import _format_input_for_judge


def _big_input() -> dict:
    """Reproduit le cas réel : table longue, beaucoup de colonnes, dernière clé `cd_type`."""
    return {
        "MONETIQUE_Dataset_Porteur_DS_REF_PORTEUR": [
            {
                "id_porteur": f"COLLAB{i:05d}",
                "banque": "001",
                "iban": f"FR76001000{i:08d}",
                "date_ouverture": "2026-01-15",
                "libelle": "porteur de test avec un libellé volontairement long " * 3,
                "cd_type": "REF",
            }
            for i in range(20)
        ]
    }


def test_input_for_judge_is_not_truncated():
    data = _big_input()
    out = _format_input_for_judge(data)

    # Bien au-delà de l'ancien cap de 800 caractères.
    assert len(out) > 800
    # JSON complet et valide — pas de coupe au milieu d'un objet.
    reparsed = json.loads(out)
    assert reparsed == data
    # La dernière clé (celle qui était coupée dans le bug) est présente en entier
    # pour CHAQUE ligne, pas seulement la première.
    assert out.count('"cd_type"') == 20


def test_input_for_judge_falls_back_on_non_serializable():
    class _NotJson:
        def __repr__(self):
            return "<NotJson>"

    out = _format_input_for_judge({"x": _NotJson()})
    # Pas d'exception : repli sur str(), sans troncature.
    assert "NotJson" in out
