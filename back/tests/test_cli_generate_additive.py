"""Régression : `mocksql generate` est ADDITIF par défaut — il n'écrase jamais les
tests existants ni leurs assertions-specs (posées via `mocksql assert`). Seul
`--overwrite` reconstruit la suite de zéro.

Sans cette garantie, relancer `generate` (ou ajouter un cas ciblé) détruirait la
cible rouge d'une boucle de fix en cours.
"""

from cli.generate import apply_generation_result, merge_test_cases


def _tc(uid, *, spec=None):
    case = {
        "test_uid": uid,
        "test_index": uid,
        "data": {"t": [{"x": 1}]},
        "assertion_results": [],
    }
    if spec:
        case["assertion_results"].append(
            {
                "assertion_uid": "spec1",
                "description": spec,
                "sql": "SELECT * FROM __result__ WHERE 1=0",
            }
        )
    return case


# ── merge_test_cases ────────────────────────────────────────────────────────


def test_merge_appends_new_case():
    existing = [_tc("a")]
    generated = [_tc("b")]
    merged = merge_test_cases(existing, generated)
    assert [c["test_uid"] for c in merged] == ["a", "b"]


def test_merge_preserves_existing_specs_over_regenerated_duplicate():
    # L'existant porte une spec ; le généré ré-émet le MÊME uid sans la spec.
    existing = [_tc("a", spec="cible rouge")]
    generated = [_tc("a")]  # même uid, pas de spec
    merged = merge_test_cases(existing, generated)
    assert len(merged) == 1
    # l'existant (avec sa spec) gagne, le doublon régénéré est ignoré
    assert merged[0]["assertion_results"][0]["description"] == "cible rouge"


def test_merge_keeps_existing_and_adds_only_genuinely_new():
    existing = [_tc("a", spec="s"), _tc("b")]
    generated = [_tc("b"), _tc("c")]  # b existe déjà, c est nouveau
    merged = merge_test_cases(existing, generated)
    assert [c["test_uid"] for c in merged] == ["a", "b", "c"]
    assert merged[0]["assertion_results"][0]["description"] == "s"


def test_merge_appends_uidless_generated_cases():
    existing = [_tc("a")]
    generated = [{"data": {}, "assertion_results": []}]  # pas de test_uid
    merged = merge_test_cases(existing, generated)
    assert len(merged) == 2


# ── apply_generation_result ─────────────────────────────────────────────────


def test_overwrite_replaces_everything():
    existing_doc = {
        "sql": "old",
        "test_cases": [_tc("a", spec="perdue")],
        "source_hash": "h",
    }
    doc = apply_generation_result(
        existing_doc,
        [_tc("z")],
        sql="new",
        used_columns=["u"],
        suggestions=None,
        overwrite=True,
    )
    assert [c["test_uid"] for c in doc["test_cases"]] == ["z"]
    assert doc["sql"] == "new"


def test_additive_preserves_specs_and_extra_fields():
    existing_doc = {
        "sql": "old",
        "test_cases": [_tc("a", spec="cible rouge")],
        "source_hash": "h",
        "query_decomposed": "[]",
    }
    doc = apply_generation_result(
        existing_doc,
        [_tc("b")],
        sql="refreshed",
        used_columns=["u"],
        suggestions=None,
        overwrite=False,
    )
    uids = [c["test_uid"] for c in doc["test_cases"]]
    assert uids == ["a", "b"]
    # spec préservée
    assert doc["test_cases"][0]["assertion_results"][0]["description"] == "cible rouge"
    # champs annexes préservés
    assert doc["source_hash"] == "h"
    assert doc["query_decomposed"] == "[]"


def test_no_existing_file_writes_fresh_suite():
    doc = apply_generation_result(
        None,
        [_tc("a"), _tc("b")],
        sql="s",
        used_columns=[],
        suggestions=["idée"],
        overwrite=False,
    )
    assert [c["test_uid"] for c in doc["test_cases"]] == ["a", "b"]
    assert doc["suggestions"] == ["idée"]
