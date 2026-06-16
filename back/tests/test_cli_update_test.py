"""Régression : `mocksql update-test` modifie UN test existant (ciblé par test_uid)
via le LLM, et **préserve les assertions-specs** (porteuses d'assertion_uid).

Contrat (≠ generate additif) :
- la version modifiée du test ciblé REMPLACE l'ancienne (l'agent gagne sur les données) ;
- les autres tests sont intacts ;
- les specs `assert` (assertion_uid) survivent — l'agent ne touche qu'aux données et
  aux assertions auto-générées, jamais à ta cible rouge.
"""

from cli.generate import apply_update_result, replace_test_case_preserving_specs


def _case(uid, rows, *, spec=None, auto=None):
    case = {"test_uid": uid, "data": {"t": rows}, "assertion_results": []}
    if spec:
        case["assertion_results"].append(
            {
                "assertion_uid": "spec1",
                "description": spec,
                "sql": "SELECT * FROM __result__ WHERE 1=0",
            }
        )
    if auto:
        case["assertion_results"].append(
            {"description": auto, "sql": "SELECT * FROM __result__ WHERE 2=2"}
        )
    return case


# ── replace_test_case_preserving_specs ──────────────────────────────────────


def test_replaces_targeted_case_data():
    existing = [_case("a", [{"x": 1}]), _case("b", [{"x": 9}])]
    generated = [_case("a", [{"x": 1}, {"x": 1}])]  # agent a ajouté une ligne dupliquée
    result = replace_test_case_preserving_specs(existing, generated, "a")
    assert result[0]["data"]["t"] == [{"x": 1}, {"x": 1}]
    # b intact
    assert result[1] == existing[1]


def test_preserves_spec_assertion_on_updated_case():
    existing = [_case("a", [{"x": 1}], spec="cible rouge")]
    generated = [_case("a", [{"x": 1}, {"x": 1}], auto="auto régénérée")]
    result = replace_test_case_preserving_specs(existing, generated, "a")
    descs = [x["description"] for x in result[0]["assertion_results"]]
    assert "cible rouge" in descs  # spec survit
    assert "auto régénérée" in descs  # assertion régénérée présente aussi


def test_keeps_stable_uid_after_update():
    existing = [_case("a", [{"x": 1}], spec="s")]
    generated = [_case("a", [{"x": 2}])]
    result = replace_test_case_preserving_specs(existing, generated, "a")
    assert result[0]["test_uid"] == "a"


def test_no_matching_generated_case_with_other_uid_is_noop():
    existing = [_case("a", [{"x": 1}], spec="s")]
    generated = [_case("zzz", [{"x": 2}])]  # un AUTRE test identifié → pas la cible
    result = replace_test_case_preserving_specs(existing, generated, "a")
    assert result == existing  # aucune modification


def test_single_uidless_generated_case_is_rebound_to_target():
    # L'executor ré-émet le test modifié sans test_uid : un seul cas sans uid → c'est la cible.
    existing = [_case("a", [{"x": 1}], spec="cible"), _case("b", [{"x": 9}])]
    generated = [
        {"data": {"t": [{"x": 1}, {"x": 1}]}, "assertion_results": []}
    ]  # pas d'uid
    result = replace_test_case_preserving_specs(existing, generated, "a")
    assert result[0]["test_uid"] == "a"  # identité rattachée
    assert result[0]["data"]["t"] == [{"x": 1}, {"x": 1}]  # données modifiées
    assert any(
        x["description"] == "cible" for x in result[0]["assertion_results"]
    )  # spec préservée
    assert result[1] == existing[1]  # b intact


def test_spec_not_duplicated_if_agent_kept_it():
    existing = [_case("a", [{"x": 1}], spec="cible")]
    # l'agent a renvoyé la spec (même assertion_uid) en plus de la sienne
    upd = _case("a", [{"x": 2}], auto="auto")
    upd["assertion_results"].insert(
        0,
        {
            "assertion_uid": "spec1",
            "description": "cible",
            "sql": "SELECT * FROM __result__ WHERE 1=0",
        },
    )
    result = replace_test_case_preserving_specs(existing, [upd], "a")
    spec_count = sum(
        1 for x in result[0]["assertion_results"] if x.get("assertion_uid") == "spec1"
    )
    assert spec_count == 1  # pas de doublon


# ── apply_update_result ─────────────────────────────────────────────────────


def test_apply_update_preserves_extra_doc_fields():
    existing_doc = {
        "sql": "old",
        "test_cases": [_case("a", [{"x": 1}], spec="cible")],
        "source_hash": "h",
    }
    doc = apply_update_result(
        existing_doc,
        [_case("a", [{"x": 1}, {"x": 1}])],
        target_uid="a",
        sql="refreshed",
        used_columns=["u"],
    )
    assert doc["source_hash"] == "h"
    assert doc["sql"] == "refreshed"
    assert doc["test_cases"][0]["data"]["t"] == [{"x": 1}, {"x": 1}]
    assert any(
        a["description"] == "cible" for a in doc["test_cases"][0]["assertion_results"]
    )
