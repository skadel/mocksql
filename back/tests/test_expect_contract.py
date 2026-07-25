"""Tests du contrat ``expect`` (spec validation-humaine, Phase 0).

Couvre le builder (colonnes des assertions, détection ORDER BY), la comparaison de
lignes (multiset / ordonnée, normalisation des valeurs), le dual-write à la
persistance (``sync_expect_on_doc`` + intégration ``write_test_doc``) et les règles
de migration §5 (``migrate_case``).
"""

import json

import pytest

from build_query.expect_contract import (
    build_expect,
    compare_expect,
    detect_ordered,
    sync_expect_on_doc,
)
from cli.expect_migrate import migrate_case


ROWS = [
    {"order_id": 1, "amount": 100.0, "label": "ok"},
    {"order_id": 2, "amount": 250.5, "label": "ko"},
]


def _results_json(rows=ROWS) -> str:
    return json.dumps(rows)


# ── build_expect ──────────────────────────────────────────────────────────────


def test_build_expect_restricts_to_assertion_columns():
    assertions = [{"expected_condition": "ROUND(AMOUNT, 2) >= 100", "sql": "x"}]
    expect = build_expect(_results_json(), assertions, "SELECT 1")
    # Casse du RÉSULTAT conservée, rapprochement insensible à la casse.
    assert expect["columns"] == ["amount"]
    assert expect["rows"] == [{"amount": 100.0}, {"amount": 250.5}]


def test_build_expect_falls_back_to_all_columns():
    assertions = [{"expected_condition": "COUNT(*) = 2", "sql": "x"}]
    expect = build_expect(_results_json(), assertions, "SELECT 1")
    assert expect["columns"] == ["order_id", "amount", "label"]


def test_build_expect_explicit_columns_win():
    # Colonnes porteuses fournies (coherence_check) → priment sur les assertions et sur
    # « toutes ». Casse du résultat conservée, rapprochement insensible à la casse.
    assertions = [{"expected_condition": "ROUND(AMOUNT, 2) >= 100", "sql": "x"}]
    expect = build_expect(
        _results_json(), assertions, "SELECT 1", columns=["ORDER_ID", "label"]
    )
    assert expect["columns"] == ["order_id", "label"]
    assert expect["rows"] == [
        {"order_id": 1, "label": "ok"},
        {"order_id": 2, "label": "ko"},
    ]


def test_build_expect_explicit_columns_unknown_falls_back():
    # Colonnes demandées absentes du résultat → repli sur toutes les colonnes.
    expect = build_expect(_results_json(), None, "SELECT 1", columns=["ghost"])
    assert expect["columns"] == ["order_id", "amount", "label"]


def test_sync_preserves_chosen_columns_on_refresh():
    # Un draft dont l'expect porte déjà un choix de colonnes (coherence_check) garde ce
    # choix quand sync rafraîchit les lignes.
    doc = _doc(
        case_extra={
            "review": {"status": "draft", "hint": "vérifie order_id=1"},
            "expect": {
                "columns": ["order_id"],
                "rows": [{"order_id": 9}],
                "ordered": True,
            },
        }
    )
    sync_expect_on_doc(doc)
    case = doc["test_cases"][0]
    assert case["expect"]["columns"] == ["order_id"]
    assert case["expect"]["rows"] == [{"order_id": 1}, {"order_id": 2}]


def test_build_expect_unparseable_results_is_none():
    assert build_expect("pas du json", [], "SELECT 1") is None
    assert build_expect(None, [], "SELECT 1") is None


def test_build_expect_empty_rows_is_valid_contract():
    expect = build_expect("[]", [], "SELECT 1")
    assert expect == {"columns": [], "rows": [], "ordered": False}


def test_detect_ordered_top_level_only():
    assert detect_ordered("SELECT a FROM t ORDER BY a", "bigquery") is True
    assert detect_ordered("SELECT a FROM t", "bigquery") is False
    # ORDER BY dans une CTE / sous-requête ≠ tri du résultat final.
    assert (
        detect_ordered(
            "WITH c AS (SELECT a FROM t ORDER BY a) SELECT a FROM c", "bigquery"
        )
        is False
    )
    assert detect_ordered("", "bigquery") is False


# ── compare_expect ────────────────────────────────────────────────────────────


def _expect(rows=ROWS, columns=("order_id", "amount", "label"), ordered=False):
    return {
        "columns": list(columns),
        "rows": [{c: r[c] for c in columns} for r in rows],
        "ordered": ordered,
    }


def test_compare_multiset_ignores_order():
    check = compare_expect(_expect(), [ROWS[1], ROWS[0]])
    assert check["passed"] is True
    assert check["missing"] == [] and check["unexpected"] == []


def test_compare_ordered_fails_on_permutation():
    check = compare_expect(_expect(ordered=True), [ROWS[1], ROWS[0]])
    assert check["passed"] is False
    # Permutation pure : diff multiset vide, seul l'ordre diffère.
    assert check["missing"] == [] and check["unexpected"] == []


def test_compare_reports_missing_and_unexpected():
    actual = [ROWS[0], {"order_id": 3, "amount": 9.0, "label": "new"}]
    check = compare_expect(_expect(), actual)
    assert check["passed"] is False
    assert check["missing"] == [{"order_id": 2, "amount": 250.5, "label": "ko"}]
    assert check["unexpected"] == [{"order_id": 3, "amount": 9.0, "label": "new"}]
    assert check["expected_count"] == 2 and check["actual_count"] == 2


def test_compare_normalizes_int_float_but_not_bool():
    expect = {"columns": ["v"], "rows": [{"v": 10}], "ordered": False}
    assert compare_expect(expect, [{"v": 10.0}])["passed"] is True
    expect_bool = {"columns": ["v"], "rows": [{"v": True}], "ordered": False}
    assert compare_expect(expect_bool, [{"v": 1}])["passed"] is False


def test_compare_case_insensitive_columns():
    expect = {"columns": ["Amount"], "rows": [{"Amount": 5.0}], "ordered": False}
    assert compare_expect(expect, [{"AMOUNT": 5.0}])["passed"] is True


def test_compare_empty_contract_requires_empty_output():
    empty = {"columns": [], "rows": [], "ordered": False}
    assert compare_expect(empty, [])["passed"] is True
    assert compare_expect(empty, [{"a": 1}])["passed"] is False


# ── sync_expect_on_doc (dual-write) ──────────────────────────────────────────


def _doc(case_extra=None, status="complete", verdict="Bon"):
    case = {
        "test_index": 0,
        "status": status,
        "verdict": verdict,
        "results_json": _results_json(),
        "assertion_results": [],
        **(case_extra or {}),
    }
    return {"sql": "SELECT * FROM t ORDER BY order_id", "test_cases": [case]}


def test_sync_writes_expect_and_draft_review():
    doc = _doc()
    sync_expect_on_doc(doc)
    case = doc["test_cases"][0]
    assert case["expect"]["ordered"] is True
    assert [r["order_id"] for r in case["expect"]["rows"]] == [1, 2]
    assert case["review"] == {"status": "draft"}


def test_sync_never_touches_confirmed_or_stale():
    for frozen in ("confirmed", "stale"):
        doc = _doc(
            case_extra={
                "review": {"status": frozen},
                "expect": {"columns": ["x"], "rows": [{"x": 1}], "ordered": False},
            }
        )
        sync_expect_on_doc(doc)
        case = doc["test_cases"][0]
        assert case["expect"] == {
            "columns": ["x"],
            "rows": [{"x": 1}],
            "ordered": False,
        }
        assert case["review"]["status"] == frozen


def test_sync_skips_deadborn_and_missing_results():
    doc = _doc(status="error", verdict="Insuffisant")
    sync_expect_on_doc(doc)
    assert "expect" not in doc["test_cases"][0]

    doc2 = {"sql": "SELECT 1", "test_cases": [{"test_index": 0}]}
    sync_expect_on_doc(doc2)
    assert "expect" not in doc2["test_cases"][0]


def test_sync_preserves_prescriptive_expect_when_output_unchanged():
    """Boucle repro : un ``expect.rows`` rendu PRESCRIPTIF à la main (≠ sortie observée)
    ne doit pas être re-snapshotté quand une AUTRE écriture du doc survient (confirmer un
    autre cas, ``generate -i``…) tant que la sortie du cas n'a pas bougé — sinon la cible
    rouge éditée est silencieusement détruite (perte de donnée)."""
    prescriptive = {
        "columns": ["order_id", "amount", "label"],
        "rows": [{"order_id": 1, "amount": 999.0, "label": "voulu"}],  # ≠ results_json
        "ordered": True,
    }
    doc = _doc(
        case_extra={
            "test_uid": "aaaa",
            "review": {"status": "draft"},
            "expect": prescriptive,
        }
    )
    # La sortie observée (results_json) est INCHANGÉE par rapport au disque.
    previous_cases = [dict(doc["test_cases"][0])]
    sync_expect_on_doc(doc, previous_sql=doc["sql"], previous_cases=previous_cases)
    assert doc["test_cases"][0]["expect"] == prescriptive


def test_sync_refreshes_draft_expect_when_output_changed():
    """Contrepartie : si la sortie observée a changé (data éditée via update-test), le
    draft EST re-snapshotté sur la nouvelle sortie (comportement historique conservé)."""
    doc = _doc(
        case_extra={
            "test_uid": "aaaa",
            "review": {"status": "draft"},
            "expect": {
                "columns": ["order_id"],
                "rows": [{"order_id": 9}],
                "ordered": True,
            },
        }
    )
    # Sortie précédente différente → results_json a bougé → refresh légitime.
    previous_cases = [
        {
            "test_uid": "aaaa",
            "test_index": 0,
            "results_json": json.dumps([{"order_id": 7, "amount": 1.0, "label": "x"}]),
        }
    ]
    sync_expect_on_doc(doc, previous_sql=doc["sql"], previous_cases=previous_cases)
    assert doc["test_cases"][0]["expect"]["rows"] == [{"order_id": 1}, {"order_id": 2}]


def test_write_test_doc_puts_expect_in_committed_definition(tmp_path):
    from storage.test_files import write_test_doc

    path = tmp_path / "tests" / "orders.json"
    write_test_doc(path, _doc())
    definition = json.loads(path.read_text(encoding="utf-8"))
    case = definition["test_cases"][0]
    assert case["expect"]["columns"] == ["order_id", "amount", "label"]
    assert case["review"] == {"status": "draft"}
    # La sortie observée reste dans le cache gitignoré, pas dans la définition.
    assert "results_json" not in case


# ── confirm_case + bascule stale (Phase 1) ───────────────────────────────────


def test_confirm_case_freezes_current_output_as_user_contract():
    from build_query.expect_contract import confirm_case

    case = {
        "test_uid": "aaaa",
        "test_index": 0,
        "status": "complete",
        "results_json": _results_json(),
        "assertion_results": [],
        "review": {"status": "stale", "confirmed_by": "verdict-llm-legacy"},
        "expect": {"columns": ["old"], "rows": [{"old": 1}], "ordered": False},
    }
    confirmed = confirm_case(case, "SELECT 1")
    # Le contrat gelé est la sortie ACTUELLE (pas l'ancien expect stale).
    assert confirmed["expect"]["columns"] == ["order_id", "amount", "label"]
    assert confirmed["review"]["status"] == "confirmed"
    assert confirmed["review"]["confirmed_by"] == "user"
    # Pure : le cas d'origine n'est pas muté.
    assert case["review"]["status"] == "stale"


def test_confirm_case_falls_back_to_stored_expect_without_results():
    from build_query.expect_contract import confirm_case

    stored = {"columns": ["x"], "rows": [{"x": 1}], "ordered": False}
    case = {"test_uid": "aaaa", "expect": stored}
    confirmed = confirm_case(case, "SELECT 1")
    assert confirmed["expect"] == stored
    assert confirmed["review"]["confirmed_by"] == "user"


def test_confirm_case_nothing_to_freeze_raises():
    from build_query.expect_contract import confirm_case

    with pytest.raises(ValueError, match="Rien à confirmer"):
        confirm_case({"test_uid": "aaaa"}, "SELECT 1")


def test_sync_flips_confirmed_to_stale_on_sql_change():
    doc = _doc(
        case_extra={
            "review": {"status": "confirmed", "confirmed_by": "user"},
            "expect": {"columns": ["x"], "rows": [{"x": 1}], "ordered": False},
        }
    )
    sync_expect_on_doc(doc, previous_sql="SELECT * FROM t -- ancienne version")
    case = doc["test_cases"][0]
    assert case["review"]["status"] == "stale"
    assert case["review"]["confirmed_by"] == "user"  # trace conservée pour le diff
    # Le contrat confirmé n'est PAS écrasé : c'est la base du diff de re-confirmation.
    assert case["expect"] == {"columns": ["x"], "rows": [{"x": 1}], "ordered": False}


def test_sync_same_sql_does_not_flip():
    doc = _doc(case_extra={"review": {"status": "confirmed"}})
    sync_expect_on_doc(doc, previous_sql=doc["sql"])
    assert doc["test_cases"][0]["review"]["status"] == "confirmed"


def test_write_test_doc_flips_stale_on_disk_sql_change(tmp_path):
    from storage.test_files import read_test_doc, write_test_doc

    path = tmp_path / "tests" / "orders.json"
    doc = _doc(
        case_extra={
            "review": {"status": "confirmed", "confirmed_by": "user"},
            "expect": {"columns": ["x"], "rows": [{"x": 1}], "ordered": False},
        }
    )
    write_test_doc(path, doc)
    changed = read_test_doc(path)
    changed["sql"] = "SELECT * FROM t2"
    write_test_doc(path, changed)
    saved = read_test_doc(path)
    assert saved["test_cases"][0]["review"]["status"] == "stale"


def test_write_carries_review_when_client_strips_it(tmp_path):
    # Un PATCH front (auto-save) qui pousse les test_cases sans review/expect ne doit
    # jamais effacer une confirmation : le fichier est propriétaire de la revue.
    from build_query.expect_contract import confirm_case
    from storage.test_files import read_test_doc, write_test_doc

    path = tmp_path / "tests" / "orders.json"
    doc = _doc(case_extra={"test_uid": "u1"})
    write_test_doc(path, doc)
    saved = read_test_doc(path)
    saved["test_cases"] = [confirm_case(saved["test_cases"][0], saved["sql"])]
    write_test_doc(path, saved)

    stripped = {
        "sql": doc["sql"],
        "test_cases": [
            {
                "test_uid": "u1",
                "test_index": 0,
                "status": "complete",
                "verdict": "Bon",
                "results_json": _results_json(),
                "assertion_results": [],
            }
        ],
    }
    write_test_doc(path, stripped)
    final = read_test_doc(path)
    assert final["test_cases"][0]["review"]["status"] == "confirmed"
    assert final["test_cases"][0]["review"]["confirmed_by"] == "user"


def test_compare_order_only_mismatch_flag():
    check = compare_expect(_expect(ordered=True), [ROWS[1], ROWS[0]])
    assert check["order_only_mismatch"] is True
    check2 = compare_expect(_expect(), [ROWS[0]])
    assert check2["order_only_mismatch"] is False


def test_run_confirm_cli(tmp_path):
    """`mocksql confirm` = replay-on-confirm : le cas est rejoué contre le SQL disque
    et c'est CETTE sortie qui est gelée — le `results_json` du cache (potentiellement
    figé au generate) est ignoré. Un cas sans données n'est pas confirmable."""
    from cli.manage_cmd import run_confirm
    from storage.test_files import read_test_doc, write_test_doc

    sql = (
        "SELECT payment, SUM(amount) AS total FROM `p.d.t` "
        "GROUP BY payment ORDER BY payment"
    )
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text(sql, encoding="utf-8")
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    (tmp_path / ".mocksql").mkdir()
    (tmp_path / ".mocksql" / "schema_cache.json").write_text(
        json.dumps(
            {
                "tables": [
                    {
                        "table_name": "p.d.t",
                        "columns": [
                            {
                                "name": "payment",
                                "type": "STRING",
                                "bq_ddl_type": "STRING",
                            },
                            {
                                "name": "amount",
                                "type": "FLOAT64",
                                "bq_ddl_type": "FLOAT64",
                            },
                        ],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    path = tmp_path / ".mocksql" / "tests" / "orders.json"
    doc = {
        "sql": sql,
        "used_columns": [
            {
                "project": "p",
                "database": "d",
                "table": "t",
                "used_columns": ["payment", "amount"],
            }
        ],
        "test_cases": [
            {
                "test_uid": "aaaa",
                "test_index": "0",
                "test_name": "nominal",
                "status": "complete",
                "verdict": "Bon",
                "data": {
                    "d_t": [
                        {"payment": "cb", "amount": 10.0},
                        {"payment": "paypal", "amount": 5.0},
                    ]
                },
                # Cache périmé (autre sortie) : le gel doit venir du replay, pas de lui.
                "results_json": json.dumps([{"payment": "x", "total": 0.0}]),
                "assertion_results": [],
            }
        ],
    }
    write_test_doc(path, doc)
    result = run_confirm(tmp_path / "mocksql.yml", "orders", "aaaa")
    assert result["review"]["status"] == "confirmed"
    assert result["expect_rows"] == 2
    saved = read_test_doc(path)
    assert saved["test_cases"][0]["review"]["confirmed_by"] == "user"
    assert saved["test_cases"][0]["expect"]["rows"] == [
        {"payment": "cb", "total": 10.0},
        {"payment": "paypal", "total": 5.0},
    ]


# ── migrate_case (§5) ─────────────────────────────────────────────────────────


@pytest.mark.parametrize("verdict", ["Excellent", "Bon"])
def test_migrate_success_verdict_is_legacy_confirmed(verdict):
    case = {
        "test_index": 0,
        "status": "complete",
        "verdict": verdict,
        "results_json": _results_json(),
        "assertion_results": [],
    }
    outcome = migrate_case(case, "SELECT 1", "snowflake")
    assert outcome == "confirmed"
    assert case["review"]["status"] == "confirmed"
    assert case["review"]["confirmed_by"] == "verdict-llm-legacy"
    assert case["expect"]["rows"]


def test_migrate_insufficient_is_draft_with_expect():
    case = {
        "test_index": 0,
        "status": "complete",
        "verdict": "Insuffisant",
        "results_json": _results_json(),
    }
    assert migrate_case(case, "SELECT 1", None) == "draft"
    assert case["review"] == {"status": "draft"}
    assert case["expect"]["rows"]


def test_migrate_deadborn_is_draft_without_expect():
    case = {
        "test_index": 0,
        "status": "error",
        "verdict": "Insuffisant",
        "results_json": "[]",
    }
    assert migrate_case(case, "SELECT 1", None) == "no_results"
    assert case["review"] == {"status": "draft"}
    assert "expect" not in case


def test_migrate_empty_intent_pass_is_confirmed_empty_contract():
    # PASS « vide intentionnel » : status empty_results + verdict Bon → contrat vide gelé.
    case = {
        "test_index": 0,
        "status": "empty_results",
        "verdict": "Bon",
        "results_json": "[]",
        "assertion_results": [],
    }
    assert migrate_case(case, "SELECT 1", None) == "confirmed"
    assert case["expect"]["rows"] == []
