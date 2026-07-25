"""Phase 2 — le replay ``mocksql test`` compare les LIGNES (contrat ``expect``), pas
les assertions (spec validation-humaine §7).

Contrat cible :
- un cas qui porte un ``expect`` : la comparaison de lignes (multiset | ordonnée)
  détermine le verdict — les assertions ne sont plus rejouées (plus de
  ``_remap_assertion_sql``) ;
- ``confirmed`` = gate de non-régression : lignes ≠ contrat → ``fail`` (exit 1) ;
- ``draft`` / ``stale`` / non confirmé = jamais un échec bloquant : lignes ≠ contrat →
  ``unconfirmed`` (rapporté, montré avec diff, hors exit code par défaut) ;
- repli legacy : un cas SANS ``expect`` garde le chemin assertions (inchangé).

Bout-en-bout sur un vrai DuckDB local (zéro LLM), sur le snapshot figé (``--frozen``)
pour un verdict déterministe sans bruit de dérive disque.
"""

import asyncio
import json

import pytest

from cli.test_runner import _expect_verdict, run_tests


# ── _expect_verdict (verdict pur, sans DuckDB) ───────────────────────────────


def _check(passed, order_only=False):
    return {"passed": passed, "order_only_mismatch": order_only}


@pytest.mark.parametrize("review", ["confirmed", "draft", "stale", None])
def test_verdict_pass_when_rows_match(review):
    assert _expect_verdict(_check(True), review) == "pass"


@pytest.mark.parametrize("review", ["confirmed", "draft", "stale", None])
def test_verdict_order_only_is_never_blocking(review):
    # Ex-æquo (mêmes lignes, ordre différent) = non-déterminisme, jamais un échec — même
    # sur un contrat confirmé (spec §8 : flag, pas fail).
    assert _expect_verdict(_check(False, order_only=True), review) == "pass"


def test_verdict_confirmed_mismatch_is_a_regression_fail():
    assert _expect_verdict(_check(False), "confirmed") == "fail"


@pytest.mark.parametrize("review", ["draft", "stale", None])
def test_verdict_unconfirmed_mismatch_is_not_a_failure(review):
    assert _expect_verdict(_check(False), review) == "unconfirmed"


_SQL = "SELECT payment, SUM(amount) AS total FROM `p.d.t` GROUP BY payment ORDER BY payment"

_SCHEMAS = [
    {
        "table_name": "p.d.t",
        "columns": [
            {"name": "payment", "type": "STRING", "bq_ddl_type": "STRING"},
            {"name": "amount", "type": "FLOAT64", "bq_ddl_type": "FLOAT64"},
        ],
    }
]


def _expect(rows, columns=("payment", "total"), ordered=True):
    return {"columns": list(columns), "rows": rows, "ordered": ordered}


def _case(uid, data, *, expect=None, review=None, assertions=None):
    case = {
        "test_uid": uid,
        "test_index": uid,
        "test_name": f"case {uid}",
        "status": "complete",
        "verdict": "Bon",
        "data": data,
        "assertion_results": assertions or [],
    }
    if expect is not None:
        case["expect"] = expect
    if review is not None:
        case["review"] = review
    return case


def _write_project(tmp_path, cases):
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text(_SQL, encoding="utf-8")
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    mocksql_dir = tmp_path / ".mocksql"
    (mocksql_dir / "tests").mkdir(parents=True)
    (mocksql_dir / "schema_cache.json").write_text(
        json.dumps({"tables": _SCHEMAS}), encoding="utf-8"
    )
    doc = {
        "sql": _SQL,
        "used_columns": [
            {
                "project": "p",
                "database": "d",
                "table": "t",
                "used_columns": ["payment", "amount"],
            }
        ],
        "test_cases": cases,
    }
    (mocksql_dir / "tests" / "orders.json").write_text(
        json.dumps(doc, ensure_ascii=False), encoding="utf-8"
    )
    return mocksql_dir / "tests" / "orders.json"


def _run(tmp_path):
    return asyncio.run(run_tests(tmp_path / "mocksql.yml", None, False, frozen=True))


def _by_uid(model_results):
    return {c["index"]: c for c in model_results[0]["cases"]}


CB10 = {"d_t": [{"payment": "cb", "amount": 10.0}]}


def test_confirmed_matching_expect_passes(tmp_path):
    _write_project(
        tmp_path,
        [
            _case(
                "A",
                CB10,
                expect=_expect([{"payment": "cb", "total": 10.0}]),
                review={"status": "confirmed", "confirmed_by": "user"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    assert _by_uid(results)["A"]["status"] == "pass"


def test_confirmed_regression_fails(tmp_path):
    # Contrat gelé ≠ sortie réelle → régression attrapée : fail + exit 1.
    _write_project(
        tmp_path,
        [
            _case(
                "B",
                CB10,
                expect=_expect([{"payment": "cb", "total": 999.0}]),
                review={"status": "confirmed", "confirmed_by": "user"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 1
    case = _by_uid(results)["B"]
    assert case["status"] == "fail"
    # Le diff de lignes est exposé (écran de détection de régression).
    assert case["expect_check"]["passed"] is False
    assert case["expect_check"]["unexpected"] == [{"payment": "cb", "total": 10.0}]


def test_draft_regression_is_unconfirmed_not_a_failure(tmp_path):
    # Un draft n'est pas un contrat : lignes ≠ snapshot → unconfirmed, jamais un échec
    # bloquant (exit 0), diff montré.
    _write_project(
        tmp_path,
        [
            _case(
                "C",
                CB10,
                expect=_expect([{"payment": "cb", "total": 999.0}]),
                review={"status": "draft"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    case = _by_uid(results)["C"]
    assert case["status"] == "unconfirmed"
    assert case["expect_check"]["passed"] is False


def test_expect_wins_over_assertions(tmp_path):
    # Un cas porte un expect (correct) ET des assertions (qui échoueraient) : le contrat
    # de lignes est autoritaire, les assertions ne sont plus rejouées.
    _write_project(
        tmp_path,
        [
            _case(
                "D",
                CB10,
                expect=_expect([{"payment": "cb", "total": 10.0}]),
                review={"status": "confirmed", "confirmed_by": "user"},
                assertions=[
                    {
                        "description": "assertion qui échouerait",
                        "sql": "SELECT * FROM __result__ WHERE total > 0",
                    }
                ],
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    case = _by_uid(results)["D"]
    assert case["status"] == "pass"
    # Les assertions ne sont pas rapportées (le contrat les remplace).
    assert case["assertions"] == []


def test_legacy_case_without_expect_keeps_assertions(tmp_path):
    # Repli : sans expect, le chemin assertions reste en place (assertion qui passe).
    _write_project(
        tmp_path,
        [
            _case(
                "E",
                CB10,
                assertions=[
                    {
                        "description": "total positif",
                        "sql": "SELECT * FROM __result__ WHERE total < 0",
                    }
                ],
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    case = _by_uid(results)["E"]
    assert case["status"] == "pass"
    assert len(case["assertions"]) == 1


def test_legacy_case_without_expect_or_assertions_skips(tmp_path):
    _write_project(tmp_path, [_case("F", CB10)])
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    assert _by_uid(results)["F"]["status"] == "skip"
