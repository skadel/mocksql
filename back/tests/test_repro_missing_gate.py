"""Phase 1 — verrou RED ``repro_missing`` (thèse TDD honnête).

Un test de repro doit NAÎTRE ROUGE sur le SQL bugué : ses données doivent SÉPARER le
comportement bugué du désiré (cf. RAPPORT-repro-fitness §2). Un test « né vert » (le
contrat ``expect`` == la sortie courante) marqué ``intent=repro`` est insensible au bug
→ on le refuse explicitement (``repro_missing``, exit 1) au lieu de le laisser passer.

- ``_expect_verdict`` : ``intent=repro`` + non confirmé + PASSE → ``repro_missing`` ;
- ``run_tests`` : ``repro_missing`` = échec (exit 1) par défaut ;
- input rendu discriminant (expect ≠ sortie) → ``unconfirmed`` (rouge établi), le verrou
  s'éteint ; après fix + confirm → ``pass``.

Bout-en-bout sur un vrai DuckDB local (zéro LLM), snapshot figé (``--frozen``).
"""

import asyncio
import json

import pytest

from cli.test_runner import _expect_verdict, run_tests


def _check(passed, order_only=False):
    return {"passed": passed, "order_only_mismatch": order_only}


# ── _expect_verdict avec intent (verdict pur, sans DuckDB) ───────────────────


def test_repro_intent_born_green_is_repro_missing():
    # Marqué repro, encore draft, mais PASSE sur le SQL courant → né vert → refusé.
    assert _expect_verdict(_check(True), "draft", "repro") == "repro_missing"


def test_repro_intent_born_green_without_status_is_repro_missing():
    assert _expect_verdict(_check(True), None, "repro") == "repro_missing"


def test_repro_intent_red_draft_is_unconfirmed():
    # Rouge établi (expect ≠ sortie) → unconfirmed, PAS repro_missing : la repro tient.
    assert _expect_verdict(_check(False), "draft", "repro") == "unconfirmed"


def test_repro_intent_confirmed_pass_is_pass():
    # Une fois confirmé (après fix), l'intent repro est consommé → sémantique normale.
    assert _expect_verdict(_check(True), "confirmed", "repro") == "pass"


def test_repro_intent_confirmed_mismatch_is_fail():
    assert _expect_verdict(_check(False), "confirmed", "repro") == "fail"


@pytest.mark.parametrize("intent", [None, "", "other"])
def test_without_repro_intent_born_green_still_passes(intent):
    # Rétro-compat : sans intent repro, un cas qui passe reste `pass`.
    assert _expect_verdict(_check(True), "draft", intent) == "pass"


def test_two_arg_call_is_backward_compatible():
    # L'ancien appel à 2 arguments (partout dans le code) doit rester valide.
    assert _expect_verdict(_check(True), "draft") == "pass"
    assert _expect_verdict(_check(False), "confirmed") == "fail"


def test_repro_order_only_counts_as_born_green():
    # Ex-æquo (mêmes lignes, ordre ≠) = les lignes ne diffèrent pas → pas de repro.
    assert (
        _expect_verdict(_check(False, order_only=True), "draft", "repro")
        == "repro_missing"
    )


# ── run_tests bout-en-bout (DuckDB local, --frozen) ──────────────────────────

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
CB10 = {"d_t": [{"payment": "cb", "amount": 10.0}]}


def _expect(rows, columns=("payment", "total"), ordered=True):
    return {"columns": list(columns), "rows": rows, "ordered": ordered}


def _case(uid, data, *, expect=None, review=None):
    case = {
        "test_uid": uid,
        "test_index": uid,
        "test_name": f"case {uid}",
        "status": "complete",
        "data": data,
        "assertion_results": [],
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


def _run(tmp_path):
    return asyncio.run(run_tests(tmp_path / "mocksql.yml", None, False, frozen=True))


def _by_uid(results):
    return {c["index"]: c for c in results[0]["cases"]}


def test_e2e_born_green_repro_case_is_repro_missing_exit1(tmp_path):
    # expect == sortie réelle (10.0) + intent repro non confirmé → repro_missing, exit 1.
    _write_project(
        tmp_path,
        [
            _case(
                "A",
                CB10,
                expect=_expect([{"payment": "cb", "total": 10.0}]),
                review={"status": "draft", "intent": "repro"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 1
    assert _by_uid(results)["A"]["status"] == "repro_missing"


def test_e2e_red_repro_case_is_unconfirmed_exit0(tmp_path):
    # expect ≠ sortie (999 ≠ 10) → rouge établi → unconfirmed, verrou éteint, exit 0.
    _write_project(
        tmp_path,
        [
            _case(
                "A",
                CB10,
                expect=_expect([{"payment": "cb", "total": 999.0}]),
                review={"status": "draft", "intent": "repro"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    assert _by_uid(results)["A"]["status"] == "unconfirmed"


def test_e2e_born_green_without_intent_still_passes(tmp_path):
    # Contrôle : même cas SANS intent repro → pass, exit 0 (rétro-compat).
    _write_project(
        tmp_path,
        [
            _case(
                "A",
                CB10,
                expect=_expect([{"payment": "cb", "total": 10.0}]),
                review={"status": "draft"},
            )
        ],
    )
    exit_code, results = _run(tmp_path)
    assert exit_code == 0
    assert _by_uid(results)["A"]["status"] == "pass"


# ── mark-repro (pose review.intent=repro, déterministe, zéro LLM) ────────────
# Imports locaux : la commande n'existe pas encore au moment d'écrire le test
# (test-first) — on n'empêche pas la collecte des tests du verrou déjà verts.


def _write_doc(tmp_path, cases):
    from storage.test_files import write_test_doc

    write_test_doc(
        tmp_path / ".mocksql" / "tests" / "orders.json",
        {"sql": "SELECT 1", "test_cases": cases},
    )
    return tmp_path / "mocksql.yml"


def _saved_case(tmp_path, uid):
    from storage.test_files import read_test_doc

    doc = read_test_doc(tmp_path / ".mocksql" / "tests" / "orders.json")
    return next(c for c in doc["test_cases"] if c["test_uid"] == uid)


def test_mark_repro_sets_intent(tmp_path):
    from cli.manage_cmd import run_mark_repro

    config = _write_doc(tmp_path, [{"test_uid": "aaaa", "test_index": "0"}])
    result = run_mark_repro(config, "orders", "aaaa")
    assert result["review"]["intent"] == "repro"
    assert _saved_case(tmp_path, "aaaa")["review"]["intent"] == "repro"


def test_mark_repro_preserves_existing_status(tmp_path):
    from cli.manage_cmd import run_mark_repro

    config = _write_doc(
        tmp_path,
        [{"test_uid": "aaaa", "test_index": "0", "review": {"status": "draft"}}],
    )
    run_mark_repro(config, "orders", "aaaa")
    saved = _saved_case(tmp_path, "aaaa")
    assert saved["review"]["status"] == "draft"  # non écrasé
    assert saved["review"]["intent"] == "repro"


def test_mark_repro_preserves_prescriptive_expect_when_results_are_cached(tmp_path):
    """Une mutation de review ne re-snapshotte jamais expect depuis le sidecar."""
    from cli.manage_cmd import run_mark_repro
    from storage.test_files import read_test_doc, write_test_doc

    path = tmp_path / ".mocksql" / "tests" / "orders.json"
    observed = [{"amount": 10}]
    write_test_doc(
        path,
        {
            "sql": "SELECT 1",
            "test_cases": [
                {
                    "test_uid": "aaaa",
                    "test_index": "0",
                    "results_json": json.dumps(observed),
                }
            ],
        },
    )
    definition = json.loads(path.read_text(encoding="utf-8"))
    definition["test_cases"][0]["expect"]["rows"] = [{"amount": 99}]
    path.write_text(json.dumps(definition), encoding="utf-8")

    run_mark_repro(tmp_path / "mocksql.yml", "orders", "aaaa")

    saved = read_test_doc(path)["test_cases"][0]
    assert saved["expect"]["rows"] == [{"amount": 99}]
    assert saved["review"]["intent"] == "repro"


def test_mark_repro_unknown_uid_raises(tmp_path):
    from cli.doc_io import TestDocError
    from cli.manage_cmd import run_mark_repro

    config = _write_doc(tmp_path, [{"test_uid": "aaaa", "test_index": "0"}])
    with pytest.raises(TestDocError):
        run_mark_repro(config, "orders", "zzzz")


# ── --require-red (gate CI : tout doit être rouge) ───────────────────────────


def _mr(*statuses):
    return [
        {
            "model": "orders",
            "cases": [{"index": i, "status": s} for i, s in enumerate(statuses)],
        }
    ]


def test_require_red_all_red_no_violation():
    from cli.main import _not_red_cases

    assert _not_red_cases(_mr("unconfirmed", "fail")) == []


def test_require_red_flags_pass_and_repro_missing_and_error():
    from cli.main import _not_red_cases

    violations = _not_red_cases(_mr("pass", "repro_missing", "error", "unconfirmed"))
    # pass(0), repro_missing(1), error(2) violent ; unconfirmed(3) est rouge.
    assert [i for _, i in violations] == [0, 1, 2]


def test_require_red_skip_is_excluded():
    from cli.main import _not_red_cases

    assert _not_red_cases(_mr("skip", "fail")) == []
