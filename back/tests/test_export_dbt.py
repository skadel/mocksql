"""Phase 2.3 — export des tests MockSQL en unit tests dbt (spec export-dbt + §7 valid-hum).

Le contrat ``expect`` (lignes confirmées) EST le bloc ``expect:`` dbt : export
déterministe, zéro LLM, sans replay. Couvre le mapping nominal, les gates d'exclusion
(non confirmé / mort-né / hors DAG / non sérialisable / modèle incremental), le vide
intentionnel, l'idempotence et le ``--check``.
"""

import json

import pytest
import yaml

from cli.export_dbt import ExportError, export_doc, render_yaml, run_export
from storage.dbt_manifest import DbtProject, _load_manifest


# ── Fixture projet dbt minimal ───────────────────────────────────────────────


def _manifest(materialized="table"):
    return {
        "nodes": {
            "model.pkg.orders": {
                "resource_type": "model",
                "name": "orders",
                "package_name": "pkg",
                "original_file_path": "models/marts/orders.sql",
                "relation_name": '"db"."marts"."orders"',
                "config": {"materialized": materialized},
                "depends_on": {
                    "nodes": ["model.pkg.stg_customers", "source.pkg.raw.events"]
                },
            },
            "model.pkg.stg_customers": {
                "resource_type": "model",
                "name": "stg_customers",
                "package_name": "pkg",
                "original_file_path": "models/staging/stg_customers.sql",
                "relation_name": '"db"."staging"."stg_customers"',
            },
        },
        "sources": {
            "source.pkg.raw.events": {
                "resource_type": "source",
                "source_name": "raw",
                "name": "events",
                "relation_name": '"db"."raw"."events"',
            }
        },
    }


def _project(tmp_path, materialized="table"):
    target = tmp_path / "warehouse" / "target"
    target.mkdir(parents=True)
    (target / "manifest.json").write_text(
        json.dumps(_manifest(materialized)), encoding="utf-8"
    )
    _load_manifest.cache_clear()
    return DbtProject(tmp_path / "warehouse")


def _confirmed_case(uid, *, data, expect_rows, expect_columns, name="Cas nominal"):
    return {
        "test_uid": uid,
        "test_index": uid,
        "test_name": name,
        "unit_test_description": f"desc {uid}",
        "status": "complete",
        "verdict": "Bon",
        "data": data,
        "expect": {
            "columns": list(expect_columns),
            "rows": expect_rows,
            "ordered": False,
        },
        "review": {"status": "confirmed", "confirmed_by": "user"},
    }


# Table peuplée : staging_stg_customers (flatten de db.staging.stg_customers).
STG_ROWS = [{"customer_id": 1, "name": "Ada"}]


# ── Mapping nominal ──────────────────────────────────────────────────────────


def test_nominal_mapping_given_and_expect(tmp_path):
    project = _project(tmp_path)
    doc = {
        "test_cases": [
            _confirmed_case(
                "aaaa1111",
                data={"staging_stg_customers": STG_ROWS},
                expect_rows=[{"customer_id": 1, "orders_count": 2}],
                expect_columns=["customer_id", "orders_count"],
            )
        ]
    }
    result = export_doc(doc, "marts/orders", project)
    assert result.model_error is None
    assert result.excluded == []
    assert len(result.unit_tests) == 1
    ut = result.unit_tests[0]
    assert ut["name"] == "mocksql__cas_nominal__aaaa1111"
    assert ut["model"] == "orders"
    assert ut["config"] == {"tags": ["mocksql"]}
    assert ut["description"] == "desc aaaa1111"
    # given liste TOUS les parents (ref + source) ; le parent non peuplé a rows: [].
    given = {g["input"]: g["rows"] for g in ut["given"]}
    assert given["ref('stg_customers')"] == STG_ROWS
    assert given["source('raw', 'events')"] == []
    assert ut["expect"]["rows"] == [{"customer_id": 1, "orders_count": 2}]


def test_render_yaml_is_valid_and_reparseable(tmp_path):
    project = _project(tmp_path)
    doc = {
        "test_cases": [
            _confirmed_case(
                "aaaa1111",
                data={"staging_stg_customers": STG_ROWS},
                expect_rows=[{"customer_id": 1, "orders_count": 2}],
                expect_columns=["customer_id", "orders_count"],
            )
        ]
    }
    result = export_doc(doc, "marts/orders", project)
    rendered = render_yaml(result.unit_tests)
    assert rendered.startswith("# Généré par MockSQL")
    parsed = yaml.safe_load(rendered)
    ut = parsed["unit_tests"][0]
    assert ut["model"] == "orders"
    # Les refs restent des scalaires plats exploitables par dbt.
    assert any(g["input"] == "ref('stg_customers')" for g in ut["given"])
    assert any(g["input"] == "source('raw', 'events')" for g in ut["given"])


# ── Gates d'exclusion ────────────────────────────────────────────────────────


def test_draft_case_excluded_not_confirmed(tmp_path):
    project = _project(tmp_path)
    case = _confirmed_case(
        "bbbb2222",
        data={"staging_stg_customers": STG_ROWS},
        expect_rows=[{"customer_id": 1, "orders_count": 2}],
        expect_columns=["customer_id", "orders_count"],
    )
    case["review"] = {"status": "draft"}
    result = export_doc({"test_cases": [case]}, "marts/orders", project)
    assert result.unit_tests == []
    assert result.excluded[0][0] == "bbbb2222"
    assert "non confirmé" in result.excluded[0][1]


def test_deadborn_case_excluded(tmp_path):
    project = _project(tmp_path)
    case = _confirmed_case(
        "cccc3333",
        data={"staging_stg_customers": STG_ROWS},
        expect_rows=[],
        expect_columns=[],
    )
    case["status"] = "error"
    case["verdict"] = "Insuffisant"
    result = export_doc({"test_cases": [case]}, "marts/orders", project)
    assert result.unit_tests == []
    assert "mort-né" in result.excluded[0][1]


def test_table_outside_dag_excluded(tmp_path):
    project = _project(tmp_path)
    case = _confirmed_case(
        "dddd4444",
        data={"some_random_table": STG_ROWS},
        expect_rows=[{"customer_id": 1}],
        expect_columns=["customer_id"],
    )
    result = export_doc({"test_cases": [case]}, "marts/orders", project)
    assert result.unit_tests == []
    assert "hors DAG" in result.excluded[0][1]


def test_unserializable_value_excluded(tmp_path):
    project = _project(tmp_path)
    case = _confirmed_case(
        "eeee5555",
        data={"staging_stg_customers": [{"customer_id": 1, "payload": {"k": "v"}}]},
        expect_rows=[{"customer_id": 1}],
        expect_columns=["customer_id"],
    )
    result = export_doc({"test_cases": [case]}, "marts/orders", project)
    assert result.unit_tests == []
    assert "non sérialisable" in result.excluded[0][1]


def test_empty_intent_exports_empty_expect_rows(tmp_path):
    # Vide intentionnel confirmé → expect.rows: [] exportable tel quel.
    project = _project(tmp_path)
    case = _confirmed_case(
        "ffff6666",
        data={"staging_stg_customers": STG_ROWS},
        expect_rows=[],
        expect_columns=[],
        name="Plage vide",
    )
    result = export_doc({"test_cases": [case]}, "marts/orders", project)
    assert len(result.unit_tests) == 1
    assert result.unit_tests[0]["expect"]["rows"] == []


# ── Gates niveau modèle ──────────────────────────────────────────────────────


def test_incremental_model_excluded(tmp_path):
    project = _project(tmp_path, materialized="incremental")
    doc = {
        "test_cases": [
            _confirmed_case(
                "aaaa1111",
                data={"staging_stg_customers": STG_ROWS},
                expect_rows=[{"customer_id": 1}],
                expect_columns=["customer_id"],
            )
        ]
    }
    result = export_doc(doc, "marts/orders", project)
    assert result.unit_tests == []
    assert "incremental" in result.model_error


def test_non_dbt_model_excluded(tmp_path):
    project = _project(tmp_path)
    result = export_doc({"test_cases": []}, "marts/ghost", project)
    assert result.model_error is not None
    assert "pas un modèle dbt" in result.model_error


# ── Déterminisme / idempotence ───────────────────────────────────────────────


def test_cases_sorted_by_uid_deterministically(tmp_path):
    project = _project(tmp_path)
    c1 = _confirmed_case(
        "ffff9999",
        data={"staging_stg_customers": STG_ROWS},
        expect_rows=[{"customer_id": 1}],
        expect_columns=["customer_id"],
        name="Z",
    )
    c2 = _confirmed_case(
        "aaaa0000",
        data={"staging_stg_customers": STG_ROWS},
        expect_rows=[{"customer_id": 2}],
        expect_columns=["customer_id"],
        name="A",
    )
    r_forward = export_doc({"test_cases": [c1, c2]}, "marts/orders", project)
    r_reverse = export_doc({"test_cases": [c2, c1]}, "marts/orders", project)
    assert render_yaml(r_forward.unit_tests) == render_yaml(r_reverse.unit_tests)
    # Tri par test_uid : aaaa0000 avant ffff9999.
    assert [ut["name"] for ut in r_forward.unit_tests] == [
        "mocksql__a__aaaa0000",
        "mocksql__z__ffff9999",
    ]


# ── run_export bout-en-bout (--dry-run / write / --check) ────────────────────


def _write_project_fs(tmp_path):
    (tmp_path / "models" / "marts").mkdir(parents=True)
    (tmp_path / "models" / "marts" / "orders.sql").write_text(
        "SELECT 1", encoding="utf-8"
    )
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\ndbt:\n  project_dir: warehouse\n",
        encoding="utf-8",
    )
    _project(tmp_path)  # écrit le manifest
    tests_root = tmp_path / ".mocksql" / "tests" / "marts"
    tests_root.mkdir(parents=True)
    doc = {
        "sql": "SELECT 1",
        "test_cases": [
            _confirmed_case(
                "aaaa1111",
                data={"staging_stg_customers": STG_ROWS},
                expect_rows=[{"customer_id": 1, "orders_count": 2}],
                expect_columns=["customer_id", "orders_count"],
            )
        ],
    }
    (tests_root / "orders.json").write_text(
        json.dumps(doc, ensure_ascii=False), encoding="utf-8"
    )
    return tmp_path / "mocksql.yml"


def test_run_export_writes_then_idempotent(tmp_path):
    cfg = _write_project_fs(tmp_path)
    out = tmp_path / "models" / "marts" / "orders.mocksql.yml"

    code, exports = run_export(cfg, targets=["marts/orders"])
    assert code == 0
    assert exports[0].action == "written"
    assert out.exists()
    first = out.read_text(encoding="utf-8")

    # Ré-export : octets identiques → unchanged.
    code, exports = run_export(cfg, targets=["marts/orders"])
    assert code == 0
    assert exports[0].action == "unchanged"
    assert out.read_text(encoding="utf-8") == first


def test_run_export_check_detects_drift(tmp_path):
    cfg = _write_project_fs(tmp_path)
    run_export(cfg, targets=["marts/orders"])  # écrit le fichier de référence

    # --check sans changement → vert.
    code, exports = run_export(cfg, targets=["marts/orders"], check=True)
    assert code == 0 and exports[0].action == "unchanged"

    # On mute le doc → le YAML exporté diverge → --check rouge, rien réécrit.
    test_file = tmp_path / ".mocksql" / "tests" / "marts" / "orders.json"
    doc = json.loads(test_file.read_text(encoding="utf-8"))
    doc["test_cases"][0]["expect"]["rows"] = [{"customer_id": 1, "orders_count": 99}]
    test_file.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")

    code, exports = run_export(cfg, targets=["marts/orders"], check=True)
    assert code == 1 and exports[0].action == "drift"


def test_run_export_dry_run_writes_nothing(tmp_path):
    cfg = _write_project_fs(tmp_path)
    out = tmp_path / "models" / "marts" / "orders.mocksql.yml"
    code, exports = run_export(cfg, targets=["marts/orders"], dry_run=True)
    assert code == 0
    assert exports[0].action == "dry-run"
    assert not out.exists()


def test_run_export_all_discovers_dbt_models(tmp_path):
    cfg = _write_project_fs(tmp_path)
    code, exports = run_export(cfg, all_models=True)
    assert code == 0
    assert [e.result.model for e in exports] == ["marts/orders"]


def test_run_export_requires_dbt_block(tmp_path):
    (tmp_path / "mocksql.yml").write_text("dialect: bigquery\n", encoding="utf-8")
    with pytest.raises(ExportError):
        run_export(tmp_path / "mocksql.yml", targets=["x"])
