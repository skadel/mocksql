"""`mocksql parity` — audit de parité DuckDB ↔ warehouse (cf. docs/spec-parity.md).

Couvre les trois piliers du design :
- l'**empreinte** d'attestation (sha256 SQL normalisé + données + dialecte + version
  transpileur) et ses états verified / stale / unverified ;
- le **comparateur** (multiset vs ordonné, tolérance flottante, NULL ≡ NULL, dates en
  ISO UTC, noms de colonnes insensibles à la casse, casse des chaînes = vrai diff) ;
- la **requête mockée** en dialecte warehouse (CTEs inline typées EN TÊTE du WITH,
  vrai schéma sans inférence, table vide typée) et l'orchestration idempotente de
  `run_parity` (attestation committée, rejeu seulement si empreinte périmée, diff
  n'écrit rien, codes de sortie 0/1/2).
"""

import asyncio
import json

import pytest

from cli.parity import (
    ParityExecutionError,
    build_mocked_warehouse_sql,
    compare_results,
    compute_fingerprint,
    has_terminal_order_by,
    normalize_value,
    parity_state,
    run_parity,
)
from cli.test_runner import SchemaMissingError


# ── Empreinte ─────────────────────────────────────────────────────────────────


def test_fingerprint_stable_across_reformatting():
    # Un reformatage sans changement sémantique ne doit PAS invalider l'attestation.
    a = compute_fingerprint("SELECT  a,b   FROM `p.d.t`", {"d_t": []}, "bigquery")
    b = compute_fingerprint("select a, b from `p.d.t`", {"d_t": []}, "bigquery")
    assert a == b
    assert a.startswith("sha256:")


def test_fingerprint_changes_with_sql_data_and_dialect():
    base = compute_fingerprint("SELECT a FROM `p.d.t`", {"d_t": [{"a": 1}]}, "bigquery")
    assert base != compute_fingerprint(
        "SELECT a, b FROM `p.d.t`", {"d_t": [{"a": 1}]}, "bigquery"
    )
    assert base != compute_fingerprint(
        "SELECT a FROM `p.d.t`", {"d_t": [{"a": 2}]}, "bigquery"
    )
    assert base != compute_fingerprint(
        "SELECT a FROM p.d.t", {"d_t": [{"a": 1}]}, "snowflake"
    )


def test_parity_state_lifecycle():
    fp = compute_fingerprint("SELECT 1", {}, "bigquery")
    assert parity_state({}, fp, "bigquery") == "unverified"
    attested = {"parity": {"fingerprint": fp, "dialect": "bigquery"}}
    assert parity_state(attested, fp, "bigquery") == "verified"
    # Empreinte périmée (SQL/données/version ont changé) OU dialecte différent → stale.
    assert parity_state(attested, "sha256:autre", "bigquery") == "stale"
    other_dialect = {"parity": {"fingerprint": fp, "dialect": "snowflake"}}
    assert parity_state(other_dialect, fp, "bigquery") == "stale"


# ── Comparateur ───────────────────────────────────────────────────────────────


def test_compare_multiset_ignores_order_and_column_case():
    from decimal import Decimal

    local = [{"a": 1.0, "b": "x"}, {"a": 2.0, "b": "y"}]
    warehouse = [{"A": Decimal("2"), "B": "y"}, {"A": Decimal("1"), "B": "x"}]
    assert compare_results(local, warehouse, ordered=False) is None


def test_compare_ordered_detects_order_mismatch():
    local = [{"a": 1}, {"a": 2}]
    warehouse = [{"a": 2}, {"a": 1}]
    assert compare_results(local, warehouse, ordered=False) is None
    assert compare_results(local, warehouse, ordered=True) is not None


def test_compare_float_tolerance_and_true_numeric_diff():
    # Les moteurs n'additionnent pas dans le même ordre : 0.1+0.2 ≡ 0.3.
    assert compare_results([{"x": 0.1 + 0.2}], [{"x": 0.3}], ordered=False) is None
    diff = compare_results([{"x": 0.30001}], [{"x": 0.3}], ordered=False)
    assert diff is not None and diff["reason"] == "rows_mismatch"


def test_compare_null_equivalence_and_nan():
    # NULL ≡ NULL quel que soit le type porteur (NaN pandas compris).
    assert compare_results([{"x": float("nan")}], [{"X": None}], ordered=False) is None


def test_compare_string_case_is_a_diff():
    # Une différence de casse EST un diff (peut-être la collation — le DE doit le voir).
    diff = compare_results([{"s": "abc"}], [{"s": "ABC"}], ordered=False)
    assert diff is not None
    assert diff["local_only"] and diff["warehouse_only"]


def test_compare_engine_generated_column_names_align_positionally():
    # `_col_0` (DuckDB) vs `f0_` (BigQuery) sur une projection non nommée : les noms
    # auto-générés ne doivent pas fabriquer un faux DIFF — alignement par position.
    assert compare_results([{"_col_0": 42}], [{"f0_": 42}], ordered=False) is None


def test_compare_row_count_mismatch_is_a_diff():
    diff = compare_results([{"a": 1}], [], ordered=False)
    assert diff is not None
    assert diff["local_count"] == 1 and diff["warehouse_count"] == 0


def test_normalize_timestamps_to_utc_iso():
    import datetime as dt

    aware = dt.datetime(2024, 1, 1, 12, 0, tzinfo=dt.timezone.utc)
    naive = dt.datetime(2024, 1, 1, 12, 0)
    assert normalize_value(aware) == normalize_value(naive)


def test_normalize_json_canonical():
    # VARIANT Snowflake pretty-printé vs JSON DuckDB compact → forme canonique.
    assert normalize_value('{ "b" : 1.0, "a": 2 }') == normalize_value('{"a":2,"b":1}')


# ── Requête mockée warehouse ──────────────────────────────────────────────────

_BQ_SCHEMAS = [
    {
        "table_name": "p.d.t",
        "columns": [
            {"name": "payment", "type": "STRING", "bq_ddl_type": "STRING"},
            {"name": "amount", "type": "FLOAT64", "bq_ddl_type": "FLOAT64"},
        ],
    }
]


def test_mocked_sql_inlines_ctes_before_model_ctes():
    sql = (
        "WITH base AS (SELECT payment, amount FROM `p.d.t`) "
        "SELECT payment, SUM(amount) AS total FROM base GROUP BY payment"
    )
    data = {"d_t": [{"payment": "cb", "amount": 7.5}]}
    mocked = build_mocked_warehouse_sql(sql, "bigquery", _BQ_SCHEMAS, data)

    # La CTE mock est définie AVANT la CTE du modèle (pas de référence en avant),
    # et plus aucune table physique n'est référencée.
    assert mocked.index("d_t_mocksql_parity") < mocked.index("base AS")
    assert "`p.d.t`" not in mocked
    assert "'cb'" in mocked and "7.5" in mocked


def test_mocked_sql_empty_table_keeps_types():
    # Table vide → CTE typée (CAST NULL + LIMIT 0), jamais d'inférence de littéraux.
    sql = "SELECT payment FROM `p.d.t`"
    mocked = build_mocked_warehouse_sql(sql, "bigquery", _BQ_SCHEMAS, {"d_t": []})
    assert "CAST(NULL AS STRING)" in mocked
    assert "LIMIT 0" in mocked


def test_mocked_sql_snowflake_variant_uses_parse_json():
    schemas = [
        {
            "table_name": "DB.S.EVENTS",
            "columns": [
                {"name": "ID", "type": "NUMBER(38,0)"},
                {"name": "PAYLOAD", "type": "VARIANT"},
            ],
        }
    ]
    sql = "SELECT ID FROM DB.S.EVENTS"
    data = {"S_EVENTS": [{"ID": 1, "PAYLOAD": '{"k": "v"}'}]}
    mocked = build_mocked_warehouse_sql(sql, "snowflake", schemas, data)
    assert "PARSE_JSON(" in mocked
    assert "S_EVENTS_mocksql_parity" in mocked


def test_mocked_sql_missing_schema_raises():
    # Même contrat que le réplay : vrai schéma obligatoire, zéro inférence.
    with pytest.raises(SchemaMissingError):
        build_mocked_warehouse_sql("SELECT x FROM `p.ghost.t`", "bigquery", [], {})


def test_has_terminal_order_by():
    assert has_terminal_order_by("SELECT a FROM `p.d.t` ORDER BY a", "bigquery")
    assert not has_terminal_order_by(
        "SELECT * FROM (SELECT a FROM `p.d.t` ORDER BY a LIMIT 3)", "bigquery"
    )


# ── Orchestration run_parity (bout-en-bout, warehouse simulé) ────────────────


def _write_project(tmp_path, rows, exec_status=None):
    """Projet MockSQL minimal : mocksql.yml + models/orders.sql + schema_cache +
    tests/orders.json (un cas portant les lignes `rows`)."""
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text(
        "SELECT payment, SUM(amount) AS total FROM `p.d.t` "
        "GROUP BY payment ORDER BY payment",
        encoding="utf-8",
    )
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    mocksql_dir = tmp_path / ".mocksql"
    (mocksql_dir / "tests").mkdir(parents=True)
    (mocksql_dir / "schema_cache.json").write_text(
        json.dumps({"tables": _BQ_SCHEMAS}), encoding="utf-8"
    )
    case = {
        "test_index": "0",
        "test_name": "Somme par moyen de paiement",
        "data": {"d_t": rows},
        "assertion_results": [],
    }
    if exec_status:
        case["exec_status"] = exec_status
    doc = {
        "sql": "SELECT 1",
        "used_columns": [
            {
                "project": "p",
                "database": "d",
                "table": "t",
                "used_columns": ["payment", "amount"],
            }
        ],
        "test_cases": [case],
    }
    test_file = mocksql_dir / "tests" / "orders.json"
    test_file.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")
    return test_file


def _duckdb_echo_executor(calls):
    """Simulateur warehouse : transpile la requête mockée (bigquery → duckdb) et
    l'exécute sur une connexion DuckDB fraîche — la requête doit être auto-portante
    (aucune table physique), sinon elle plante ici comme elle planterait en prod."""
    import duckdb
    import sqlglot

    def _executor(sql, dialect):
        calls.append(sql)
        duck_sql = sqlglot.transpile(sql, read=dialect, write="duckdb")[0]
        with duckdb.connect(":memory:") as con:
            df = con.execute(duck_sql).fetchdf()
        return df.to_dict("records")

    return _executor


ROWS = [
    {"payment": "cb", "amount": 7.5},
    {"payment": "cb", "amount": 2.5},
    {"payment": "cash", "amount": 1.0},
]


def test_run_parity_verifies_then_is_idempotent(tmp_path, monkeypatch):
    test_file = _write_project(tmp_path, ROWS)
    calls: list[str] = []
    monkeypatch.setattr(
        "cli.parity._execute_on_warehouse", _duckdb_echo_executor(calls)
    )

    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert exit_code == 0
    assert results[0]["cases"][0]["state"] == "verified"
    assert len(calls) == 1

    # L'attestation est écrite dans la DÉFINITION committée (voyage avec la repo).
    saved = json.loads(test_file.read_text(encoding="utf-8"))
    attestation = saved["test_cases"][0]["parity"]
    assert attestation["fingerprint"].startswith("sha256:")
    assert attestation["dialect"] == "bigquery"
    assert attestation["verified_at"]

    # Relance : rien à rejouer, aucune exécution warehouse (idempotent).
    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert exit_code == 0
    assert results[0]["cases"][0]["state"] == "verified_cached"
    assert len(calls) == 1

    # --all force le rejeu même vérifié.
    exit_code, _ = asyncio.run(run_parity(tmp_path / "mocksql.yml", force_all=True))
    assert exit_code == 0
    assert len(calls) == 2


def test_run_parity_stale_fingerprint_triggers_replay(tmp_path, monkeypatch):
    test_file = _write_project(tmp_path, ROWS)
    calls: list[str] = []
    monkeypatch.setattr(
        "cli.parity._execute_on_warehouse", _duckdb_echo_executor(calls)
    )
    asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert len(calls) == 1

    # Les données du test changent (patch agent) → empreinte périmée → rejeu.
    doc = json.loads(test_file.read_text(encoding="utf-8"))
    doc["test_cases"][0]["data"]["d_t"][0]["amount"] = 99.0
    test_file.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")

    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert exit_code == 0
    assert results[0]["cases"][0]["state"] == "verified"
    assert len(calls) == 2


def test_run_parity_diff_reports_and_writes_nothing(tmp_path, monkeypatch):
    test_file = _write_project(tmp_path, ROWS)
    before = test_file.read_text(encoding="utf-8")

    def _diverging_executor(sql, dialect):
        return [{"payment": "cb", "total": 12345.0}]

    monkeypatch.setattr("cli.parity._execute_on_warehouse", _diverging_executor)
    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))

    # Le diff n'est PAS un échec de test : exit 1, rapport côte à côte, et RIEN
    # n'est écrit (pas d'attestation négative committée).
    assert exit_code == 1
    case = results[0]["cases"][0]
    assert case["state"] == "diff"
    assert case["diff"]["local_only"] and case["diff"]["warehouse_only"]
    assert test_file.read_text(encoding="utf-8") == before


def test_run_parity_warehouse_error_exits_2(tmp_path, monkeypatch):
    _write_project(tmp_path, ROWS)

    def _failing_executor(sql, dialect):
        raise ParityExecutionError("credentials invalides")

    monkeypatch.setattr("cli.parity._execute_on_warehouse", _failing_executor)
    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert exit_code == 2
    assert results[0]["cases"][0]["state"] == "error"


def test_run_parity_skips_deadborn_cases(tmp_path, monkeypatch):
    _write_project(tmp_path, ROWS, exec_status="error")

    def _must_not_run(sql, dialect):  # pragma: no cover
        raise AssertionError("un test mort-né ne doit jamais être rejoué")

    monkeypatch.setattr("cli.parity._execute_on_warehouse", _must_not_run)
    exit_code, results = asyncio.run(run_parity(tmp_path / "mocksql.yml"))
    assert exit_code == 0
    assert results[0]["cases"][0]["state"] == "skip"


def test_run_parity_rejects_unsupported_dialect(tmp_path):
    _write_project(tmp_path, ROWS)
    cfg = tmp_path / "mocksql.yml"
    cfg.write_text("dialect: postgres\nmodels_path: models\n", encoding="utf-8")
    with pytest.raises(ParityExecutionError):
        asyncio.run(run_parity(cfg))
