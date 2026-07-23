"""``mocksql inspect <model> -u <uid>`` — diagnostic déterministe d'un cas rouge.

Contrat cible (cf. docs/inspect-diagnostic.md), zéro LLM, bout-en-bout sur un vrai
DuckDB local :

1. rejeu + `sql_source` (dont le repli `snapshot-fallback` = garde-fou F4) + le diff
   `expect_check` ;
2. trace CTE par CTE — la 1ʳᵉ CTE requise vide est le suspect n°1 (`blocking`) ;
3. sondes de cardinalité join par join (left/right/result) — descripteur factuel de la
   transformation (`fan_out` / `shrinks` / `preserves`), l'oracle `expect` primant dessus.

Le champ `diagnosis.code` résume la cause probable par priorité déterministe.
"""

import asyncio
import json

from cli.test_runner import inspect_case


# ── Scaffolding projet (mêmes conventions que test_replay_expect_contract) ────


def _write_project(tmp_path, *, sql, schemas, used_columns, cases, write_sql=True):
    (tmp_path / "models").mkdir()
    if write_sql:
        (tmp_path / "models" / "m.sql").write_text(sql, encoding="utf-8")
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    mocksql_dir = tmp_path / ".mocksql"
    (mocksql_dir / "tests").mkdir(parents=True)
    (mocksql_dir / "schema_cache.json").write_text(
        json.dumps({"tables": schemas}), encoding="utf-8"
    )
    doc = {"sql": sql, "used_columns": used_columns, "test_cases": cases}
    (mocksql_dir / "tests" / "m.json").write_text(
        json.dumps(doc, ensure_ascii=False), encoding="utf-8"
    )
    return tmp_path / "mocksql.yml"


def _case(uid, data, *, expect=None):
    case = {
        "test_uid": uid,
        "test_index": uid,
        "test_name": f"case {uid}",
        "unit_test_description": f"desc {uid}",
        "data": data,
    }
    if expect is not None:
        case["expect"] = expect
    return case


def _inspect(config_path, uid):
    return asyncio.run(inspect_case(config_path, "m", uid))


def _cte(trace, name):
    return next(c for c in trace if c["name"] == name)


# ── 1. CTE amont vide → suspect n°1 ──────────────────────────────────────────

_SQL_FILTER = (
    "WITH filtered AS (\n"
    "  SELECT customer_id, amount FROM `p.d.orders` WHERE amount > 100\n"
    ")\n"
    "SELECT customer_id, SUM(amount) AS total FROM filtered "
    "GROUP BY customer_id ORDER BY customer_id"
)
_SCHEMAS_ORDERS = [
    {
        "table_name": "p.d.orders",
        "columns": [
            {"name": "customer_id", "type": "INT64", "bq_ddl_type": "INT64"},
            {"name": "amount", "type": "FLOAT64", "bq_ddl_type": "FLOAT64"},
        ],
    }
]
_UC_ORDERS = [
    {
        "project": "p",
        "database": "d",
        "table": "orders",
        "used_columns": ["customer_id", "amount"],
    }
]


def test_inspect_empty_upstream_cte_is_suspect(tmp_path):
    # amount <= 100 → la CTE `filtered` produit 0 ligne → résultat final vide.
    cfg = _write_project(
        tmp_path,
        sql=_SQL_FILTER,
        schemas=_SCHEMAS_ORDERS,
        used_columns=_UC_ORDERS,
        cases=[
            _case(
                "aaaa",
                {"d_orders": [{"customer_id": 42, "amount": 10.0}]},
                # Contrat prescriptif : on VEUT une ligne — le cas est rouge.
                expect={
                    "columns": ["customer_id", "total"],
                    "rows": [{"customer_id": 42, "total": 10.0}],
                    "ordered": True,
                },
            )
        ],
    )
    out = _inspect(cfg, "aaaa")

    assert out["model"] == "m"
    assert out["test_uid"] == "aaaa"
    assert out["sql_source"] == "disk"
    assert out["sql_source_warning"] is None
    # Diff de lignes exposé.
    assert out["expect_check"]["passed"] is False
    assert out["observed"]["row_count"] == 0
    # Trace CTE ordonnée : `filtered` vide et bloquante = suspect n°1.
    ft = _cte(out["cte_trace"], "filtered")
    assert ft["row_count"] == 0
    assert ft["blocking"] is True
    assert out["diagnosis"]["code"] == "empty_upstream_cte"
    assert out["diagnosis"]["suspect"] == "filtered"
    # Déterministe : pas de verdict LLM sans --llm.
    assert out["llm_verdict"] is None


# ── 2. JOIN qui sur-produit (doublons) ───────────────────────────────────────

_SQL_JOIN = (
    "WITH j AS (\n"
    "  SELECT o.id AS id, p.amount AS amount\n"
    "  FROM `p.d.orders2` o JOIN `p.d.payments` p ON o.id = p.order_id\n"
    ")\n"
    "SELECT id, SUM(amount) AS total FROM j GROUP BY id ORDER BY id"
)
_SCHEMAS_JOIN = [
    {
        "table_name": "p.d.orders2",
        "columns": [{"name": "id", "type": "INT64", "bq_ddl_type": "INT64"}],
    },
    {
        "table_name": "p.d.payments",
        "columns": [
            {"name": "order_id", "type": "INT64", "bq_ddl_type": "INT64"},
            {"name": "amount", "type": "FLOAT64", "bq_ddl_type": "FLOAT64"},
        ],
    },
]
_UC_JOIN = [
    {"project": "p", "database": "d", "table": "orders2", "used_columns": ["id"]},
    {
        "project": "p",
        "database": "d",
        "table": "payments",
        "used_columns": ["order_id", "amount"],
    },
]


def test_inspect_join_fan_out_without_expect(tmp_path):
    # 1 commande, 2 paiements → le JOIN multiplie la commande (fan-out). SANS contrat
    # `expect`, il n'y a pas d'oracle pour ancrer le diagnostic → le fait structurel de
    # cardinalité est le seul signal et remonte comme cause (`join_fan_out`).
    cfg = _write_project(
        tmp_path,
        sql=_SQL_JOIN,
        schemas=_SCHEMAS_JOIN,
        used_columns=_UC_JOIN,
        cases=[
            _case(
                "bbbb",
                {
                    "d_orders2": [{"id": 1}],
                    "d_payments": [
                        {"order_id": 1, "amount": 10.0},
                        {"order_id": 1, "amount": 20.0},
                    ],
                },
            )
        ],
    )
    out = _inspect(cfg, "bbbb")

    probe = next(p for p in out["join_probes"] if p["cte"] == "j")
    assert probe["join_index"] == 0
    assert probe["join_type"] == "INNER"
    assert probe["left_rows"] == 1
    assert probe["right_rows"] == 2
    assert probe["result_rows"] == 2
    # Descripteur factuel (renommé), pas un jugement à charge.
    assert probe["verdict"] == "fan_out"
    assert out["diagnosis"]["code"] == "join_fan_out"
    assert out["diagnosis"]["suspect"] == "j#0"


def test_inspect_expect_diff_outranks_incidental_fan_out(tmp_path):
    # Point #1 : un JOIN un-à-plusieurs SAIN fan-out (1 → 2), mais la vraie cause du rouge
    # est un écart de VALEUR (total attendu ≠ observé). Le diagnostic doit s'ancrer sur
    # l'oracle (`expect_diff`), PAS pointer le JOIN sain (`join_fan_out`). La sonde continue
    # de VOIR le fan-out (fait conservé) — elle ne le promeut simplement plus en cause.
    cfg = _write_project(
        tmp_path,
        sql=_SQL_JOIN,
        schemas=_SCHEMAS_JOIN,
        used_columns=_UC_JOIN,
        cases=[
            _case(
                "eeee",
                {
                    "d_orders2": [{"id": 1}],
                    "d_payments": [
                        {"order_id": 1, "amount": 10.0},
                        {"order_id": 1, "amount": 20.0},
                    ],
                },
                # SUM(amount) observé = 30 ; le contrat en veut 99 → cas rouge sur la VALEUR.
                expect={
                    "columns": ["id", "total"],
                    "rows": [{"id": 1, "total": 99.0}],
                    "ordered": True,
                },
            )
        ],
    )
    out = _inspect(cfg, "eeee")

    # Le fan-out est toujours mesuré et rapporté comme fait…
    probe = next(p for p in out["join_probes"] if p["cte"] == "j")
    assert probe["verdict"] == "fan_out"
    assert probe["left_rows"] == 1 and probe["result_rows"] == 2
    # …mais le diagnostic est ancré sur l'oracle, pas sur le JOIN sain.
    assert out["expect_check"]["passed"] is False
    assert out["diagnosis"]["code"] == "expect_diff"
    assert out["diagnosis"]["suspect"] is None


# ── 3. Repli snapshot (F4) : le .sql n'a pas été lu → warning ─────────────────


def test_inspect_snapshot_fallback_warns(tmp_path):
    # models/m.sql absent → resolve_run_sql retombe sur le snapshot figé (snapshot-fallback).
    cfg = _write_project(
        tmp_path,
        sql=_SQL_FILTER,
        schemas=_SCHEMAS_ORDERS,
        used_columns=_UC_ORDERS,
        cases=[_case("cccc", {"d_orders": [{"customer_id": 42, "amount": 10.0}]})],
        write_sql=False,
    )
    out = _inspect(cfg, "cccc")

    assert out["sql_source"] == "snapshot-fallback"
    assert isinstance(out["sql_source_warning"], str)
    assert out["sql_source_warning"]
    # F4 : le repli empoisonne la confiance → il prime sur tout autre code.
    assert out["diagnosis"]["code"] == "sql_source_fallback"


# ── 4. Dérive SQL : inspect s'aligne sur `test` (confirmé + disque dérivé) ────


def test_inspect_matches_test_semantics_on_drifted_confirmed(tmp_path):
    # Un contrat confirmé + un `.sql` disque qui a DÉRIVÉ du snapshot = régime « edit
    # SQL » (le cas d'usage même d'inspect). `test` rapporte alors unconfirmed/stale
    # (pas un fail dur — F3). inspect DOIT donner la même sémantique, sinon il mislabelle
    # tout modèle confirmé édité en `fail`.
    snapshot_sql = "SELECT customer_id, amount AS total FROM `p.d.orders`"
    disk_sql = "SELECT customer_id, amount + 1 AS total FROM `p.d.orders`"
    cfg = _write_project(
        tmp_path,
        sql=disk_sql,  # écrit dans models/m.sql (le disque)
        schemas=_SCHEMAS_ORDERS,
        used_columns=_UC_ORDERS,
        cases=[
            {
                "test_uid": "dddd",
                "test_index": "dddd",
                "test_name": "confirmé + dérivé",
                "unit_test_description": "desc",
                "data": {"d_orders": [{"customer_id": 42, "amount": 10.0}]},
                # Contrat gelé sur la sortie du snapshot (total 10) ; le disque produit 11.
                "expect": {
                    "columns": ["customer_id", "total"],
                    "rows": [{"customer_id": 42, "total": 10.0}],
                    "ordered": True,
                },
                "review": {"status": "confirmed", "confirmed_by": "user"},
            }
        ],
    )
    # Le snapshot du doc diffère du .sql disque → dérive.
    doc_path = tmp_path / ".mocksql" / "tests" / "m.json"
    doc = json.loads(doc_path.read_text(encoding="utf-8"))
    doc["sql"] = snapshot_sql
    doc_path.write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")

    out = _inspect(cfg, "dddd")
    assert out["sql_source"] == "disk"
    # Dérive d'un contrat confirmé = stale/unconfirmed (comme `test`), pas fail/confirmed.
    assert out["status"] == "unconfirmed"
    assert out["review"] == "stale"
    assert out["expect_check"]["passed"] is False


# ── 5. test_uid introuvable → erreur claire ──────────────────────────────────


def test_inspect_unknown_uid_raises(tmp_path):
    cfg = _write_project(
        tmp_path,
        sql=_SQL_FILTER,
        schemas=_SCHEMAS_ORDERS,
        used_columns=_UC_ORDERS,
        cases=[_case("aaaa", {"d_orders": [{"customer_id": 1, "amount": 1.0}]})],
    )
    try:
        _inspect(cfg, "zzzz")
        assert False, "attendu : RuntimeError sur uid inconnu"
    except RuntimeError as exc:
        assert "zzzz" in str(exc)
