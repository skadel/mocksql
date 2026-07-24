"""Phase 1.5b — `inspect --live` : waterfall de cardinalité sur l'entrepôt (gaté).

Vérifie l'orchestration (décomposition disque → sondes → waterfall annoté) avec un
executor entrepôt MOCKÉ (aucun accès réseau) : assemblage des comptes, ratios de
fan-out, tiered vs full, cas sans jointure, et propagation du refus du gate coût.
"""

import asyncio
from unittest.mock import patch

import pytest

from build_query.warehouse_gate import CostEstimate, WarehouseQueryDenied
from cli.test_runner import inspect_live

_SQL = (
    "SELECT COUNT(DISTINCT O.order_id) AS c\n"
    "FROM `p.d.order_items` AS OI\n"
    "JOIN `p.d.orders` AS O ON OI.order_id = O.order_id\n"
    "JOIN `p.d.products` AS P ON OI.product_id = P.id\n"
    "WHERE O.status = 'Complete'"
)


def _project(tmp_path, sql=_SQL):
    (tmp_path / "models").mkdir()
    (tmp_path / "models" / "orders.sql").write_text(sql, encoding="utf-8")
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    (tmp_path / ".mocksql").mkdir()
    return tmp_path / "mocksql.yml"


def _mock_warehouse(
    scan=90, after_j0=340, after_j1=340, right=(100, 100), pre_agg=(340, 90)
):
    """Executor entrepôt factice : COUNT(*) selon le nb de JOIN du préfixe ; les requêtes
    COUNT(DISTINCT) renvoient (n, d) — côté droit (sans join) ou pré-agrégat (avec joins)."""

    def ex(sql: str):
        up = sql.upper()
        njoins = up.count(" JOIN ")
        if "COUNT(DISTINCT" in up:
            n, d = right if njoins == 0 else pre_agg
            return [{"n": n, "d": d}]
        return [{"n": {0: scan, 1: after_j0, 2: after_j1}.get(njoins, 0)}]

    return ex


def test_waterfall_assembles_counts_and_fanout(tmp_path):
    config = _project(tmp_path)
    payload = asyncio.run(
        inspect_live(
            config, "orders", auto_approve=True, warehouse_executor=_mock_warehouse()
        )
    )
    assert payload["model"] == "orders"
    assert payload["dialect"] == "bigquery"
    wf = payload["live_waterfall"]
    assert len(wf) == 1  # tiered : final_query seulement
    probes = wf[0]["probes"]
    # scan + 2 joins + sonde pré-agrégat (le SQL porte un COUNT(DISTINCT …))
    assert [p["boundary"] for p in probes] == ["scan", "join", "join", "pre_agg"]
    assert probes[0]["rows"] == 90
    # ⋈ orders : 90 → 340 = fan_out ×3,78
    assert probes[1]["rows"] == 340
    assert probes[1]["verdict"] == "fan_out"
    assert probes[1]["fanout_ratio"] == 3.78
    # ⋈ products : 340 → 340 = preserves
    assert probes[2]["verdict"] == "preserves"
    # pré-agrégat : 340 lignes / 90 clés distinctes = la signature COUNT vs DISTINCT.
    assert probes[3]["rows"] == 340
    assert probes[3]["distinct_rows"] == 90
    assert probes[3]["verdict"] == "fan_out"
    assert probes[3]["agg_fanout_ratio"] == 3.78


def test_right_side_uniqueness_probed(tmp_path):
    config = _project(tmp_path)
    # order_items côté droit : 340 lignes mais 90 clés → non-unique (source du fan-out).
    payload = asyncio.run(
        inspect_live(
            config,
            "orders",
            auto_approve=True,
            warehouse_executor=_mock_warehouse(right=(340, 90)),
        )
    )
    j0 = payload["live_waterfall"][0]["probes"][1]
    assert j0["right_rows"] == 340
    assert j0["right_distinct"] == 90
    assert j0["right_non_unique"] is True


def test_full_probes_more_than_tiered(tmp_path):
    # Requête à CTE + join final → full sonde les deux, tiered seulement le dernier.
    sql = (
        "WITH agg AS (SELECT k FROM `p.d.a` AS x JOIN `p.d.b` AS y ON x.k = y.k)\n"
        "SELECT * FROM agg JOIN `p.d.c` AS z ON agg.k = z.k"
    )
    config = _project(tmp_path, sql=sql)
    tiered = asyncio.run(
        inspect_live(
            config, "orders", auto_approve=True, warehouse_executor=_mock_warehouse()
        )
    )
    full = asyncio.run(
        inspect_live(
            config,
            "orders",
            full=True,
            auto_approve=True,
            warehouse_executor=_mock_warehouse(),
        )
    )
    assert len(tiered["live_waterfall"]) == 1
    assert len(full["live_waterfall"]) == 2


def test_no_joins_returns_note(tmp_path):
    config = _project(tmp_path, sql="SELECT 1 AS x FROM `p.d.t`")
    payload = asyncio.run(
        inspect_live(
            config, "orders", auto_approve=True, warehouse_executor=_mock_warehouse()
        )
    )
    assert payload["live_waterfall"] == []
    assert "note" in payload


def test_refusal_raises_denied_and_emits_nothing(tmp_path):
    config = _project(tmp_path)
    emitted = []

    def spy(sql):
        emitted.append(sql)
        return [{"n": 1}]

    # Estimation facturée + refus utilisateur → WarehouseQueryDenied, aucune requête émise.
    billed = CostEstimate(dialect="bigquery", method="bq_dry_run", cost=1.0)
    with patch("build_query.warehouse_gate.estimate", return_value=billed):
        with pytest.raises(WarehouseQueryDenied):
            asyncio.run(
                inspect_live(
                    config,
                    "orders",
                    auto_approve=False,
                    prompt_fn=lambda q: False,
                    warehouse_executor=spy,
                )
            )
    assert emitted == []  # le refus intervient AVANT tout tir facturé


def test_missing_sql_raises(tmp_path):
    (tmp_path / "mocksql.yml").write_text(
        "dialect: bigquery\nmodels_path: models\n", encoding="utf-8"
    )
    with pytest.raises(RuntimeError):
        asyncio.run(
            inspect_live(
                tmp_path / "mocksql.yml",
                "ghost",
                auto_approve=True,
                warehouse_executor=_mock_warehouse(),
            )
        )
