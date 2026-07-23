"""Tests du chokepoint coût entrepôt (Phase T).

Principe produit : toute requête facturée vers l'entrepôt réel passe par UN point
de passage (``warehouse_gate``) qui estime le coût AVANT et exige une confirmation.
Refus → ``WarehouseQueryDenied`` propre, AUCUNE requête facturée émise.

Ces tests couvrent :
- ``estimate`` sur mocks (BigQuery dry-run, Snowflake EXPLAIN USING JSON, local bypass) ;
- ``confirm_or_raise`` (auto-approve, refus, env CI, notice métadonnées) ;
- ``run_gated`` / ``GatedExecutor`` : le refus n'exécute jamais la requête facturée.
"""

from unittest.mock import MagicMock, patch

import pytest

from build_query import warehouse_gate as wg
from build_query.warehouse_gate import CostEstimate


def _fake_bq_module(total_bytes):
    """Un faux module google.cloud.bigquery dont le dry-run renvoie *total_bytes*."""
    mod = MagicMock()
    job = MagicMock()
    job.total_bytes_processed = total_bytes
    client = MagicMock()
    client.query.return_value = job
    mod.Client.return_value = client
    mod.QueryJobConfig.return_value = MagicMock(name="job_config")
    return mod, client


class TestEstimate:
    def test_bigquery_dry_run_computes_cost(self):
        mod, client = _fake_bq_module(2 * 2**40)  # 2 TiB
        with (
            patch.object(wg, "import_bigquery", return_value=mod),
            patch.object(wg, "get_bq_price_per_tib", return_value=6.25),
        ):
            est = wg.estimate("SELECT 1", "bigquery", billing_project="p")

        assert est.method == "bq_dry_run"
        assert est.bytes_processed == 2 * 2**40
        assert est.cost == pytest.approx(12.5)  # 2 TiB × 6,25 $/TiB
        assert est.currency == "USD"
        assert est.is_billed is True
        # Le job DOIT être un dry-run (0 octet facturé pour l'estimation elle-même).
        assert mod.QueryJobConfig.call_args.kwargs.get("dry_run") is True

    def test_bigquery_failure_yields_unknown(self):
        mod = MagicMock()
        mod.Client.side_effect = RuntimeError("no creds")
        with patch.object(wg, "import_bigquery", return_value=mod):
            est = wg.estimate("SELECT 1", "bigquery", billing_project="p")
        assert est.method == "unknown"
        assert est.cost is None
        assert est.warnings  # explique pourquoi le coût est inconnu

    def test_snowflake_explain_json_no_money(self):
        stats = {"bytesAssigned": 1234, "partitionsAssigned": 3, "partitionsTotal": 10}
        with patch.object(wg, "explain_json", return_value=stats):
            est = wg.estimate("SELECT 1", "snowflake")
        assert est.method == "sf_explain_json"
        assert est.bytes_processed == 1234
        assert est.partitions_assigned == 3
        assert est.partitions_total == 10
        # Snowflake facture le temps de warehouse, pas les octets : jamais de montant.
        assert est.cost is None
        assert any("warehouse" in w.lower() for w in est.warnings)

    def test_local_dialect_bypasses_client(self):
        with patch.object(wg, "import_bigquery") as imp:
            est = wg.estimate("SELECT 1", "duckdb")
        assert est.method == "local"
        assert est.is_billed is False
        imp.assert_not_called()

    def test_metadata_estimate_is_not_billed(self):
        est = wg.metadata_estimate("snowflake", context="récupération du schéma")
        assert est.method == "metadata"
        assert est.is_billed is False


class TestConfirm:
    def _billed(self):
        return CostEstimate(
            dialect="bigquery", method="bq_dry_run", bytes_processed=2**40, cost=6.25
        )

    def test_auto_approve_skips_prompt(self):
        seen = []
        wg.confirm_or_raise(
            self._billed(),
            auto_approve=True,
            prompt_fn=lambda q: seen.append(q) or True,
        )
        assert seen == []

    def test_refusal_raises_denied(self):
        with pytest.raises(wg.WarehouseQueryDenied):
            wg.confirm_or_raise(self._billed(), prompt_fn=lambda q: False)

    def test_approval_returns_none(self):
        assert wg.confirm_or_raise(self._billed(), prompt_fn=lambda q: True) is None

    def test_env_auto_approve_skips_prompt(self, monkeypatch):
        monkeypatch.setenv("MOCKSQL_AUTO_APPROVE_DWH", "1")
        seen = []
        # prompt_fn refuserait — mais l'env auto-approuve avant de demander.
        wg.confirm_or_raise(self._billed(), prompt_fn=lambda q: seen.append(q) or False)
        assert seen == []

    def test_zero_cost_bigquery_never_prompts(self):
        # BQ on-demand à 0 octet = 0 $ (ex. requête à CTEs inline, parity) → pas de
        # friction. Snowflake à 0 octet, lui, reste facturé (temps de warehouse).
        free = CostEstimate(
            dialect="bigquery", method="bq_dry_run", bytes_processed=0, cost=0.0
        )
        assert free.is_billed is False
        seen = []
        wg.confirm_or_raise(free, prompt_fn=lambda q: seen.append(q) or False)
        assert seen == []

    def test_snowflake_zero_bytes_still_billed(self):
        est = CostEstimate(
            dialect="snowflake", method="sf_explain_json", bytes_processed=0, cost=None
        )
        assert est.is_billed is True

    def test_local_only_never_prompts(self):
        seen = []
        wg.confirm_or_raise(
            CostEstimate(dialect="duckdb", method="local"),
            prompt_fn=lambda q: seen.append(q) or False,
        )
        assert seen == []

    def test_metadata_notice_no_prompt(self):
        est = wg.metadata_estimate("snowflake", context="récupération du schéma")
        prompts, echoes = [], []
        wg.confirm_or_raise(
            est,
            prompt_fn=lambda q: prompts.append(q) or False,
            echo_fn=lambda m: echoes.append(m),
        )
        assert prompts == []  # une notice, jamais un blocage
        assert any("schéma" in e.lower() for e in echoes)


class TestRunGated:
    def test_refusal_never_runs_query(self):
        ran = []
        with patch.object(
            wg,
            "estimate",
            return_value=CostEstimate(
                dialect="bigquery", method="bq_dry_run", cost=1.0
            ),
        ):
            with pytest.raises(wg.WarehouseQueryDenied):
                wg.run_gated(
                    "SELECT 1",
                    "bigquery",
                    lambda: ran.append(1),
                    prompt_fn=lambda q: False,
                )
        assert ran == []  # AUCUNE requête facturée émise

    def test_approval_runs_and_returns(self):
        with patch.object(
            wg,
            "estimate",
            return_value=CostEstimate(
                dialect="bigquery", method="bq_dry_run", cost=1.0
            ),
        ):
            out = wg.run_gated(
                "SELECT 1", "bigquery", lambda: "rows", prompt_fn=lambda q: True
            )
        assert out == "rows"

    def test_local_runs_without_prompt(self):
        seen = []
        out = wg.run_gated(
            "SELECT 1",
            "duckdb",
            lambda: "rows",
            prompt_fn=lambda q: seen.append(q) or True,
        )
        assert out == "rows"
        assert seen == []


class TestGatedExecutor:
    def test_prompts_once_then_runs_all(self):
        seen, prompts = [], []
        inner = lambda sql: seen.append(sql) or [{"n": 1}]  # noqa: E731
        with patch.object(
            wg,
            "estimate",
            side_effect=lambda sql, dialect, **k: CostEstimate(
                dialect="bigquery", method="bq_dry_run", cost=0.5
            ),
        ):
            ex = wg.GatedExecutor(
                inner, "bigquery", prompt_fn=lambda q: prompts.append(q) or True
            )
            ex("Q1")
            ex("Q2")
            ex("Q3")
        assert seen == ["Q1", "Q2", "Q3"]
        assert len(prompts) == 1  # confirmé une seule fois pour tout le run

    def test_refusal_stops_before_any_query(self):
        seen = []
        inner = lambda sql: seen.append(sql) or []  # noqa: E731
        with patch.object(
            wg,
            "estimate",
            return_value=CostEstimate(
                dialect="bigquery", method="bq_dry_run", cost=1.0
            ),
        ):
            ex = wg.GatedExecutor(inner, "bigquery", prompt_fn=lambda q: False)
            with pytest.raises(wg.WarehouseQueryDenied):
                ex("Q1")
        assert seen == []
