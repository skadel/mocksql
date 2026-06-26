"""Tests for budget-aware profiling in build_profile_request.

When a scan budget (To) is supplied, profile queries whose estimated BigQuery
scan exceeds it are deferred (left out of profile_queries) and reported under
``deferred`` so the UI can show a partial profile + a "compléter" affordance.
``budget_tb=None`` keeps the historical behaviour (profile everything).
"""

from unittest.mock import patch

from build_query import profile_checker as pc
from storage.config import get_profile_budget_tb

SCHEMAS = [
    {"table_name": "proj.ds.small", "columns": [{"name": "id", "type": "INTEGER"}]},
    {"table_name": "proj.ds.big", "columns": [{"name": "id", "type": "INTEGER"}]},
]

MISSING = [
    {"table": "small", "project": "proj", "database": "ds", "used_columns": ["id"]},
    {"table": "big", "project": "proj", "database": "ds", "used_columns": ["id"]},
]

STATE = {
    "project": "proj",
    "dialect": "bigquery",
    "session": "s",
    "schemas": SCHEMAS,
    "query": "SELECT id FROM proj.ds.small",
}


def _resolve(missing, schemas):
    return [
        {"table": f"proj.ds.{m['table']}", "used_columns": m["used_columns"]}
        for m in missing
    ]


async def _fake_estimate(sql, _billing):
    # The "big" table query scans 5 TB, the "small" one a sliver.
    return 5.0 if "big" in sql else 0.01


async def _build(budget_tb):
    with (
        patch.object(pc, "_estimate_profile_bytes", new=_fake_estimate),
        patch("models.env_variables.BQ_TEST_PROJECT", "billing-proj"),
        patch.object(pc, "_resolve_full_table_names", side_effect=_resolve),
    ):
        return await pc.build_profile_request(
            STATE,
            MISSING,
            profile={"tables": {}, "joins": []},
            budget_tb=budget_tb,
        )


class TestProfileBudget:
    async def test_budget_defers_over_budget_table(self):
        r = await _build(budget_tb=0.3)
        assert len(r["profile_queries"]) == 1  # only the small table profiled
        assert r["deferred"] == [{"scope": "big", "billing_tb": 5.0}]
        assert r["profile_billing_tb"] == 0.01  # sum of within-budget only
        assert r["budget_tb"] == 0.3

    async def test_no_budget_profiles_everything(self):
        r = await _build(budget_tb=None)
        assert len(r["profile_queries"]) == 2
        assert r["deferred"] == []
        assert r["budget_tb"] is None

    async def test_generous_budget_profiles_everything(self):
        r = await _build(budget_tb=10.0)
        assert len(r["profile_queries"]) == 2
        assert r["deferred"] == []

    async def test_zero_budget_treated_as_no_budget(self):
        # Parité avec la config (<=0 => non configuré) : 0 ne doit PAS tout différer.
        r = await _build(budget_tb=0)
        assert len(r["profile_queries"]) == 2
        assert r["deferred"] == []
        assert r["budget_tb"] is None


class TestBudgetConfig:
    def test_unset_returns_none(self):
        with (
            patch("storage.config.load_config", return_value={}),
            patch.dict("os.environ", {}, clear=False),
        ):
            import os

            os.environ.pop("PROFILE_BUDGET_TB", None)
            assert get_profile_budget_tb() is None

    def test_config_value(self):
        with patch(
            "storage.config.load_config", return_value={"profile_budget_tb": 0.5}
        ):
            assert get_profile_budget_tb() == 0.5

    def test_zero_or_negative_treated_as_unset(self):
        with patch("storage.config.load_config", return_value={"profile_budget_tb": 0}):
            assert get_profile_budget_tb() is None

    def test_invalid_value_returns_none(self):
        with patch(
            "storage.config.load_config", return_value={"profile_budget_tb": "abc"}
        ):
            assert get_profile_budget_tb() is None
