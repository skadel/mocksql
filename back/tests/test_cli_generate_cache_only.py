"""Cache-only dialects must never fall through to BigQuery schema import."""

import pytest

from cli.generate import cache_miss_message


@pytest.mark.parametrize("dialect", ["duckdb", "postgres", "postgresql"])
def test_cache_only_dialects_explain_schema_cache_requirement(dialect: str) -> None:
    message = cache_miss_message(dialect)

    assert "cache-only" in message
    assert "schema_cache" in message
    assert "BQ_TEST_PROJECT" not in message
    assert "BigQuery" not in message


def test_unknown_dialect_does_not_claim_bigquery_import() -> None:
    assert "BigQuery" not in cache_miss_message("sqlite")
