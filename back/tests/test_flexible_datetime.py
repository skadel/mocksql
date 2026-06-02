"""
Tests for FlexibleDatetime — normalisation des formats datetime générés par le LLM.

Le LLM produit parfois "2026-01-01 00:00:00+00" (espace au lieu de T, offset +00 sans
les minutes) ce qui échoue la validation Pydantic strict ISO 8601. FlexibleDatetime
corrige ces deux écarts avant validation.
"""

import datetime
from typing import Optional

import pytest
from pydantic import BaseModel, ValidationError

from common_vars import FlexibleDatetime, _normalize_datetime_str


# ---------------------------------------------------------------------------
# _normalize_datetime_str — unitaire
# ---------------------------------------------------------------------------


class TestNormalizeDatetimeStr:
    def test_space_separator_replaced_by_T(self):
        assert _normalize_datetime_str("2026-01-01 00:00:00") == "2026-01-01T00:00:00"

    def test_short_offset_padded(self):
        assert (
            _normalize_datetime_str("2026-01-01T00:00:00+00")
            == "2026-01-01T00:00:00+00:00"
        )

    def test_both_corrections_applied(self):
        # Format exact produit par le LLM qui causait l'erreur
        assert (
            _normalize_datetime_str("2026-01-01 00:00:00+00")
            == "2026-01-01T00:00:00+00:00"
        )

    def test_already_valid_z_suffix_unchanged(self):
        assert _normalize_datetime_str("2026-01-01T00:00:00Z") == "2026-01-01T00:00:00Z"

    def test_already_valid_full_offset_unchanged(self):
        assert (
            _normalize_datetime_str("2026-01-01T12:30:00+02:00")
            == "2026-01-01T12:30:00+02:00"
        )

    def test_non_string_passthrough(self):
        dt = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
        assert _normalize_datetime_str(dt) is dt

    def test_none_passthrough(self):
        assert _normalize_datetime_str(None) is None

    def test_offset_only_on_trailing_two_digit_offset(self):
        # +01 trailing → padded ; +01:00 already full → unchanged
        assert (
            _normalize_datetime_str("2026-06-01T10:00:00+01")
            == "2026-06-01T10:00:00+01:00"
        )
        assert (
            _normalize_datetime_str("2026-06-01T10:00:00+01:00")
            == "2026-06-01T10:00:00+01:00"
        )


# ---------------------------------------------------------------------------
# FlexibleDatetime — validation Pydantic
# ---------------------------------------------------------------------------


class _Model(BaseModel):
    ts: Optional[FlexibleDatetime] = None


class TestFlexibleDatetime:
    def test_rejects_llm_format_without_fix_would_fail(self):
        # Vérifie que le format LLM est maintenant accepté après normalisation
        inst = _Model(ts="2026-01-01 00:00:00+00")
        assert inst.ts == datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)

    def test_valid_z_suffix(self):
        inst = _Model(ts="2026-01-01T00:00:00Z")
        assert inst.ts.year == 2026

    def test_valid_full_offset(self):
        inst = _Model(ts="2026-01-01T00:00:00+00:00")
        assert inst.ts.tzinfo is not None

    def test_none_accepted(self):
        inst = _Model(ts=None)
        assert inst.ts is None

    def test_datetime_object_accepted(self):
        dt = datetime.datetime(2026, 6, 1, 12, 0, tzinfo=datetime.timezone.utc)
        inst = _Model(ts=dt)
        assert inst.ts == dt

    def test_space_separator_only(self):
        inst = _Model(ts="2026-03-15 08:30:00Z")
        assert inst.ts.month == 3

    def test_invalid_string_raises(self):
        with pytest.raises(ValidationError):
            _Model(ts="not-a-date")


# ---------------------------------------------------------------------------
# Intégration — create_pydantic_models avec TIMESTAMP
# ---------------------------------------------------------------------------


def test_timestamp_column_accepts_llm_format():
    """TIMESTAMP via create_pydantic_models doit accepter le format LLM."""
    from utils.examples import create_pydantic_models

    model = create_pydantic_models(
        [{"table_name": "T", "columns": [{"name": "ts", "type": "TIMESTAMP"}]}]
    )
    inst = model(T=[{"ts": "2026-01-01 00:00:00+00"}])
    assert inst.T[0].ts.year == 2026


def test_timestamp_bq_ddl_accepts_llm_format():
    """TIMESTAMP via bq_ddl_type doit aussi accepter le format LLM."""
    from utils.examples import create_pydantic_models

    model = create_pydantic_models(
        [{"table_name": "T", "columns": [{"name": "ts", "bq_ddl_type": "TIMESTAMP"}]}]
    )
    inst = model(T=[{"ts": "2026-01-01 00:00:00+00"}])
    assert inst.T[0].ts is not None


def test_timestamp_column_accepts_standard_iso():
    from utils.examples import create_pydantic_models

    model = create_pydantic_models(
        [{"table_name": "T", "columns": [{"name": "ts", "type": "TIMESTAMP"}]}]
    )
    inst = model(T=[{"ts": "2026-06-01T10:00:00Z"}])
    assert inst.T[0].ts.month == 6
