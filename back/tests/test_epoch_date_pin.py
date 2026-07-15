"""Fix 4 — pin déterministe date→epoch (sf_bq093).

Le LLM ne calcule pas fiablement un epoch : pour un filtre
``TO_DATE(TO_TIMESTAMP_NTZ(block_timestamp / 1000000)) = '2016-10-14'`` il a émis
``1476326400000000`` (= 2016-10-13 02:40 UTC) au lieu de ``1476403200000000`` → la CTE
d'entrée est vide, résultat dégénéré. On extrait la plage epoch valide HORS LLM (directive
``epoch_date_eq``) et on corrige toute valeur générée qui manque le jour filtré.
"""

import datetime

import sqlglot
from sqlglot import exp

from build_query.constraint_simplifier import (
    _epoch_date_range,
    _extract_epoch_date_eq,
    simplify_with_hint,
)
from build_query.examples_generator import _pin_epoch_date_values

# 2016-10-14 00:00:00 UTC
_MIDNIGHT_S = 1476403200
_MIDNIGHT_US = 1476403200000000
# valeur fautive réellement produite par le LLM sur sf_bq093 (2016-10-13)
_WRONG_US = 1476326400000000


def _first_eq_directive(sql: str, dialect: str):
    tree = sqlglot.parse_one(sql, read=dialect)
    for eq in tree.find_all(exp.EQ):
        r = _extract_epoch_date_eq(eq, dialect)
        if r is not None:
            return r
    return None


# ── extraction (pur) ─────────────────────────────────────────────────────────


def test_epoch_date_range_microseconds():
    d = datetime.date(2016, 10, 14)
    low, high, value = _epoch_date_range(d, 1_000_000)
    assert low == _MIDNIGHT_US
    assert high == (_MIDNIGHT_S + 86400) * 1_000_000
    assert value == _MIDNIGHT_US
    # la valeur fautive du LLM tombe hors de la plage
    assert not (low <= _WRONG_US < high)


def test_extract_snowflake_to_timestamp_divided():
    res = _first_eq_directive(
        "SELECT 1 FROM t WHERE TO_DATE(TO_TIMESTAMP_NTZ(t.block_timestamp / 1000000)) = '2016-10-14'",
        "snowflake",
    )
    assert res is not None
    col, d = res
    assert col.name == "block_timestamp"
    assert d["kind"] == "epoch_date_eq"
    assert d["low"] == _MIDNIGHT_US
    assert d["value"] == _MIDNIGHT_US


def test_extract_bigquery_timestamp_micros_and_seconds():
    micros = _first_eq_directive(
        "SELECT 1 FROM t WHERE DATE(TIMESTAMP_MICROS(t.ts)) = '2016-10-14'", "bigquery"
    )
    assert micros is not None and micros[1]["value"] == _MIDNIGHT_US
    seconds = _first_eq_directive(
        "SELECT 1 FROM t WHERE DATE(TIMESTAMP_SECONDS(t.ts)) = '2016-10-14'", "bigquery"
    )
    assert seconds is not None and seconds[1]["value"] == _MIDNIGHT_S


def test_explicit_date_format_is_not_epoch():
    # TO_DATE(col, 'YYYYMMDD') porte un format → directive date_format, PAS epoch_date_eq.
    assert (
        _first_eq_directive(
            "SELECT 1 FROM t WHERE TO_DATE(t.d, 'YYYYMMDD') = '2016-10-14'", "snowflake"
        )
        is None
    )


def test_plain_date_equality_is_not_epoch():
    # comparaison de dates natives, aucune conversion epoch → rien à pinner.
    assert (
        _first_eq_directive("SELECT 1 FROM t WHERE t.day = '2016-10-14'", "snowflake")
        is None
    )


# ── pipeline complet (résolution CTE → table de base) ────────────────────────


def test_simplify_with_hint_keys_directive_to_base_table():
    sql = """
    WITH FILTERED_TX AS (
      SELECT t.hash AS hash
      FROM crypto.ce.transactions AS t
      WHERE TO_DATE(TO_TIMESTAMP_NTZ(t.block_timestamp / 1000000)) = '2016-10-14'
    )
    SELECT f.hash FROM FILTERED_TX f
    """
    schema = [
        {
            "table_name": "transactions",
            "columns": [
                {"name": "hash", "type": "STRING"},
                {"name": "block_timestamp", "type": "INT64"},
            ],
        }
    ]
    _sim, hint = simplify_with_hint(sql, dialect="snowflake", schema=schema)
    directives = hint.get("column_directives") or {}
    assert "transactions.block_timestamp" in directives
    entry = directives["transactions.block_timestamp"][0]
    assert entry["kind"] == "epoch_date_eq"
    assert entry["value"] == _MIDNIGHT_US


# ── post-passe déterministe ──────────────────────────────────────────────────

_DIRECTIVES = {
    ("crypto_transactions", "block_timestamp"): [
        {
            "kind": "epoch_date_eq",
            "date": "2016-10-14",
            "low": _MIDNIGHT_US,
            "high": (_MIDNIGHT_S + 86400) * 1_000_000,
            "value": _MIDNIGHT_US,
        }
    ]
}


def test_pin_corrects_out_of_range_when_none_match():
    filled = {"crypto_transactions": [{"block_timestamp": _WRONG_US, "hash": "a"}]}
    _pin_epoch_date_values(filled, _DIRECTIVES)
    assert filled["crypto_transactions"][0]["block_timestamp"] == _MIDNIGHT_US
    assert filled["crypto_transactions"][0]["hash"] == "a"  # autres colonnes intactes


def test_pin_leaves_rows_untouched_when_one_matches():
    # une ligne dans la plage → la CTE n'est pas vide → on ne touche à rien (la ligne
    # hors plage peut être un cas d'exclusion voulu).
    inrange = _MIDNIGHT_US + 5
    filled = {
        "crypto_transactions": [
            {"block_timestamp": inrange},
            {"block_timestamp": _WRONG_US},
        ]
    }
    _pin_epoch_date_values(filled, _DIRECTIVES)
    assert filled["crypto_transactions"][0]["block_timestamp"] == inrange
    assert filled["crypto_transactions"][1]["block_timestamp"] == _WRONG_US


def test_pin_case_insensitive_column_key():
    filled = {"crypto_transactions": [{"BLOCK_TIMESTAMP": _WRONG_US}]}
    _pin_epoch_date_values(filled, _DIRECTIVES)
    assert filled["crypto_transactions"][0]["BLOCK_TIMESTAMP"] == _MIDNIGHT_US


def test_pin_ignores_non_integer_and_missing():
    filled = {
        "crypto_transactions": [{"block_timestamp": "not-a-number"}, {"other": 1}]
    }
    _pin_epoch_date_values(filled, _DIRECTIVES)
    # valeur non entière laissée telle quelle (la directive date_format gère les strings)
    assert filled["crypto_transactions"][0]["block_timestamp"] == "not-a-number"


# ── fallback colonne non résolue (directive keyée sur une CTE, sf_bq093 réel) ─

_EPOCH_PIN = {
    "kind": "epoch_date_eq",
    "date": "2016-10-14",
    "low": _MIDNIGHT_US,
    "high": (_MIDNIGHT_S + 86400) * 1_000_000,
    "value": _MIDNIGHT_US,
}


def test_unresolved_pin_applies_to_unique_column_table():
    # La directive a résolu vers une CTE (filtered_tx) → keyée par nom de colonne. Une
    # SEULE table de filled_data porte block_timestamp → pin appliqué.
    filled = {"crypto_transactions": [{"block_timestamp": _WRONG_US, "hash": "a"}]}
    _pin_epoch_date_values(filled, {}, [("block_timestamp", _EPOCH_PIN)])
    assert filled["crypto_transactions"][0]["block_timestamp"] == _MIDNIGHT_US


def test_unresolved_pin_skipped_when_ambiguous():
    # Deux tables portent block_timestamp → ambigu → on s'abstient (hint + garde CTE amont
    # vide prennent le relais).
    filled = {
        "tx_a": [{"block_timestamp": _WRONG_US}],
        "tx_b": [{"block_timestamp": _WRONG_US}],
    }
    _pin_epoch_date_values(filled, {}, [("block_timestamp", _EPOCH_PIN)])
    assert filled["tx_a"][0]["block_timestamp"] == _WRONG_US
    assert filled["tx_b"][0]["block_timestamp"] == _WRONG_US
