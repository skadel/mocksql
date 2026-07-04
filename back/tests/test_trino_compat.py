"""Tests de compatibilité Trino : transpilation d'idiomes → DuckDB.

Le pipeline d'exécution réel est DuckDB ; chaque cas vérifie donc que le SQL Trino,
une fois transpilé via ``parse_test_query`` (read="trino" → render="duckdb" +
``_fix_trino_idioms``), **s'exécute** ET **donne le bon résultat**.

Couvre les deux profils de risque identifiés à la sonde de transpilation :
  - échec bruyant : ``format_datetime`` (Joda) laissé en Anonymous → Catalog Error ;
  - échec SILENCIEUX : ``reduce(...)`` dont sqlglot abandonne la 4ᵉ lambda (finition)
    → résultat faux sans erreur dès que la finition n'est pas l'identité.
"""

import asyncio

import duckdb
import pytest

from cli.main import DIALECTS
from utils.examples import (
    _fix_trino_idioms,
    _trino_joda_to_strftime,
    parse_test_query,
)


def _ptq(sql: str) -> str:
    return asyncio.run(parse_test_query(sql, "sfx", "trino"))


def _run(sql: str):
    return duckdb.sql(_ptq(sql)).fetchall()


# ---------------------------------------------------------------------------
# Enregistrement du dialecte
# ---------------------------------------------------------------------------


def test_trino_is_a_registered_dialect():
    assert "trino" in DIALECTS


# ---------------------------------------------------------------------------
# format_datetime (Joda-Time) → strftime
# ---------------------------------------------------------------------------


def test_format_datetime_becomes_strftime():
    out = _ptq(
        "SELECT format_datetime(TIMESTAMP '2024-01-15 09:07:05', 'yyyy-MM-dd HH:mm:ss') AS v"
    )
    assert "STRFTIME(" in out.upper()
    assert "%Y-%m-%d %H:%M:%S" in out
    assert "FORMAT_DATETIME" not in out.upper()


def test_format_datetime_executes_and_is_correct():
    res = _run(
        "SELECT format_datetime(TIMESTAMP '2024-01-15 09:07:05', 'yyyy-MM-dd') AS v"
    )
    assert res[0][0] == "2024-01-15"


def test_joda_case_sensitivity_month_vs_minute():
    # MM = mois, mm = minute — surtout pas de collision.
    assert _trino_joda_to_strftime("MM") == "%m"
    assert _trino_joda_to_strftime("mm") == "%M"
    assert _trino_joda_to_strftime("HH") == "%H"
    assert _trino_joda_to_strftime("hh") == "%I"


def test_joda_longest_token_wins():
    assert _trino_joda_to_strftime("yyyy-MMMM-dd") == "%Y-%B-%d"


def test_format_datetime_no_date_token_left_untouched():
    # Aucun token reconnu → on ne fabrique pas un format vide, l'expression survit.
    assert _trino_joda_to_strftime("literalonly") is None
    out = _ptq("SELECT format_datetime(TIMESTAMP '2024-01-15 09:07:05', 'zzz') AS v")
    assert "FORMAT_DATETIME" in out.upper()  # inchangé, pas de strftime bidon


# ---------------------------------------------------------------------------
# reduce(...) — la finition ne doit JAMAIS être perdue silencieusement
# ---------------------------------------------------------------------------


def test_reduce_identity_finish_is_sum():
    res = _run("SELECT reduce(ARRAY[1,2,3,4], 0, (s,x)->s+x, s->s) AS v")
    assert res[0][0] == 10


def test_reduce_non_identity_finish_is_preserved():
    # s -> s / cardinality(arr) : moyenne. Sans le fix, sqlglot rend la SOMME (=10)
    # et le résultat serait faux sans aucune erreur.
    res = _run(
        "SELECT reduce(ARRAY[1,2,3,4], 0, (s,x)->s+x, s->s/cardinality(ARRAY[1,2,3,4])) AS v"
    )
    assert res[0][0] == 2.5


def test_reduce_finish_inside_larger_expression():
    res = _run("SELECT 1 + reduce(ARRAY[10,20], 0, (s,x)->s+x, s->s*2) AS v")
    assert res[0][0] == 61  # 1 + (30 * 2)


def test_reduce_finish_removed_from_output():
    # Le rendu DuckDB ne doit plus contenir de 4ᵉ argument lambda dans list_reduce.
    out = _ptq("SELECT reduce(ARRAY[1,2,3], 0, (s,x)->s+x, s->s*10) AS v")
    assert out.upper().count("LAMBDA") == 1  # merge seulement, plus la finition


# ---------------------------------------------------------------------------
# Batterie d'idiomes ETL Trino — verrou de non-régression bout-en-bout
# ---------------------------------------------------------------------------

_BATTERY = [
    ("SELECT TRY_CAST('12' AS INTEGER) AS v", 12),
    ("SELECT date_diff('day', DATE '2024-01-01', DATE '2024-01-10') AS v", 9),
    ("SELECT element_at(ARRAY[10,20,30], 2) AS v", 20),
    ("SELECT cardinality(ARRAY[1,2,3]) AS v", 3),
    ("SELECT contains(ARRAY[1,2,3], 2) AS v", True),
    ("SELECT regexp_extract('abc123', '([0-9]+)', 1) AS v", "123"),
    ("SELECT split('a,b,c', ',') AS v", ["a", "b", "c"]),
    ("SELECT json_extract_scalar('{\"k\": 5}', '$.k') AS v", "5"),
    ("SELECT element_at(MAP(ARRAY['a','b'], ARRAY[1,2]), 'a') AS v", 1),
    ("SELECT transform(ARRAY[1,2,3], x -> x * 2) AS v", [2, 4, 6]),
    ("SELECT filter(ARRAY[1,2,3,4], x -> x % 2 = 0) AS v", [2, 4]),
]


@pytest.mark.parametrize("sql,expected", _BATTERY)
def test_trino_idiom_battery_executes(sql, expected):
    res = _run(sql)
    assert res[0][0] == expected


def test_unnest_with_ordinality_executes():
    res = _run(
        "SELECT x, i FROM UNNEST(ARRAY[10,20,30]) WITH ORDINALITY AS t(x, i) ORDER BY i"
    )
    assert res == [(10, 1), (20, 2), (30, 3)]


def test_fix_trino_idioms_is_idempotent():
    import sqlglot

    tree = sqlglot.parse_one(
        "SELECT format_datetime(ts, 'yyyy-MM') AS v FROM t", dialect="trino"
    )
    once = _fix_trino_idioms(tree).sql(dialect="duckdb")
    twice = _fix_trino_idioms(sqlglot.parse_one(once, dialect="duckdb")).sql(
        dialect="duckdb"
    )
    assert once == twice
