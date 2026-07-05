"""Tests des branches SQL Trino du profiler (Phase 4).

Le profiling s'exécute sur l'entrepôt source (Trino ici). Les builders SQL
doivent émettre des fonctions valides Trino là où le `else` générique produirait
du BigQuery/ANSI que Trino refuse :
  - agrégation de chaînes : STRING_AGG/LISTAGG → ARRAY_JOIN(ARRAY_AGG(...)) ;
  - régularité temporelle : EXTRACT(EPOCH)/soustraction → date_diff('unit', ...) ;
  - comptage conditionnel : COUNTIF → COUNT_IF (via exp.CountIf natif).

Fonctions pures — pas de connexion Trino requise.
"""

from sqlglot import exp

from build_query.profiler import (
    _build_regularity_query,
    _string_agg_expr,
)


# ---------------------------------------------------------------------------
# _string_agg_expr — agrégation de chaînes
# ---------------------------------------------------------------------------


def test_string_agg_trino_uses_array_join():
    sql = _string_agg_expr(exp.column("v"), "|||", "trino").sql(dialect="trino")
    assert "ARRAY_JOIN" in sql.upper()
    assert "ARRAY_AGG" in sql.upper()
    assert "STRING_AGG" not in sql.upper()
    assert "LISTAGG" not in sql.upper()


def test_string_agg_other_dialects_keep_string_agg():
    for d in ("bigquery", "duckdb", "postgres"):
        sql = _string_agg_expr(exp.column("v"), "|||", d).sql(dialect=d)
        assert "STRING_AGG" in sql.upper()


# ---------------------------------------------------------------------------
# _build_regularity_query — diffs temporels
# ---------------------------------------------------------------------------


def test_regularity_date_trino_uses_date_diff():
    sql = _build_regularity_query("events", "event_date", "DATE", "trino")
    assert sql is not None
    up = sql.upper()
    assert "DATE_DIFF('DAY'" in up
    assert "EXTRACT(EPOCH" not in up  # le fallback ANSI cassait Trino


def test_regularity_timestamp_trino_uses_date_diff_second():
    sql = _build_regularity_query("events", "ts", "TIMESTAMP", "trino")
    assert sql is not None
    up = sql.upper()
    assert "DATE_DIFF('SECOND'" in up
    assert "EXTRACT(EPOCH" not in up


def test_regularity_non_temporal_is_none():
    assert _build_regularity_query("t", "name", "STRING", "trino") is None


# ---------------------------------------------------------------------------
# COUNTIF → COUNT_IF : les deux sites du profiler passent par exp.CountIf natif,
# qui rend la bonne fonction par dialecte (COUNTIF en BQ, COUNT_IF en Trino).
# ---------------------------------------------------------------------------


def test_count_if_renders_per_dialect():
    node = exp.CountIf(this=exp.column("x").is_(exp.Null()))
    assert node.sql(dialect="bigquery").upper().startswith("COUNTIF")
    assert node.sql(dialect="trino").upper().startswith("COUNT_IF")
