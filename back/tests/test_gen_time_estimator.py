"""Tests de l'estimateur grosse-maille de durée de génération."""

from build_query.gen_time_estimator import (
    _DEFAULT_MINUTES,
    count_constraints,
    estimate_minutes,
    extract_features,
    log_timing,
)

_SQL = (
    "SELECT a.id, b.name FROM proj.ds.users a "
    "JOIN proj.ds.orders b ON a.id = b.uid "
    "WHERE a.age > 18 AND b.total <= 100 AND a.status IN (1, 2)"
)
_USED = [
    '{"project": "proj", "database": "ds", "table": "users", "used_columns": ["id", "age"]}',
    '{"project": "proj", "database": "ds", "table": "orders", "used_columns": ["uid", "total", "name"]}',
]


def test_count_constraints_counts_predicates():
    # a.id=b.uid (ON), age>18, total<=100, status IN (...) → 4 prédicats
    assert count_constraints(_SQL) == 4


def test_count_constraints_unparsable_returns_zero():
    assert count_constraints("this is not sql ((") == 0


def test_extract_features():
    f = extract_features(_SQL, _USED, "bigquery")
    assert f["n_tables"] == 2
    assert f["n_used_cols"] == 5
    assert f["n_constraints"] == 4
    assert f["sql_len"] == len(_SQL)


def test_extract_features_without_used_columns():
    f = extract_features(_SQL, None, "bigquery")
    assert f["n_used_cols"] == 0


def test_estimate_empty_dataset_returns_default(tmp_path):
    p = tmp_path / "timings.jsonl"
    assert estimate_minutes(extract_features(_SQL, _USED), p) == _DEFAULT_MINUTES


def test_estimate_averages_same_bucket(tmp_path):
    p = tmp_path / "timings.jsonl"
    f = extract_features(_SQL, _USED)
    log_timing(f, 350.0, p)  # moyenne 380s → round(6.33) = 6 min
    log_timing(f, 410.0, p)
    assert estimate_minutes(f, p) == 6


def test_estimate_falls_back_to_global_when_bucket_empty(tmp_path):
    p = tmp_path / "timings.jsonl"
    # Échantillon d'un autre bucket (1 table, petit SQL)
    log_timing(extract_features("SELECT x FROM proj.ds.t WHERE x > 1"), 120.0, p)
    # Cible : 2 tables, gros SQL → bucket différent → repli moyenne globale (120s → 2 min)
    assert estimate_minutes(extract_features(_SQL, _USED), p) == 2


def test_estimate_floor_is_one_minute(tmp_path):
    p = tmp_path / "timings.jsonl"
    f = extract_features(_SQL, _USED)
    log_timing(f, 5.0, p)
    assert estimate_minutes(f, p) == 1
