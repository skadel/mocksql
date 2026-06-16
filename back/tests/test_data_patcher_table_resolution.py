"""Régression : `add_test_row` ne trouvait pas la table quand le LLM la référence par
son nom BigQuery COMPLET (bigquery-public-data.chicago_taxi_trips.taxi_trips) alors
que `filter_columns` produit toujours un nom APLATI (chicago_taxi_trips_taxi_trips).

Conséquence : `scoped_schema` vide → aucune ligne ajoutée (no-op silencieux). Bug
général (UI comprise), pas seulement CLI offline. Le fix normalise les noms demandés.
"""

from build_query.data_patcher import _flatten_table_name, _scope_schema_for_tables


def _fs():
    return [
        {"table_name": "chicago_taxi_trips_taxi_trips", "columns": []},
        {"table_name": "other_db_orders", "columns": []},
    ]


def test_flatten_full_bq_name():
    assert (
        _flatten_table_name("bigquery-public-data.chicago_taxi_trips.taxi_trips")
        == "chicago_taxi_trips_taxi_trips"
    )


def test_flatten_two_part():
    assert (
        _flatten_table_name("chicago_taxi_trips.taxi_trips")
        == "chicago_taxi_trips_taxi_trips"
    )


def test_flatten_already_flat_unchanged():
    assert (
        _flatten_table_name("chicago_taxi_trips_taxi_trips")
        == "chicago_taxi_trips_taxi_trips"
    )


def test_scope_matches_full_bq_name():
    scoped = _scope_schema_for_tables(
        _fs(), ["bigquery-public-data.chicago_taxi_trips.taxi_trips"]
    )
    assert [t["table_name"] for t in scoped] == ["chicago_taxi_trips_taxi_trips"]


def test_scope_matches_two_part_name():
    scoped = _scope_schema_for_tables(_fs(), ["chicago_taxi_trips.taxi_trips"])
    assert [t["table_name"] for t in scoped] == ["chicago_taxi_trips_taxi_trips"]


def test_scope_matches_already_flat():
    scoped = _scope_schema_for_tables(_fs(), ["chicago_taxi_trips_taxi_trips"])
    assert len(scoped) == 1


def test_scope_no_match_returns_empty():
    assert _scope_schema_for_tables(_fs(), ["nonexistent.table"]) == []


def test_scope_multiple_tables():
    scoped = _scope_schema_for_tables(
        _fs(),
        ["bigquery-public-data.chicago_taxi_trips.taxi_trips", "x.other_db.orders"],
    )
    assert {t["table_name"] for t in scoped} == {
        "chicago_taxi_trips_taxi_trips",
        "other_db_orders",
    }
