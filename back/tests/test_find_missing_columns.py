import json
from build_query.profile_checker import _find_missing_columns

PROFILE = {
    "tables": {
        "bigquery-public-data.world_bank_wdi.country_summary": {
            "columns": {
                "country_code": {},
                "income_group": {},
                "region": {},
                "short_name": {},
            }
        },
        "bigquery-public-data.world_bank_wdi.indicators_data": {
            "columns": {
                "country_code": {},
                "indicator_code": {},
                "value": {},
                "year": {},
            }
        },
    },
    "joins": [],
}

USED_COLUMNS = [
    json.dumps(
        {
            "project": "bigquery-public-data",
            "database": "world_bank_wdi",
            "table": "country_summary",
            "used_columns": ["country_code", "income_group", "region", "short_name"],
        }
    ),
    json.dumps(
        {
            "project": "bigquery-public-data",
            "database": "world_bank_wdi",
            "table": "indicators_data",
            "used_columns": ["country_code", "indicator_code", "value", "year"],
        }
    ),
]


def test_no_missing_columns_when_all_profiled():
    result = _find_missing_columns(PROFILE, USED_COLUMNS)
    assert result == [], f"Expected no missing columns, got: {result}"


def test_missing_column_detected():
    used = [
        json.dumps(
            {
                "project": "bigquery-public-data",
                "database": "world_bank_wdi",
                "table": "country_summary",
                "used_columns": ["country_code", "gdp"],  # gdp is not profiled
            }
        )
    ]
    result = _find_missing_columns(PROFILE, used)
    assert len(result) == 1
    assert result[0]["table"] == "bigquery-public-data.world_bank_wdi.country_summary"
    assert result[0]["used_columns"] == ["gdp"]


def test_unknown_table_all_columns_missing():
    used = [
        json.dumps(
            {
                "project": "bigquery-public-data",
                "database": "world_bank_wdi",
                "table": "some_other_table",
                "used_columns": ["col_a"],
            }
        )
    ]
    result = _find_missing_columns(PROFILE, used)
    assert len(result) == 1
    assert result[0]["used_columns"] == ["col_a"]
