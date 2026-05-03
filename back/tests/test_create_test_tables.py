"""
Tests pour create_test_tables + insert_examples avec des colonnes STRUCT/ARRAY.

Couvre :
- Reconstruction du type STRUCT depuis sous-champs (cache ancien, pas de bq_ddl_type)
- Reconstruction depuis bq_ddl_type (cache nouveau)
- REPEATED RECORD → ARRAY<STRUCT<...>>
- duckdb_typed_tables ne retourne que les colonnes racines
- INSERT sans "Duplicate column name"
- Chaîne complète : create → insert → SELECT
"""

import os

os.environ.setdefault("DB_CONNECTION_TYPE", "duckdb")
os.environ.setdefault("DUCKDB_PATH", ":memory:")

import duckdb
import pytest

from utils.examples import create_test_tables
from utils.insert_examples import insert_examples


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

GA_SESSIONS_COLUMNS = [
    {"name": "fullVisitorId", "type": "STRING", "mode": "NULLABLE"},
    {"name": "visitId", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "trafficSource", "type": "RECORD", "mode": "NULLABLE"},
    {"name": "trafficSource.campaign", "type": "STRING", "mode": "NULLABLE"},
    {"name": "trafficSource.keyword", "type": "STRING", "mode": "NULLABLE"},
    {"name": "hits", "type": "RECORD", "mode": "REPEATED"},
    {"name": "hits.type", "type": "STRING", "mode": "NULLABLE"},
    {"name": "hits.hitNumber", "type": "INTEGER", "mode": "NULLABLE"},
]

GA_SESSIONS_TABLE = {
    "table_name": "google_analytics_sample.ga_sessions",
    "description": "",
    "columns": GA_SESSIONS_COLUMNS,
    "primary_keys": [],
}

SUFFIX = "test_session_1"


@pytest.fixture
def con():
    return duckdb.connect()


@pytest.fixture
def duckdb_tables(con):
    return create_test_tables(
        tables=[GA_SESSIONS_TABLE],
        suffix=SUFFIX,
        con=con,
        dialect="bigquery",
        used_columns=[
            {
                "table": "google_analytics_sample.ga_sessions",
                "used_columns": ["fullVisitorId", "visitId", "trafficSource", "hits"],
            }
        ],
    )


# ---------------------------------------------------------------------------
# create_test_tables — structure DDL
# ---------------------------------------------------------------------------


class TestCreateTestTables:
    def test_table_created_without_error(self, duckdb_tables):
        assert len(duckdb_tables) == 1

    def test_returned_columns_are_root_only(self, duckdb_tables):
        col_names = [c["name"] for c in duckdb_tables[0]["columns"]]
        assert "trafficSource" in col_names
        assert "hits" in col_names
        # Les sous-champs ne doivent PAS être dans duckdb_typed_tables
        assert "trafficSource.campaign" not in col_names
        assert "hits.type" not in col_names

    def test_struct_column_type_in_duckdb(self, con, duckdb_tables):
        table_name = duckdb_tables[0]["table_name"]
        info = con.execute(f"DESCRIBE {table_name}").fetchall()
        col_types = {row[0]: row[1] for row in info}
        # DuckDB représente STRUCT<campaign STRING, keyword STRING> comme STRUCT(...)
        assert "STRUCT" in col_types["trafficSource"].upper()

    def test_repeated_record_is_array_in_duckdb(self, con, duckdb_tables):
        table_name = duckdb_tables[0]["table_name"]
        info = con.execute(f"DESCRIBE {table_name}").fetchall()
        col_types = {row[0]: row[1] for row in info}
        hits_type = col_types["hits"].upper()
        assert "STRUCT" in hits_type
        assert "[]" in hits_type or "LIST" in hits_type or "ARRAY" in hits_type

    def test_old_cache_without_bq_ddl_type(self, con):
        """Cache ancien : pas de bq_ddl_type, reconstruction depuis sous-champs."""
        columns_no_bq_type = [
            col
            for col in GA_SESSIONS_COLUMNS
            if "bq_ddl_type" not in col  # aucun ne l'a, c'est le cas cache ancien
        ]
        table = {**GA_SESSIONS_TABLE, "columns": columns_no_bq_type}
        result = create_test_tables(
            tables=[table],
            suffix="old_cache",
            con=con,
            dialect="bigquery",
        )
        assert len(result) == 1
        col_names = [c["name"] for c in result[0]["columns"]]
        assert "trafficSource" in col_names
        assert "trafficSource.campaign" not in col_names

    def test_bq_ddl_type_takes_precedence_over_subfields(self, con):
        """Cache nouveau : bq_ddl_type utilisé en priorité, sous-champs ignorés."""
        columns_with_bq_type = [
            {
                "name": "trafficSource",
                "type": "RECORD",
                "mode": "NULLABLE",
                "bq_ddl_type": "STRUCT<campaign STRING, keyword STRING>",
            }
        ]
        table = {
            "table_name": "ds.t",
            "description": "",
            "columns": columns_with_bq_type,
            "primary_keys": [],
        }
        result = create_test_tables(
            tables=[table], suffix="new_cache", con=con, dialect="bigquery"
        )
        assert len(result) == 1
        info = con.execute(f"DESCRIBE {result[0]['table_name']}").fetchall()
        col_types = {row[0]: row[1] for row in info}
        assert "STRUCT" in col_types["trafficSource"].upper()


# ---------------------------------------------------------------------------
# insert_examples — pas de "Duplicate column name"
# ---------------------------------------------------------------------------


class TestInsertExamples:
    def test_insert_struct_no_duplicate_column_error(self, con, duckdb_tables):
        data = {
            "google_analytics_sample.ga_sessions": [
                {
                    "fullVisitorId": "abc123",
                    "visitId": 1001,
                    "trafficSource": {"campaign": "Data Share", "keyword": None},
                    "hits": [{"type": "PAGE", "hitNumber": 1}],
                }
            ]
        }
        stmts = list(insert_examples(data, duckdb_tables, SUFFIX))
        assert len(stmts) == 1
        # Ne doit pas lever "Duplicate column name"
        con.execute(stmts[0])

    def test_insert_null_struct(self, con, duckdb_tables):
        data = {
            "google_analytics_sample.ga_sessions": [
                {
                    "fullVisitorId": "xyz",
                    "visitId": 2,
                    "trafficSource": None,
                    "hits": [],
                }
            ]
        }
        stmts = list(insert_examples(data, duckdb_tables, SUFFIX))
        con.execute(stmts[0])  # pas de crash


# ---------------------------------------------------------------------------
# Intégration complète : create → insert → SELECT
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_select_after_insert(self, con, duckdb_tables):
        data = {
            "google_analytics_sample.ga_sessions": [
                {
                    "fullVisitorId": "visitor_1",
                    "visitId": 42,
                    "trafficSource": {"campaign": "organic", "keyword": "sql"},
                    "hits": [
                        {"type": "PAGE", "hitNumber": 1},
                        {"type": "EVENT", "hitNumber": 2},
                    ],
                }
            ]
        }
        stmts = list(insert_examples(data, duckdb_tables, SUFFIX))
        con.execute(stmts[0])

        table_name = duckdb_tables[0]["table_name"]
        rows = con.execute(
            f"SELECT fullVisitorId, visitId FROM {table_name}"
        ).fetchall()
        assert rows == [("visitor_1", 42)]

    def test_struct_field_accessible_via_dot_notation(self, con, duckdb_tables):
        data = {
            "google_analytics_sample.ga_sessions": [
                {
                    "fullVisitorId": "v2",
                    "visitId": 99,
                    "trafficSource": {"campaign": "Data Share", "keyword": None},
                    "hits": [{"type": "PAGE", "hitNumber": 1}],
                }
            ]
        }
        stmts = list(insert_examples(data, duckdb_tables, SUFFIX))
        con.execute(stmts[0])

        table_name = duckdb_tables[0]["table_name"]
        result = con.execute(
            f"SELECT trafficSource.campaign FROM {table_name}"
        ).fetchone()
        assert result[0] == "Data Share"

    def test_multiple_rows(self, con, duckdb_tables):
        data = {
            "google_analytics_sample.ga_sessions": [
                {
                    "fullVisitorId": f"v{i}",
                    "visitId": i,
                    "trafficSource": {"campaign": "c", "keyword": None},
                    "hits": [],
                }
                for i in range(5)
            ]
        }
        stmts = list(insert_examples(data, duckdb_tables, SUFFIX))
        con.execute(stmts[0])

        table_name = duckdb_tables[0]["table_name"]
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        assert count == 5
