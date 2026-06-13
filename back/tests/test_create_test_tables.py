"""
Tests pour create_test_tables + insert_examples avec des colonnes STRUCT/ARRAY.

Couvre :
- Reconstruction du type STRUCT depuis sous-champs (cache ancien, pas de bq_ddl_type)
- Reconstruction depuis bq_ddl_type (cache nouveau)
- REPEATED RECORD → ARRAY<STRUCT<...>>
- duckdb_typed_tables ne retourne que les colonnes racines
- INSERT sans "Duplicate column name"
- Chaîne complète : create → insert → SELECT
- Colonnes nommées avec des mots réservés DuckDB (begin, end)
"""

import os

os.environ.setdefault("DB_CONNECTION_TYPE", "duckdb")
os.environ.setdefault("DUCKDB_PATH", ":memory:")

import datetime

import duckdb
import pytest

from utils.examples import create_test_tables
from utils.insert_examples import (
    build_insert_statement,
    insert_examples,
    to_duck_expr,
)


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

    def test_duckdb_dialect_creates_table(self, con):
        """dialect=duckdb (modèles dbt/DuckDB-natifs) : le DDL interne est en
        syntaxe BigQuery (backticks + STRING/STRUCT<>) quel que soit le dialect
        SOURCE. Il doit toujours être parsé comme bigquery → DuckDB, sinon les
        backticks font échouer le parse ('Expecting )')."""
        table = {
            "table_name": "airbnb.main.fct_reviews",
            "description": "",
            "columns": [
                {"name": "LISTING_ID", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "REVIEW_TEXT", "type": "STRING", "mode": "NULLABLE"},
            ],
            "primary_keys": [],
        }
        result = create_test_tables(
            tables=[table], suffix="duck", con=con, dialect="duckdb"
        )
        assert len(result) == 1
        info = con.execute(f"DESCRIBE {result[0]['table_name']}").fetchall()
        col_types = {row[0]: row[1].upper() for row in info}
        assert col_types["LISTING_ID"].startswith(("INT", "BIGINT"))
        assert col_types["REVIEW_TEXT"] == "VARCHAR"


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


# ---------------------------------------------------------------------------
# Mots réservés DuckDB comme noms de colonnes (begin, end, …)
# ---------------------------------------------------------------------------

RESERVED_WORDS_TABLE = {
    "table_name": "bigquery-public-data.noaa_gsod.stations",
    "description": "",
    "columns": [
        {"name": "usaf", "type": "STRING", "mode": "NULLABLE"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "state", "type": "STRING", "mode": "NULLABLE"},
        {"name": "begin", "type": "STRING", "mode": "NULLABLE"},
        {"name": "end", "type": "STRING", "mode": "NULLABLE"},
    ],
    "primary_keys": [],
}

RESERVED_SUFFIX = "reserved_test"


class TestReservedWordColumns:
    @pytest.fixture
    def reserved_tables(self, con):
        return create_test_tables(
            tables=[RESERVED_WORDS_TABLE],
            suffix=RESERVED_SUFFIX,
            con=con,
            dialect="bigquery",
        )

    def test_create_table_with_reserved_words(self, reserved_tables):
        assert len(reserved_tables) == 1

    def test_insert_with_reserved_word_columns(self, con, reserved_tables):
        data = {
            "bigquery-public-data.noaa_gsod.stations": [
                {
                    "usaf": "727930",
                    "name": "SEATTLE TACOMA INTL AP",
                    "state": "WA",
                    "begin": "19490101",
                    "end": "20231231",
                }
            ]
        }
        stmts = list(insert_examples(data, reserved_tables, RESERVED_SUFFIX))
        assert len(stmts) == 1
        con.execute(stmts[0])

    def test_select_reserved_word_columns(self, con, reserved_tables):
        data = {
            "bigquery-public-data.noaa_gsod.stations": [
                {
                    "usaf": "727930",
                    "name": "SEATTLE TACOMA INTL AP",
                    "state": "WA",
                    "begin": "19490101",
                    "end": "20231231",
                }
            ]
        }
        stmts = list(insert_examples(data, reserved_tables, RESERVED_SUFFIX))
        con.execute(stmts[0])

        table_name = reserved_tables[0]["table_name"]
        row = con.execute(f'SELECT "begin", "end" FROM {table_name}').fetchone()
        assert row == ("19490101", "20231231")


# ---------------------------------------------------------------------------
# Valeurs scalaires déjà entourées de quotes (artefact LLM qui recopie un
# littéral SQL verbatim, ex. WHERE partition_date = '2025-04-01').
# Le contenu d'un type DATE/TIMESTAMP/numérique ne peut jamais contenir
# d'apostrophe → on la retire. Pour TEXT, le contenu est intouchable.
# ---------------------------------------------------------------------------

QUOTED_SCHEMA = {
    "table_name": "ds.t",
    "columns": [
        {"name": "compte", "type": "TEXT"},
        {"name": "banque", "type": "TEXT"},
        {"name": "partition_date", "type": "DATE"},
        {"name": "ts", "type": "TIMESTAMP"},
        {"name": "montant", "type": "BIGINT"},
    ],
}


class TestPrequotedScalarValues:
    def test_date_prequoted_not_double_quoted(self):
        expr = to_duck_expr("'2025-04-01'", "DATE")
        assert "'''" not in expr, expr

    def test_int_prequoted_stripped(self):
        assert to_duck_expr("'5'", "BIGINT") == "5"

    def test_text_apostrophe_content_preserved(self):
        # Une apostrophe dans une vraie valeur texte doit rester (échappée).
        assert to_duck_expr("O'Brien", "TEXT") == "'O''Brien'"

    def test_text_fully_quoted_value_preserved(self):
        # Pour TEXT, on NE strippe PAS une SEULE paire : "'hello'" est un contenu légitime.
        assert to_duck_expr("'hello'", "TEXT") == "'''hello'''"

    def test_text_double_nested_quote_artifact_stripped(self):
        # bq086 : artefact LLM non-ambigu — valeur emballée dans DEUX couches de quotes
        # imbriquées ('"FRA"'). On dé-quote jusqu'à la valeur nue → 'FRA'.
        assert to_duck_expr("'\"FRA\"'", "VARCHAR") == "'FRA'"

    def test_text_double_nested_inverse_quotes_stripped(self):
        # Couches inversées ("'FRA'") : même artefact, même traitement.
        assert to_duck_expr("\"'FRA'\"", "VARCHAR") == "'FRA'"

    def test_text_single_double_quote_layer_preserved(self):
        # Une SEULE couche de doubles quotes reste intacte (peut être une donnée
        # légitime — on ne strippe que l'imbrication non-ambiguë).
        assert to_duck_expr('"hello"', "VARCHAR") == "'\"hello\"'"

    def test_clean_date_still_works(self, con):
        con.execute('CREATE TABLE t1 ("partition_date" DATE);')
        stmt = build_insert_statement(
            "t1",
            [{"partition_date": "2025-04-01"}],
            {
                "table_name": "t1",
                "columns": [{"name": "partition_date", "type": "DATE"}],
            },
        )
        con.execute(stmt)
        assert con.execute("SELECT partition_date FROM t1").fetchone()[0] == (
            datetime.date(2025, 4, 1)
        )

    def test_insert_prequoted_date_executes(self, con):
        con.execute(
            'CREATE TABLE t2 ("compte" TEXT, "banque" TEXT, '
            '"partition_date" DATE, "ts" TIMESTAMP, "montant" BIGINT);'
        )
        stmt = build_insert_statement(
            "t2",
            [
                {
                    "compte": "10107",
                    "banque": "BP",
                    "partition_date": "'2025-04-01'",
                    "ts": "'2025-04-01 12:30:00'",
                    "montant": "'42'",
                }
            ],
            QUOTED_SCHEMA,
        )
        con.execute(stmt)
        row = con.execute("SELECT partition_date, ts, montant FROM t2").fetchone()
        assert row[0] == datetime.date(2025, 4, 1)
        assert row[1] == datetime.datetime(2025, 4, 1, 12, 30, 0)
        assert row[2] == 42


class TestBooleanValues:
    """La string "false" est truthy en Python : un `if value` naïf rend TRUE."""

    def test_bool_real_true_false(self):
        assert to_duck_expr(True, "BOOL") == "TRUE"
        assert to_duck_expr(False, "BOOL") == "FALSE"

    def test_string_false_is_false(self):
        assert to_duck_expr("false", "BOOL") == "FALSE"
        assert to_duck_expr("False", "BOOLEAN") == "FALSE"
        assert to_duck_expr("FALSE", "BOOL") == "FALSE"

    def test_string_true_is_true(self):
        assert to_duck_expr("true", "BOOL") == "TRUE"

    def test_prequoted_bool(self):
        assert to_duck_expr("'false'", "BOOL") == "FALSE"

    def test_numeric_bool(self):
        assert to_duck_expr(0, "BOOL") == "FALSE"
        assert to_duck_expr(1, "BOOL") == "TRUE"

    def test_string_zero_one(self):
        assert to_duck_expr("0", "BOOL") == "FALSE"
        assert to_duck_expr("1", "BOOL") == "TRUE"

    def test_insert_string_false_stored_as_false(self, con):
        con.execute('CREATE TABLE tb ("actif" BOOLEAN);')
        stmt = build_insert_statement(
            "tb",
            [{"actif": "false"}],
            {"table_name": "tb", "columns": [{"name": "actif", "type": "BOOLEAN"}]},
        )
        con.execute(stmt)
        assert con.execute("SELECT actif FROM tb").fetchone()[0] is False
