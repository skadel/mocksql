"""
Tests unitaires pour la pipeline de schéma BigQuery :
_schema_fields_to_dicts → _flatten_bq_schema → generate_tables_and_columns → build_schema

Pas de BQ, pas de réseau. Les SchemaField sont mockés via un simple dataclass.
"""

import os

os.environ.setdefault("DB_CONNECTION_TYPE", "duckdb")
os.environ.setdefault("DUCKDB_PATH", ":memory:")

from dataclasses import dataclass, field
from typing import List

import pytest
from google.cloud import bigquery

from build_query.schema_fetcher import _flatten_bq_schema, _schema_fields_to_dicts
from utils.schema_utils import generate_tables_and_columns_from_project_schema
from utils.bigquery_test_helper import BigQueryTestHelper


# ─── Mock SchemaField ─────────────────────────────────────────────────────────


@dataclass
class FakeField:
    name: str
    field_type: str
    mode: str = "NULLABLE"
    description: str = ""
    fields: List["FakeField"] = field(default_factory=list)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_flat_row(field_path, data_type, mode="NULLABLE", catalog="p", schema="ds", table="t"):
    return {
        "table_catalog": catalog,
        "table_schema": schema,
        "table_name": table,
        "field_path": field_path,
        "data_type": data_type,
        "mode": mode,
        "description": "",
    }


def _build_via_helper(columns):
    helper = BigQueryTestHelper.__new__(BigQueryTestHelper)
    return helper.build_schema(columns)


# ─── _schema_fields_to_dicts ──────────────────────────────────────────────────


def test_fields_to_dicts_preserves_mode_nullable():
    fields = [FakeField(name="user_id", field_type="STRING", mode="NULLABLE")]
    result = _schema_fields_to_dicts(fields)
    assert result == [{"name": "user_id", "type": "STRING", "mode": "NULLABLE", "description": ""}]


def test_fields_to_dicts_preserves_mode_repeated():
    fields = [FakeField(name="hits", field_type="RECORD", mode="REPEATED")]
    result = _schema_fields_to_dicts(fields)
    assert result[0]["mode"] == "REPEATED"


def test_fields_to_dicts_nested_record():
    hits = FakeField(
        name="hits",
        field_type="RECORD",
        mode="REPEATED",
        fields=[FakeField(name="page", field_type="STRING", mode="NULLABLE")],
    )
    result = _schema_fields_to_dicts([hits])
    assert result[0]["mode"] == "REPEATED"
    assert result[0]["fields"][0]["name"] == "page"
    assert result[0]["fields"][0]["mode"] == "NULLABLE"


# ─── _flatten_bq_schema ───────────────────────────────────────────────────────


def test_flatten_simple_field():
    dicts = [{"name": "user_id", "type": "STRING", "mode": "NULLABLE", "description": ""}]
    rows = _flatten_bq_schema(dicts)
    assert rows == [
        {"field_path": "user_id", "data_type": "STRING", "mode": "NULLABLE", "description": ""}
    ]


def test_flatten_repeated_record_preserves_mode():
    dicts = [
        {
            "name": "hits",
            "type": "RECORD",
            "mode": "REPEATED",
            "description": "",
            "fields": [{"name": "page", "type": "STRING", "mode": "NULLABLE", "description": ""}],
        }
    ]
    rows = _flatten_bq_schema(dicts)
    hits_row = next(r for r in rows if r["field_path"] == "hits")
    page_row = next(r for r in rows if r["field_path"] == "hits.page")
    assert hits_row["mode"] == "REPEATED"
    assert page_row["mode"] == "NULLABLE"


def test_flatten_missing_mode_defaults_nullable():
    dicts = [{"name": "x", "type": "STRING", "description": ""}]
    rows = _flatten_bq_schema(dicts)
    assert rows[0]["mode"] == "NULLABLE"


# ─── generate_tables_and_columns_from_project_schema ──────────────────────────


def test_generate_preserves_mode():
    project_schema = {
        "data": [
            _make_flat_row("hits", "RECORD", mode="REPEATED"),
            _make_flat_row("hits.page", "STRING", mode="NULLABLE"),
        ]
    }
    tables = generate_tables_and_columns_from_project_schema(project_schema)
    assert len(tables) == 1
    cols_by_name = {c["name"]: c for c in tables[0]["columns"]}
    assert cols_by_name["hits"]["mode"] == "REPEATED"
    assert cols_by_name["hits.page"]["mode"] == "NULLABLE"


def test_generate_missing_mode_defaults_nullable():
    project_schema = {
        "data": [
            {
                "table_catalog": "p",
                "table_schema": "ds",
                "table_name": "t",
                "field_path": "user_id",
                "data_type": "STRING",
                "description": "",
            }
        ]
    }
    tables = generate_tables_and_columns_from_project_schema(project_schema)
    col = tables[0]["columns"][0]
    assert col["mode"] == "NULLABLE"


# ─── convert_type ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "type_str, expected",
    [
        ("STRING", "STRING"),
        ("INT64", "INTEGER"),
        ("FLOAT64", "FLOAT"),
        ("BOOL", "BOOLEAN"),
        ("TIMESTAMP", "TIMESTAMP"),
        ("DATE", "DATE"),
        ("RECORD", "RECORD"),
        ("STRUCT", "RECORD"),
        ("ARRAY<STRING>", "STRING"),
        ("ARRAY<STRUCT<x STRING>>", "RECORD"),
        ("UNKNOWN_TYPE", "STRING"),
    ],
)
def test_convert_type(type_str, expected):
    helper = BigQueryTestHelper.__new__(BigQueryTestHelper)
    assert helper.convert_type(type_str) == expected


# ─── build_schema ────────────────────────────────────────────────────────────


def test_build_schema_flat():
    columns = [
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "count", "type": "INT64", "mode": "NULLABLE"},
    ]
    schema = _build_via_helper(columns)
    assert len(schema) == 2
    assert schema[0].name == "user_id"
    assert schema[0].field_type == "STRING"
    assert schema[0].mode == "NULLABLE"


def test_build_schema_repeated_record():
    columns = [
        {"name": "hits", "type": "RECORD", "mode": "REPEATED"},
        {"name": "hits.page", "type": "STRING", "mode": "NULLABLE"},
        {"name": "hits.type", "type": "STRING", "mode": "NULLABLE"},
    ]
    schema = _build_via_helper(columns)
    assert len(schema) == 1
    hits = schema[0]
    assert hits.name == "hits"
    assert hits.field_type == "RECORD"
    assert hits.mode == "REPEATED"
    assert len(hits.fields) == 2
    field_names = {f.name for f in hits.fields}
    assert field_names == {"page", "type"}


def test_build_schema_nested_repeated():
    columns = [
        {"name": "sessions", "type": "RECORD", "mode": "REPEATED"},
        {"name": "sessions.hits", "type": "RECORD", "mode": "REPEATED"},
        {"name": "sessions.hits.page", "type": "STRING", "mode": "NULLABLE"},
    ]
    schema = _build_via_helper(columns)
    sessions = schema[0]
    assert sessions.mode == "REPEATED"
    hits = sessions.fields[0]
    assert hits.name == "hits"
    assert hits.mode == "REPEATED"
    assert hits.fields[0].name == "page"


# ─── Chaîne complète : FakeField → build_schema ───────────────────────────────


def test_full_chain_ga_hits():
    """
    Simule un schéma Google Analytics : hits est REPEATED RECORD avec un sous-champ page.
    Vérifie que mode=REPEATED survit à toute la pipeline.
    """
    bq_schema = [
        FakeField(
            name="visitId",
            field_type="INTEGER",
            mode="NULLABLE",
        ),
        FakeField(
            name="hits",
            field_type="RECORD",
            mode="REPEATED",
            fields=[
                FakeField(name="hitNumber", field_type="INTEGER", mode="NULLABLE"),
                FakeField(name="page", field_type="STRING", mode="NULLABLE"),
            ],
        ),
    ]

    # Étape 1 : SchemaField → dicts imbriqués
    dicts = _schema_fields_to_dicts(bq_schema)

    # Étape 2 : dicts → lignes aplaties (dot-notation)
    flat_rows = _flatten_bq_schema(dicts)

    # Étape 3 : lignes → project_schema → tables_and_columns
    project_schema = {
        "data": [
            {
                "table_catalog": "my_project",
                "table_schema": "ga",
                "table_name": "sessions",
                **row,
            }
            for row in flat_rows
        ]
    }
    tables = generate_tables_and_columns_from_project_schema(project_schema)
    columns = tables[0]["columns"]

    # Étape 4 : tables_and_columns → BigQuery SchemaField
    schema = _build_via_helper(columns)

    schema_by_name = {f.name: f for f in schema}
    assert schema_by_name["visitId"].mode == "NULLABLE"
    assert schema_by_name["hits"].mode == "REPEATED"
    assert schema_by_name["hits"].field_type == "RECORD"
    hits_subfields = {f.name: f for f in schema_by_name["hits"].fields}
    assert "hitNumber" in hits_subfields
    assert "page" in hits_subfields
