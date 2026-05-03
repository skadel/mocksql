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


from build_query.schema_fetcher import (
    _bq_ddl_type,
    _flatten_bq_schema,
    _schema_fields_to_dicts,
)
from utils.schema_utils import generate_tables_and_columns_from_project_schema


# ─── Mock SchemaField ─────────────────────────────────────────────────────────


@dataclass
class FakeField:
    name: str
    field_type: str
    mode: str = "NULLABLE"
    description: str = ""
    fields: List["FakeField"] = field(default_factory=list)


# ─── Helpers ──────────────────────────────────────────────────────────────────


def _make_flat_row(
    field_path,
    data_type,
    bq_ddl_type,
    mode="NULLABLE",
    catalog="p",
    schema="ds",
    table="t",
):
    return {
        "table_catalog": catalog,
        "table_schema": schema,
        "table_name": table,
        "field_path": field_path,
        "data_type": data_type,
        "bq_ddl_type": bq_ddl_type,
        "mode": mode,
        "description": "",
    }


# ─── _schema_fields_to_dicts ──────────────────────────────────────────────────


def test_fields_to_dicts_preserves_mode_nullable():
    fields = [FakeField(name="user_id", field_type="STRING", mode="NULLABLE")]
    result = _schema_fields_to_dicts(fields)
    assert result == [
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE", "description": ""}
    ]


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
    dicts = [
        {"name": "user_id", "type": "STRING", "mode": "NULLABLE", "description": ""}
    ]
    rows = _flatten_bq_schema(dicts)
    assert rows == [
        {
            "field_path": "user_id",
            "data_type": "STRING",
            "mode": "NULLABLE",
            "description": "",
            "bq_ddl_type": "STRING",
        }
    ]


def test_flatten_repeated_record_preserves_mode():
    dicts = [
        {
            "name": "hits",
            "type": "RECORD",
            "mode": "REPEATED",
            "description": "",
            "fields": [
                {
                    "name": "page",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "",
                }
            ],
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


# ─── _bq_ddl_type ────────────────────────────────────────────────────────────


def test_bq_ddl_type_scalar():
    assert _bq_ddl_type({"type": "STRING", "mode": "NULLABLE"}) == "STRING"


def test_bq_ddl_type_scalar_repeated():
    assert _bq_ddl_type({"type": "STRING", "mode": "REPEATED"}) == "ARRAY<STRING>"


def test_bq_ddl_type_record_with_subfields():
    field = {
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {"type": "STRING", "mode": "NULLABLE", "name": "campaign"},
            {"type": "STRING", "mode": "NULLABLE", "name": "keyword"},
        ],
    }
    assert _bq_ddl_type(field) == "STRUCT<campaign STRING, keyword STRING>"


def test_bq_ddl_type_repeated_record():
    field = {
        "type": "RECORD",
        "mode": "REPEATED",
        "fields": [
            {"type": "STRING", "mode": "NULLABLE", "name": "type"},
            {"type": "INTEGER", "mode": "NULLABLE", "name": "hitNumber"},
        ],
    }
    assert _bq_ddl_type(field) == "ARRAY<STRUCT<type STRING, hitNumber INTEGER>>"


def test_bq_ddl_type_nested_record():
    field = {
        "type": "RECORD",
        "mode": "NULLABLE",
        "fields": [
            {
                "type": "RECORD",
                "mode": "NULLABLE",
                "name": "page",
                "fields": [{"type": "STRING", "mode": "NULLABLE", "name": "pagePath"}],
            }
        ],
    }
    assert _bq_ddl_type(field) == "STRUCT<page STRUCT<pagePath STRING>>"


def test_bq_ddl_type_record_without_subfields():
    # Cache ancien sans sous-champs : fallback sur le type brut, pas de crash
    assert _bq_ddl_type({"type": "RECORD", "mode": "NULLABLE"}) == "RECORD"


def test_flatten_stores_bq_ddl_type_on_record():
    dicts = [
        {
            "name": "trafficSource",
            "type": "RECORD",
            "mode": "NULLABLE",
            "description": "",
            "fields": [
                {
                    "name": "campaign",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "",
                }
            ],
        }
    ]
    rows = _flatten_bq_schema(dicts)
    root = next(r for r in rows if r["field_path"] == "trafficSource")
    assert root["bq_ddl_type"] == "STRUCT<campaign STRING>"


def test_flatten_stores_bq_ddl_type_on_repeated_record():
    dicts = [
        {
            "name": "hits",
            "type": "RECORD",
            "mode": "REPEATED",
            "description": "",
            "fields": [
                {
                    "name": "type",
                    "type": "STRING",
                    "mode": "NULLABLE",
                    "description": "",
                }
            ],
        }
    ]
    rows = _flatten_bq_schema(dicts)
    root = next(r for r in rows if r["field_path"] == "hits")
    assert root["bq_ddl_type"] == "ARRAY<STRUCT<type STRING>>"


# ─── generate_tables_and_columns_from_project_schema ──────────────────────────


def test_generate_preserves_mode():
    project_schema = {
        "data": [
            _make_flat_row(
                "hits",
                "RECORD",
                bq_ddl_type="ARRAY<STRUCT<type STRING>>",
                mode="REPEATED",
            ),
            _make_flat_row(
                "hits.page", "STRING", bq_ddl_type="STRING", mode="NULLABLE"
            ),
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
                "bq_ddl_type": "STRING",
                "description": "",
            }
        ]
    }
    tables = generate_tables_and_columns_from_project_schema(project_schema)
    col = tables[0]["columns"][0]
    assert col["mode"] == "NULLABLE"
