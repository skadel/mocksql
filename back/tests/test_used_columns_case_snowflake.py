"""used_columns : la casse schema_cache ↔ SQL ne doit jamais casser l'extraction.

Incident (debug sf_bq263, spider2-snow) : ``extract_used_columns_from_sql``
levait ``OptimizeError: Unknown column: created_at`` — les colonnes du
schema_cache étaient ajoutées NON-quotées au ``MappingSchema`` sqlglot, donc
normalisées MAJUSCULE par le dialecte snowflake, alors que le SQL les quote en
minuscule. ``build_used_columns`` (CLI) avalait l'exception (``except: pass``)
→ repli silencieux sur TOUTES les colonnes du schéma (29 au lieu de 9 sur
sf_bq263) → prompt du generator gonflé.

Fix : casse neutralisée (minuscule) des DEUX côtés — schéma et identifiants de
l'AST — même famille que le fix Trino ([[project_trino_used_columns_case_mismatch]],
version minuscule ; ici la normalisation dialecte snowflake fabriquait la
version MAJUSCULE). Le repli de ``build_used_columns`` reste (dégradation sûre)
mais est journalisé en warning AVEC le SQL fautif.
"""

import json
import logging

import cli.generate as cli_generate
from cli.generate import build_used_columns
from utils.sql_code import extract_used_columns_from_sql

SNOW_SCHEMAS = [
    {
        "table_name": "GITHUB_REPOS.repos",
        "columns": [
            {"name": "repo_name", "type": "TEXT"},
            {"name": "created_at", "type": "TIMESTAMP_NTZ"},
            {"name": "stars", "type": "NUMBER"},
            {"name": "language", "type": "TEXT"},
        ],
    },
    {
        "table_name": "GITHUB_REPOS.licenses",
        "columns": [
            {"name": "repo_name", "type": "TEXT"},
            {"name": "license", "type": "TEXT"},
        ],
    },
]


def _entries(sql: str, schemas=SNOW_SCHEMAS) -> dict[str, set[str]]:
    out = {}
    for e in extract_used_columns_from_sql(sql, "snowflake", schemas):
        entry = json.loads(e)
        out[entry["table"]] = set(entry["used_columns"])
    return out


def test_sf_bq263_uppercase_tables_lowercase_columns():
    """Repro fidèle sf_bq263 : tables quotées MAJUSCULE + colonnes quotées
    minuscule, schéma connu. Levait ``OptimizeError: Unknown column:
    created_at`` (colonnes du cache normalisées MAJUSCULE par le dialecte)."""
    schemas = [
        {
            "table_name": "THELOOK_ECOMMERCE.THELOOK_ECOMMERCE.ORDERS",
            "columns": [
                {"name": "order_id", "type": "NUMBER"},
                {"name": "status", "type": "TEXT"},
                {"name": "created_at", "type": "NUMBER"},
                {"name": "user_id", "type": "NUMBER"},
            ],
        },
        {
            "table_name": "THELOOK_ECOMMERCE.THELOOK_ECOMMERCE.ORDER_ITEMS",
            "columns": [
                {"name": "order_id", "type": "NUMBER"},
                {"name": "product_id", "type": "NUMBER"},
                {"name": "sale_price", "type": "FLOAT"},
            ],
        },
    ]
    sql = (
        "SELECT DATE_TRUNC('MONTH',"
        ' TO_TIMESTAMP_NTZ("O"."created_at" / 1000000)) AS "month",'
        ' SUM("OI"."sale_price") AS "total_sales"'
        ' FROM "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDER_ITEMS" AS "OI"'
        ' JOIN "THELOOK_ECOMMERCE"."THELOOK_ECOMMERCE"."ORDERS" AS "O"'
        ' ON "OI"."order_id" = "O"."order_id"'
        ' WHERE "O"."status" = \'Complete\''
        " GROUP BY 1"
    )
    entries = _entries(sql, schemas)
    assert entries == {
        "orders": {"created_at", "order_id", "status"},
        "order_items": {"order_id", "sale_price"},
    }


def test_snowflake_quoted_lowercase_columns_extracted():
    """Pattern sf_bq263 : cache minuscule + SQL quoté minuscule → extraction OK,
    seules les colonnes réellement référencées (pas les 29 du schéma)."""
    sql = (
        'SELECT r."repo_name", DATE_TRUNC(\'MONTH\', r."created_at") AS month'
        ' FROM GITHUB_REPOS."repos" AS r'
        " WHERE r.\"created_at\" >= '2023-01-01'"
    )
    entries = _entries(sql)
    assert entries == {"repos": {"repo_name", "created_at"}}


def test_snowflake_unquoted_refs_match_lowercase_cache():
    """Réfs non-quotées (normalisées MAJUSCULE par le dialecte) → rapprochées du
    cache minuscule quand même."""
    sql = (
        "SELECT r.repo_name, l.license FROM GITHUB_REPOS.repos AS r"
        " JOIN GITHUB_REPOS.licenses AS l ON r.repo_name = l.repo_name"
        " WHERE r.stars > 100"
    )
    entries = _entries(sql)
    assert entries["repos"] == {"repo_name", "stars"}
    assert entries["licenses"] == {"repo_name", "license"}


def test_uppercase_cache_lowercase_sql():
    """Cache MAJUSCULE (autre entrepôt) + SQL minuscule : même neutralisation."""
    schemas = [
        {
            "table_name": "MYDB.EVENTS",
            "columns": [
                {"name": "ID", "type": "NUMBER"},
                {"name": "TS", "type": "TIMESTAMP_NTZ"},
                {"name": "PAYLOAD", "type": "VARIANT"},
            ],
        }
    ]
    sql = "SELECT id FROM mydb.events WHERE ts >= '2023-01-01'"
    entries = _entries(sql, schemas)
    assert entries == {"events": {"id", "ts"}}


def test_build_used_columns_fallback_logs_warning_with_sql(monkeypatch, caplog):
    """Extraction en échec → repli sur toutes les colonnes (dégradation sûre),
    mais journalisé en warning AVEC le SQL fautif — plus jamais silencieux."""
    boom_sql = "SELECT broken FROM GITHUB_REPOS.repos"

    def _boom(sql, dialect, schemas):
        raise ValueError("kaboom")

    monkeypatch.setattr(cli_generate, "extract_used_columns_from_sql", _boom)
    with caplog.at_level(logging.WARNING):
        result = build_used_columns(SNOW_SCHEMAS, boom_sql, "snowflake")

    # Repli : toutes les colonnes de toutes les tables du schéma.
    parsed = [json.loads(e) for e in result]
    assert {p["table"] for p in parsed} == {"repos", "licenses"}
    assert set(parsed[0]["used_columns"]) == {
        "repo_name",
        "created_at",
        "stars",
        "language",
    }
    # Warning avec le SQL fautif (règle projet : toujours logger la requête).
    warnings = [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]
    assert any(boom_sql in m for m in warnings), warnings
    assert any("kaboom" in m for m in warnings), warnings


def test_build_used_columns_no_warning_when_extraction_succeeds(caplog):
    """Chemin nominal : pas de warning parasite."""
    sql = 'SELECT "repo_name" FROM GITHUB_REPOS."repos"'
    with caplog.at_level(logging.WARNING):
        result = build_used_columns(SNOW_SCHEMAS, sql, "snowflake")
    assert [json.loads(e)["used_columns"] for e in result] == [["repo_name"]]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]
