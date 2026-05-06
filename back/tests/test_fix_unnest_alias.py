"""
Tests for UNNEST alias resolution and schema qualification.
"""

from build_query.validator import optimize_query

from utils.examples import _fix_bare_unnest_col_refs

import duckdb
import pytest
import sqlglot
from sqlglot import MappingSchema
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables


def run_simulated_duckdb_pipeline(sql: str, con) -> list:
    """
    Simule la VRAIE exécution de production :
    optimize_query() + la boucle de Retry pilotée par DuckDB.
    """
    mock_tables = {
        "ga": {
            "hits": "ARRAY<STRUCT<type STRING, hitNumber INT, product ARRAY<STRUCT<productRevenue INT>>>>"
        },
        "ds_ga_sfx": {
            "id": "STRING",
            # On rajoute fullvisitorid pour reproduire le bug de prod
            "fullvisitorid": "STRING",
            "hits": "ARRAY<STRUCT<type STRING, hitNumber INT, product ARRAY<STRUCT<productRevenue INT>>>>",
        },
    }

    parsed_tree = sqlglot.parse_one(sql, dialect="bigquery")
    optimized_tree = optimize_query(
        parsed_tree, mock_tables, dialect="bigquery", optimize=False
    )
    current_sql = optimized_tree.sql(dialect="duckdb")

    # Simulation de run_query_on_test_dataset (Boucle de Retry)
    for _ in range(10):
        try:
            return con.execute(current_sql).fetchall()
        except duckdb.BinderException as e:
            patched = _fix_bare_unnest_col_refs(current_sql, str(e))
            if patched is None:
                raise e  # Fait crasher le test si l'erreur n'est pas "Column not found"
            current_sql = patched

    return con.execute(current_sql).fetchall()


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE ds_ga_sfx (
            id TEXT, 
            fullvisitorid TEXT,
            hits STRUCT(type TEXT, hitNumber INT, product STRUCT(productRevenue INT)[])[]
        )
    """)
    c.execute("""
        INSERT INTO ds_ga_sfx VALUES (
            'v1', 
            'visitor_123',
            [{'type': 'PAGE', 'hitNumber': 1, 'product': [{'productRevenue': 100}]}]
        )
    """)
    return c


class TestProductionPipeline:
    def test_duckdb_executes_without_ambiguity_error(self, con):
        """Teste l'ambiguïté simple (hits.type)"""
        sql = "SELECT hits.type FROM ds_ga_sfx CROSS JOIN UNNEST(hits) AS hits"
        result = run_simulated_duckdb_pipeline(sql, con)
        assert result == [("PAGE",)]

    def test_duckdb_resolves_bare_product_revenue_safely(self, con):
        """
        LE TEST DE TA PROD : Doit résoudre productRevenue GRÂCE AU RETRY LOOP,
        SANS corrompre fullvisitorid !
        """
        sql = """
            SELECT productRevenue, fullvisitorid 
            FROM ds_ga_sfx 
            CROSS JOIN UNNEST(hits) AS hits 
            CROSS JOIN UNNEST(hits.product) AS product
            ORDER BY fullvisitorid
        """
        result = run_simulated_duckdb_pipeline(sql, con)
        # On vérifie que la valeur est bien extraite (100) et que visitor_123 n'a pas planté
        assert result == [(100, "visitor_123")]


def test_alert_when_sqlglot_fixes_duckdb_structs(con):
    """
    CANARY TEST : Le jour où SQLGlot résout les structs nativement,
    on pourra supprimer la boucle de retry !
    """
    mock_tables = {
        "ds_ga_sfx": {
            "id": "STRING",
            "hits": "ARRAY<STRUCT<type STRING, hitNumber INT, product ARRAY<STRUCT<productRevenue INT>>>>",
        },
    }
    schema = MappingSchema()
    for table_name, columns in mock_tables.items():
        schema.add_table(table_name, columns, dialect="bigquery")

    sql = "SELECT productRevenue FROM ds_ga_sfx CROSS JOIN UNNEST(hits) AS hits CROSS JOIN UNNEST(hits.product) AS product"
    tree = sqlglot.parse_one(sql, dialect="bigquery")

    tree = qualify_tables(tree)
    tree = qualify_columns(tree, schema, infer_schema=True)
    duckdb_sql = tree.sql(dialect="duckdb")

    with pytest.raises(duckdb.BinderException):
        con.execute(duckdb_sql).fetchall()


@pytest.fixture
def con_ga():
    c = duckdb.connect()
    c.execute("""
        CREATE TABLE ga_sessions (
            fullvisitorid TEXT,
            hits STRUCT(product STRUCT(v2productname TEXT)[])[]
        )
    """)
    c.execute("""
        INSERT INTO ga_sessions VALUES
        ('visitor_youtube', [{'product': [{'v2productname': 'youtube t-shirt'}]}]),
        ('visitor_youtube', [{'product': [{'v2productname': 'google hoodie'}]}]),
        ('visitor_other',   [{'product': [{'v2productname': 'google pen'}]}])
    """)
    return c


def test_duckdb_resolves_unnest_cols_with_subquery_scoping(con_ga):
    """
    Reproduit le bug de prod : qualify_columns résolvait product.v2productname
    dans la scope externe avec _t3 (alias UNNEST de la sous-requête) au lieu
    de _t1 (alias local). DuckDB levait "Referenced table _t3 not found".
    Cause : _fix_unnest_alias_conflicts était appelé avant qualify_columns,
    ce qui rendait les refs introuvables par scope. Fix : appeler après.
    """
    sql = """
        SELECT DISTINCT product.v2productname
        FROM ga_sessions
        CROSS JOIN UNNEST(hits) AS hits
        CROSS JOIN UNNEST(hits.product) AS product
        WHERE NOT REGEXP_CONTAINS(LOWER(product.v2productname), 'youtube')
        AND fullvisitorid IN (
            SELECT DISTINCT fullvisitorid
            FROM ga_sessions
            CROSS JOIN UNNEST(hits) AS hits
            CROSS JOIN UNNEST(hits.product) AS product
            WHERE REGEXP_CONTAINS(LOWER(product.v2productname), 'youtube')
        )
    """
    mock_tables = {
        "ga_sessions": {
            "fullvisitorid": "STRING",
            "hits": "ARRAY<STRUCT<product ARRAY<STRUCT<v2productname STRING>>>>",
        }
    }

    parsed_tree = sqlglot.parse_one(sql, dialect="bigquery")
    optimized_tree = optimize_query(
        parsed_tree, mock_tables, dialect="bigquery", optimize=False
    )
    current_sql = optimized_tree.sql(dialect="duckdb")

    for _ in range(10):
        try:
            result = con_ga.execute(current_sql).fetchall()
            assert sorted(result) == [("google hoodie",)]
            return
        except duckdb.BinderException as e:
            patched = _fix_bare_unnest_col_refs(current_sql, str(e))
            if patched is None:
                pytest.fail(
                    f"BinderException non récupérable — vérifier l'ordre "
                    f"qualify_columns / _fix_unnest_alias_conflicts:\n{e}\nSQL:\n{current_sql}"
                )
            current_sql = patched

    pytest.fail("Retry loop épuisé sans succès")


def test_alert_when_duckdb_error_message_format_changes(con):
    """
    CANARY TEST : Si DuckDB change le format de son message d'erreur pour les colonnes
    non résolues, _fix_bare_unnest_col_refs retournera None et le retry loop ne pourra
    plus corriger automatiquement. Ce test doit casser si le format change.
    """
    sql = """
        SELECT productRevenue
        FROM ds_ga_sfx
        CROSS JOIN UNNEST(hits) AS _hits_u(hits)
        CROSS JOIN UNNEST(_hits_u.hits.product) AS _product_u(product)
    """
    try:
        con.execute(sql).fetchall()
        pytest.fail(
            "DuckDB devrait lever une BinderException pour productRevenue non résolu"
        )
    except duckdb.BinderException as e:
        error_msg = str(e)
        patched = _fix_bare_unnest_col_refs(sql, error_msg)
        assert patched is not None, (
            f"_fix_bare_unnest_col_refs n'a pas reconnu le message d'erreur DuckDB.\n"
            f"Message reçu : {error_msg!r}\n"
            f"Vérifier le regex dans _fix_bare_unnest_col_refs et mettre à jour si besoin."
        )
