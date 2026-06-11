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


def test_fix_unnest_scope_leak_on_broken_duckdb_sql(con_ga):
    """
    Regression test for the _t3 scope-leak bug.

    Old sqlglot versions produced DuckDB SQL where the outer scope's column refs
    pointed to _t3 (inner subquery's UNNEST alias) instead of _t1 (outer alias).
    DuckDB raises "Referenced table '_t3' not found".

    _fix_unnest_scope_leak must detect Column(db="_t3", table="_product_u",
    this="field") in the outer scope and remap db="_t3" → db="_t1".
    """
    from utils.examples import _fix_unnest_scope_leak

    con_ga.execute("""
        CREATE OR REPLACE TABLE ga_sfx AS
        SELECT 'v_youtube' AS fullvisitorid,
               [{'product': [{'v2productname': 'youtube t-shirt'}]}] AS hits
        UNION ALL
        SELECT 'v_youtube',
               [{'product': [{'v2productname': 'google hoodie'}]}]
        UNION ALL
        SELECT 'v_other',
               [{'product': [{'v2productname': 'google pen'}]}]
    """)

    # DuckDB SQL as generated by old sqlglot: _t3 wrongly used in outer scope.
    # Outer scope has _t0(_hits_u), _t1(_product_u).
    # Inner scope has _t2(_hits_u), _t3(_product_u).
    broken_duckdb_sql = """
        WITH product_and_quatity AS (
          SELECT DISTINCT _t3._product_u.v2productname AS other_purchased_products
          FROM ga_sfx AS ga_sessions_20170201
          CROSS JOIN UNNEST(ga_sessions_20170201.hits) AS _t0(_hits_u)
          CROSS JOIN UNNEST(_hits_u.product) AS _t1(_product_u)
          WHERE NOT REGEXP_MATCHES(LOWER(_t3._product_u.v2productname), 'youtube')
          AND ga_sessions_20170201.fullvisitorid IN (
            SELECT DISTINCT ga_sessions_20170201.fullvisitorid AS fullvisitorid
            FROM ga_sfx AS ga_sessions_20170201
            CROSS JOIN UNNEST(ga_sessions_20170201.hits) AS _t2(_hits_u)
            CROSS JOIN UNNEST(_hits_u.product) AS _t3(_product_u)
            WHERE REGEXP_MATCHES(LOWER(_t3._product_u.v2productname), 'youtube')
          )
          GROUP BY _t3._product_u.v2productname
        )
        SELECT other_purchased_products FROM product_and_quatity LIMIT 1
    """

    # Verify it fails without the fix
    with pytest.raises(duckdb.BinderException, match="_t3"):
        con_ga.execute(broken_duckdb_sql)

    # Apply the fix and verify it executes correctly
    tree = sqlglot.parse_one(broken_duckdb_sql, dialect="duckdb")
    fixed_sql = _fix_unnest_scope_leak(tree).sql(dialect="duckdb")

    assert "_t3._product_u" not in fixed_sql.split("IN (")[0], (
        "outer scope still references _t3 after fix"
    )

    result = con_ga.execute(fixed_sql).fetchall()
    assert result == [("google hoodie",)]


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


def test_bare_unnest_fix_does_not_leak_alias_across_cte_scopes():
    """Régression c2.sql : _fix_bare_unnest_col_refs ne doit PAS préfixer une colonne
    avec l'alias UNNEST d'une AUTRE CTE.

    CTE `prep` contient l'UNNEST `_t0(value)`. CTE `main` référence une colonne nue
    `cd_evenement` (du fait d'une qualification incomplète en amont) dans son LEFT JOIN.
    L'ancien code prenait le DERNIER alias UNNEST du tree (`_t0`) et l'injectait
    partout → `_t0.value.cd_evenement` dans `main`, où `_t0` est hors scope →
    DuckDB "Referenced table _t0 not found". Le fix scope-aware doit laisser
    `main` intacte (aucun UNNEST local) et donc retourner None ici.
    """
    sql = """
        WITH prep AS (
          SELECT TRIM(_t0.value) AS code
          FROM ref_table AS ref_table
          CROSS JOIN UNNEST(STR_SPLIT(ref_table.raw, ',')) AS _t0(value)
        ),
        main AS (
          SELECT r.id
          FROM porteur AS r
          JOIN prep AS p ON r.code_smp = p.code
          LEFT JOIN evt AS e
            ON cd_evenement IN ('A', 'B') AND e.bank = r.bank
        )
        SELECT * FROM main
    """
    error_msg = (
        'Binder Error: Referenced column "cd_evenement" not found in FROM clause!'
    )
    patched = _fix_bare_unnest_col_refs(sql, error_msg)

    # main n'a aucun UNNEST local → le fix ne doit rien toucher.
    assert patched is None, f"Le fix a injecté un alias UNNEST hors scope :\n{patched}"


def test_bare_unnest_fix_still_patches_within_same_scope():
    """Le fix scope-aware doit TOUJOURS corriger une colonne nue qui est un champ
    de struct de l'UNNEST défini dans le MÊME scope (cas GA nominal)."""
    sql = """
        SELECT productrevenue
        FROM ds_ga
        CROSS JOIN UNNEST(hits) AS _hits_u(hits)
        CROSS JOIN UNNEST(_hits_u.hits.product) AS _product_u(product)
    """
    error_msg = (
        'Binder Error: Referenced column "productrevenue" not found in FROM clause!'
    )
    patched = _fix_bare_unnest_col_refs(sql, error_msg)

    assert patched is not None
    assert "_product_u.product.productrevenue" in patched.lower()
