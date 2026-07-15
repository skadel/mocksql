"""Tests de compatibilité Snowflake : transpilation d'idiomes + import de schéma.

Couvre les correctifs apportés après l'évaluation Spider2-snow :
  - import live : DictCursor MAJUSCULES, INFORMATION_SCHEMA qualifié, casse préservée,
    reconstruction NUMBER(p, s) ;
  - transpilation : TO_TIMESTAMP[_NTZ/LTZ/TZ] type-aware, TO_CHAR(date), débordement
    NUMBER → DECIMAL large ;
  - débogage : SQL de debug quoté selon le dialecte (plus de backticks BigQuery).

Le pipeline d'exécution réel est DuckDB ; chaque test transpilé vérifie donc que la
sortie s'exécute (ou échoue exactement là où c'est attendu).
"""

import asyncio

import duckdb
import pytest
import sqlglot

from build_query.debug_executor import _quote_ident
from build_query.schema_fetcher import _sf_get, _sf_quote, _sf_snow_data_type
from build_query.examples_executor import _is_duckdb_data_error
from utils.examples import (
    _get_ddl_type,
    _resolve_duck_type,
    _widen_bare_decimals,
    create_test_tables,
    fix_duck_db_sql,
    initialize_duckdb,
    parse_test_query,
)


def _ptq(sql: str, dialect: str = "snowflake") -> str:
    return asyncio.run(parse_test_query(sql, "sfx", dialect))


# ---------------------------------------------------------------------------
# TO_TIMESTAMP / TO_TIMESTAMP_NTZ — type-aware
# ---------------------------------------------------------------------------


def test_to_timestamp_ntz_numeric_epoch_uses_to_timestamp():
    """TO_TIMESTAMP_NTZ(epoch) → to_timestamp (DuckDB ne sait pas CAST DOUBLE→TIMESTAMP)."""
    out = _ptq('SELECT TO_TIMESTAMP_NTZ("bt" / 1000000) AS d FROM mydb.s.t')
    assert "TO_TIMESTAMP(" in out.upper()
    assert "AS TIMESTAMP)" not in out.upper()


def test_to_timestamp_ntz_string_uses_cast():
    out = _ptq("SELECT TO_TIMESTAMP_NTZ(s) AS d FROM mydb.s.t")
    assert "CAST(s AS TIMESTAMP)" in out


def test_fix_duck_db_sql_does_not_clobber_to_timestamp():
    """Le regex TO_TIMESTAMP→CAST a été retiré : un to_timestamp(epoch) doit survivre."""
    s = "SELECT TO_TIMESTAMP(c / 1000000) FROM t"
    assert fix_duck_db_sql(s, "snowflake") == s


def test_to_timestamp_ntz_epoch_executes_on_duckdb():
    out = _ptq("SELECT TO_DATE(TO_TIMESTAMP_NTZ(1672531200000000 / 1000000)) AS d")
    res = duckdb.sql(out).fetchall()
    assert res[0][0].isoformat() == "2023-01-01"


# ---------------------------------------------------------------------------
# TO_CHAR — formats date → strftime, formats numériques laissés en CAST AS TEXT
# ---------------------------------------------------------------------------


def test_to_char_date_format_becomes_strftime():
    out = _ptq("SELECT TO_CHAR(d, 'YYYY-MM-DD HH24:MI:SS') AS x FROM mydb.s.t")
    assert "STRFTIME(" in out.upper()
    assert "%Y-%m-%d %H:%M:%S" in out


def test_to_char_numeric_format_stays_cast_text():
    out = _ptq("SELECT TO_CHAR(n, '999,999.00') AS x FROM mydb.s.t")
    assert "STRFTIME" not in out.upper()
    assert "AS TEXT)" in out.upper()


def test_to_char_date_executes_on_duckdb():
    out = _ptq("SELECT TO_CHAR(DATE '2023-07-15', 'YYYY/MM/DD') AS x")
    assert duckdb.sql(out).fetchall()[0][0] == "2023/07/15"


# ---------------------------------------------------------------------------
# NUMBER sans précision → DECIMAL large (anti-débordement)
# ---------------------------------------------------------------------------


def test_widen_bare_decimals():
    tree = sqlglot.parse_one(
        "CREATE TABLE t (a NUMBER, b NUMBER(12, 2), c DECIMAL, d NUMERIC)",
        dialect="bigquery",
    )
    _widen_bare_decimals(tree)
    out = tree.sql(dialect="duckdb")
    assert out.count("DECIMAL(38, 9)") == 3  # a, c, d
    assert "DECIMAL(12, 2)" in out  # b inchangé


def test_bare_number_column_holds_large_integer():
    """Un grand entier (epoch µs / wei) ne doit plus déborder DECIMAL(18, 3)."""
    schema = [
        {
            "table_name": "db.sch.events",
            "columns": [{"name": "amount", "type": "NUMBER", "mode": "NULLABLE"}],
        }
    ]
    with initialize_duckdb(":memory:") as con:
        create_test_tables(schema, "sfx", con, "snowflake", overwrite=True)
        con.execute("INSERT INTO sch_events_sfx VALUES (2000000000000000)")
        assert (
            con.execute("SELECT amount FROM sch_events_sfx").fetchone()[0]
            == 2000000000000000
        )


# ---------------------------------------------------------------------------
# Import live Snowflake : robustesse aux clés DictCursor + reconstruction NUMBER
# ---------------------------------------------------------------------------


def test_sf_get_uppercase_dictcursor_keys():
    """Le DictCursor Snowflake renvoie les clés en MAJUSCULES."""
    row = {"COLUMN_NAME": "C_CUSTKEY", "DATA_TYPE": "NUMBER"}
    assert _sf_get(row, "COLUMN_NAME") == "C_CUSTKEY"
    assert _sf_get(row, "column_name") == "C_CUSTKEY"  # tolère minuscule aussi
    assert _sf_get(row, "MISSING") == ""


def test_sf_snow_data_type_reconstructs_number_precision():
    row = {"DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": "38", "NUMERIC_SCALE": "0"}
    assert _sf_snow_data_type(row) == "NUMBER(38,0)"
    row2 = {"DATA_TYPE": "NUMBER", "NUMERIC_PRECISION": "12", "NUMERIC_SCALE": "2"}
    assert _sf_snow_data_type(row2) == "NUMBER(12,2)"
    # NUMBER sans précision exposée → laissé tel quel (le widen DuckDB s'en charge)
    assert _sf_snow_data_type({"DATA_TYPE": "NUMBER"}) == "NUMBER"
    assert _sf_snow_data_type({"DATA_TYPE": "TEXT"}) == "TEXT"


def test_sf_quote_identifier():
    assert _sf_quote("SNOWFLAKE_SAMPLE_DATA") == '"SNOWFLAKE_SAMPLE_DATA"'


# ---------------------------------------------------------------------------
# Debug executor : quoting dépendant du dialecte (plus de backticks BigQuery)
# ---------------------------------------------------------------------------


def test_quote_ident_dialect_aware():
    assert _quote_ident("tx_fees", "bigquery") == "`tx_fees`"
    assert _quote_ident("tx_fees", "snowflake") == '"tx_fees"'
    assert _quote_ident("tx_fees", "duckdb") == '"tx_fees"'


def test_snowflake_debug_sql_parses():
    """Un WITH quoté pour snowflake doit se parser (le backtick BigQuery cassait)."""
    name = _quote_ident("tx_fees", "snowflake")
    sql = f"WITH {name} AS (SELECT 1 AS x)\nSELECT * FROM {name}"
    # ne doit pas lever de ParseError
    sqlglot.parse_one(sql, read="snowflake")


# ---------------------------------------------------------------------------
# Hex string → DOUBLE : CAST('0x' || h AS FLOAT) → hexstr_to_double(h)
# (sf_bq083 : Snowflake parse l'hexa depuis une chaîne runtime, DuckDB non)
# ---------------------------------------------------------------------------


def _fresh_con(monkeypatch):
    """Connexion DuckDB préparée comme en prod (extensions + macros), sans réseau."""
    from storage import config

    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: [])
    con = duckdb.connect(":memory:")
    config.apply_duckdb_extensions(con)
    return con


def test_hexstr_macro_values(monkeypatch):
    """Valeurs de référence : uint256 sans overflow, vide=0, invalide/NULL → NULL."""
    con = _fresh_con(monkeypatch)
    cases = [
        ("f4240", 1_000_000.0),
        ("de0b6b3a7640000", 1e18),
        ("ff", 255.0),
        ("F4240", 1_000_000.0),  # casse indifférente
        ("", 0.0),  # valeur 0 encodée : LTRIM a tout mangé
        ("zz", None),  # non-hexa → NULL (pas de 0 silencieux)
        (None, None),
    ]
    for h, expected in cases:
        got = con.execute("SELECT hexstr_to_double(?)", [h]).fetchone()[0]
        assert got == expected, f"hexstr_to_double({h!r}) = {got}, attendu {expected}"


def test_hex_cast_rewritten_to_macro():
    out = _ptq(
        "SELECT CAST('0x' || LTRIM(SUBSTRING(\"input\", 75), '0') AS FLOAT) AS v"
        " FROM mydb.s.t"
    )
    assert "HEXSTR_TO_DOUBLE" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_chained_concat():
    """'0x' || a || b : DPipe imbriqué à gauche — le préfixe doit être trouvé en feuille."""
    out = _ptq("SELECT CAST('0x' || a || b AS FLOAT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_uppercase_prefix_and_try_cast():
    out = _ptq("SELECT TRY_CAST('0X' || h AS DOUBLE) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" in out.upper()


def test_hex_cast_concat_function_form():
    out = _ptq("SELECT CAST(CONCAT('0x', h) AS FLOAT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" in out.upper()


def test_normal_float_casts_untouched():
    """Un CAST float ordinaire (littéral ou colonne) ne doit PAS déclencher le fixer."""
    for sql in [
        "SELECT CAST('3.14' AS FLOAT) AS v",
        "SELECT CAST(col AS FLOAT) AS v FROM mydb.s.t",
        "SELECT CAST(a || b AS FLOAT) AS v FROM mydb.s.t",  # concat sans préfixe 0x
    ]:
        assert "HEXSTR_TO_DOUBLE" not in _ptq(sql).upper()


def test_hex_cast_to_varchar_untouched():
    out = _ptq("SELECT CAST('0x' || h AS VARCHAR) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" not in out.upper()


def test_hex_cast_executes_end_to_end(monkeypatch):
    """Chaîne complète : transpilation + exécution DuckDB sur l'idiome sf_bq083."""
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT CAST('0x' || LTRIM('000f4240', '0') AS FLOAT) AS v")
    assert con.execute(out).fetchone()[0] == 1_000_000.0


def test_validator_helper_has_macro(monkeypatch):
    """Le dry-run PREPARE du validateur passe par DuckDBTestHelper : macro requis."""
    from storage import config

    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: [])
    from utils.duckdb_test_helper import DuckDBTestHelper

    helper = DuckDBTestHelper(":memory:")
    assert helper.conn.execute("SELECT hexstr_to_double('ff')").fetchone()[0] == 255.0


def test_macro_is_connection_local_no_catalog_conflict(monkeypatch, tmp_path):
    """Le macro doit être TEMP : sinon deux connexions concurrentes sur la même
    base fichier écrivent le catalogue en transactions chevauchantes → DuckDB
    lève « Catalog write-write conflict », cassant l'ouverture de connexion
    (executor, validator, pool) pour TOUS les dialectes. En prod DUCKDB_PATH est
    un fichier partagé, pas ':memory:'."""
    from storage import config

    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: [])
    db_file = str(tmp_path / "shared.duckdb")
    con_a = duckdb.connect(db_file)
    con_b = duckdb.connect(db_file)
    try:
        # Transactions chevauchantes : reproduit le scénario multi-session.
        con_a.execute("BEGIN")
        con_b.execute("BEGIN")
        config.apply_duckdb_extensions(con_a)
        config.apply_duckdb_extensions(con_b)  # ne doit PAS lever
        con_a.execute("COMMIT")
        con_b.execute("COMMIT")
        assert con_b.execute("SELECT hexstr_to_double('ff')").fetchone()[0] == 255.0
    finally:
        con_a.close()
        con_b.close()


def test_hex_idiom_not_folded_to_null_by_scalar_folder():
    """Le constant-folder (validator) tourne AVANT le fixer hexa. Un idiome hexa
    tout-littéral sous TRY_CAST ne doit pas être replié en NULL, sinon le fixer
    ne voit plus jamais le CAST et le résultat est silencieusement faux."""
    import sqlglot

    from build_query.scalar_folder import fold_scalar_expressions

    sql = "SELECT TRY_CAST('0x' || 'f4240' AS DOUBLE) AS v FROM db.s.t"
    tree = sqlglot.parse_one(sql, dialect="snowflake")
    folded = fold_scalar_expressions(tree, "snowflake").sql(dialect="snowflake")
    # L'idiome '0x' || ... doit survivre au fold pour que le fixer le réécrive.
    assert "'0x'" in folded.lower(), f"idiome hexa replié/perdu : {folded}"
    assert " NULL " not in f" {folded} ".upper()


def test_hex_idiom_all_literal_executes_end_to_end(monkeypatch):
    """Chaîne complète fold → fixer → exécution sur un idiome hexa tout-littéral."""
    con = _fresh_con(monkeypatch)
    import sqlglot

    from build_query.scalar_folder import fold_scalar_expressions

    sql = "SELECT TRY_CAST('0x' || 'f4240' AS DOUBLE) AS v"
    folded = fold_scalar_expressions(
        sqlglot.parse_one(sql, dialect="snowflake"), "snowflake"
    )
    out = _ptq(folded.sql(dialect="snowflake"))
    assert con.execute(out).fetchone()[0] == 1_000_000.0


# ---------------------------------------------------------------------------
# Durcissement du fixer hexa : formes fonction, parenthèses, CONCAT imbriqué,
# CAST imbriqué, cibles entières/décimales (revue #3/#5/#6)
# ---------------------------------------------------------------------------


def _rewrites(sql: str) -> bool:
    out = _ptq(sql).upper()
    return "HEXSTR_TO_DOUBLE" in out and "'0X'" not in out


def test_hex_function_form_to_double():
    """TO_DOUBLE('0x' || h) — forme fonction idiomatique Snowflake (exp.ToDouble)."""
    assert _rewrites("SELECT TO_DOUBLE('0x' || h) AS v FROM mydb.s.t")


def test_hex_function_form_try_to_double():
    assert _rewrites("SELECT TRY_TO_DOUBLE('0x' || h) AS v FROM mydb.s.t")


def test_hex_paren_in_concat_chain():
    """('0x' || a) || b — parenthèses explicites dans la chaîne."""
    assert _rewrites("SELECT CAST(('0x' || a) || b AS FLOAT) AS v FROM mydb.s.t")


def test_hex_nested_concat_function():
    """CONCAT(CONCAT('0x', a), b) — CONCAT imbriqué, préfixe non en tête directe."""
    assert _rewrites(
        "SELECT CAST(CONCAT(CONCAT('0x', a), b) AS FLOAT) AS v FROM mydb.s.t"
    )


def test_hex_nested_cast_fully_rewritten():
    """CAST imbriqué : aucun '0x' résiduel ne doit subsister (pas de faux succès)."""
    out = _ptq(
        "SELECT CAST('0x' || CAST('0x' || h AS FLOAT) AS DOUBLE) AS v FROM mydb.s.t"
    )
    assert "'0x'" not in out.lower(), f"'0x' résiduel : {out}"


def test_hex_cast_to_number_target():
    """CAST('0x' || h AS NUMBER) : cible décimale → CAST(hexstr_to_double(h) AS ...)."""
    out = _ptq("SELECT CAST('0x' || h AS NUMBER) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_to_int_target():
    out = _ptq("SELECT CAST('0x' || h AS INT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_to_number_function_form():
    assert _rewrites("SELECT TO_NUMBER('0x' || h) AS v FROM mydb.s.t")


def test_hex_int_target_executes_end_to_end(monkeypatch):
    """Cible entière : la valeur revient bien en entier, pas en double brut."""
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT CAST('0x' || LTRIM('000f4240', '0') AS INT) AS v")
    assert con.execute(out).fetchone()[0] == 1_000_000


def test_hex_function_form_executes_end_to_end(monkeypatch):
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT TO_DOUBLE('0x' || LTRIM('000f4240', '0')) AS v")
    assert con.execute(out).fetchone()[0] == 1_000_000.0


def test_normal_numeric_casts_still_untouched():
    """Régression : un CAST numérique ordinaire ne déclenche jamais le fixer."""
    for sql in [
        "SELECT CAST('3.14' AS FLOAT) AS v",
        "SELECT CAST(col AS INT) AS v FROM mydb.s.t",
        "SELECT TO_NUMBER(col) AS v FROM mydb.s.t",
        "SELECT CAST(a || b AS FLOAT) AS v FROM mydb.s.t",
    ]:
        assert "HEXSTR_TO_DOUBLE" not in _ptq(sql).upper()


# ---------------------------------------------------------------------------
# Sémantique d'erreur : CAST strict lève sur hexa invalide, TRY_* rend NULL
# (revue #4 — fidélité Snowflake : CAST strict échoue, TRY_CAST → NULL)
# ---------------------------------------------------------------------------


def test_strict_cast_uses_strict_macro():
    """CAST (non-TRY) → hexstr_to_double_strict (lève sur invalide)."""
    out = _ptq("SELECT CAST('0x' || h AS FLOAT) AS v FROM mydb.s.t").upper()
    assert "HEXSTR_TO_DOUBLE_STRICT(" in out


def test_try_cast_uses_lenient_macro():
    """TRY_CAST → hexstr_to_double (NULL sur invalide), PAS la variante stricte."""
    out = _ptq("SELECT TRY_CAST('0x' || h AS FLOAT) AS v FROM mydb.s.t").upper()
    assert "HEXSTR_TO_DOUBLE(" in out
    assert "STRICT" not in out


def test_try_to_double_uses_lenient_macro():
    out = _ptq("SELECT TRY_TO_DOUBLE('0x' || h) AS v FROM mydb.s.t").upper()
    assert "HEXSTR_TO_DOUBLE(" in out
    assert "STRICT" not in out


def test_to_double_function_uses_strict_macro():
    """TO_DOUBLE (non-TRY) est strict."""
    out = _ptq("SELECT TO_DOUBLE('0x' || h) AS v FROM mydb.s.t").upper()
    assert "HEXSTR_TO_DOUBLE_STRICT(" in out


def test_strict_cast_raises_on_invalid_hex(monkeypatch):
    """Un CAST strict sur une donnée non-hexa LÈVE (fidèle à Snowflake), au lieu
    de rendre NULL silencieusement."""
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT CAST('0x' || 'zz' AS FLOAT) AS v")
    with pytest.raises(Exception) as exc:
        con.execute(out).fetchone()
    assert "invalide" in str(exc.value).lower() or "invalid" in str(exc.value).lower()


def test_try_cast_returns_null_on_invalid_hex(monkeypatch):
    """TRY_CAST sur une donnée non-hexa rend NULL (fidèle à Snowflake TRY_CAST)."""
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT TRY_CAST('0x' || 'zz' AS FLOAT) AS v")
    assert con.execute(out).fetchone()[0] is None


def test_strict_cast_null_input_stays_null(monkeypatch):
    """NULL en entrée reste NULL même en strict (Snowflake CAST(NULL) ne lève pas)."""
    con = _fresh_con(monkeypatch)
    # '0x' || NULL → NULL en amont ; le macro strict ne doit pas lever sur NULL.
    out = _ptq("SELECT CAST('0x' || CAST(NULL AS VARCHAR) AS FLOAT) AS v")
    assert con.execute(out).fetchone()[0] is None


def test_strict_cast_valid_hex_executes(monkeypatch):
    """Le strict n'entrave pas le cas nominal : hexa valide → valeur."""
    con = _fresh_con(monkeypatch)
    out = _ptq("SELECT CAST('0x' || LTRIM('000f4240', '0') AS FLOAT) AS v")
    assert con.execute(out).fetchone()[0] == 1_000_000.0


# ---------------------------------------------------------------------------
# LATERAL FLATTEN → CROSS/LEFT JOIN UNNEST (Snowflake → DuckDB)
# ---------------------------------------------------------------------------


def _run_duck(sql: str, setup: list[str]) -> list:
    """Exécute `sql` sur un DuckDB in-memory après avoir joué `setup`."""
    con = duckdb.connect(":memory:")
    for stmt in setup:
        con.execute(stmt)
    return con.execute(sql).fetchall()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "Canari : sqlglot 30.11.0 rend LATERAL FLATTEN en DuckDB invalide (virgule "
        "orpheline + kwarg `input =>` survivant), d'où le workaround "
        "_fix_snowflake_flatten. Le jour où sqlglot corrige ce rendu, ce test passe "
        "(xpass) → la suite échoue en mode strict, signalant qu'on peut retirer le "
        "contournement (examples.py:_fix_snowflake_flatten + _fix_snowflake_variant_string_cast)."
    ),
)
def test_canary_sqlglot_flatten_render_still_broken():
    """Alerte si sqlglot corrige nativement la transpilation FLATTEN → DuckDB.

    Rendu BRUT (sans passer par `_fix_snowflake_idioms`) : on affirme que sqlglot
    produit un DuckDB VALIDE. Tant que le bug persiste, l'assertion échoue → xfail.
    """
    tree = sqlglot.parse_one(
        'SELECT x.value FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c")) x',
        dialect="snowflake",
    )
    raw = tree.sql(dialect="duckdb")
    assert ", CROSS JOIN" not in " ".join(raw.split())  # pas de virgule orpheline
    assert "=>" not in raw  # kwarg Snowflake ne survit pas


def test_flatten_input_parse_json_transpiles_and_executes():
    """Cas nominal : `LATERAL FLATTEN(input => PARSE_JSON(x))` + `.value::STRING`.

    Vérifie la correction des 3 défauts : virgule orpheline, kwarg `input =>`
    survivant, et alias 6-colonnes sans équivalent DuckDB.
    """
    out = _ptq(
        "SELECT FLATTENED.value::STRING AS address "
        'FROM inputs i, LATERAL FLATTEN(input => PARSE_JSON(i."addresses")) FLATTENED'
    )
    # défaut 1 : plus de virgule implicite qui coexiste avec le CROSS JOIN
    assert ", CROSS JOIN" not in " ".join(out.split())
    # défaut 2 : le kwarg Snowflake `input =>` a disparu
    assert "=>" not in out
    # défaut 3 : rendu en CROSS JOIN UNNEST sur une seule colonne value
    assert "CROSS JOIN UNNEST" in out.upper()
    rows = _run_duck(
        out,
        [
            "CREATE TABLE inputs (addresses TEXT)",
            'INSERT INTO inputs VALUES (\'["addr1","addr2"]\'), (\'["solo"]\')',
        ],
    )
    # unquoting : `.value::STRING` ne garde pas les guillemets JSON
    assert sorted(r[0] for r in rows) == ["addr1", "addr2", "solo"]


def test_flatten_bare_column_input_executes():
    """Entrée = colonne nue (VARIANT/ARRAY Snowflake, texte JSON côté DuckDB)."""
    out = _ptq(
        'SELECT c.value:"code"::STRING AS code '
        'FROM pubs p, LATERAL FLATTEN(input => p."cpc") c'
    )
    assert "->>" in out  # value:"code"::STRING → unquoting
    rows = _run_duck(
        out,
        [
            "CREATE TABLE pubs (cpc TEXT)",
            'INSERT INTO pubs VALUES (\'[{"code":"A61"},{"code":"B22"}]\')',
        ],
    )
    assert sorted(r[0] for r in rows) == ["A61", "B22"]


def test_flatten_variant_field_string_cast_unquoted():
    """`value:"name"::STRING` → `value ->> '$.name'` (déquotage JSON)."""
    out = _ptq(
        'SELECT f.value:"name"::STRING AS n '
        'FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c")) f'
    )
    assert "->>" in out
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (c TEXT)",
            'INSERT INTO t VALUES (\'[{"name":"ACME"},{"name":"BETA"}]\')',
        ],
    )
    assert sorted(r[0] for r in rows) == ["ACME", "BETA"]


def test_flatten_outer_true_preserves_empty_rows():
    """`outer => TRUE` → LEFT JOIN UNNEST … ON TRUE (ligne parent conservée)."""
    out = _ptq(
        "SELECT t.id, x.value::STRING AS v "
        'FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c"), outer => TRUE) x'
    )
    assert "LEFT JOIN UNNEST" in out.upper()
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (id INT, c TEXT)",
            "INSERT INTO t VALUES (1, '[\"a\"]'), (2, '[]')",
        ],
    )
    # la ligne 2 (tableau vide) survit avec value NULL
    assert (2, None) in rows
    assert (1, "a") in rows


def test_flatten_left_join_lateral_is_outer():
    """`LEFT JOIN LATERAL FLATTEN(...)` implique aussi la sémantique outer."""
    out = _ptq(
        "SELECT t.id, x.value::STRING AS v "
        'FROM t LEFT JOIN LATERAL FLATTEN(input => PARSE_JSON(t."c")) x'
    )
    assert "LEFT JOIN UNNEST" in out.upper()
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (id INT, c TEXT)",
            "INSERT INTO t VALUES (1, '[\"a\"]'), (2, '[]')",
        ],
    )
    assert (2, None) in rows


def test_flatten_table_function_form_executes():
    """Forme `TABLE(FLATTEN(...))` → UNNEST (pas de `TABLE(` résiduel)."""
    out = _ptq(
        'SELECT ld.value::STRING AS v FROM t, TABLE(FLATTEN(PARSE_JSON(t."c"))) ld'
    )
    assert "UNNEST" in out.upper()
    assert "TABLE(" not in out.upper()
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (c TEXT)",
            'INSERT INTO t VALUES (\'["x","y"]\')',
        ],
    )
    assert sorted(r[0] for r in rows) == ["x", "y"]


def test_parse_json_field_string_cast_unquoted_without_flatten():
    """Correctif variant→string général : `PARSE_JSON(x):field::STRING` déquote
    même hors FLATTEN (cf. sf_bq412)."""
    out = _ptq('SELECT PARSE_JSON(t."d"):"reason"::STRING AS r FROM t')
    assert "->>" in out
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (d TEXT)",
            'INSERT INTO t VALUES (\'{"reason":"spam"}\')',
        ],
    )
    assert rows[0][0] == "spam"


def test_flatten_index_column_executes():
    """`f.index` référencé → forme LATERAL zippée value+index (0-based, cf. sf_bq216).

    L'ancienne reconstruction n'exposait que `value` → `Binder Error: Table "f"
    does not have a column named "index"`.
    """
    out = _ptq(
        "SELECT f.index AS i, f.value::STRING AS v "
        'FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c")) f'
    )
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (c TEXT)",
            'INSERT INTO t VALUES (\'["a","b","c"]\')',
        ],
    )
    # index Snowflake = position 0-based, zippée avec value
    assert sorted(rows) == [(0, "a"), (1, "b"), (2, "c")]


def test_flatten_index_join_dot_product_sf_bq216():
    """Jointure de deux FLATTEN sur `.index` (produit scalaire, forme sf_bq216)."""
    out = _ptq(
        "WITH tgt AS ("
        "  SELECT f.index, f.value::FLOAT AS v"
        '  FROM emb, LATERAL FLATTEN(input => emb."e") f'
        "  WHERE emb.\"pub\" = 'TARGET'"
        "), cand AS ("
        '  SELECT emb."pub" AS pub, f.index, f.value::FLOAT AS v'
        '  FROM emb, LATERAL FLATTEN(input => emb."e") f'
        "  WHERE emb.\"pub\" <> 'TARGET'"
        ") "
        "SELECT c.pub FROM cand c JOIN tgt t ON c.index = t.index "
        "GROUP BY c.pub ORDER BY SUM(c.v * t.v) DESC"
    )
    rows = _run_duck(
        out,
        [
            "CREATE TABLE emb (pub TEXT, e TEXT)",
            "INSERT INTO emb VALUES ('TARGET', '[1.0, 2.0]'), "
            "('A', '[1.0, 2.0]'), ('B', '[2.0, 0.0]')",
        ],
    )
    assert [r[0] for r in rows] == ["A", "B"]


def test_flatten_index_outer_preserves_empty_rows():
    """`.index` + `outer => TRUE` → LEFT JOIN LATERAL, ligne parent conservée."""
    out = _ptq(
        "SELECT t.id, f.index AS i, f.value::STRING AS v "
        'FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c"), outer => TRUE) f'
    )
    rows = _run_duck(
        out,
        [
            "CREATE TABLE t (id INT, c TEXT)",
            "INSERT INTO t VALUES (1, '[\"a\"]'), (2, '[]')",
        ],
    )
    assert (1, 0, "a") in rows
    assert (2, None, None) in rows


def test_flatten_without_index_keeps_simple_unnest():
    """Non-régression : `.index` absent → forme simple CROSS JOIN UNNEST inchangée
    (pas de sous-requête LATERAL inutile)."""
    out = _ptq(
        "SELECT x.value::STRING AS v "
        'FROM t, LATERAL FLATTEN(input => PARSE_JSON(t."c")) x'
    )
    assert "CROSS JOIN UNNEST" in out.upper()
    assert "LATERAL (" not in out.upper()


# ---------------------------------------------------------------------------
# VARIANT — exécution (sf_bq444) : DDL VARIANT→JSON + bracket 0-based
# ---------------------------------------------------------------------------


def test_variant_ddl_maps_to_json():
    """VARIANT/OBJECT → JSON DuckDB (accès 0-based + fail-fast INSERT), pas VARIANT nu."""
    assert _resolve_duck_type("VARIANT") == "JSON"
    assert _resolve_duck_type("OBJECT") == "JSON"
    cols = [{"name": "topics", "type": "VARIANT", "mode": "NULLABLE"}]
    assert _get_ddl_type("topics", cols) == "JSON"


def test_variant_column_created_as_json():
    con = duckdb.connect(":memory:")
    create_test_tables(
        tables=[
            {
                "table_name": "ce.logs",
                "database": "ce",
                "table": "logs",
                "columns": [{"name": "topics", "type": "VARIANT", "mode": "NULLABLE"}],
            }
        ],
        suffix="v1",
        overwrite=True,
        con=con,
        dialect="snowflake",
    )
    dtype = con.execute(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'ce_logs_v1' AND column_name = 'topics'"
    ).fetchone()[0]
    assert dtype == "JSON"


def test_variant_bracket_string_cast_zero_based():
    # Snowflake `col[0]::STRING` (0-based) → `col ->> 0` (déquoté, 0-based DuckDB JSON),
    # PAS `col[1]` (le +1 de sqlglot viserait le 2ᵉ élément sur une colonne JSON).
    out = _ptq('SELECT "topics"[0]::STRING AS x FROM t')
    assert "->> 0" in out
    assert "[1]" not in out


def test_variant_bracket_without_cast_zero_based():
    out = _ptq('SELECT "topics"[0] AS x FROM t')
    assert "-> 0" in out and "->> 0" not in out
    assert "[1]" not in out


def test_bracket_on_function_result_untouched():
    # Un bracket sur résultat de fonction transpile vers une liste native DuckDB
    # (1-based) : le +1 de sqlglot est correct, on n'y touche pas.
    out = _ptq("SELECT SPLIT(s, ',')[0] AS y FROM t")
    assert "[1]" in out  # 0-based Snowflake → 1-based liste native, inchangé
    assert "-> 0" not in out


def test_variant_bracket_roundtrip_first_element():
    # array JSON ["0xAAA","0xBBB"] → topics[0]::STRING doit rendre "0xAAA" (0-based).
    rows = _run_duck(
        _ptq('SELECT "topics"[0]::STRING AS first FROM ce_logs_v1'),
        [
            "CREATE TABLE ce_logs_v1 (topics JSON)",
            'INSERT INTO ce_logs_v1 VALUES (\'["0xAAA","0xBBB"]\')',
        ],
    )
    assert rows == [("0xAAA",)]


def test_variant_bare_string_insert_fails_fast_as_data_error():
    # Un INSERT de string non-JSON dans une colonne JSON échoue tôt → classé
    # bad_data_error (routé vers la boucle de correction), pas un NULL silencieux.
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE ce_logs_v1 (topics JSON)")
    with pytest.raises(Exception) as exc_info:
        con.execute("INSERT INTO ce_logs_v1 VALUES ('0xAAA')")
    assert _is_duckdb_data_error(exc_info.value)


# ---------------------------------------------------------------------------
# ARRAY_SIZE / ARRAY_LENGTH sur VARIANT/JSON → json_array_length (sf_bq091)
# ---------------------------------------------------------------------------


def test_array_size_on_variant_column_uses_json_array_length():
    # ARRAY_SIZE(col) Snowflake → array_length (sqlglot) qui n'existe pas sur JSON/VARIANT ;
    # on réécrit en json_array_length (couvre les deux).
    out = _ptq('SELECT ARRAY_SIZE(awm."assignee_harmonized") AS n FROM t AS awm')
    assert "JSON_ARRAY_LENGTH" in out.upper()
    # pas d'array_length NU (json_array_length contient array_length en sous-chaîne)
    assert "ARRAY_LENGTH(" not in out.upper().replace("JSON_ARRAY_LENGTH(", "")


def test_array_length_on_split_stays_native():
    # ARRAY_SIZE sur une vraie liste native (SPLIT) garde array_length (1-based natif OK).
    out = _ptq("SELECT ARRAY_SIZE(SPLIT(s, ',')) AS n FROM t")
    assert "JSON_ARRAY_LENGTH" not in out.upper()
    assert "LENGTH(" in out.upper()  # array_length / length natif


def test_array_size_json_column_executes():
    rows = _run_duck(
        _ptq('SELECT ARRAY_SIZE(t."arr") AS n FROM ce_arr AS t'),
        [
            "CREATE TABLE ce_arr (arr JSON)",
            "INSERT INTO ce_arr VALUES ('[1,2,3]'), ('[]')",
        ],
    )
    assert (3,) in rows and (0,) in rows


# ---------------------------------------------------------------------------
# TO_DATE(CAST(col AS VARCHAR), 'YYYYMMDD') sur colonne numérique (sf_bq216)
# ---------------------------------------------------------------------------


def test_numeric_date_parse_wraps_bigint():
    # Une colonne NUMBER→DECIMAL(38,9) rend '20160101.000000000' → STRPTIME('%Y%m%d')
    # casse. On intercale CAST(... AS BIGINT) pour supprimer le suffixe décimal.
    out = _ptq(
        "SELECT TO_DATE(CAST(p.\"filing_date\" AS VARCHAR), 'YYYYMMDD') AS d FROM t p"
    )
    assert "AS BIGINT)" in out.upper()


def test_separator_date_format_not_wrapped():
    # Format avec séparateur → la valeur est déjà une date formatée, pas un entier compact :
    # aucun cast BIGINT (qui casserait '2016-01-01').
    out = _ptq("SELECT TO_DATE(p.d, 'YYYY-MM-DD') AS d FROM t p")
    assert "AS BIGINT)" not in out.upper()


def test_numeric_date_parse_decimal_executes():
    rows = _run_duck(
        _ptq(
            'SELECT EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), '
            "'YYYYMMDD')) AS y FROM fd_tbl p"
        ),
        [
            'CREATE TABLE fd_tbl ("filing_date" DECIMAL(38, 9))',
            "INSERT INTO fd_tbl VALUES (20160101), (20150701)",
        ],
    )
    assert sorted(rows) == [(2015,), (2016,)]
