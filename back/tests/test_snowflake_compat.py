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
import sqlglot

from build_query.debug_executor import _quote_ident
from build_query.schema_fetcher import _sf_get, _sf_quote, _sf_snow_data_type
from utils.examples import (
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
    assert "HEXSTR_TO_DOUBLE(" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_chained_concat():
    """'0x' || a || b : DPipe imbriqué à gauche — le préfixe doit être trouvé en feuille."""
    out = _ptq("SELECT CAST('0x' || a || b AS FLOAT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE(" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_uppercase_prefix_and_try_cast():
    out = _ptq("SELECT TRY_CAST('0X' || h AS DOUBLE) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE(" in out.upper()


def test_hex_cast_concat_function_form():
    out = _ptq("SELECT CAST(CONCAT('0x', h) AS FLOAT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE(" in out.upper()


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
    return "HEXSTR_TO_DOUBLE(" in out and "'0X'" not in out


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
    assert "HEXSTR_TO_DOUBLE(" in out.upper()
    assert "'0x'" not in out.lower()


def test_hex_cast_to_int_target():
    out = _ptq("SELECT CAST('0x' || h AS INT) AS v FROM mydb.s.t")
    assert "HEXSTR_TO_DOUBLE(" in out.upper()
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
