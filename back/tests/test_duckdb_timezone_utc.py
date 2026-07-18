"""Fuseau horaire DuckDB : toute connexion MockSQL travaille en UTC.

Incident (debug sf_bq263, spider2-snow) : ``TO_TIMESTAMP_NTZ(epoch)`` — naïf en
Snowflake — est transpilé en ``TO_TIMESTAMP()`` DuckDB, qui rend un TIMESTAMPTZ
dans le fuseau de la session, hérité de la machine (Europe/Paris). Symptômes :
« May 2023 » sérialisé ``2023-04-30T22:00:00Z`` (pandas ``to_json`` reconvertit
en UTC), bornes ``WHERE >= '2023-01-01'`` décalées de 2 h, et l'assertion
generator qui émet des assertions défensives en OR (``… = '2023-04-30' OR … =
'2023-05-01'``) qui laisseraient passer un vrai bug de bornes.

Fix : ``SET TimeZone='UTC'`` sur toute connexion préparée
(``open_duckdb_connection``) et sur les connexions d'évaluation du
``scalar_folder`` — un littéral foldé ne doit pas dépendre du fuseau machine.
"""

import sqlglot

from build_query import scalar_folder
from storage import config

_MAY_5_2023_UTC_EPOCH = 1683244800  # 2023-05-05T00:00:00Z
_APR_30_2023_LAST_SEC_EPOCH = 1682899199  # 2023-04-30T23:59:59Z


def _utc_conn(monkeypatch):
    monkeypatch.setattr(config, "get_duckdb_extensions", lambda: [])
    return config.open_duckdb_connection(":memory:")


def test_open_duckdb_connection_session_timezone_is_utc(monkeypatch):
    """La factory fige le fuseau de session, quel que soit celui de la machine."""
    con = _utc_conn(monkeypatch)
    try:
        assert con.execute("SELECT current_setting('TimeZone')").fetchone()[0] == "UTC"
    finally:
        con.close()


def test_epoch_month_trunc_serializes_first_of_month(monkeypatch):
    """Pattern sf_bq263 : epoch → TO_TIMESTAMP → DATE_TRUNC('MONTH') doit rendre
    le 1ᵉʳ du mois à minuit UTC — pas 2023-04-30T22:00:00Z (session Paris)."""
    con = _utc_conn(monkeypatch)
    try:
        df = con.execute(
            f"SELECT DATE_TRUNC('MONTH', TO_TIMESTAMP({_MAY_5_2023_UTC_EPOCH}))"
            " AS month"
        ).fetchdf()
        # Même sérialisation que l'executor (examples_executor: res.to_json).
        payload = df.to_json(orient="records", date_format="iso", date_unit="s")
        assert "2023-05-01T00:00:00" in payload
        assert "2023-04-30" not in payload
    finally:
        con.close()


def test_where_epoch_bound_not_shifted(monkeypatch):
    """1682899199 = 2023-04-30T23:59:59Z : EXCLU par ``>= '2023-05-01'``.

    En session Europe/Paris la borne devient 2023-04-30T22:00:00Z et la ligne
    passait à tort (décalage de 2 h sur tous les filtres de date)."""
    con = _utc_conn(monkeypatch)
    try:
        rows = con.execute(
            f"SELECT * FROM (SELECT TO_TIMESTAMP({_APR_30_2023_LAST_SEC_EPOCH})"
            " AS ts) WHERE ts >= TIMESTAMP '2023-05-01'"
        ).fetchall()
        assert rows == []
    finally:
        con.close()


def test_scalar_fold_timestamp_literal_is_utc():
    """Un littéral foldé (TIMESTAMPTZ pur, aucune colonne) ne dépend pas du
    fuseau machine : le scalar_folder évalue sur ses propres connexions
    in-memory, qui doivent elles aussi être en UTC."""
    ast = sqlglot.parse_one(
        "SELECT DATE_TRUNC('MONTH',"
        " CAST('2023-05-05 00:00:00+00:00' AS TIMESTAMPTZ)) AS m",
        read="duckdb",
    )
    folded = scalar_folder.fold_scalar_expressions(ast, "duckdb")
    rendered = folded.sql(dialect="duckdb")
    assert "2023-05-01 00:00:00+00" in rendered, rendered


def test_eval_with_duckdb_helper_is_utc():
    """Le helper unitaire d'évaluation du scalar_folder suit la même règle."""
    value = scalar_folder._eval_with_duckdb(
        f"DATE_TRUNC('MONTH', TO_TIMESTAMP({_MAY_5_2023_UTC_EPOCH}))"
    )
    assert str(value) == "2023-05-01 00:00:00+00:00"
