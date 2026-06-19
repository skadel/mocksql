"""Tests des helpers AST tolérants aux clés (utils/sqlglot_ast)."""

import sqlglot
from sqlglot import exp

from utils.sqlglot_ast import get_from, pop_with, set_from, strip_with

DIALECT = "bigquery"


def test_get_from_returns_from_node():
    parsed = sqlglot.parse_one("SELECT * FROM t", read=DIALECT)
    from_ = get_from(parsed)
    assert isinstance(from_, exp.From)
    assert next(from_.find_all(exp.Table)).name == "t"


def test_get_from_none_when_absent():
    parsed = sqlglot.parse_one("SELECT 1", read=DIALECT)
    assert get_from(parsed) is None


def test_set_from_renders_clause():
    """Régression du bug latent : set('from', …) sur sqlglot ≥ 30 ne rendait rien."""
    select = exp.Select()
    select.set("expressions", [exp.Star()])
    from_node = exp.From(this=exp.Table(this=exp.Identifier(this="t")))
    set_from(select, from_node)
    sql = select.sql(dialect=DIALECT)
    assert "FROM t" in sql, sql


def test_set_from_then_get_from_roundtrip():
    select = exp.Select()
    select.set("expressions", [exp.Star()])
    from_node = sqlglot.parse_one("SELECT * FROM t", read=DIALECT).args.get(
        "from"
    ) or sqlglot.parse_one("SELECT * FROM t", read=DIALECT).args.get("from_")
    set_from(select, from_node)
    assert get_from(select) is not None


def test_pop_with_removes_cte_clause():
    parsed = sqlglot.parse_one("WITH a AS (SELECT 1) SELECT * FROM a", read=DIALECT)
    pop_with(parsed)
    assert parsed.ctes == []
    # Le SQL rendu ne porte plus de WITH (et reste parsable).
    sql = parsed.sql(dialect=DIALECT)
    assert "WITH" not in sql.upper()
    sqlglot.parse_one(sql, read=DIALECT)


def test_strip_with_does_not_mutate_original():
    parsed = sqlglot.parse_one("WITH a AS (SELECT 1) SELECT * FROM a", read=DIALECT)
    stripped = strip_with(parsed)
    assert stripped.ctes == []
    assert parsed.ctes != []  # l'original est intact
