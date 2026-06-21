"""Sélection du catalogue de pièges (`_select_pitfalls`) selon les constructions SQL.

Couvre les fixes :
- #2 : ``CROSS/LEFT JOIN UNNEST(...)`` ne doit PAS déclencher la section JOINs
  relationnels (fan-out / clé non-unique) — c'est un aplatissement de tableau, pas
  une jointure entre tables.
- #3 : ``GROUP BY ROLLUP/CUBE/GROUPING SETS`` doit déclencher la section dédiée aux
  agrégats multi-niveaux (lignes de sous-total / total global).
"""

from build_query.suggestions_node import (
    _PITFALL_GROUPING,
    _PITFALL_JOINS,
    _select_pitfalls,
)


# Les requêtes ci-dessous incluent un agrégat pour que `sections` ne soit pas vide :
# sinon `_select_pitfalls` retombe sur le catalogue COMPLET (filet anti-perte de
# couverture), ce qui réintroduirait la section JOINs indépendamment du fix #2.
def test_cross_join_unnest_does_not_trigger_join_pitfalls():
    sql = """
        SELECT t.id, COUNT(item) AS n
        FROM my_table AS t
        CROSS JOIN UNNEST(t.items) AS item
        GROUP BY t.id
    """
    out = _select_pitfalls(sql, "bigquery")
    assert "Fan-out silencieux" not in out
    assert _PITFALL_JOINS not in out


def test_left_join_unnest_with_offset_does_not_trigger_join_pitfalls():
    sql = """
        SELECT t.id, COUNT(item) AS n
        FROM my_table AS t
        LEFT JOIN UNNEST(t.items) AS item WITH OFFSET AS pos
        GROUP BY t.id
    """
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_JOINS not in out


def test_real_table_join_still_triggers_join_pitfalls():
    sql = """
        SELECT a.id, b.label
        FROM facts AS a
        JOIN dim AS b ON a.dim_id = b.id
    """
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_JOINS in out


def test_unnest_plus_real_join_still_triggers_join_pitfalls():
    """Un UNNEST ne doit pas masquer un vrai join présent par ailleurs."""
    sql = """
        SELECT a.id, b.label, item
        FROM facts AS a
        JOIN dim AS b ON a.dim_id = b.id
        CROSS JOIN UNNEST(a.items) AS item
    """
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_JOINS in out


def test_rollup_triggers_grouping_pitfalls():
    sql = "SELECT region, SUM(amount) FROM sales GROUP BY ROLLUP(region)"
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_GROUPING in out


def test_cube_triggers_grouping_pitfalls():
    sql = (
        "SELECT region, product, SUM(amount) FROM sales GROUP BY CUBE(region, product)"
    )
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_GROUPING in out


def test_grouping_sets_triggers_grouping_pitfalls():
    sql = (
        "SELECT region, product, SUM(amount) FROM sales "
        "GROUP BY GROUPING SETS ((region), (product))"
    )
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_GROUPING in out


def test_plain_group_by_does_not_trigger_grouping_pitfalls():
    sql = "SELECT region, SUM(amount) FROM sales GROUP BY region"
    out = _select_pitfalls(sql, "bigquery")
    assert _PITFALL_GROUPING not in out


def test_unparsable_sql_falls_back_to_full_catalog():
    out = _select_pitfalls("this is not sql @@@", "bigquery")
    assert _PITFALL_JOINS in out
    assert _PITFALL_GROUPING in out
