"""Régression : _lightweight_query_decomposed (chemin CLI) doit produire un
`final_query` PARSABLE même quand la requête a une clause WITH.

Bug d'origine : `ctes.clear()` vidait la liste de CTE mais laissait le nœud `With`
en place → rendu `WITH \nSELECT …` (WITH orphelin) → ParseError sur le code de
`final_query`. Conséquence pour le path-slicing : le UNION ALL de la requête finale
devenait invisible (parse=None), et le slicer se rabattait à tort sur un UNION ALL
interne à une CTE.

Décision produit (post-fix) : quand une CTE ET la requête finale portent chacune un
UNION ALL de 1er niveau, la requête finale est prioritaire (ses branches sont les
vraies sorties) — voir _find_host.
"""

import json

import sqlglot

from build_query.path_slicer import _strip_with, build_path_plans, list_union_paths
from build_query.query_chain import _lightweight_query_decomposed

DIALECT = "bigquery"

# Deux UNION ALL de 1er niveau : un dans la CTE `inner_u`, un dans la requête finale.
MULTI_UNION_SQL = """
WITH inner_u AS (
  SELECT a FROM t1
  UNION ALL
  SELECT a FROM t2
),
agg AS (
  SELECT a FROM inner_u
)
SELECT a FROM agg
UNION ALL
SELECT a FROM t3
"""


def _final_query_node(sql: str) -> dict:
    qd = json.loads(_lightweight_query_decomposed(sql, DIALECT) or "[]")
    return next(n for n in qd if n["name"] == "final_query")


def test_final_query_code_is_parseable():
    """Le code du nœud final_query ne doit pas commencer par un WITH orphelin."""
    node = _final_query_node(MULTI_UNION_SQL)
    # Ne doit pas lever : avant le fix → ParseError "Expected CTE to have alias".
    sqlglot.parse_one(node["code"], read=DIALECT)


def test_final_query_code_has_no_cte_clause():
    """Le code de final_query doit être le SELECT final SEUL — la clause WITH (et donc
    les définitions de CTE) doit avoir disparu. Sinon le DAG de cte_graph y voit
    chaque CTE définie comme une dépendance directe de final_query (régression de
    la clé `with` vs `with_` selon la version de sqlglot)."""
    sql = (
        "WITH a AS (SELECT 1 AS x FROM base_table), "
        "b AS (SELECT x FROM a), "
        "c AS (SELECT x FROM b JOIN a USING (x)) SELECT * FROM c"
    )
    node = _final_query_node(sql)
    parsed = sqlglot.parse_one(node["code"], read=DIALECT)
    assert parsed.ctes == [], f"final_query porte encore un WITH : {node['code']!r}"


def test_final_union_takes_priority_over_cte_union():
    """CTE union (inner_u) + union finale → la requête finale est prioritaire :
    on focalise ses branches (t1-via-agg / t3), pas l'union interne à la CTE."""
    qd = json.loads(_lightweight_query_decomposed(MULTI_UNION_SQL, DIALECT) or "[]")
    paths = list_union_paths(qd, DIALECT)
    assert paths, (
        "le UNION ALL final doit être focalisable (régression du WITH orphelin)"
    )
    assert all(p.host_cte == "final_query" for p in paths)
    assert len(paths) == 2
    plans = build_path_plans(MULTI_UNION_SQL, qd, [], DIALECT)
    assert plans is not None
    # Le CORPS de la requête finale ne doit plus être un set-op (l'union finale est
    # slicée) — une CTE conservée (inner_u) peut, elle, garder son propre UNION ALL.
    for name, plan in plans.items():
        if name == "all":
            continue
        body = _strip_with(sqlglot.parse_one(plan["sliced_sql"], read=DIALECT))
        assert not isinstance(body, sqlglot.exp.Union)


def test_single_final_union_still_focuses():
    """Garde-fou : une seule union (la finale) reste focalisable après le fix."""
    sql = """
    WITH base AS (SELECT a FROM t1)
    SELECT a FROM base
    UNION ALL
    SELECT a FROM t2
    """
    qd = json.loads(_lightweight_query_decomposed(sql, DIALECT) or "[]")
    paths = list_union_paths(qd, DIALECT)
    assert len(paths) == 2
    assert all(p.host_cte == "final_query" for p in paths)
