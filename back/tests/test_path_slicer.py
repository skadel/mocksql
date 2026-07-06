"""Tests du path-slicer UNION ALL (génération focalisée par branche).

Test-before-fix : écrits avant le câblage. Fixtures exécutables sur DuckDB pour
vérifier que le SQL slicé reste valide.
"""

import duckdb
import pytest
import sqlglot

import json

from build_query.path_slicer import (
    build_path_plans,
    list_all_union_branches,
    list_union_paths,
    prune_dead_projections,
    resolve_active_sql,
    slice_to_path,
    slice_used_columns,
)


def _decompose(sql: str, dialect: str = "duckdb") -> list[dict]:
    """Mini-décomposition {name, code} équivalente à validator.split_query.

    Suffisant pour les fonctions pures du path_slicer (qui n'utilisent que
    name + code). Le dernier nœud est `final_query`.
    """
    parsed = sqlglot.parse_one(sql, read=dialect)
    nodes: list[dict] = []
    for cte in parsed.ctes:
        nodes.append({"name": cte.alias_or_name, "code": cte.this.sql(dialect=dialect)})
    body = parsed.copy()
    body.args.pop("with", None)
    body.args.pop("with_", None)
    nodes.append({"name": "final_query", "code": body.sql(dialect=dialect)})
    return nodes


# --- Fixtures (exécutables sur DuckDB, sans tables externes) --------------------------

# UNION ALL dans une CTE, 3 branches lisant des CTE propres disjointes
THREE_BRANCH = """
WITH a AS (SELECT 1 AS id),
     b AS (SELECT 2 AS id),
     c AS (SELECT 3 AS id),
     combined AS (
         SELECT id, 'a' AS kind FROM a
         UNION ALL
         SELECT id, 'b' AS kind FROM b
         UNION ALL
         SELECT id, 'c' AS kind FROM c
     )
SELECT id, kind FROM combined ORDER BY id
"""

# UNION ALL au niveau de la requête finale
FINAL_UNION = """
WITH a AS (SELECT 1 AS id),
     b AS (SELECT 2 AS id)
SELECT id FROM a
UNION ALL
SELECT id FROM b
"""

# Pas d'UNION ALL
NO_UNION = """
WITH a AS (SELECT 1 AS id)
SELECT id FROM a
"""

# UNION imbriqué dans une sous-requête FROM (pas un set-op de 1er niveau)
NESTED_UNION = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT t.id FROM (SELECT id FROM a UNION ALL SELECT id FROM b) AS t
"""

# UNION DISTINCT (pas UNION ALL)
UNION_DISTINCT = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT id FROM a
UNION
SELECT id FROM b
"""

# Deux CTE chacune avec un UNION ALL de 1er niveau (ambigu → path all)
TWO_UNIONS = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id),
     u1 AS (SELECT id FROM a UNION ALL SELECT id FROM b),
     u2 AS (SELECT id FROM a UNION ALL SELECT id FROM b)
SELECT u1.id FROM u1 JOIN u2 USING (id)
"""

# Transpose c6 : 6 branches partageant le MÊME FROM (`filter_high_scores`, UNNEST), ne
# différant que par la constante projetée `'<x>' AS indicator` (le tag discriminant).
# Nommer par la source du FROM donnerait `filter_high_scores` × 6 (→ branch_2…6) ; il
# faut nommer par la valeur du tag. (Parsé en bigquery : UNNEST style BigQuery.)
TRANSPOSE_UNION = """
WITH filter_high_scores AS (
    SELECT 'E1' AS entity, [1.0, 2.0, 3.0] AS nb_ope_lags
),
res AS (
    SELECT entity, 'nb_ope' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
    UNION ALL
    SELECT entity, 'ouvertures' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
    UNION ALL
    SELECT entity, 'fermetures' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
    UNION ALL
    SELECT entity, 'parc' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
    UNION ALL
    SELECT entity, 'parc_actifs' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
    UNION ALL
    SELECT entity, 'mt_ope' AS indicator, val AS value
    FROM filter_high_scores, UNNEST(nb_ope_lags) AS val
)
SELECT * FROM res
"""

# Branches sur des tables DIFFÉRENTES, sans tag constant → nommage par source du FROM
# (régression : le tag ne doit pas écraser le comportement existant).
HETERO_NO_TAG = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id), c AS (SELECT 3 AS id)
SELECT id FROM a
UNION ALL
SELECT id FROM b
UNION ALL
SELECT id FROM c
"""

# Branches sans tag constant ET sans table exploitable au FROM (sous-requête aliasée) →
# fallback branch_N.
NO_TAG_NO_TABLE = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT x.id FROM (SELECT id FROM a) AS x
UNION ALL
SELECT y.id FROM (SELECT id FROM b) AS y
"""

# Constante projetée présente dans toutes les branches mais IDENTIQUE → pas discriminante
# (il faut ≥2 valeurs distinctes) → retombe sur la source du FROM.
SAME_TAG = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id)
SELECT id, 'X' AS kind FROM a
UNION ALL
SELECT id, 'X' AS kind FROM b
"""

# Union interne UNIQUE dont le résultat est agrégé cross-branches en aval (sf_bq012 :
# all_flows → net_balances `SUM … GROUP BY addr` → sortie). Slicer une branche fausserait
# l'agrégat du script complet (les branches partagent la clé `addr` et se compensent) →
# NON sliçable : le système doit retomber sur le path `all` (toutes les tables peuplées).
AGG_CONSUMER_UNION = """
WITH a AS (SELECT 1 AS addr, 10 AS amt),
     b AS (SELECT 1 AS addr, -4 AS amt),
     flows AS (
         SELECT addr, amt FROM a
         UNION ALL
         SELECT addr, amt FROM b
     ),
     net AS (
         SELECT addr, SUM(amt) AS bal FROM flows GROUP BY addr
     )
SELECT AVG(bal) AS avg_bal FROM net
"""

# Union interne UNIQUE consommée par un simple passthrough (aucune agrégation) → RESTE
# sliçable : les lignes de chaque branche traversent jusqu'à la sortie sans fusion.
PASSTHROUGH_CONSUMER_UNION = """
WITH a AS (SELECT 1 AS id), b AS (SELECT 2 AS id),
     u AS (
         SELECT id FROM a
         UNION ALL
         SELECT id FROM b
     ),
     renamed AS (SELECT id FROM u)
SELECT id FROM renamed
"""


# --- list_union_paths -----------------------------------------------------------------


def test_list_union_paths_three_branches():
    paths = list_union_paths(_decompose(THREE_BRANCH))
    assert [p.name for p in paths] == ["a", "b", "c"]
    assert [p.branch_index for p in paths] == [0, 1, 2]
    assert {p.host_cte for p in paths} == {"combined"}


def test_list_union_paths_final_query_host():
    paths = list_union_paths(_decompose(FINAL_UNION))
    assert [p.name for p in paths] == ["a", "b"]
    assert {p.host_cte for p in paths} == {"final_query"}


def test_list_union_paths_no_union():
    assert list_union_paths(_decompose(NO_UNION)) == []


def test_list_union_paths_nested_union_bails():
    assert list_union_paths(_decompose(NESTED_UNION)) == []


def test_list_union_paths_union_distinct_bails():
    assert list_union_paths(_decompose(UNION_DISTINCT)) == []


def test_list_union_paths_multiple_unions_bails():
    assert list_union_paths(_decompose(TWO_UNIONS)) == []


def test_list_union_paths_bails_on_aggregating_consumer():
    """Union interne agrégée cross-branches en aval (sf_bq012) → bail : slicer une
    branche fausserait l'agrégat du script complet."""
    assert list_union_paths(_decompose(AGG_CONSUMER_UNION)) == []


def test_build_path_plans_none_on_aggregating_consumer():
    plans = build_path_plans(
        AGG_CONSUMER_UNION, _decompose(AGG_CONSUMER_UNION), [], "duckdb"
    )
    assert plans is None


def test_list_union_paths_sliceable_through_passthrough():
    """Union interne consommée par un passthrough non-agrégeant → reste sliçable."""
    paths = list_union_paths(_decompose(PASSTHROUGH_CONSUMER_UNION))
    assert [p.name for p in paths] == ["a", "b"]
    assert {p.host_cte for p in paths} == {"u"}


# --- Inventaire de branches (P1-2 : repli quand le slicer bail) -----------------------


def test_inventory_names_branches_when_slicer_bails():
    """TWO_UNIONS : deux UNION internes, aucune union finale → `list_union_paths` bail ([]),
    mais l'inventaire nomme quand même les branches de CHAQUE hôte (c2 : MEG/FERMETURE)."""
    decomposed = _decompose(TWO_UNIONS)
    assert list_union_paths(decomposed) == []  # rappel : le slicer ne peut pas cibler
    inv = list_all_union_branches(decomposed)
    assert {h for h, _ in inv} == {"u1", "u2"}
    assert all(names == ["a", "b"] for _, names in inv)


def test_inventory_names_branches_by_discriminant_tag():
    inv = list_all_union_branches(_decompose(TRANSPOSE_UNION, "bigquery"), "bigquery")
    assert len(inv) == 1
    host, names = inv[0]
    assert host == "res"
    assert names == [
        "nb_ope",
        "ouvertures",
        "fermetures",
        "parc",
        "parc_actifs",
        "mt_ope",
    ]


def test_inventory_empty_without_union():
    assert list_all_union_branches(_decompose(NO_UNION)) == []


def test_inventory_ignores_union_distinct_and_buried_subquery():
    # Cohérent avec le scope du slicer : ni UNION DISTINCT, ni union enterrée en sous-requête.
    assert list_all_union_branches(_decompose(UNION_DISTINCT)) == []
    assert list_all_union_branches(_decompose(NESTED_UNION)) == []


# --- SPEC 2 : nommage de branche par tag constant discriminant -----------------------


def test_transpose_branches_named_by_discriminant_tag():
    # 6 branches, même FROM `filter_high_scores` → le tag `indicator` doit nommer.
    paths = list_union_paths(_decompose(TRANSPOSE_UNION, "bigquery"), "bigquery")
    names = [p.name for p in paths]
    assert names == [
        "nb_ope",
        "ouvertures",
        "fermetures",
        "parc",
        "parc_actifs",
        "mt_ope",
    ]
    assert not any(n.startswith("branch_") for n in names)
    assert "filter_high_scores" not in names
    # target_path persisté = le tag → titre `[Focus <tag>]` (examples_generator).
    assert paths[0].name == "nb_ope"


def test_from_heterogeneous_naming_unchanged():
    # Tables différentes, aucun tag constant → nommage par source du FROM (régression).
    paths = list_union_paths(_decompose(HETERO_NO_TAG))
    assert [p.name for p in paths] == ["a", "b", "c"]


def test_no_tag_no_table_falls_to_branch_n():
    paths = list_union_paths(_decompose(NO_TAG_NO_TABLE))
    assert [p.name for p in paths] == ["branch_1", "branch_2"]


def test_identical_constant_is_not_discriminant():
    # 'X' AS kind partout (1 seule valeur) → pas un tag → source du FROM.
    paths = list_union_paths(_decompose(SAME_TAG))
    assert [p.name for p in paths] == ["a", "b"]


def test_build_path_plans_keys_are_tags_for_transpose():
    plans = build_path_plans(
        TRANSPOSE_UNION, _decompose(TRANSPOSE_UNION, "bigquery"), [], "bigquery"
    )
    assert set(plans) == {
        "nb_ope",
        "ouvertures",
        "fermetures",
        "parc",
        "parc_actifs",
        "mt_ope",
        "all",
    }


# --- slice_to_path --------------------------------------------------------------------


def test_slice_to_path_keeps_only_target_branch_and_prunes_orphans():
    sliced = slice_to_path(THREE_BRANCH, "a", _decompose(THREE_BRANCH), "duckdb")
    parsed = sqlglot.parse_one(sliced, read="duckdb")
    # Plus aucun set-op
    assert not list(parsed.find_all(sqlglot.exp.Union))
    # CTE orphelines b et c retirées du WITH ; a et combined conservées
    cte_names = {c.alias_or_name for c in parsed.ctes}
    assert "a" in cte_names and "combined" in cte_names
    assert "b" not in cte_names and "c" not in cte_names


def test_slice_to_path_executable_on_duckdb():
    sliced = slice_to_path(THREE_BRANCH, "b", _decompose(THREE_BRANCH), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(2, "b")]


def test_slice_to_path_final_query_host():
    sliced = slice_to_path(FINAL_UNION, "a", _decompose(FINAL_UNION), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(1,)]
    assert not list(
        sqlglot.parse_one(sliced, read="duckdb").find_all(sqlglot.exp.Union)
    )


def test_slice_to_path_unknown_path_raises():
    with pytest.raises((KeyError, ValueError)):
        slice_to_path(THREE_BRANCH, "zzz", _decompose(THREE_BRANCH), "duckdb")


def test_slice_to_path_all_is_noop():
    # "all" et None → SQL inchangé sémantiquement (toutes les branches)
    sliced = slice_to_path(THREE_BRANCH, "all", _decompose(THREE_BRANCH), "duckdb")
    rows = duckdb.sql(sliced).fetchall()
    assert rows == [(1, "a"), (2, "b"), (3, "c")]


# --- slice_used_columns ---------------------------------------------------------------


def test_slice_used_columns_drops_other_branch_tables():
    # used_columns sur des tables de base ; après slice sur la branche 'a',
    # seules les tables encore référencées survivent.
    used = [
        {"project": "p", "database": "d", "table": "a", "used_columns": ["id"]},
        {"project": "p", "database": "d", "table": "b", "used_columns": ["id"]},
        {"project": "p", "database": "d", "table": "c", "used_columns": ["id"]},
    ]
    sliced = slice_to_path(THREE_BRANCH, "a", _decompose(THREE_BRANCH), "duckdb")
    reduced = slice_used_columns(used, sliced)
    tables = {e["table"] for e in reduced}
    assert "a" in tables
    assert "b" not in tables and "c" not in tables
    assert len(reduced) < len(used)


# --- build_path_plans -----------------------------------------------------------------


def test_build_path_plans_three_branches_plus_all():
    used = [
        {"table": "a", "used_columns": ["id"]},
        {"table": "b", "used_columns": ["id"]},
        {"table": "c", "used_columns": ["id"]},
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    assert set(plans) == {"a", "b", "c", "all"}
    # path 'a' : SQL slicé exécutable, used_columns réduit
    assert duckdb.sql(plans["a"]["sliced_sql"]).fetchall() == [(1, "a")]
    assert {e["table"] for e in plans["a"]["used_columns"]} == {"a"}
    # 'all' : SQL complet + used_columns complet
    assert plans["all"]["sliced_sql"] == THREE_BRANCH
    assert len(plans["all"]["used_columns"]) == 3


def test_build_path_plans_none_without_union():
    assert build_path_plans(NO_UNION, _decompose(NO_UNION), [], "duckdb") is None


def test_build_path_plans_tolerates_json_string_used_columns():
    # En CLI, state["used_columns"] est une liste de strings JSON (pas de dicts).
    # build_path_plans doit normaliser sans planter (régression e2e bq002).
    used = [
        json.dumps({"table": "a", "used_columns": ["id"]}),
        json.dumps({"table": "b", "used_columns": ["id"]}),
        json.dumps({"table": "c", "used_columns": ["id"]}),
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    assert set(plans) == {"a", "b", "c", "all"}
    assert {e["table"] for e in plans["a"]["used_columns"]} == {"a"}


# --- resolve_active_sql ---------------------------------------------------------------


def test_resolve_active_sql_returns_sliced_for_target_path():
    used = [
        {"table": "a", "used_columns": ["id"]},
        {"table": "b", "used_columns": ["id"]},
    ]
    plans = build_path_plans(THREE_BRANCH, _decompose(THREE_BRANCH), used, "duckdb")
    state = {
        "optimized_sql": THREE_BRANCH,
        "used_columns": [json.dumps(u) for u in used],
        "path_plans": json.dumps(plans),
        "target_path": "b",
    }
    sql, uc = resolve_active_sql(state)
    assert duckdb.sql(sql).fetchall() == [(2, "b")]
    assert {e["table"] for e in uc} == {"b"}


def test_resolve_active_sql_falls_back_to_full_for_all_or_missing():
    used = [{"table": "a", "used_columns": ["id"]}]
    base = {
        "optimized_sql": THREE_BRANCH,
        "used_columns": [json.dumps(u) for u in used],
        "path_plans": None,
    }
    # target "all" → complet
    sql, _ = resolve_active_sql({**base, "target_path": "all"})
    assert sql == THREE_BRANCH
    # target None → complet
    sql, _ = resolve_active_sql({**base, "target_path": None})
    assert sql == THREE_BRANCH
    # path inconnu + catalogue absent → complet (défensif)
    sql, _ = resolve_active_sql({**base, "target_path": "zzz"})
    assert sql == THREE_BRANCH


# --- SPEC 3 : pruning des projections mortes du SQL slicé -----------------------------

# Branche `nb_ope` d'un transpose c6-like : 6 arrays de lags en amont, mais seules
# `nb_ope_lags` (projetée) et `parc_lags` (filtre ARRAY_LENGTH) sont vivantes.
_C6_LIKE_BRANCH = """
WITH base AS (
  SELECT id, nb_ope_lags, ouvertures_lags, fermetures_lags, parc_lags, parc_actifs_lags, mt_ope_lags
  FROM proj.ds.metrics
),
filtered AS (
  SELECT * FROM base WHERE ARRAY_LENGTH(parc_lags) >= 2
),
nb_ope_branch AS (
  SELECT id, 'nb_ope' AS indicator, nb_ope_lags AS lags FROM filtered
)
SELECT id, indicator, lags FROM nb_ope_branch
"""

_C6_LIKE_SCHEMA = [
    {
        "table_name": "proj.ds.metrics",
        "columns": [
            {"name": "id", "type": "INT64"},
            {"name": "nb_ope_lags", "type": "ARRAY<FLOAT64>"},
            {"name": "ouvertures_lags", "type": "ARRAY<FLOAT64>"},
            {"name": "fermetures_lags", "type": "ARRAY<FLOAT64>"},
            {"name": "parc_lags", "type": "ARRAY<FLOAT64>"},
            {"name": "parc_actifs_lags", "type": "ARRAY<FLOAT64>"},
            {"name": "mt_ope_lags", "type": "ARRAY<FLOAT64>"},
        ],
    }
]


def test_prune_drops_dead_sibling_arrays_keeps_used_and_filtered():
    pruned = prune_dead_projections(_C6_LIKE_BRANCH, _C6_LIKE_SCHEMA, "bigquery")
    # Vivantes : projetée + filtre ARRAY_LENGTH.
    assert "nb_ope_lags" in pruned
    assert "parc_lags" in pruned
    # Sœurs mortes retirées.
    for dead in (
        "ouvertures_lags",
        "fermetures_lags",
        "parc_actifs_lags",
        "mt_ope_lags",
    ):
        assert dead not in pruned, f"{dead} aurait dû être pruné"


def test_prune_without_schema_is_noop():
    # Sans schéma, `SELECT *` n'est pas résolu → renvoyé inchangé (pas de pruning hasardeux).
    assert prune_dead_projections(_C6_LIKE_BRANCH, None, "bigquery") == _C6_LIKE_BRANCH


def test_prune_unparseable_is_noop():
    junk = "SELECT FROM WHERE )("
    assert prune_dead_projections(junk, _C6_LIKE_SCHEMA, "bigquery") == junk


def test_prune_empty_sql_is_noop():
    assert prune_dead_projections("", _C6_LIKE_SCHEMA, "bigquery") == ""


# --- régression -----------------------------------------------------------------------


def test_no_union_query_unaffected():
    assert list_union_paths(_decompose(NO_UNION)) == []
