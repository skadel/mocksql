"""
P1c — Diagnostic par prédicat de JOIN (`_run_join_predicate_breakdown`).

Incident du 2026-06-11 : la trace CTE affichait
``+ JOIN (corr_cartes.cd_chef_file IS NOT NULL) → 0 ligne(s)`` alors que le
prédicat réellement bloquant était l'égalité sur ``code_produit_bpce_ps`` —
l'agent a patché la mauvaise colonne, désignée par le diagnostic lui-même.
Le breakdown évalue chaque prédicat du ON indépendamment sur les données
réelles et nomme le prédicat fautif avec les ensembles de valeurs des deux côtés.
"""

import duckdb
import pytest

import sqlglot

from build_query.examples_executor import (
    _extract_eq_subquery_filters,
    _run_cte_trace,
    _run_join_predicate_breakdown,
    _run_scalar_filter_breakdown,
)
from build_query.examples_generator import _format_cte_trace_hint

_SUFFIX = "t1"

_CTES = [
    {
        "name": "corr",
        "code": (
            "SELECT cartes.code_produit AS code_produit, "
            "cartes.cd_chef_file AS cd_chef_file FROM proj.ds.cartes AS cartes"
        ),
    },
    {
        "name": "temp_carte",
        "code": (
            "SELECT corr.code_produit AS cp FROM corr AS corr "
            "JOIN proj.ds.ref_port AS rp ON corr.cd_chef_file = rp.cd_chef_file "
            "AND corr.code_produit = rp.cd_type_carte_smp"
        ),
    },
    {
        "name": "final_query",
        "code": "SELECT temp_carte.cp FROM temp_carte AS temp_carte",
    },
]


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute(
        f"CREATE TABLE ds_cartes_{_SUFFIX} (code_produit TEXT, cd_chef_file TEXT)"
    )
    c.execute(f"INSERT INTO ds_cartes_{_SUFFIX} VALUES ('PROD1', '1'), ('PROD2', '1')")
    c.execute(
        f"CREATE TABLE ds_ref_port_{_SUFFIX} (cd_chef_file TEXT, cd_type_carte_smp TEXT)"
    )
    c.execute(f"INSERT INTO ds_ref_port_{_SUFFIX} VALUES ('1', 'ROD')")
    return c


@pytest.mark.asyncio
async def test_breakdown_names_the_actual_blocking_predicate(con):
    lines = await _run_join_predicate_breakdown(
        _CTES, 1, _SUFFIX, "proj", "bigquery", con
    )
    text = "\n".join(lines)
    # le prédicat sur cd_chef_file matche (1 valeur commune) — PAS bloquant
    assert "cd_chef_file" in text
    # le prédicat fautif est nommé, avec les ensembles des deux côtés
    blocking = [ln for ln in lines if "BLOQUANT" in ln]
    assert len(blocking) == 1
    assert "cd_type_carte_smp" in blocking[0]
    assert "ROD" in blocking[0]
    assert "PROD1" in blocking[0] and "PROD2" in blocking[0]
    # le prédicat qui matche n'est pas marqué bloquant
    matching = [ln for ln in lines if "cd_chef_file = " in ln and "BLOQUANT" not in ln]
    assert matching


@pytest.mark.asyncio
async def test_cte_trace_errors_carry_message_and_sql(con):
    ctes = [
        {"name": "broken", "code": "SELECT no_such_col FROM proj.ds.cartes AS cartes"},
        {"name": "final_query", "code": "SELECT 1"},
    ]
    trace = await _run_cte_trace(ctes, _SUFFIX, "proj", "bigquery", con)
    info = trace["broken"]
    assert info["row_count"] == -1
    assert info.get("error")  # message DuckDB présent
    assert "no_such_col" in info.get("sql", "")

    hint = _format_cte_trace_hint("broken", trace)
    # le rendu porte le message ET le SQL de l'étape (règle de logging projet)
    assert "erreur d'exécution — " in hint
    assert "SQL de l'étape" in hint


# ── Prédicat OR (égalité OU IS NULL) : décomposition par branche ─────────────
# Incident 2026-06-11 (requête bancaire) : le prédicat réellement bloquant
# `(corr.filtre_didd = rp.cd_chef_file OR corr.filtre_didd IS NULL)` était le
# seul affiché « (prédicat non décomposé) » — l'agent n'avait aucun signal.

_OR_CTES = [
    {
        "name": "corr",
        "code": (
            "SELECT cartes.filtre_didd AS filtre_didd, "
            "cartes.cd_chef_file AS cd_chef_file FROM proj.ds.cartes AS cartes"
        ),
    },
    {
        "name": "temp_carte",
        "code": (
            "SELECT corr.cd_chef_file AS cd FROM corr AS corr "
            "JOIN proj.ds.ref_port AS rp ON corr.cd_chef_file = rp.cd_chef_file "
            "AND (corr.filtre_didd = rp.cd_chef_file OR corr.filtre_didd IS NULL)"
        ),
    },
    {
        "name": "final_query",
        "code": "SELECT temp_carte.cd FROM temp_carte AS temp_carte",
    },
]


def _or_con(filtre_values):
    c = duckdb.connect()
    c.execute(f"CREATE TABLE ds_cartes_{_SUFFIX} (filtre_didd TEXT, cd_chef_file TEXT)")
    for v in filtre_values:
        c.execute(f"INSERT INTO ds_cartes_{_SUFFIX} VALUES (?, '1')", [v])
    c.execute(f"CREATE TABLE ds_ref_port_{_SUFFIX} (cd_chef_file TEXT)")
    c.execute(f"INSERT INTO ds_ref_port_{_SUFFIX} VALUES ('1')")
    return c


@pytest.mark.asyncio
async def test_or_predicate_decomposed_and_marked_blocking():
    # filtre_didd ∈ {D, I}, jamais NULL ; cd_chef_file = '1' → aucune branche
    # du OR n'est satisfiable → le OR est le prédicat bloquant.
    c = _or_con(["D", "I"])
    lines = await _run_join_predicate_breakdown(
        _OR_CTES, 1, _SUFFIX, "proj", "bigquery", c
    )
    text = "\n".join(lines)
    assert "(prédicat non décomposé)" not in text
    # chaque branche est évaluée : l'égalité (0 commune) et la branche IS NULL
    assert "0 valeur(s) commune(s)" in text
    assert "IS NULL" in text and "NULL" in text
    blocking = [ln for ln in lines if "BLOQUANT" in ln]
    assert len(blocking) == 1
    assert "OR" in blocking[0]


@pytest.mark.asyncio
async def test_or_predicate_with_satisfiable_null_branch_not_blocking():
    # Une ligne avec filtre_didd NULL satisfait la branche IS NULL → pas de
    # marqueur BLOQUANT sur le OR.
    c = _or_con(["D", None])
    lines = await _run_join_predicate_breakdown(
        _OR_CTES, 1, _SUFFIX, "proj", "bigquery", c
    )
    text = "\n".join(lines)
    assert "IS NULL" in text
    assert "BLOQUANT" not in text


# ── Filtre `WHERE col = (sous-requête scalaire)` ─────────────────────────────
# bq130 : `WHERE state_name = (SELECT state_name FROM FourthState)` — la
# décomposition de JOIN ne couvre pas ce blocage (valeur attendue calculée par
# une sous-requête sur une CTE amont). `_run_scalar_filter_breakdown` nomme la
# valeur ATTENDUE vs les valeurs PRÉSENTES.


def test_extract_eq_subquery_filters_pure():
    where = sqlglot.parse_one(
        "SELECT b.county FROM cnt b "
        "WHERE b.date >= '2020-03-01' "
        "AND b.state_name = (SELECT state_name FROM FourthState)",
        read="bigquery",
    ).args.get("where")
    filters = _extract_eq_subquery_filters(where)
    assert len(filters) == 1
    col, sub = filters[0]
    assert col.sql() == "b.state_name"
    assert "FourthState" in sub.sql()

    # ordre inversé `(SELECT …) = col`
    where_inv = sqlglot.parse_one(
        "SELECT * FROM t b WHERE (SELECT x FROM s) = b.k", read="bigquery"
    ).args.get("where")
    assert len(_extract_eq_subquery_filters(where_inv)) == 1

    # égalités sans sous-requête → rien
    where_neg = sqlglot.parse_one(
        "SELECT * FROM t WHERE a = 1 AND b = c", read="bigquery"
    ).args.get("where")
    assert _extract_eq_subquery_filters(where_neg) == []


_SUB_CTES = [
    {"name": "fourth_state", "code": "SELECT 'StateD' AS state_name"},
    {
        "name": "cnt",
        "code": (
            "SELECT b.county AS county FROM proj.ds.counties AS b "
            "WHERE b.state_name = (SELECT state_name FROM fourth_state)"
        ),
    },
    {"name": "final_query", "code": "SELECT cnt.county FROM cnt AS cnt"},
]


def _counties_con(state_name):
    c = duckdb.connect()
    c.execute(f"CREATE TABLE ds_counties_{_SUFFIX} (county TEXT, state_name TEXT)")
    c.execute(f"INSERT INTO ds_counties_{_SUFFIX} VALUES ('LA', ?)", [state_name])
    return c


@pytest.mark.asyncio
async def test_scalar_filter_breakdown_marks_value_mismatch():
    # pivot veut 'StateD', mais la colonne ne contient que 'StateB' → BLOQUANT
    c = _counties_con("StateB")
    lines = await _run_scalar_filter_breakdown(
        _SUB_CTES, 1, _SUFFIX, "proj", "bigquery", c
    )
    assert len(lines) == 1
    assert "BLOQUANT" in lines[0]
    assert "veut 'StateD'" in lines[0]
    assert "StateB" in lines[0]


@pytest.mark.asyncio
async def test_scalar_filter_breakdown_satisfiable_not_blocking():
    # la colonne contient bien 'StateD' → satisfiable, pas de marqueur
    c = _counties_con("StateD")
    lines = await _run_scalar_filter_breakdown(
        _SUB_CTES, 1, _SUFFIX, "proj", "bigquery", c
    )
    assert len(lines) == 1
    assert "BLOQUANT" not in lines[0]
    assert "veut 'StateD'" in lines[0]


@pytest.mark.asyncio
async def test_left_join_not_decomposed(con):
    ctes = [
        _CTES[0],
        {
            "name": "temp_carte",
            "code": (
                "SELECT corr.code_produit AS cp FROM corr AS corr "
                "LEFT JOIN proj.ds.ref_port AS rp ON corr.code_produit = rp.cd_type_carte_smp"
            ),
        },
        _CTES[2],
    ]
    lines = await _run_join_predicate_breakdown(
        ctes, 1, _SUFFIX, "proj", "bigquery", con
    )
    # LEFT JOIN non forçant : la non-correspondance est tolérée → pas de bruit
    assert lines == []
