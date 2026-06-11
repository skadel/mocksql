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

from build_query.examples_executor import (
    _run_cte_trace,
    _run_join_predicate_breakdown,
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
