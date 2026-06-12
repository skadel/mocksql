"""Vérification forward DuckDB de l'exemple few-shot statique (P2a).

L'exemple injecté dans le prompt du générateur (prompt_tools.FEW_SHOT_EXAMPLE_*)
doit être VRAI : les données « correctes » produisent exactement une ligne, et
les deux erreurs annotées ✗ dans le prompt vident réellement le résultat.
Si l'exemple est modifié, ce test garantit qu'il reste honnête.
"""

import copy

import duckdb
import pytest

from build_query.prompt_tools import (
    FEW_SHOT_EXAMPLE_DATA,
    FEW_SHOT_EXAMPLE_SQL,
)

# clé `data` du prompt → table qualifiée référencée par la mini-requête
_TABLE_MAP = {
    "cartes_stock": ("cartes", "stock"),
    "ref_produits": ("ref", "produits"),
}


def _run_example(data: dict) -> list[tuple]:
    con = duckdb.connect(":memory:")
    for key, rows in data.items():
        schema, table = _TABLE_MAP[key]
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        columns = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        con.execute(
            f"CREATE TABLE {schema}.{table} ({', '.join(f'{c} VARCHAR' for c in columns)})"
        )
        con.executemany(
            f"INSERT INTO {schema}.{table} VALUES ({placeholders})",
            [[row[c] for c in columns] for row in rows],
        )
    return con.execute(FEW_SHOT_EXAMPLE_SQL).fetchall()


def test_correct_data_returns_one_opening():
    rows = _run_example(FEW_SHOT_EXAMPLE_DATA)
    assert rows == [("C001", "Carte Gold BP")]


def test_pitfall_final_value_in_derived_key_empties_result():
    # ✗ Erreur 1 annoncée dans le prompt : valeur FINALE de la clé au lieu de
    # la valeur SOURCE → le CASE produit 'BPBPGOLD', la jointure échoue.
    data = copy.deepcopy(FEW_SHOT_EXAMPLE_DATA)
    data["cartes_stock"][0]["type_carte"] = "BPGOLD"
    assert _run_example(data) == []


def test_pitfall_row_in_both_photos_empties_result():
    # ✗ Erreur 2 annoncée dans le prompt : une ligne « par complétude » en M-1
    # → la carte n'est plus une ouverture, le WHERE IS NULL l'élimine.
    data = copy.deepcopy(FEW_SHOT_EXAMPLE_DATA)
    m1_row = dict(data["cartes_stock"][0], dt_photo="2024-02-29")
    data["cartes_stock"].append(m1_row)
    assert _run_example(data) == []


def test_prompt_contains_static_example_between_reference_and_ask():
    # L'exemple est injecté entre le message de référence et l'ask, quel que
    # soit le modèle testé (exemple statique, indépendant du SQL utilisateur).
    from build_query.prompt_tools import generate_data_prompt

    msgs = generate_data_prompt(
        history=[],
        dialect="bigquery",
        format_instructions="FORMAT_INSTRUCTIONS",
        used_columns=[
            {
                "database": "MARKETING",
                "table": "banques",
                "used_columns": ["code_banque"],
            }
        ],
        sql="SELECT code_banque FROM MARKETING.banques",
    ).format_messages()

    ref_idx = next(i for i, m in enumerate(msgs) if m.content.startswith("<schema>"))
    example_idx = next(
        i for i, m in enumerate(msgs) if m.content.startswith("<example>")
    )
    ask_idx = next(
        i for i, m in enumerate(msgs) if m.content.lstrip().startswith("<task>")
    )
    assert ref_idx < example_idx < ask_idx
    # la réponse correcte suit immédiatement l'énoncé de l'exemple (paire few-shot)
    assert msgs[example_idx + 1].content.lstrip().startswith("{")
    assert "BPGOLD" in msgs[example_idx + 1].content


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
