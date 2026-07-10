"""Régression (audit c6.sql) — les blocs `<result_schema>` et `<result_sample>` injectés
dans les prompts d'assertion.

Deux classes de bugs, une seule cause racine (round-trip JSON du résultat) :

P0-2 · `<result_schema>` menteur. `assertion_generator` reconstruit le DataFrame via
`pd.read_json`, qui perd les types : une colonne date ISO redevient `object` (VARCHAR une
fois registrée dans DuckDB), une mesure FLOAT64 à valeurs entières redevient `int64`. Le
prompt affichait le dtype PANDAS (`object`) tout en promettant « le schéma exact de
`__result__` », et n'annotait aucune valeur d'exemple. Le juge écrivait alors
`partition_date = '2024-03-01'` — toujours FAUX puisque la valeur est
`'2024-03-01T00:00:00'`. `_result_schema_and_sample` doit rendre le type DuckDB RÉEL (via
DESCRIBE de la vue registrée) et une valeur d'exemple par colonne.

P0-1 · `<result_sample>` non plafonné. Le résultat entier (1344 lignes sur `c6.sql`) était
injecté intégralement dans deux appels (~963K tokens chacun → 429 Vertex).
`_render_result_sample_block` doit plafonner par LIGNES ENTIÈRES (jamais de troncature
caractère), conserver le `row_count`, et lister les valeurs distinctes des colonnes
discriminantes pour que le juge sache ce qui existe hors échantillon.
"""

import duckdb
import pandas as pd

from build_query.examples_executor import (
    _render_result_sample_block,
    _result_schema_and_sample,
)


# ─────────────────────────── P0-2 · schéma véridique ───────────────────────────


def _lying_result_df() -> pd.DataFrame:
    """DataFrame tel que reconstruit après round-trip JSON : la date est une chaîne ISO
    (dtype object → VARCHAR dans DuckDB), la mesure entière est int64 (was FLOAT64)."""
    return pd.DataFrame(
        {
            "partition_date": ["2024-03-01T00:00:00", "2024-04-01T00:00:00"],
            "mt_ope": [500, 100],
            "zscore": [8.23, 0.11],
        }
    )


def test_schema_block_uses_real_duckdb_types_via_describe():
    df = _lying_result_df()
    con = duckdb.connect()
    con.register("__result__test", df)

    block, _, _ = _result_schema_and_sample(df, con=con, view_name="__result__test")

    # Le type affiché est celui de DuckDB (VARCHAR), pas le dtype pandas trompeur (object).
    assert "VARCHAR" in block
    assert "object" not in block
    # La colonne date est bien annoncée VARCHAR — c'est le signal qui évite `date = '2024-03-01'`.
    partition_line = next(
        line for line in block.splitlines() if "partition_date" in line
    )
    assert "VARCHAR" in partition_line


def test_schema_block_annotates_example_value():
    df = _lying_result_df()
    con = duckdb.connect()
    con.register("__result__test", df)

    block, _, _ = _result_schema_and_sample(df, con=con, view_name="__result__test")

    # La valeur d'exemple porte la partie heure → le juge voit le format ISO complet et
    # n'écrit pas une comparaison à un jour nu qui échouerait.
    assert "2024-03-01T00:00:00" in block


def test_schema_block_falls_back_to_pandas_dtypes_without_con():
    df = _lying_result_df()
    block, _, _ = _result_schema_and_sample(df)
    # Sans connexion DuckDB, repli sur les dtypes pandas (comportement historique préservé),
    # mais la valeur d'exemple reste présente.
    assert "partition_date" in block
    assert "2024-03-01T00:00:00" in block


def test_schema_block_empty_df():
    block, _, _ = _result_schema_and_sample(pd.DataFrame())
    assert "aucune colonne" in block


# ─────────────────────────── P0-1 · sample plafonné ───────────────────────────


def _wide_result_records(n_rows: int = 200) -> list:
    indicators = ["nb_ope", "mt_ope", "nb_cartes", "zscore", "stddev", "Global"]
    dates = [f"2024-{m:02d}-01T00:00:00" for m in range(1, 13)]
    return [
        {
            "indicateur": indicators[i % len(indicators)],
            "partition_date": dates[i % len(dates)],
            "valeur": float(i),
        }
        for i in range(n_rows)
    ]


def test_sample_block_small_result_is_untruncated():
    block = _render_result_sample_block(_wide_result_records(10))
    # <= plafond → résultat complet, pas de marqueur de troncature (0 régression cas courant).
    assert "10 ligne(s)" in block
    assert "autres" not in block


def test_sample_block_large_result_is_capped_by_whole_rows():
    block = _render_result_sample_block(_wide_result_records(200), max_rows=60)
    # Le nombre total réel est conservé.
    assert "200 ligne(s)" in block
    # Marqueur de troncature (lignes entières).
    assert "+140 autres" in block
    # On n'a PAS injecté les 200 lignes : la dernière valeur (199.0) est hors échantillon.
    assert "199.0" not in block


def test_sample_block_lists_distinct_discriminant_values_when_truncated():
    block = _render_result_sample_block(_wide_result_records(200), max_rows=60)
    # Les valeurs distinctes des colonnes catégorielles sont listées → le juge sait ce qui
    # existe hors échantillon (les 6 indicateurs, dont un jamais dans les 60 premières lignes).
    assert "Valeurs distinctes" in block
    for indicator in ("nb_ope", "mt_ope", "nb_cartes", "Global"):
        assert indicator in block


def test_sample_block_prioritizes_subject_rows():
    block = _render_result_sample_block(
        _wide_result_records(200),
        test_description="focus sur l'indicateur nb_cartes",
        max_rows=6,
    )
    # Avec un plafond serré, les lignes du sujet (nb_cartes) doivent apparaître dans
    # l'échantillon malgré leur position tardive dans le résultat brut.
    assert "nb_cartes" in block


def test_sample_block_skips_unhashable_columns():
    """Colonnes ARRAY/STRUCT (ex. les *_lags de c6) : jamais discriminantes — ignorées
    sans casser le rendu."""
    records = [
        {"indicateur": f"ind_{i % 4}", "lags": [i, i + 1], "valeur": float(i)}
        for i in range(100)
    ]
    block = _render_result_sample_block(records, max_rows=10)
    assert "100 ligne(s)" in block
    assert "+90 autres" in block
    # La colonne catégorielle hashable est bien résumée, la colonne liste est ignorée.
    assert "ind_0" in block
    assert "`lags`" not in block.split("Valeurs distinctes")[-1]


def test_sample_block_never_char_slices_json():
    """Le JSON de l'échantillon reste parsable — jamais de coupe au milieu d'un objet."""
    import json
    import re

    block = _render_result_sample_block(_wide_result_records(200), max_rows=60)
    # Le premier tableau JSON du bloc doit être intégralement parsable.
    match = re.search(r"\[.*?\]\n\(\+", block, re.DOTALL)
    assert match, "tableau d'échantillon introuvable"
    parsed = json.loads(match.group(0)[:-3])
    assert isinstance(parsed, list)
    assert len(parsed) == 60
