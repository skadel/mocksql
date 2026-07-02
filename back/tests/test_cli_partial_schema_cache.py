"""Régression : résolution des schémas de `mocksql test` (fidélité prod, sans inférence).

Politique (correctness-first, cf. schema_cache_stale_failfast) : le replay utilise
TOUJOURS le vrai schéma de l'entrepôt (`schema_cache`). Toute table présente dans les
données du test mais absente du cache lève `SchemaMissingError` — on n'infère JAMAIS un
schéma depuis les lignes, ce qui masquerait un bug de type réel (colonne date-like →
VARCHAR → "Cannot compare VARCHAR and DATE"). Le message pointe vers `refresh-schemas -t …`.
"""

import json

import pytest

from cli.test_runner import (
    SchemaMissingError,
    _flatten_table_key,
    _resolve_model_schemas,
    collect_test_table_refs,
)


def _uc(database, table, cols, project="p"):
    return json.dumps(
        {
            "project": project,
            "database": database,
            "table": table,
            "used_columns": cols,
        }
    )


def _schema(table_name, cols):
    return {
        "table_name": table_name,
        "columns": [{"name": c, "type": "TEXT", "description": ""} for c in cols],
        "description": "",
        "primary_keys": [],
    }


def _case(data):
    return {"test_index": "0", "data": data}


def test_partial_cache_raises_missing_schema():
    # Le cache ne connaît QUE banques ; le SQL/les données utilisent aussi ref_porteur.
    used = [
        _uc("MONETIQUE_Dataset_Porteur", "banques", ["code_banque"]),
        _uc("MONETIQUE_Dataset_Porteur", "DS_REF_PORTEUR", ["id_porteur"]),
    ]
    cache = [_schema("p.MONETIQUE_Dataset_Porteur.banques", ["code_banque"])]
    cases = [
        _case(
            {
                "MONETIQUE_Dataset_Porteur_banques": [{"code_banque": "BP"}],
                "MONETIQUE_Dataset_Porteur_DS_REF_PORTEUR": [{"id_porteur": "X1"}],
            }
        )
    ]

    with pytest.raises(SchemaMissingError) as exc:
        _resolve_model_schemas(used, cache, cases)

    msg = str(exc.value)
    # La table manquante est nommée par sa réf BQ complète, ET la commande d'import
    # actionnable est fournie — la table déjà couverte (banques) n'apparaît pas.
    assert "p.MONETIQUE_Dataset_Porteur.DS_REF_PORTEUR" in msg
    assert "refresh-schemas" in msg
    assert "-t p.MONETIQUE_Dataset_Porteur.DS_REF_PORTEUR" in msg
    assert "banques" not in msg.split("refresh-schemas")[0]


def test_no_cache_match_at_all_raises():
    # Aucune des tables des données n'est dans le cache (cas réel c1) → erreur, pas
    # d'inférence silencieuse.
    used = [_uc("MARKETING_Referentiels", "banques_france", ["partition_date"])]
    cache = [_schema("p.OTHER_dataset.unrelated", ["x"])]
    cases = [
        _case(
            {
                "MARKETING_Referentiels_banques_france": [
                    {"partition_date": "2026-01-01"}
                ]
            }
        )
    ]

    with pytest.raises(SchemaMissingError) as exc:
        _resolve_model_schemas(used, cache, cases)
    assert "p.MARKETING_Referentiels.banques_france" in str(exc.value)


def test_full_cache_resolves_without_error():
    # Quand le cache couvre tout, aucune erreur : on retourne le schéma du cache.
    used = [_uc("d", "t", ["a"])]
    cache = [_schema("p.d.t", ["a"])]
    cases = [_case({"d_t": [{"a": "1"}]})]

    schemas = _resolve_model_schemas(used, cache, cases)
    assert len(schemas) == 1
    assert schemas[0]["table_name"] == "p.d.t"


def test_cache_schema_not_filtered_by_incomplete_used_columns():
    # `used_columns` ne liste que 'a', mais la table réelle a a,b,c. Le réplay doit
    # créer TOUTES les colonnes réelles — sinon une colonne utilisée par le SQL mais
    # absente des used_columns (extraction ratée, cf. bq234 `total_day_supply`) est
    # droppée → "Referenced column ... not found in FROM clause".
    used = [_uc("d", "t", ["a"])]
    cache = [_schema("p.d.t", ["a", "b", "c"])]
    cases = [_case({"d_t": [{"a": "1"}]})]

    schemas = _resolve_model_schemas(used, cache, cases)
    assert len(schemas) == 1
    assert {c["name"] for c in schemas[0]["columns"]} == {"a", "b", "c"}


def test_no_cache_at_all_raises():
    # Sans AUCUN schema_cache : on n'infère pas — erreur listant les tables des données
    # (par leur clé plate, faute de used_columns pour reconstruire la réf BQ).
    cases = [_case({"d_t": [{"a": "1"}], "d_u": [{"b": "2"}]})]
    with pytest.raises(SchemaMissingError) as exc:
        _resolve_model_schemas([], [], cases)
    msg = str(exc.value)
    assert "d_t" in msg and "d_u" in msg


def test_missing_without_used_columns_falls_back_to_flat_key():
    # Cache présent, table absente, et pas de used_columns pour reconstruire la réf BQ
    # → le message retombe sur la clé de données plate (toujours actionnable).
    cache = [_schema("p.d.t", ["a"])]
    cases = [_case({"some_missing_table": [{"a": "1"}]})]

    with pytest.raises(SchemaMissingError) as exc:
        _resolve_model_schemas([], cache, cases)
    assert "some_missing_table" in str(exc.value)


def _write_def(path, used_columns):
    # `used_columns` sur disque = list[dict] (forme lisible), ré-encodée en list[str]
    # par read_test_doc à la lecture (cf. storage/test_files.py).
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"sql": "SELECT 1", "used_columns": used_columns, "test_cases": []}),
        encoding="utf-8",
    )


def test_collect_test_table_refs(tmp_path):
    tests_root = tmp_path / ".mocksql" / "tests"
    _write_def(
        tests_root / "m1.json",
        [
            {"project": "p", "database": "d1", "table": "t1", "used_columns": ["a"]},
            {"project": "p", "database": "d1", "table": "t2", "used_columns": ["b"]},
        ],
    )
    # Sous-dossier + chevauchement (t1) + nouvelle table (t3).
    _write_def(
        tests_root / "sub" / "m2.json",
        [
            {"project": "p", "database": "d1", "table": "t1", "used_columns": ["a"]},
            {"project": "p", "database": "d2", "table": "t3", "used_columns": ["c"]},
        ],
    )
    # Fichier de session nommé en UUID → ignoré (comme run_tests).
    _write_def(
        tests_root / "12345678-1234-1234-1234-123456789abc.json",
        [{"project": "p", "database": "d9", "table": "skipme", "used_columns": []}],
    )

    refs = collect_test_table_refs(tests_root)
    assert refs == ["p.d1.t1", "p.d1.t2", "p.d2.t3"]


def test_collect_test_table_refs_missing_dir(tmp_path):
    assert collect_test_table_refs(tmp_path / "nope") == []


def test_flatten_table_key_matches_dotted_and_flat_forms():
    # Une clé de données plate et la forme pointée du cache se rejoignent.
    assert _flatten_table_key("p.MONETIQUE_Dataset_Porteur.DS_REF_PORTEUR") == (
        "monetique_dataset_porteur_ds_ref_porteur"
    )
    assert _flatten_table_key("MONETIQUE_Dataset_Porteur_DS_REF_PORTEUR") == (
        "monetique_dataset_porteur_ds_ref_porteur"
    )
