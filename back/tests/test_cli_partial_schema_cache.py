"""Régression : `mocksql test` crée une table DuckDB pour CHAQUE table présente
dans les données, même quand le schema_cache ne couvre qu'une partie d'entre elles.

Sans ça, une table référencée par le SQL mais absente du cache (ex. ajoutée au
modèle après le dernier profilage) ne reçoit aucun schéma → aucune table DuckDB
créée, alors que le SQL réécrit quand même sa référence avec le suffixe →
"Catalog Error: Table ... does not exist" au moment d'exécuter la requête.
"""

import json

from cli.test_runner import _flatten_table_key, _resolve_model_schemas


def _uc(database, table, cols):
    return json.dumps(
        {"project": "p", "database": database, "table": table, "used_columns": cols}
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


def test_partial_cache_completed_from_data_rows():
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

    schemas = _resolve_model_schemas(used, cache, cases)
    bases = {_flatten_table_key(s["table_name"]) for s in schemas}

    # Les DEUX tables doivent avoir un schéma : la table du cache + celle inférée
    # depuis les lignes pour combler le trou de couverture.
    assert "monetique_dataset_porteur_banques" in bases
    assert "monetique_dataset_porteur_ds_ref_porteur" in bases


def test_full_cache_not_duplicated_by_inference():
    # Quand le cache couvre tout, on n'ajoute aucun schéma inféré redondant.
    used = [_uc("d", "t", ["a"])]
    cache = [_schema("p.d.t", ["a"])]
    cases = [_case({"d_t": [{"a": "1"}]})]

    schemas = _resolve_model_schemas(used, cache, cases)
    assert len(schemas) == 1
    assert schemas[0]["table_name"] == "p.d.t"


def test_no_cache_falls_back_to_full_inference():
    cases = [_case({"d_t": [{"a": "1"}], "d_u": [{"b": "2"}]})]
    schemas = _resolve_model_schemas([], [], cases)
    bases = {_flatten_table_key(s["table_name"]) for s in schemas}
    assert bases == {"d_t", "d_u"}


def test_flatten_table_key_matches_dotted_and_flat_forms():
    # Une clé de données plate et la forme pointée du cache se rejoignent.
    assert _flatten_table_key("p.MONETIQUE_Dataset_Porteur.DS_REF_PORTEUR") == (
        "monetique_dataset_porteur_ds_ref_porteur"
    )
    assert _flatten_table_key("MONETIQUE_Dataset_Porteur_DS_REF_PORTEUR") == (
        "monetique_dataset_porteur_ds_ref_porteur"
    )
