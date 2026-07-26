"""Régression Trino : la qualification sqlglot met les identifiants de
used_columns en minuscules, alors que le schema_cache conserve la casse
d'origine de l'entrepôt (BigQuery). Tous les rapprochements used_columns ↔
schéma doivent être insensibles à la casse — sinon, en dialecte Trino :

  - le générateur ne produit aucune table (filter_columns → 0),
  - l'executor ne crée aucune table DuckDB (filter_schemas_by_used_columns → 0),

et la requête échoue avec « Table ... does not exist ».
"""

import unittest

from build_query.examples_executor import filter_schemas_by_used_columns


# schema_cache : casse d'origine BigQuery (mixte), noms 3 parties.
SCHEMAS = [
    {
        "table_name": "pipetalk-493612.MONETIQUE_Dataset_Porteur.DS_RCOMP_DASHBOARD_RESEAU",
        "columns": [
            {"name": "NO_SIRET", "type": "STRING", "description": ""},
            {"name": "MT_BRUT_TRANSACTION", "type": "FLOAT", "description": ""},
            {"name": "CD_ERT", "type": "STRING", "description": ""},
        ],
    },
    {
        "table_name": "pipetalk-493612.MARKETING_Referentiels.banques",
        "columns": [
            {"name": "code_banque", "type": "STRING", "description": ""},
            {"name": "reseau", "type": "STRING", "description": ""},
        ],
    },
]

# used_columns tel que produit par la qualification Trino : tout en minuscules.
TRINO_USED_COLUMNS = [
    {
        "project": "",
        "database": "monetique_dataset_porteur",
        "table": "ds_rcomp_dashboard_reseau",
        "used_columns": ["no_siret", "mt_brut_transaction", "cd_ert"],
    },
    {
        "project": "",
        "database": "marketing_referentiels",
        "table": "banques",
        "used_columns": ["code_banque", "reseau"],
    },
]


class TestFilterSchemasCaseInsensitive(unittest.TestCase):
    def test_trino_lowercased_used_columns_still_match(self):
        filtered = filter_schemas_by_used_columns(SCHEMAS, TRINO_USED_COLUMNS)
        names = sorted(t["table_name"] for t in filtered)
        self.assertEqual(
            names,
            [
                "pipetalk-493612.MARKETING_Referentiels.banques",
                "pipetalk-493612.MONETIQUE_Dataset_Porteur.DS_RCOMP_DASHBOARD_RESEAU",
            ],
        )
        # Les colonnes utilisées doivent aussi être conservées malgré la casse.
        rcomp = next(
            t for t in filtered if t["table_name"].endswith("DS_RCOMP_DASHBOARD_RESEAU")
        )
        self.assertEqual(len(rcomp["columns"]), 3)

    def test_bigquery_matching_case_unchanged(self):
        """Casse déjà alignée (BigQuery) → comportement inchangé."""
        bq_used = [
            {
                "project": "pipetalk-493612",
                "database": "MARKETING_Referentiels",
                "table": "banques",
                "used_columns": ["code_banque", "reseau"],
            }
        ]
        filtered = filter_schemas_by_used_columns(SCHEMAS, bq_used)
        self.assertEqual(len(filtered), 1)
        self.assertTrue(filtered[0]["table_name"].endswith("banques"))


if __name__ == "__main__":
    unittest.main()
