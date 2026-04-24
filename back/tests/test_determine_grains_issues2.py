import unittest

from utils.find_grains import determine_query_grain


class TestDetermineQueryGrainWithSamePrimaryKeyNames(unittest.TestCase):
    def setUp(self):
        self.tables_and_columns = [
            {
                "table_name": "segmentation_cartes_bp",
                "columns": [
                    {
                        "name": "numero",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "code_produit_groupe",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "coprid",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "libelle_carte_bpce",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "libelle_carte_nps",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "didd",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "partition_date",
                        "type": "DATE",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "processing_time",
                        "type": "TIMESTAMP",
                        "description": "",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["numero"],
            },
            {
                "table_name": "banques",
                "columns": [
                    {
                        "name": "code_banque",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "libelle_banque_ancien",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "libelle_banque",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "tb",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "tb_pmr",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "tb_pcr",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "actif",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "tb_retrait",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "tb_coview",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "libelle_courte",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "tb_dg",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "tb_nit",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "libelle_nit",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "coetb",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "libelle_payplug",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "reseau",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "poids_naturel",
                        "type": "FLOAT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "poids_nature_bdd",
                        "type": "FLOAT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "poids_nature_bdr",
                        "type": "FLOAT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "cd_chef_file",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "event_date",
                        "type": "DATE",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "partition_date",
                        "type": "DATE",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "processing_time",
                        "type": "TIMESTAMP",
                        "description": "",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["code_banque"],
            },
            {
                "table_name": "se_segmentation_cartes_bp",
                "columns": [
                    {
                        "name": "coetb",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "banque",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "code_produit_groupe",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "carte",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "coprid",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "type_carte",
                        "type": "STRING",
                        "description": "",
                        "is_categorical": True,
                    },
                    {
                        "name": "nombre_cartes_particuliers",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "nombre_cartes_professionnels",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "nombre_cartes_pme_pmi",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "nombre_cartes_autres_marches",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "nombre_cartes_non_qualifie",
                        "type": "INT64",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "partition_date",
                        "type": "DATE",
                        "description": "",
                        "is_categorical": False,
                    },
                    {
                        "name": "processing_time",
                        "type": "TIMESTAMP",
                        "description": "",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": [
                    "coetb",
                ],
            },
        ]

    def test_simple_select_with_composed_table_name(self):
        query = """WITH
  `_u_0` AS (
  SELECT
    MAX(`banques`.`partition_date`) AS `_col_0`
  FROM
    `MARKETING_Referentiels.banques` AS `banques`
  WHERE
    `banques`.`partition_date` <= PARSE_DATE('%d-%m-%Y', '01-03-2025')),
  `banque_view` AS (
  SELECT
    `banques`.`code_banque` AS `code_banque`,
    `banques`.`coetb` AS `coetb`
  FROM
    `MARKETING_Referentiels.banques` AS `banques`
  LEFT JOIN
    `_u_0` AS `_u_0`
  ON
    `_u_0`.`_col_0` = `banques`.`partition_date`
  WHERE
    (`banques`.`cd_chef_file` = 1
      OR `banques`.`cd_chef_file` = 4)
    AND (`banques`.`cd_chef_file` = 1
      OR `banques`.`code_banque` IN ('42559',
        '30258'))
    AND `banques`.`tb` IN (1,
      2,
      3)
    AND NOT `_u_0`.`_col_0` IS NULL),
  `_u_3` AS (
  SELECT
    MAX(`segmentation_cartes_bp`.`partition_date`) AS `_col_0`
  FROM
    `MARKETING_Referentiels.segmentation_cartes_bp` AS `segmentation_cartes_bp`
  WHERE
    `segmentation_cartes_bp`.`partition_date` <= PARSE_DATE('%d-%m-%Y', '01-03-2025')),
  `ref_segmentation_cartes_view` AS (
  SELECT
    `segmentation_cartes_bp`.`code_produit_groupe` AS `code_produit_groupe`,
    `segmentation_cartes_bp`.`libelle_carte_nps` AS `libelle_carte_nps`,
    `segmentation_cartes_bp`.`coprid` AS `coprid`,
    `segmentation_cartes_bp`.`didd` AS `didd`
  FROM
    `MARKETING_Referentiels.segmentation_cartes_bp` AS `segmentation_cartes_bp`
  LEFT JOIN
    `_u_3` AS `_u_3`
  ON
    `_u_3`.`_col_0` = `segmentation_cartes_bp`.`partition_date`
  WHERE
    NOT `_u_3`.`_col_0` IS NULL),
  `_u_4` AS (
  SELECT
    MAX(`se_segmentation_cartes_bp`.`partition_date`) AS `_col_0`
  FROM
    `MARKETING_Sources_externes.se_segmentation_cartes_bp` AS `se_segmentation_cartes_bp`
  WHERE
    `se_segmentation_cartes_bp`.`partition_date` <= PARSE_DATE('%d-%m-%Y', '01-03-2025')),
  `cb_seg_agg_view` AS (
  SELECT
    `bq`.`code_banque` AS `code_banque`,
    `rseg`.`libelle_carte_nps` AS `libelle_carte`,
    `rseg`.`didd` AS `didd`,
    SUM(`seg`.`nombre_cartes_particuliers`) AS `nombre_cartes_particuliers`,
    SUM(`seg`.`nombre_cartes_professionnels`) AS `nombre_cartes_professionnels`,
    SUM(`seg`.`nombre_cartes_non_qualifie`) AS `nombre_cartes_non_qualifie`,
    SUM((`seg`.`nombre_cartes_pme_pmi` + `seg`.`nombre_cartes_autres_marches`)) AS `nombre_carte_entreprises`
  FROM
    `MARKETING_Sources_externes.se_segmentation_cartes_bp` AS `seg`
  LEFT JOIN
    `_u_4` AS `_u_4`
  ON
    `_u_4`.`_col_0` = `seg`.`partition_date`
  LEFT JOIN
    `banque_view` AS `bq`
  ON
    `bq`.`coetb` = `seg`.`coetb`
  LEFT JOIN
    `ref_segmentation_cartes_view` AS `rseg`
  ON
    `rseg`.`code_produit_groupe` = `seg`.`code_produit_groupe`
    AND `rseg`.`coprid` = `seg`.`coprid`
  WHERE
    NOT `_u_4`.`_col_0` IS NULL
    AND NOT `rseg`.`libelle_carte_nps` IS NULL
  GROUP BY
    `bq`.`code_banque`,
    `rseg`.`libelle_carte_nps`,
    `rseg`.`didd`),
  `unpivoted` AS (
  SELECT
    `seg`.`code_banque` AS `code_banque`,
    `seg`.`libelle_carte` AS `libelle_carte`,
    `seg`.`didd` AS `didd`,
    `seg`.`nombre_cartes_particuliers` / (`seg`.`nombre_cartes_particuliers` + `seg`.`nombre_cartes_professionnels` + `seg`.`nombre_cartes_non_qualifie` + `seg`.`nombre_carte_entreprises`) AS `rapport_particulier`,
    `seg`.`nombre_cartes_professionnels` / (`seg`.`nombre_cartes_particuliers` + `seg`.`nombre_cartes_professionnels` + `seg`.`nombre_cartes_non_qualifie` + `seg`.`nombre_carte_entreprises`) AS `rapport_pro`,
    `seg`.`nombre_carte_entreprises` / (`seg`.`nombre_cartes_particuliers` + `seg`.`nombre_cartes_professionnels` + `seg`.`nombre_cartes_non_qualifie` + `seg`.`nombre_carte_entreprises`) AS `rapport_entreprise`,
    `seg`.`nombre_cartes_non_qualifie` / (`seg`.`nombre_cartes_particuliers` + `seg`.`nombre_cartes_professionnels` + `seg`.`nombre_cartes_non_qualifie` + `seg`.`nombre_carte_entreprises`) AS `rapport_non_renseigne`
  FROM
    `cb_seg_agg_view` AS `seg`
  JOIN
    `banque_view` AS `bq`
  ON
    `bq`.`code_banque` = `seg`.`code_banque`)
SELECT
  `_q_0`.`code_banque` AS `code_banque`,
  `_q_0`.`libelle_carte` AS `libelle_carte`,
  `_q_0`.`didd` AS `didd`,
  `_q_0`.`valeur` AS `valeur`,
  CASE
    WHEN `_q_0`.`indicateur` = 'rapport_particulier' THEN 'Particulier'
    WHEN `_q_0`.`indicateur` = 'rapport_pro' THEN 'Pro'
    WHEN `_q_0`.`indicateur` = 'rapport_entreprise' THEN 'Entreprise'
    WHEN `_q_0`.`indicateur` = 'rapport_non_renseigne' THEN 'NR'
END
  AS `code_marche`,
  PARSE_DATE('%d-%m-%Y', '01-03-2025') AS `partition_date`,
  CURRENT_TIMESTAMP() AS `processing_time`
FROM
  `unpivoted` AS `unpivoted`
UNPIVOT
  (`valeur` FOR `indicateur` IN (`unpivoted`.`rapport_particulier`,
      `unpivoted`.`rapport_pro`,
      `unpivoted`.`rapport_entreprise`,
      `unpivoted`.`rapport_non_renseigne`)) AS `_q_0`
WHERE
  `_q_0`.`valeur` > 0;
"""
        # Both table_a and table_b have a primary key named "id".
        # The expected grain should include both "a_id" and "b_id" due to the aliases.
        expected_grain = ["code_banque", "didd", "libelle_carte"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)


if __name__ == "__main__":
    unittest.main()
