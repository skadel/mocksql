"""Régression : les colonnes de regroupement implicites d'un PIVOT BigQuery.

BigQuery PIVOT expose dans sa sortie toutes les colonnes de base qui ne sont ni
la valeur agrégée ni la colonne FOR — ce sont des colonnes de regroupement
implicites. Référencées via l'alias virtuel du pivot (``_0``) dans un WHERE/GROUP
en aval, ``qualify_columns`` les attribue à cet alias, absent de ``scope.sources``.
Sans rattachement à la table de base, elles disparaissaient de ``used_columns`` →
table de test synthétique créée sans elles → DuckDB "Values list does not have a
column named ...".
"""

import json

from utils.sql_code import extract_used_columns_from_sql

SCHEMAS = [
    {
        "table_name": "MARKETING_Reporting.datamart_pr_monthly_commercants",
        "columns": [
            {"name": "libelle_banque", "type": "STRING"},
            {"name": "partition_date", "type": "DATE"},
            {"name": "axe_contrat", "type": "STRING"},
            {"name": "valeur", "type": "FLOAT64"},
            {"name": "indicateur", "type": "STRING"},
        ],
    }
]


def _used_columns_for(table: str, sql: str) -> set[str]:
    entries = [
        json.loads(e) for e in extract_used_columns_from_sql(sql, "bigquery", SCHEMAS)
    ]
    for entry in entries:
        if entry["table"] == table:
            return set(entry["used_columns"])
    return set()


def test_pivot_grouping_column_in_where_is_traced_to_base_table():
    sql = """
    WITH pivoted AS (
      SELECT libelle_banque, partition_date, SUM(Nb_ope) AS Nb_ope
      FROM `MARKETING_Reporting.datamart_pr_monthly_commercants`
      PIVOT (SUM(CAST(valeur AS FLOAT64)) FOR indicateur IN ('Nb_ope', 'Mt_ope'))
      WHERE axe_contrat IN ('Commercants')
      GROUP BY CUBE(1, 2)
    )
    SELECT * FROM pivoted
    """
    cols = _used_columns_for("datamart_pr_monthly_commercants", sql)
    # Colonnes consommées par le pivot (références explicites).
    assert "valeur" in cols
    assert "indicateur" in cols
    # Colonnes de regroupement implicites — celles qui manquaient.
    assert "axe_contrat" in cols
    assert "libelle_banque" in cols
    assert "partition_date" in cols


def test_pivot_value_columns_not_attributed_to_base_table():
    """Les colonnes générées par le pivot (IN list) ne sont PAS des colonnes de base."""
    sql = """
    SELECT libelle_banque, Nb_ope
    FROM `MARKETING_Reporting.datamart_pr_monthly_commercants`
    PIVOT (SUM(CAST(valeur AS FLOAT64)) FOR indicateur IN ('Nb_ope', 'Mt_ope'))
    """
    cols = _used_columns_for("datamart_pr_monthly_commercants", sql)
    assert "nb_ope" not in cols
    assert "libelle_banque" in cols
