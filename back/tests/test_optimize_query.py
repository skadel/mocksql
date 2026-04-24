import re

import sqlglot

from build_query.validator import optimize_query


class TestOptimizeQueryUppercaseColumns:
    def test_uppercase_col_in_query_lowercase_in_schema(self):
        """Colonne DT_MOIS_TRANSACTION en majuscule dans la requête, minuscule dans le schéma."""
        sql = (
            "SELECT acomp.DT_MOIS_TRANSACTION AS partition_date, "
            "CAST(SUM(nb_ope) AS TEXT) AS Nb_Ope "
            "FROM pr_contrats_activite AS acomp "
            "GROUP BY 1"
        )
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
        tables = {
            "pr_contrats_activite": {
                "no_contrat_commercant": "STRING",
                "cd_banque_acquereur": "STRING",
                "DT_MOIS_TRANSACTION": "DATE",
                "nature_operation": "STRING",
                "nb_ope": "INT64",
                "mt_ope": "FLOAT64",
            }
        }

        result = optimize_query(parsed, tables, dialect="bigquery")
        result_sql = result.sql(dialect="bigquery")

        assert "pr_contrats_activite" in result_sql
        assert re.search(r"(?i)dt_mois_transaction", result_sql)
        assert re.search(r"(?i)nb_ope", result_sql)
