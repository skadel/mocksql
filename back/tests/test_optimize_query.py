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


class TestOptimizeQueryPreservesIdentifierCase:
    """L'optimiseur ne doit pas re-normaliser les identifiants en lowercase :
    sur un UNPIVOT, le nom de colonne devient une *valeur* de sortie, et
    BigQuery préserve la casse — la lowercaser changerait la sémantique."""

    def test_unpivot_in_list_preserves_case(self):
        sql = "SELECT id, Jan, Feb FROM t UNPIVOT(val FOR mois IN (Jan, Feb))"
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
        tables = {"t": {"id": "INT64", "Jan": "INT64", "Feb": "INT64"}}

        result_sql = optimize_query(parsed, tables, dialect="bigquery").sql(
            dialect="bigquery"
        )

        # La liste IN de l'UNPIVOT doit garder la casse d'origine (deviendra une
        # valeur 'Jan'/'Feb' sur DuckDB, comme sur BigQuery).
        assert "Jan" in result_sql
        assert "Feb" in result_sql
        assert "jan" not in result_sql.replace("Jan", "")
        assert "feb" not in result_sql.replace("Feb", "")

    def test_output_alias_preserves_case(self):
        sql = "SELECT col_a AS MyValue FROM t"
        parsed = sqlglot.parse_one(sql, dialect="bigquery")
        tables = {"t": {"col_a": "INT64"}}

        result_sql = optimize_query(parsed, tables, dialect="bigquery").sql(
            dialect="bigquery"
        )

        assert "MyValue" in result_sql
