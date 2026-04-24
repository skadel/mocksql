"""
Vérifie que validate_query gère correctement une requête avec un commentaire SQL
au-dessus du WITH. Le bug original (remove_with_start ne gérait pas les commentaires)
a été corrigé : la requête doit maintenant retourner 'success'.
"""

import asyncio

import sqlglot
from unittest.mock import AsyncMock, patch

SQL_WITH_COMMENT = """\
-- CTE pour GDP per capita
WITH gdp_cte AS (
  SELECT
    country_code,
    value AS gdp_per_capita
  FROM
    `bigquery-public-data.world_bank_wdi.indicators_data`
) SELECT sum(gdp_per_capita) FROM gdp_cte"""

TABLES = {"indicators_data": {"country_code": "STRING", "value": "FLOAT"}}

STATE = {
    "query": SQL_WITH_COMMENT,
    "project": "test-project",
    "dialect": "bigquery",
    "user": "test-user",
    "messages": [],
    "route": "",
    "optimize": False,
}


class TestValidateQueryCteComment:
    def test_cte_with_comment_returns_success(self):
        """
        validate_query doit gérer correctement un commentaire SQL au-dessus du WITH.
        remove_with_start strip les commentaires avant de chercher 'with', donc
        split_query réussit et validate_query retourne 'success'.
        """
        optimized_ast = sqlglot.parse_one(SQL_WITH_COMMENT, read="bigquery")

        with (
            patch(
                "build_query.validator.compile_query", new_callable=AsyncMock
            ) as mock_compile,
            patch(
                "build_query.validator.get_tables_mapping", new_callable=AsyncMock
            ) as mock_tables,
            patch(
                "build_query.validator.evaluate_and_fix_query", new_callable=AsyncMock
            ) as mock_eval,
        ):
            mock_compile.return_value = 0
            mock_tables.return_value = TABLES
            mock_eval.return_value = (SQL_WITH_COMMENT, optimized_ast, [], {})

            from build_query.validator import validate_query

            result = asyncio.run(
                validate_query(
                    code=SQL_WITH_COMMENT,
                    project="test-project",
                    dialect="bigquery",
                    parent=None,
                    state=STATE,
                )
            )

        assert result["status"] == "success"
