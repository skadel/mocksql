"""
Tests documentant les cas où fold_scalar_expressions donne un résultat incorrect
ou rate un fold faute de passer par fix_duck_db_sql.

Trois catégories :
  A. Résultat silencieusement faux  — fold réussi mais valeur incorrecte
  B. Fold raté                      — DuckDB échoue, expression gardée inchangée
  C. Redondances                    — fix désormais inutile (sqlglot 30.x corrige déjà)

Ces tests servent à décider si l'intégration de fix_duck_db_sql dans
_eval_with_duckdb vaut le coût.
"""

import datetime

import duckdb
import pytest
import sqlglot

from build_query.scalar_folder import _eval_with_duckdb, fold_scalar_expressions
from utils.examples import fix_duck_db_sql


def _duck(bq_expr: str) -> str:
    """Transpile une expression BigQuery → DuckDB via sqlglot (ce que fait fold)."""
    return sqlglot.parse_one(bq_expr, read="bigquery").sql(dialect="duckdb")


def _fold(bq_sql: str) -> str:
    ast = sqlglot.parse_one(bq_sql, read="bigquery")
    return fold_scalar_expressions(ast, source_dialect="bigquery").sql(
        dialect="bigquery"
    )


# ===========================================================================
# A. Résultats silencieusement faux
#    Le fold réussit, mais la valeur produite ne correspond pas à BigQuery.
# ===========================================================================


class TestSilentlyWrongResults:
    """
    Ces cas sont les plus dangereux : le SQL optimisé compile et s'exécute,
    mais la constante inlinée est sémantiquement incorrecte.
    """

    def test_substr_zero_start_gives_wrong_value_without_fix(self):
        """
        SUBSTR('ABCD', 0, 2) :
          BigQuery : position 0 clampée à 1 → 'AB'
          DuckDB   : position 0 = avant le 1er char → 'A'  ← valeur fausse

        Sans fix_duck_db_sql, fold_scalar_expressions inline 'A' au lieu de 'AB'.
        """
        duck_expr = _duck("SUBSTR('ABCD', 0, 2)")
        # Confirme que la transpilation sqlglot laisse la position 0 inchangée
        assert ",0," in duck_expr.replace(" ", "") or ", 0," in duck_expr

        # DuckDB exécute sans erreur mais retourne 'A' — résultat incorrect
        result_without_fix = _eval_with_duckdb(duck_expr)
        assert result_without_fix == "A", (
            f"Comportement DuckDB actuel : {result_without_fix!r} "
            "(attendu 'A' tant que le bug existe)"
        )

        # Avec fix_duck_db_sql, la position est corrigée → 'AB'
        fixed_sql = fix_duck_db_sql(f"SELECT {duck_expr}")
        fixed_expr = fixed_sql[len("SELECT ") :]
        result_with_fix = _eval_with_duckdb(fixed_expr)
        assert result_with_fix == "AB"

    def test_substr_zero_start_inlined_correctly_with_fix(self):
        """
        Avec fix_duck_db_sql intégré, fold_scalar_expressions inline la valeur
        correcte 'AB' (BigQuery semantics : position 0 clampée à 1).
        """
        result = _fold("SELECT SUBSTR('ABCD', 0, 2) AS prefix FROM t")
        assert "'AB'" in result, f"Valeur inlinée inattendue dans : {result}"
        assert "'A'" not in result or "'AB'" in result

    def test_canary_sqlglot_still_passes_substr_zero_unchanged(self):
        """
        CANARY — vérifie que sqlglot n'a pas corrigé SUBSTR(str, 0, n) vers DuckDB.
        Si ce test échoue, le gap est comblé côté sqlglot et fix_duck_db_sql
        n'est plus nécessaire pour ce cas.
        """
        duck_expr = _duck("SUBSTR('ABCD', 0, 2)")
        result = duckdb.connect().execute(f"SELECT {duck_expr}").fetchone()[0]
        assert result == "A", (
            f"CANARY : sqlglot corrige désormais SUBSTR(str, 0, n) — résultat : {result!r}. "
            "La correction fix_duck_db_sql est redondante pour ce cas."
        )


# ===========================================================================
# B. Folds ratés
#    DuckDB lève une erreur → fold_scalar_expressions garde l'expression inchangée.
#    Ce n'est pas incorrect, mais le fold n'a pas lieu alors qu'il le pourrait.
# ===========================================================================


class TestMissedFolds:
    """
    L'expression scalaire est foldable en théorie, mais le SQL produit par
    sqlglot n'est pas valide en DuckDB. Le try/except de fold laisse l'expression
    intacte. Avec fix_duck_db_sql, le fold deviendrait possible.
    """

    def test_extract_date_from_timestamp_fails_in_duckdb(self):
        """
        EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00') :
          sqlglot 30 produit : EXTRACT(DATE FROM CAST('...' AS TIMESTAMPTZ))
          DuckDB rejette EXTRACT avec le champ 'DATE' → erreur → fold raté.
        """
        duck_expr = _duck("EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00')")

        # sqlglot 30 enveloppe le littéral dans un CAST TIMESTAMPTZ
        assert "CAST" in duck_expr and "TIMESTAMPTZ" in duck_expr

        with pytest.raises(duckdb.Error):
            _eval_with_duckdb(duck_expr)

    def test_extract_date_fix_duck_db_sql_handles_nested_cast(self):
        """
        Bug corrigé : fix_duck_db_sql gère maintenant les parens imbriquées.
          entrée  : EXTRACT(DATE FROM CAST('...' AS TIMESTAMPTZ))
          regex   : ((?:[^()]+|\\([^()]*\\))+) gère un niveau d'imbrication
          sortie  : CAST(CAST('...' AS TIMESTAMPTZ) AS DATE)  ← SQL valide

        Intégrer fix_duck_db_sql résout maintenant ce cas.
        """
        duck_expr = _duck("EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00')")
        fixed_sql = fix_duck_db_sql(f"SELECT {duck_expr}")
        fixed_expr = fixed_sql[len("SELECT ") :]

        result = _eval_with_duckdb(fixed_expr)
        assert result == datetime.date(2024, 1, 15)

    def test_extract_date_expression_folded_with_fix(self):
        """
        Avec fix_duck_db_sql intégré, EXTRACT(DATE FROM TIMESTAMP '...') est
        maintenant replié en la date littérale correspondante.
        """
        sql = "SELECT EXTRACT(DATE FROM TIMESTAMP '2024-01-15 12:00:00') AS d FROM t"
        result = _fold(sql)
        assert "2024-01-15" in result
        assert "EXTRACT" not in result.upper()

    def test_parse_datetime_fails_in_duckdb(self):
        """
        PARSE_DATETIME('%Y-%m-%d', '2024-01-15') :
          sqlglot 30 inverse les args → PARSE_DATETIME('2024-01-15', '%Y-%m-%d')
          DuckDB ne connaît pas PARSE_DATETIME → erreur → fold raté.
        """
        duck_expr = _duck("PARSE_DATETIME('%Y-%m-%d', '2024-01-15')")

        # sqlglot 30+ met la valeur en premier, le format en second
        assert duck_expr.startswith("PARSE_DATETIME('2024-01-15'")

        with pytest.raises(duckdb.Error):
            _eval_with_duckdb(duck_expr)

    def test_parse_datetime_fix_duck_db_sql_correctly_ordered(self):
        """
        Bug corrigé : fix_duck_db_sql détecte maintenant l'arg format via '%'.
          entrée  : PARSE_DATETIME('2024-01-15', '%Y-%m-%d')   ← valeur 1er (sqlglot 30+)
          sortie  : TRY_STRPTIME('2024-01-15', '%Y-%m-%d')     ← ordre correct
          résultat DuckDB : timestamp non-NULL

        Intégrer fix_duck_db_sql résout maintenant ce cas.
        """
        duck_expr = _duck("PARSE_DATETIME('%Y-%m-%d', '2024-01-15')")
        fixed_sql = fix_duck_db_sql(f"SELECT {duck_expr}")
        fixed_expr = fixed_sql[len("SELECT ") :]

        assert "TRY_STRPTIME" in fixed_expr
        result = _eval_with_duckdb(fixed_expr)
        assert result is not None, (
            f"TRY_STRPTIME a retourné NULL — args peut-être encore inversés : {fixed_expr!r}"
        )

    def test_parse_datetime_expression_folded_with_fix(self):
        """
        Avec fix_duck_db_sql intégré, PARSE_DATETIME('%Y-%m-%d', '2024-01-15')
        est replié en la valeur timestamp correspondante.
        """
        sql = "SELECT PARSE_DATETIME('%Y-%m-%d', '2024-01-15') AS dt FROM t"
        result = _fold(sql)
        assert "PARSE_DATETIME" not in result.upper()
        assert "2024-01-15" in result


# ===========================================================================
# C. Non-gaps — corrections déjà gérées par sqlglot 30.x
#    Ces tests documentent que fix_duck_db_sql serait redondant pour ces cas.
# ===========================================================================


class TestAlreadyHandledBySqlglot:
    """
    Pour ces expressions scalaires, sqlglot produit déjà un SQL DuckDB valide.
    fix_duck_db_sql n'apporterait rien de plus.
    """

    def test_format_date_already_transpiled_to_strftime(self):
        """
        FORMAT_DATE('%Y-%m', DATE '2024-01-15') :
        sqlglot produit STRFTIME(DATE '2024-01-15', '%Y-%m') — valide en DuckDB.
        """
        duck_expr = _duck("FORMAT_DATE('%Y-%m', DATE '2024-01-15')")
        assert "STRFTIME" in duck_expr.upper()
        result = _eval_with_duckdb(duck_expr)
        assert result == "2024-01"

    def test_safe_cast_already_transpiled_to_try_cast(self):
        """
        SAFE_CAST('123' AS INT64) :
        sqlglot produit TRY_CAST('123' AS BIGINT) — valide en DuckDB.
        """
        duck_expr = _duck("SAFE_CAST('123' AS INT64)")
        assert "TRY_CAST" in duck_expr.upper()
        result = _eval_with_duckdb(duck_expr)
        assert result == 123

    def test_date_diff_already_transpiled(self):
        """
        DATE_DIFF(DATE '2024-12-31', DATE '2024-01-01', DAY) :
        sqlglot transpile vers une expression DuckDB valide (arithmetic ou date_diff).
        """
        duck_expr = _duck("DATE_DIFF(DATE '2024-12-31', DATE '2024-01-01', DAY)")
        result = _eval_with_duckdb(duck_expr)
        assert result == 365
