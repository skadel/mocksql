"""
Constant folding for scalar SQL expressions.

Expressions de type scalaire pur (sans référence de colonne, sans agrégat,
sans fonction non-déterministe) sont transpilées vers DuckDB, évaluées, et
remplacées par le littéral résultant dans l'AST optimisé.

Point d'entrée : fold_scalar_expressions(ast, source_dialect)
"""

import decimal
from typing import Any

import duckdb
from sqlglot import expressions as exp

# ---------------------------------------------------------------------------
# Types qui rendent une expression non-foldable
# ---------------------------------------------------------------------------

_COLUMN_TYPES = (exp.Column, exp.Star)

_NONDETERMINISTIC_TYPES = (
    exp.CurrentDate,
    exp.CurrentTime,
    exp.CurrentTimestamp,
    exp.CurrentDatetime,
)

_AGGREGATE_TYPES = (exp.AggFunc,)
_WINDOW_TYPES = (exp.Window,)
_SUBQUERY_TYPES = (exp.Subquery,)

# Littéraux déjà atomiques — inutile de les envoyer à DuckDB
_ALREADY_LITERAL = (exp.Literal, exp.Null, exp.Boolean)

# Nœuds structurels à ne pas folded en tant que tout (leurs enfants le sont)
_SKIP_IN_TRANSFORM = (exp.Alias,)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_foldable(node: exp.Expression) -> bool:
    """
    Retourne True si node est une expression scalaire pure évaluable par DuckDB :
    - aucune référence de colonne ou étoile
    - aucune fonction non-déterministe (CURRENT_DATE, etc.)
    - aucun agrégat, fenêtre, ou sous-requête
    - pas déjà un littéral atomique
    """
    if isinstance(node, _ALREADY_LITERAL):
        return False
    for cls in _COLUMN_TYPES + _NONDETERMINISTIC_TYPES:
        if node.find(cls) is not None:
            return False
    for cls in _AGGREGATE_TYPES + _WINDOW_TYPES + _SUBQUERY_TYPES:
        if node.find(cls) is not None:
            return False
    return True


def _eval_with_duckdb(expr_sql: str) -> Any:
    """Exécute `SELECT <expr_sql>` sur une connexion DuckDB in-memory isolée."""
    with duckdb.connect(":memory:") as conn:
        row = conn.execute(f"SELECT {expr_sql}").fetchone()
        return row[0] if row else None


def _to_literal(value: Any) -> exp.Expression:
    """Convertit une valeur Python scalaire en nœud sqlglot."""
    if value is None:
        return exp.null()
    if isinstance(value, bool):
        return exp.Boolean(this=value)
    if isinstance(value, int):
        return exp.Literal.number(value)
    if isinstance(value, float):
        return exp.Literal.number(value)
    if isinstance(value, decimal.Decimal):
        return exp.Literal.number(float(value))
    # datetime.date, datetime.datetime, str, et tout autre type → littéral texte
    return exp.Literal.string(str(value))


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------


def fold_scalar_expressions(ast: exp.Expression, source_dialect: str) -> exp.Expression:
    """
    Parcourt tout l'AST (SELECT, WHERE, HAVING, JOIN ON…) en profondeur et
    replie toute sous-expression scalaire pure en son littéral calculé.

    Stratégie : ast.transform() depth-first → les enfants sont traités avant
    les parents. Une expression scalaire est d'abord réduite à ses atomes, puis
    le nœud parent (désormais uniquement composé de littéraux) est à son tour
    évalué en une seule passe DuckDB.

    Pour chaque nœud foldable :
    1. Transpile vers DuckDB via .sql(dialect="duckdb")
    2. Applique fix_duck_db_sql si source_dialect BigQuery/Snowflake (corrige
       SUBSTR pos 0, PARSE_DATE, EXTRACT(DATE…), etc.)
    3. Exécute SELECT <expr> localement
    4. Remplace le nœud par le littéral résultant

    En cas d'erreur DuckDB, l'expression est conservée telle quelle (fallback
    silencieux). Retourne une copie de l'AST avec les remplacements appliqués.
    """
    from utils.examples import fix_duck_db_sql

    def _fold(node: exp.Expression) -> exp.Expression:
        # Les alias sont des conteneurs structurels : leurs enfants sont traités
        # depth-first, mais l'alias lui-même ne doit pas être évalué comme expr.
        if isinstance(node, _SKIP_IN_TRANSFORM):
            return node
        if not _is_foldable(node):
            return node
        try:
            expr_duckdb = node.sql(dialect="duckdb")
            # Corrige les incompatibilités dialect-spécifiques avant l'exécution
            fixed_sql = fix_duck_db_sql(f"SELECT {expr_duckdb}", source_dialect)
            expr_fixed = fixed_sql[len("SELECT ") :]
            value = _eval_with_duckdb(expr_fixed)
            return _to_literal(value)
        except Exception:
            return node  # fallback : on garde l'expression originale

    return ast.transform(_fold)
