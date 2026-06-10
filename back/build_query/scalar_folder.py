"""
Constant folding for scalar SQL expressions.

Expressions de type scalaire pur (sans référence de colonne, sans agrégat,
sans fonction non-déterministe) sont transpilées vers DuckDB, évaluées, et
remplacées par le littéral résultant dans l'AST optimisé.

Point d'entrée : fold_scalar_expressions(ast, source_dialect)

Performance : le repli est **top-down** — dès qu'un nœud est entièrement scalaire,
on le replie en un seul appel DuckDB et on ne descend pas dans ses enfants (ils
sont subsumés). Combiné à un ``_is_foldable`` à passe unique et arrêt précoce, et
à une **unique connexion DuckDB** réutilisée pour tout l'appel, cela évite le
O(n²) + N connexions de l'ancienne version ``ast.transform`` (post-order).
"""

import decimal
import logging
import time
from typing import Any

import duckdb
from sqlglot import expressions as exp

import utils.logger  # noqa: F401 — enregistre le niveau DIAG (15)

logger = logging.getLogger("timing")

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

# Nœuds structurels : une expression scalaire pure ne contient jamais de table,
# de clause FROM/JOIN/WITH ni de sous-SELECT. Sans ça, un `SELECT f(1) FROM t`
# (aucune colonne) serait vu comme foldable et évalué en bloc (→ échec, et l'expr
# interne jamais repliée).
_STRUCTURAL_TYPES = (exp.Select, exp.From, exp.Join, exp.With, exp.Table)

# Un seul tuple de types disqualifiants → une seule passe de détection.
_DISQUALIFYING_TYPES = (
    _COLUMN_TYPES
    + _NONDETERMINISTIC_TYPES
    + _AGGREGATE_TYPES
    + _WINDOW_TYPES
    + _SUBQUERY_TYPES
    + _STRUCTURAL_TYPES
)

# Littéraux déjà atomiques — inutile de les envoyer à DuckDB
_ALREADY_LITERAL = (exp.Literal, exp.Null, exp.Boolean)

# Nœuds structurels à ne pas folder en tant que tout (leurs enfants le sont) :
# un Alias `expr AS name` doit conserver son wrapper, seul `expr` est foldé.
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

    Passe unique sur le sous-arbre avec arrêt précoce au premier type
    disqualifiant — O(taille du sous-arbre) au pire, immédiat dès qu'une colonne
    est rencontrée (cas dominant sur les vraies requêtes).
    """
    if isinstance(node, _ALREADY_LITERAL):
        return False
    for sub in node.walk():
        if isinstance(sub, _DISQUALIFYING_TYPES):
            return False
    return True


def _eval_with_duckdb(expr_sql: str) -> Any:
    """Exécute `SELECT <expr_sql>` sur une connexion DuckDB in-memory isolée.

    Helper unitaire (utilisé par les tests) ; le chemin de repli principal
    réutilise une connexion partagée pour éviter le coût d'ouverture par nœud.
    """
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
    Parcourt l'AST en profondeur et replie toute sous-expression scalaire pure
    en son littéral calculé.

    Stratégie top-down : on cherche le plus grand sous-arbre entièrement scalaire,
    on le replie en **un seul** appel DuckDB, et on ne descend pas dedans. Sinon
    on recurse dans les enfants. Les nœuds Alias ne sont jamais foldés en tant que
    tout (leur wrapper `AS name` est préservé), seuls leurs enfants le sont.

    Pour chaque sous-arbre foldable :
    1. Transpile vers DuckDB via .sql(dialect="duckdb")
    2. Applique fix_duck_db_sql si source_dialect BigQuery/Snowflake (corrige
       SUBSTR pos 0, PARSE_DATE, EXTRACT(DATE…), etc.)
    3. Exécute SELECT <expr> sur une connexion DuckDB in-memory partagée
    4. Remplace le nœud par le littéral résultant

    En cas d'erreur DuckDB, l'expression est conservée telle quelle (fallback
    silencieux). Mute et retourne l'AST fourni.
    """
    from utils.examples import fix_duck_db_sql

    # --- Passe 1 : collecte des sous-arbres scalaires maximaux (top-down, disjoints).
    foldable: list[exp.Expression] = []

    def _consider(node: exp.Expression) -> None:
        # Alias : on ne replie jamais le wrapper `AS name`, seulement ses enfants.
        if isinstance(node, _SKIP_IN_TRANSFORM):
            _descend(node)
        elif _is_foldable(node):
            foldable.append(node)  # maximal → on ne descend pas dedans
        else:
            _descend(node)

    def _descend(node: exp.Expression) -> None:
        for value in node.args.values():
            if isinstance(value, exp.Expression):
                _consider(value)
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, exp.Expression):
                        _consider(v)

    if isinstance(ast, _SKIP_IN_TRANSFORM) or not _is_foldable(ast):
        _descend(ast)
    else:
        foldable.append(ast)  # AST entièrement scalaire (cas dégénéré)

    if not foldable:
        return ast

    # --- Évaluation par expression, mémoïsée et sur connexion DuckDB partagée.
    # `node.sql(dialect="duckdb")` transpile BigQuery→DuckDB ; c'est l'opération
    # coûteuse (re-parse de templates PARSE_DATE/FORMAT_DATE). Le cache élimine
    # les répétitions (mêmes PARSE_DATE(...) répétés des dizaines de fois) et la
    # connexion partagée évite le coût d'ouverture par nœud.
    conn = duckdb.connect(":memory:")
    # Cache par SQL d'expression. _FAILED distingue « non foldable » d'une vraie
    # valeur NULL, et permet de cacher aussi les échecs (pas de re-tentative).
    cache: dict[str, Any] = {}
    _MISS = object()
    _FAILED = object()
    transpile_ms = exec_ms = 0.0
    n_eval = 0
    try:
        for node in foldable:
            t0 = time.perf_counter()
            try:
                expr_duckdb = node.sql(dialect="duckdb")
            except Exception:
                continue  # non transpilable → laissé intact
            value = cache.get(expr_duckdb, _MISS)
            if value is _MISS:
                n_eval += 1
                try:
                    fixed = fix_duck_db_sql(f"SELECT {expr_duckdb}", source_dialect)
                    transpile_ms += (time.perf_counter() - t0) * 1000
                    t1 = time.perf_counter()
                    row = conn.execute(fixed).fetchone()
                    value = row[0] if row else None
                    exec_ms += (time.perf_counter() - t1) * 1000
                except Exception:
                    value = _FAILED  # DuckDB refuse → laissé intact
                cache[expr_duckdb] = value
            if value is not _FAILED:
                node.replace(_to_literal(value))
    finally:
        conn.close()

    logger.diag(
        "[fold] %d nœuds foldables, %d distincts évalués · transpile=%.0fms exec=%.0fms",
        len(foldable),
        n_eval,
        transpile_ms,
        exec_ms,
    )
    return ast
