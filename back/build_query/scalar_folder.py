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

# Un `Identifier` est un *nom* (colonne, alias, table, CTE), jamais une expression
# scalaire évaluable. Sans ça, en descendant dans une `Column` (disqualifiante) on
# atteint son `Identifier` enfant — pur et non-littéral → vu comme foldable. Chaque
# nom de la requête était alors transpilé vers DuckDB (~4 ms) puis exécuté
# (`SELECT col_x` → échec → laissé intact) : inoffensif mais O(noms) transpilations
# gâchées, dominant le temps de fold sur les vraies requêtes.
_IDENTIFIER_TYPES = (exp.Identifier,)

# Un seul tuple de types disqualifiants → une seule passe de détection.
_DISQUALIFYING_TYPES = (
    _COLUMN_TYPES
    + _NONDETERMINISTIC_TYPES
    + _AGGREGATE_TYPES
    + _WINDOW_TYPES
    + _SUBQUERY_TYPES
    + _STRUCTURAL_TYPES
    + _IDENTIFIER_TYPES
)

# Littéraux déjà atomiques — inutile de les envoyer à DuckDB
_ALREADY_LITERAL = (exp.Literal, exp.Null, exp.Boolean)

# Nœuds structurels à ne pas folder en tant que tout (leurs enfants le sont) :
# un Alias `expr AS name` doit conserver son wrapper, seul `expr` est foldé.
_SKIP_IN_TRANSFORM = (exp.Alias,)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_hex_string_concat(node: exp.Expression) -> bool:
    """Le nœud est-il une concaténation préfixée par le littéral ``'0x'`` ?

    C'est l'idiome hexa Snowflake (``'0x' || …`` casté en numérique, cf.
    ``utils.examples._fix_snowflake_hex_cast``). Quand ses opérandes sont tous
    littéraux, il est scalaire pur donc foldable — mais l'évaluer ici sur une
    connexion nue le replierait en ``NULL`` (DuckDB ne parse pas l'hexa runtime),
    et le fixer aval, qui tourne APRÈS le fold, ne verrait plus jamais le CAST.
    On le déclare donc impur pour que le folder descende dedans sans le replier,
    laissant l'idiome intact jusqu'au fixer. Sonde la feuille littérale la plus à
    gauche (les ``||`` chaînés sont imbriqués à gauche par sqlglot).
    """
    while isinstance(node, exp.Paren):
        node = node.this
    if isinstance(node, exp.DPipe):
        return _is_hex_string_concat(node.this) or (
            isinstance(node.this, exp.Literal)
            and node.this.is_string
            and node.this.this.lower() == "0x"
        )
    if isinstance(node, exp.Concat):
        exprs = node.expressions
        return bool(
            exprs
            and isinstance(exprs[0], exp.Literal)
            and exprs[0].is_string
            and exprs[0].this.lower() == "0x"
        )
    return False


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

    # --- Passe 1a : pureté scalaire bottom-up, en UNE seule passe O(n).
    # `pure[id(node)]` = aucun type disqualifiant (colonne, agrégat, fenêtre,
    # sous-requête, nœud structurel, fonction non-déterministe) dans tout le
    # sous-arbre. L'ancienne version rappelait `_is_foldable` (= un `walk()`
    # complet) à chaque nœud considéré ; sur un AST majoritairement non-foldable
    # cela re-parcourait des sous-arbres qui se recouvrent → O(n²). Ici chaque
    # nœud est visité une fois et combine la pureté de ses enfants.
    pure: dict[int, bool] = {}

    def _compute_pure(node: exp.Expression) -> bool:
        # L'idiome hexa '0x' || … est laissé au fixer aval : impur ici pour ne
        # pas être replié en NULL avant que le CAST ne soit réécrit.
        node_pure = not isinstance(node, _DISQUALIFYING_TYPES) and not (
            _is_hex_string_concat(node)
        )
        for value in node.args.values():
            if isinstance(value, exp.Expression):
                # NB : pas de court-circuit — on doit calculer la pureté de TOUS
                # les descendants pour pouvoir descendre dedans en passe 1b.
                child_pure = _compute_pure(value)
                node_pure = node_pure and child_pure
            elif isinstance(value, list):
                for v in value:
                    if isinstance(v, exp.Expression):
                        child_pure = _compute_pure(v)
                        node_pure = node_pure and child_pure
        pure[id(node)] = node_pure
        return node_pure

    _compute_pure(ast)

    def _foldable(node: exp.Expression) -> bool:
        # Foldable = sous-arbre scalaire pur, mais pas un littéral déjà atomique.
        return pure[id(node)] and not isinstance(node, _ALREADY_LITERAL)

    # --- Passe 1b : collecte des sous-arbres scalaires maximaux (top-down, disjoints).
    foldable: list[exp.Expression] = []

    def _consider(node: exp.Expression) -> None:
        # Alias : on ne replie jamais le wrapper `AS name`, seulement ses enfants.
        if isinstance(node, _SKIP_IN_TRANSFORM):
            _descend(node)
        elif _foldable(node):
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

    if isinstance(ast, _SKIP_IN_TRANSFORM) or not _foldable(ast):
        _descend(ast)
    else:
        foldable.append(ast)  # AST entièrement scalaire (cas dégénéré)

    if not foldable:
        return ast

    # --- Évaluation par expression, mémoïsée et sur connexion DuckDB partagée.
    # L'opération coûteuse est `node.sql(dialect="duckdb")` : le générateur DuckDB
    # de sqlglot **re-parse des templates** de fonction (PARSE_DATE/FORMAT_DATE/
    # CAST…) à chaque appel → ~4 ms par nœud, soit ~180× le coût d'un rendu dans
    # le dialecte source (~0,02 ms). Le cache est donc clé par le **rendu source**
    # (bon marché), calculé AVANT toute transpilation : les expressions répétées
    # (mêmes PARSE_DATE(...) des dizaines de fois) ne paient la transpilation
    # DuckDB **qu'une seule fois**. La connexion partagée évite en plus le coût
    # d'ouverture par nœud.
    conn = duckdb.connect(":memory:")
    # _FAILED distingue « non foldable » d'une vraie valeur NULL, et permet de
    # cacher aussi les échecs (pas de re-tentative).
    cache: dict[str, Any] = {}
    _MISS = object()
    _FAILED = object()
    transpile_ms = exec_ms = 0.0
    n_eval = 0
    try:
        for node in foldable:
            t0 = time.perf_counter()
            try:
                key = node.sql(dialect=source_dialect)  # rendu source bon marché
            except Exception:
                continue  # non rendable → laissé intact
            value = cache.get(key, _MISS)
            if value is _MISS:
                n_eval += 1
                try:
                    expr_duckdb = node.sql(dialect="duckdb")  # coûteux → une fois/expr
                    fixed = fix_duck_db_sql(f"SELECT {expr_duckdb}", source_dialect)
                    transpile_ms += (time.perf_counter() - t0) * 1000
                    t1 = time.perf_counter()
                    row = conn.execute(fixed).fetchone()
                    value = row[0] if row else None
                    exec_ms += (time.perf_counter() - t1) * 1000
                except Exception:
                    value = _FAILED  # non transpilable / DuckDB refuse → laissé intact
                cache[key] = value
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
