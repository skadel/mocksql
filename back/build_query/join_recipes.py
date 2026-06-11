"""Recettes de jointure pré-calculées pour les clés de JOIN dérivées (P1a).

Quand une clé de JOIN est le produit d'une transformation (``CASE``,
``SUBSTR``+``TRIM``, ``CAST``…), demander au LLM d'inverser mentalement la
transformation échoue de façon récurrente (audit requête bancaire :
``cd_chef_file`` attendu ``'1'`` mais généré ``'BP'``, ``cd_type_carte_smp``
attendu ``'ROD'`` après SUBSTR au lieu de ``'PROD1'``…). Ces transformations
sont déterministes : l'inversion est pré-calculée ici, côté Python, et injectée
comme consigne concrète dans le bloc ``<constraints>`` du générateur et dans le
SYSTEM de l'agent conversationnel (boucle ``bad_data``).

Règles d'inversion, par type de dérivation :

* ``CASE`` à branches 100 % littérales → énumération des couples
  (valeur source → valeur clé) ;
* chaîne de fonctions sur une seule colonne (``SUBSTR``/``TRIM``/``SPLIT``/…)
  → **vérification forward sur DuckDB** : on essaie des gabarits d'entrée
  candidats et on inclut le couple vérifié dans la recette (coût : quelques ms,
  zéro LLM) ;
* ``CAST``/``SAFE_CAST`` → contrainte de format des deux côtés ;
* ``CONCAT(col, littéraux)`` → décomposition préfixe/suffixe ;
* sinon → recette générique (« choisis la valeur source telle que <expr> =
  valeur de l'autre côté ») — jamais pire que la consigne prose du SYSTEM.

Aucune recette n'est émise pour un JOIN sur colonnes nues (pas de bruit).
"""

import logging

import sqlglot
from sqlglot import expressions as exp

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.constraint_simplifier import (
    ColumnRef,
    _col_ref,
    _collect_aliases,
    _LineageResolver,
    _schemas_to_sqlglot,
)

logger = logging.getLogger(__name__)

_PLACEHOLDER = "PROD1"
# Gabarits d'entrée candidats pour la vérification forward, du plus probable au
# moins probable. "{v}" en premier : si la valeur nue traverse la transformation
# inchangée, la clé n'est pas un piège → aucune recette (anti-bruit).
_CANDIDATE_TEMPLATES = ["{v}", "'{v}'", '"{v}"', " {v} ", "({v})", "[{v}]"]

# Sentinelle interne : la transformation est neutre pour une valeur nue
# (ex. TRIM sur valeur déjà propre) → ne pas émettre de recette.
_IDENTITY_FOR_PLAIN_VALUES = object()

_recipes_cache: dict[tuple[str, str], list[str]] = {}
_CACHE_MAXSIZE = 32


def build_join_recipes(
    sql: str, dialect: str = "bigquery", schema: list[dict] | None = None
) -> list[str]:
    """Retourne une recette (chaîne prête à injecter) par clé de JOIN dérivée.

    Résultat mis en cache par ``(sql, dialect)`` — l'analyse re-parse et
    re-qualifie le SQL, mais la sortie est déterministe, donc les retries sur le
    même SQL ne recalculent pas.
    """
    if not sql:
        return []
    cache_key = (sql, dialect)
    cached = _recipes_cache.get(cache_key)
    if cached is not None:
        return cached
    try:
        recipes = _build(sql, dialect, schema)
    except Exception as exc:
        logger.warning(
            "join_recipes failed (dialect=%s): %s — sql:\n%s", dialect, exc, sql
        )
        recipes = []
    if len(_recipes_cache) >= _CACHE_MAXSIZE:
        _recipes_cache.pop(next(iter(_recipes_cache)))
    _recipes_cache[cache_key] = recipes
    return recipes


def build_join_recipes_block(
    sql: str, dialect: str = "bigquery", schema: list[dict] | None = None
) -> str:
    """Bloc prompt prêt à concaténer dans ``<constraints>`` (vide si aucune recette)."""
    recipes = build_join_recipes(sql, dialect, schema)
    if not recipes:
        return ""
    lines = "\n".join(f"- {r}" for r in recipes)
    return (
        "\n**Recettes de jointure (clés dérivées) — appliquer telles quelles :**\n"
        f"{lines}\n"
    )


def _build(sql: str, dialect: str, schema: list[dict] | None) -> list[str]:
    try:
        statement = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return []
    if statement is None:
        return []

    sqlglot_schema = _schemas_to_sqlglot(schema) if schema else None
    resolver = _LineageResolver(statement, sqlglot_schema, dialect)

    alias_map: dict[str, str] = {}
    for tbl in statement.find_all(exp.Table):
        real = tbl.name.lower()
        alias = tbl.alias.lower() if tbl.alias else real
        if real:
            alias_map[alias] = real
            alias_map[real] = real
    for sel in statement.find_all(exp.Select):
        _collect_aliases(sel, alias_map)

    recipes: list[str] = []
    seen: set[str] = set()
    for sel in statement.find_all(exp.Select):
        for join in sel.args.get("joins") or []:
            on = join.args.get("on")
            if on is None:
                continue
            for eq in on.find_all(exp.EQ):
                recipe = _recipe_for_equality(eq, alias_map, resolver, dialect)
                if recipe and recipe not in seen:
                    seen.add(recipe)
                    recipes.append(recipe)
    return recipes


# ─── Détection ────────────────────────────────────────────────────────────────


def _derived_expr_of_column(
    raw: ColumnRef, resolver: _LineageResolver
) -> exp.Expression | None:
    """Première dérivation non-identité dans le lineage de *raw*, ou None.

    Marche l'arbre de lineage depuis la racine : le premier nœud non-Table dont
    l'expression n'est pas une colonne nue est la dérivation vue depuis la clé
    de JOIN. Les nœuds agrégats sont écartés : liaison parfois spurious sur les
    clés GROUP BY (cf. garde de ``_resolve_via_lineage``) et non inversibles de
    toute façon.
    """
    if resolver._effective_cte_name(raw) is None:
        return None
    try:
        node = resolver._run_lineage(raw)
    except Exception as exc:
        logger.debug("join_recipes lineage failed for %s: %s", raw, exc)
        return None
    for n in node.walk():
        if isinstance(n.expression, exp.Table):
            continue
        expr = n.expression
        col_expr = expr.this if isinstance(expr, exp.Alias) else expr
        if isinstance(col_expr, exp.Column):
            continue
        if isinstance(col_expr, exp.AggFunc):
            return None
        return col_expr
    return None


def _derivation_of(
    side: exp.Expression, alias_map: dict[str, str], resolver: _LineageResolver
) -> exp.Expression | None:
    """Dérivation de *side* (côté d'une égalité de JOIN ON), ou None si colonne nue."""
    if isinstance(side, exp.Column):
        raw = _col_ref(side, alias_map)
        if raw is None or raw.table == "__unknown__":
            return None
        return _derived_expr_of_column(raw, resolver)
    # Expression inline dans le ON (ex. b.k = CASE WHEN a.reseau = 'BP' …) :
    # la dérivation est l'expression elle-même, si elle référence une colonne.
    if any(True for _ in side.find_all(exp.Column)):
        return side
    return None


def _tbl_label(node: exp.Expression, alias_map: dict[str, str]) -> str:
    cols = [node] if isinstance(node, exp.Column) else list(node.find_all(exp.Column))
    for c in cols:
        if c.table:
            alias = c.table.lower()
            return alias_map.get(alias, alias)
    return "?"


def _or_is_null_note(eq: exp.EQ, key_side: exp.Expression, dialect: str) -> str:
    """Note sur la branche ``OR <clé> IS NULL`` sœur de l'égalité, s'il y en a une."""
    parent = eq.parent
    while isinstance(parent, exp.Paren):
        parent = parent.parent
    if not isinstance(parent, exp.Or):
        return ""
    for is_node in parent.find_all(exp.Is):
        right = is_node.args.get("expression") or is_node.args.get("to")
        if isinstance(right, exp.Null) and is_node.this == key_side:
            return (
                f" La branche `{is_node.sql(dialect=dialect)}` est satisfaite par une "
                "valeur source hors des cas énumérés (la clé dérivée vaut alors NULL)."
            )
    return ""


def _recipe_for_equality(
    eq: exp.EQ,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    dialect: str,
) -> str | None:
    left = eq.args.get("this")
    right = eq.args.get("expression")
    if left is None or right is None:
        return None

    for side, other in ((left, right), (right, left)):
        derivation = _derivation_of(side, alias_map, resolver)
        if derivation is None:
            continue

        other_sql = other.sql(dialect=dialect)
        body = _invert(derivation, other_sql, dialect)
        if body is _IDENTITY_FOR_PLAIN_VALUES:
            return None
        if isinstance(side, exp.Column):
            key_name = side.name
        elif isinstance(other, exp.Column):
            key_name = other.name
        else:
            key_name = side.sql(dialect=dialect)
        header = (
            f"JOIN {_tbl_label(side, alias_map)} ↔ {_tbl_label(other, alias_map)} "
            f"sur {key_name}"
        )
        return f"{header} : {body}{_or_is_null_note(eq, side, dialect)}"
    return None


# ─── Règles d'inversion ───────────────────────────────────────────────────────


def _invert(expr: exp.Expression, other_sql: str, dialect: str):
    """Corps de la recette pour *expr* (sentinelle si aucune recette nécessaire)."""
    recipe = _try_case_recipe(expr, other_sql, dialect)
    if recipe is not None:
        return recipe
    recipe = _try_cast_recipe(expr, other_sql, dialect)
    if recipe is not None:
        return recipe
    forward = _try_forward_verified(expr, other_sql, dialect)
    if forward is not None:
        return forward
    recipe = _try_concat_recipe(expr, other_sql, dialect)
    if recipe is not None:
        return recipe
    return _generic_fallback(expr, other_sql, dialect)


def _try_case_recipe(expr: exp.Expression, other_sql: str, dialect: str) -> str | None:
    """CASE à branches 100 % littérales → énumération (valeur source → valeur clé)."""
    if not isinstance(expr, exp.Case):
        return None
    operand = expr.this  # CASE <operand> WHEN lit THEN … (forme simple)
    pairs: list[tuple[str, str, str]] = []
    src_cols: list[str] = []
    for if_ in expr.args.get("ifs") or []:
        cond = if_.this
        val = if_.args.get("true")
        if not isinstance(val, (exp.Literal, exp.Null)):
            return None  # branche non littérale → fallback générique
        if operand is not None and isinstance(operand, exp.Column):
            # CASE col WHEN 'BP' THEN '1' …
            if not isinstance(cond, exp.Literal):
                return None
            src_sql = operand.sql(dialect=dialect)
            lit_sql = cond.sql(dialect=dialect)
        elif (
            isinstance(cond, exp.EQ)
            and isinstance(cond.this, exp.Column)
            and isinstance(cond.expression, exp.Literal)
        ):
            # CASE WHEN col = 'BP' THEN '1' …
            src_sql = cond.this.sql(dialect=dialect)
            lit_sql = cond.expression.sql(dialect=dialect)
        else:
            return None
        if src_sql not in src_cols:
            src_cols.append(src_sql)
        pairs.append((src_sql, lit_sql, val.sql(dialect=dialect)))
    if not pairs:
        return None
    default = expr.args.get("default")
    if default is not None and not isinstance(default, (exp.Literal, exp.Null)):
        return None
    default_sql = default.sql(dialect=dialect) if default is not None else "NULL"
    mapping = " ; ".join(
        f"{src} = {lit} → la clé vaut {key}" for src, lit, key in pairs
    )
    return (
        f"la clé est dérivée par CASE sur {', '.join(src_cols)}. Pose la valeur "
        f"SOURCE, pas la valeur finale : {mapping} ; sinon la clé vaut "
        f"{default_sql}. L'autre côté ({other_sql}) doit porter la valeur DÉRIVÉE "
        "correspondante."
    )


def _try_cast_recipe(expr: exp.Expression, other_sql: str, dialect: str) -> str | None:
    """CAST / SAFE_CAST → contrainte de format identique des deux côtés."""
    if not isinstance(expr, (exp.Cast, exp.TryCast)):
        return None
    cols = list(expr.find_all(exp.Column))
    if len(cols) != 1:
        return None
    to_type = expr.args.get("to")
    type_sql = to_type.sql(dialect=dialect).upper() if to_type else "?"
    fn = "SAFE_CAST" if isinstance(expr, exp.TryCast) else "CAST"
    col_sql = cols[0].sql(dialect=dialect)
    return (
        f"la clé passe par {fn}({col_sql} AS {type_sql}) : pose dans {col_sql} une "
        f"valeur convertible en {type_sql} (ex. chaîne numérique), égale après "
        f"conversion à la valeur de l'autre côté ({other_sql})."
    )


def _try_forward_verified(expr: exp.Expression, other_sql: str, dialect: str):
    """Chaîne de fonctions sur une seule colonne → vérification forward DuckDB.

    Plutôt que d'inverser symboliquement, essaie des gabarits d'entrée candidats,
    exécute ``SELECT <expr(candidat)>`` sur DuckDB local et retient le premier
    gabarit dont la sortie vaut la valeur cible. Retourne la sentinelle
    ``_IDENTITY_FOR_PLAIN_VALUES`` si la valeur nue traverse inchangée (aucun
    piège → pas de recette), None si aucun gabarit ne fonctionne (fallback).
    """
    cols = list(expr.find_all(exp.Column))
    if len({(c.table, c.name) for c in cols}) != 1:
        return None
    col_sql = cols[0].sql(dialect=dialect)

    try:
        import duckdb
    except ImportError:  # pragma: no cover
        return None

    for tmpl in _CANDIDATE_TEMPLATES:
        candidate = tmpl.format(v=_PLACEHOLDER)
        try:
            probe = expr.copy()
            for c in list(probe.find_all(exp.Column)):
                c.replace(exp.Literal.string(candidate))
            select_sql = f"SELECT {probe.sql(dialect=dialect)}"
            duck_sql = sqlglot.transpile(select_sql, read=dialect, write="duckdb")[0]
            out = duckdb.sql(duck_sql).fetchone()[0]
        except Exception as exc:
            logger.debug(
                "forward check failed (candidate=%r): %s — sql: %s",
                candidate,
                exc,
                expr.sql(dialect=dialect),
            )
            continue
        if out == _PLACEHOLDER:
            if tmpl == "{v}":
                return _IDENTITY_FOR_PLAIN_VALUES
            display = tmpl.format(v="X")
            return (
                f"la colonne source subit {expr.sql(dialect=dialect)}. Pour que la "
                f'valeur transformée soit X, écris {col_sql} = "{display}" '
                f'(vérifié : "{candidate}" → {_PLACEHOLDER}). L\'autre côté '
                f"({other_sql}) doit valoir X."
            )
    return None


def _try_concat_recipe(
    expr: exp.Expression, other_sql: str, dialect: str
) -> str | None:
    """CONCAT(col, littéraux) → décomposition : la valeur source est la clé moins les littéraux."""
    if isinstance(expr, exp.Concat):
        operands = list(expr.expressions)
    elif isinstance(expr, exp.DPipe):
        operands = [expr.this, expr.expression]
    else:
        return None
    cols = [o for o in operands if isinstance(o, exp.Column)]
    lits = [o for o in operands if isinstance(o, exp.Literal)]
    if len(cols) != 1 or len(cols) + len(lits) != len(operands):
        return None
    col_sql = cols[0].sql(dialect=dialect)
    parts = " || ".join(o.sql(dialect=dialect) for o in operands)
    return (
        f"la clé vaut {parts} : pose dans {col_sql} la valeur cible SANS les "
        f"littéraux concaténés, et dans l'autre côté ({other_sql}) la valeur "
        "concaténée complète."
    )


def _generic_fallback(expr: exp.Expression, other_sql: str, dialect: str) -> str:
    expr_sql = expr.sql(dialect=dialect)
    src_cols = ", ".join(
        sorted({c.sql(dialect=dialect) for c in expr.find_all(exp.Column)})
    )
    src_part = src_cols or "la valeur source"
    return (
        f"cette clé est dérivée par `{expr_sql}` : choisis {src_part} telle que "
        f"`{expr_sql}` = la valeur de l'autre côté ({other_sql})."
    )
