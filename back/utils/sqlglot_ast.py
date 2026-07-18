"""Accès AST sqlglot tolérants aux variations de clés entre versions.

Selon la version de sqlglot, certains arguments d'AST changent de nom : la clause
FROM est stockée sous ``from`` ou ``from_``, la clause WITH sous ``with`` ou
``with_`` (sqlglot ≥ 30 utilise les variantes suffixées). Disperser des
``args.get("from") or args.get("from_")`` dans tout le code est fragile, et une
**écriture** sur la mauvaise clé est pire : ``select.set("from", node)`` est
silencieusement ignoré quand la clé réelle est ``from_`` (le FROM disparaît du SQL
rendu, sans erreur).

Ce module centralise ces accès en fonctions **pures** et **testées contre la version
installée**, pour n'avoir qu'un seul endroit à corriger si une future version de
sqlglot rebaptise encore ces clés.
"""

from __future__ import annotations

from functools import lru_cache

from sqlglot import exp

_FROM_KEYS = ("from", "from_")
_WITH_KEYS = ("with", "with_")


@lru_cache(maxsize=1)
def _canonical_from_key() -> str:
    """Clé d'argument réellement utilisée pour la clause FROM dans la version
    installée (détectée via un SELECT construit par l'API sqlglot)."""
    sample = exp.select("*").from_("t")
    for key in _FROM_KEYS:
        if sample.args.get(key) is not None:
            return key
    return "from"


def get_from(expr: exp.Expression) -> exp.Expression | None:
    """Clause FROM d'un SELECT (``exp.From``) ou ``None``, quelle que soit la clé."""
    for key in _FROM_KEYS:
        value = expr.args.get(key)
        if value is not None:
            return value
    return None


def set_from(expr: exp.Expression, from_node: exp.Expression) -> exp.Expression:
    """Pose la clause FROM sur la clé canonique de la version installée.

    À utiliser à la place de ``expr.set("from", …)``, qui est ignoré quand la clé
    réelle est ``from_``. Retourne ``expr`` (chaînable)."""
    # On nettoie les deux variantes avant de poser la bonne, pour ne jamais laisser
    # une clé orpheline issue d'une autre version.
    for key in _FROM_KEYS:
        expr.args.pop(key, None)
    expr.set(_canonical_from_key(), from_node)
    return expr


def pop_with(expr: exp.Expression) -> exp.Expression:
    """Retire la clause WITH **en place** (les deux clés). Retourne ``expr``.

    Évite le piège de ``ctes.clear()`` (vide la liste mais laisse un nœud ``With``
    vide → rendu ``WITH `` orphelin non re-parsable) et celui de ``set("with", None)``
    (no-op quand la clé est ``with_``)."""
    for key in _WITH_KEYS:
        expr.args.pop(key, None)
    return expr


def strip_with(expr: exp.Expression) -> exp.Expression:
    """Renvoie une **copie** de ``expr`` sans sa clause WITH (les deux clés)."""
    return pop_with(expr.copy())


def quote_identifier(name: str, dialect: str) -> str:
    """Identifiant quoté selon le dialecte (backticks BigQuery, guillemets ailleurs).

    Un backtick codé en dur casse le re-parse ``read=dialect`` sur tout dialecte
    non-BigQuery (snowflake/postgres/duckdb → ParseError « Expecting ( »). Incidents :
    le ``run_cte`` de debug_executor (corrigé localement, _quote_ident), puis le
    CTE-trace de l'executor (root-cause spider2-snow : ``row_count -1`` sur toutes les
    CTEs, ``failing_cte=None`` → boucle de correction aveugle). Toute construction de
    SQL par f-string qui cite un identifiant doit passer par ici.
    """
    return exp.to_identifier(name, quoted=True).sql(dialect=dialect)
