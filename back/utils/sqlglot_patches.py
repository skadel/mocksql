"""Patches du générateur DuckDB de sqlglot — parsing de dates tolérant aux erreurs.

Pourquoi
--------
Les données de test sont synthétiques (générées par LLM) : elles ont
constamment des décalages de format par rapport à ce qu'attend un
``PARSE_DATETIME('%Y-%m-%d %H:%M:%S', col)``. BigQuery rend NULL dans ce cas.
DuckDB, lui, lève ``InvalidInputException`` — ce qui fait tomber le test entier
au lieu de laisser sortir un NULL que le verdict peut pointer.

On enveloppe donc tout parsing de date dans ``TRY(...)`` (DuckDB ≥ 1.2), qui
convertit n'importe quelle erreur *runtime* en NULL.

Pourquoi au niveau du générateur, et pas en regex sur le SQL rendu
------------------------------------------------------------------
Le SQL rendu par sqlglot change à chaque bump — 30.11 rend
``PARSE_DATETIME(col, fmt)`` (une fonction que DuckDB ne connaît même pas),
30.12 rend ``STRPTIME('1970 ' || col, fmt)``. Une regex calibrée sur l'un est
silencieusement court-circuitée par l'autre : le fix ne s'applique plus, et
personne ne le voit tant qu'une donnée mal formatée n'arrive pas en prod.

Le *type de nœud* AST (``ParseDatetime``, ``StrToTime``, …) est stable là où le
texte ne l'est pas. On s'y accroche, et on enveloppe le rendu **que sqlglot
aurait produit de toute façon** — on hérite donc automatiquement de ses
améliorations (le préfixe ``'1970 '`` de 30.12 corrige un vrai écart : DuckDB
fait défaut à 1900 sur un format sans année, BigQuery à 1970).

Deux cas dérogent à ce « on enveloppe l'existant » :

- ``ParseDatetime``, que sqlglot ≤ 30.11 ne traduit pas du tout — on fournit le
  rendu. Le cas est détecté **par sondage** (on rend un nœud témoin et on
  regarde le résultat), jamais par comparaison de numéro de version : au bump
  où sqlglot ajoutera la traduction, le sondage la verra et on se contentera de
  l'envelopper.
- ``SafeFunc`` (préfixe BigQuery ``SAFE.``), rendu tel quel — ``SAFE.STRPTIME``
  n'existe pas côté DuckDB. Or ``SAFE.f(x)`` *signifie* « f(x), NULL si erreur »
  = exactement ``TRY(f(x))`` : on remplace le préfixe au lieu de l'envelopper.

Note sur le cache de dispatch
-----------------------------
sqlglot 30 précalcule un dispatch ``{classe de nœud → rendu}`` par classe de
générateur (``generator._DISPATCH_CACHE``), construit à la première génération.
Muter ``TRANSFORMS`` après coup est alors un **no-op silencieux** — le piège
exact que ce module existe pour éviter. On invalide donc le cache, et surtout
on **vérifie le résultat par sondage** : si le patch ne prend pas, on lève au
démarrage plutôt que de rendre du SQL fragile en silence.
"""

from __future__ import annotations

from sqlglot import exp, generator, parse_one
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.generator import Generator

# Nœuds de parsing de date/heure : tout ce qui peut lever sur une valeur dont le
# format ne colle pas.
_TRY_WRAPPED_NODES = (exp.ParseDatetime, exp.StrToTime, exp.StrToDate, exp.SafeFunc)

_APPLIED_MARKER = "_mocksql_try_wrapped"

# Renseigné par `apply_duckdb_date_parse_patches` : sqlglot traduisait-il
# PARSE_DATETIME nativement au moment du patch ? Tant que c'est False, notre
# rendu de secours (`_parse_datetime_to_strptime`) est nécessaire ; le jour où
# sqlglot ajoute la traduction, ça passe True et le canary dédié le signale.
# C'est un *constat* de sondage, pas un réglage — ne pas l'écrire à la main.
parse_datetime_native_support: bool | None = None

# Sondes : (expression BigQuery, doit-on retrouver TRY( dans le rendu DuckDB).
_SELF_CHECK_PROBES = (
    "PARSE_DATETIME('%Y-%m-%d', col)",
    "PARSE_TIMESTAMP('%Y-%m-%d', col)",
    "PARSE_DATE('%Y-%m-%d', col)",
    "SAFE.PARSE_TIMESTAMP('%Y-%m-%d', col)",
)


def _render_duckdb(bq_expr: str) -> str:
    return parse_one(bq_expr, dialect="bigquery").sql(dialect="duckdb")


def _original_renderer(node_cls: type[exp.Expression]):
    """Rendu que sqlglot appliquerait sans nous, capturé une fois à l'import."""
    transform = DuckDB.Generator.TRANSFORMS.get(node_cls)
    if transform is not None:
        return transform

    method = getattr(DuckDB.Generator, f"{node_cls.key}_sql", None) or getattr(
        Generator, f"{node_cls.key}_sql", None
    )
    if method is not None:
        return method

    return lambda self, expression: self.function_fallback_sql(expression)


def _renders_natively(node_cls: type[exp.Expression], probe: str) -> bool:
    """sqlglot sait-il traduire ce nœud vers du DuckDB valide ?

    Sans traduction, le générateur retombe sur un rendu générique qui reprend le
    nom BigQuery (``PARSE_DATETIME(...)``) — une fonction absente du catalogue
    DuckDB, qu'un ``TRY()`` ne rattraperait pas (erreur de binder, pas runtime).
    """
    return node_cls.sql_names()[0].upper() not in _render_duckdb(probe).upper()


def _parse_datetime_to_strptime(self: Generator, expression: exp.Expression) -> str:
    """Rendu de secours pour sqlglot ≤ 30.11, qui ne traduit pas PARSE_DATETIME."""
    return f"STRPTIME({self.sql(expression, 'this')}, {self.sql(expression, 'format')})"


def _safe_prefix_to_try(self: Generator, expression: exp.Expression) -> str:
    """``SAFE.f(x)`` → ``f(x)``, l'enveloppe TRY() rétablissant la sémantique.

    Si l'appel enveloppé est lui-même un nœud qu'on patche (``SAFE.PARSE_DATE``
    → ``StrToDate`` déjà rendu ``TRY(...)``), on ne re-enveloppe pas : le SQL
    exécuté est identique, mais un `TRY(TRY(…))` pollue les logs et les diffs.
    """
    return self.sql(expression, "this")


def _invalidate_dispatch_cache() -> None:
    """Force la reconstruction du dispatch précalculé (sqlglot 30+).

    Best-effort : `_DISPATCH_CACHE` est privé et peut disparaître. C'est le
    sondage final d'`apply_duckdb_date_parse_patches` qui fait foi.
    """
    cache = getattr(generator, "_DISPATCH_CACHE", None)
    if isinstance(cache, dict):
        cache.clear()


def apply_duckdb_date_parse_patches() -> None:
    """Enveloppe les nœuds de parsing de date DuckDB dans ``TRY(...)``.

    Idempotent. Lève ``RuntimeError`` si le patch ne prend pas — un patch de
    générateur qui échoue en silence est pire que pas de patch du tout.
    """
    global parse_datetime_native_support

    for node_cls in _TRY_WRAPPED_NODES:
        existing = DuckDB.Generator.TRANSFORMS.get(node_cls)
        if getattr(existing, _APPLIED_MARKER, False):
            continue

        if node_cls is exp.SafeFunc:
            inner = _safe_prefix_to_try
        elif node_cls is exp.ParseDatetime:
            parse_datetime_native_support = _renders_natively(
                node_cls, "PARSE_DATETIME('%Y-%m-%d', col)"
            )
            inner = (
                _original_renderer(node_cls)
                if parse_datetime_native_support
                else _parse_datetime_to_strptime
            )
        else:
            inner = _original_renderer(node_cls)

        def wrapped(self, expression, _inner=inner):
            rendered = _inner(self, expression)
            if rendered.upper().startswith("TRY("):
                return rendered
            return f"TRY({rendered})"

        setattr(wrapped, _APPLIED_MARKER, True)
        DuckDB.Generator.TRANSFORMS[node_cls] = wrapped

    _invalidate_dispatch_cache()

    for probe in _SELF_CHECK_PROBES:
        rendered = _render_duckdb(probe)
        if "TRY(" not in rendered.upper():
            raise RuntimeError(
                "Patch sqlglot→DuckDB inopérant : "
                f"{probe} rend {rendered!r}, sans TRY(). "
                "sqlglot a probablement changé sa résolution de rendu "
                "(cf. generator._DISPATCH_CACHE). Voir utils/sqlglot_patches.py."
            )
