"""Patch du générateur DuckDB de sqlglot pour les fonctions ``SAFE.PARSE_*``.

Pourquoi
--------
BigQuery distingue les fonctions strictes ``PARSE_*`` (erreur sur une valeur
malformée) de leurs variantes ``SAFE.PARSE_*`` (NULL sur erreur). DuckDB doit
conserver cette distinction pour qu'un test local ne masque pas une erreur de
production. Seul le nœud ``SafeFunc`` est donc enveloppé dans ``TRY(...)``.

Pourquoi au niveau du générateur, et pas en regex sur le SQL rendu
------------------------------------------------------------------
Le SQL rendu par sqlglot change à chaque bump — 30.11 rend
``PARSE_DATETIME(col, fmt)`` (une fonction que DuckDB ne connaît même pas),
30.12 rend ``STRPTIME('1970 ' || col, fmt)``. Une regex calibrée sur l'un est
silencieusement court-circuitée par l'autre : le fix ne s'applique plus, et
personne ne le voit tant qu'une donnée mal formatée n'arrive pas en prod.

Le type de nœud AST ``SafeFunc`` est stable là où le texte rendu ne l'est pas.
Son préfixe ``SAFE.`` n'existe pas côté DuckDB ; ``SAFE.f(x)`` signifie
exactement ``TRY(f(x))``. Les nœuds stricts restent rendus tels quels par
sqlglot.

``ParseDatetime`` mérite une note : sqlglot ne le traduisait pas du tout jusqu'à
30.11 (un rendu de secours ``STRPTIME`` était nécessaire) ; depuis 30.12 il le
traduit nativement (``STRPTIME('1970 ' || col, …)``, le préfixe corrigeant
l'écart d'année par défaut DuckDB↔BigQuery), et on se contente de l'envelopper
comme les autres. Un sondage à l'import (``parse_datetime_native_support``) le
vérifie : s'il repasse faux, sqlglot a régressé et ``_original_renderer``
retomberait sur un nom de fonction que DuckDB ignore — un canary dédié l'annonce.

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

import duckdb

from sqlglot import exp, generator, parse_one
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.generator import Generator

# Seules les fonctions explicitement SAFE sont tolérantes aux erreurs.
_TRY_WRAPPED_NODES = (exp.SafeFunc,)

_APPLIED_MARKER = "_mocksql_try_wrapped"

# Renseigné par `apply_duckdb_date_parse_patches` : sqlglot traduit-il
# PARSE_DATETIME nativement vers DuckDB ? Depuis 30.12 c'est True et on se
# contente d'envelopper son rendu. Le flag reste comme **garde de régression** :
# s'il repasse False, sqlglot a cessé de traduire et `_original_renderer`
# retomberait sur `PARSE_DATETIME(...)`, une fonction que DuckDB ignore (erreur
# de binder, non rattrapée par TRY()) — le canary dédié le signale.
# C'est un *constat* de sondage, pas un réglage — ne pas l'écrire à la main.
parse_datetime_native_support: bool | None = None

# Sondes autonomes exécutées au chargement. Les strictes utilisent une valeur
# valide ; les SAFE une valeur invalide et doivent retourner NULL.
_SELF_CHECK_PROBES = (
    ("PARSE_DATE('%Y-%m-%d', '2024-01-15')", False),
    ("PARSE_DATETIME('%Y-%m-%d', '2024-01-15')", False),
    ("PARSE_TIMESTAMP('%Y-%m-%d', '2024-01-15')", False),
    ("SAFE.PARSE_DATE('%Y-%m-%d', 'invalid')", True),
    ("SAFE.PARSE_DATETIME('%Y-%m-%d', 'invalid')", True),
    ("SAFE.PARSE_TIMESTAMP('%Y-%m-%d', 'invalid')", True),
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


def _safe_prefix_to_try(self: Generator, expression: exp.Expression) -> str:
    """``SAFE.f(x)`` → ``f(x)``, l'enveloppe TRY() rétablissant la sémantique.

    Si l'appel enveloppé est lui-même un nœud qu'on patche (``SAFE.PARSE_DATE``
    → ``StrToDate`` déjà rendu ``TRY(...)``), on ne re-enveloppe pas : le SQL
    exécuté est identique, mais un `TRY(TRY(…))` pollue les logs et les diffs.
    """
    return self.sql(expression, "this")


def _assert_duckdb_probe_executable(probe: str, rendered: str) -> tuple:
    """Refuse un rendu que DuckDB ne peut pas réellement préparer/exécuter."""
    try:
        result = duckdb.connect(":memory:").execute(f"SELECT {rendered}").fetchone()
    except duckdb.Error as exc:
        raise RuntimeError(
            "Patch sqlglot→DuckDB inexécutable : "
            f"{probe} rend {rendered!r}, refusé par DuckDB : {exc}"
        ) from exc
    return result


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

        inner = _safe_prefix_to_try

        def wrapped(self, expression, _inner=inner):
            rendered = _inner(self, expression)
            if rendered.upper().startswith("TRY("):
                return rendered
            return f"TRY({rendered})"

        setattr(wrapped, _APPLIED_MARKER, True)
        DuckDB.Generator.TRANSFORMS[node_cls] = wrapped

    _invalidate_dispatch_cache()

    parse_datetime_native_support = _renders_natively(
        exp.ParseDatetime, "PARSE_DATETIME('%Y-%m-%d', col)"
    )

    for probe, should_be_safe in _SELF_CHECK_PROBES:
        rendered = _render_duckdb(probe)
        has_try = "TRY(" in rendered.upper() or "TRY_STRPTIME" in rendered.upper()
        if has_try != should_be_safe:
            raise RuntimeError(
                "Patch sqlglot→DuckDB inopérant : "
                f"{probe} rend {rendered!r}, sémantique TRY incorrecte. "
                "sqlglot a probablement changé sa résolution de rendu "
                "(cf. generator._DISPATCH_CACHE). Voir utils/sqlglot_patches.py."
            )
        result = _assert_duckdb_probe_executable(probe, rendered)
        if should_be_safe and result[0] is not None:
            raise RuntimeError(
                "Patch sqlglot→DuckDB inopérant : "
                f"{probe} rend {rendered!r}, mais la valeur invalide ne produit pas NULL."
            )
