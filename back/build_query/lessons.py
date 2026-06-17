"""Mémoire des leçons de correction.

Persiste, **par table** et **par jointure**, les règles apprises quand le LLM
corrige une erreur de génération de données (ex : mauvaise jointure entre X et Y)
et que la correction **converge** — soit l'évaluateur donne un verdict suffisant,
soit l'utilisateur accepte le diff. Ces leçons sont réinjectées dans le prompt de
génération pour qu'une même erreur ne se répète pas d'un test à l'autre (cf. le
symptôme : le LLM corrige une jointure, puis refait la même erreur au test suivant).

Stockées dans le profil partagé (``schema_cache``, clé ``lessons``), à côté des
statistiques de colonnes et des cardinalités de jointures — donc visibles par tous
les modèles qui touchent les mêmes tables. Forme ::

    profile["lessons"] = {
        "tables": {"orders": ["règle 1", "règle 2"]},
        "joins":  {"orders↔users": ["règle 1"]},
    }

Les leçons sont des **heuristiques** (pas la vérité-terrain comme les cardinalités
réelles) : on les plafonne (``LESSONS_CAP``) pour ne pas gonfler le prompt ni
laisser une vieille leçon contredire la réalité d'un autre test.
"""

from __future__ import annotations

import json
import logging
from typing import List, Optional

import utils.logger  # noqa: F401 — enregistre le niveau DIAG (logger.diag)
from build_query.profile_checker import _load_model_profile, _save_model_profile
from storage.test_repository import get_test

logger = logging.getLogger(__name__)

# Plafond par clé (par table ET par jointure), demandé produit : 3.
LESSONS_CAP = 3


# ---------------------------------------------------------------------------
# Clés canoniques
# ---------------------------------------------------------------------------


def _short(table: str) -> str:
    """Nom court d'une table (dernier segment), insensible à la casse.

    ``project.dataset.orders`` → ``orders``. Aligne le rapprochement table↔leçon
    sur le format des autres blocs (``_format_profile_block`` indexe par nom court).
    """
    return (table or "").split(".")[-1].strip().lower()


def join_key(left: str, right: str) -> str:
    """Clé canonique d'une jointure : paire de noms courts triée, séparée par ``↔``.

    Triée pour que ``X JOIN Y`` et ``Y JOIN X`` partagent la même leçon.
    """
    a, b = _short(left), _short(right)
    lo, hi = sorted((a, b))
    return f"{lo}↔{hi}"


# ---------------------------------------------------------------------------
# Écriture (pure) — dédup + plafond
# ---------------------------------------------------------------------------


def add_lesson(
    profile: Optional[dict],
    scope: str,
    key: str,
    rule: str,
    cap: int = LESSONS_CAP,
) -> dict:
    """Ajoute une leçon au profil et retourne le profil (muté sur place).

    - ``scope`` : ``"table"`` ou ``"join"``.
    - ``key`` : nom court de table, ou clé de jointure (cf. :func:`join_key`).
    - Dédup insensible à la casse/espaces ; la plus récente passe en tête.
    - Plafonné à ``cap`` par clé : au-delà, la plus ancienne tombe.
    """
    profile = profile if isinstance(profile, dict) else {}
    rule = (rule or "").strip()
    if scope not in ("table", "join") or not key or not rule:
        return profile

    bucket_name = "tables" if scope == "table" else "joins"
    lessons = profile.setdefault("lessons", {})
    bucket = lessons.setdefault(bucket_name, {})
    existing: List[str] = list(bucket.get(key) or [])

    # Dédup insensible à la casse, la nouvelle en tête.
    rule_norm = rule.casefold()
    deduped = [r for r in existing if r.strip().casefold() != rule_norm]
    bucket[key] = ([rule] + deduped)[:cap]
    return profile


# ---------------------------------------------------------------------------
# Lecture (pure) — bloc injecté dans le prompt de génération
# ---------------------------------------------------------------------------


def _used_short_tables(used_columns: list) -> set:
    """Noms courts des tables référencées par la requête (depuis ``used_columns``)."""
    out: set = set()
    for entry in used_columns or []:
        if isinstance(entry, str):
            try:
                entry = json.loads(entry)
            except Exception:
                continue
        tbl = entry.get("table", "")
        if tbl:
            out.add(_short(tbl))
    return out


def format_lessons_block(profile: Optional[dict], used_columns: list) -> str:
    """Bloc « leçons apprises » pour les tables/jointures de la requête courante.

    On ne montre que les leçons pertinentes : table effectivement utilisée, ou
    jointure dont **les deux** tables sont dans la requête. Retourne ``""`` si rien.
    """
    if not isinstance(profile, dict):
        return ""
    lessons = profile.get("lessons") or {}
    if not lessons:
        return ""

    used = _used_short_tables(used_columns)

    table_lines: List[str] = []
    for tbl, rules in (lessons.get("tables") or {}).items():
        if used and tbl not in used:
            continue
        for r in rules or []:
            table_lines.append(f"  table `{tbl}` : {r}")

    join_lines: List[str] = []
    for jkey, rules in (lessons.get("joins") or {}).items():
        sides = jkey.split("↔")
        # On exige les deux côtés présents pour éviter une leçon hors-contexte.
        if used and not (len(sides) == 2 and all(s in used for s in sides)):
            continue
        pretty = jkey.replace("↔", " ↔ ")
        for r in rules or []:
            join_lines.append(f"  jointure `{pretty}` : {r}")

    if not table_lines and not join_lines:
        return ""

    out = (
        "Leçons apprises de corrections passées — **évite de répéter ces erreurs** :\n"
    )
    out += "\n".join(table_lines + join_lines)
    return out


# ---------------------------------------------------------------------------
# Capture : entrée canonique + flush vers le profil (gated convergence, sans LLM)
# ---------------------------------------------------------------------------
#
# Les leçons sont FORMULÉES par l'agent conversationnel pendant la correction (outil
# `note_lesson`) — là où il réfléchit déjà à la cause racine. Pas d'appel LLM dédié
# ni de re-passage de contexte. Elles s'accumulent dans ``state["pending_lessons"]``
# au fil du run et ne sont écrites dans le profil partagé qu'à **convergence** (une
# leçon issue d'un run qui n'aboutit jamais pourrait être fausse → on ne propage pas).

_SUFFICIENT_VERDICTS = {"bon", "excellent"}


def make_lesson_entry(
    scope: str,
    rule: str,
    table: str = "",
    left_table: str = "",
    right_table: str = "",
    source: str = "correction",
) -> Optional[dict]:
    """Construit une entrée de leçon canonique ``{scope, key, rule, source}`` ou ``None``.

    Utilisé par l'outil ``note_lesson`` de l'agent : il reçoit les noms de tables
    bruts, on en dérive la clé canonique (nom court / paire triée).

    ``source`` :
    - ``"correction"`` : déduite d'une correction de contrainte (boucle bad_data) —
      persistée seulement à convergence.
    - ``"user"`` : règle métier énoncée par l'utilisateur en chat — persistée sur son
      autorité, sans gate de convergence.
    """
    rule = (rule or "").strip()
    if not rule:
        return None
    if scope == "join" and left_table and right_table:
        return {
            "scope": "join",
            "key": join_key(left_table, right_table),
            "rule": rule,
            "source": source,
        }
    if scope == "table" and table:
        return {"scope": "table", "key": _short(table), "rule": rule, "source": source}
    return None


def _converged_target(state: dict) -> Optional[dict]:
    """Retourne le cas de test corrigé s'il a CONVERGÉ (verdict suffisant), sinon None.

    Cible par ``test_uid`` (identité stable) puis ``test_index``. Sans cible
    identifiable mais avec un verdict suffisant global, on ne capture pas (on ne
    saurait pas à quelle table/jointure rattacher la leçon).
    """
    test = get_test(state.get("session"))
    if not test:
        return None
    cases = test.get("test_cases") or []
    uid = state.get("test_uid")
    idx = state.get("test_index")

    target = None
    if uid:
        target = next((c for c in cases if c.get("test_uid") == uid), None)
    if target is None and idx is not None:
        target = next((c for c in cases if str(c.get("test_index")) == str(idx)), None)
    if target is None:
        return None
    if str(target.get("verdict", "")).strip().lower() in _SUFFICIENT_VERDICTS:
        return target
    return None


def persist_pending_lessons(state: dict) -> Optional[dict]:
    """Persiste ``state["pending_lessons"]`` dans le profil partagé, selon la source :

    - ``source == "user"`` : règle énoncée par l'utilisateur → persistée toujours
      (il fait autorité, pas de gate de convergence).
    - ``source == "correction"`` : déduite d'une correction de contrainte → persistée
      seulement si le test corrigé a convergé (verdict suffisant). Une leçon d'un run
      qui n'aboutit jamais pourrait être fausse et polluer tous les modèles.

    Pas d'appel LLM : les leçons sont déjà formulées par l'agent. Dédup + plafond
    :func:`LESSONS_CAP` par clé appliqués à l'écriture. Retourne le profil ou ``None``.
    """
    pending = state.get("pending_lessons") or []
    if not pending:
        return None

    converged = _converged_target(state) is not None
    to_write = [
        lesson for lesson in pending if lesson.get("source") == "user" or converged
    ]
    if not to_write:
        logger.diag(
            "[lessons] non convergé — %d leçon(s) de correction non persistée(s)",
            len(pending),
        )
        return None

    profile = _load_model_profile() or {}
    for lesson in to_write:
        add_lesson(profile, lesson.get("scope"), lesson.get("key"), lesson.get("rule"))
    _save_model_profile(profile)
    logger.diag("[lessons] %d leçon(s) persistée(s)", len(to_write))
    return profile
