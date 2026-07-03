"""Runtime des commandes CLI qui comblent les trous UI ↔ CLI :

- ``mocksql remove-test``  → équivalent de la suppression via le chat (delete_test_node) :
  retire un cas de la suite, déterministe, sans LLM.
- ``mocksql validate``     → équivalent du bouton « Je valide l'état actuel »
  (accept_validation) : applique la description réalignée proposée par l'évaluateur et
  flippe le verdict à « Bon ».
- ``mocksql suggest ...``  → gestion du panneau de suggestions : list / regenerate /
  use / dismiss.

Tout est basé fichiers (``.mocksql/tests/{model}.json``) — le même stockage que le
serveur (storage.test_repository), donc un modèle géré en CLI reste cohérent dans l'UI.
Le ciblage se fait par ``test_uid`` (jamais par position) et par texte/numéro 1-based
pour les suggestions.
"""

from __future__ import annotations

import json
import os
import uuid
from pathlib import Path
from typing import Any

from cli.doc_io import TestDocError, load_doc, require_test_case, save_doc

# Cas dont le narratif est en attente d'arbitrage utilisateur (cf. test_evaluator →
# VALIDATION_PROMPT) : écart de cardinalité, de valeur de sortie, ou de données d'entrée.
_VALIDATABLE_REASONS = ("needs_validation", "bad_description", "bad_input_description")


def _point_storage_at(config_path: Path) -> None:
    """Aligne storage.config (lu par test_repository, make_llm, etc.) sur le projet
    ciblé par --config — sinon il retombe sur le cwd sans mocksql.yml (cf. run_generate).
    """
    import storage.config as storage_config

    os.environ["MOCKSQL_BASE_DIR"] = str(config_path.resolve().parent)
    storage_config.load_config.cache_clear()


# ── remove-test ────────────────────────────────────────────────────────────────


def run_remove_test(config_path: Path, model: str, test_uid: str) -> dict[str, Any]:
    """Retire le cas ciblé de la suite (assertions-specs comprises). Déterministe."""
    path, doc = load_doc(config_path, model)
    removed = require_test_case(doc, test_uid)
    doc["test_cases"] = [
        c for c in doc.get("test_cases") or [] if c.get("test_uid") != test_uid
    ]
    save_doc(path, doc)
    return {
        "model": model,
        "removed": {
            "test_uid": test_uid,
            "test_name": removed.get("test_name"),
        },
        "remaining": len(doc["test_cases"]),
    }


# ── validate ───────────────────────────────────────────────────────────────────


async def run_validate(config_path: Path, model: str, test_uid: str) -> dict[str, Any]:
    """Accepte la sortie réelle d'un test en attente de validation.

    Chemin nominal sans LLM : l'évaluateur a déjà proposé une description réalignée
    (``corrected_description``) — on l'applique telle quelle. Fallback réalignement LLM
    uniquement pour les tests anciens sauvés avant ce champ (même logique que le nœud
    ``accept_validation``).
    """
    path, doc = load_doc(config_path, model)
    tc = require_test_case(doc, test_uid)

    corrected_desc = (tc.get("corrected_description") or "").strip()
    if tc.get("reason_type") not in _VALIDATABLE_REASONS and not corrected_desc:
        raise TestDocError(
            f"Rien à valider sur le test '{test_uid}' : aucune validation en attente "
            "(le verdict ne signale pas de désynchronisation description ↔ réel)."
        )

    if corrected_desc:
        new_desc = corrected_desc
        new_name = (tc.get("corrected_name") or "").strip()
    else:
        _point_storage_at(config_path)
        from models.env_variables import validate_required_env

        validate_required_env()

        from build_query.accept_validation import _realign_description

        try:
            actual_rows = len(json.loads(tc.get("results_json") or "[]"))
        except Exception:
            actual_rows = 0
        realigned = await _realign_description(tc, actual_rows)
        new_desc = realigned.unit_test_description
        new_name = realigned.test_name

    from build_query.accept_validation import apply_validation_to_case

    doc["test_cases"] = [
        apply_validation_to_case(c, new_desc, new_name)
        if c.get("test_uid") == test_uid
        else c
        for c in doc.get("test_cases") or []
    ]
    save_doc(path, doc)

    updated = require_test_case(doc, test_uid)
    return {
        "model": model,
        "test_uid": test_uid,
        "verdict": updated.get("verdict"),
        "test_name": updated.get("test_name"),
        "unit_test_description": updated.get("unit_test_description"),
    }


# ── suggest ────────────────────────────────────────────────────────────────────


def run_suggest_list(config_path: Path, model: str) -> dict[str, Any]:
    """Suggestions en attente dans le panneau, numérotées (1-based) pour use/dismiss."""
    _, doc = load_doc(config_path, model)
    rationales = doc.get("suggestion_rationales") or {}
    paths = doc.get("suggestion_paths") or {}
    return {
        "model": model,
        "suggestions": [
            {
                "number": i,
                "text": s,
                "rationale": rationales.get(s) or None,
                "target_path": paths.get(s) or None,
            }
            for i, s in enumerate(doc.get("suggestions") or [], 1)
        ],
    }


def _resolve_suggestion(
    doc: dict[str, Any], number: int | None, text: str | None
) -> str:
    """Résout la suggestion ciblée par numéro (1-based, cf. ``suggest list``) OU texte exact."""
    suggestions = list(doc.get("suggestions") or [])
    if not suggestions:
        raise TestDocError(
            "Aucune suggestion en attente pour ce modèle. "
            "Lance `mocksql suggest regenerate` pour en produire."
        )
    if (number is None) == (text is None):
        raise TestDocError(
            "Cible la suggestion par --number OU --text (exactement un des deux)."
        )
    if number is not None:
        if not 1 <= number <= len(suggestions):
            raise TestDocError(
                f"--number {number} hors limites (1..{len(suggestions)})."
            )
        return suggestions[number - 1]
    wanted = (text or "").strip()
    for s in suggestions:
        if s.strip() == wanted:
            return s
    raise TestDocError(
        f"Suggestion introuvable : '{wanted}'. "
        "Lance `mocksql suggest list <model>` pour voir les textes exacts."
    )


def consume_suggestion_updates(
    stored: dict[str, Any], text: str, *, fate: str
) -> dict[str, Any]:
    """Champs à écrire pour sortir ``text`` du panneau. ``fate`` ∈ {accepted, dismissed} :
    la suggestion part dans ``accepted_suggestions`` (transformée en test — le suggesteur
    ne la reproposera pas) ou ``dismissed_suggestions`` (rejetée — ni elle ni une variante
    proche). Miroir exact de la consommation serveur (utils/saver.py, history_saver) et de
    ``POST /suggestions/dismiss``.
    """
    consumed = text.strip()
    remaining = [s for s in (stored.get("suggestions") or []) if s.strip() != consumed]
    key = f"{fate}_suggestions"
    resolved = list(stored.get(key) or [])
    if consumed not in (s.strip() for s in resolved):
        resolved.append(consumed)
    rationales = {
        k: v
        for k, v in (stored.get("suggestion_rationales") or {}).items()
        if k.strip() != consumed
    }
    paths = {
        k: v
        for k, v in (stored.get("suggestion_paths") or {}).items()
        if k.strip() != consumed
    }
    return {
        "suggestions": remaining,
        key: resolved,
        "suggestion_rationales": rationales,
        "suggestion_paths": paths,
    }


def run_suggest_dismiss(
    config_path: Path, model: str, number: int | None, text: str | None
) -> dict[str, Any]:
    """Rejette une suggestion (elle ne sera jamais reproposée). Déterministe, sans LLM."""
    path, doc = load_doc(config_path, model)
    chosen = _resolve_suggestion(doc, number, text)
    doc.update(consume_suggestion_updates(doc, chosen, fate="dismissed"))
    save_doc(path, doc)
    return {"model": model, "dismissed": chosen, "pending": doc["suggestions"]}


async def run_suggest_regenerate(config_path: Path, model: str) -> dict[str, Any]:
    """Équivalent du bouton « Régénérer » du panneau : appelle directement le nœud
    ``generate_suggestions`` avec le flag ``regenerate_suggestions`` (mode replace).

    Le nœud persiste lui-même les suggestions sur le fichier via storage.test_repository
    (même stockage que la CLI) et tient compte des suggestions acceptées/rejetées.
    Pas de profil chargé en CLI → pas de suggestions [PROD].
    """
    _point_storage_at(config_path)
    from models.env_variables import validate_required_env

    validate_required_env()

    from storage import test_repository as repo

    tests = repo.list_tests(model)
    if not tests:
        raise TestDocError(
            f"Aucun test pour le modèle '{model}'. Lance `mocksql generate {model}.sql`."
        )
    doc = tests[0]
    if not doc.get("test_cases"):
        raise TestDocError(
            "Aucun cas de test : les suggestions se contextualisent sur les tests "
            "existants. Lance `mocksql generate` d'abord."
        )

    from cli.generate import load_config

    cfg = load_config(config_path)
    state = {
        "session": doc["test_id"],
        "query": doc.get("sql") or "",
        "optimized_sql": doc.get("optimized_sql") or doc.get("sql") or "",
        "dialect": cfg.get("dialect", "bigquery"),
        "used_columns": doc.get("used_columns") or [],
        "query_decomposed": doc.get("query_decomposed") or "",
        "path_plans": doc.get("path_plans"),
        "messages": [],
        "regenerate_suggestions": True,
        "request_id": str(uuid.uuid4()),
    }
    from build_query.suggestions_node import generate_suggestions

    await generate_suggestions(state)

    _, refreshed = load_doc(config_path, model)
    return {"model": model, "suggestions": refreshed.get("suggestions") or []}


async def run_suggest_use(
    config_path: Path,
    model: str,
    number: int | None,
    text: str | None,
    output_dir: Path,
) -> dict[str, Any]:
    """Équivalent du clic sur une suggestion : la transforme en test (mode additif,
    ``suggestion_intent`` posé par run_generate) puis la consomme (``accepted_suggestions``)
    — miroir du couple conversational_agent + history_saver côté serveur. Le focus par
    branche (``suggestion_paths``) est propagé au state pour que l'agent appelle
    ``set_target_path`` sans deviner de nom de branche.
    """
    _, doc = load_doc(config_path, model)
    chosen = _resolve_suggestion(doc, number, text)
    target_path = (doc.get("suggestion_paths") or {}).get(chosen)

    from cli.generate import load_config

    cfg = load_config(config_path)
    models_base = (config_path.parent / cfg.get("models_path", "./models")).resolve()
    model_sql = models_base / f"{model}.sql"
    if not model_sql.exists():
        raise TestDocError(f"Source SQL introuvable : {model_sql}")

    from cli.generate import run_generate

    await run_generate(
        model_sql,
        config_path,
        output_dir,
        instruction=chosen,
        target_path=target_path,
    )

    # Consommation post-génération (le history_saver serveur est neutralisé en CLI) :
    # la suggestion sort du panneau et entre dans accepted_suggestions pour que le
    # suggesteur ne la repropose pas. Une génération en échec lève avant d'arriver ici.
    path, refreshed = load_doc(config_path, model)
    refreshed.update(consume_suggestion_updates(refreshed, chosen, fate="accepted"))
    save_doc(path, refreshed)
    return {"model": model, "used": chosen, "pending": refreshed["suggestions"]}
