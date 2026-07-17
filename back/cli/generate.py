"""mocksql generate — parse SQL, fetch schemas, generate test data."""

import json
import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import sqlglot
import yaml

from build_query.schema_fetcher import fetch_tables_schema, validate_bq_ref
from cli.schema_cache import (
    load_schema_cache,
    match_refs_against_cache,
    merge_into_cache,
    save_schema_cache,
)
from storage.config import load_preprocessor_fn
from storage.test_files import is_deadborn_case, read_test_doc, write_test_doc
from utils.sqlglot_ast import get_from
from utils.schema_utils import generate_tables_and_columns_from_project_schema
from utils.sql_code import (
    extract_real_table_refs,
    extract_select_statement,
    extract_used_columns_from_sql,
)

logger = logging.getLogger(__name__)


# ── Config ────────────────────────────────────────────────────────────────────


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config not found: {config_path}. Run `mocksql init` first."
        )
    with open(config_path) as f:
        return yaml.safe_load(f)


# ── SQL reading ───────────────────────────────────────────────────────────────


def read_sql(
    model_path: Path,
    preprocessor_fn: str | None,
    config_dir: Path,
    dialect: str = "bigquery",
) -> str:
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    raw_sql = model_path.read_text(encoding="utf-8")
    sql = (
        load_preprocessor_fn(preprocessor_fn, config_dir)(raw_sql)
        if preprocessor_fn
        else raw_sql
    )
    clean = extract_select_statement(sql, dialect)
    return clean if clean is not None else sql


# ── State builder ─────────────────────────────────────────────────────────────


def build_used_columns(
    schemas: list[dict], sql: str = "", dialect: str = "bigquery"
) -> list[str]:
    if sql:
        try:
            return extract_used_columns_from_sql(sql, dialect, schemas)
        except Exception as e:
            # Repli sûr (sur-inclusion) mais jamais silencieux : toutes les
            # colonnes du schéma gonflent le prompt du generator (29 au lieu
            # de 9 sur sf_bq263) et diluent les contraintes.
            logger.warning(
                "Extraction des used_columns échouée (%s: %s) — repli sur "
                "TOUTES les colonnes du schéma. SQL fautif :\n%s",
                type(e).__name__,
                e,
                sql,
            )

    result = []
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        project = parts[0] if len(parts) == 3 else ""
        database = parts[1] if len(parts) >= 2 else ""
        table = parts[-1]
        cols = [c["name"] for c in tbl.get("columns", [])]
        result.append(
            json.dumps(
                {
                    "project": project,
                    "database": database,
                    "table": table,
                    "used_columns": cols,
                }
            )
        )
    return result


def find_used_columns_missing_from_schema(
    used_columns: list, schemas: list[dict]
) -> list[tuple[str, list[str]]]:
    """Colonnes référencées dans le SQL (used_columns) mais ABSENTES du schéma
    en cache de leur table.

    Sans cette détection, ces colonnes sont droppées silencieusement (modèle de
    génération + création de table DuckDB) → "column not found" à l'exécution.
    La cause est un schéma en cache périmé : on échoue tôt avec un message clair
    (cf. refresh-schemas) plutôt que de fabriquer un test vert sur des données
    impossibles. Retourne [(table_name complet, [colonnes manquantes]), ...].
    """
    used_index: dict[str, list[str]] = {}
    for uc in used_columns:
        entry = json.loads(uc) if isinstance(uc, str) else uc
        db = entry.get("database") or ""
        table = entry.get("table") or ""
        key = (f"{db}.{table}" if db else table).lower()
        used_index[key] = entry.get("used_columns", [])

    problems: list[tuple[str, list[str]]] = []
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        key = ".".join(parts[-2:]).lower() if len(parts) >= 2 else parts[-1].lower()
        if key not in used_index:
            continue
        schema_cols = {c["name"].lower() for c in tbl.get("columns", [])}
        missing = sorted(
            c for c in used_index[key] if "." not in c and c.lower() not in schema_cols
        )
        if missing:
            problems.append((tbl["table_name"], missing))
    return problems


def diagnose_stale_schema_from_qualify_error(
    sql: str, schemas: list[dict], dialect: str, error_msg: str
) -> list[str]:
    """Diagnostique un cache périmé derrière une qualify-error « Unknown column: X ».

    Quand la qualification échoue sur une colonne ``X`` introuvable, on cherche une
    table **étoilée** (``SELECT * FROM <table>``) dont le schéma en cache ne contient
    PAS ``X`` : c'est la signature d'un schéma tronqué/périmé (le ``SELECT *`` ne
    s'étend qu'aux quelques colonnes connues, donc une référence ``alias.X`` en aval
    ne se résout pas). Renvoie le(s) ``table_name`` complet(s) fautif(s), sinon ``[]``.

    Sert à transformer le fallback silencieux « using raw SQL » (qui produit un test
    cassé — la table DuckDB n'a pas la colonne) en échec fail-fast actionnable
    (``refresh-schemas``). Faible risque de faux positif : on ne signale que si la
    colonne est à la fois irrésoluble (qualify a planté dessus) ET absente d'une table
    étoilée en scope.
    """
    import re

    m = re.search(
        r"unknown column:\s*([A-Za-z_][A-Za-z0-9_]*)", error_msg or "", re.IGNORECASE
    )
    if not m:
        return []
    col = m.group(1).lower()

    try:
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return []

    # Qualifieurs (alias/table) sous lesquels ``col`` est référencée — sert à épingler
    # la VRAIE table étoilée fautive (``ref_comm.col``) et à ne pas signaler les autres
    # tables étoilées qui n'ont simplement jamais eu cette colonne.
    col_qualifiers = {
        c.table.lower()
        for c in parsed.find_all(sqlglot.exp.Column)
        if c.name.lower() == col and c.table
    }

    # Tables étoilées : (alias_or_name, clé base db.table). La clé du nœud FROM varie
    # selon la version de sqlglot ("from"/"from_") et la table n'est pas forcément en
    # .this (alias) → find_all (un SELECT * sur un JOIN étend toutes les tables).
    starred: list[tuple[str, str]] = []
    for select in parsed.find_all(sqlglot.exp.Select):
        has_star = any(
            isinstance(p, sqlglot.exp.Star)
            or (
                isinstance(p, sqlglot.exp.Column)
                and isinstance(p.this, sqlglot.exp.Star)
            )
            for p in select.expressions
        )
        if not has_star:
            continue
        from_ = get_from(select)
        if from_ is None:
            continue
        for t in from_.find_all(sqlglot.exp.Table):
            key = ".".join(p for p in [t.db, t.name] if p).lower()
            if key:
                starred.append(((t.alias or t.name).lower(), key))
    if not starred:
        return []

    schema_cols = {}
    schema_full = {}
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        key = ".".join(parts[-2:]).lower() if len(parts) >= 2 else parts[-1].lower()
        schema_cols[key] = {c["name"].lower() for c in tbl.get("columns", [])}
        schema_full[key] = tbl["table_name"]

    missing = [
        (alias, key)
        for alias, key in starred
        if key in schema_cols and col not in schema_cols[key]
    ]
    # Si ``col`` est qualifiée quelque part, on épingle la table dont l'alias matche
    # (précision) ; sinon (référence nue) on garde tous les candidats étoilés.
    if col_qualifiers:
        pinned = [(a, k) for a, k in missing if a in col_qualifiers]
        if pinned:
            missing = pinned

    culprits: list[str] = []
    for _alias, key in missing:
        full = schema_full[key]
        if full not in culprits:
            culprits.append(full)
    return culprits


def build_initial_state(
    sql: str,
    dialect: str,
    schemas: list[dict],
    project_id: str,
    session_id: str,
) -> dict[str, Any]:
    msg_id = str(uuid.uuid4())
    return {
        "query": sql,
        "validated_sql": sql,
        "optimized_sql": sql,
        "dialect": dialect,
        "session": session_id,
        "project": project_id,
        "schemas": schemas,
        "used_columns": build_used_columns(schemas, sql, dialect),
        "used_columns_changed": True,
        "gen_retries": 10,
        "debug_retries": 3,
        "status": None,
        "input": "",
        "user_tables": "",
        "user_message_id": msg_id,
        "parent_message_id": msg_id,
        "request_id": str(uuid.uuid4()),
        "messages": [],
        "examples": [],
        "history": [],
        "query_decomposed": "[]",
        "title": "",
        "route": "generator",
        "error": "",
        "reasoning": "",
        "current_query": "",
        "test_index": None,
        "profile_complete": None,
        "profile": None,
        "profile_billing_tb": None,
        "rerun_all_tests": False,
        "user_rerun": False,
        "optimize": False,
        "save": None,
        "changed_message_id": "",
        # final_response skippe son appel LLM (message jamais affiché en CLI).
        "cli_mode": True,
    }


def _inject_schemas_into_cache(project_id: str, schemas: list[dict]) -> None:
    """Pre-populate the in-memory schema cache so generator/executor skip the DB."""
    import models.schemas as _s

    _s._cache = schemas
    _s._cache_by_name = {t["table_name"]: t for t in schemas}
    _s._cache_time = datetime.now() + timedelta(hours=1)


def _patch_db_calls() -> None:
    """Neutralise history_saver for CLI — no session exists in DB to save to."""
    import build_query.query_chain as _qc

    async def _noop(_state):
        return {}

    _qc.history_saver = _noop


# ── Output extraction ─────────────────────────────────────────────────────────


def _extract_test_cases(final_state: dict) -> list | None:
    """Return the full list of test-case result dicts from the executor message.

    The executor emits an AIMessage whose content is a JSON-serialised list where
    each element is a test-case dict containing at minimum:
      - "data":             {table_name: [rows]}
      - "assertion_results": [{sql, description, ...}]
    """
    for msg in reversed(final_state.get("messages", [])):
        try:
            content = json.loads(msg.content)
        except Exception:
            continue

        if not isinstance(content, list) or not content:
            continue

        if isinstance(content[0], dict) and content[0].get("data") is not None:
            return content

    return None


def _extract_suggestions(final_state: dict) -> list[str]:
    """Extract suggestions from the SUGGESTIONS message in final state."""
    from utils.msg_types import MsgType
    from utils.saver import get_message_type

    for msg in reversed(final_state.get("messages", [])):
        if get_message_type(msg) == MsgType.SUGGESTIONS:
            try:
                suggestions = json.loads(msg.content)
                if isinstance(suggestions, list):
                    return [s for s in suggestions if isinstance(s, str)]
            except Exception:
                pass
    return []


def _last_evaluation_by_test_index(messages: list) -> dict:
    """Dernier message EVALUATION par test_index, préfixe « **verdict** — » retiré.

    Sur le chemin `empty_results`, le verdict « Insuffisant » de l'évaluateur n'existe
    QUE dans ces messages (jamais fusionné dans le RESULTS que la CLI persiste) — c'est
    ici qu'on le récupère pour le fichier.
    """
    from utils.msg_types import MsgType
    from utils.saver import get_message_type

    out: dict = {}
    for msg in messages:
        if get_message_type(msg) != MsgType.EVALUATION:
            continue
        idx = msg.additional_kwargs.get("test_index")
        if idx is None:
            continue
        text = re.sub(r"^\*\*[^*]+\*\*\s*—\s*", "", str(msg.content)).strip()
        if text:
            out[str(idx)] = text
    return out


def _fallback_explanation(tc: dict) -> str:
    """Explication dérivée du cas lui-même quand aucun message EVALUATION n'existe
    (statut `error` : route directe vers history_saver, sans évaluateur)."""
    err_lines = str(tc.get("exec_error") or tc.get("error") or "").strip().splitlines()
    first = err_lines[0] if err_lines else ""
    status = tc.get("status")
    if status == "empty_results":
        cte = tc.get("failing_cte")
        return (
            f"La requête ne retourne aucune ligne — CTE bloquante : {cte}."
            if cte
            else "La requête ne retourne aucune ligne avec les données générées."
        )
    if status == "bad_data_error":
        return (
            f"Les données générées ont été rejetées par DuckDB : {first}"
            if first
            else "Les données générées ont été rejetées par DuckDB."
        )
    return (
        f"Le test n'a pas pu s'exécuter : {first}"
        if first
        else ("Le test n'a pas pu s'exécuter.")
    )


def mark_failed_cases(test_cases: list, messages: list | None = None) -> list:
    """Tague les cas morts-nés (statut d'exécution en échec) et les renvoie.

    Le stub `FAILED_AUTO_GEN` du circuit-breaker de `test_evaluator` part sur le canal
    `examples`, que la CLI ne lit pas (`_extract_test_cases` ne parcourt que
    messages/RESULTS) — le marquage n'atteignait donc jamais le fichier. On le dérive
    ici du `status` porté par chaque cas, ce qui couvre AUSSI les échecs hors
    circuit-breaker (transpile `error`, sortie de boucle anticipée). Contrairement au
    stub, on ne vide PAS les données : elles sont souvent à un patch près de
    fonctionner, et le mode additif doit pouvoir repartir d'elles.

    Pose aussi le verdict qui manquait au fichier (root-cause 12/07 : `verdict=null`
    sur 66/110 modèles d'éval) : `Insuffisant` + cause typée + explication reprise du
    dernier message EVALUATION du test (fallback : dérivée de l'erreur d'exécution).
    Le PASS « vide intentionnel » (verdict Bon/Excellent) n'est jamais marqué — cf.
    `is_deadborn_case`. Idempotent : un mort-né déjà marqué garde son explication.
    """
    failed = [tc for tc in test_cases if is_deadborn_case(tc)]
    explanations = _last_evaluation_by_test_index(messages or [])
    for tc in failed:
        tags = list(tc.get("tags") or [])
        tags += [
            t for t in ("FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED") if t not in tags
        ]
        tc["tags"] = tags
        if not tc.get("verdict"):
            tc["verdict"] = "Insuffisant"
        if not tc.get("reason_type"):
            tc["reason_type"] = (
                "execution_error" if tc.get("status") == "error" else "bad_data"
            )
        if not tc.get("evaluation_explanation"):
            tc["evaluation_explanation"] = explanations.get(
                str(tc.get("test_index"))
            ) or _fallback_explanation(tc)
    return failed


def merge_test_cases(existing: list | None, generated: list | None) -> list:
    """Fusionne les cas générés DANS les cas existants, sans rien détruire.

    - Les cas existants sont préservés tels quels (avec leurs assertions-specs).
    - Un cas généré n'est ajouté que si son test_uid n'existe pas déjà : un doublon
      régénéré est ignoré au profit de l'existant (qui porte les specs).
    - Un cas généré sans test_uid est toujours ajouté (impossible à dédupliquer).
    """
    existing = existing or []
    generated = generated or []
    seen = {tc.get("test_uid") for tc in existing if tc.get("test_uid")}
    merged = list(existing)
    for g in generated:
        uid = g.get("test_uid")
        if uid and uid in seen:
            continue
        merged.append(g)
        if uid:
            seen.add(uid)
    return merged


def apply_generation_result(
    existing_doc: dict | None,
    generated_cases: list,
    *,
    sql: str,
    used_columns: list[str],
    suggestions: list[str] | None,
    overwrite: bool,
    path_plans: str | None = None,
) -> dict:
    """Construit le document à écrire selon le mode.

    - `overwrite` ou pas de fichier existant → suite fraîche (écrasement / bootstrap).
    - sinon (additif, défaut) → préserve le doc existant (specs + champs annexes
      type source_hash/query_decomposed) et n'ajoute que les nouveaux cas.

    `path_plans` (catalogue UNION ALL) est persisté pour qu'un modèle généré en CLI
    puis ouvert côté serveur garde le focus par branche (l'executor en a besoin pour
    résoudre le SQL slicé d'un test focalisé).
    """
    if overwrite or not existing_doc:
        doc: dict = {
            "sql": sql,
            "used_columns": used_columns,
            "test_cases": generated_cases,
        }
        if suggestions:
            doc["suggestions"] = suggestions
        if path_plans:
            doc["path_plans"] = path_plans
        return doc

    doc = dict(existing_doc)
    doc["test_cases"] = merge_test_cases(
        existing_doc.get("test_cases"), generated_cases
    )
    doc["sql"] = sql
    doc["used_columns"] = used_columns
    if suggestions:
        doc["suggestions"] = suggestions
    if path_plans:
        doc["path_plans"] = path_plans
    return doc


def _carry_specs(old: dict, new: dict) -> dict:
    """Reporte les assertions-specs (porteuses d'assertion_uid) de l'ancien cas sur le
    nouveau, en gardant le test_uid stable. Les specs sont préfixées et jamais dupliquées.
    """
    merged = dict(new)
    merged["test_uid"] = old.get("test_uid")
    new_assertions = list(new.get("assertion_results") or [])
    present_spec_uids = {
        a.get("assertion_uid") for a in new_assertions if a.get("assertion_uid")
    }
    old_specs = [
        a
        for a in (old.get("assertion_results") or [])
        if a.get("assertion_uid") and a.get("assertion_uid") not in present_spec_uids
    ]
    merged["assertion_results"] = old_specs + new_assertions
    return merged


def replace_test_case_preserving_specs(
    existing_cases: list | None, generated_cases: list | None, target_uid: str
) -> list:
    """Remplace le cas ciblé (test_uid) par sa version régénérée, en préservant ses specs.

    Les autres cas sont intacts. Si l'agent n'a pas produit de cas pour `target_uid`,
    la liste est renvoyée inchangée (no-op — le câblage CLI émet alors un warning).
    """
    existing_cases = existing_cases or []
    generated_cases = generated_cases or []
    gen_by_uid = {c.get("test_uid"): c for c in generated_cases if c.get("test_uid")}
    updated = gen_by_uid.get(target_uid)
    # L'executor ré-émet souvent le test modifié SANS test_uid. Si un seul cas est
    # produit et qu'il n'a pas d'uid, c'est forcément le test ciblé (l'agent était
    # scopé sur target_uid via state["test_uid"]) → on le rattache.
    if (
        updated is None
        and len(generated_cases) == 1
        and not generated_cases[0].get("test_uid")
    ):
        updated = generated_cases[0]
    if updated is None:
        return existing_cases
    return [
        _carry_specs(c, updated) if c.get("test_uid") == target_uid else c
        for c in existing_cases
    ]


def apply_update_result(
    existing_doc: dict,
    generated_cases: list,
    *,
    target_uid: str,
    sql: str,
    used_columns: list[str],
) -> dict:
    """Document à écrire après un `update-test` : remplace le cas ciblé (specs préservées),
    rafraîchit sql/used_columns, et conserve tous les champs annexes du doc existant.
    """
    doc = dict(existing_doc)
    doc["test_cases"] = replace_test_case_preserving_specs(
        existing_doc.get("test_cases"), generated_cases, target_uid
    )
    doc["sql"] = sql
    doc["used_columns"] = used_columns
    return doc


def model_test_path(output_dir: Path, model_name: str) -> Path:
    """Test-file path for a model, mirroring test_runner/check : {model_name}.json
    sous output_dir. Utilise le model_name relatif (niché : demo/payment_summary),
    pas le seul stem — sinon les modèles dbt (staging/, marts/) seraient introuvables.
    """
    return output_dir / f"{model_name}.json"


def _write_test_file(
    out_path: Path,
    sql: str,
    used_columns: list[str],
    generated_cases: list,
    suggestions: list[str] | None = None,
    *,
    overwrite: bool = False,
    path_plans: str | None = None,
) -> tuple[Path, int]:
    """Write the {model_name}.json test file (additive by default, full rebuild on overwrite).

    Returns (path, n_added) where n_added is the number of NEW test cases written
    (= total in overwrite/bootstrap mode).
    """
    out_path.parent.mkdir(parents=True, exist_ok=True)
    existing_doc = read_test_doc(out_path) if out_path.exists() else None
    before = len((existing_doc or {}).get("test_cases", [])) if not overwrite else 0
    doc = apply_generation_result(
        existing_doc,
        generated_cases,
        sql=sql,
        used_columns=used_columns,
        suggestions=suggestions,
        overwrite=overwrite,
        path_plans=path_plans,
    )
    write_test_doc(out_path, doc)
    return out_path, len(doc["test_cases"]) - before


# ── Business context ──────────────────────────────────────────────────────────


def _load_model_context(model_name: str, models_base: Path) -> str:
    """Collect mocksql.md files for model_name relative to models_base."""
    if not model_name:
        return ""
    parts = Path(model_name).parts
    fragments: list[str] = []
    for i in range(len(parts)):
        level_dir = models_base.joinpath(*parts[:i])
        candidate = level_dir / "mocksql.md"
        if candidate.exists():
            text = candidate.read_text(encoding="utf-8").strip()
            if text:
                fragments.append(text)
    file_md = models_base / f"{model_name}.md"
    if file_md.exists():
        text = file_md.read_text(encoding="utf-8").strip()
        if text:
            fragments.append(text)
    return "\n\n---\n\n".join(fragments)


# ── Entrypoint ────────────────────────────────────────────────────────────────


def _run_profile_bq(
    schemas: list[dict], sql: str, dialect: str, billing_project: str
) -> dict:
    """Run BigQuery profiling queries and return a normalized profile dict."""
    from utils.optional_deps import import_bigquery

    from build_query.profile_checker import _to_profiler_schema
    from build_query.profiler import profile_joins_for_query, profile_schema

    _bq = import_bigquery()
    client = _bq.Client(project=billing_project)

    def executor(bq_sql: str) -> list[dict]:
        return [dict(row) for row in client.query(bq_sql).result()]

    schema_for_profiler = _to_profiler_schema(schemas)
    profile = profile_schema(schema_for_profiler, executor, dialect=dialect)
    profile["joins"] = profile_joins_for_query(
        schema_for_profiler, sql, executor, dialect=dialect
    )
    return profile


async def run_generate(
    model: Path,
    config: Path,
    output_dir: Path,
    profile: bool = False,
    overwrite: bool = False,
    instruction: str | None = None,
    update_uid: str | None = None,
    target_path: str | None = None,
) -> None:
    import typer

    import storage.config as storage_config
    from models.env_variables import validate_required_env

    # Aligne storage.config (lu par apply_duckdb_extensions, etc.) sur le projet
    # ciblé par --config, sinon il retombe sur le cwd (back/) sans mocksql.yml.
    os.environ["MOCKSQL_BASE_DIR"] = str(config.resolve().parent)
    storage_config.load_config.cache_clear()

    validate_required_env()

    from init.init_db import run_migrations
    from models.database import db_pool

    await db_pool.init_pool()
    await run_migrations()

    cfg = load_config(config)
    dialect = cfg.get("dialect", "bigquery")
    cache_path = str(
        config.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    preprocessor_fn = cfg.get("preprocessor_fn")

    # dbt connector : si un bloc `dbt:` est configuré, MockSQL lit le SQL **compilé**
    # (refs résolus, macros rendues) et infère les schémas amont depuis le manifest —
    # sans jamais interroger l'entrepôt.
    dbt_project = storage_config.get_dbt_project()
    models_path_str = cfg.get("models_path", "./models")
    models_base = (config.parent / models_path_str).resolve()
    try:
        model_name = model.resolve().relative_to(models_base).with_suffix("").as_posix()
    except ValueError:
        model_name = model.stem

    # Step 1 — read SQL (DECLARE/SET preambles are stripped inside read_sql)
    typer.echo(f"Reading {model}...")
    if dbt_project and dbt_project.is_dbt_model(model_name):
        typer.echo(f"[dbt] SQL compilé depuis le manifest pour '{model_name}'.")
        compiled = dbt_project.compiled_sql_for_model(model_name)
        sql = extract_select_statement(compiled, dialect) or compiled
    else:
        sql = read_sql(model, preprocessor_fn, config.parent, dialect)

    # Step 1.5 — fail fast if the query requires generating too many rows
    from build_query.constraint_simplifier import (
        check_correlated_aggregate_cardinality,
        check_having_cardinality,
    )

    try:
        check_having_cardinality(sql, dialect)
        check_correlated_aggregate_cardinality(sql, dialect)
    except ValueError as exc:
        typer.echo(f"[ERROR] {exc}", err=True)
        raise typer.Exit(1)

    # Step 2 — extract table refs
    refs = extract_real_table_refs(sql, dialect)
    if not refs:
        typer.echo("[WARN] No source tables found in the SQL.")
    else:
        ref_names = [".".join(p for p in [r.catalog, r.db, r.name] if p) for r in refs]
        typer.echo(f"Found {len(refs)} source table(s): {ref_names}")

    billing_project = os.getenv("BQ_TEST_PROJECT") or os.getenv("VERTEX_PROJECT")

    # Step 3 — resolve schemas via cache local + fetch BigQuery des manquants.
    # En mode dbt, le SQL est déjà compilé (refs résolus en noms réels) ; la résolution
    # de schéma passe par ce MÊME chemin — le connecteur dbt ne fournit pas les schémas.
    cached = load_schema_cache(cache_path)
    schemas, missing = match_refs_against_cache(refs, cached)

    if missing:
        typer.echo(f"Fetching schema for: {missing}")
        if not billing_project:
            typer.echo(
                "[ERROR] BQ_TEST_PROJECT not set. Cannot fetch schemas from BigQuery. "
                "Set it in your .env or shell environment."
            )
            raise typer.Exit(1)

        unqualified = [r for r in missing if not validate_bq_ref(r)]
        if unqualified:
            typer.echo(
                f"[WARN] Unqualified table refs (need project.dataset.table): {unqualified}"
            )

        to_fetch = [r for r in missing if validate_bq_ref(r)]
        if to_fetch:
            schema_rows, failed, partitions = await fetch_tables_schema(
                to_fetch, billing_project
            )
            if failed:
                typer.echo(f"[WARN] Could not fetch: {[f['table'] for f in failed]}")
            if schema_rows:
                new_tables = generate_tables_and_columns_from_project_schema(
                    {"data": schema_rows}
                )
                if partitions:
                    for tbl in new_tables:
                        full_name = tbl.get("table_name", "")
                        info = partitions.get(full_name) or partitions.get(
                            full_name.split(".")[-1]
                        )
                        if info:
                            tbl["partition"] = info
                updated = merge_into_cache(cached, new_tables)
                save_schema_cache(cache_path, updated)
                typer.echo(f"[OK] Schema cache updated ({len(new_tables)} table(s)).")
                schemas, _ = match_refs_against_cache(refs, updated)

    if not schemas:
        typer.echo("[ERROR] No schemas available — cannot generate tests.")
        raise typer.Exit(1)

    # Step 3.5 — profile (optional)
    profile_data: dict | None = None
    if profile:
        if not billing_project:
            typer.echo(
                "[ERROR] --profile requires BQ_TEST_PROJECT. "
                "Set it in your .env or shell environment."
            )
            raise typer.Exit(1)
        typer.echo("Profiling tables on BigQuery (this may take a moment)...")
        try:
            profile_data = _run_profile_bq(schemas, sql, dialect, billing_project)
            typer.echo(
                f"[OK] Profile complete ({len(profile_data.get('tables', {}))} table(s), "
                f"{len(profile_data.get('joins', []))} join(s))."
            )
        except Exception as exc:
            typer.echo(f"[WARN] Profiling failed: {exc}. Continuing without profile.")

    # Step 4 — build state + inject schemas into in-memory cache
    # (model_name / models_base déjà calculés en amont pour la résolution dbt)
    project_id = model.stem
    session_id = str(uuid.uuid4())

    model_context = _load_model_context(model_name, models_base) or None

    state = build_initial_state(sql, dialect, schemas, project_id, session_id)

    # Garde-fou : colonnes du SQL absentes du schéma en cache → schéma périmé.
    # On échoue tôt (avant l'appel LLM) avec la commande de refresh ciblée.
    schema_gaps = find_used_columns_missing_from_schema(state["used_columns"], schemas)
    if schema_gaps:
        typer.echo(
            "[ERROR] Colonne(s) référencée(s) dans le SQL mais absente(s) du schéma "
            "en cache :",
            err=True,
        )
        for table_name, cols in schema_gaps:
            typer.echo(f"  - {table_name}: {', '.join(cols)}", err=True)
        refreshable = [tn for tn, _ in schema_gaps if len(tn.split(".")) == 3]
        typer.echo(
            "\nLe schéma en cache est probablement périmé. Rafraîchis-le puis "
            "relance la génération :",
            err=True,
        )
        if refreshable:
            hint = " ".join(f"-t {tn}" for tn in refreshable)
            typer.echo(f"  mocksql refresh-schemas {hint}", err=True)
        else:
            typer.echo("  mocksql refresh-schemas", err=True)
        typer.echo(
            "(Si la colonne n'existe vraiment pas dans la table, corrige le SQL.)",
            err=True,
        )
        raise typer.Exit(1)

    if model_context:
        state["model_context"] = model_context
        typer.echo(f"[OK] Business context loaded ({len(model_context)} chars).")
    if profile_data:
        state["profile"] = profile_data
        state["profile_complete"] = True

    # Step 4.5 — route le conversational_agent (cf. routing : input + has_existing_tests) :
    #   - update_uid → MODIFIE le test ciblé (specs préservées à la réécriture) ;
    #   - sinon, mode additif (défaut) → AJOUTE un cas sans toucher aux tests/specs existants ;
    #   - --overwrite → reconstruction complète (chemin première génération, pas de prep ici).
    out_path = model_test_path(output_dir, model_name)
    existing_cases: list = []
    if out_path.exists():
        existing_cases = (read_test_doc(out_path) or {}).get("test_cases") or []

    def _inject_existing(input_text: str) -> None:
        from langchain_core.messages import AIMessage

        from utils.msg_types import MsgType

        state["has_existing_tests"] = True
        state["input"] = input_text
        # retrieve_existing_tests lit en priorité un message RESULTS in-pipeline : on injecte
        # les cas existants ainsi, faute de session persistée en base dans le flux CLI offline.
        state["messages"] = [
            AIMessage(
                content=json.dumps(existing_cases, default=str),
                additional_kwargs={"type": MsgType.RESULTS},
            )
        ]

    if update_uid:
        if not any(c.get("test_uid") == update_uid for c in existing_cases):
            known = [c.get("test_uid") for c in existing_cases]
            typer.echo(
                f"[ERROR] test_uid '{update_uid}' introuvable. Connus : {known}",
                err=True,
            )
            raise typer.Exit(1)
        state["test_uid"] = update_uid
        _inject_existing(instruction or "")
        typer.echo(f"[update] test {update_uid} ciblé — specs préservées.")
    elif existing_cases and not overwrite:
        # suggestion_intent force l'agent à produire une ACTION de test (jamais du texte libre).
        state["suggestion_intent"] = True
        _inject_existing(
            instruction
            or "Ajoute un nouveau test couvrant un cas limite non encore couvert."
        )
        typer.echo(
            f"[additif] {len(existing_cases)} test(s) existant(s) préservé(s) — "
            f"ajout d'un cas{' ciblé' if instruction else ''} "
            f"(--overwrite pour reconstruire à la place)."
        )

    if target_path:
        # Focus par branche issu d'une suggestion (suggestion_paths, cf. `mocksql suggest
        # use`) : posé comme focus autoritaire — l'agent le valide contre path_plans et
        # appelle set_target_path sans deviner de nom (cf. conversational_agent).
        state["target_path"] = target_path

    _inject_schemas_into_cache(project_id, schemas)

    # Qualify the SQL using the same optimize_query path as the UI validator.
    # This applies qualify_columns + _fix_unnest_alias_conflicts + _fix_unnest_scope_leak,
    # which prevents DuckDB "Ambiguous reference" errors on UNNEST aliases.
    from common_vars import get_tables_mapping
    from build_query.validator import expand_positional_group_by, optimize_query

    try:
        tables_mapping = await get_tables_mapping(project_id)
        parsed_ast = sqlglot.parse_one(sql, read=dialect)
        qualified_ast = optimize_query(parsed_ast, tables_mapping, dialect=dialect)
        state["optimized_sql"] = qualified_ast.sql(dialect=dialect, pretty=True)
    except Exception as e:
        # Une « Unknown column » due à une table étoilée au schéma tronqué = cache
        # périmé : on échoue tôt (sinon le repli SQL brut fabrique un test cassé — la
        # table DuckDB n'a pas la colonne) avec la commande de refresh ciblée.
        stale = diagnose_stale_schema_from_qualify_error(sql, schemas, dialect, str(e))
        if stale:
            typer.echo(
                "[ERROR] Schéma en cache incomplet : une table lue en `SELECT *` ne "
                "contient pas une colonne utilisée par la requête (cache périmé).",
                err=True,
            )
            for tn in stale:
                typer.echo(f"  - {tn}", err=True)
            refreshable = [tn for tn in stale if len(tn.split(".")) == 3]
            typer.echo("\nRafraîchis le schéma puis relance la génération :", err=True)
            if refreshable:
                hint = " ".join(f"-t {tn}" for tn in refreshable)
                typer.echo(f"  mocksql refresh-schemas {hint}", err=True)
            else:
                typer.echo("  mocksql refresh-schemas", err=True)
            raise typer.Exit(1)
        typer.echo(f"[WARN] SQL qualification failed ({e}), using raw SQL.")
        # Repli sur le SQL brut : qualify n'a pas tourné, donc le GROUP BY positionnel
        # survit. On le binde au moins aux colonnes du SELECT pour éviter un
        # « GROUP BY out of range » si une projection est élaguée plus loin.
        try:
            guarded = expand_positional_group_by(sqlglot.parse_one(sql, read=dialect))
            state["optimized_sql"] = guarded.sql(dialect=dialect, pretty=True)
        except Exception:
            pass

    from build_query.query_chain import _lightweight_query_decomposed

    state["query_decomposed"] = _lightweight_query_decomposed(
        state.get("optimized_sql") or sql, dialect
    )

    # Catalogue des paths UNION ALL (la CLI ne passe pas par validate_query qui le pose
    # en mode serveur) → focus par branche dispo aussi en CLI/éval/démo. None si pas
    # d'union de 1er niveau exploitable (comportement inchangé).
    try:
        import json as _json

        from build_query.path_slicer import build_path_plans

        _plans = build_path_plans(
            state.get("optimized_sql") or sql,
            _json.loads(state["query_decomposed"] or "[]"),
            state.get("used_columns") or [],
            dialect,
        )
        state["path_plans"] = _json.dumps(_plans) if _plans else None
    except Exception as e:
        typer.echo(f"[WARN] build_path_plans failed ({e}), path 'all' only.")
        state["path_plans"] = None

    _patch_db_calls()

    # Step 5 — run graph (same as UI, history_saver neutralised above)
    from build_query.query_chain import build_query_graph

    typer.echo(f"Generating tests for {project_id} ({len(schemas)} table(s))...")
    graph = build_query_graph()
    final_state = await graph.ainvoke(state, config={"recursion_limit": 50})

    if final_state.get("error"):
        err = final_state["error"]
        typer.echo(f"[ERROR] {err[:500]}{'…' if len(err) > 500 else ''}")
        raise typer.Exit(1)

    # Step 6 — write outputs
    test_cases = _extract_test_cases(final_state)
    suggestions = _extract_suggestions(final_state)
    failed_cases = (
        mark_failed_cases(test_cases, final_state.get("messages")) if test_cases else []
    )
    if test_cases and update_uid:
        existing_doc = read_test_doc(out_path)
        before = next(
            (
                c
                for c in existing_doc.get("test_cases", [])
                if c.get("test_uid") == update_uid
            ),
            None,
        )
        doc = apply_update_result(
            existing_doc,
            test_cases,
            target_uid=update_uid,
            sql=sql,
            used_columns=state["used_columns"],
        )
        after = next(
            (c for c in doc["test_cases"] if c.get("test_uid") == update_uid), None
        )
        write_test_doc(out_path, doc)
        if before != after:
            typer.echo(f"[OK] test {update_uid} mis à jour → {out_path}")
        else:
            typer.echo(
                f"[WARN] L'agent n'a pas modifié le test {update_uid} (aucun changement)."
            )
    elif test_cases:
        out_path, n_added = _write_test_file(
            out_path,
            sql,
            state["used_columns"],
            test_cases,
            suggestions,
            overwrite=overwrite,
            path_plans=state.get("path_plans"),
        )
        action = "écrits (reconstruction)" if overwrite else "ajoutés"
        typer.echo(f"[OK] {n_added} test case(s) {action} → {out_path}")
    else:
        typer.echo("[WARN] No output produced — check the SQL and schemas.")

    if suggestions:
        typer.echo("\nSuggestions de cas non couverts :")
        for i, s in enumerate(suggestions, 1):
            typer.echo(f"  {i}. {s}")

    # Morts-nés : le fichier est écrit (les données restent exploitables) mais l'échec
    # doit être VISIBLE — `[OK] … écrits` seul était indistinguable d'un succès et
    # l'éval batch enchaînait sans broncher (root-cause spider2-snow, 66 modèles).
    # rc reste 0 (décision 13/07) : le fichier EST écrit, et un rc≠0 casserait les
    # harnais batch (set -e) ; l'échec est porté par [FAIL] + verdict/exec_status.
    if failed_cases:
        for tc in failed_cases:
            name = (
                tc.get("test_name")
                or tc.get("unit_test_description")
                or f"test {tc.get('test_index')}"
            )
            err = str(tc.get("exec_error") or tc.get("error") or "").strip()
            detail = f" — {err[:200]}" if err else ""
            typer.echo(f"[FAIL] {name} : {tc.get('status')}{detail}", err=True)
        typer.echo(
            f"[FAIL] {len(failed_cases)} test(s) mort-né(s) (verdict Insuffisant, "
            f"exec_status posé dans {out_path}) — corrige les données ou relance "
            "la génération.",
            err=True,
        )
