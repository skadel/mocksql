"""mocksql test — replay saved test cases against DuckDB (no LLM calls)."""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path

import yaml

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)


# ── Config / cache helpers ────────────────────────────────────────────────────


def _load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_schema_cache(cache_path: str) -> list[dict]:
    p = Path(cache_path)
    if not p.exists():
        return []
    with open(p, encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return data.get("tables", [])
    return data


def _read_json(p: Path) -> dict | None:
    # Définition commitée + cache sidecar (absent en CI/clone) fusionnés ; `used_columns`
    # est ré-encodé en list[str] côté mémoire → le `json.loads` plus bas reste valide.
    from storage.test_files import read_test_doc

    return read_test_doc(p)


# ── Source SQL resolution ─────────────────────────────────────────────────────


def resolve_run_sql(
    cfg: dict,
    config_path: Path,
    model_name: str,
    snapshot_sql: str,
    frozen: bool,
) -> tuple[str, str]:
    """Résout le SQL à rejouer pour un modèle.

    Retourne (sql, source) où source vaut :
      - "frozen"            : --frozen → snapshot figé dans le JSON.
      - "disk"              : SQL lu depuis le `.sql` source (défaut) + preprocessor.
      - "snapshot-fallback" : source introuvable/illisible → snapshot (warning amont).

    Le défaut lit le DISQUE pour que `test` reflète ce que l'utilisateur/agent a
    réellement écrit. Le fallback évite un crash sur les suites portables
    (examples/spider) qui n'ont pas le `.sql` source à côté.
    """
    if frozen:
        return snapshot_sql, "frozen"

    models_path = Path(cfg.get("models_path", "models"))
    if not models_path.is_absolute():
        models_path = config_path.parent / models_path
    sql_file = models_path / f"{model_name}.sql"
    if not sql_file.exists():
        return snapshot_sql, "snapshot-fallback"

    from cli.generate import read_sql

    dialect = cfg.get("dialect", "bigquery")
    preprocessor_fn = cfg.get("preprocessor_fn")
    try:
        return (
            read_sql(sql_file, preprocessor_fn, config_path.parent, dialect),
            "disk",
        )
    except Exception:
        return snapshot_sql, "snapshot-fallback"


# ── Schema resolution ─────────────────────────────────────────────────────────


def _schemas_from_cache(used_columns_raw: list[str], cache: list[dict]) -> list[dict]:
    """Schémas COMPLETS des tables du cache référencées par `used_columns`.

    On identifie les tables via `used_columns` (une entrée par table), mais on renvoie le
    schéma **complet** (toutes les colonnes réelles), **sans filtrer** par la liste
    `used_columns` : le réplay crée la table telle qu'en prod, pour que TOUTE colonne
    référencée par le SQL existe. Filtrer par un `used_columns` incomplet (extraction
    ratée en amont) droppait une colonne pourtant utilisée par le SQL →
    "Referenced column ... not found in FROM clause" (cf. bq234, `total_day_supply`).
    """
    idx: dict[str, dict] = {}
    for s in cache:
        name = s["table_name"].lower()
        idx[name] = s
        parts = name.split(".")
        if len(parts) >= 2:
            idx[".".join(parts[-2:])] = s
        if parts:
            idx[parts[-1]] = s

    result: list[dict] = []
    seen: set[str] = set()
    for raw in used_columns_raw:
        try:
            u = json.loads(raw)
        except Exception:
            continue
        project = u.get("project", "")
        database = u.get("database", "")
        table = u.get("table", "")

        candidates: list[str] = []
        if project and database:
            candidates.append(f"{project}.{database}.{table}".lower())
        if database:
            candidates.append(f"{database}.{table}".lower())
        candidates.append(table.lower())

        schema = next((idx[c] for c in candidates if c in idx), None)
        if not schema:
            continue
        key = schema["table_name"].lower()
        if key in seen:
            continue
        seen.add(key)
        result.append(schema)
    return result


def _flatten_table_key(name: str) -> str:
    """Aplati un nom de table (`proj.dataset.table` ou clé de données déjà plate)
    vers la forme `dataset_table` minuscule — le même format que le nom de table
    DuckDB créé (`create_test_tables`) et que la référence réécrite dans le SQL
    (`strip_qualifiers_with_scope`). Sert à rapprocher schémas et tables de données.
    """
    parts = name.replace("`", "").split(".")
    base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
    return base.lower()


def _full_refs_from_used_columns(used_columns_raw: list[str]) -> dict[str, str]:
    """Mappe la clé de table aplatie → réf BQ complète (`project.dataset.table`),
    reconstruite depuis les `used_columns` sauvegardés. Sert à afficher une commande
    `refresh-schemas -t …` actionnable quand un schéma manque.
    """
    refs: dict[str, str] = {}
    for raw in used_columns_raw:
        try:
            u = json.loads(raw)
        except Exception:
            continue
        project = u.get("project", "")
        database = u.get("database", "")
        table = u.get("table", "")
        if not table:
            continue
        full = ".".join(p for p in (project, database, table) if p)
        key = _flatten_table_key(f"{database}.{table}" if database else table)
        refs[key] = full
    return refs


def collect_test_table_refs(tests_root: Path) -> list[str]:
    """Toutes les réfs `project.dataset.table` référencées par les tests sauvegardés
    (via leurs `used_columns`), dédupliquées et triées.

    Sert à `mocksql refresh-schemas --from-tests` : importer/rafraîchir d'un coup le
    schéma de tout ce que les tests utilisent (le réplay `test` exige le vrai schéma).
    Ignore les fichiers de session nommés en UUID, comme `run_tests`.
    """
    if not tests_root.exists():
        return []
    refs: set[str] = set()
    for f in tests_root.rglob("*.json"):
        if _UUID_RE.match(f.stem):
            continue
        doc = _read_json(f)
        if not doc:
            continue
        refs.update(
            _full_refs_from_used_columns(doc.get("used_columns") or []).values()
        )
    return sorted(refs)


class SchemaMissingError(Exception):
    """Une table référencée par un test n'a pas de schéma dans le `schema_cache`.

    `mocksql test` rejoue avec le VRAI schéma de l'entrepôt (fidélité prod) et n'infère
    jamais depuis les lignes — inférer masquerait un bug de type réel (ex. colonne
    date-like → VARCHAR → "Cannot compare VARCHAR and DATE"). Le message pointe vers
    `refresh-schemas -t …` pour importer le schéma manquant.
    """

    def __init__(self, missing_tables: list[str], full_refs: dict[str, str]) -> None:
        self.missing_tables = missing_tables
        refs = [full_refs.get(_flatten_table_key(t), t) for t in missing_tables]
        cmd = "mocksql refresh-schemas " + " ".join(f"-t {r}" for r in refs)
        super().__init__(
            f"Schéma introuvable pour {len(refs)} table(s) référencée(s) par ce test : "
            f"{', '.join(refs)}. Le replay utilise le vrai schéma de l'entrepôt (aucune "
            f"inférence). Importe le schéma manquant puis relance :\n  {cmd}"
        )


def _resolve_model_schemas(
    used_columns_raw: list[str],
    schema_cache: list[dict],
    test_cases: list[dict],
) -> list[dict]:
    """Résout les schémas des tables du modèle depuis le `schema_cache` — SOURCE UNIQUE.

    Les cas d'un même modèle partagent le même SQL → le même schéma : les tables DuckDB
    sont créées une seule fois par modèle.

    Fidélité prod (pas d'inférence) : le replay utilise le VRAI schéma de l'entrepôt
    (types compris), jamais un schéma deviné depuis les lignes synthétiques. Inférer
    masquerait un bug de type réel — une colonne date-like typée VARCHAR passerait le
    test alors qu'elle casse en prod ("Cannot compare VARCHAR and DATE"). Toute table
    présente dans les données mais absente du cache lève donc `SchemaMissingError`, qui
    pointe vers `refresh-schemas` pour importer le schéma manquant.
    """
    # Tables réellement présentes dans les données d'au moins un cas → elles doivent
    # toutes avoir un schéma dans le cache.
    data_tables: set[str] = set()
    for tc in test_cases:
        for tname, rows in (tc.get("data") or {}).items():
            if isinstance(rows, list) and rows:
                data_tables.add(tname)

    schemas = (
        _schemas_from_cache(used_columns_raw, schema_cache) if used_columns_raw else []
    )
    covered = {_flatten_table_key(s["table_name"]) for s in schemas}
    missing = [t for t in data_tables if _flatten_table_key(t) not in covered]
    if missing:
        raise SchemaMissingError(
            sorted(missing), _full_refs_from_used_columns(used_columns_raw)
        )
    return schemas


# ── Assertion SQL remapping ───────────────────────────────────────────────────


def _remap_assertion_sql(sql: str, data_keys: list[str], case_suffix: str) -> str:
    """Replace old session-scoped DuckDB table names with the current case_suffix.

    Assertions saved during `generate` contain hardcoded table names like
    "the_met_objects_<old_uuid>". When replaying with `test`, tables are
    created with a new suffix, so we patch the SQL before evaluation.
    """
    for base in data_keys:
        # Match double-quoted DuckDB table names: "base_<anything>"
        sql = re.sub(
            r'"(' + re.escape(base) + r')_[^"]+"',
            f'"\\1_{case_suffix}"',
            sql,
        )
    return sql


# ── Single test-case execution ────────────────────────────────────────────────


async def _run_one_case(
    test_case: dict,
    sql: str,
    duckdb_schemas: list[dict],
    used_columns_parsed: list[dict],
    dialect: str,
    suffix: str,
    con,
    precompiled_sql: str,
    setup_error: str | None = None,
) -> dict:
    """Rejoue UN cas dans les tables déjà créées par modèle (cf. `_setup_model`).

    Les tables et le SQL transpilé sont partagés par tous les cas du modèle : ici on se
    contente de vider les tables, d'insérer les données du cas, d'exécuter le SQL
    pré-transpilé, puis d'évaluer les assertions. `suffix` est le suffixe STABLE du modèle
    (pas de `test_index` concaténé) — il est commun à toutes les tables et au SQL.

    `setup_error` : si le setup modèle a échoué (schéma manquant, DDL/transpile), le cas
    ne peut pas s'exécuter. On classe quand même les cas sans données/assertions en `skip`
    (rien à exécuter), et on remonte l'erreur sur les cas exécutables SANS toucher DuckDB
    (éviter d'exécuter à vide, qui logue des erreurs `Failed to run query` trompeuses).
    """
    from build_query.assertion_eval import _evaluate_assertions
    from utils.examples import execute_queries, run_query_on_test_dataset
    from utils.insert_examples import insert_examples, replace_missing_with_null

    test_index = str(test_case.get("test_index", "0"))
    # Titre court (`test_name`, 3–6 mots) affiché en tête de chaque test ; la
    # `unit_test_description` (phrase complète) sert de sous-ligne descriptive.
    test_name = (test_case.get("test_name") or "").strip()
    description = (test_case.get("unit_test_description") or "").strip()
    name = test_name or description or f"Test {test_index}"
    meta = {"name": name, "description": description}
    data: dict = test_case.get("data") or {}
    saved_assertions = [
        a for a in (test_case.get("assertion_results") or []) if a.get("sql")
    ]

    if not data:
        return {
            "index": test_index,
            **meta,
            "status": "skip",
            "reason": "no data",
            "assertions": [],
        }
    if not saved_assertions:
        return {
            "index": test_index,
            **meta,
            "status": "skip",
            "reason": "no assertions",
            "assertions": [],
        }
    if setup_error is not None:
        return {
            "index": test_index,
            **meta,
            "status": "error",
            "error": setup_error,
            "assertions": [],
        }

    try:
        # Vide les tables partagées avant d'insérer les données de CE cas (les lignes du
        # cas précédent ne doivent pas fuiter).
        for sch in duckdb_schemas:
            con.execute(f'DELETE FROM "{sch["table_name"]}"')

        test_data = replace_missing_with_null(data, duckdb_schemas)
        insert_stmts = list(
            insert_examples(
                data_dict=test_data,
                schemas=duckdb_schemas,
                suffix=suffix,
                used_columns=used_columns_parsed or None,
            )
        )
        execute_queries(insert_stmts, con)

        result_df, _ = await run_query_on_test_dataset(
            sql, suffix, "cli", dialect, con, precompiled_sql=precompiled_sql
        )

        remapped_assertions = [
            {
                **a,
                "sql": _remap_assertion_sql(
                    a.get("sql", ""), list(data.keys()), suffix
                ),
            }
            for a in saved_assertions
        ]

        view_name = f"__result__{suffix}"
        con.register(view_name, result_df)
        try:
            assertion_results = _evaluate_assertions(
                remapped_assertions, view_name, con
            )
        finally:
            con.execute(f'DROP VIEW IF EXISTS "{view_name}"')

        all_passed = all(a.get("passed", False) for a in assertion_results)
        return {
            "index": test_index,
            **meta,
            "status": "pass" if all_passed else "fail",
            "assertions": assertion_results,
        }
    except Exception as exc:
        return {
            "index": test_index,
            **meta,
            "status": "error",
            "error": str(exc),
            "assertions": [],
        }


async def _setup_model(
    schemas: list[dict],
    sql: str,
    dialect: str,
    suffix: str,
    con,
) -> tuple[list[dict], str]:
    """Crée les tables DuckDB et transpile le SQL UNE FOIS par modèle.

    Tous les cas d'un modèle partagent le même schéma et le même SQL : on évite ainsi de
    re-parser le DDL et le SQL via sqlglot à chaque cas (le poste dominant après les
    imports). Retourne (duckdb_schemas, precompiled_sql).
    """
    from utils.examples import create_test_tables, fix_duck_db_sql, parse_test_query

    duckdb_schemas = create_test_tables(
        tables=schemas, suffix=suffix, overwrite=True, con=con, dialect=dialect
    )
    duckdb_sql = await parse_test_query(sql, suffix, dialect)
    precompiled_sql = fix_duck_db_sql(duckdb_sql, dialect)
    return duckdb_schemas, precompiled_sql


# ── Main entrypoint ───────────────────────────────────────────────────────────


async def run_tests(
    config_path: Path,
    model_filters: list[str] | None = None,
    fail_fast: bool = False,
    frozen: bool = False,
) -> tuple[int, list[dict]]:
    """
    Replay all saved test cases from .mocksql/tests/ against DuckDB.

    Returns (exit_code, model_results):
      - exit_code 0 = all pass, 1 = at least one failure / error
      - model_results is a list of {model, cases} dicts
    """
    from utils.examples import DB_PATH, initialize_duckdb

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    cache_path = str(
        config_path.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json")
    )
    schema_cache = _load_schema_cache(cache_path)

    tests_root = config_path.parent / ".mocksql" / "tests"
    if not tests_root.exists():
        return 0, []

    # Collect model test files (skip old UUID-named session files)
    test_files = sorted(
        f for f in tests_root.rglob("*.json") if not _UUID_RE.match(f.stem)
    )
    if not test_files:
        return 0, []

    session_prefix = uuid.uuid4().hex[:8]
    model_results: list[dict] = []
    has_failures = False

    with initialize_duckdb(DB_PATH) as con:
        for test_file in test_files:
            rel = test_file.relative_to(tests_root).with_suffix("")
            model_name = rel.as_posix()

            if model_filters and model_name not in model_filters:
                continue

            test_doc = _read_json(test_file)
            if not test_doc:
                continue

            sql, sql_source = resolve_run_sql(
                cfg=cfg,
                config_path=config_path,
                model_name=model_name,
                snapshot_sql=test_doc.get("sql", ""),
                frozen=frozen,
            )
            used_columns_raw: list[str] = test_doc.get("used_columns") or []
            used_columns_parsed: list[dict] = []
            for raw in used_columns_raw:
                try:
                    used_columns_parsed.append(json.loads(raw))
                except Exception:
                    pass

            test_cases: list[dict] = test_doc.get("test_cases") or []
            case_results: list[dict] = []
            # Unique suffix per model to avoid table collisions between models. Stable
            # across all cases of the model → tables created once, SQL transpiled once.
            model_suffix = (
                f"{session_prefix}_{re.sub(r'[^a-z0-9]', '_', model_name.lower())}"
            )

            # Setup partagé : tables + SQL transpilé une seule fois pour tout le modèle.
            # `_resolve_model_schemas` peut lever `SchemaMissingError` (cache incomplet) :
            # on la capture ici pour la remonter en erreur par cas plutôt que planter le run.
            duckdb_schemas: list[dict] = []
            precompiled_sql = ""
            setup_error: str | None = None
            try:
                schemas = _resolve_model_schemas(
                    used_columns_raw, schema_cache, test_cases
                )
                duckdb_schemas, precompiled_sql = await _setup_model(
                    schemas=schemas,
                    sql=sql,
                    dialect=dialect,
                    suffix=model_suffix,
                    con=con,
                )
            except Exception as exc:
                setup_error = str(exc)

            for tc in test_cases:
                # Si le setup modèle a échoué, `_run_one_case` classe les cas vides en
                # skip et remonte `setup_error` sur les cas exécutables sans toucher DuckDB.
                result = await _run_one_case(
                    test_case=tc,
                    sql=sql,
                    duckdb_schemas=duckdb_schemas,
                    used_columns_parsed=used_columns_parsed,
                    dialect=dialect,
                    suffix=model_suffix,
                    con=con,
                    precompiled_sql=precompiled_sql,
                    setup_error=setup_error,
                )
                case_results.append(result)

                if result["status"] in ("fail", "error"):
                    has_failures = True
                    if fail_fast:
                        model_results.append(
                            {
                                "model": model_name,
                                "cases": case_results,
                                "sql_source": sql_source,
                            }
                        )
                        return 1, model_results

            model_results.append(
                {"model": model_name, "cases": case_results, "sql_source": sql_source}
            )

    return (1 if has_failures else 0), model_results
