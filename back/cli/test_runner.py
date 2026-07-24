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


def _expect_verdict(
    expect_check: dict,
    review_status: str | None,
    review_intent: str | None = None,
) -> str:
    """Verdict d'un cas depuis la comparaison de lignes au contrat ``expect`` (Phase 2).

    - lignes = contrat → ``pass`` ;
    - ``order_only_mismatch`` (mêmes lignes, ordre différent) → ``pass`` : c'est un ex-æquo
      sur la clé de tri, donc du **non-déterminisme**, pas une régression. Spec §8 : on
      FLAGGE (le CLI affiche l'avertissement), on ne bloque pas — la parade produit est de
      rendre les données discriminantes (axe ``tie``). Un vrai changement d'ordre
      déterministe passe forcément par une dérive SQL → ``stale``, pas par ce chemin ;
    - sinon, lignes ≠ contrat : ``fail`` si le contrat est ``confirmed`` (gate de
      non-régression), sinon ``unconfirmed`` (draft/stale/non confirmé — jamais bloquant,
      spec §3/§7).

    **Verrou repro (Phase 1)** : un cas marqué ``review.intent == "repro"`` mais encore
    non confirmé qui **PASSE** sur le SQL courant est *né vert* — ses données ne séparent
    pas le comportement bugué du désiré (cf. RAPPORT-repro-fitness §2), donc il ne
    reproduit PAS le bug qu'il prétend geler. On le refuse explicitement en
    ``repro_missing`` (échec, exit 1) au lieu de le laisser passer silencieusement. Une
    fois l'input rendu discriminant, le contrat ne passe plus → ``unconfirmed`` (rouge
    établi) et le verrou s'éteint ; une fois ``confirmed`` (après fix), l'intent est
    consommé et la sémantique normale reprend.
    """
    passed = expect_check["passed"] or expect_check.get("order_only_mismatch")
    if review_intent == "repro" and review_status != "confirmed" and passed:
        return "repro_missing"
    if passed:
        return "pass"
    if review_status == "confirmed":
        return "fail"
    return "unconfirmed"


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
    sql_drifted: bool = False,
    collect_rows: bool = False,
) -> dict:
    """Rejoue UN cas dans les tables déjà créées par modèle (cf. `_setup_model`).

    `collect_rows` : expose la sortie observée sous `observed_rows` (et rejoue même un
    cas sans expect ni assertions) — brique de `replay_case_rows` / replay-on-confirm.

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
    from build_query.expect_contract import compare_expect, rows_from_df
    from utils.examples import execute_queries, run_query_on_test_dataset
    from utils.insert_examples import insert_examples, replace_missing_with_null

    test_index = str(test_case.get("test_index", "0"))
    # Titre court (`test_name`, 3–6 mots) affiché en tête de chaque test ; la
    # `unit_test_description` (phrase complète) sert de sous-ligne descriptive.
    test_name = (test_case.get("test_name") or "").strip()
    description = (test_case.get("unit_test_description") or "").strip()
    name = test_name or description or f"Test {test_index}"
    meta = {
        "name": name,
        "description": description,
        "test_uid": test_case.get("test_uid"),
    }
    data: dict = test_case.get("data") or {}
    saved_assertions = [
        a for a in (test_case.get("assertion_results") or []) if a.get("sql")
    ]
    # Contrat `expect` (spec validation-humaine) : comparé en OMBRE des assertions
    # (Phase 0 — jamais bloquant tant que des assertions existent). Sans assertions,
    # le contrat devient le check et détermine le statut du cas.
    expect = (
        test_case.get("expect") if isinstance(test_case.get("expect"), dict) else None
    )
    review = test_case.get("review") or {}
    review_status = review.get("status") if isinstance(review, dict) else None
    # Intention du cas (verrou repro, Phase 1) : `repro` = ce cas DOIT naître rouge sur
    # le SQL courant (posée par `mocksql mark-repro`). Exposée dans le résultat pour le
    # gate CI `--require-red` et l'affichage.
    review_intent = review.get("intent") if isinstance(review, dict) else None
    # Dérive détectée au replay (SQL disque ≠ snapshot) : un contrat confirmé ne vaut
    # plus tel quel — rapporté `stale` SANS écrire (le replay est lecture seule ; la
    # bascule persistée se fait à l'écriture, cf. sync_expect_on_doc).
    if sql_drifted and review_status == "confirmed":
        review_status = "stale"
    meta["review"] = review_status
    meta["intent"] = review_intent

    if not data:
        return {
            "index": test_index,
            **meta,
            "status": "skip",
            "reason": "no data",
            "assertions": [],
        }
    if not saved_assertions and expect is None and not collect_rows:
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
        observed_extra = (
            {"observed_rows": rows_from_df(result_df)} if collect_rows else {}
        )

        if expect is not None:
            # Phase 2 (spec validation-humaine §7) : le contrat `expect` est AUTORITAIRE.
            # La comparaison de lignes (multiset | ordonnée, zéro LLM) fait le verdict —
            # les assertions ne sont plus rejouées (fin de `_remap_assertion_sql` pour ces
            # cas). Le statut de revue module le sens de l'échec :
            #   - confirmed → contrat gelé par un humain = gate de non-régression (fail).
            #   - draft / stale / non confirmé → jamais un échec bloquant : `unconfirmed`
            #     (rapporté avec diff, hors exit code par défaut ; gaté par
            #     --require-confirmed). Cf. spec §3 (« rejouable mais rapporté non
            #     confirmé, pas un échec »).
            expect_check = compare_expect(expect, rows_from_df(result_df))
            status = _expect_verdict(expect_check, review_status, review_intent)
            return {
                "index": test_index,
                **meta,
                "status": status,
                "assertions": [],
                "expect_check": expect_check,
                **observed_extra,
            }

        # Repli legacy : un cas SANS contrat `expect` (jamais migré) garde le chemin
        # assertions — remapping des noms de tables + évaluation dbt-style.
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
            **observed_extra,
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


# ── Replay d'un cas isolé (replay-on-confirm) ────────────────────────────────


async def replay_case_rows(
    config_path: Path, model_name: str, test_uid: str
) -> tuple[list[dict], str]:
    """Rejoue UN cas contre le SQL DISQUE et retourne (lignes observées, sql rejoué).

    Brique de `mocksql confirm` : dans la boucle agent, le `results_json` du cache
    date du generate — la sortie « actuellement observée » d'un cas est celle du
    replay disque, pas du cache. Déterministe, zéro LLM. Lève RuntimeError si le cas
    est introuvable ou ne produit pas de sortie exploitable.
    """
    from utils.examples import DB_PATH, initialize_duckdb

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    schema_cache = _load_schema_cache(
        str(config_path.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json"))
    )
    test_file = config_path.parent / ".mocksql" / "tests" / f"{model_name}.json"
    test_doc = _read_json(test_file) if test_file.exists() else None
    if not test_doc:
        raise RuntimeError(f"aucune suite de tests pour le modèle {model_name}")
    case = next(
        (c for c in test_doc.get("test_cases") or [] if c.get("test_uid") == test_uid),
        None,
    )
    if case is None:
        raise RuntimeError(f"test_uid {test_uid} introuvable dans {model_name}")

    sql, _source = resolve_run_sql(
        cfg=cfg,
        config_path=config_path,
        model_name=model_name,
        snapshot_sql=test_doc.get("sql", ""),
        frozen=False,
    )
    used_columns_raw: list[str] = test_doc.get("used_columns") or []
    used_columns_parsed: list[dict] = []
    for raw in used_columns_raw:
        try:
            used_columns_parsed.append(json.loads(raw))
        except Exception:
            pass
    suffix = f"{uuid.uuid4().hex[:8]}_confirm"
    with initialize_duckdb(DB_PATH) as con:
        # `SchemaMissingError` (table absente du cache, fréquent sur clone frais) porte
        # déjà un message actionnable `refresh-schemas` : on le convertit en RuntimeError
        # pour que le call-site (`run_confirm`, qui n'attrape que RuntimeError) l'affiche
        # proprement au lieu de laisser fuir une traceback.
        try:
            schemas = _resolve_model_schemas(used_columns_raw, schema_cache, [case])
        except SchemaMissingError as exc:
            raise RuntimeError(str(exc)) from exc
        duckdb_schemas, precompiled_sql = await _setup_model(
            schemas=schemas, sql=sql, dialect=dialect, suffix=suffix, con=con
        )
        result = await _run_one_case(
            test_case=case,
            sql=sql,
            duckdb_schemas=duckdb_schemas,
            used_columns_parsed=used_columns_parsed,
            dialect=dialect,
            suffix=suffix,
            con=con,
            precompiled_sql=precompiled_sql,
            collect_rows=True,
        )
    if result.get("status") == "error":
        raise RuntimeError(result.get("error") or "replay en erreur")
    if "observed_rows" not in result:
        raise RuntimeError(
            f"cas non exécutable ({result.get('reason') or result.get('status')})"
        )
    return result["observed_rows"], sql


# ── inspect : diagnostic déterministe d'un cas rouge (boucle TDD agent) ───────

_OBSERVED_ROWS_CAP = 20


def _cte_trace_to_list(cte_trace: dict) -> list[dict]:
    """Transforme le ``cte_trace`` de l'executor (dict ordonné ``{name: {...}}``) en LISTE
    ordonnée ``[{name, row_count, ...}]``.

    L'ordre du pipeline est signifiant (la 1ʳᵉ CTE requise vide = suspect n°1) ; une
    liste l'expose explicitement au parseur du skill. ``join_breakdown`` (décomposition
    par prédicat de la CTE bloquante) est renommé ``blocking_predicates`` — la lentille
    *pourquoi vide*.
    """
    out: list[dict] = []
    for name, info in cte_trace.items():
        if not isinstance(info, dict):
            continue
        entry: dict = {"name": name, "row_count": info.get("row_count")}
        if "blocking" in info:
            entry["blocking"] = info["blocking"]
        for key in ("sample", "steps", "error"):
            if info.get(key) is not None:
                entry[key] = info[key]
        if info.get("join_breakdown"):
            entry["blocking_predicates"] = info["join_breakdown"]
        out.append(entry)
    return out


def _build_diagnosis(
    sql_source: str,
    status: str,
    cte_trace_list: list[dict],
    join_probes: list[dict],
    expect_check: dict | None,
) -> dict:
    """Cause probable résumée en un ``code`` déterministe, par priorité (première règle qui
    matche gagne — cf. docs/inspect-diagnostic.md, table ``diagnosis.code``)."""
    if sql_source == "snapshot-fallback":
        return {
            "code": "sql_source_fallback",
            "suspect": None,
            "detail": (
                "Le `.sql` source n'a pas été lu — trace fondée sur le snapshot figé. "
                "Un résultat vert refléterait l'ANCIEN snapshot, pas ton édition."
            ),
        }
    if status == "error":
        return {
            "code": "error",
            "suspect": None,
            "detail": "Le rejeu a levé une erreur.",
        }
    if expect_check is not None and expect_check.get("passed"):
        return {
            "code": "consistent",
            "suspect": None,
            "detail": "La sortie observée satisfait le contrat `expect` (cas vert).",
        }
    blocking_empty = next(
        (c for c in cte_trace_list if c.get("row_count") == 0 and c.get("blocking")),
        None,
    )
    if blocking_empty:
        return {
            "code": "empty_upstream_cte",
            "suspect": blocking_empty["name"],
            "detail": (
                f"La CTE requise `{blocking_empty['name']}` produit 0 ligne "
                "(suspect n°1)."
            ),
        }
    # L'ORACLE d'abord : quand le contrat `expect` a quelque chose à dire (diff de lignes),
    # il MÈNE. Une sonde de cardinalité de JOIN n'énonce qu'un FAIT (`fan_out`/`shrinks`)
    # sans savoir s'il est voulu — un LEFT un-à-plusieurs fan-out TOUJOURS. La laisser
    # primer sur le diff épinglait des JOINs sains comme cause racine et enterrait le vrai
    # écart de valeur. Le diff `expect`, lui, est ancré sur l'attendu du test.
    if expect_check is not None and expect_check.get("order_only_mismatch"):
        return {
            "code": "nondeterministic_order",
            "suspect": None,
            "detail": (
                "Mêmes lignes, ordre différent — ex-æquo sur la clé de tri "
                "(sortie non-déterministe)."
            ),
        }
    if expect_check is not None and not expect_check.get("passed"):
        return {
            "code": "expect_diff",
            "suspect": None,
            "detail": (
                "La sortie diverge du contrat `expect` (voir `expect_check`). Les sondes "
                "`join_probes` sont fournies comme ÉVIDENCE, pas comme cause épinglée."
            ),
        }
    # Faute d'oracle (`expect` absent) : les faits structurels de cardinalité sont le seul
    # signal — on les remonte alors, mais comme DESCRIPTION à confirmer, pas comme verdict.
    fan = next((p for p in join_probes if p.get("verdict") == "fan_out"), None)
    if fan:
        sid = f"{fan['cte']}#{fan['join_index']}"
        return {
            "code": "join_fan_out",
            "suspect": sid,
            "detail": (
                f"Sans contrat `expect` pour ancrer le diagnostic : le JOIN {sid} multiplie "
                f"les lignes ({fan['left_rows']} → {fan['result_rows']}). Fait, pas verdict "
                "— à confirmer contre l'intention du test."
            ),
        }
    shrink = next((p for p in join_probes if p.get("verdict") == "shrinks"), None)
    if shrink:
        sid = f"{shrink['cte']}#{shrink['join_index']}"
        return {
            "code": "join_shrinks",
            "suspect": sid,
            "detail": (
                f"Sans contrat `expect` pour ancrer le diagnostic : le JOIN {sid} réduit "
                f"les lignes ({shrink['left_rows']} → {shrink['result_rows']}). Fait, pas "
                "verdict — à confirmer contre l'intention du test."
            ),
        }
    return {
        "code": "consistent",
        "suspect": None,
        "detail": "Aucune anomalie structurelle détectée.",
    }


async def _inspect_llm_verdict(payload: dict) -> str:
    """Verdict LLM **opt-in** (``--llm``) : une à deux phrases de cause racine à partir du
    diagnostic DÉTERMINISTE déjà assemblé (trace CTE + sondes join + diff).

    Jamais le défaut : ``inspect`` existe pour donner un signal déterministe et gratuit.
    Aucune donnée réelle ni appel warehouse — le LLM ne voit que des comptes et des
    lignes synthétiques. Défensif : renvoie un message d'erreur plutôt que de planter le
    diagnostic si le LLM est indisponible.
    """
    try:
        from storage.config import output_language_directive
        from utils.llm_errors import normalize_llm_content
        from utils.llm_factory import make_llm

        diag = {
            k: payload.get(k)
            for k in (
                "diagnosis",
                "expect_check",
                "cte_trace",
                "join_probes",
                "observed",
                "sql_source",
            )
        }
        prompt = (
            output_language_directive()
            + "\nTu es un assistant de debug SQL. Voici le diagnostic DÉTERMINISTE "
            "(trace CTE, sondes de cardinalité de JOIN, diff de lignes) d'un test qui "
            "échoue. En une à deux phrases, nomme la cause racine la plus probable et "
            "l'action de correction. Ne réinvente pas les chiffres.\n\n"
            + json.dumps(diag, ensure_ascii=False, default=str, indent=2)
        )
        llm = make_llm()
        resp = await llm.ainvoke(prompt)
        return normalize_llm_content(resp.content).strip()
    except Exception as exc:
        return f"[verdict LLM indisponible : {exc}]"


async def inspect_case(
    config_path: Path,
    model_name: str,
    test_uid: str,
    *,
    llm: bool = False,
) -> dict:
    """Diagnostic déterministe (zéro LLM par défaut) d'un cas rejoué contre le SQL DISQUE.

    Trois lentilles (cf. docs/inspect-diagnostic.md) : (1) rejeu + ``sql_source`` (dont le
    repli ``snapshot-fallback`` = garde-fou F4) + diff ``expect_check`` ; (2) trace CTE par
    CTE (1ʳᵉ CTE requise vide = suspect n°1) ; (3) sondes de cardinalité join par join
    (sur-production vs perte de lignes). ``diagnosis.code`` résume la cause.

    ``llm=True`` (opt-in) ajoute un ``llm_verdict`` ; sinon il reste ``None`` (aucun appel
    LLM). Lève ``RuntimeError`` si le modèle ou le ``test_uid`` est introuvable.
    """
    from build_query.examples_executor import (
        _run_cte_trace,
        _run_join_count_probes,
        _run_join_predicate_breakdown,
        _run_scalar_filter_breakdown,
        _select_failing_cte,
    )
    from build_query.query_chain import _lightweight_query_decomposed
    from utils.examples import DB_PATH, initialize_duckdb

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    schema_cache = _load_schema_cache(
        str(config_path.parent / cfg.get("schema_cache", ".mocksql/schema_cache.json"))
    )
    test_file = config_path.parent / ".mocksql" / "tests" / f"{model_name}.json"
    test_doc = _read_json(test_file) if test_file.exists() else None
    if not test_doc:
        raise RuntimeError(f"aucune suite de tests pour le modèle {model_name}")
    case = next(
        (c for c in test_doc.get("test_cases") or [] if c.get("test_uid") == test_uid),
        None,
    )
    if case is None:
        raise RuntimeError(f"test_uid {test_uid} introuvable dans {model_name}")

    sql, sql_source = resolve_run_sql(
        cfg=cfg,
        config_path=config_path,
        model_name=model_name,
        snapshot_sql=test_doc.get("sql", ""),
        frozen=False,
    )
    # Dérive : le SQL disque diffère du snapshot → un contrat confirmé est rapporté
    # `stale`/`unconfirmed`, pas `fail` (même sémantique que `run_tests`). Inspect tourne
    # justement APRÈS une édition du `.sql` : sans ça, tout modèle confirmé édité serait
    # mislabellé `fail`.
    sql_drifted = (
        sql_source == "disk" and sql.strip() != (test_doc.get("sql") or "").strip()
    )
    used_columns_raw: list[str] = test_doc.get("used_columns") or []
    used_columns_parsed: list[dict] = []
    for raw in used_columns_raw:
        try:
            used_columns_parsed.append(json.loads(raw))
        except Exception:
            pass

    suffix = f"{uuid.uuid4().hex[:8]}_inspect"
    project = "cli"
    cte_trace: dict = {}
    join_probes: list = []
    with initialize_duckdb(DB_PATH) as con:
        # Schéma manquant = message actionnable `refresh-schemas`, pas une traceback :
        # `inspect_cmd` n'attrape que RuntimeError, on convertit donc à la source.
        try:
            schemas = _resolve_model_schemas(used_columns_raw, schema_cache, [case])
        except SchemaMissingError as exc:
            raise RuntimeError(str(exc)) from exc
        duckdb_schemas, precompiled_sql = await _setup_model(
            schemas=schemas, sql=sql, dialect=dialect, suffix=suffix, con=con
        )
        # Rejeu du cas : status + expect_check + observed_rows. `_run_one_case` laisse les
        # données du cas chargées dans `con` (delete+insert sans cleanup final) → les
        # sondes ci-dessous tournent sur les MÊMES tables, sans réinsertion.
        result = await _run_one_case(
            test_case=case,
            sql=sql,
            duckdb_schemas=duckdb_schemas,
            used_columns_parsed=used_columns_parsed,
            dialect=dialect,
            suffix=suffix,
            con=con,
            precompiled_sql=precompiled_sql,
            sql_drifted=sql_drifted,
            collect_rows=True,
        )
        if result.get("status") != "error":
            ctes = json.loads(_lightweight_query_decomposed(sql, dialect) or "[]")
            try:
                cte_trace = await _run_cte_trace(ctes, suffix, project, dialect, con)
                failing_cte = _select_failing_cte(ctes, cte_trace, dialect)
                if failing_cte and cte_trace.get(failing_cte, {}).get("row_count") == 0:
                    failing_idx = next(
                        (i for i, c in enumerate(ctes) if c["name"] == failing_cte),
                        None,
                    )
                    if failing_idx is not None:
                        breakdown: list = []
                        try:
                            breakdown += await _run_join_predicate_breakdown(
                                ctes, failing_idx, suffix, project, dialect, con
                            )
                            breakdown += await _run_scalar_filter_breakdown(
                                ctes, failing_idx, suffix, project, dialect, con
                            )
                        except Exception:
                            pass
                        if breakdown:
                            cte_trace[failing_cte]["join_breakdown"] = breakdown
            except Exception:
                cte_trace = {}
            try:
                join_probes = await _run_join_count_probes(
                    ctes, suffix, project, dialect, con
                )
            except Exception:
                join_probes = []

    expect_check = result.get("expect_check")
    observed_rows = result.get("observed_rows") or []
    cte_trace_list = _cte_trace_to_list(cte_trace)
    diagnosis = _build_diagnosis(
        sql_source,
        result.get("status", ""),
        cte_trace_list,
        join_probes,
        expect_check,
    )

    sql_warning = None
    if sql_source == "snapshot-fallback":
        sql_warning = (
            f"source .sql introuvable pour `{model_name}` — rejoué sur le snapshot "
            "figé du JSON ; un résultat vert peut refléter l'ancien SQL, pas ton édition."
        )

    payload: dict = {
        "model": model_name,
        "test_uid": test_uid,
        "test_name": case.get("test_name"),
        "description": (case.get("unit_test_description") or "").strip() or None,
        "sql_source": sql_source,
        "sql_source_warning": sql_warning,
        "status": result.get("status"),
        "review": result.get("review"),
        "diagnosis": diagnosis,
        "expect_check": expect_check,
        "observed": {
            "row_count": len(observed_rows),
            "truncated": len(observed_rows) > _OBSERVED_ROWS_CAP,
            "rows": observed_rows[:_OBSERVED_ROWS_CAP],
        },
        "cte_trace": cte_trace_list,
        "join_probes": join_probes,
        "llm_verdict": None,
    }
    if result.get("status") == "error":
        payload["error"] = result.get("error")
    if llm:
        payload["llm_verdict"] = await _inspect_llm_verdict(payload)
    return payload


# ── inspect --live : waterfall de cardinalité sur l'entrepôt RÉEL (gaté) ──────


def _scalar(rows: list, key: str) -> int:
    """Lit un scalaire de comptage d'une ligne (``[{"n": 90}]``), robuste à la casse
    des clés (Snowflake DictCursor → MAJUSCULES). Repli sur la 1ʳᵉ colonne."""
    if not rows:
        return 0
    row = rows[0]
    if isinstance(row, dict):
        for k in (key, key.upper(), key.lower()):
            if k in row:
                v = row[k]
                return int(v) if v is not None else 0
        vals = list(row.values())
        return int(vals[0]) if vals and vals[0] is not None else 0
    # tuple/list row (curseur non-Dict)
    try:
        return int(row[0])
    except (TypeError, ValueError, IndexError):
        return 0


async def inspect_live(
    config_path: Path,
    model_name: str,
    *,
    full: bool = False,
    auto_approve: bool = False,
    prompt_fn=None,
    warehouse_executor=None,
) -> dict:
    """Waterfall de cardinalité join-par-join sur l'ENTREPÔT RÉEL (BQ/SF), gaté.

    Décompose le SQL disque en préfixes (``build_live_probes``) et tire, par frontière,
    ``COUNT(*)`` [+ ``COUNT(DISTINCT clé_droite)``] sur les VRAIES tables — révèle où le
    fan-out se produit en prod (« orders filtrés = 90 → ⋈ order_items = 340 ×3,8 »). Chaque
    tir passe par ``warehouse_gate`` (estimé + confirmé UNE fois pour tout le run). Tiered :
    ``full=False`` sonde la dernière CTE jointe (cheap), ``full=True`` le waterfall complet.

    ``warehouse_executor`` (fn ``sql -> list[dict]``) est injectable pour les tests ;
    par défaut le connecteur parité réel. Lève ``WarehouseQueryDenied`` sur refus (aucune
    requête facturée émise après le refus), ``RuntimeError`` si le SQL est introuvable.
    """
    import os

    from build_query.live_probes import build_live_probes, classify_waterfall
    from build_query.query_chain import _lightweight_query_decomposed
    from build_query.warehouse_gate import GatedExecutor

    cfg = _load_config(config_path)
    dialect: str = cfg.get("dialect", "bigquery")
    sql, sql_source = resolve_run_sql(
        cfg=cfg,
        config_path=config_path,
        model_name=model_name,
        snapshot_sql="",
        frozen=False,
    )
    if not sql or not sql.strip():
        raise RuntimeError(f"SQL introuvable pour le modèle `{model_name}`")

    ctes = json.loads(_lightweight_query_decomposed(sql, dialect) or "[]")
    targets = build_live_probes(ctes, dialect, full=full)
    base = {
        "model": model_name,
        "dialect": dialect,
        "sql_source": sql_source,
        "full": full,
    }
    if not targets:
        return {**base, "live_waterfall": [], "note": "aucune jointure à sonder"}

    if warehouse_executor is None:
        from cli.parity import _execute_on_warehouse

        def warehouse_executor(q: str, _d=dialect):  # noqa: ANN001
            return _execute_on_warehouse(q, _d)

    billing_project = os.getenv("BQ_TEST_PROJECT") or os.getenv("VERTEX_PROJECT")
    gated = GatedExecutor(
        warehouse_executor,
        dialect,
        billing_project=billing_project,
        context=f"inspect --live · {model_name}",
        auto_approve=auto_approve,
        prompt_fn=prompt_fn,
    )

    waterfall: list = []
    for target in targets:
        annotated: list = []
        for probe in target["probes"]:
            resolved = dict(probe)
            res = gated(probe["count_sql"])
            resolved["rows"] = _scalar(res, "n")
            # Pré-agrégat : une seule requête mesure n ET d (COUNT vs COUNT DISTINCT).
            if probe.get("boundary") == "pre_agg":
                resolved["distinct_rows"] = _scalar(res, "d")
            if probe.get("right_distinct_sql"):
                rd = gated(probe["right_distinct_sql"])
                resolved["right_rows"] = _scalar(rd, "n")
                resolved["right_distinct"] = _scalar(rd, "d")
            # Le SQL est déjà porté par le probe ; on ne le ré-expose pas (bruit).
            resolved.pop("count_sql", None)
            resolved.pop("right_distinct_sql", None)
            annotated.append(resolved)
        waterfall.append(
            {"cte": target["cte"], "probes": classify_waterfall(annotated)}
        )

    return {**base, "live_waterfall": waterfall}


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
    from cli.parity import compute_fingerprint, parity_state
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
            # Dérive : le SQL rejoué (disque) diffère du snapshot stocké → les contrats
            # confirmés seront rapportés `stale` (re-confirmation attendue, cf. spec).
            sql_drifted = (
                sql_source == "disk"
                and sql.strip() != (test_doc.get("sql") or "").strip()
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
                    sql_drifted=sql_drifted,
                )
                # Attestation de parité warehouse (informatif, jamais bloquant) :
                # verified / stale (empreinte périmée) / unverified (jamais audité).
                result["parity"] = parity_state(
                    tc, compute_fingerprint(sql, tc.get("data"), dialect), dialect
                )
                case_results.append(result)

                # `repro_missing` (verrou Phase 1) = échec bloquant par défaut : un test
                # de repro né vert ne garde rien. Au même titre que fail/error → exit 1.
                if result["status"] in ("fail", "error", "repro_missing"):
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
