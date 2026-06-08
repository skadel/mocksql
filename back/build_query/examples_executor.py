import asyncio
import json
import logging
import re
import uuid
from typing import List, Dict, Any, Literal, Optional

import sqlglot
from langchain_core.messages import AIMessage
from pandas import DataFrame
from pydantic import BaseModel, Field, model_validator
from sqlglot import exp

from utils.llm_errors import normalize_llm_content
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from build_query.state import QueryState
from utils.examples import (
    run_query_on_test_dataset,
    create_test_tables,
    execute_queries,
    initialize_duckdb,
    DB_PATH,
)
from utils.insert_examples import replace_missing_with_null, insert_examples
from storage.test_repository import get_test
from utils.saver import examples_state_retriever


import utils.logger  # noqa: F401 — registers DIAG level (15)

logger = logging.getLogger(__name__)


class _Assertion(BaseModel):
    description: str = Field(
        description=(
            "Phrase courte (max 15 mots) décrivant l'assertion en termes métier, "
            "lisible par un responsable non-développeur. "
            "✓ Bon : 'Le montant total est toujours positif.' "
            "'Chaque commande appartient à un client actif.' "
            "✗ À proscrire : 'price > 0 pour toutes les lignes de __result__', "
            "'COALESCE(amount, 0) != NULL dans la CTE finale'."
        )
    )
    sql: str


class _AssertionFix(BaseModel):
    test_name: str
    unit_test_description: str
    unit_test_build_reasoning: str
    tags: List[str]
    suggestions: List[str]


class DiagnosticBlock(BaseModel):
    root_cause: str
    sql_pattern: str
    data_issue: str
    fix_summary: str
    fix_recipe: str
    affected_tables: List[str]
    affected_ctes: List[str]


class _AssertionsAndEvaluation(BaseModel):
    reasoning: str  # chain-of-thought: intention du test, cohérence données/résultat, qualité des assertions
    assertions: List[_Assertion] = Field(min_length=1)
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    reason_type: Optional[Literal["bad_data", "bad_assertions"]] = None
    explanation: str
    assertion_fix: Optional[_AssertionFix] = None
    diagnostic: Optional[DiagnosticBlock] = None

    @model_validator(mode="after")
    def _diagnostic_required_for_bad_data(self) -> "_AssertionsAndEvaluation":
        if self.reason_type == "bad_data" and self.diagnostic is None:
            self.diagnostic = DiagnosticBlock(
                root_cause="Données d'entrée insuffisantes ou incohérentes avec la logique SQL",
                sql_pattern="(non déterminé automatiquement)",
                data_issue="Le LLM n'a pas fourni d'analyse détaillée",
                fix_summary="Régénérer les données en ciblant la contrainte SQL du test.",
                fix_recipe="Régénérer les données en ciblant la contrainte SQL identifiée dans le reasoning",
                affected_tables=[],
                affected_ctes=[],
            )
        return self


def _load_existing_tests(session_id: str) -> List[Dict[str, Any]]:
    """Load the persisted test suite from the test file."""
    test = get_test(session_id)
    if test:
        return test.get("test_cases", [])
    return []


async def run_on_examples(state: "QueryState") -> Dict[str, Any]:
    """
    Exécute les unit tests sur les données générées et renvoie les résultats.
    """
    if state.get("error"):
        return {}

    rerun_all = state.get("rerun_all_tests", False)

    # Contexte commun
    session_id_duckdb = state["session"].replace("-", "_")
    dialect = state["dialect"]
    from models.schemas import get_schemas

    schemas = await get_schemas(project_id=state["project"])
    used_columns = [json.loads(c) for c in state.get("used_columns") or []]

    logger.debug(
        "\n[DEBUG] >>> run_on_examples : used_columns bruts récupérés depuis le state:"
    )
    for uc in used_columns:
        logger.debug(f"      - {uc}")

    filtered_schemas = filter_schemas_by_used_columns(schemas, used_columns)

    # Détermination de la liste de tests à exécuter
    if rerun_all:
        # Charger tous les tests existants depuis la DB
        existing_tests = _load_existing_tests(state["session"])
        # Ajouter/remplacer avec le nouveau test du générateur (s'il y en a un)
        examples_msgs = examples_state_retriever(state)
        if examples_msgs:
            new_test = json.loads(examples_msgs[-1].content)
            if isinstance(new_test, dict):
                merged = {t["test_index"]: t for t in existing_tests}
                merged[new_test["test_index"]] = new_test
                unit_tests = sorted(merged.values(), key=lambda x: int(x["test_index"]))
            else:
                unit_tests = existing_tests
        else:
            unit_tests = existing_tests
    else:
        unit_tests = _parse_unit_tests_from_state(state)
        if unit_tests is None:
            # Le générateur n'a pas produit de nouveau test : ré-exécuter les tests existants
            unit_tests = _load_existing_tests(state["session"])

    if not unit_tests:
        return {}

    # Exécution des tests
    all_tests_results: List[Dict[str, Any]] = []
    with initialize_duckdb(DB_PATH) as con:
        for loop_index, test_case in enumerate(unit_tests):
            logger.debug(
                f"\n[DEBUG] >>> Lancement test {loop_index} avec table(s) : {list(test_case.get('data', {}).keys())}"
            )
            test_result = await _run_single_test_case(
                state=state,
                test_case=test_case,
                loop_index=loop_index,
                session_id=session_id_duckdb,
                query=state.get("optimized_sql"),
                schemas=filtered_schemas,
                used_columns=used_columns,
                con=con,
                dialect=dialect,
                rerun_all=rerun_all,
            )
            all_tests_results.append(test_result)

    global_status = _determine_global_status(all_tests_results)
    content_msg = json.dumps(all_tests_results, indent=2)
    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 1
    )

    sql = state.get("query", "").strip()
    optimized_sql = state.get("optimized_sql", "").strip()
    examples_msgs = examples_state_retriever(state)
    generated_test_index = (
        examples_msgs[-1].additional_kwargs.get("generated_test_index")
        if examples_msgs
        else None
    )
    results_kwargs = {
        "type": MsgType.RESULTS,
        "parent": (
            state.get("user_message_id") if state.get("input", "").strip() else None
        )
        or state.get("parent_message_id")
        or (state["messages"][-1].id if state.get("messages") else None),
        "request_id": state.get("request_id"),
        **({"sql": sql} if sql else {}),
        **({"optimized_sql": optimized_sql} if optimized_sql else {}),
        **(
            {"generated_test_index": generated_test_index}
            if generated_test_index is not None
            else {}
        ),
        **({"rerun_all": True} if rerun_all else {}),
    }

    return {
        "messages": [
            AIMessage(
                content=content_msg,
                id=str(uuid.uuid4()),
                additional_kwargs=results_kwargs,
            )
        ],
        "status": global_status,
        "gen_retries": gen_retries,
    }


def filter_schemas_by_used_columns(
    schemas: List[dict], used_columns_info: List[dict]
) -> List[dict]:
    """
    Ne garde dans 'schemas' que les tables et colonnes réellement utilisées,
    selon la structure de 'used_columns_info'.

    used_columns_info ressemble à :
    [
      {
        "table": "REF_MODELE_MATERIEL",
        "used_columns": [
          "dt_creation_modele_materiel",
          "id_modele_materiel",
          ...
        ]
      },
      ...
    ]
    """
    # 1. Construire un dictionnaire { "NomTable" -> [colonne1, colonne2, ...] }
    used_cols_dict = {
        f"{item['database']}.{item['table']}"
        if item.get("database")
        else item["table"]: [col.lower() for col in item["used_columns"]]
        for item in used_columns_info
    }

    logger.debug(
        "\n[DEBUG] >>> filter_schemas_by_used_columns : used_cols_dict généré:"
    )
    logger.debug(f"      - {used_cols_dict}")

    filtered_schemas = []
    for table_schema in schemas:
        parts = table_schema["table_name"].split(".")
        qualified = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]

        if qualified in used_cols_dict:
            wanted_cols = used_cols_dict[qualified]
            logger.debug(
                f"\n[DEBUG] >>> Filtrage de la table {qualified}. wanted_cols: {wanted_cols}"
            )

            filtered_columns = [
                col
                for col in table_schema["columns"]
                if col["name"].lower() in wanted_cols
                or any(col["name"].lower().startswith(f"{w}.") for w in wanted_cols)
            ]

            logger.debug(
                f"[DEBUG] >>> Table {qualified} - Colonnes conservées: {[c['name'] for c in filtered_columns]}"
            )

            if filtered_columns:
                filtered_schemas.append(
                    {
                        "table_name": table_schema["table_name"],
                        "description": table_schema.get("description", ""),
                        "columns": filtered_columns,
                        "primary_keys": table_schema.get("primary_keys", []),
                    }
                )

    return filtered_schemas


def _parse_unit_tests_from_state(state: QueryState) -> Optional[List[Dict[str, Any]]]:
    """
    Récupère la liste de unit tests depuis l'état.
    Priorité : user_tables > EXAMPLES en mémoire.
    Retourne None si aucun test n'est disponible en mémoire (signal : charger depuis la DB).
    """
    if state["user_tables"] and state["user_tables"] != "":
        unit_tests = json.loads(state["user_tables"])
        if isinstance(unit_tests, dict):
            unit_tests = [unit_tests]
        return unit_tests

    examples_msgs = examples_state_retriever(state)
    if not examples_msgs:
        return None  # Aucun test en mémoire : l'appelant chargera depuis le fichier

    test = json.loads(examples_msgs[-1].content)
    if isinstance(test, dict):
        return [test]
    if isinstance(test, list):
        return test
    return None


def _extract_columns(expr: exp.Expression) -> List[exp.Expression]:
    """
    Trouve toutes les colonnes (exp.Column) dans l'expression fournie
    et les retourne en tant qu'expressions prêtes à être mises dans un SELECT.
    """
    return list(expr.find_all(exp.Column))


def _decompose_cte_in_steps(cte_sql_code: str, dialect: str) -> List[Dict[str, str]]:
    """
    Décompose le code SQL d'une CTE (ou requête) en plusieurs étapes, avec :
      - 1 étape par condition si un JOIN comporte un ON avec plusieurs conditions (via AND).
      - Par défaut, on force désormais chaque JOIN en FULL JOIN sauf si la jointure est latérale (UDTF).
      - Au lieu de COUNT(0), on affiche toutes les colonnes détectées dans la clause ON.
    On retourne une liste de dicts: [{"name": "...", "code": "..."}].
    """
    steps = []
    parsed = sqlglot.parse_one(cte_sql_code, read=dialect)

    # Récupération des parties importantes
    from_expr = parsed.args.get("from_")  # exp.From
    joins_expr = parsed.args.get("joins") or []
    where_expr = parsed.args.get("where")

    def build_query(select_list, from_part, joins_part=None, where_part=None):
        """
        Construit une requête SELECT complète à partir des différents blocs
        (SELECT, FROM, JOIN, WHERE) puis retourne son code SQL en dialecte spécifié.
        """
        query_exp = exp.Select()

        # SELECT
        if select_list:
            query_exp.set("expressions", select_list)
        else:
            # fallback si besoin
            query_exp.set(
                "expressions",
                [exp.Star()],  # ou exp.Count(this=exp.Literal.number(0)) au choix
            )

        # FROM
        if from_part is not None:
            query_exp.set("from", from_part)

        # JOINS
        if joins_part:
            query_exp.set("joins", joins_part)

        # WHERE
        if where_part:
            query_exp.set("where", where_part)

        return query_exp.sql(dialect=dialect)

    # On stocke la table de départ
    tables = []
    if from_expr:
        tables.append(from_expr)

    # -------------------------------------------------------------------------
    # Parcours de chaque JOIN pour générer des étapes
    # -------------------------------------------------------------------------
    join_steps = []
    from sqlglot.expressions import UDTF  # Pour identifier les UDTF (ex: UNNEST)

    for j_idx, join_expr in enumerate(joins_expr, start=1):
        # Copie pour ne pas altérer l'original
        join_copy = join_expr.copy()

        # Si la jointure n'est pas une UDTF (donc pas latérale implicite), forcer le FULL JOIN
        if not isinstance(join_copy.this, UDTF):
            join_copy.set("side", "FULL")
            join_copy.set("kind", None)
        # Sinon, on laisse la jointure en l'état

        # Récupérer la clause ON, s’il y en a une, pour déterminer les colonnes
        on_clause = join_copy.args.get("on")
        if on_clause:
            # Décomposition via AND
            conditions = _extract_conditions(on_clause)
            if len(conditions) > 1:
                # On génère une requête par condition
                for c_idx, cond in enumerate(conditions, start=1):
                    single_join_expr = join_copy.copy()
                    # On remplace la clause ON par une seule condition
                    single_join_expr.set("on", cond)

                    # Récupération de toutes les colonnes présentes dans la condition
                    columns_in_cond = _extract_columns(cond)
                    # fallback si aucune colonne détectée
                    if not columns_in_cond:
                        columns_in_cond = [exp.Star()]

                    step_sql = build_query(
                        select_list=columns_in_cond,
                        from_part=tables[0],
                        joins_part=(tables[1:] if len(tables) > 1 else [])
                        + [single_join_expr],
                    )
                    join_steps.append(
                        {"name": f"step_join_{j_idx}_cond_{c_idx}", "code": step_sql}
                    )
            else:
                # Une seule condition => un seul step
                cond = conditions[0] if conditions else None
                columns_in_cond = _extract_columns(cond) if cond else []
                if not columns_in_cond:
                    columns_in_cond = [exp.Star()]

                step_sql = build_query(
                    select_list=columns_in_cond,
                    from_part=tables[0],
                    joins_part=(tables[1:] if len(tables) > 1 else []) + [join_copy],
                )
                join_steps.append({"name": f"step_join_{j_idx}", "code": step_sql})
        else:
            # JOIN sans clause ON => un step unique
            step_sql = build_query(
                select_list=[exp.Star()],
                from_part=tables[0],
                joins_part=(tables[1:] if len(tables) > 1 else []) + [join_copy],
            )
            join_steps.append({"name": f"step_join_{j_idx}", "code": step_sql})

        # On ajoute ce join à la liste "tables" pour construire la suite
        tables.append(join_expr)

    # On ajoute tous les steps de joins
    steps.extend(join_steps)

    # -------------------------------------------------------------------------
    # Gérer la clause WHERE (exemple : un step "avant WHERE" et un step COUNTIF si on veut)
    # -------------------------------------------------------------------------
    if where_expr:
        # Étape "avant WHERE"
        step_sql_before_where = build_query(
            select_list=[exp.Star()],
            from_part=tables[0],
            joins_part=tables[1:] if len(tables) > 1 else None,
        )
        steps.append({"name": "step_before_where", "code": step_sql_before_where})

        # Étape "COUNTIF par condition de WHERE"
        countif_expressions = _build_countif_expressions(where_expr)
        step_sql_where = build_query(
            select_list=countif_expressions,
            from_part=tables[0],
            joins_part=tables[1:] if len(tables) > 1 else None,
            where_part=None,  # On retire la clause WHERE pour ne faire que le COUNTIF
        )
        steps.append({"name": "step_where", "code": step_sql_where})

    # -------------------------------------------------------------------------
    # Étape finale : la requête complète telle qu’elle était
    # -------------------------------------------------------------------------
    full_sql = parsed.sql(dialect=dialect)
    steps.append({"name": "", "code": full_sql})

    return steps


def _extract_conditions(expr: exp.Expression) -> List[exp.Expression]:
    """
    Extrait récursivement toutes les conditions d'une expression en décomposant
    les noeuds And. Si l'expression n'est pas un And, elle est retournée seule.
    Les doublons (même SQL généré) sont supprimés en conservant l'ordre.
    """

    def _recurse(e: exp.Expression) -> List[exp.Expression]:
        if isinstance(e, exp.And):
            return _recurse(e.this) + _recurse(e.expression)
        return [e]

    seen: dict[str, bool] = {}
    result = []
    for cond in _recurse(expr):
        key = cond.sql()
        if key not in seen:
            seen[key] = True
            result.append(cond)
    return result


def _build_countif_expressions(where_expr: exp.Expression) -> List[exp.Expression]:
    """
    Construit une liste de COUNTIF(...) à partir des conditions extraites de l'expression WHERE.

    Par exemple, pour un WHERE équivalent à "col1 > 10 AND col2 = 'ABC'",
    on génère :
       [COUNTIF(col1 > 10) AS count_cond1, COUNTIF(col2 = 'ABC') AS count_cond2]

    Pour des clauses plus complexes (avec des OR ou des parenthèses imbriquées),
    il faudra éventuellement affiner la logique.
    """
    # Extraction des conditions à partir de l'expression (souvent where_expr correspond à parsed.args.get("where").this)
    conditions = _extract_conditions(where_expr.this)

    countif_list = []
    for idx, cond in enumerate(conditions, start=1):
        # On crée un noeud COUNTIF enveloppé dans un alias
        countif_node = exp.Alias(
            this=exp.CountIf(this=cond), alias=exp.Identifier(this=f"count_cond{idx}")
        )
        countif_list.append(countif_node)

    return countif_list


def _build_cte_sql_with_suffix(
    sql_code: str, last_query_decomposed: List[Dict[str, Any]], suffix: str
) -> str:
    """
    Remplace toutes les occurrences des noms de CTE dans 'sql_code' par un nom suffixé
    afin d'éviter des collisions dans DuckDB.
    (Ici, on ne fait PAS d'exception pour la dernière CTE,
     car on veut vraiment suffixer toute référence aux CTE antérieures.)
    """
    cte_names = [c["name"] for c in last_query_decomposed]
    for dependency in cte_names:
        # Suffixage
        sql_code = sql_code.replace(f"`{dependency}`", f"`{dependency}_{suffix}`")
    return sql_code


def _extract_right_key_from_join(join_expr: exp.Expression) -> Optional[exp.Column]:
    """Return the right-side column of the first equality in the ON clause."""
    on = join_expr.args.get("on")
    if on:
        for eq in on.find_all(exp.EQ):
            right = eq.expression
            if isinstance(right, exp.Column):
                return right
        cols = list(on.find_all(exp.Column))
        if cols:
            return cols[-1]
    using = join_expr.args.get("using")
    if using and isinstance(using, list):
        for item in using:
            if isinstance(item, exp.Column):
                return item
            if isinstance(item, exp.Identifier):
                return exp.column(item.name)
    return None


def _build_count_steps_query(
    cte_code: str,
    preceding_ctes: List[Dict[str, str]],
    dialect: str,
) -> tuple[str, List[str]]:
    """Single query with SUM(CASE WHEN …) columns for each JOIN then each WHERE condition.

    All INNER JOINs are converted to LEFT JOINs so every base row is preserved.
    Returns (full_sql, labels) where labels[i] describes the i-th SELECT column.
    """
    tree = sqlglot.parse_one(cte_code, read=dialect)
    from_expr: Optional[exp.Expression] = tree.args.get("from") or tree.args.get(
        "from_"
    )
    joins: List[exp.Expression] = tree.args.get("joins") or []
    where: Optional[exp.Expression] = tree.args.get("where")

    labels: List[str] = []
    select_parts: List[str] = ["COUNT(*) AS base_count"]
    base_name = from_expr.this.alias_or_name if from_expr else "base"
    labels.append(base_name)

    join_null_conditions: List[str] = []
    left_join_sqls: List[str] = []

    for i, join in enumerate(joins):
        join_copy = join.copy()
        join_copy.set("side", "LEFT")
        join_copy.set("kind", None)
        left_join_sqls.append(join_copy.sql(dialect=dialect))

        right_col = _extract_right_key_from_join(join)
        if right_col:
            col_sql = right_col.sql(dialect=dialect)
            join_null_conditions.append(f"{col_sql} IS NOT NULL")
            cumul = " AND ".join(join_null_conditions)
            select_parts.append(
                f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_join_{i + 1}"
            )
            labels.append(f"+ JOIN ({col_sql} IS NOT NULL)")
        else:
            select_parts.append(f"COUNT(*) AS after_join_{i + 1}")
            labels.append(f"+ JOIN {i + 1}")

    where_conds = _extract_conditions(where.this) if where else []
    cumul_parts = list(join_null_conditions)

    for j, cond in enumerate(where_conds):
        cond_sql = cond.sql(dialect=dialect)
        cumul_parts.append(f"({cond_sql})")
        cumul = " AND ".join(cumul_parts)
        select_parts.append(
            f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_cond_{j + 1}"
        )
        labels.append(f"+ WHERE {cond_sql}")

    from_sql = from_expr.sql(dialect=dialect) if from_expr else ""
    joins_sql = ("\n" + "\n".join(left_join_sqls)) if left_join_sqls else ""
    select_cols = ",\n  ".join(select_parts)
    body = f"SELECT\n  {select_cols}\n{from_sql}{joins_sql}"

    if preceding_ctes:
        with_parts = [f"`{c['name']}` AS ({c['code']})" for c in preceding_ctes]
        return f"WITH {', '.join(with_parts)}\n{body}", labels

    return body, labels


async def _run_cte_step_trace(
    ctes: list, failing_idx: int, suffix: str, project: str, dialect: str, con
) -> list:
    """Step-level breakdown for a failing CTE (row_count==0).

    Runs a single query with cumulative SUM(CASE WHEN …) columns so the generator knows
    exactly which JOIN condition or WHERE predicate filters out all rows.
    Returns [{label, count}].
    """
    cte = ctes[failing_idx]
    preceding = [c for c in ctes[:failing_idx] if c["name"] != "final_query"]

    try:
        full_sql, labels = _build_count_steps_query(cte["code"], preceding, dialect)
    except Exception:
        return []

    try:
        df, _ = await run_query_on_test_dataset(full_sql, suffix, project, dialect, con)
    except Exception:
        return []

    if df.empty:
        return [{"label": lbl, "count": 0} for lbl in labels]

    row = df.iloc[0].to_dict()
    col_names = list(row.keys())
    return [
        {"label": lbl, "count": int(row.get(col_names[i], 0) or 0)}
        for i, lbl in enumerate(labels)
        if i < len(col_names)
    ]


async def _run_cte_trace(
    ctes: list, suffix: str, project: str, dialect: str, con
) -> dict:
    """
    For each CTE, builds a WITH ... SELECT * FROM cteN query and runs it to capture row counts.
    For CTEs that return 0 rows, also runs a step-by-step breakdown (per JOIN/WHERE condition).
    Returns {"cte_name": {"row_count": N, "steps": [...]}} for every non-final CTE.
    """
    trace = {}
    for i, cte in enumerate(ctes):
        if cte["name"] == "final_query":
            continue
        with_parts = [
            f"`{ctes[j]['name']}` AS ({ctes[j]['code']})" for j in range(i + 1)
        ]
        sql = "WITH " + ",\n".join(with_parts) + f"\nSELECT * FROM `{cte['name']}`"
        try:
            df, _ = await run_query_on_test_dataset(sql, suffix, project, dialect, con)
            row_count = df.shape[0]
            result: dict = {"row_count": row_count}
            if row_count == 0:
                steps = await _run_cte_step_trace(
                    ctes, i, suffix, project, dialect, con
                )
                if steps:
                    result["steps"] = steps
            trace[cte["name"]] = result
        except Exception as e:
            trace[cte["name"]] = {"row_count": -1, "error": str(e)}
    return trace


async def _run_single_test_case(
    state: QueryState,
    test_case: Dict[str, Any],
    loop_index: int,
    session_id: str,
    query: str,
    schemas: list,
    used_columns: Optional[List[Dict[str, List[str]]]],
    con,
    dialect,
    rerun_all: bool = False,
) -> Dict[str, Any]:
    """
    Exécute la logique d'un seul cas de test.
    Retourne un dict fusionné contenant les métadonnées du test (issues du LLM)
    et les résultats d'exécution DuckDB. Les erreurs sont capturées dans le résultat.
    test_index provient du test_case lui-même pour conserver l'identifiant logique.
    """
    # Preserve the logical test_index from the test case (string like "1", "2"…)
    test_index = test_case.get("test_index", str(loop_index))
    base = {
        "test_index": test_index,
        "test_name": test_case.get("test_name", ""),
        "unit_test_description": test_case.get("unit_test_description", ""),
        "unit_test_build_reasoning": test_case.get("unit_test_build_reasoning", ""),
        "tags": test_case.get("tags", []),
        "suggestions": test_case.get("suggestions", []),
        "data": test_case.get("data", {}),
    }

    try:
        # 1) Préparation et insertion des données de test
        test_data = _prepare_test_data(test_case, schemas)
        suffix = f"{session_id}{test_index}"

        logger.debug("Creating temp tables for suffix=%s", suffix)

        logger.diag(
            "[executor] tables dans les données: %s",
            list(test_case.get("data", {}).keys()),
        )
        for tname, rows in test_case.get("data", {}).items():
            logger.diag(
                "  %s: %s ligne(s)", tname, len(rows) if isinstance(rows, list) else "?"
            )

        # Création des tables de test dans DuckDB + insertion
        # Toujours overwrite=True : chaque passage (retry inclus) repart sur des tables fraîches.
        # L'ancien overwrite=False sur empty_results accumulait les anciennes lignes + les nouvelles,
        # causant des conflits dans les CTEs qui lisent les mêmes tables (ex: SIRET_ONUS).
        logger.diag(
            "[executor] overwrite=True (status précédent=%s)", state.get("status")
        )
        from utils.timing import atimed

        async with atimed("exec:duckdb_setup+query"):
            duckdb_tables_schema = create_test_tables(
                tables=schemas,
                suffix=suffix,
                overwrite=True,
                con=con,
                dialect=dialect,
            )
            insert_queries = insert_examples(
                data_dict=test_data,
                schemas=duckdb_tables_schema,
                suffix=suffix,
                used_columns=used_columns,
            )
            execute_queries(list(insert_queries), con)
            # 2) On exécute la requête globale
            final_res_df, final_duckdb_sql = await run_query_on_test_dataset(
                query, suffix, state["project"], dialect, con
            )
        logger.diag("[executor] DuckDB SQL exécuté:\n%s", final_duckdb_sql[:2000])
        logger.diag("[executor] résultat: %s ligne(s)", len(final_res_df))

        if final_res_df.empty:
            ctes = json.loads(state.get("query_decomposed") or "[]")
            cte_trace = await _run_cte_trace(
                ctes, suffix, state["project"], dialect, con
            )
            failing_cte = next(
                (name for name, info in cte_trace.items() if info["row_count"] == 0),
                None,
            )
            return {
                **base,
                "status": "empty_results",
                "results_json": await format_result(final_res_df),
                "cte_trace": cte_trace,
                "failing_cte": failing_cte,
                "assertion_results": [],
            }

        existing_assertions = [
            a for a in (test_case.get("assertion_results") or []) if a.get("sql")
        ]

        if rerun_all and existing_assertions:
            # Re-run existing assertions without LLM (user-triggered rerun or SQL update)
            view_name = f"__result__{suffix}"
            con.register(view_name, final_res_df)
            try:
                retry_kwargs = dict(
                    view_name=view_name,
                    con=con,
                    duckdb_sql=final_duckdb_sql,
                    test_data=test_data,
                    result_df=final_res_df,
                    test_description=test_case.get("unit_test_description", ""),
                )
                assertion_results = await _evaluate_assertions_with_retry(
                    existing_assertions, **retry_kwargs
                )
            finally:
                con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
            has_failing = any(not a.get("passed") for a in assertion_results)
            return {
                **base,
                "status": "complete",
                "results_json": await format_result(final_res_df),
                "assertion_results": assertion_results,
                "verdict": "Insuffisant" if has_failing else "Bon",
                "reason_type": "bad_assertions" if has_failing else None,
                "evaluation_explanation": (
                    "Les assertions échouent sur les données re-exécutées."
                    if has_failing
                    else "Les assertions passent sur les données re-exécutées."
                ),
            }

        # Assertions and LLM evaluation are handled by the assertion_generator node
        return {
            **base,
            "status": "complete",
            "results_json": await format_result(final_res_df),
            "assertion_results": [],
        }

    except asyncio.CancelledError:
        logger.warning(
            "[executor] test annulé (CancelledError) — statut error pour history_saver"
        )
        return {
            **base,
            "status": "error",
            "error": "cancelled",
            "results_json": "[]",
        }
    except Exception as e:
        if _is_duckdb_data_error(e):
            logger.warning(
                "[executor] Erreur de données DuckDB → bad_data_error: %s", e
            )
            return {
                **base,
                "status": "bad_data_error",
                "exec_error": str(e),
                "results_json": "[]",
                "assertion_results": [],
            }
        return {
            **base,
            "status": "error",
            "error": str(e),
            "results_json": "[]",
        }


_DUCKDB_DATA_ERROR_PREFIXES = ("Invalid Input Error", "Conversion Error")


def _is_duckdb_data_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(msg.startswith(p) for p in _DUCKDB_DATA_ERROR_PREFIXES)


def _prepare_test_data(
    test_case: Dict[str, Any], schemas: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Récupère les données de test, les parse en JSON, et remplace les valeurs manquantes par NULL.
    """
    test_data_json = test_case.get("data", {})
    return replace_missing_with_null(test_data_json, schemas)


async def _save_step_partial_results(
    cte: Dict[str, Any],
    partial_res: DataFrame,
) -> List[Dict[str, Any]]:
    """
    Construit la liste des résultats partiels :
      - version standard
      - version no_where seulement si has_where == True
    """
    results = [
        {
            "cte_name": cte["name"],
            "sql_code": cte["code"],
            "row_count": partial_res.shape[0],
            "result_json": await format_result(partial_res),
        }
    ]

    return results


async def _handle_test_result(
    state: QueryState,
    test_case: Dict[str, Any],
    test_index: int,
    test_data: Dict[str, Any],
    test_res_df: DataFrame,
    simplified_partial_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Gère la construction du résultat final pour un test donné.
    - En cas de DataFrame vide => statut 'empty_results'
    - Sinon => statut 'complete'
    """
    format_res = await format_result(test_res_df)
    if test_res_df.size == 0 and state["gen_retries"] > 0:
        return {
            "test_index": test_index,
            "unit_test_description": test_case.get("unit_test_description", ""),
            "status": "empty_results",
            "test_data": test_data,
            "results_json": format_res,
            "step_by_step_results": simplified_partial_results,
        }

    return {
        "test_index": test_index,
        "unit_test_description": test_case.get("unit_test_description", ""),
        "status": "complete",
        "test_data": test_data,
        "results_json": format_res,
        "step_by_step_results": simplified_partial_results,
    }


REGEN_ASSERTION_LIMIT = 3


async def _generate_assertions_and_evaluate(
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> _AssertionsAndEvaluation:
    """
    Single LLM call that generates 1-N dbt-style assertions AND evaluates test quality.
    Returns an _AssertionsAndEvaluation with assertions, verdict, explanation, and optional fix.
    Falls back to an empty assertions + Bon verdict on failure.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    prompt = f"""Tu es un expert en tests SQL dbt-style avec DuckDB.

Description du test : {test_description}

Données d'entrée :
{test_data}

Requête SQL testée :
```sql
{duckdb_sql}
```

Résultat après exécution sur DuckDB — {row_count} ligne(s).

Schéma exact de `__result__` :
{schema_str}

Exemples de lignes :
{json.dumps(sample, ensure_ascii=False, default=str)}

Commence par raisonner à voix haute (`reasoning`, 3–5 phrases) :
- Quelle est l'intention de ce test ? Quel comportement SQL veut-il vérifier ?
- Les données d'entrée sont-elles cohérentes avec cette intention (types, cardinalité, cas limites) ?
- Si la requête contient GROUP BY + agrégat (COUNT, STDDEV, AVG, SUM, MAX…) : est-ce que
  TOUS les groupes ont exactement la même cardinalité ? Ex : 1 ligne par groupe partout →
  COUNT=1 constant → STDDEV=0 → bad_data. En revanche, si les groupes ont des cardinalités
  différentes (ex : 3, 2, 1, 1, 1) → STDDEV calculable → test valide, ne pas signaler bad_data.
  La correction est de dupliquer des lignes sur la même clé GROUP BY, pas d'ajouter de nouvelles valeurs.
- Le résultat DuckDB est-il conforme à ce qu'on attendrait ?
- Les assertions à générer valident-elles réellement la logique métier ? Regarde les "Exemples de lignes" ci-dessus
  pour juger : si la colonne vérifiée par `IS NULL` contient déjà des valeurs non-nulles dans les exemples, l'assertion
  est triviale (retourne toujours 0 ligne). Plus généralement, une assertion est triviale si elle passe quel que soit
  le contenu réel du résultat — elle ne discrimine pas une bonne réponse d'une mauvaise. Exemples typiques :
  `WHERE col IS NULL` (colonne jamais nulle), `WHERE 1=0`, ou un `NOT IN (...)` dont la liste englobe tous les
  cas possibles sauf un. Si toutes les assertions sont triviales et ne vérifient aucune valeur concrète ou invariant
  réel du résultat, c'est `bad_assertions`.
  Une bonne assertion pince soit la valeur exacte retournée (`WHERE date != '2026-03-07'`), soit un invariant
  structurel observable (`WHERE z_score = (SELECT MAX(z_score) FROM __result__)`).
- Si la requête utilise ORDER BY + LIMIT ou OFFSET : est-ce que plusieurs lignes ont exactement la même
  valeur de tri à la position retournée ? Si oui, le résultat est non-déterministe (ex : 3 groupes avec
  le même COUNT → même Z-score → OFFSET 1 retourne n'importe lequel) → `bad_data`. La correction est
  d'assigner des cardinalités distinctes à chaque groupe de façon à avoir un ordre unique.

Puis produis :

1. Entre 1 et plusieurs assertions SQL dbt-style sur `__result__` — autant que nécessaire pour valider ce scénario (1 suffit si le test est simple, plusieurs si la requête couvre plusieurs calculs ou cas).
   - Convention : 0 ligne si OK, des lignes si KO.
   - Utilise UNIQUEMENT les colonnes du schéma ci-dessus (noms exacts, sensibles à la casse).
   - INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`. Pour vérifier un MAX ou une valeur relative, utilise une sous-requête sur `__result__` uniquement : `SELECT * FROM __result__ WHERE val != (SELECT MAX(val) FROM __result__)`
   - Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête :
     `SELECT * FROM (SELECT *, expr AS col FROM __result__) WHERE col ...`
   - INTERDIT — assertions triviales (retournent toujours 0 ligne quelle que soit la valeur du résultat) :
     ✗ `WHERE col IS NULL` si les exemples ci-dessus montrent des valeurs non-nulles pour cette colonne
     ✗ `WHERE COUNT(*) = 0` ou `WHERE 1=0` (tautologies)
     ✗ `WHERE col NOT IN (...)` si la liste couvre toutes les valeurs possibles sauf l'impossible
     Une assertion triviale ne discrimine pas une bonne réponse d'une mauvaise — elle est inutile.
   - OBLIGATOIRE pour les requêtes ORDER BY + LIMIT/OFFSET : inclure au moins une assertion qui vérifie
     la VALEUR CONCRÈTE retournée. Utilise les "Exemples de lignes" pour identifier le résultat attendu.
     Exemple : si `__result__` contient `{{"date": "2026-01-02"}}`, l'assertion est `WHERE date != '2026-01-02'`.
     Pour les colonnes date/timestamp, utilise le format `'YYYY-MM-DD'` (ex: `'2016-01-02'`) sans la partie heure.

2. Le verdict de qualité :
   - `verdict` : "Excellent", "Bon", ou "Insuffisant"
   - `reason_type` (uniquement si Insuffisant) : "bad_data" (données d'entrée incorrectes —
     mauvais types, contraintes non respectées, résultat inattendu) ou "bad_assertions"
     (les assertions générées ne permettent pas de valider ce scénario — y compris si elles
     sont triviales : toujours vraies indépendamment de la valeur réelle du résultat)
   - `explanation` : une phrase ultra-concise (max 20 mots) en français, lisible par un responsable métier —
     sans noms de colonnes, de CTEs ni de mots-clés SQL.
     ✓ 'Les données couvrent correctement le scénario nominal.'
     ✓ 'Les valeurs d'entrée ne produisent pas le résultat attendu pour ce cas limite.'
     ✗ 'La CTE orders_filtered retourne 0 lignes car user_id IS NULL.'
   - `assertion_fix` (uniquement si `reason_type == "bad_assertions"`) : objet décrivant
     la correction à apporter au test pour permettre une meilleure génération d'assertions :
     - `test_name` : nom court corrigé (3–6 mots)
     - `unit_test_description` : description précise et correcte, sans ambiguïté
     - `unit_test_build_reasoning` : explication de la correction
     - `tags` : liste parmi Logique métier, Null checks, Cas limites, Intégration,
       Valeurs dupliquées, Performance
     - `suggestions` : 2–3 vérifications correctives précises ("Vérifie que …")
     Si `reason_type != "bad_assertions"`, `assertion_fix` doit être `null`.
   - `diagnostic` : OBLIGATOIRE si `reason_type == "bad_data"`, sinon `null`.
     Quand `reason_type == "bad_data"`, tu DOIS remplir ce bloc — ne laisse pas null :
     - `root_cause` : phrase courte identifiant la cause (ex: "STDDEV=0 — chaque date n'apparaît qu'une fois")
     - `sql_pattern` : clause SQL en cause (ex: "COUNT(descript) GROUP BY date → variance nulle")
     - `data_issue` : ce qui manque dans les données générées (ex: "6 dates distinctes avec 1 ligne chacune, COUNT=1 partout")
     - `fix_summary` : phrase courte (max 15 mots) lisible par l'utilisateur dans l'UI —
       décrit le mécanisme sans les valeurs concrètes ni les détails techniques.
       ✓ Bon : "Dupliquer des lignes sur la même date pour varier le COUNT par groupe."
       ✓ Bon : "Ajouter une ligne de JOIN manquante pour que la jointure produise un résultat."
       ✗ Interdit : noms de colonnes, de CTEs, valeurs spécifiques, termes SQL.
     - `fix_recipe` : instruction opérationnelle complète passée au correcteur — 4 éléments requis :
       (1) table exacte et champ(s) à modifier (nom exact tel qu'affiché dans "Données d'entrée"),
       (2) mécanisme précis : pour les bugs GROUP BY/agrégat, écrire impérativement
           "dupliquer N lignes avec [col_group_by]='[valeur]'" — JAMAIS "ajouter des valeurs variables"
           ni aucune formulation abstraite,
       (3) valeurs concrètes tirées des données d'entrée ci-dessus, avec le compte par groupe
           (ex: "'2016-01-02' × 3 lignes, '2016-01-03' × 2 lignes"),
       (4) effet attendu sur le calcul SQL (ex: "→ COUNT ∈ {2, 3} → STDDEV > 0").
       ✗ Interdit : "ajouter des données variables", "modifier les valeurs", tout terme générique.
       ✓ Bon : "Dans [table], dupliquer la ligne [col]='2016-01-02' pour en avoir 3 copies
                et [col]='2016-01-03' pour en avoir 2 → COUNT varie → STDDEV > 0."
     - `affected_tables` : liste des noms de tables dont les données doivent être corrigées
     - `affected_ctes` : liste des CTEs impactées par le problème

Cas particulier — résultat vide intentionnel : si la description mentionne explicitement
"plage vide", "aucune ligne", "filtre qui exclut tout", alors le résultat vide est correct.
Évalue si les données d'entrée sont bien construites pour produire ce vide (Bon/Excellent),
ou si les données ne semblent pas configurées pour ce scénario (Insuffisant + bad_data)."""

    llm = make_llm()
    structured_llm = llm.with_structured_output(_AssertionsAndEvaluation)
    try:
        logger.diag("[assertions_eval] prompt (extrait):\n%s", prompt[:3000])
        result: _AssertionsAndEvaluation = await structured_llm.ainvoke(prompt)
        logger.diag("[assertions_eval] reasoning:\n%s", result.reasoning)
        logger.diag(
            "[assertions_eval] verdict=%s reason_type=%s assertions=%s",
            result.verdict,
            result.reason_type,
            len(result.assertions),
        )
        for i, a in enumerate(result.assertions):
            logger.diag(
                "[assertions_eval] [%d] %s | sql: %s",
                i,
                a.description,
                a.sql,
            )
        return result
    except Exception as e:
        logger.diag("[assertions_eval] ERREUR: %s", e)
        return _AssertionsAndEvaluation(
            assertions=[],
            verdict="Bon",
            explanation="Évaluation indisponible.",
        )


async def _generate_diagnostic(
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
    eval_reasoning: str,
) -> Optional[DiagnosticBlock]:
    """Second focused LLM call to produce a surgical DiagnosticBlock when bad_data is detected.
    Uses DiagnosticBlock directly as structured output schema — all fields required, no Optional."""
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    prompt = f"""Tu es un expert en tests SQL. Le test suivant a été jugé "bad_data" : les données d'entrée ne permettent pas de valider le scénario.

Description du test : {test_description}

Données d'entrée injectées dans DuckDB :
{test_data}

Requête SQL testée :
```sql
{duckdb_sql}
```

Résultat DuckDB — {row_count} ligne(s) :
{sample}

Raisonnement de l'évaluateur :
{eval_reasoning}

Produis une analyse chirurgicale en remplissant TOUS les champs :
- `root_cause` : phrase courte identifiant la cause racine (ex: "STDDEV=0 — chaque date n'apparaît qu'une fois")
- `sql_pattern` : clause SQL en cause (ex: "COUNT(descript) GROUP BY date → variance nulle → STDDEV=0")
- `data_issue` : description précise du défaut dans les données (ex: "6 dates distinctes avec 1 ligne chacune → COUNT=1 partout")
- `fix_summary` : phrase courte (max 15 mots) lisible par l'utilisateur — mécanisme sans détails techniques
  ✓ "Dupliquer des lignes sur la même date pour varier le COUNT par groupe."
  ✗ Noms de colonnes, CTEs, valeurs spécifiques, termes SQL
- `fix_recipe` : instruction complète pour le correcteur :
  (1) table exacte et champ(s) à modifier,
  (2) mécanisme précis — pour GROUP BY/agrégat : "dupliquer N lignes avec [col]='[valeur]'" JAMAIS "ajouter des valeurs variables",
  (3) valeurs concrètes avec compte par groupe (ex: "'2016-01-02' × 3, '2016-01-03' × 2, '2016-01-01' × 1"),
  (4) effet attendu (ex: "→ COUNT ∈ {{1,2,3}} → STDDEV > 0").
- `affected_tables` : noms des tables dont les données doivent être corrigées
- `affected_ctes` : CTEs impactées par le problème"""

    llm = make_llm()
    structured_llm = llm.with_structured_output(DiagnosticBlock)
    try:
        logger.diag("[diagnostic] appel LLM ciblé bad_data")
        diag: DiagnosticBlock = await structured_llm.ainvoke(prompt)
        logger.diag(
            "[diagnostic] root_cause=%r\n  data_issue=%r\n  fix_recipe=%r\n  fix_summary=%r\n  affected_tables=%s\n  affected_ctes=%s",
            diag.root_cause,
            diag.data_issue,
            diag.fix_recipe,
            diag.fix_summary,
            diag.affected_tables,
            diag.affected_ctes,
        )
        return diag
    except Exception as e:
        logger.diag("[diagnostic] ERREUR: %s", e)
        return None


def _evaluate_assertions(
    assertions: List[Dict[str, Any]], view_name: str, con
) -> List[Dict[str, Any]]:
    """
    Évalue chaque assertion dbt-style contre le DataFrame résultat enregistré sous view_name.

    Convention dbt-style : une assertion SQL doit retourner les lignes ÉCHOUANTES.
      - 0 ligne retournée → assertion passée (passed=True)
      - ≥1 ligne retournée → assertion échouée (passed=False), les lignes sont des contre-exemples

    Exemple : pour vérifier que `start_station_name` vaut toujours 'Central Park' :
      SELECT * FROM __result__ WHERE start_station_name != 'Central Park'
      → retourne les lignes où la station est incorrecte ; 0 ligne = OK.

    Ne pas confondre avec une assertion positive (WHERE col = 'X') qui retournerait
    des lignes quand la condition est vraie — ce serait l'inverse de la convention.
    """
    results = []
    for a in assertions:
        sql = (a.get("sql") or "").replace("__result__", view_name)
        try:
            fail_df = con.execute(sql).fetchdf()
            passed = len(fail_df) == 0
            results.append(
                {
                    "description": a.get("description", ""),
                    "sql": a.get("sql", ""),
                    "passed": passed,
                    "failing_rows": fail_df.to_dict(orient="records")
                    if not passed
                    else [],
                }
            )
        except Exception as e:
            results.append(
                {
                    "description": a.get("description", ""),
                    "sql": a.get("sql", ""),
                    "passed": False,
                    "error": str(e),
                }
            )
    return results


async def _regenerate_assertion(
    original: Dict[str, Any],
    error: str,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> Optional[Dict[str, Any]]:
    """
    Demande au LLM de corriger une assertion dont l'exécution a produit une erreur.
    Retourne un nouveau dict {"description": ..., "sql": ...} ou None en cas d'échec.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"

    prompt = f"""Tu es un expert en tests SQL DuckDB dbt-style.

L'assertion SQL suivante a produit une erreur lors de son exécution :

Description : {original.get("description", "")}
SQL :
```sql
{original.get("sql", "")}
```

Erreur : {error}

Contexte :
- Description du test : {test_description}
- Requête SQL testée :
```sql
{duckdb_sql}
```
- Schéma exact de `__result__` :
{schema_str}
- Données de test input : {test_data}

Corrige uniquement le SQL pour qu'il soit valide en DuckDB.
Règle : l'assertion doit retourner 0 ligne si OK, des lignes si KO.
INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`. Si l'assertion originale référençait une autre table (source ou suffixée), réécris-la pour n'utiliser que `__result__` et ses colonnes du schéma ci-dessus.
Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête.

Réponds UNIQUEMENT avec un objet JSON (aucun texte autour) :
{{"description": "...", "sql": "SELECT ..."}}"""

    llm = make_llm()
    try:
        logger.diag(
            "[regen_assertion] assertion à corriger: %r",
            original.get("description", ""),
        )
        logger.diag("[regen_assertion] erreur: %s", error)
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
        logger.diag("[regen_assertion] réponse brute:\n%s", content[:500])
        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            parsed = json.loads(json_match.group())
            if isinstance(parsed, dict) and parsed.get("sql"):
                return parsed
    except Exception as e:
        logger.diag("[regen_assertion] ERREUR: %s", e)
    return None


_SUFFIXED_TABLE_RE = re.compile(
    r'"[^"]+_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}[^"]*"'
)


def _assertion_references_source_tables(sql: str) -> bool:
    """Return True if the assertion SQL contains session-suffixed table names (UUID pattern).
    These are invalid outside the current DuckDB session and must be rejected."""
    return bool(_SUFFIXED_TABLE_RE.search(sql))


async def _evaluate_assertions_with_retry(
    assertions: List[Dict[str, Any]],
    view_name: str,
    con,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> List[Dict[str, Any]]:
    """
    Évalue les assertions et retente la régénération (jusqu'à REGEN_ASSERTION_LIMIT fois)
    de celles qui produisent une erreur d'exécution (pas juste un échec métier).
    """
    logger.diag("[assertion_retry] évaluation de %s assertion(s)", len(assertions))
    results = _evaluate_assertions(assertions, view_name, con)
    logger.diag(
        "[assertion_retry] résultats initiaux: %s",
        [{"passed": r.get("passed"), "error": bool(r.get("error"))} for r in results],
    )

    for attempt in range(REGEN_ASSERTION_LIMIT):
        errored_indices = [i for i, r in enumerate(results) if r.get("error")]
        if not errored_indices:
            break
        logger.diag(
            "[assertion_retry] tentative %s/%s — %s assertion(s) en erreur",
            attempt + 1,
            REGEN_ASSERTION_LIMIT,
            len(errored_indices),
        )
        for i in errored_indices:
            new_assertion = await _regenerate_assertion(
                original=results[i],
                error=results[i]["error"],
                duckdb_sql=duckdb_sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
            )
            if new_assertion and not _assertion_references_source_tables(
                new_assertion.get("sql", "")
            ):
                new_eval = _evaluate_assertions([new_assertion], view_name, con)
                results[i] = new_eval[0]
            elif new_assertion:
                logger.diag(
                    "[assertion_retry] assertion régénérée rejetée — référence table non-__result__: %s",
                    new_assertion.get("sql", "")[:200],
                )

    return results


async def _fix_logically_failing_assertions(
    assertion_results: List[Dict[str, Any]],
    view_name: str,
    con,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> List[Dict[str, Any]]:
    """
    Pour les assertions qui échouent logiquement (passed=False, sans erreur SQL),
    demande au LLM si l'assertion elle-même est incorrecte. Si oui, la régénère
    et la réévalue une fois. Appelée uniquement lors de la génération initiale.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    results = list(assertion_results)

    failing_indices = [
        i for i, r in enumerate(results) if not r.get("passed") and not r.get("error")
    ]
    logger.diag(
        "[assertion_fixer] %s assertion(s) logiquement échouée(s) sur %s",
        len(failing_indices),
        len(assertion_results),
    )
    if not failing_indices:
        return results

    for i in failing_indices:
        a = results[i]
        failing_rows = a.get("failing_rows", [])
        logger.diag(
            "[assertion_fixer] correction assertion %s: %r", i, a.get("description", "")
        )

        prompt = f"""Tu es un expert en tests SQL DuckDB dbt-style.

Tu viens de générer une assertion qui échoue (retourne des lignes alors qu'elle devrait en retourner 0).
Détermine si l'assertion est logiquement correcte ou si tu as fait une erreur dans sa logique.

Description du test : {test_description}

Données de test input :
{test_data}

Requête SQL testée :
```sql
{duckdb_sql}
```

Schéma exact de `__result__` :
{schema_str}

Exemples de résultat réel :
{json.dumps(sample, ensure_ascii=False, default=str)}

Assertion qui échoue :
- Description : {a.get("description", "")}
- SQL :
```sql
{a.get("sql", "")}
```

Lignes retournées par l'assertion (violations détectées) :
{json.dumps(failing_rows[:10], ensure_ascii=False, default=str)}

Question : l'assertion est-elle logiquement correcte par rapport au résultat réel, \
ou as-tu fait une erreur dans sa formulation (mauvaise valeur attendue, mauvaise colonne, condition inversée, etc.) ?

- Si l'assertion est **correcte** et le test échoue vraiment → réponds : {{"correct": true}}
- Si l'assertion est **incorrecte** (tu as fait une erreur) → régénère-la : \
{{"correct": false, "description": "...", "sql": "SELECT ..."}}

Règles DuckDB strictes :
- Utilise UNIQUEMENT les colonnes du schéma ci-dessus
- Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête

Réponds UNIQUEMENT avec un objet JSON (aucun texte autour)."""

        llm = make_llm()
        try:
            result = await llm.ainvoke(prompt)
            content = normalize_llm_content(result.content)
            logger.diag("[assertion_fixer] réponse LLM:\n%s", content[:500])
            json_match = re.search(r"\{[\s\S]*\}", content)
            if not json_match:
                continue
            parsed = json.loads(json_match.group())
            if not isinstance(parsed, dict):
                continue
            if parsed.get("correct"):
                continue
            new_sql = parsed.get("sql")
            if not new_sql:
                continue
            new_assertion = {
                "description": parsed.get("description", a["description"]),
                "sql": new_sql,
            }
            new_eval = _evaluate_assertions([new_assertion], view_name, con)
            results[i] = new_eval[0]
        except Exception:
            pass

    return results


def _determine_global_status(all_tests_results: List[Dict[str, Any]]) -> str:
    """
    Détermine le statut global en fonction des résultats de tous les tests.
    Seul le premier test (cas standard sans instruction utilisateur) peut déclencher
    un retry : si son résultat est vide, on renvoie 'empty_results'.
    Les tests suivants (avec instruction utilisateur) peuvent légitimement être vides.
    Une erreur DuckDB (parsing, binder…) n'est pas corrigeable par les données : on
    renvoie 'error' pour stopper les boucles de retry.
    """
    if not all_tests_results:
        return "complete"
    first = all_tests_results[0]
    if first.get("status") == "error":
        return "error"
    if first.get("status") == "bad_data_error":
        return "bad_data_error"
    if first.get("status") == "empty_results":
        return "empty_results"
    return "complete"


async def format_result(res: DataFrame) -> str:
    """
    Convertit le DataFrame en JSON (orientation = records).
    Retourne une chaîne JSON.
    """
    format_res = res.to_json(orient="records", date_format="iso", date_unit="s")
    return str(format_res)
