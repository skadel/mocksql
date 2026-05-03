import json
import logging
import re
import uuid
from typing import List, Dict, Any, Optional

import sqlglot
from langchain_core.messages import AIMessage
from pandas import DataFrame
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



logger = logging.getLogger(__name__)

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
    
    logger.debug("\n[DEBUG] >>> run_on_examples : used_columns bruts récupérés depuis le state:")
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
            logger.debug(f"\n[DEBUG] >>> Lancement test {loop_index} avec table(s) : {list(test_case.get('data', {}).keys())}")
            test_result = await _run_single_test_case(
                state=state,
                test_case=test_case,
                loop_index=loop_index,
                session_id=session_id_duckdb,
                query=state.get("query"),
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
        state.get("gen_retries") if state.get("gen_retries") is not None else 2
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
        "parent": state["messages"][-1].id if len(state["messages"]) > 0 else None,
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
    
    logger.debug("\n[DEBUG] >>> filter_schemas_by_used_columns : used_cols_dict généré:")
    logger.debug(f"      - {used_cols_dict}")

    filtered_schemas = []
    for table_schema in schemas:
        parts = table_schema["table_name"].split(".")
        qualified = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]

        if qualified in used_cols_dict:
            wanted_cols = used_cols_dict[qualified]
            logger.debug(f"\n[DEBUG] >>> Filtrage de la table {qualified}. wanted_cols: {wanted_cols}")

            filtered_columns = [
                col
                for col in table_schema["columns"]
                if col["name"].lower() in wanted_cols
                or any(col["name"].lower().startswith(f"{w}.") for w in wanted_cols)
            ]

            logger.debug(f"[DEBUG] >>> Table {qualified} - Colonnes conservées: {[c['name'] for c in filtered_columns]}")

            if filtered_columns:
                filtered_schemas.append(
                    {
                        "table_name": table_schema["table_name"],
                        "description": table_schema["description"],
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


# TODO fonction (pas utilisée) sensé servir pour débug quand j'ai un status = "empty_results"
# je devrais utiliser cette fonction;
# en fait c'est sensé m'aider à voir quelle étape est en train de rendre mon résultat vide;
# renommer cette fonction et rajouter son utilisation
# voir dans d'autres branche comment c'est utilisé ça peut être utile.
# aussi voir _build_countif_expressions peut être que ça peut servir la même logique
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
    """
    if isinstance(expr, exp.And):
        # Récupérer les conditions à gauche et à droite de l'opérateur AND
        return _extract_conditions(expr.this) + _extract_conditions(expr.expression)
    return [expr]


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


async def _run_cte_trace(
    ctes: list, suffix: str, project: str, dialect: str, con
) -> dict:
    """
    For each CTE, builds a WITH ... SELECT * FROM cteN query and runs it to capture row counts.
    Returns {"cte_name": {"row_count": N}} for every non-final CTE.
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
            trace[cte["name"]] = {"row_count": df.shape[0]}
        except Exception as e:
            trace[cte["name"]] = {"row_count": -1, "error": str(e)}
    return trace


async def _is_empty_result_expected(
    test_description: str,
    duckdb_sql: str,
) -> bool:
    """
    Demande au LLM si un résultat vide est intentionnel pour ce test.
    Retourne True si vide attendu (ex : cas "plage vide", filtre qui exclut tout),
    False si le résultat vide est probablement une erreur de génération de données.
    """
    prompt = f"""Description du test : {test_description}

Requête SQL :
```sql
{duckdb_sql}
```

L'exécution de cette requête sur les données de test a produit 0 ligne.
Est-ce que ce résultat vide est **intentionnel** (le test vérifie justement qu'aucune ligne ne ressort) ?

Réponds UNIQUEMENT par `true` ou `false`."""

    llm = make_llm()
    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content).strip().lower()
        return content.startswith("true")
    except Exception:
        return False


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

        # Création des tables de test dans DuckDB + insertion
        duckdb_tables_schema = create_test_tables(
            tables=schemas,
            suffix=suffix,
            overwrite=(state["status"] != "empty_results"),
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

        if final_res_df.empty:
            ctes = json.loads(state.get("query_decomposed") or "[]")
            cte_trace = await _run_cte_trace(
                ctes, suffix, state["project"], dialect, con
            )
            failing_cte = next(
                (name for name, info in cte_trace.items() if info["row_count"] == 0),
                None,
            )
            empty_intended = await _is_empty_result_expected(
                test_description=test_case.get("unit_test_description", ""),
                duckdb_sql=final_duckdb_sql,
            )
            if not empty_intended:
                return {
                    **base,
                    "status": "empty_results",
                    "results_json": await format_result(final_res_df),
                    "cte_trace": cte_trace,
                    "failing_cte": failing_cte,
                    "assertion_results": [],
                }
            # Résultat vide intentionnel : on pose une assertion count = 0
            assertion_results = [
                {
                    "description": "Le résultat doit être vide (0 ligne attendue)",
                    "sql": "SELECT * FROM (SELECT COUNT(*) AS cnt FROM __result__) WHERE cnt > 0",
                    "passed": True,
                    "failing_rows": [],
                }
            ]
            return {
                **base,
                "status": "complete",
                "results_json": await format_result(final_res_df),
                "cte_trace": cte_trace,
                "failing_cte": failing_cte,
                "assertion_results": assertion_results,
            }

        view_name = f"__result__{suffix}"
        con.register(view_name, final_res_df)
        try:
            existing_assertions = [
                a for a in (test_case.get("assertion_results") or []) if a.get("sql")
            ]
            retry_kwargs = dict(
                view_name=view_name,
                con=con,
                duckdb_sql=final_duckdb_sql,
                test_data=test_data,
                result_df=final_res_df,
                test_description=test_case.get("unit_test_description", ""),
            )
            if rerun_all and existing_assertions:
                assertion_results = await _evaluate_assertions_with_retry(
                    existing_assertions, **retry_kwargs
                )
            else:
                assertions = await _generate_assertions_from_result(
                    duckdb_sql=final_duckdb_sql,
                    test_data=test_data,
                    result_df=final_res_df,
                    test_description=test_case.get("unit_test_description", ""),
                )
                assertion_results = await _evaluate_assertions_with_retry(
                    assertions, **retry_kwargs
                )
        finally:
            con.execute(f'DROP VIEW IF EXISTS "{view_name}"')

        return {
            **base,
            "status": "complete",
            "results_json": await format_result(final_res_df),
            "assertion_results": assertion_results,
        }

    except Exception as e:
        return {
            **base,
            "status": "error",
            "error": str(e),
            "results_json": "[]",
        }


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


async def _generate_assertions_from_result(
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> List[Dict[str, Any]]:
    """
    Asks the LLM to generate 2-3 dbt-style assertions using the real DuckDB result schema.
    Called after query execution so column names and types are exact.
    Returns a list of {"description": ..., "sql": ...} dicts (empty list on failure).
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    prompt = f"""Tu es un expert en tests SQL avec Duckdb dbt-style.
Description du test : {test_description}

Données de tests input :
{test_data}

Requête SQL testée :
```sql
{duckdb_sql}
```

Résultat après execution du test sur DuckDB — {row_count} ligne(s).

Schéma exact de `__result__` :
{schema_str}

Exemples de lignes :
{json.dumps(sample, ensure_ascii=False, default=str)}

Génère 2 à 3 assertions SQL dbt-style sur `__result__`. Chaque assertion = SELECT DuckDB retournant **0 ligne si OK, des lignes si KO**.

⚠️ Règles DuckDB strictes :
- Utilise UNIQUEMENT les colonnes listées dans le schéma ci-dessus (noms exacts, sensibles à la casse)
- Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête :
  `SELECT * FROM (SELECT *, expr AS col FROM __result__) WHERE col ...`

Réponds UNIQUEMENT avec un tableau JSON (aucun texte autour) :
[
  {{"description": "...", "sql": "SELECT ..."}},
  ...
]"""

    llm = make_llm()
    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
        # Greedy match captures the outermost [...] even when the LLM adds surrounding text
        json_match = re.search(r"\[[\s\S]*\]", content)
        if json_match:
            parsed = json.loads(json_match.group())
            return [a for a in parsed if isinstance(a, dict) and a.get("sql")]
    except Exception:
        pass
    return []


def _evaluate_assertions(
    assertions: List[Dict[str, Any]], view_name: str, con
) -> List[Dict[str, Any]]:
    """
    Évalue chaque assertion dbt-style contre le DataFrame résultat enregistré sous view_name.
    Chaque assertion SQL doit retourner 0 ligne pour passer.
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
Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête.

Réponds UNIQUEMENT avec un objet JSON (aucun texte autour) :
{{"description": "...", "sql": "SELECT ..."}}"""

    llm = make_llm()
    try:
        result = await llm.ainvoke(prompt)
        content = normalize_llm_content(result.content)
        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            parsed = json.loads(json_match.group())
            if isinstance(parsed, dict) and parsed.get("sql"):
                return parsed
    except Exception:
        pass
    return None


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
    results = _evaluate_assertions(assertions, view_name, con)

    for _ in range(REGEN_ASSERTION_LIMIT):
        errored_indices = [i for i, r in enumerate(results) if r.get("error")]
        if not errored_indices:
            break
        for i in errored_indices:
            new_assertion = await _regenerate_assertion(
                original=results[i],
                error=results[i]["error"],
                duckdb_sql=duckdb_sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
            )
            if new_assertion:
                new_eval = _evaluate_assertions([new_assertion], view_name, con)
                results[i] = new_eval[0]

    return results


def _determine_global_status(all_tests_results: List[Dict[str, Any]]) -> str:
    """
    Détermine le statut global en fonction des résultats de tous les tests.
    Seul le premier test (cas standard sans instruction utilisateur) peut déclencher
    un retry : si son résultat est vide, on renvoie 'empty_results'.
    Les tests suivants (avec instruction utilisateur) peuvent légitimement être vides.
    """
    if all_tests_results and all_tests_results[0].get("status") == "empty_results":
        return "empty_results"
    return "complete"


async def format_result(res: DataFrame) -> str:
    """
    Convertit le DataFrame en JSON (orientation = records).
    Retourne une chaîne JSON.
    """
    format_res = res.to_json(orient="records", date_format="iso", date_unit="s")
    return str(format_res)
