import json
import logging
import uuid
from datetime import datetime, date
from typing import List, Optional

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import AIMessage
from models.schemas import get_schemas
from pydantic import Field, create_model


from build_query.prompt_tools import generate_data_prompt, update_data_prompt
from build_query.state import QueryState
from utils.examples import (
    create_pydantic_models,
    filter_columns,
)
from utils.llm_factory import make_llm
from storage.config import get_llm_model
from utils.msg_types import MsgType
from utils.prompt_utils import create_output_fixing_parser
from utils.saver import get_message_type, get_history_from_state

logger = logging.getLogger(__name__)


def _convert_datetime_fields(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
            elif isinstance(value, date):
                data[key] = value.strftime("%Y-%m-%d")
            elif isinstance(value, (dict, list)):
                data[key] = _convert_datetime_fields(value)
    elif isinstance(data, list):
        return [_convert_datetime_fields(item) for item in data]
    return data


def _should_regenerate(state, existing_tests: list) -> bool:
    """Return True if test data should be (re)generated."""
    if not existing_tests:
        return True
    if state.get("input", "").strip():
        return True
    if state.get("used_columns_changed"):
        return True
    if state.get("status") == "empty_results":
        return True
    return False


_OP_LABELS = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "like": "LIKE",
    "not_like": "NOT LIKE",
    "in": "IN",
    "not_in": "NOT IN",
    "between": "BETWEEN",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
}


def _run_simplify(
    sql_query: str, schema: list[dict] | None = None, dialect: str = "bigquery"
):
    """Call constraint_simplifier.simplify() and return the result, or None on failure."""
    if not sql_query:
        return None
    try:
        from build_query.constraint_simplifier import simplify as _simplify_sql

        return _simplify_sql(sql_query, schema=schema, dialect=dialect)
    except Exception as exc:
        logger.warning(
            "constraint_simplifier failed (sql_hash=%s dialect=%s): %s",
            hash(sql_query),
            dialect,
            exc,
            exc_info=True,
        )
        return None


def _format_filter_constraints(constraints: list) -> list[str]:
    """Format a list of FilterConstraints as human-readable strings."""
    filters = []
    for c in constraints:
        col_ref = c.column
        op = _OP_LABELS.get(c.op, c.op.upper())
        if c.op == "between":
            lo, hi = (
                c.value if isinstance(c.value, (list, tuple)) else (c.value, c.value)
            )
            filters.append(f"{col_ref} {op} {lo} AND {hi}")
        elif c.op in ("in", "not_in"):
            vals = c.value or []
            val_str = "(" + ", ".join(repr(v) for v in vals[:6])
            if len(vals) > 6:
                val_str += ", ..."
            val_str += ")"
            filters.append(f"{col_ref} {op} {val_str}")
        elif c.op in ("is_null", "is_not_null"):
            filters.append(f"{col_ref} {op}")
        else:
            val = repr(c.value) if c.value is not None else ""
            filters.append(f"{col_ref} {op} {val}".rstrip())
    return filters


def _branch_to_dict(result) -> dict:
    """Convert a SimplificationResult to a plain dict (joins/filters/anti_joins)."""
    joins = [
        " = ".join(sorted(str(c) for c in group))
        for group in result.equivalence_classes
    ]
    anti_joins = [f"{col_a} NOT IN {col_b}" for col_a, col_b in result.col_inequalities]
    all_constraints = [c for cs in result.source_columns.values() for c in cs]
    filters = _format_filter_constraints(all_constraints)
    d: dict = {}
    if joins:
        d["joins"] = joins
    if anti_joins:
        d["anti_joins"] = anti_joins
    if filters:
        d["filters"] = filters
    return d


def _or_path_to_dict(or_path_filters: list, result) -> dict:
    """Build a path dict for one OR branch, keeping join/anti-join context from result."""
    joins = [
        " = ".join(sorted(str(c) for c in group))
        for group in result.equivalence_classes
    ]
    anti_joins = [f"{col_a} NOT IN {col_b}" for col_a, col_b in result.col_inequalities]
    filters = _format_filter_constraints(or_path_filters)
    d: dict = {}
    if joins:
        d["joins"] = joins
    if anti_joins:
        d["anti_joins"] = anti_joins
    if filters:
        d["filters"] = filters
    return d


def _simplification_to_hint(result) -> str:
    """Convert a SimplificationResult to a JSON constraints hint string.

    Priority:
      1. UNION branches  → {"paths": [branch0, branch1, ...]}
         Each branch with OR inside is further expanded into sub-paths.
      2. OR paths only   → {"paths": [or_path0, or_path1, ...]}
         Each path includes join/anti-join context so the LLM sees complete rows.
      3. Flat query      → {"joins": ..., "filters": ..., "anti_joins": ...}
    """
    if result is None:
        return ""

    if result.union_branches:
        paths = []
        truncated = False
        for branch in result.union_branches:
            if branch.or_paths:
                for or_path in branch.or_paths:
                    d = _or_path_to_dict(or_path, branch)
                    if d:
                        paths.append(d)
                if branch.or_paths_truncated:
                    truncated = True
            else:
                d = _branch_to_dict(branch)
                if d:
                    paths.append(d)
        paths = [p for p in paths if p]
        if paths:
            hint: dict = {"paths": paths}
            if truncated:
                hint["paths_truncated"] = True
            return json.dumps(hint, ensure_ascii=False, indent=2)

    if result.or_paths:
        paths = [_or_path_to_dict(p, result) for p in result.or_paths]
        paths = [p for p in paths if p]
        if paths:
            hint = {"paths": paths}
            if result.or_paths_truncated:
                hint["paths_truncated"] = True
            return json.dumps(hint, ensure_ascii=False, indent=2)

    structured = _branch_to_dict(result)
    if not structured:
        return ""
    return json.dumps(structured, ensure_ascii=False, indent=2)


def _strip_unconstrained_from_sql(
    sql: str, excluded_col_names: list[str], dialect: str = "bigquery"
) -> str:
    """Remove unconstrained columns from SELECT lists in SQL (LLM context only)."""
    if not sql or not excluded_col_names:
        return sql

    excluded_pairs: set[tuple[str, str]] = set()
    for entry in excluded_col_names:
        if "." in entry:
            tbl, col = entry.rsplit(".", 1)
            excluded_pairs.add((tbl.lower(), col.lower()))

    if not excluded_pairs:
        return sql

    try:
        import sqlglot.expressions as exp
        from sqlglot import parse_one

        tree = parse_one(sql, dialect=dialect)

        for select in tree.find_all(exp.Select):
            new_exprs = []
            for expr in select.expressions:
                col_node = expr.this if isinstance(expr, exp.Alias) else expr
                if not isinstance(col_node, exp.Column):
                    new_exprs.append(expr)
                    continue
                col_name = col_node.name.lower()
                table_qualifier = (col_node.table or "").lower().split(".")[-1]
                should_exclude = any(
                    col == col_name and (not table_qualifier or table_qualifier == tbl)
                    for tbl, col in excluded_pairs
                )
                if not should_exclude:
                    new_exprs.append(expr)
            select.set("expressions", new_exprs)

        return tree.sql(dialect=dialect)
    except Exception:
        return sql


def _extract_constraints_per_cte(query_decomposed: list, dialect: str) -> dict:
    """Returns {cte_name: parsed_constraints_dict} for each non-final CTE."""
    result_map = {}
    for cte in query_decomposed:
        if cte["name"] == "final_query":
            continue
        sim = _run_simplify(cte["code"], dialect=dialect)
        hint = _simplification_to_hint(sim)
        if hint:
            result_map[cte["name"]] = json.loads(hint)
    return result_map


def _get_failing_cte_from_results(history) -> tuple:
    """Scans history for the last RESULTS message that has a failing CTE."""
    for msg in reversed(history):
        if get_message_type(msg) == MsgType.RESULTS:
            try:
                results = json.loads(msg.content)
                if isinstance(results, list):
                    results = results[0]
                if results.get("status") == "empty_results" and results.get(
                    "failing_cte"
                ):
                    return results["failing_cte"], results.get("cte_trace", {})
            except Exception:
                pass
    return None, {}


async def retrieve_existing_tests(session_id: str, state) -> list:
    """
    Returns the current list of all tests (as dicts).
    Priority:
    1. In-pipeline RESULTS messages (during retry, before saver has persisted)
    2. Filesystem (test_repository)
    """
    # 1. In-pipeline RESULTS (executor ran this cycle but saver hasn't run yet)
    results_msgs = [
        m for m in state.get("messages", []) if get_message_type(m) == MsgType.RESULTS
    ]
    if results_msgs:
        try:
            data = json.loads(results_msgs[-1].content)
            if isinstance(data, list) and data:
                return data
        except Exception:
            pass

    # 2. Filesystem storage
    from storage.test_repository import get_test

    test = get_test(session_id)
    if test and isinstance(test.get("test_cases"), list):
        return test["test_cases"]
    return []


async def generate_examples(state: QueryState):
    """Generates a single new test (or skips if regeneration is not needed)."""
    from utils.llm_errors import (
        is_vertex_permission_error,
        format_vertex_permission_message,
    )

    session_id = state["session"]
    history = get_history_from_state(state)

    existing_tests = await retrieve_existing_tests(session_id, state)

    used_columns = [json.loads(c) for c in state.get("used_columns") or []]

    if state.get("input", "").strip():
        parent = state["user_message_id"]
    else:
        if state.get("status") == "empty_results" and state.get("messages"):
            parent = state["messages"][-1].id
        else:
            parent = state.get("parent_message_id") or state.get("user_message_id")

    try:
        generated_test, generated_test_index = await generate_examples_(
            state, used_columns, existing_tests, history
        )
    except Exception as exc:
        if is_vertex_permission_error(exc):
            error_msg = format_vertex_permission_message(get_llm_model())
            return {
                "messages": [
                    AIMessage(
                        content=error_msg,
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.ERROR,
                            "parent": parent,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "error": "llm_permission_denied",
            }
        raise

    if generated_test is None:
        # No generation needed; executor will load existing tests from DB
        return {"examples": []}

    examples_kwargs = {
        "type": MsgType.EXAMPLES,
        "parent": parent,
        "request_id": state.get("request_id"),
        "is_analysis": state.get("is_analysis"),
        "generated_test_index": generated_test_index,
    }
    return {
        "examples": [
            AIMessage(
                content=json.dumps(generated_test),
                id=str(uuid.uuid4()),
                additional_kwargs=examples_kwargs,
            )
        ]
    }


def _resolve_target_key(state, existing_list: list) -> Optional[str]:
    """
    Return the test_index to overwrite, or None to create a new test.
    Only overwrite if the frontend explicitly passed test_index, or during a retry.
    """
    test_index = state.get("test_index")
    if test_index is not None and 0 <= test_index < len(existing_list):
        return existing_list[test_index]["test_index"]
    if state.get("status") == "empty_results" and existing_list:
        return existing_list[0]["test_index"]  # always retry the first (standard) test
    return None


async def generate_examples_(
    state, used_columns, existing_tests: list, history
) -> tuple:
    """
    Returns (single_test_dict_with_test_index, test_index_str) or (None, None).
    The returned dict is a single new test — merging with the full suite happens in saver.py.
    """
    if not _should_regenerate(state, existing_tests):
        return None, None

    schema = await get_schemas(project_id=state["project"])

    dialect = state.get("dialect", "bigquery")
    optimized_sql = state.get("optimized_sql", "")

    # Single simplify() call — result reused for hint + mandatory set + unconstrained
    sim_result = _run_simplify(optimized_sql, schema=schema, dialect=dialect)
    constraints = _simplification_to_hint(sim_result)

    # TODO(#XXX): filtrer uniquement les colonnes contraintes (mandatory) pour réduire
    # le contexte LLM — en attente de stabilisation de sim_result.
    filtered_schema = filter_columns(schema, used_columns)
    excluded_col_names = []

    data_model = create_pydantic_models(filtered_schema)
    output_type = get_generation_output_type(data_model, existing_tests)
    parser = create_output_fixing_parser(
        PydanticOutputParser(pydantic_object=output_type)
    )

    prompt = await create_appropriate_prompt(
        state,
        existing_tests,
        history,
        used_columns,
        parser.get_format_instructions(),
        constraints_hint=constraints,
        excluded_columns=excluded_col_names,
    )
    if prompt is None:
        return None, None
    llm = make_llm()
    generated_data = await (prompt | llm | parser).ainvoke({})

    filled_data = _convert_datetime_fields(generated_data.data.dict())

    generated = {
        "test_name": generated_data.test_name,
        "unit_test_description": generated_data.unit_test_description,
        "unit_test_build_reasoning": generated_data.unit_test_build_reasoning,
        "tags": generated_data.tags,
        "suggestions": generated_data.suggestions,
        "data": filled_data,
    }

    # Determine which test_index slot this new test occupies
    target_key = _resolve_target_key(state, existing_tests)
    if target_key is not None:
        test_index = target_key
    else:
        test_index = str(len(existing_tests) + 1)

    return {**generated, "test_index": test_index}, test_index


def get_generation_output_type(data_model, existing_tests):
    reasoning_desc = (
        "Réflexion pas à pas sur comment modifier les données de tests."
        if existing_tests
        else "Réflexion pas à pas sur comment construire les données de tests."
    )

    return create_model(
        "UnitTestData",
        test_name=(
            str,
            Field(
                description=(
                    "Nom court du scénario (3-6 mots), commençant par un verbe ou un nom. "
                    "Exemple : 'Commandes actives France', 'Ventes nulles juillet'."
                )
            ),
        ),
        unit_test_description=(
            str,
            Field(
                description=(
                    "Assertion courte et actionnable décrivant ce que ce test vérifie, "
                    "commençant par un verbe : 'Vérifie que…', 'S'assure que…'. "
                    "Exemple : 'Vérifie que price > 0 pour toutes les lignes France'."
                )
            ),
        ),
        unit_test_build_reasoning=(str, Field(description=reasoning_desc)),
        tags=(
            List[str],
            Field(
                description=(
                    "Labels décrivant les types de cas couverts. "
                    "Choisir parmi : 'Logique métier', 'Null checks', 'Cas limites', 'Intégration', 'Valeurs dupliquées', 'Performance'. "
                    "Inclure tous les labels pertinents."
                )
            ),
        ),
        suggestions=(
            List[str],
            Field(
                description=(
                    "Exactement 2 suggestions de tests complémentaires à générer ensuite, "
                    "sous forme d'assertions actionnables courtes commençant par un verbe. "
                    "Exemple : ['Vérifie que le revenu total est non nul', 'S'assure que year >= 2000']."
                )
            ),
        ),
        data=(data_model, Field(description="Données du test unitaire.")),
    )


async def create_appropriate_prompt(
    state,
    existing_tests: list,
    history,
    used_columns,
    format_instructions,
    constraints_hint: str = "",
    excluded_columns: list[str] | None = None,
):
    sql = state.get("optimized_sql", "")
    dialect = state.get("dialect", "bigquery")
    profile = state.get("profile")
    stripped_sql = _strip_unconstrained_from_sql(sql, excluded_columns or [], dialect)
    if not existing_tests:
        return generate_data_prompt(
            history,
            dialect,
            format_instructions,
            used_columns,
            constraints_hint=constraints_hint,
            sql=stripped_sql,
            profile=profile,
        )
    elif state.get("input", "").strip():
        if state.get("test_index") is not None:
            idx = state["test_index"]
            existing_test = existing_tests[idx] if 0 <= idx < len(existing_tests) else None
            return update_data_prompt(
                history, state["input"], dialect, format_instructions, sql=sql, existing_test=existing_test
            )
        return generate_data_prompt(
            history,
            dialect,
            format_instructions,
            used_columns,
            constraints_hint=constraints_hint,
            sql=stripped_sql,
            user_instruction=state["input"],
            profile=profile,
        )
    elif state.get("status") == "empty_results":
        return generate_data_prompt(
            history,
            dialect,
            format_instructions,
            used_columns,
            constraints_hint=constraints_hint,
            sql=stripped_sql,
            profile=profile,
        )
    else:
        return None


async def create_combined_model(used_columns, schemas):
    filtered_columns = filter_columns(schemas, used_columns)
    return create_pydantic_models(filtered_columns)
