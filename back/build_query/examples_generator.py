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
from utils.faker_fill import generate_faker_rows
from utils.llm_factory import make_llm
from storage.config import get_llm_model
from utils.msg_types import MsgType
from utils.prompt_utils import create_output_fixing_parser
from utils.saver import get_message_type, get_history_from_state

import utils.logger  # noqa: F401 — registers DIAG level (15)

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


_simplify_cache: dict[tuple[str, str], object] = {}
_SIMPLIFY_CACHE_MAXSIZE = 64


def _run_simplify(
    sql_query: str, schema: list[dict] | None = None, dialect: str = "bigquery"
):
    """Call constraint_simplifier.simplify() and return the result, or None on failure."""
    if not sql_query:
        return None
    cache_key = (sql_query, dialect)
    if cache_key in _simplify_cache:
        return _simplify_cache[cache_key]
    try:
        from build_query.constraint_simplifier import simplify as _simplify_sql

        result = _simplify_sql(sql_query, schema=schema, dialect=dialect)
        if len(_simplify_cache) >= _SIMPLIFY_CACHE_MAXSIZE:
            _simplify_cache.pop(next(iter(_simplify_cache)))
        _simplify_cache[cache_key] = result
        return result
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


def _col_str_join(col) -> str:
    """Return 'real_table.column', preferring real_table over subquery alias."""
    t = (
        col.real_table
        if (col.real_table and col.real_table != col.table)
        else col.table
    )
    return f"{t}.{col.column}"


def _branch_to_dict(result) -> dict:
    """Convert a SimplificationResult to a plain dict (joins/filters/anti_joins)."""
    joins = [
        " = ".join(sorted(_col_str_join(c) for c in group))
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
        " = ".join(sorted(_col_str_join(c) for c in group))
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

    When result.constraint_groups is non-empty (multiple satisfying paths from
    UNION ALL branches, OR conditions, or CTE cross-products), emits:
        {"paths": [group0, group1, ...]}
    Otherwise emits the flat single-path form:
        {"joins": ..., "filters": ..., "anti_joins": ...}
    """
    if result is None:
        return ""

    if result.constraint_groups:
        paths = [_branch_to_dict(g) for g in result.constraint_groups]
        paths = [p for p in paths if p]
        if paths:
            hint: dict = {"paths": paths}
            if result.constraint_groups_truncated:
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


def _build_eval_context(state, existing_tests: list) -> str:
    """Build a context block for the generator when called after a bad_data evaluation."""
    if state.get("evaluation_feedback") != "bad_data":
        return ""

    eval_msgs = [
        m
        for m in state.get("messages", [])
        if get_message_type(m) == MsgType.EVALUATION
    ]
    if not eval_msgs:
        return ""

    latest_eval = eval_msgs[-1]
    eval_test_idx = latest_eval.additional_kwargs.get("test_index")
    verdict_text = latest_eval.content

    failing_test = next(
        (t for t in existing_tests if str(t.get("test_index")) == str(eval_test_idx)),
        None,
    )

    lines = [
        "\n⚠️ **Contexte de correction** — Ce test a été jugé Insuffisant (données incorrectes).",
        f"\n**Verdict de l'évaluateur :**\n{verdict_text}\n",
    ]

    if failing_test:
        input_data = failing_test.get("data", {})
        results_json = failing_test.get("results_json", "[]")
        assertion_results = failing_test.get("assertion_results", [])

        if input_data:
            try:
                input_summary = json.dumps(input_data, ensure_ascii=False, indent=2)
            except Exception:
                input_summary = str(input_data)
            lines.append(
                f"**Données d'entrée actuelles (à corriger) :**\n```json\n{input_summary}\n```\n"
            )

        if results_json and results_json != "[]":
            try:
                parsed = (
                    json.loads(results_json)
                    if isinstance(results_json, str)
                    else results_json
                )
                results_summary = json.dumps(parsed[:10], ensure_ascii=False, indent=2)
            except Exception:
                results_summary = str(results_json)[:500]
            lines.append(
                f"**Sortie DuckDB obtenue :**\n```json\n{results_summary}\n```\n"
            )
        else:
            lines.append("**Sortie DuckDB obtenue :** vide (0 lignes)\n")

        failing_assertions = [
            a for a in (assertion_results or []) if a.get("status") != "pass"
        ]
        if failing_assertions:
            try:
                assertions_summary = json.dumps(
                    failing_assertions, ensure_ascii=False, indent=2
                )
            except Exception:
                assertions_summary = str(failing_assertions)[:500]
            lines.append(
                f"**Assertions en échec :**\n```json\n{assertions_summary}\n```\n"
            )

    lines.append(
        "**Ta mission :** génère de nouvelles données d'entrée qui corrigent exactement le problème identifié ci-dessus.\n"
    )
    return "\n".join(lines)


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
    Assigns a short stable test_uid to any test case that lacks one (lazy migration).
    """
    # 1. In-pipeline RESULTS (executor ran this cycle but saver hasn't run yet)
    results_msgs = [
        m for m in state.get("messages", []) if get_message_type(m) == MsgType.RESULTS
    ]
    if results_msgs:
        try:
            data = json.loads(results_msgs[-1].content)
            if isinstance(data, list) and data:
                _ensure_test_uids(data)
                return data
        except Exception:
            pass

    # 2. Filesystem storage
    from storage.test_repository import get_test, update_test

    test = get_test(session_id)
    if test and isinstance(test.get("test_cases"), list):
        cases = test["test_cases"]
        if _ensure_test_uids(cases):
            update_test(session_id, {"test_cases": cases})
        return cases
    return []


def _ensure_test_uids(cases: list) -> bool:
    """Assign test_uid to any case that lacks one. Returns True if any uid was assigned."""
    changed = False
    for tc in cases:
        if not tc.get("test_uid"):
            tc["test_uid"] = uuid.uuid4().hex[:4]
            changed = True
    return changed


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

    if state.get("agent_message_id"):
        parent = state["agent_message_id"]
    elif state.get("input", "").strip():
        parent = state["user_message_id"]
    elif state.get("status") == "empty_results" and state.get("messages"):
        parent = state["messages"][-1].id
    else:
        parent = state.get("parent_message_id") or state.get("user_message_id")

    try:
        generated_test, generated_test_index = await generate_examples_(
            state, used_columns, existing_tests, history
        )
    except ValueError as exc:
        error_msg = str(exc)
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
            "error": error_msg,
            "status": "error",
        }
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

    # Fail fast if the query has an unsatisfiable HAVING threshold
    from build_query.constraint_simplifier import check_having_cardinality

    check_having_cardinality(optimized_sql, dialect)

    # Single simplify() call — result reused for hint + mandatory set + unconstrained
    sim_result = _run_simplify(optimized_sql, schema=schema, dialect=dialect)
    constraints = _simplification_to_hint(sim_result)

    logger.debug("[generator] constraints_hint: %s", constraints or "(empty)")
    if sim_result is not None:
        logger.debug(
            "[generator] sim_result.source_columns: %s",
            [
                (str(r), [str(c) for c in cs])
                for r, cs in sim_result.source_columns.items()
            ],
        )

    filtered_schema = filter_columns(schema, used_columns)

    # Compute Faker-eligible columns only when constraint extraction fully succeeded
    # and all ColumnRefs resolved to known base tables (no silent lineage failure).
    # UNNEST queries are skipped: array-of-struct constraints are not reliably captured.
    # Faker is also disabled on retry (empty_results) — inconsistent data may have been
    # caused by Faker-filled values conflicting with LLM-generated values.
    base_tables = {entry["table"].lower() for entry in used_columns}
    faker_cols: dict[str, set[str]] = {}
    _has_unnest = "unnest" in optimized_sql.lower()
    _is_retry = state.get("status") == "empty_results"
    logger.debug(
        "[generator] _has_unnest=%s  _is_retry=%s  sim_result=%s",
        _has_unnest,
        _is_retry,
        sim_result is not None,
    )
    if (
        sim_result is not None
        and not _has_unnest
        and not _is_retry
        and _all_refs_resolved(sim_result, base_tables)
    ):
        faker_cols = _compute_faker_columns(sim_result, used_columns, base_tables)

    logger.debug(
        "[generator] faker_cols: %s", {k: list(v) for k, v in faker_cols.items()}
    )

    # Build LLM schema with Faker-eligible columns removed to shrink the prompt
    llm_filtered_schema = filtered_schema
    excluded_col_names: list[str] = []
    if faker_cols:
        llm_filtered_schema = []
        for table_entry in filtered_schema:
            uc_key = table_entry["table_name"]
            if uc_key not in faker_cols:
                llm_filtered_schema.append(table_entry)
            else:
                remaining = [
                    c
                    for c in table_entry["columns"]
                    if c["name"].lower() not in faker_cols[uc_key]
                ]
                if remaining:
                    llm_filtered_schema.append({**table_entry, "columns": remaining})
                excluded_col_names.extend(f"{uc_key}.{c}" for c in faker_cols[uc_key])
        logger.debug(
            "[generator] Faker pre-fill: %d col(s) across %d table(s) removed from LLM schema — %s",
            sum(len(cols) for cols in faker_cols.values()),
            len(faker_cols),
            excluded_col_names,
        )

    data_model = create_pydantic_models(llm_filtered_schema)
    output_type = get_generation_output_type(data_model, existing_tests)
    parser = create_output_fixing_parser(
        PydanticOutputParser(pydantic_object=output_type)
    )

    eval_context = _build_eval_context(state, existing_tests)

    prompt = await create_appropriate_prompt(
        state,
        existing_tests,
        history,
        used_columns,
        parser.get_format_instructions(),
        constraints_hint=constraints,
        excluded_columns=excluded_col_names,
        eval_context=eval_context,
    )
    if prompt is None:
        return None, None

    logger.diag("\n%s", "=" * 60)
    logger.diag(
        "[generator] constraints_hint:\n%s",
        constraints or "(vide — sous-requêtes corrélées non capturées ?)",
    )
    logger.diag(
        "[generator] faker_cols: %s", {k: list(v) for k, v in faker_cols.items()}
    )
    try:
        formatted_msgs = prompt.format_messages()
        logger.diag(
            "[generator] PROMPT LLM (dernier message):\n%s",
            formatted_msgs[-1].content[:3000],
        )
    except Exception:
        logger.diag(
            "[generator] PROMPT LLM (template):\n%s", str(prompt.messages[-1])[:3000]
        )
    logger.diag("%s\n", "=" * 60)

    llm = make_llm()
    generated_data = await (prompt | llm | parser).ainvoke({})

    filled_data = _convert_datetime_fields(generated_data.data.dict())

    logger.diag("[generator] données générées par le LLM:")
    for table_name, rows in filled_data.items():
        logger.diag(
            "  %s: %s ligne(s)",
            table_name,
            len(rows) if isinstance(rows, list) else "?",
        )

    # Merge Faker-generated values into LLM output
    if faker_cols:
        faker_data = generate_faker_rows(
            schema, faker_cols, filled_data, profile=state.get("profile")
        )
        for uc_key, faker_rows in faker_data.items():
            llm_rows = filled_data.get(uc_key) or []
            if llm_rows:
                filled_data[uc_key] = [
                    {**(row or {}), **faker_row}
                    for row, faker_row in zip(llm_rows, faker_rows)
                ]
            else:
                filled_data[uc_key] = faker_rows

    generated = {
        "test_name": generated_data.test_name,
        "unit_test_description": generated_data.unit_test_description,
        "unit_test_build_reasoning": generated_data.unit_test_build_reasoning,
        "tags": generated_data.tags,
        "data": filled_data,
    }

    # Determine which test_index slot this new test occupies
    target_key = _resolve_target_key(state, existing_tests)
    if target_key is not None:
        test_index = target_key
        # Preserve the existing test_uid so the frontend keeps the same reference
        existing_tc = next(
            (t for t in existing_tests if str(t.get("test_index")) == str(target_key)),
            None,
        )
        test_uid = (existing_tc or {}).get("test_uid") or uuid.uuid4().hex[:4]
    else:
        test_index = str(len(existing_tests) + 1)
        test_uid = uuid.uuid4().hex[:4]

    return {**generated, "test_index": test_index, "test_uid": test_uid}, test_index


def get_generation_output_type(data_model, existing_tests):
    reasoning_desc = (
        "Avant de générer le JSON final, simulez mentalement la traversée des données à travers chaque CTE et filtre "
        "du SQL : listez les clauses structurelles présentes (OFFSET, LIMIT, RANK, ROW_NUMBER, JOIN restrictifs), "
        "indiquez combien de lignes doivent survivre à chaque étape, et expliquez comment vos données le garantissent. "
        "Précisez ensuite la modification apportée par rapport aux données existantes."
        if existing_tests
        else "Avant de générer le JSON final, simulez mentalement la traversée des données à travers chaque CTE et filtre "
        "du SQL : listez les clauses structurelles présentes (OFFSET, LIMIT, RANK, ROW_NUMBER, JOIN restrictifs), "
        "indiquez combien de lignes doivent survivre à chaque étape, et expliquez comment vos données le garantissent."
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
    eval_context: str = "",
):
    sql = state.get("optimized_sql", "")
    dialect = state.get("dialect", "bigquery")
    profile = state.get("profile")
    stripped_sql = _strip_unconstrained_from_sql(sql, excluded_columns or [], dialect)
    model_context = state.get("model_context") or ""
    if not existing_tests:
        return generate_data_prompt(
            history,
            dialect,
            format_instructions,
            used_columns,
            constraints_hint=constraints_hint,
            sql=stripped_sql,
            profile=profile,
            model_context=model_context,
            eval_context=eval_context,
        )
    elif state.get("input", "").strip():
        if state.get("test_index") is not None:
            idx = state["test_index"]
            existing_test = (
                existing_tests[idx] if 0 <= idx < len(existing_tests) else None
            )
            return update_data_prompt(
                history,
                state["input"],
                dialect,
                format_instructions,
                sql=sql,
                existing_test=existing_test,
                model_context=model_context,
                eval_context=eval_context,
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
            model_context=model_context,
            eval_context=eval_context,
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
            model_context=model_context,
        )
    else:
        return None


def _all_refs_resolved(sim_result, base_tables: set[str]) -> bool:
    """Return True iff every ColumnRef in sim_result maps to a known base table.

    A ColumnRef whose table is NOT in base_tables indicates that lineage resolution
    silently fell back to an unresolved CTE alias — in that case Faker must not be
    activated because we cannot tell which base-table columns are constrained.
    """
    all_refs = (
        list(sim_result.source_columns.keys())
        + list(sim_result.derived_columns.keys())
        + [ref for eq_class in sim_result.equivalence_classes for ref in eq_class]
    )
    return all(ref.table.lower() in base_tables for ref in all_refs)


def _compute_faker_columns(
    sim_result, used_columns: list, base_tables: set[str]
) -> dict[str, set[str]]:
    """Return {uc_key: {col_names}} for columns safe to Faker-fill.

    Only called when sim_result is not None and _all_refs_resolved() is True.
    uc_key matches the table_name produced by filter_columns() (database_table).
    """
    constrained: set[tuple[str, str]] = set()
    for ref in sim_result.source_columns:
        constrained.add((ref.table.lower(), ref.column.lower()))
    for ref in sim_result.derived_columns:
        constrained.add((ref.table.lower(), ref.column.lower()))
    for eq_class in sim_result.equivalence_classes:
        for ref in eq_class:
            constrained.add((ref.table.lower(), ref.column.lower()))

    # If the simplifier found no constraints at all (e.g. filters inside an anonymous
    # subquery that it can't propagate), don't Faker-fill anything — the LLM sees the
    # full SQL and will respect the WHERE clause on its own.
    if not constrained:
        logger.debug(
            "[faker] source_columns empty — skipping Faker fill, delegating to LLM"
        )
        return {}

    faker_cols: dict[str, set[str]] = {}
    for entry in used_columns:
        db = entry.get("database", "")
        table = entry["table"]
        uc_key = f"{db}_{table}" if db else table
        table_lower = table.lower()
        for col in entry["used_columns"]:
            if (table_lower, col.lower()) not in constrained:
                faker_cols.setdefault(uc_key, set()).add(col.lower())
    return faker_cols


async def create_combined_model(used_columns, schemas):
    filtered_columns = filter_columns(schemas, used_columns)
    return create_pydantic_models(filtered_columns)
