import json
import logging
import uuid
from datetime import datetime, date
from typing import List, Optional

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.messages import AIMessage, HumanMessage
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
from storage.config import get_llm_model, is_native_thinking_active
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
_hint_cache: dict[tuple[str, str], str] = {}
_output_type_cache: dict[tuple[str, bool, bool], object] = {}
_SIMPLIFY_CACHE_MAXSIZE = 64

# Budget de la chaîne `conditions` du hint (P2b). Au-delà, le LLM ne lit plus le
# bloc et `<query>` fait autorité : on tronque avec un renvoi explicite vers elle.
_CONDITIONS_MAX_CHARS = 2000


def _serialize_hint(hint: dict | None) -> str:
    """Sérialise le hint de contraintes en JSON, en tronquant `conditions` au-delà
    du budget (P2b). La troncature se fait sur la *valeur* (chaîne), pas sur le JSON
    global, pour ne pas casser les autres clés (`anti_joins`, `format_constraints`…)."""
    if not hint:
        return ""
    conditions = hint.get("conditions")
    if isinstance(conditions, str) and len(conditions) > _CONDITIONS_MAX_CHARS:
        hint = {
            **hint,
            "conditions": conditions[:_CONDITIONS_MAX_CHARS]
            + " … (tronqué — voir <query>)",
        }
    return json.dumps(hint, ensure_ascii=False, indent=2)


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
    # Bare columns: referenced in WHERE/JOIN ON/QUALIFY inside complex expressions
    # (e.g. UPPER(col) * 2) — no extractable constraint, but must not be Faker-filled.
    bare = [_col_str_join(col) for col, cs in result.source_columns.items() if not cs]
    d: dict = {}
    if joins:
        d["joins"] = joins
    if anti_joins:
        d["anti_joins"] = anti_joins
    if filters:
        d["filters"] = filters
    if bare:
        d["referenced"] = bare
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


def _simplification_to_hint(
    result,
    sql: str | None = None,
    dialect: str = "bigquery",
    schema: list[dict] | None = None,
) -> str:
    """Build the LLM constraints hint string.

    Uses ``build_conditions_hint`` from constraint_simplifier to produce a
    single ``{"conditions": "...", "format_constraints": [...]}`` dict that
    preserves AND/OR structure without DNF expansion.

    Falls back to an empty string when SQL is unavailable or the hint is empty.

    Result is cached by ``(sql, dialect)``: ``build_conditions_hint`` re-parses and
    re-simplifies the SQL (several seconds on complex queries), but the output is
    deterministic, so retries on the same SQL must not recompute it.
    """
    if sql:
        cache_key = (sql, dialect)
        cached = _hint_cache.get(cache_key)
        if cached is not None:
            return cached

        from build_query.constraint_simplifier import build_conditions_hint

        hint = build_conditions_hint(sql, dialect=dialect, schema=schema)
        result_str = _serialize_hint(hint)
        if len(_hint_cache) >= _SIMPLIFY_CACHE_MAXSIZE:
            _hint_cache.pop(next(iter(_hint_cache)))
        _hint_cache[cache_key] = result_str
        return result_str
    return ""


def _run_simplify_and_hint(
    sql: str, schema: list[dict] | None = None, dialect: str = "bigquery"
):
    """Return ``(SimplificationResult|None, hint_str)`` sharing one parse + qualify pass.

    Equivalent to calling ``_run_simplify`` then ``_simplification_to_hint`` but
    ``simplify_with_hint`` reuses a single qualified scope map across both, halving the
    (expensive) qualify cost on wide queries. Populates ``_simplify_cache`` and
    ``_hint_cache`` so retries on the same SQL stay free, just like the split path.
    """
    if not sql:
        return None, ""
    cache_key = (sql, dialect)
    sim_cached = _simplify_cache.get(cache_key)
    hint_cached = _hint_cache.get(cache_key)
    if sim_cached is not None and hint_cached is not None:
        return sim_cached, hint_cached

    try:
        from build_query.constraint_simplifier import simplify_with_hint

        sim_result, hint = simplify_with_hint(sql, dialect=dialect, schema=schema)
    except Exception as exc:
        logger.warning(
            "constraint_simplifier failed (sql_hash=%s dialect=%s): %s",
            hash(sql),
            dialect,
            exc,
            exc_info=True,
        )
        sim_result, hint = None, {}

    hint_str = _serialize_hint(hint)
    if len(_simplify_cache) >= _SIMPLIFY_CACHE_MAXSIZE:
        _simplify_cache.pop(next(iter(_simplify_cache)))
    _simplify_cache[cache_key] = sim_result
    if len(_hint_cache) >= _SIMPLIFY_CACHE_MAXSIZE:
        _hint_cache.pop(next(iter(_hint_cache)))
    _hint_cache[cache_key] = hint_str
    return sim_result, hint_str


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
        hint = _simplification_to_hint(sim, sql=cte["code"], dialect=dialect)
        if hint:
            result_map[cte["name"]] = json.loads(hint)
    return result_map


def _build_eval_messages(state, existing_tests: list) -> list:
    """Build few-shot messages for the generator when called after a bad_data evaluation.

    Returns alternating [AIMessage(past_attempt_json), HumanMessage(duckdb_feedback), ...]
    pairs to be inserted after the initial generation request in the conversation. The LLM
    sees exactly what it generated, what DuckDB returned, and why it failed — in the native
    human/ai conversation format rather than as a text description in the system prompt.
    """
    if state.get("evaluation_feedback") != "bad_data":
        return []

    messages = state.get("messages", [])

    eval_msgs = [m for m in messages if get_message_type(m) == MsgType.EVALUATION]
    if not eval_msgs:
        return []

    target_test_idx = eval_msgs[-1].additional_kwargs.get("test_index")
    result_msgs_out = []
    results_map = {m.id: m for m in messages if get_message_type(m) == MsgType.RESULTS}

    for eval_msg in eval_msgs:
        if eval_msg.additional_kwargs.get("test_index") != target_test_idx:
            continue

        parent_id = eval_msg.additional_kwargs.get("parent")
        result_msg = results_map.get(parent_id)
        if not result_msg:
            continue

        try:
            results_data = json.loads(result_msg.content)
            if not isinstance(results_data, list):
                results_data = [results_data]

            test_case = next(
                (
                    t
                    for t in results_data
                    if str(t.get("test_index")) == str(target_test_idx)
                ),
                None,
            )
            if not test_case:
                continue

            # AI message: reproduce the exact JSON the generator produced so the LLM
            # sees its own previous output in its native output format (few-shot)
            ai_content = json.dumps(
                {
                    "unit_test_build_reasoning": test_case.get(
                        "unit_test_build_reasoning", ""
                    ),
                    "test_name": test_case.get("test_name", ""),
                    "unit_test_description": test_case.get("unit_test_description", ""),
                    "tags": test_case.get("tags", []),
                    "data": test_case.get("data", {}),
                },
                ensure_ascii=False,
                indent=2,
            )

            # Human feedback: actual DuckDB output (no truncation) + verdict
            status = test_case.get("status", "")
            if status == "empty_results":
                failing_cte = test_case.get("failing_cte", "")
                cte_trace = test_case.get("cte_trace", {})
                if failing_cte and cte_trace:
                    # Trace niveau-étape : pointe la clause EXACTE qui bloque. Sans elle,
                    # le LLM ignore lequel des joins/filtres de la CTE est coupable, devine
                    # (souvent à tort) et reboucle à l'identique.
                    output_str = _format_cte_trace_hint(failing_cte, cte_trace)
                elif failing_cte:
                    output_str = f"0 lignes — CTE bloquante : `{failing_cte}`"
                else:
                    output_str = "0 lignes retournées"
                # Fix ciblé sur l'étape bloquante, pas un thrashing « tout refaire ».
                next_step = (
                    "Corrige précisément l'étape bloquante identifiée ci-dessus (pas "
                    "l'ensemble du test) : ajuste les données des tables qui l'alimentent. "
                    "Si l'étape bloquante est un anti-join (`… IS NULL` sur une clé jointe), "
                    "génère des données qui NE matchent PAS la table anti-jointe."
                )
            else:
                raw_output = test_case.get("results_json", "[]")
                try:
                    parsed = (
                        json.loads(raw_output)
                        if isinstance(raw_output, str)
                        else raw_output
                    )
                    output_str = json.dumps(parsed, ensure_ascii=False, indent=2)
                except Exception:
                    output_str = str(raw_output)
                next_step = "Génère une nouvelle version avec une approche structurellement différente."

            raw_verdict = eval_msg.content
            verdict_label = "Insuffisant"
            for label in ("Excellent", "Bon", "Insuffisant"):
                if raw_verdict.startswith(f"**{label}**"):
                    verdict_label = label
                    break
            verdict_detail = raw_verdict.replace(f"**{verdict_label}** — ", "").strip()

            human_feedback = (
                f"Résultat DuckDB :\n{output_str}\n\n"
                f"Verdict : {verdict_label} — {verdict_detail}\n\n"
                f"{next_step}"
            )

            result_msgs_out.append(AIMessage(content=ai_content))
            result_msgs_out.append(HumanMessage(content=human_feedback))

        except Exception:
            pass

    return result_msgs_out


def _get_failing_cte_from_results(messages) -> tuple:
    """Scans messages for the last RESULTS message that has a failing CTE."""
    for msg in reversed(messages):
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


def _format_cte_trace_hint(failing_cte: str, cte_trace: dict) -> str:
    """Format enriched CTE trace into a diagnostic block for the generator prompt."""
    lines = ["⚠️ **Diagnostic DuckDB (tentative précédente) :**"]
    # Les CTEs situées APRÈS la CTE bloquante ciblée ne font que propager son
    # 0-ligne (cascades de « 0 ligne — filtre bloquant » et erreurs d'exécution
    # avec leur dump SQL) : repliées en une ligne chacune, sinon elles noient
    # le seul signal actionnable. La trace suit l'ordre de définition des CTEs.
    past_failing = False
    for cte_name, info in cte_trace.items():
        downstream = past_failing
        if cte_name == failing_cte:
            past_failing = True
        row_count = info.get("row_count", -1)
        if row_count == -1:
            err = info.get("error", "")
            err_txt = f" — {err}" if err else ""
            if downstream:
                lines.append(
                    f"- `{cte_name}` : erreur d'exécution{err_txt} "
                    "(conséquence probable du 0-ligne amont)"
                )
                continue
            lines.append(f"- `{cte_name}` : erreur d'exécution{err_txt}")
            step_sql = info.get("sql", "")
            if step_sql:
                lines.append(f"  - SQL de l'étape : `{step_sql[:400]}`")
            continue
        if downstream and info.get("blocking", row_count == 0):
            lines.append(
                f"- `{cte_name}` : 0 ligne(s) (conséquence du 0-ligne de "
                f"`{failing_cte}` en amont)"
            )
            continue
        # Une CTE vide n'est « bloquante » que si elle est atteignable depuis le
        # résultat final par des arêtes requises (cf. _select_failing_cte /
        # classify_blocking_ctes). Sans annotation `blocking` (anciennes traces,
        # tests), on retombe sur l'heuristique row_count == 0.
        is_blocking = info.get("blocking", row_count == 0)
        marker = " ← **0 ligne — filtre bloquant**" if is_blocking else ""
        lines.append(f"- `{cte_name}` : {row_count} ligne(s){marker}")
        steps = info.get("steps")
        if steps and is_blocking:
            # Ne montrer que la transition bloquante : dernière étape > 0 → première à 0.
            # Le reste (longues séries de "→ N ligne(s)" inchangées) est du bruit qui
            # noie le seul signal utile.
            blocker_idx = next(
                (i for i, s in enumerate(steps) if s.get("count", -1) == 0), None
            )
            if blocker_idx is not None:
                prev = steps[blocker_idx - 1] if blocker_idx > 0 else None
                if prev:
                    lines.append(
                        f"  - {prev.get('label', '?')} → {prev.get('count')} ligne(s)"
                    )
                blk = steps[blocker_idx]
                lines.append(
                    f"  - {blk.get('label', '?')} → 0 ligne(s) ← **étape bloquante**"
                )
            else:
                # Pas de transition à 0 identifiée : retomber sur la liste complète.
                for step in steps:
                    cnt = step.get("count", -1)
                    zero_marker = " ← filtre actif ici" if cnt == 0 else ""
                    lines.append(
                        f"  - {step.get('label', '?')} → {cnt} ligne(s){zero_marker}"
                    )
        # Décomposition par prédicat des JOINs (étiquette cumulative pas fiable
        # quand le ON porte plusieurs prédicats) : nomme le prédicat fautif avec
        # les ensembles de valeurs des deux côtés.
        join_breakdown = info.get("join_breakdown")
        if join_breakdown and is_blocking:
            for bl in join_breakdown:
                lines.append(f"  - {bl}")
    lines.append(
        f"\nLa CTE `{failing_cte}` produit 0 ligne, bloquée à l'étape ci-dessus. "
        "Si l'étape bloquante est un anti-join (`… IS NULL` sur une clé jointe), génère "
        "des données qui NE matchent PAS la table anti-jointe ; sinon, ajuste les données "
        "pour satisfaire ce filtre précis."
    )
    return "\n".join(lines)


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


def _build_understanding_payload(state, used_columns: list) -> Optional[dict]:
    """Build the "query understanding" card payload (tables, columns, constraints,
    derived expressions).

    Best-effort — returns ``None`` on any failure so the card is simply omitted and
    never blocks generation. ``_run_simplify`` is cached by ``(sql, dialect)`` so the
    call here reuses the result already computed in ``generate_examples_``.
    """
    try:
        dialect = state.get("dialect", "bigquery")
        optimized_sql = state.get("optimized_sql", "") or state.get("query", "")

        tables = [
            {
                "database": entry.get("database", ""),
                "table": entry.get("table", ""),
                "columns": list(entry.get("used_columns", []) or []),
            }
            for entry in used_columns
            if entry.get("table")
        ]

        constraints: dict = {}
        sim_result = _run_simplify(optimized_sql, dialect=dialect)
        if sim_result is not None:
            constraints = _branch_to_dict(sim_result)

        derived: list = []
        try:
            from build_query.constraint_simplifier import (
                detect_select_derived_expressions,
            )

            for e in detect_select_derived_expressions(optimized_sql, dialect)[:8]:
                derived.append(
                    {
                        "expr": e.get("expr_sql", ""),
                        "source_tables": e.get("source_tables", []),
                    }
                )
        except Exception:
            pass  # derived expressions are a bonus — never fail the card on them

        if not tables and not constraints and not derived:
            return None
        return {
            "tables": tables,
            "constraints": constraints,
            "derived_expressions": derived,
            "optimized_sql": optimized_sql,
        }
    except Exception:
        logger.debug("[generator] understanding payload build failed", exc_info=True)
        return None


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
        raw = str(exc)
        # OutputParserException messages include the full LLM completion JSON, which
        # is very long and not useful to display. Detect and replace with a short message.
        if "OUTPUT_PARSING_FAILURE" in raw or "Failed to parse" in raw:
            error_msg = "Le modèle n'a pas pu générer un test valide (erreur de format LLM). Réessaie ou reformule ta demande."
        else:
            error_msg = raw
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
    result: dict = {
        "examples": [
            AIMessage(
                content=json.dumps(generated_test),
                id=str(uuid.uuid4()),
                additional_kwargs=examples_kwargs,
            )
        ]
    }

    # Boucle bad_data : consigne la régénération complète dans le ledger des
    # tentatives, au même titre qu'un lot de patches (le round suivant doit savoir
    # qu'un regen a déjà été tenté sans effet).
    if state.get("evaluation_feedback") == "bad_data":
        from build_query.data_patcher import append_correction_attempt

        result["correction_attempts"] = append_correction_attempt(
            state, state.get("test_uid"), [{"tool": "regen"}]
        )

    # Emit the "query understanding" card on the very first generation only.
    # Skip retries (which re-enter this node with status == "empty_results") AND
    # subsequent add-a-test runs (suggestion clicks, chat edits) — those already
    # have existing tests, so re-emitting the card would duplicate it in the
    # thread. Best-effort: the card is a bonus and must never break generation.
    if state.get("status") != "empty_results" and not existing_tests:
        understanding = _build_understanding_payload(state, used_columns)
        if understanding is not None:
            result["messages"] = [
                AIMessage(
                    content=json.dumps(understanding),
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.QUERY_UNDERSTANDING,
                        "parent": parent,
                        "request_id": state.get("request_id"),
                    },
                )
            ]

    return result


def _next_test_index(existing_tests: list) -> str:
    """Return the next test_index slot, collision-free.

    Uses ``max(existing)+1`` rather than ``len+1``: delete_test_node does NOT
    renumber, so ``len+1`` can land on an index that is still taken (e.g. after
    deleting "1" from ["1","2"], ``len+1`` == "2" → would overwrite the survivor
    at merge time). max+1 keeps test_index a UNIQUE, stable order/display slot.
    """
    existing_indices = [
        int(ti)
        for t in existing_tests
        if str(ti := t.get("test_index", "")).lstrip("-").isdigit()
    ]
    return str((max(existing_indices) if existing_indices else 0) + 1)


def _resolve_target_key(state, existing_list: list) -> Optional[str]:
    """
    Return the test_index to overwrite, or None to create a new test.
    Only overwrite if the frontend/agent explicitly passed a test_index that
    matches an existing test, or during a retry.

    Targeting is keyed on ``test_uid`` (stable random hash, the identity the
    frontend and agent speak). ``test_index`` (a slot/order number) is only used
    as a legacy fallback: it is matched BY VALUE against each test's stored
    ``test_index``, never as a positional offset into ``existing_list`` (the two
    conventions disagree — labels are 1-based, list positions 0-based — and
    conflating them mis-targeted tests off-by-one or out of range).
    """
    test_uid = state.get("test_uid")
    if test_uid:
        match = next((t for t in existing_list if t.get("test_uid") == test_uid), None)
        if match is not None:
            return match["test_index"]
    test_index = state.get("test_index")
    if test_index is not None:
        match = next(
            (t for t in existing_list if str(t.get("test_index")) == str(test_index)),
            None,
        )
        if match is not None:
            return match["test_index"]
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
    from utils.timing import timed

    check_having_cardinality(optimized_sql, dialect)

    # Single shared parse + qualify for both the SimplificationResult (reused for the
    # mandatory set + unconstrained columns) and the conditions hint.
    with timed("gen:simplify+hint"):
        sim_result, constraints = _run_simplify_and_hint(
            optimized_sql, schema=schema, dialect=dialect
        )

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
        faker_cols = _compute_faker_columns(
            sim_result, used_columns, base_tables, sql=optimized_sql, dialect=dialect
        )

    logger.debug(
        "[generator] faker_cols: %s", {k: list(v) for k, v in faker_cols.items()}
    )

    # Precompute constraints per column
    col_hints = {}
    if sim_result is not None:
        table_to_uc = {}
        for entry in used_columns:
            db = entry.get("database", "")
            table = entry["table"]
            uc_key = f"{db}_{table}" if db else table
            table_to_uc[table.lower()] = uc_key

        for ref, constraints_list in sim_result.source_columns.items():
            if not constraints_list:
                continue
            table_lower = ref.table.lower()
            col_lower = ref.column.lower()
            uc_key = table_to_uc.get(table_lower)
            if uc_key:
                hints = _format_filter_constraints(constraints_list)
                if hints:
                    col_hints.setdefault((uc_key, col_lower), []).extend(hints)

        for eq_class in sim_result.equivalence_classes:
            refs = list(eq_class)
            joined = " = ".join(f"{r.table}.{r.column}" for r in refs)
            for ref in refs:
                uc_key = table_to_uc.get(ref.table.lower())
                if uc_key:
                    col_hints.setdefault((uc_key, ref.column.lower()), []).append(
                        f"Égalité stricte avec {joined}"
                    )

        for col_a, col_b in sim_result.col_inequalities:
            for ref, other in [(col_a, col_b), (col_b, col_a)]:
                uc_key = table_to_uc.get(ref.table.lower())
                if uc_key:
                    col_hints.setdefault((uc_key, ref.column.lower()), []).append(
                        f"Anti-join (NOT IN) avec {other.table}.{other.column}"
                    )

    # Build LLM schema with Faker-eligible columns removed and constraint hints injected
    llm_filtered_schema = []
    excluded_col_names: list[str] = []
    for table_entry in filtered_schema:
        uc_key = table_entry["table_name"]
        new_columns = []
        for c in table_entry["columns"]:
            col_name = c["name"].lower()
            # Skip if Faker will fill this
            if faker_cols and uc_key in faker_cols and col_name in faker_cols[uc_key]:
                excluded_col_names.append(f"{uc_key}.{col_name}")
                continue

            c_copy = dict(c)
            hints = col_hints.get((uc_key, col_name))
            if hints:
                hint_str = " | ATTENTION contrainte SQL : " + " ; ".join(hints)
                existing_desc = c_copy.get("description")
                if existing_desc:
                    c_copy["description"] = str(existing_desc) + hint_str
                else:
                    c_copy["description"] = hint_str.lstrip(" | ")
            new_columns.append(c_copy)

        if new_columns:
            llm_filtered_schema.append({**table_entry, "columns": new_columns})

    if faker_cols:
        logger.debug(
            "[generator] Faker pre-fill: %d col(s) across %d table(s) removed from LLM schema — %s",
            sum(len(cols) for cols in faker_cols.values()),
            len(faker_cols),
            excluded_col_names,
        )

    # Modèle recommandé (flash/pro) : le thinking natif porte le raisonnement →
    # le champ in-schema n'est qu'une justification brève. Sinon, fallback sur un
    # CoT in-schema complet (fonctionnel mais plus lent + risque de troncature) :
    # on prévient l'ingé pour qu'il bascule sur flash/pro.
    native_thinking = is_native_thinking_active()
    if not native_thinking:
        logger.warning(
            "[generator] thinking natif inactif (modèle=%s) — MockSQL est optimisé "
            "pour gemini-2.5-flash/pro avec thinking activé. Le raisonnement est "
            "généré dans le JSON (plus lent, risque de troncature sur requêtes "
            "complexes). Bascule sur flash/pro pour de meilleures performances.",
            get_llm_model(),
        )

    with timed("gen:pydantic"):
        # The dynamic Pydantic model depends only on the (constraint-annotated)
        # schema, whether tests already exist, and the reasoning mode — all stable
        # across retries on the same SQL. Building it costs ~1s on wide schemas, so cache it.
        model_key = (
            json.dumps(llm_filtered_schema, sort_keys=True, default=str),
            bool(existing_tests),
            native_thinking,
        )
        output_type = _output_type_cache.get(model_key)
        if output_type is None:
            data_model = create_pydantic_models(llm_filtered_schema)
            output_type = get_generation_output_type(
                data_model, existing_tests, native_thinking=native_thinking
            )
            if len(_output_type_cache) >= _SIMPLIFY_CACHE_MAXSIZE:
                _output_type_cache.pop(next(iter(_output_type_cache)))
            _output_type_cache[model_key] = output_type
        raw_parser = PydanticOutputParser(pydantic_object=output_type)
        parser = create_output_fixing_parser(raw_parser)
        format_instructions = raw_parser.get_format_instructions()

    with timed("gen:prompt_build"):
        eval_history = _build_eval_messages(state, existing_tests)

        # Recettes de jointure pré-calculées (clés dérivées) : inversion CASE,
        # vérification forward DuckDB, format CAST. Mises en cache par (sql, dialect).
        from build_query.join_recipes import build_join_recipes_block

        join_recipes_block = build_join_recipes_block(
            optimized_sql, dialect=dialect, schema=schema
        )
        if join_recipes_block:
            logger.debug("[generator] join_recipes_block:\n%s", join_recipes_block)

        prompt = await create_appropriate_prompt(
            state,
            existing_tests,
            history,
            used_columns,
            format_instructions,
            constraints_hint=constraints,
            excluded_columns=excluded_col_names,
            eval_history=eval_history,
            native_thinking=native_thinking,
            join_recipes_block=join_recipes_block,
        )
    if prompt is None:
        return None, None

    _retry_label = ""
    if state.get("evaluation_feedback") == "bad_data":
        _retry_label = f" RETRY (gen_retries={state.get('gen_retries')}, test_index={state.get('test_index')})"
    logger.diag("\n%s", "=" * 60)
    logger.diag("[generator]%s", _retry_label or " (première génération)")
    if eval_history:
        logger.diag(
            "[generator] EVAL_HISTORY injecté : %d message(s) few-shot",
            len(eval_history),
        )
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
    logger.diag("[generator] appel LLM model=%s", get_llm_model())
    try:
        generated_data = await (prompt | llm | parser).ainvoke({})
    except Exception as _parse_exc:
        logger.diag(
            "[generator] PARSING FAILURE type=%s msg=%s",
            type(_parse_exc).__name__,
            str(_parse_exc)[:500],
        )
        raise

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
        test_index = _next_test_index(existing_tests)
        test_uid = uuid.uuid4().hex[:4]

    return {**generated, "test_index": test_index, "test_uid": test_uid}, test_index


def get_generation_output_type(
    data_model, existing_tests, native_thinking: bool = False
):
    if native_thinking:
        # Le raisonnement détaillé est fait nativement (canal thinking) en amont :
        # ce champ ne porte qu'une justification courte, persistée et réutilisée
        # en aval (few-shot, suggestions). 1 phrase → coût output négligeable,
        # pas de risque de troncature du JSON.
        reasoning_desc = (
            "**1 phrase maximum.** Justification courte du scénario : quelle clause "
            "structurelle du SQL est ciblée et pourquoi les données la satisfont."
        )
    else:
        # Pas de thinking natif : ce champ est le SEUL chain-of-thought disponible.
        # Capé à 3 phrases pour rester sous la limite de tokens output (sinon le
        # JSON peut être tronqué après ce champ sur requêtes complexes).
        reasoning_desc = (
            "**3 phrases maximum.** Simulez mentalement la traversée des données à travers chaque CTE et filtre "
            "du SQL : citez les clauses structurelles présentes (OFFSET, LIMIT, RANK, ROW_NUMBER, JOIN restrictifs), "
            "indiquez combien de lignes doivent survivre à chaque étape, et précisez la modification apportée par "
            "rapport aux données existantes."
            if existing_tests
            else "**3 phrases maximum.** Simulez mentalement la traversée des données à travers chaque CTE et filtre "
            "du SQL : citez les clauses structurelles présentes (OFFSET, LIMIT, RANK, ROW_NUMBER, JOIN restrictifs) "
            "et indiquez combien de lignes doivent survivre à chaque étape."
        )

    return create_model(
        "UnitTestData",
        unit_test_build_reasoning=(str, Field(description=reasoning_desc)),
        test_name=(
            str,
            Field(
                description=(
                    "Nom court du scénario (3-6 mots) destiné à un lecteur métier, sans jargon SQL ni noms techniques. "
                    "✓ Bons exemples : 'Commandes actives France', 'Client sans historique', 'Ventes nulles juillet'. "
                    "✗ À proscrire : 'CTE orders_filtered vide', 'JOIN sur user_id NULL', 'WHERE status active'."
                )
            ),
        ),
        unit_test_description=(
            str,
            Field(
                description=(
                    "Description métier contextualisée au format "
                    '"Pour [sujet avec valeurs concrètes : IDs, dates, montants, statuts] '
                    '[condition] → [résultat attendu]". Destinée à un responsable métier non-développeur. '
                    "Nommer la branche choisie quand le SQL a des alternatives (OR, CASE, UNION). "
                    "✓ Bons exemples : "
                    "'Pour un client sans commande sur janvier → son chiffre d'affaires est nul.' "
                    "'Pour le porteur COLLAB789 (banque 001) dont la carte démarre le 2026-01-15 → il est compté comme OUVERTURE sur le mois d'analyse.' "
                    "✗ À proscrire absolument — noms de colonnes SQL, noms de CTEs, syntaxe SQL, "
                    "formulation générique type 'Vérifie que le calcul est correct' : "
                    "'Vérifie que price > 0 dans la CTE orders_filtered.' "
                    "'S'assure que le LEFT JOIN sur user_id retourne NULL.'"
                )
            ),
        ),
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
    eval_history: list | None = None,
    native_thinking: bool = False,
    join_recipes_block: str = "",
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
            eval_history=eval_history,
            native_thinking=native_thinking,
            join_recipes_block=join_recipes_block,
        )
    elif state.get("input", "").strip():
        if state.get("test_uid") or state.get("test_index") is not None:
            # Sélection du test montré au LLM par la MÊME clé que _resolve_target_key :
            # test_uid en priorité (identité stable), test_index par valeur en repli.
            target_uid = state.get("test_uid")
            target_idx = (
                str(state["test_index"])
                if state.get("test_index") is not None
                else None
            )
            existing_test = next(
                (
                    t
                    for t in existing_tests
                    if (target_uid and t.get("test_uid") == target_uid)
                    or (
                        target_idx is not None
                        and str(t.get("test_index")) == target_idx
                    )
                ),
                None,
            )
            return update_data_prompt(
                history,
                state["input"],
                dialect,
                format_instructions,
                sql=sql,
                existing_test=existing_test,
                model_context=model_context,
                eval_history=eval_history,
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
            eval_history=eval_history,
            native_thinking=native_thinking,
            join_recipes_block=join_recipes_block,
        )
    elif state.get("status") == "empty_results":
        failing_cte, cte_trace = _get_failing_cte_from_results(
            state.get("messages", [])
        )
        trace_hint = (
            _format_cte_trace_hint(failing_cte, cte_trace) if failing_cte else ""
        )
        return generate_data_prompt(
            history,
            dialect,
            format_instructions,
            used_columns,
            constraints_hint=constraints_hint,
            sql=stripped_sql,
            profile=profile,
            model_context=model_context,
            trace_hint=trace_hint,
            eval_history=eval_history,
            native_thinking=native_thinking,
            join_recipes_block=join_recipes_block,
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
    # is_identity=False refs are DELIBERATELY CTE-qualified (predicate on a derived
    # column, never remapped to its base column) — not a silent fallback. Their base
    # source columns are excluded from Faker via FilterConstraint.source_columns.
    return all(ref.table.lower() in base_tables for ref in all_refs if ref.is_identity)


def _compute_faker_columns(
    sim_result,
    used_columns: list,
    base_tables: set[str],
    sql: str = "",
    dialect: str = "bigquery",
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
    # Base columns feeding a constraint kept in CTE form (is_identity=False):
    # the constraint key doesn't name them, but Faker must not fill them blindly —
    # the LLM has to pick their values so the derived expression satisfies the filter.
    for f in sim_result.filters:
        for src in f.source_columns:
            constrained.add((src.table.lower(), src.column.lower()))

    # GROUP BY columns need repeated values across rows — Faker would assign unique values
    # per row, destroying the aggregation structure (STDDEV=0, wrong counts, etc.).
    if sql:
        try:
            import sqlglot
            import sqlglot.expressions as exp

            group_by_cols: set[str] = set()
            for statement in sqlglot.parse(sql, dialect=dialect):
                if statement is None:
                    continue
                for node in statement.walk():
                    if isinstance(node, exp.Group):
                        for col in node.find_all(exp.Column):
                            group_by_cols.add(col.name.lower())
            for table in base_tables:
                for col in group_by_cols:
                    constrained.add((table, col))
        except Exception:
            pass

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
