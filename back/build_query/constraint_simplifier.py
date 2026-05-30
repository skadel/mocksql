"""
constraint_simplifier.py — SQL constraint extraction and simplification.

Public API
----------
    simplify(sql, dialect="bigquery", schema=None) -> SimplificationResult
    extract_constraints(sql, dialect, schema)       -> list[ConstraintGroup]

extract_constraints() algorithm
--------------------------------
Walks the SQL AST and returns one ConstraintGroup per independent satisfying path:

  • AND predicates   → accumulated into the current group (no new groups)
  • OR predicates    → DNF expansion → one group per AND-path (cartesian product)
  • UNION ALL        → groups from each branch are concatenated (not multiplied)
  • CTE / subquery   → cross-multiplied with the outer SELECT's groups when used
                       in a positive JOIN/FROM context; skipped for anti-joins
                       (LEFT JOIN … WHERE b IS NULL patterns).

The expansion is capped at _MAX_CONSTRAINT_GROUPS (32) to prevent exponential blowup.

simplify() algorithm
--------------------
  1. Calls extract_constraints() → list[ConstraintGroup].
  2. For each group, applies Union-Find to build source_columns / derived_columns /
     equivalence_classes.
  3. When more than one group exists, populates result.constraint_groups with one
     SimplificationResult per group, and also builds a merged flat result for compat.

Known gaps
----------
  • OR in JOIN ON clauses is not expanded (unknown which side satisfies first).
  • HAVING predicates are not captured (apply to groups, not rows).
  • Inner WHERE of IN (subquery) is not walked.
  • CASE WHEN branch conditions are not extracted.

SimplificationResult fields
----------------------------
  source_columns          dict[ColumnRef, list[FilterConstraint]]
  derived_columns         dict[ColumnRef, (source_ColumnRef, "FUNC(source)")]
  equivalence_classes     list[frozenset[ColumnRef]]
  filters                 list[FilterConstraint]        (raw, for downstream use)
  functional              list[FunctionalConstraint]    (raw)
  col_inequalities        list[(ColumnRef, ColumnRef)]  (anti-join pairs)
  constraint_groups       list[SimplificationResult]    (one per satisfying path; empty = single flat path)
  constraint_groups_truncated  bool
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.simplify import simplify as sg_simplify

import utils.logger  # noqa: F401 — registers DIAG level (15)

logger = logging.getLogger(__name__)


# ─── Data structures ──────────────────────────────────────────────────────────


@dataclass(frozen=True, order=True)
class ColumnRef:
    table: str  # alias as written in the query (e.g. "indicators_data_2")
    column: str
    # SQL lineage string tracing how this column was derived through CTEs,
    # rendered via sqlglot (e.g. "cte2.a -> cte1.y -> base.x").
    # compare=False: excluded from __eq__, __hash__, and ordering — two ColumnRefs
    # with the same table/column are identical regardless of lineage.
    lineage: str = field(default="", compare=False)
    # Real base-table name when the alias differs (compare=False: doesn't affect equality/hash).
    # e.g. table="indicators_data_2", real_table="indicators_data"
    real_table: str = field(default="", compare=False)

    def __str__(self) -> str:
        if self.real_table and self.real_table != self.table:
            return f"{self.table}.{self.column} (from {self.real_table})"
        return f"{self.table}.{self.column}"


@dataclass
class FilterConstraint:
    """A predicate on a single column: column <op> value."""

    column: ColumnRef
    op: str  # "eq", "neq", "gt", "gte", "lt", "lte", "like", "not_like",
    # "in", "not_in", "between", "is_null", "is_not_null"
    value: Any = None  # literal value, list[Any] for in/not_in, (lo, hi) for between
    # Base-table columns that feed into this constraint's column expression.
    # For a simple column this is [column]; for a computed CTE column like
    # `a.x1 + b.d2 AS cte_mix` it will be [a.x1, b.d2].
    source_columns: list[ColumnRef] = field(default_factory=list)

    def needs_llm(self) -> bool:
        return self.op not in {
            "eq",
            "in",
            "between",
            "is_null",
            "is_not_null",
            "safe_cast_not_null",
        }


@dataclass
class FunctionalConstraint:
    """derived_col = func(source_col)  e.g. A.a1 = TRIM(B.b1)"""

    derived: ColumnRef
    source: ColumnRef
    func: str  # function name, e.g. "TRIM"


@dataclass
class ConstraintGroup:
    """One independent satisfying path through the SQL.

    Each ConstraintGroup represents a complete set of constraints that, when satisfied
    simultaneously, produces at least one output row.  Multiple groups arise from
    UNION ALL branches, OR conditions (via DNF), and CTE cross-products.
    """

    filters: list[FilterConstraint] = field(default_factory=list)
    equalities: list[tuple[ColumnRef, ColumnRef]] = field(default_factory=list)
    functional: list[FunctionalConstraint] = field(default_factory=list)
    col_inequalities: list[tuple[ColumnRef, ColumnRef]] = field(default_factory=list)
    # Columns referenced in WHERE / JOIN ON / QUALIFY that couldn't be tied to an
    # extractable constraint (e.g. inside arithmetic or multi-arg functions).
    # Ensures the generator still produces values for them even without a specific op.
    bare_columns: list[ColumnRef] = field(default_factory=list)


@dataclass
class SimplificationResult:
    """Output of simplify()."""

    # Columns that must be generated, keyed by ColumnRef.
    # Value is a list of FilterConstraints that apply to that column (may be empty).
    source_columns: dict[ColumnRef, list[FilterConstraint]] = field(
        default_factory=dict
    )

    # col → (source_col, func_expression_text)
    # e.g. A.a1 → (B.b1, "TRIM(B.b1)")
    derived_columns: dict[ColumnRef, tuple[ColumnRef, str]] = field(
        default_factory=dict
    )

    # Each inner frozenset is a group of columns that must all share the same value.
    equivalence_classes: list[frozenset[ColumnRef]] = field(default_factory=list)

    # Raw constraints, for downstream use.
    filters: list[FilterConstraint] = field(default_factory=list)
    functional: list[FunctionalConstraint] = field(default_factory=list)

    # Anti-join column pairs: (a, b) means "rows where a has no match on b".
    # Comes from LEFT/RIGHT/FULL JOIN … ON a = b WHERE b IS NULL patterns.
    col_inequalities: list[tuple[ColumnRef, ColumnRef]] = field(default_factory=list)

    # One SimplificationResult per independent satisfying path (UNION branch, OR path,
    # CTE cross-product).  Empty when the query has only a single path.
    constraint_groups: list["SimplificationResult"] = field(default_factory=list)
    constraint_groups_truncated: bool = False


# ─── Union-Find ───────────────────────────────────────────────────────────────


class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[ColumnRef, ColumnRef] = {}

    def add(self, col: ColumnRef) -> None:
        if col not in self._parent:
            self._parent[col] = col

    def find(self, col: ColumnRef) -> ColumnRef:
        self.add(col)
        while self._parent[col] != col:
            self._parent[col] = self._parent[self._parent[col]]  # path compression
            col = self._parent[col]
        return col

    def union(self, a: ColumnRef, b: ColumnRef) -> None:
        self.add(a)
        self.add(b)
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # deterministic: smaller (lexicographic) becomes root
            if ra < rb:
                self._parent[rb] = ra
            else:
                self._parent[ra] = rb

    def groups(self) -> list[frozenset[ColumnRef]]:
        """Return all equivalence classes with more than one member."""
        buckets: dict[ColumnRef, set[ColumnRef]] = {}
        for col in self._parent:
            root = self.find(col)
            buckets.setdefault(root, set()).add(col)
        return [frozenset(v) for v in buckets.values() if len(v) > 1]

    def all_groups(self) -> list[frozenset[ColumnRef]]:
        """Return ALL equivalence classes (including singletons)."""
        buckets: dict[ColumnRef, set[ColumnRef]] = {}
        for col in self._parent:
            root = self.find(col)
            buckets.setdefault(root, set()).add(col)
        return [frozenset(v) for v in buckets.values()]


# ─── AST helpers ──────────────────────────────────────────────────────────────


def _literal_value(node: exp.Expression) -> Any:
    """Extract a Python scalar from a SQL literal node, or return None."""
    if isinstance(node, exp.Literal):
        if node.is_number:
            s = node.name
            return float(s) if "." in s else int(s)
        return node.name  # string literal
    if isinstance(node, exp.Boolean):
        return node.this  # True or False
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        inner = _literal_value(node.this)
        return -inner if inner is not None else None
    # e.g. CAST('2025-01-01' AS DATE) — return the inner string
    if isinstance(node, (exp.Cast, exp.TryCast)):
        return _literal_value(node.this)
    # Any expression with no column references is a constant (PARSE_DATE(...), CURRENT_DATE(), etc.)
    # Exclude aggregate functions (COUNT(*), SUM(1), …) — they are not constants.
    if not any(True for _ in node.find_all(exp.Column)) and not any(
        True for _ in node.find_all(exp.AggFunc)
    ):
        return node.sql()
    return None


def _is_literal(node: exp.Expression) -> bool:
    return _literal_value(node) is not None or isinstance(node, exp.Null)


def _is_column(node: exp.Expression) -> bool:
    return isinstance(node, exp.Column)


def _col_ref(node: exp.Column, alias_map: dict[str, str]) -> ColumnRef | None:
    """Build a ColumnRef keeping the alias as table, storing the real table name separately."""
    table = node.table
    col = node.name
    if not col:
        return None
    if not table:
        return ColumnRef("__unknown__", col)
    alias = table.lower()
    real = alias_map.get(alias, alias)
    return ColumnRef(alias, col.lower(), real_table=real)


def _func_name(node: exp.Expression) -> str | None:
    """Return the function name if node is a function call, else None."""
    if isinstance(node, exp.Anonymous):
        return node.name.upper()
    if isinstance(node, exp.Func):
        return type(node).__name__.upper()
    return None


def _find_column_in(node: exp.Expression) -> exp.Column | None:
    """Return the first Column node found inside *node* (for func arg extraction)."""
    if isinstance(node, exp.Column):
        return node
    for child in node.args.values():
        if isinstance(child, exp.Expression):
            result = _find_column_in(child)
            if result:
                return result
        elif isinstance(child, list):
            for item in child:
                if isinstance(item, exp.Expression):
                    result = _find_column_in(item)
                    if result:
                        return result
    return None


def _sql_of(node: exp.Expression, dialect: str = "bigquery") -> str:
    return node.sql(dialect=dialect)


# ─── Schema conversion ────────────────────────────────────────────────────────


def _schemas_to_sqlglot(schemas: list[dict]) -> dict:
    """Convert the project schema list (from get_schemas) to sqlglot's dict format.

    Input:  [{"table_name": "ds.orders", "columns": [{"name": "id", "type": "INT64"}, ...]}, ...]
    Output: {"orders": {"id": "INT64", ...}, ...}
    """
    result: dict[str, dict[str, str]] = {}
    for table in schemas:
        table_name = (
            table.get("table_name", table.get("name", "")).split(".")[-1].lower()
        )
        if table_name:
            result[table_name] = {
                col["name"].lower(): col.get("type", "TEXT")
                for col in table.get("columns", [])
            }
    return result


# ─── Lineage SQL builder ──────────────────────────────────────────────────────


def _build_column_sql(
    col_expr: exp.Expression, source: exp.Expression, dialect: str
) -> str:
    """Return SELECT <col_expr> FROM <minimal_source> — no WHERE, only needed JOINs.

    Given the expression that defines a column and the SELECT it came from, build
    the simplest executable query that produces that column's value:
      • Strip the WHERE clause entirely (we only want the expression, not the filter).
      • Keep only the JOINs whose alias appears in col_expr (e.g. for p.p1+a.b2
        keep the JOIN on a, drop everything else).
    """
    simplified_expr = sg_simplify(col_expr.copy(), dialect=dialect)

    if not isinstance(source, exp.Select):
        src_sql = source.sql(dialect=dialect)
        col_sql = simplified_expr.sql(dialect=dialect)
        return f"SELECT {col_sql} FROM ({src_sql})"

    # Collect table aliases referenced in the column expression
    used_tables: set[str] = {
        node.table.lower()
        for node in simplified_expr.walk()
        if isinstance(node, exp.Column) and node.table
    }

    from_clause = source.args.get("from_")
    joins = source.args.get("joins") or []

    # Keep only joins whose table/alias is referenced by col_expr
    needed_joins = [
        j
        for j in joins
        if not isinstance(j.this, exp.Table)  # subquery — keep
        or (j.this.alias or j.this.name).lower() in used_tables
    ]

    new_sel = exp.select(simplified_expr.copy())
    if from_clause:
        new_sel = new_sel.from_(from_clause.this.copy())
    for j in needed_joins:
        new_sel.append("joins", j.copy())

    return new_sel.sql(dialect=dialect)


# ─── CTE lineage resolver ─────────────────────────────────────────────────────


class _LineageResolver:
    """
    Resolves CTE column references to their base-table source using sqlglot's
    built-in lineage engine, and builds a SQL lineage string for each column.

    Only columns whose table is a CTE name trigger a lineage call; base-table
    columns are returned unchanged.  Results are cached to avoid redundant calls.
    """

    def __init__(
        self,
        statement: exp.Expression,
        schema: dict | None,
        dialect: str,
    ) -> None:
        from sqlglot.lineage import lineage as _sg_lineage

        self._statement = statement
        self._schema = schema
        self._dialect = dialect
        self._sg_lineage = _sg_lineage
        self._cache: dict[tuple[str, str], ColumnRef] = {}
        self._all_cache: dict[tuple[str, str], list[ColumnRef]] = {}
        with_clause = statement.args.get("with_")
        ctes = (
            with_clause.expressions
            if with_clause and hasattr(with_clause, "expressions")
            else []
        )
        self._cte_names: set[str] = {cte.alias.lower() for cte in ctes if cte.alias}

    def _effective_cte_name(self, col: ColumnRef) -> str | None:
        """Return the CTE name this column resolves to, or None if it's a base table.

        Handles FROM aliases: `FROM cte1 AS c` → col.table='c', col.real_table='cte1'.
        """
        if col.table in self._cte_names:
            return col.table
        if col.real_table and col.real_table in self._cte_names:
            return col.real_table
        return None

    def resolve(self, col: ColumnRef) -> ColumnRef:
        """Return a base-table ColumnRef with a SQL lineage string.

        Calls sqlglot.lineage.lineage() for CTE-sourced columns;
        passes base-table columns through unchanged.
        """
        key = (col.table, col.column)
        if key in self._cache:
            return self._cache[key]
        resolved = (
            self._resolve_via_lineage(col)
            if self._effective_cte_name(col) is not None
            else col
        )
        self._cache[key] = resolved
        return resolved

    def resolve_all(self, col: ColumnRef) -> list[ColumnRef]:
        """Return ALL base-table ColumnRefs that feed into this column.

        For a simple column (e.g. cte1.z → b.z) returns [b.z].
        For a computed expression (e.g. cte1.cte_mix where cte_mix = a.x1 + b.d2)
        returns [a.x1, b.d2] — all leaf base-table columns in the lineage tree.
        Non-CTE columns are returned as-is in a single-element list.
        """
        if self._effective_cte_name(col) is None:
            return [col]
        key = (col.table, col.column)
        if key in self._all_cache:
            return self._all_cache[key]
        result = self._resolve_all_via_lineage(col)
        self._all_cache[key] = result
        return result

    def _make_lineage_stmt(self, col: ColumnRef) -> exp.Expression:
        """Build WITH <all_ctes> SELECT col.column FROM col.table.

        Ensures col.column always appears in the outermost SELECT so that
        sqlglot lineage can trace it through multi-level CTEs, even when the
        column is not projected by the original outer query.
        Uses the real CTE name (not a FROM alias) so the WITH clause matches.
        """
        cte_name = self._effective_cte_name(col) or col.table
        wrapper = sqlglot.parse_one(
            f"SELECT {col.column} FROM {cte_name}",
            dialect=self._dialect,
        )
        with_clause = self._statement.args.get("with_")
        if with_clause:
            wrapper.set("with_", with_clause.copy())
        return wrapper

    def _resolve_all_via_lineage(self, col: ColumnRef) -> list[ColumnRef]:
        try:
            node = self._sg_lineage(
                col.column,
                self._make_lineage_stmt(col),
                schema=self._schema,
                dialect=self._dialect,
                trim_selects=False,
            )
            sources: list[ColumnRef] = []
            for n in node.walk():
                if isinstance(n.expression, exp.Table):
                    base_table = n.expression.name.lower()
                    base_col = n.name.split(".")[-1].lower()
                    sources.append(ColumnRef(base_table, base_col))
            return sources if sources else [col]
        except Exception as exc:
            logger.debug(
                "resolve_all lineage failed for %s.%s: %s", col.table, col.column, exc
            )
            return [col]

    def _resolve_via_lineage(self, col: ColumnRef) -> ColumnRef:
        try:
            node = self._sg_lineage(
                col.column,
                self._make_lineage_stmt(col),
                schema=self._schema,
                dialect=self._dialect,
                trim_selects=False,
            )
            base_table = col.table
            base_col = col.column
            lineage_sql = ""
            for n in node.walk():
                if isinstance(n.expression, exp.Table):
                    base_table = n.expression.name.lower()
                    base_col = n.name.split(".")[-1].lower()
                else:
                    try:
                        expr = n.expression
                        col_expr = expr.this if isinstance(expr, exp.Alias) else expr
                        lineage_sql = _build_column_sql(
                            col_expr, n.source, self._dialect
                        )
                    except Exception as exc:
                        logger.debug(
                            "lineage sql build failed for %s.%s: %s",
                            col.table,
                            col.column,
                            exc,
                        )
                        lineage_sql = n.name
            return ColumnRef(base_table, base_col, lineage=lineage_sql)
        except Exception as exc:
            logger.debug(
                "resolve lineage failed for %s.%s: %s", col.table, col.column, exc
            )
            return col


# ─── Alias collectors ─────────────────────────────────────────────────────────


def _collect_aliases(select: exp.Select, alias_map: dict[str, str]) -> None:
    """Populate alias_map with {alias_lower → real_table_lower} from FROM/JOINs."""
    from_clause = select.args.get("from_")
    joins = select.args.get("joins") or []

    sources = []
    if from_clause:
        sources.append(from_clause.this)
    for j in joins:
        sources.append(j.this)

    for src in sources:
        if isinstance(src, exp.Table):
            real = src.name.lower()
            alias = src.alias.lower() if src.alias else real
            alias_map[alias] = real
            alias_map[real] = real  # identity mapping
        elif isinstance(src, exp.Subquery):
            alias = src.alias.lower() if src.alias else ""
            if alias:
                # Resolve alias to the underlying base table when the inner SELECT
                # is a simple single-table scan (no sub-JOINs, no nested subqueries).
                real = alias
                inner = src.this
                if isinstance(inner, exp.Select):
                    inner_from = inner.args.get("from_")
                    inner_joins = inner.args.get("joins") or []
                    if (
                        inner_from
                        and not inner_joins
                        and isinstance(inner_from.this, exp.Table)
                    ):
                        real = inner_from.this.name.lower()
                alias_map[alias] = real
                if real != alias:
                    alias_map[real] = real


# ─── Condition flattening ─────────────────────────────────────────────────────


def _flatten_and(cond: exp.Expression) -> list[exp.Expression]:
    """Recursively flatten AND-chains into a flat list of predicates."""
    if isinstance(cond, exp.And):
        return _flatten_and(cond.left) + _flatten_and(cond.right)
    return [cond]


_MAX_CONSTRAINT_GROUPS = (
    32  # max groups emitted by extract_constraints (UNION ALL branches)
)


def _collect_format_constraints(
    select: exp.Select,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
) -> None:
    """Add format constraints for each SAFE_CAST, CAST, PARSE_DATE, etc. in *select*.

    Scans SELECT projections, WHERE, and JOIN-ON expressions.
    Does not descend into subqueries or CTE definitions — those are handled
    by the recursive _walk_tree / CTE loop in extract_constraints.
    """

    def _iter_format_nodes(node: exp.Expression):
        """Yield format-related nodes, not descending into Subquery or With subtrees."""
        for child in node.args.values():
            items = child if isinstance(child, list) else [child]
            for item in items:
                if not isinstance(item, exp.Expression):
                    continue
                if isinstance(item, (exp.Subquery, exp.With)):
                    continue
                if isinstance(
                    item, (exp.TryCast, exp.Cast, exp.StrToDate, exp.TimeToStr)
                ):
                    yield item
                yield from _iter_format_nodes(item)

    seen: set[tuple[str, str, str, str]] = set()
    for node in _iter_format_nodes(select):
        inner_col = _find_column_in(node)
        if inner_col is None:
            continue
        ref = _col_ref(inner_col, alias_map)
        if ref is None:
            continue
        src_cols = resolver.resolve_all(ref)
        ref = resolver.resolve(ref)

        op = (
            "safe_cast_not_null"
            if isinstance(node, exp.TryCast)
            else "format_constraint"
        )

        if isinstance(node, (exp.TryCast, exp.Cast)):
            to_type = node.args.get("to")
            val_str = to_type.sql(dialect=resolver._dialect).upper() if to_type else ""
        elif isinstance(node, exp.StrToDate):
            fmt = node.args.get("format")
            val_str = (
                f"PARSE_DATE {fmt.sql(dialect=resolver._dialect)}"
                if fmt
                else "PARSE_DATE"
            )
        elif isinstance(node, exp.TimeToStr):
            fmt = node.args.get("format")
            val_str = (
                f"FORMAT_DATE {fmt.sql(dialect=resolver._dialect)}"
                if fmt
                else "FORMAT_DATE"
            )
        else:
            val_str = "FORMAT"

        key = (ref.table, ref.column, op, val_str)
        if key not in seen:
            seen.add(key)
            filters.append(
                FilterConstraint(
                    column=ref,
                    op=op,
                    value=val_str,
                    source_columns=src_cols,
                )
            )


def _collect_cols_shallow(
    node: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    default_table: str | None,
) -> list[ColumnRef]:
    """Return all ColumnRefs reachable from *node* without descending into subqueries.

    Used to collect bare column references from WHERE / JOIN ON / QUALIFY so that
    columns embedded in complex expressions (arithmetic, multi-arg functions, …)
    are still added to source_columns even when no extractable constraint was found.
    """
    seen: set[ColumnRef] = set()
    result: list[ColumnRef] = []

    def _walk(n: exp.Expression) -> None:
        if isinstance(n, (exp.Subquery, exp.With)):
            return
        if isinstance(n, exp.Column) and n.name:
            raw = _col_ref(n, alias_map)
            if raw:
                if raw.table == "__unknown__" and default_table:
                    raw = ColumnRef(default_table, raw.column, real_table=default_table)
                ref = resolver.resolve(raw)
                if ref not in seen:
                    seen.add(ref)
                    result.append(ref)
            return
        for child in n.args.values():
            if isinstance(child, exp.Expression):
                _walk(child)
            elif isinstance(child, list):
                for item in child:
                    if isinstance(item, exp.Expression):
                        _walk(item)

    _walk(node)
    return result


def _collect_is_null_cols(
    cond: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
) -> set[ColumnRef]:
    """Return resolved ColumnRefs that appear in IS NULL predicates in top-level ANDs.

    Used to detect anti-join patterns: when a column from the nullable side of
    an outer join appears here, the corresponding ON-clause equality is not a
    true equality constraint but a filter saying "no match in the other table".
    """
    result: set[ColumnRef] = set()
    for pred in _flatten_and(cond):
        if not isinstance(pred, exp.Is):
            continue
        right = pred.args.get("expression") or pred.args.get("to")
        if _is_column(pred.this) and isinstance(right, exp.Null):
            ref = _col_ref(pred.this, alias_map)
            if ref:
                result.add(resolver.resolve(ref))
    return result


# ─── Constraint extraction ────────────────────────────────────────────────────

_CMP_OP_MAP = {
    exp.EQ: "eq",
    exp.NEQ: "neq",
    exp.GT: "gt",
    exp.GTE: "gte",
    exp.LT: "lt",
    exp.LTE: "lte",
}

_FLIP_OP = {
    "gt": "lt",
    "lt": "gt",
    "gte": "lte",
    "lte": "gte",
    "eq": "eq",
    "neq": "neq",
}


def _extract_from_condition(
    cond: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
    equalities: list[tuple[ColumnRef, ColumnRef]],
    functional: list[FunctionalConstraint],
) -> None:
    """Parse a single predicate and append to the appropriate list."""
    for pred in _flatten_and(cond):
        _dispatch_pred(pred, alias_map, resolver, filters, equalities, functional)


def _dispatch_pred(
    pred: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
    equalities: list[tuple[ColumnRef, ColumnRef]],
    functional: list[FunctionalConstraint],
) -> None:
    # ── IS NULL / IS NOT NULL ──────────────────────────────────────────────────
    if isinstance(pred, exp.Is):
        left = pred.this
        right = pred.args.get("expression") or pred.args.get("to")
        if _is_column(left):
            raw = _col_ref(left, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                op = "is_null" if isinstance(right, exp.Null) else "is_not_null"
                filters.append(
                    FilterConstraint(column=ref, op=op, source_columns=src_cols)
                )
        return

    if isinstance(pred, exp.Not):
        inner = pred.this
        if isinstance(inner, exp.Is) and _is_column(inner.this):
            raw = _col_ref(inner.this, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                filters.append(
                    FilterConstraint(
                        column=ref, op="is_not_null", source_columns=src_cols
                    )
                )
        elif isinstance(inner, exp.Like):
            pat_node = inner.args.get("expression") or inner.args.get("pattern")
            col_node = inner.this
            if _is_column(col_node):
                raw = _col_ref(col_node, alias_map)
                if raw and pat_node:
                    src_cols = resolver.resolve_all(raw)
                    ref = resolver.resolve(raw)
                    filters.append(
                        FilterConstraint(
                            column=ref,
                            op="not_like",
                            value=_literal_value(pat_node) or _sql_of(pat_node),
                            source_columns=src_cols,
                        )
                    )
            elif _func_name(col_node) is not None and pat_node:
                # NOT func(col) LIKE 'pattern'
                inner_col = _find_column_in(col_node)
                if inner_col:
                    raw = _col_ref(inner_col, alias_map)
                    if raw:
                        src_cols = resolver.resolve_all(raw)
                        ref = resolver.resolve(raw)
                        filters.append(
                            FilterConstraint(
                                column=ref,
                                op="not_like",
                                value=_literal_value(pat_node) or _sql_of(pat_node),
                                source_columns=src_cols,
                            )
                        )
        elif isinstance(inner, exp.In) and _is_column(inner.this):
            raw = _col_ref(inner.this, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                vals = [_literal_value(v) for v in inner.expressions]
                filters.append(
                    FilterConstraint(
                        column=ref, op="not_in", value=vals, source_columns=src_cols
                    )
                )
        return

    # ── LIKE ──────────────────────────────────────────────────────────────────
    if isinstance(pred, exp.Like):
        col_node = pred.this
        pat_node = pred.args.get("expression") or pred.args.get("pattern")
        if _is_column(col_node) and pat_node:
            raw = _col_ref(col_node, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                filters.append(
                    FilterConstraint(
                        column=ref,
                        op="like",
                        value=_literal_value(pat_node) or _sql_of(pat_node),
                        source_columns=src_cols,
                    )
                )
        elif _func_name(col_node) is not None and pat_node:
            # func(col) LIKE 'pattern' — e.g. UPPER(lib_carte) LIKE 'M%'
            inner_col = _find_column_in(col_node)
            if inner_col:
                raw = _col_ref(inner_col, alias_map)
                if raw:
                    src_cols = resolver.resolve_all(raw)
                    ref = resolver.resolve(raw)
                    filters.append(
                        FilterConstraint(
                            column=ref,
                            op="like",
                            value=_literal_value(pat_node) or _sql_of(pat_node),
                            source_columns=src_cols,
                        )
                    )
        return

    # ── IN / NOT IN ───────────────────────────────────────────────────────────
    if isinstance(pred, exp.In):
        col_node = pred.this
        unnested = pred.args.get("unnest")
        if _is_column(col_node) and not unnested:
            raw = _col_ref(col_node, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                vals = [_literal_value(v) for v in pred.expressions]
                op = "not_in" if pred.args.get("not") else "in"
                filters.append(
                    FilterConstraint(
                        column=ref, op=op, value=vals, source_columns=src_cols
                    )
                )
        return

    # ── BETWEEN ───────────────────────────────────────────────────────────────
    if isinstance(pred, exp.Between):
        col_node = pred.this
        lo = pred.args.get("low")
        hi = pred.args.get("high")
        if _is_column(col_node) and lo and hi:
            raw = _col_ref(col_node, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                filters.append(
                    FilterConstraint(
                        column=ref,
                        op="between",
                        value=(_literal_value(lo), _literal_value(hi)),
                        source_columns=src_cols,
                    )
                )
        return

    # ── Binary comparisons ────────────────────────────────────────────────────
    op_str = _CMP_OP_MAP.get(type(pred))
    if op_str is None:
        return

    left = pred.left if hasattr(pred, "left") else pred.args.get("this")
    right = pred.right if hasattr(pred, "right") else pred.args.get("expression")
    if left is None or right is None:
        return

    left_is_col = _is_column(left)
    right_is_col = _is_column(right)
    left_is_lit = _is_literal(left)
    right_is_lit = _is_literal(right)
    left_func = _func_name(left)
    right_func = _func_name(right)

    # col = col  →  equality / equivalence
    if left_is_col and right_is_col and op_str == "eq":
        ref_l = _col_ref(left, alias_map)
        ref_r = _col_ref(right, alias_map)
        if ref_l and ref_r:
            ref_l = resolver.resolve(ref_l)
            ref_r = resolver.resolve(ref_r)
            if ref_l != ref_r:
                equalities.append((ref_l, ref_r))
        return

    # col = func(col)  →  functional dependency
    if left_is_col and right_func and op_str == "eq":
        inner_col = _find_column_in(right)
        if inner_col:
            ref_derived = _col_ref(left, alias_map)
            if ref_derived:
                ref_source = _col_ref(inner_col, alias_map)
                if ref_source:
                    ref_derived = resolver.resolve(ref_derived)
                    ref_source = resolver.resolve(ref_source)
                    if ref_derived != ref_source:
                        functional.append(
                            FunctionalConstraint(
                                derived=ref_derived,
                                source=ref_source,
                                func=right_func,
                            )
                        )
            return
        # no column inside function → constant expression, fall through to literal check

    # func(col) = col  →  same, flipped
    if right_is_col and left_func and op_str == "eq":
        inner_col = _find_column_in(left)
        ref_derived = _col_ref(right, alias_map)
        if ref_derived and inner_col:
            ref_source = _col_ref(inner_col, alias_map)
            if ref_source:
                ref_derived = resolver.resolve(ref_derived)
                ref_source = resolver.resolve(ref_source)
                if ref_derived != ref_source:
                    functional.append(
                        FunctionalConstraint(
                            derived=ref_derived,
                            source=ref_source,
                            func=left_func,
                        )
                    )
        return

    # func(col) <op> literal  →  constraint on the inner column
    # e.g. LOWER(email) = 'john', EXTRACT(YEAR FROM dt) = 2024, FORMAT_DATE('%Y', dt) = '2024'
    if left_func and right_is_lit and not left_is_col and not right_is_col:
        inner_col = _find_column_in(left)
        if inner_col:
            raw = _col_ref(inner_col, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                filters.append(
                    FilterConstraint(
                        column=ref,
                        op=op_str,
                        value=_literal_value(right),
                        source_columns=src_cols,
                    )
                )
        return

    # literal <op> func(col)  →  flip the operator
    if right_func and left_is_lit and not left_is_col and not right_is_col:
        inner_col = _find_column_in(right)
        if inner_col:
            raw = _col_ref(inner_col, alias_map)
            if raw:
                src_cols = resolver.resolve_all(raw)
                ref = resolver.resolve(raw)
                filters.append(
                    FilterConstraint(
                        column=ref,
                        op=_FLIP_OP.get(op_str, op_str),
                        value=_literal_value(left),
                        source_columns=src_cols,
                    )
                )
        return

    # col <op> literal
    if left_is_col and right_is_lit:
        raw = _col_ref(left, alias_map)
        if raw:
            src_cols = resolver.resolve_all(raw)
            ref = resolver.resolve(raw)
            filters.append(
                FilterConstraint(
                    column=ref,
                    op=op_str,
                    value=_literal_value(right),
                    source_columns=src_cols,
                )
            )
        return

    # literal <op> col  →  flip the operator
    if right_is_col and left_is_lit:
        raw = _col_ref(right, alias_map)
        if raw:
            src_cols = resolver.resolve_all(raw)
            ref = resolver.resolve(raw)
            filters.append(
                FilterConstraint(
                    column=ref,
                    op=_FLIP_OP.get(op_str, op_str),
                    value=_literal_value(left),
                    source_columns=src_cols,
                )
            )
        return


# ─── Recursive condition walker (replaces DNF) ───────────────────────────────


def _extract_from_condition_recursive(
    cond: exp.Expression,
    alias_map: dict[str, str],
    resolver: "_LineageResolver",
    filters: list[FilterConstraint],
    equalities: list[tuple[ColumnRef, ColumnRef]],
    functional: list[FunctionalConstraint],
) -> None:
    """Walk *cond* recursively, collecting constraints from ALL branches (AND + OR).

    OR branches are not expanded — all constraints from any branch are accumulated
    into the same lists.  This is conservative for Faker pre-fill: every column
    mentioned in any OR branch is marked as constrained.
    """
    if isinstance(cond, (exp.And, exp.Or, exp.Paren)):
        left = cond.args.get("this")
        right = cond.args.get("expression")
        if left is not None:
            _extract_from_condition_recursive(
                left, alias_map, resolver, filters, equalities, functional
            )
        if right is not None:
            _extract_from_condition_recursive(
                right, alias_map, resolver, filters, equalities, functional
            )
    else:
        _dispatch_pred(cond, alias_map, resolver, filters, equalities, functional)


# ─── AND/OR-preserving serializer (for build_conditions_hint) ─────────────────


def _collect_window_aliases(statement: exp.Expression) -> set[str]:
    """Return all column alias names defined by window functions in *statement*.

    Aliases like ``rn`` in ``ROW_NUMBER() OVER (...) AS rn`` are collected so
    predicates such as ``rn <= 3`` can be recognised as volume constraints and
    excluded from the conditions hint.
    """
    aliases: set[str] = set()
    for node in statement.walk():
        if isinstance(node, exp.Window) and isinstance(
            node.this, (exp.RowNumber, exp.Rank, exp.DenseRank, exp.Ntile)
        ):
            parent = node.parent
            if isinstance(parent, exp.Alias) and parent.alias:
                aliases.add(parent.alias.lower())
    return aliases


def _is_volume_pred(node: exp.Expression, window_aliases: set[str]) -> bool:
    """Return True if *node* filters on a window-function alias (volume constraint).

    Matches patterns like ``alias <= N``, ``alias < N``, ``alias = N``,
    ``alias > N``, ``alias >= N`` where *alias* is a known window alias.
    """
    if not isinstance(node, (exp.LTE, exp.LT, exp.EQ, exp.GT, exp.GTE)):
        return False
    left = node.args.get("this")
    right = node.args.get("expression")
    col_node = None
    if isinstance(left, exp.Column) and _is_literal(
        right or exp.Literal(this="", is_string=False)
    ):
        col_node = left
    elif isinstance(right, exp.Column) and _is_literal(
        left or exp.Literal(this="", is_string=False)
    ):
        col_node = right
    return col_node is not None and col_node.name.lower() in window_aliases


def _resolve_pred_node(
    node: exp.Expression,
    alias_map: dict[str, str],
    resolver: "_LineageResolver",
    dialect: str,
) -> str:
    """Render *node* with every Column reference replaced by ``base_table.col``.

    Uses sqlglot's ``transform()`` so nested functions (UPPER, CAST, …) are
    handled correctly.  Falls back to the original SQL on any error.
    """

    def _transform_fn(n: exp.Expression) -> exp.Expression:
        if isinstance(n, exp.Column) and n.table:
            raw = _col_ref(n, alias_map)
            if raw is None:
                return n
            resolved = resolver.resolve(raw)
            tbl = (
                resolved.real_table
                if (resolved.real_table and resolved.real_table != resolved.table)
                else resolved.table
            )
            if tbl and tbl != "__unknown__":
                return exp.column(resolved.column, table=tbl)
        return n

    try:
        return node.transform(_transform_fn).sql(dialect=dialect)
    except Exception:
        return node.sql(dialect=dialect)


def _serialize_cond(
    node: exp.Expression,
    alias_map: dict[str, str],
    resolver: "_LineageResolver",
    window_aliases: set[str],
    dialect: str,
) -> str | None:
    """Recursively serialise *node* preserving AND/OR structure.

    Returns ``None`` for volume constraints (window alias filters) so they are
    silently dropped by callers.

    * ``exp.And``   → ``"left AND right"`` (None parts filtered)
    * ``exp.Or``    → ``"left OR right"``  (no extra parens — caller wraps if needed)
    * ``exp.Paren`` → ``"(inner)"``
    * volume pred   → ``None``
    * leaf pred     → ``_resolve_pred_node()``
    """
    if isinstance(node, exp.Paren):
        inner = _serialize_cond(node.this, alias_map, resolver, window_aliases, dialect)
        return f"({inner})" if inner else None

    if isinstance(node, exp.And):
        left = _serialize_cond(
            node.args["this"], alias_map, resolver, window_aliases, dialect
        )
        right = _serialize_cond(
            node.args["expression"], alias_map, resolver, window_aliases, dialect
        )
        parts = [p for p in (left, right) if p]
        return " AND ".join(parts) if parts else None

    if isinstance(node, exp.Or):
        left = _serialize_cond(
            node.args["this"], alias_map, resolver, window_aliases, dialect
        )
        right = _serialize_cond(
            node.args["expression"], alias_map, resolver, window_aliases, dialect
        )
        parts = [p for p in (left, right) if p]
        if not parts:
            return None
        return " OR ".join(parts)

    if _is_volume_pred(node, window_aliases):
        return None

    return _resolve_pred_node(node, alias_map, resolver, dialect)


def _collect_format_constraints_strings(
    statement: exp.Expression,
    alias_map: dict[str, str],
    resolver: "_LineageResolver",
    dialect: str,
) -> list[str]:
    """Return human-readable format-constraint strings for SAFE_CAST, CAST, PARSE_DATE, etc.

    Each entry has the form ``"base_table.col : SAFE_CAST AS FLOAT64"`` or
    ``"base_table.col : PARSE_DATE('%Y%m')"`` so the LLM knows which columns
    require a specific format.  Results are deduplicated.
    """
    seen: set[str] = set()
    results: list[str] = []

    def _resolved_tbl_col(col_node: exp.Column) -> tuple[str, str] | None:
        raw = _col_ref(col_node, alias_map)
        if raw is None:
            return None
        resolved = resolver.resolve(raw)
        tbl = (
            resolved.real_table
            if (resolved.real_table and resolved.real_table != resolved.table)
            else resolved.table
        )
        if not tbl or tbl == "__unknown__":
            return None
        return tbl, resolved.column

    def _add(entry: str) -> None:
        if entry not in seen:
            seen.add(entry)
            results.append(entry)

    # TryCast → SAFE_CAST AS T  /  Cast → CAST AS T
    for node in statement.find_all((exp.TryCast, exp.Cast)):
        inner_col = _find_column_in(node.this)
        if inner_col is None:
            continue
        tc = _resolved_tbl_col(inner_col)
        if tc is None:
            continue
        tbl, col_name = tc
        to_type = node.args.get("to")
        type_str = to_type.sql(dialect=dialect).upper() if to_type else ""
        fn = "SAFE_CAST" if isinstance(node, exp.TryCast) else "CAST"
        _add(f"{tbl}.{col_name} : {fn} AS {type_str}")

    # Named date-format function types (sqlglot maps dialect-specific names to these)
    # exp.StrToDate    → PARSE_DATE('%Y%m', col)
    # exp.TimeToStr    → FORMAT_DATE('%Y-%m', col)
    # exp.StrToTime    → PARSE_TIMESTAMP('%Y', col)
    # exp.ParseDatetime → PARSE_DATETIME('%Y-%m-%d', col)
    _NAMED_DATE_FNS: tuple[tuple[type, str], ...] = (
        (exp.StrToDate, "PARSE_DATE"),
        (exp.TimeToStr, "FORMAT_DATE"),
        (exp.StrToTime, "PARSE_TIMESTAMP"),
        (getattr(exp, "ParseDatetime", type(None)), "PARSE_DATETIME"),
    )
    for fn_type, fn_name in _NAMED_DATE_FNS:
        if fn_type is type(None):
            continue
        for node in statement.find_all(fn_type):
            fmt_node = node.args.get("format")
            fmt_str = _literal_value(fmt_node) if fmt_node else None
            inner_col = _find_column_in(node.this)
            if inner_col is None:
                continue
            tc = _resolved_tbl_col(inner_col)
            if tc is None:
                continue
            tbl, col_name = tc
            fmt_repr = (
                repr(fmt_str)
                if fmt_str is not None
                else (fmt_node.sql(dialect=dialect) if fmt_node else "?")
            )
            _add(f"{tbl}.{col_name} : {fn_name}({fmt_repr})")

    # Fallback: Anonymous function nodes (non-BigQuery dialects or unknown functions)
    _DATE_FNS: frozenset[str] = frozenset(
        {"PARSE_DATE", "FORMAT_DATE", "PARSE_TIMESTAMP", "PARSE_DATETIME"}
    )
    for node in statement.find_all(exp.Anonymous):
        fname = (node.name or "").upper()
        if fname not in _DATE_FNS:
            continue
        args = node.args.get("expressions") or []
        if len(args) < 2:
            continue
        fmt_arg, col_arg = args[0], args[1]
        fmt_str = _literal_value(fmt_arg)
        inner_col = _find_column_in(col_arg)
        if inner_col is None:
            continue
        tc = _resolved_tbl_col(inner_col)
        if tc is None:
            continue
        tbl, col_name = tc
        fmt_repr = (
            repr(fmt_str) if fmt_str is not None else fmt_arg.sql(dialect=dialect)
        )
        _add(f"{tbl}.{col_name} : {fname}({fmt_repr})")

    return results


# ─── Grouped AST walk ─────────────────────────────────────────────────────────


def _walk_select_grouped(
    select: exp.Select,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    cte_groups_map: dict[str, list[ConstraintGroup]],
) -> list[ConstraintGroup]:
    """Return one ConstraintGroup per satisfying path through a single SELECT."""
    _collect_aliases(select, alias_map)

    from_clause = select.args.get("from_")
    joins = select.args.get("joins") or []
    all_sources = ([from_clause.this] if from_clause else []) + [j.this for j in joins]

    # ── Anti-join detection ────────────────────────────────────────────────────
    where = select.args.get("where")
    where_is_null_cols: set[ColumnRef] = set()
    if where:
        where_is_null_cols = _collect_is_null_cols(where.this, alias_map, resolver)

    # Collect aliases on the nullable side of outer joins
    outer_join_aliases: set[str] = set()
    for join in joins:
        if (join.args.get("side") or "").upper() in {"LEFT", "RIGHT", "FULL"}:
            src = join.this
            alias = ""
            if isinstance(src, exp.Table):
                alias = (src.alias or src.name).lower()
            elif isinstance(src, exp.Subquery):
                alias = (src.alias or "").lower()
            if alias:
                outer_join_aliases.add(alias)

    anti_join_sources: set[str] = set()
    if where and outer_join_aliases:
        for pred in _flatten_and(where.this):
            if isinstance(pred, exp.Is):
                right_node = pred.args.get("expression") or pred.args.get("to")
                if _is_column(pred.this) and isinstance(right_node, exp.Null):
                    tbl = (pred.this.table or "").lower()
                    if tbl in outer_join_aliases:
                        anti_join_sources.add(tbl)

    # ── Shared constraints (JOIN ON) ───────────────────────────────────────────
    shared_filters: list[FilterConstraint] = []
    shared_equalities: list[tuple[ColumnRef, ColumnRef]] = []
    shared_functional: list[FunctionalConstraint] = []
    shared_col_inequalities: list[tuple[ColumnRef, ColumnRef]] = []

    for join in joins:
        on = join.args.get("on")
        if not on:
            continue
        join_side = (join.args.get("side") or "").upper()
        is_outer_join = join_side in {"LEFT", "RIGHT", "FULL"}
        if is_outer_join and where_is_null_cols:
            tmp_eq: list[tuple[ColumnRef, ColumnRef]] = []
            tmp_func: list[FunctionalConstraint] = []
            _extract_from_condition(
                on, alias_map, resolver, shared_filters, tmp_eq, tmp_func
            )
            for a, b in tmp_eq:
                if a not in where_is_null_cols and b not in where_is_null_cols:
                    shared_equalities.append((a, b))
                else:
                    shared_col_inequalities.append((a, b))
            for fc in tmp_func:
                if (
                    fc.derived not in where_is_null_cols
                    and fc.source not in where_is_null_cols
                ):
                    shared_functional.append(fc)
        else:
            _extract_from_condition(
                on,
                alias_map,
                resolver,
                shared_filters,
                shared_equalities,
                shared_functional,
            )

    _collect_format_constraints(select, alias_map, resolver, shared_filters)

    # ── WHERE → flat recursive walk (AND/OR branches merged into one group) ────
    real_tables = {v for v in alias_map.values() if v and not v.startswith("__")}
    default_table = next(iter(real_tables)) if len(real_tables) == 1 else None

    where_filters: list[FilterConstraint] = []
    where_equalities: list[tuple[ColumnRef, ColumnRef]] = []
    where_functional: list[FunctionalConstraint] = []
    if where:
        _extract_from_condition_recursive(
            where.this,
            alias_map,
            resolver,
            where_filters,
            where_equalities,
            where_functional,
        )
        if default_table:
            where_filters = [
                FilterConstraint(
                    column=ColumnRef(default_table, f.column.column),
                    op=f.op,
                    value=f.value,
                    source_columns=[ColumnRef(default_table, f.column.column)],
                )
                if f.column.table == "__unknown__"
                else f
                for f in where_filters
            ]

    result_groups: list[ConstraintGroup] = [
        ConstraintGroup(
            filters=shared_filters + where_filters,
            equalities=shared_equalities + where_equalities,
            functional=shared_functional + where_functional,
            col_inequalities=list(shared_col_inequalities),
        )
    ]

    # ── Merge CTE / inline-subquery constraints into the single group ──────────
    # Guard: merge each CTE at most once per SELECT, even if referenced multiple
    # times under different aliases (e.g. self-join: FROM a AS p JOIN a AS q).
    seen_cte_merge: set[str] = set()
    for src in all_sources:
        if isinstance(src, exp.Table):
            tbl_name = (src.alias or src.name).lower()
            real_tbl = alias_map.get(tbl_name, tbl_name)
            cte_key = real_tbl if real_tbl in cte_groups_map else tbl_name
            if (
                cte_key in cte_groups_map
                and tbl_name not in anti_join_sources
                and cte_key not in seen_cte_merge
            ):
                seen_cte_merge.add(cte_key)
                cte_gs = cte_groups_map[cte_key]
                if any(g.filters or g.equalities or g.functional for g in cte_gs):
                    for cte_g in cte_gs:
                        for g in result_groups:
                            g.filters = g.filters + cte_g.filters
                            g.equalities = g.equalities + cte_g.equalities
                            g.functional = g.functional + cte_g.functional
                            g.col_inequalities = (
                                g.col_inequalities + cte_g.col_inequalities
                            )
                            g.bare_columns = list(
                                {*g.bare_columns, *cte_g.bare_columns}
                            )
        elif isinstance(src, exp.Subquery):
            sub_alias = (src.alias or "").lower()
            if sub_alias not in anti_join_sources:
                sub_gs = _walk_tree_grouped(src.this, {}, resolver, cte_groups_map)
                if any(g.filters or g.equalities or g.functional for g in sub_gs):
                    for sub_g in sub_gs:
                        for g in result_groups:
                            g.filters = g.filters + sub_g.filters
                            g.equalities = g.equalities + sub_g.equalities
                            g.functional = g.functional + sub_g.functional
                            g.col_inequalities = (
                                g.col_inequalities + sub_g.col_inequalities
                            )
                            g.bare_columns = list(
                                {*g.bare_columns, *sub_g.bare_columns}
                            )

    # ── Scalar subqueries in SELECT projections (correlated) ──────────────────
    for projection in select.expressions:
        for subq in projection.find_all(exp.Subquery):
            sub_gs = _walk_tree_grouped(
                subq.this, dict(alias_map), resolver, cte_groups_map
            )
            for sub_g in sub_gs:
                if not (sub_g.filters or sub_g.equalities or sub_g.functional):
                    continue
                for g in result_groups:
                    g.filters = g.filters + sub_g.filters
                    g.equalities = g.equalities + sub_g.equalities
                    g.functional = g.functional + sub_g.functional
                    g.col_inequalities = g.col_inequalities + sub_g.col_inequalities

    # ── Bare columns: WHERE + JOIN ON + QUALIFY (not HAVING — aggregated cols) ─
    # Catches column refs in complex expressions (arithmetic, multi-arg functions)
    # that _dispatch_pred couldn't map to a specific constraint.
    bare_candidates: list[ColumnRef] = []
    if where:
        bare_candidates.extend(
            _collect_cols_shallow(where.this, alias_map, resolver, default_table)
        )
    for join in joins:
        on = join.args.get("on")
        if on:
            bare_candidates.extend(
                _collect_cols_shallow(on, alias_map, resolver, default_table)
            )
    qualify = select.args.get("qualify")
    if qualify:
        qualify_node = qualify.this if isinstance(qualify, exp.Qualify) else qualify
        bare_candidates.extend(
            _collect_cols_shallow(qualify_node, alias_map, resolver, default_table)
        )
    if bare_candidates:
        for g in result_groups:
            captured = {f.column for f in g.filters} | set(g.bare_columns)
            g.bare_columns = g.bare_columns + [
                c for c in bare_candidates if c not in captured
            ]

    return result_groups


def _walk_tree_grouped(
    node: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    cte_groups_map: dict[str, list[ConstraintGroup]],
) -> list[ConstraintGroup]:
    """Recursively walk a SELECT / UNION / Subquery tree, returning grouped constraints."""
    if isinstance(node, exp.Select):
        return _walk_select_grouped(node, dict(alias_map), resolver, cte_groups_map)
    if isinstance(node, exp.Union):
        left = _walk_tree_grouped(node.left, alias_map, resolver, cte_groups_map)
        right = _walk_tree_grouped(node.right, alias_map, resolver, cte_groups_map)
        return (left + right)[:_MAX_CONSTRAINT_GROUPS]
    if isinstance(node, exp.Subquery):
        return _walk_tree_grouped(node.this, alias_map, resolver, cte_groups_map)
    return [ConstraintGroup()]


# ─── Null-contradiction filter ────────────────────────────────────────────────

_IS_NULL_EXACT = re.compile(r"^(\S+)\s+IS\s+NULL$", re.IGNORECASE)
_IS_NOT_NULL_EXACT_1 = re.compile(r"^NOT\s+(\S+)\s+IS\s+NULL$", re.IGNORECASE)
_IS_NOT_NULL_EXACT_2 = re.compile(r"^(\S+)\s+IS\s+NOT\s+NULL$", re.IGNORECASE)


def _remove_null_contradictions(parts: list[str]) -> list[str]:
    """Remove contradictory IS NULL / IS NOT NULL pairs from cond_parts.

    When the SQL uses separate CTEs as UNION ALL branches (e.g. one CTE that
    filters `WHERE NOT col IS NULL` and another that filters `WHERE col IS NULL`),
    the flat CTE scan collects both conditions and ANDs them — making it
    impossible for the LLM to satisfy simultaneously.  Removing both predicates
    lets the LLM choose freely which branch to satisfy (guided by the full SQL).

    Only standalone exact-match entries are removed (not conditions embedded
    inside a larger AND chain), which is the common pattern for branch-CTE
    WHERE clauses.
    """
    is_null_idxs: dict[str, list[int]] = {}
    is_not_null_idxs: dict[str, list[int]] = {}

    for i, p in enumerate(parts):
        stripped = p.strip()
        m = _IS_NULL_EXACT.match(stripped)
        if m:
            is_null_idxs.setdefault(m.group(1).lower(), []).append(i)
            continue
        m = _IS_NOT_NULL_EXACT_1.match(stripped)
        if m:
            is_not_null_idxs.setdefault(m.group(1).lower(), []).append(i)
            continue
        m = _IS_NOT_NULL_EXACT_2.match(stripped)
        if m:
            is_not_null_idxs.setdefault(m.group(1).lower(), []).append(i)

    contradictory = set(is_null_idxs) & set(is_not_null_idxs)
    if not contradictory:
        return parts

    remove: set[int] = set()
    for col in contradictory:
        remove.update(is_null_idxs[col])
        remove.update(is_not_null_idxs[col])
    return [p for i, p in enumerate(parts) if i not in remove]


# ─── Conditions hint builder ─────────────────────────────────────────────────


def build_conditions_hint(
    sql: str,
    dialect: str = "bigquery",
    schema: list[dict] | None = None,
) -> dict:
    """Build a concise LLM hint dict from *sql*.

    Returns::

        {
          "conditions":         "table.col = 'x' AND (t2.col = 'a' OR t2.col = 'b')",
          "format_constraints": ["table.col : SAFE_CAST AS FLOAT64"]
        }

    * WHERE + JOIN ON + QUALIFY predicates are collected and ANDed.
    * AND/OR structure is preserved — no DNF expansion.
    * Volume constraints (ROW_NUMBER / RANK / NTILE alias filters) are excluded.
    * CTE column aliases are resolved to their base table via lineage.
    * ``format_constraints`` lists SAFE_CAST, CAST, PARSE_DATE, FORMAT_DATE entries.

    Returns an empty dict on unparseable input.
    """
    if not sql:
        return {}
    try:
        statement = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return {}

    sqlglot_schema = _schemas_to_sqlglot(schema) if schema else None
    resolver = _LineageResolver(statement, sqlglot_schema, dialect)

    # Build a global alias map from all Table nodes + per-SELECT _collect_aliases.
    # CTE names land in resolver._cte_names; base-table aliases land in alias_map.
    alias_map: dict[str, str] = {}
    for tbl in statement.find_all(exp.Table):
        real = tbl.name.lower()
        alias = tbl.alias.lower() if tbl.alias else real
        if real:
            alias_map[alias] = real
            alias_map[real] = real
    for sel in statement.find_all(exp.Select):
        _collect_aliases(sel, alias_map)

    window_aliases = _collect_window_aliases(statement)

    def _collect_union_branches(node) -> list:
        """Recursively collect leaf SELECT nodes from a UNION tree."""
        if isinstance(node, exp.Union):
            return _collect_union_branches(node.this) + _collect_union_branches(
                node.expression
            )
        return [node]

    def _collect_select_conds(
        sel, alias_map, resolver, window_aliases, dialect
    ) -> list[str]:
        """Collect WHERE/JOIN ON/QUALIFY condition strings from a single SELECT."""
        parts: list[str] = []
        seen: set[str] = set()

        def _add(s: str | None) -> None:
            if s and s not in seen:
                seen.add(s)
                parts.append(s)

        where = sel.args.get("where")
        if where:
            _add(
                _serialize_cond(
                    where.this, alias_map, resolver, window_aliases, dialect
                )
            )
        for join in sel.args.get("joins") or []:
            on = join.args.get("on")
            if on:
                _add(_serialize_cond(on, alias_map, resolver, window_aliases, dialect))
        qualify = sel.args.get("qualify")
        if qualify:
            qualify_node = qualify.this if isinstance(qualify, exp.Qualify) else qualify
            _add(
                _serialize_cond(
                    qualify_node, alias_map, resolver, window_aliases, dialect
                )
            )
        return parts

    def _safe_and_part(s: str) -> str:
        if " OR " in s and not (s.startswith("(") and s.endswith(")")):
            return f"({s})"
        return s

    def _union_to_branch_conds(union_node) -> list[str]:
        """Return per-branch condition strings from a UNION node, deduplicated."""
        branches = _collect_union_branches(union_node)
        branch_conds: list[str] = []
        seen_branch: set[str] = set()
        for branch_sel in branches:
            parts = _collect_select_conds(
                branch_sel, alias_map, resolver, window_aliases, dialect
            )
            if parts:
                bc = " AND ".join(_safe_and_part(p) for p in parts)
                if bc not in seen_branch:
                    seen_branch.add(bc)
                    branch_conds.append(bc)
        return branch_conds

    # Identify which SELECT nodes belong to UNION branches (at any level of the tree,
    # including inside CTEs) so we can present their conditions per-branch instead of
    # ANDing contradictory constraints (e.g. year=2016 AND year=2017 AND year=2018).
    #
    # Only process "root" Union nodes — i.e. Union nodes whose parent is NOT another
    # Union. For nested UNION ALL (A UNION ALL B UNION ALL C), sqlglot builds
    # Union(Union(A,B), C). Processing only the outermost Union gives all 3 branches
    # without duplicates.
    union_branch_selects: set[int] = set()
    union_cond_parts: list[str] = []
    for union_node in statement.find_all(exp.Union):
        if isinstance(union_node.parent, exp.Union):
            continue  # skip nested; will be handled by the top-level Union
        branch_conds = _union_to_branch_conds(union_node)
        if len(branch_conds) > 1:
            labeled = " | ".join(
                f"[Branch {i + 1}] {bc}" for i, bc in enumerate(branch_conds)
            )
            union_cond_parts.append(labeled)
        elif branch_conds:
            union_cond_parts.append(branch_conds[0])
        # Mark these SELECT nodes so we skip them in the flat scan below
        for branch_sel in _collect_union_branches(union_node):
            union_branch_selects.add(id(branch_sel))

    # Collect conditions from SELECT nodes that are NOT inside a UNION branch
    cond_parts: list[str] = list(union_cond_parts)
    seen_cond: set[str] = set()
    for s in union_cond_parts:
        seen_cond.add(s)

    def _add_cond(s: str | None) -> None:
        if s and s not in seen_cond:
            seen_cond.add(s)
            cond_parts.append(s)

    for sel in statement.find_all(exp.Select):
        if id(sel) in union_branch_selects:
            continue
        for part in _collect_select_conds(
            sel, alias_map, resolver, window_aliases, dialect
        ):
            _add_cond(part)

    cond_parts = _remove_null_contradictions(cond_parts)

    conditions = " AND ".join(_safe_and_part(p) for p in cond_parts)
    format_constraints = _collect_format_constraints_strings(
        statement, alias_map, resolver, dialect
    )

    lineages: list[str] = []
    seen_lineage: set[str] = set()
    for (_tbl, _col), resolved in resolver._cache.items():
        if resolved.lineage and resolved.lineage not in seen_lineage:
            seen_lineage.add(resolved.lineage)
            col_label = f"{resolved.real_table or resolved.table}.{resolved.column}"
            lineages.append(f"{col_label} : {resolved.lineage}")

    result: dict = {}
    if conditions:
        result["conditions"] = conditions
    if format_constraints:
        result["format_constraints"] = format_constraints
    if lineages:
        result["lineages"] = lineages
    return result


# ─── Main extractor ───────────────────────────────────────────────────────────


def extract_constraints(
    sql: str,
    dialect: str = "bigquery",
    schema: list[dict] | None = None,
) -> list[ConstraintGroup]:
    """Parse *sql* and return one ConstraintGroup per independent satisfying path.

    Each group is a complete set of constraints (filters, join equalities, functional
    dependencies, anti-join pairs) that, when satisfied simultaneously, produces at
    least one output row.

    Multiple groups arise from:
      • UNION ALL branches  (independent, concatenated)
      • OR in WHERE         (DNF expansion, cartesian product within a SELECT)
      • CTE / subquery OR   (cross-multiplied with the outer SELECT's groups)

    Args:
        schema: project schema from get_schemas(), used by sqlglot lineage for
                accurate CTE resolution and type-aware column tracking.
    """
    _t0 = time.monotonic()
    statement = sqlglot.parse_one(sql, dialect=dialect)
    sqlglot_schema = _schemas_to_sqlglot(schema) if schema else None
    resolver = _LineageResolver(statement, sqlglot_schema, dialect)

    # Process CTEs in definition order so later CTEs can reference earlier ones
    cte_groups_map: dict[str, list[ConstraintGroup]] = {}
    with_clause = statement.args.get("with_") if hasattr(statement, "args") else None
    if with_clause:
        for cte in with_clause.expressions or []:
            cte_name = (cte.alias or "").lower()
            inner = cte.this
            if inner is not None and cte_name:
                cte_groups_map[cte_name] = _walk_tree_grouped(
                    inner, {}, resolver, cte_groups_map
                )

    groups = _walk_tree_grouped(statement, {}, resolver, cte_groups_map)
    logger.debug(
        "extract_constraints: %.1fms — groups=%d",
        (time.monotonic() - _t0) * 1000,
        len(groups),
    )
    return groups


# ─── HAVING cardinality guard ─────────────────────────────────────────────────

_HAVING_MAX_ROWS = 20


def check_having_cardinality(
    sql: str,
    dialect: str = "bigquery",
    threshold: int = _HAVING_MAX_ROWS,
) -> None:
    """Raise ValueError if the SQL has a HAVING clause that requires more than *threshold* rows.

    Detects patterns like ``HAVING COUNT(*) > 150`` or ``HAVING rainy_days > 150``
    (where rainy_days is an alias of COUNT(*)).  Both > and >= are handled.
    Raises early — before any LLM call — so the user gets a clear message instead
    of a bad test that silently produces 0 results.
    """
    try:
        statement = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return  # unparseable SQL — let downstream handle it

    for having in statement.find_all(exp.Having):
        for pred in _flatten_and(having.this):
            _check_having_threshold_pred(pred, threshold, dialect)


def _check_having_threshold_pred(
    pred: exp.Expression, threshold: int, dialect: str
) -> None:
    op_type = type(pred)
    if op_type not in (exp.GT, exp.GTE, exp.LT, exp.LTE):
        return

    left = pred.args.get("this")
    right = pred.args.get("expression")
    if left is None or right is None:
        return

    # Normalise to  expr <effective_op> literal  (flip when literal is on the left)
    _flip = {exp.GT: exp.LT, exp.GTE: exp.LTE, exp.LT: exp.GT, exp.LTE: exp.GTE}
    if _is_literal(right) and not _is_literal(left):
        _, lit_node, effective_op = left, right, op_type
    elif _is_literal(left) and not _is_literal(right):
        _, lit_node, effective_op = right, left, _flip[op_type]
    else:
        return

    if effective_op not in (exp.GT, exp.GTE):
        return  # LT / LTE on expr side → no minimum rows imposed

    val = _literal_value(lit_node)
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        return

    # Minimum rows required to satisfy the predicate.
    # threshold + 1 is the effective limit: HAVING x > threshold needs threshold+1 rows,
    # which the user confirmed still passes ("having >20 ça passe au max").
    rows_needed = int(val) + (1 if effective_op is exp.GT else 0)
    if rows_needed <= threshold + 1:
        return

    cond_sql = pred.sql(dialect=dialect)
    raise ValueError(
        f"Ce script demande la génération de beaucoup trop de lignes : "
        f"la condition HAVING `{cond_sql}` requiert au moins {rows_needed} ligne(s) par groupe, "
        f"mais MockSQL est limité à {threshold} lignes max. "
        f"Simplifiez la requête ou abaissez le seuil du HAVING."
    )


# ─── Correlated-aggregate cardinality guard ───────────────────────────────────


def _find_aggregate_cte_columns(
    statement: exp.Expression,
) -> dict[tuple[str, str], str]:
    """Return {(cte_alias, col_name): agg_func} for CTE columns defined as scalar subquery aggregates.

    Detects patterns like:
        cte AS (SELECT (SELECT COUNT(*) FROM t WHERE ...) AS col_name FROM ...)
    where the column's value is the result of an aggregate function — meaning
    the number of rows in t must satisfy any threshold applied to col_name.
    """
    result: dict[tuple[str, str], str] = {}
    with_clause = statement.args.get("with_") if hasattr(statement, "args") else None
    if not with_clause:
        return result

    for cte in with_clause.expressions or []:
        cte_name = (cte.alias or "").lower()
        if not cte_name:
            continue
        inner = cte.this
        if not isinstance(inner, exp.Select):
            continue

        for proj in inner.expressions:
            if not isinstance(proj, exp.Alias):
                continue
            alias_name = (proj.alias or "").lower()
            expr = proj.this

            if not isinstance(expr, exp.Subquery):
                continue
            inner_sel = expr.this
            if not isinstance(inner_sel, exp.Select):
                continue

            for inner_proj in inner_sel.expressions:
                agg_expr = (
                    inner_proj.this if isinstance(inner_proj, exp.Alias) else inner_proj
                )
                for agg_node in agg_expr.find_all(exp.AggFunc):
                    result[(cte_name, alias_name)] = type(agg_node).__name__.upper()
                    break
                if (cte_name, alias_name) in result:
                    break

    return result


def _check_aggregate_col_threshold(
    pred: exp.Expression,
    agg_cols: dict[tuple[str, str], str],
    threshold: int,
    dialect: str,
) -> None:
    op_type = type(pred)
    if op_type not in (exp.GT, exp.GTE, exp.LT, exp.LTE):
        return

    left = pred.args.get("this")
    right = pred.args.get("expression")
    if left is None or right is None:
        return

    _flip = {exp.GT: exp.LT, exp.GTE: exp.LTE, exp.LT: exp.GT, exp.LTE: exp.GTE}
    if _is_literal(right) and _is_column(left):
        col_node, lit_node, effective_op = left, right, op_type
    elif _is_literal(left) and _is_column(right):
        col_node, lit_node, effective_op = right, left, _flip[op_type]
    else:
        return

    if effective_op not in (exp.GT, exp.GTE):
        return

    val = _literal_value(lit_node)
    if not isinstance(val, (int, float)) or isinstance(val, bool):
        return

    rows_needed = int(val) + (1 if effective_op is exp.GT else 0)
    if rows_needed <= threshold + 1:
        return

    table_alias = (col_node.table or "").lower()
    col_name = (col_node.name or "").lower()

    agg_func = agg_cols.get((table_alias, col_name))
    if agg_func is None and not table_alias:
        for (_, cn), func in agg_cols.items():
            if cn == col_name:
                agg_func = func
                break

    if agg_func is None:
        return

    cte_label = table_alias or col_name
    cond_sql = pred.sql(dialect=dialect)
    raise ValueError(
        f"Ce script demande la génération de beaucoup trop de lignes : "
        f"la condition `{cond_sql}` requiert au moins {rows_needed} ligne(s) de données "
        f"car `{col_name}` dans la CTE `{cte_label}` est calculé par {agg_func}(*), "
        f"mais MockSQL est limité à {threshold} lignes max. "
        f"Simplifiez la requête ou abaissez le seuil."
    )


def check_correlated_aggregate_cardinality(
    sql: str,
    dialect: str = "bigquery",
    threshold: int = _HAVING_MAX_ROWS,
) -> None:
    """Raise ValueError if a WHERE filters on a CTE aggregate column above the row threshold.

    Detects patterns like:
        WITH cte AS (SELECT (SELECT COUNT(*) FROM t WHERE ...) AS n FROM ...)
        SELECT ... WHERE cte.n > 150
    where satisfying the WHERE would require more than *threshold* rows in t.
    """
    try:
        statement = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return

    agg_cols = _find_aggregate_cte_columns(statement)
    if not agg_cols:
        return

    for sel in statement.find_all(exp.Select):
        where = sel.args.get("where")
        if not where:
            continue
        for pred in _flatten_and(where.this):
            _check_aggregate_col_threshold(pred, agg_cols, threshold, dialect)


# ─── Simplifier ───────────────────────────────────────────────────────────────


def _process_constraint_group(group: ConstraintGroup) -> SimplificationResult:
    """Apply Union-Find to a ConstraintGroup → SimplificationResult (no sub-groups)."""
    seen_f: set[tuple] = set()
    filters: list[FilterConstraint] = []
    for f in group.filters:
        key = (f.column, f.op, repr(f.value))
        if key not in seen_f:
            seen_f.add(key)
            filters.append(f)

    seen_eq: set[frozenset] = set()
    equalities: list[tuple[ColumnRef, ColumnRef]] = []
    for a, b in group.equalities:
        key: frozenset = frozenset({a, b})
        if key not in seen_eq:
            seen_eq.add(key)
            equalities.append((a, b))

    seen_fc: set[tuple] = set()
    functional: list[FunctionalConstraint] = []
    for fc in group.functional:
        key = (fc.derived, fc.source, fc.func)
        if key not in seen_fc:
            seen_fc.add(key)
            functional.append(fc)

    seen_ci: set[frozenset] = set()
    col_inequalities: list[tuple[ColumnRef, ColumnRef]] = []
    for a, b in group.col_inequalities:
        key = frozenset({a, b})
        if key not in seen_ci:
            seen_ci.add(key)
            col_inequalities.append((a, b))

    uf = _UnionFind()
    for a, b in equalities:
        uf.union(a, b)
    for f in filters:
        uf.add(f.column)
    func_derived: set[ColumnRef] = set()
    for fc in functional:
        uf.add(fc.source)
        uf.add(fc.derived)
        func_derived.add(fc.derived)

    filter_index: dict[ColumnRef, list[FilterConstraint]] = {}
    for f in filters:
        filter_index.setdefault(f.column, []).append(f)

    result = SimplificationResult(
        filters=filters, functional=functional, col_inequalities=col_inequalities
    )
    result.equivalence_classes = uf.groups()

    for grp in uf.all_groups():
        candidates = [c for c in grp if c in filter_index]
        rep = min(candidates) if candidates else min(grp)
        for col_ref in grp:
            if col_ref == rep:
                continue
            if col_ref not in func_derived:
                result.derived_columns[col_ref] = (rep, str(rep))
        if rep not in func_derived:
            result.source_columns[rep] = filter_index.get(rep, [])

    for fc in functional:
        src = uf.find(fc.source)
        if src not in result.source_columns and src not in result.derived_columns:
            result.source_columns[src] = filter_index.get(src, [])
        result.derived_columns[fc.derived] = (fc.source, f"{fc.func}({fc.source})")

    for col_ref in group.bare_columns:
        uf.add(col_ref)
        rep = uf.find(col_ref)
        if rep not in result.source_columns and rep not in result.derived_columns:
            result.source_columns[rep] = filter_index.get(rep, [])

    return result


def simplify(
    sql: str, dialect: str = "bigquery", schema: list[dict] | None = None
) -> SimplificationResult:
    """Analyse *sql* and return a SimplificationResult.

    Calls extract_constraints() to get one ConstraintGroup per satisfying path,
    then applies Union-Find per group to build source_columns / derived_columns /
    equivalence_classes.

    When more than one group is found, result.constraint_groups is populated with
    one processed SimplificationResult per group, and the flat fields (filters,
    source_columns, …) reflect the union of all groups.
    """
    _t0 = time.monotonic()
    groups = extract_constraints(sql, dialect, schema)

    if not groups:
        return SimplificationResult()

    if len(groups) == 1:
        result = _process_constraint_group(groups[0])
    else:
        # Multiple paths: build per-group results + merged flat view
        all_filters = [f for g in groups for f in g.filters]
        all_equalities = [e for g in groups for e in g.equalities]
        all_functional = [fc for g in groups for fc in g.functional]
        all_col_inequalities = [ci for g in groups for ci in g.col_inequalities]
        all_bare = list({c for g in groups for c in g.bare_columns})
        flat = _process_constraint_group(
            ConstraintGroup(
                filters=all_filters,
                equalities=all_equalities,
                functional=all_functional,
                col_inequalities=all_col_inequalities,
                bare_columns=all_bare,
            )
        )
        result = SimplificationResult(
            source_columns=flat.source_columns,
            derived_columns=flat.derived_columns,
            equivalence_classes=flat.equivalence_classes,
            filters=flat.filters,
            functional=flat.functional,
            col_inequalities=flat.col_inequalities,
            constraint_groups_truncated=len(groups) >= _MAX_CONSTRAINT_GROUPS,
        )
        for g in groups:
            result.constraint_groups.append(_process_constraint_group(g))

    logger.debug(
        "simplify: %.1fms — source_cols=%d derived=%d equiv=%d groups=%d truncated=%s",
        (time.monotonic() - _t0) * 1000,
        len(result.source_columns),
        len(result.derived_columns),
        len(result.equivalence_classes),
        len(result.constraint_groups),
        result.constraint_groups_truncated,
    )
    return result


# ─── Derived-expression detection (for profiler) ─────────────────────────────

# sqlglot expression types that are too trivial to profile (output is a
# predictable/semantic-free transformation of input columns already profiled).
_TRIVIAL_FUNC_TYPES: frozenset[type] = frozenset(
    {
        exp.Upper,
        exp.Lower,
        exp.Trim,
        exp.Concat,
        exp.DPipe,  # || string concat
        exp.Length,
        exp.ByteLength,
        exp.BitLength,
        exp.Abs,
        exp.Round,
        exp.Floor,
        exp.Ceil,
        exp.Mod,
        # sqlglot makes And/Or/Xor inherit from both Connector and Func — exclude them
        # so find_all(exp.Func) doesn't surface logical operators as function calls.
        exp.Connector,
        # aggregate functions (SUM, COUNT, AVG…) — input columns are already profiled
        exp.AggFunc,
        # table-valued functions used in FROM clauses — not usable as scalar column
        # expressions; including these produces invalid SQL in profiling queries
        exp.Unnest,
        # complex-typed constructors — MIN/CAST/DISTINCT fail on STRUCT and ARRAY
        exp.Struct,
        exp.Array,
    }
)

# Same exclusion list for Anonymous nodes (function names sqlglot doesn't
# map to a named expression type).
_TRIVIAL_FUNC_NAMES: frozenset[str] = frozenset(
    {
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "LENGTH",
        "LEN",
        "CHAR_LENGTH",
        "CHARACTER_LENGTH",
        "SUBSTR",
        "SUBSTRING",
        "CONCAT",
        "CONCAT_WS",
        "ROUND",
        "FLOOR",
        "CEIL",
        "CEILING",
        "ABS",
        "MOD",
        "REPLACE",
        "LPAD",
        "RPAD",
        "LEFT",
        "RIGHT",
        "REPEAT",
        "REVERSE",
        "SPACE",
        "ASCII",
        "CHR",
        "CHAR",
        "TO_STRING",
        "TO_VARCHAR",
        "CAST",
        "CONVERT",
    }
)


def detect_select_derived_expressions(
    sql: str,
    dialect: str = "bigquery",
) -> list[dict]:
    """Find interesting derived expressions in SQL SELECT projections.

    Reuses the CTE lineage resolver and alias infrastructure from
    :func:`extract_constraints` so CTE-sourced columns are traced back to their
    real base tables.

    Returns a list (capped at 10) of::

        {
          "expr_sql":      str,        — expression as SQL in *dialect*
          "source_tables": list[str],  — resolved base-table names (alias stripped)
          "col_refs":      list[(alias, col_name)],  — raw column refs in expression
        }

    Expressions with no column references (constants) are skipped.
    Deduplication is by ``expr_sql``.

    Detects any ``exp.Func`` node in a SELECT projection, excluding trivial
    transformations listed in :data:`_TRIVIAL_FUNC_TYPES` /
    :data:`_TRIVIAL_FUNC_NAMES` (``UPPER``, ``LOWER``, ``ROUND``, arithmetic,
    etc.). Covers ``COALESCE``, ``SAFE_CAST``, ``REGEXP_EXTRACT``, date parsers,
    and any other non-trivial function call.

    Examples:
        >>> exprs = detect_select_derived_expressions(
        ...     "SELECT SAFE_CAST(t.v AS INT64), COALESCE(t.x, t.y) FROM tbl t"
        ... )
        >>> len(exprs) >= 1
        True
        >>> any("COALESCE" in e["expr_sql"].upper() for e in exprs)
        True
        >>> exprs[0]["source_tables"]
        ['tbl']
    """
    try:
        statement = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return []

    resolver = _LineageResolver(statement, None, dialect)

    # Build alias map from every Table node in the AST — more robust than
    # _collect_aliases which relies on "from" key (renamed "from_" in newer sqlglot).
    alias_map: dict[str, str] = {}
    for tbl in statement.find_all(exp.Table):
        parts = [p for p in [tbl.catalog, tbl.db, tbl.name] if p]
        real_full = ".".join(parts).lower()
        real_short = tbl.name.lower()
        alias = tbl.alias.lower() if tbl.alias else real_full
        alias_map[alias] = real_full
        alias_map[real_short] = real_full
        alias_map[real_full] = real_full

    seen: set[str] = set()
    results: list[dict] = []

    # Exclude CTEs — they are virtual, not base tables
    unique_real_tables: frozenset[str] = frozenset(
        t for t in alias_map.values() if t not in resolver._cte_names
    )

    def _source_tables_for(node: exp.Expression) -> list[str]:
        tables: set[str] = set()
        for col in node.find_all(exp.Column):
            if not col.name:
                continue
            ref = _col_ref(col, alias_map)
            if ref is None:
                continue
            if ref.table == "__unknown__":
                # Unqualified column — infer source table only when unambiguous
                if len(unique_real_tables) == 1:
                    tables.update(unique_real_tables)
                continue
            resolved_list = resolver.resolve_all(ref)
            for resolved in resolved_list:
                tbl = resolved.real_table or resolved.table
                if tbl and tbl != "__unknown__":
                    tables.add(tbl)
        return sorted(tables)

    def _col_refs_for(node: exp.Expression) -> list[tuple[str, str]]:
        return [(c.table or "", c.name) for c in node.find_all(exp.Column) if c.name]

    def _register(node: exp.Expression) -> None:
        col_refs = _col_refs_for(node)
        if not col_refs:  # pure constant — no column references
            return
        sql_repr = node.sql(dialect=dialect)
        if sql_repr in seen:
            return
        seen.add(sql_repr)
        results.append(
            {
                "expr_sql": sql_repr,
                "source_tables": _source_tables_for(node),
                "col_refs": col_refs,
            }
        )

    def _is_trivial(node: exp.Func) -> bool:
        if isinstance(node, tuple(_TRIVIAL_FUNC_TYPES)):
            return True
        return (
            isinstance(node, exp.Anonymous)
            and (node.name or "").upper() in _TRIVIAL_FUNC_NAMES
        )

    def _scan_clause(clause: exp.Expression) -> None:
        for node in clause.find_all(exp.Func):
            if not _is_trivial(node):
                _register(node)

    # Walk every SELECT — projections + WHERE, GROUP BY, QUALIFY, HAVING.
    for sel in statement.find_all(exp.Select):
        for projection in sel.expressions:
            _scan_clause(projection)
        for clause_key in ("where", "group", "qualify", "having"):
            clause = sel.args.get(clause_key)
            if clause is not None:
                _scan_clause(clause)
    return results[:10]


# ─── Structural volume hints (OFFSET, NTILE, RANK/ROW_NUMBER filters) ──────────


@dataclass
class VolumeHint:
    """A structural SQL clause that imposes a minimum row count at a specific scope."""

    hint_type: str  # "offset" | "ntile" | "rank_filter"
    context: (
        str  # human label: "CTE `paginated`" | "sous-requête `sub`" | "SELECT final"
    )
    min_rows: int  # minimum rows needed in the sources of that scope
    clause_sql: str  # display string, e.g. "OFFSET 3" or "RANK() <= 5 (via `rn`)"


def _volume_context_label(node: exp.Expression) -> str:
    """Walk up from *node* to find the nearest enclosing CTE or named subquery."""
    p = node.parent
    while p:
        if isinstance(p, exp.CTE):
            return f"CTE `{p.alias}`"
        if isinstance(p, exp.Subquery):
            alias = p.alias
            return f"sous-requête `{alias}`" if alias else "sous-requête anonyme"
        p = p.parent
    return "SELECT final"


def _flatten_and_volume(node: exp.Expression) -> list[exp.Expression]:
    if isinstance(node, exp.And):
        return _flatten_and_volume(node.args["this"]) + _flatten_and_volume(
            node.args["expression"]
        )
    return [node]


def extract_volume_hints(sql: str, dialect: str = "bigquery") -> list[VolumeHint]:
    """Return structural volume requirements inferred from the SQL AST.

    Detects three patterns, each with its enclosing CTE or subquery name:

    * **OFFSET N** — the scope must produce at least N+1 rows so that at least
      one row survives the skip.
    * **NTILE(N)** — the scope needs at least N rows to fill every bucket.
    * **RANK / ROW_NUMBER alias filtered by <= N** — the CTE that defines the
      window alias must contain at least N rows (per partition if PARTITION BY
      is present).
    """
    if not sql:
        return []
    try:
        tree = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return []

    hints: list[VolumeHint] = []

    # ── OFFSET N ──────────────────────────────────────────────────────────────
    for node in tree.walk():
        if isinstance(node, exp.Offset):
            val = node.args.get("expression")
            if not isinstance(val, exp.Literal):
                continue
            try:
                n = int(val.this)
            except (ValueError, TypeError):
                continue
            ctx = _volume_context_label(node)
            hints.append(
                VolumeHint(
                    hint_type="offset",
                    context=ctx,
                    min_rows=n + 1,
                    clause_sql=f"OFFSET {n}",
                )
            )

    # ── NTILE(N) ──────────────────────────────────────────────────────────────
    for node in tree.walk():
        if isinstance(node, exp.Window) and isinstance(node.this, exp.Ntile):
            arg = node.this.args.get("this")
            if not isinstance(arg, exp.Literal):
                continue
            try:
                n = int(arg.this)
            except (ValueError, TypeError):
                continue
            ctx = _volume_context_label(node)
            hints.append(
                VolumeHint(
                    hint_type="ntile",
                    context=ctx,
                    min_rows=n,
                    clause_sql=f"NTILE({n})",
                )
            )

    # ── RANK / ROW_NUMBER alias filtered by <= N ──────────────────────────────
    # Step 1: collect all window aliases (rn, rnk, …) defined in CTEs/subqueries.
    window_aliases: dict[str, tuple[str, str]] = {}  # lower_alias -> (ctx, fn_name)
    for node in tree.walk():
        if not isinstance(node, exp.Window):
            continue
        if not isinstance(node.this, (exp.RowNumber, exp.Rank, exp.DenseRank)):
            continue
        alias_node = node.parent
        if not isinstance(alias_node, exp.Alias):
            continue
        alias_name = alias_node.alias.lower()
        ctx = _volume_context_label(node)
        fn_name = type(node.this).__name__.upper()
        window_aliases[alias_name] = (ctx, fn_name)

    # Step 2: find WHERE / QUALIFY that applies <= N or = 1 to a known alias.
    seen_alias_filters: set[str] = set()
    for node in tree.walk():
        if not isinstance(node, (exp.Where, exp.Qualify)):
            continue
        for pred in _flatten_and_volume(node.this):
            if not isinstance(pred, (exp.LTE, exp.LT, exp.EQ)):
                continue
            left = pred.args.get("this")
            right = pred.args.get("expression")
            if not (isinstance(right, exp.Literal) and isinstance(left, exp.Column)):
                continue
            col_name = left.name.lower()
            if col_name not in window_aliases:
                continue
            try:
                n = int(right.this)
            except (ValueError, TypeError):
                continue
            if isinstance(pred, exp.LTE):
                min_rows = n
            elif isinstance(pred, exp.LT):
                min_rows = max(1, n - 1)
            else:  # EQ
                min_rows = n
            if min_rows < 1 or col_name in seen_alias_filters:
                continue
            seen_alias_filters.add(col_name)
            ctx, fn_name = window_aliases[col_name]
            op = (
                "<="
                if isinstance(pred, exp.LTE)
                else ("<" if isinstance(pred, exp.LT) else "=")
            )
            hints.append(
                VolumeHint(
                    hint_type="rank_filter",
                    context=ctx,
                    min_rows=min_rows,
                    clause_sql=f"{fn_name}() {op} {n} (via alias `{col_name}`)",
                )
            )

    return hints
