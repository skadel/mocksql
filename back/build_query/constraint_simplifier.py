"""
constraint_simplifier.py — SQL constraint extraction and simplification.

Public API:
    simplify(sql, dialect="bigquery") -> SimplificationResult

Given a SQL query the function:
  1. Resolves CTE column lineage back to base tables.
  2. Extracts three kinds of constraints:
       • FilterConstraint  – column <op> literal  (WHERE / ON predicates)
       • EqualityConstraint – col = col            (join keys, simple equalities)
       • FunctionalConstraint – col = f(col)       (TRIM, UPPER, …)
  3. Builds Union-Find equivalence classes from column equalities.
  4. Simplifies to a minimal generation set:
       • source_columns  – must be generated (one rep per equivalence class + filtered cols)
       • derived_columns – can be computed from a source
       • equivalence_classes – groups of columns sharing the same value
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import sqlglot
from sqlglot import expressions as exp
from sqlglot.optimizer.simplify import simplify as sg_simplify


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
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Neg):
        inner = _literal_value(node.this)
        return -inner if inner is not None else None
    # e.g. CAST('2025-01-01' AS DATE) — return the inner string
    if isinstance(node, (exp.Cast, exp.TryCast)):
        return _literal_value(node.this)
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


def _sql_of(node: exp.Expression) -> str:
    return node.sql(dialect="bigquery")


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

    from_clause = source.args.get("from")
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

    def resolve(self, col: ColumnRef) -> ColumnRef:
        """Return a base-table ColumnRef with a SQL lineage string.

        Calls sqlglot.lineage.lineage() for CTE-sourced columns;
        passes base-table columns through unchanged.
        """
        key = (col.table, col.column)
        if key in self._cache:
            return self._cache[key]
        resolved = (
            self._resolve_via_lineage(col) if col.table in self._cte_names else col
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
        if col.table not in self._cte_names:
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
        """
        wrapper = sqlglot.parse_one(
            f"SELECT {col.column} FROM {col.table}",
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
        except Exception:
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
                    except Exception:
                        lineage_sql = n.name
            return ColumnRef(base_table, base_col, lineage=lineage_sql)
        except Exception:
            return col


# ─── Alias collectors ─────────────────────────────────────────────────────────


def _collect_aliases(select: exp.Select, alias_map: dict[str, str]) -> None:
    """Populate alias_map with {alias_lower → real_table_lower} from FROM/JOINs."""
    from_clause = select.args.get("from")
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
                alias_map[alias] = alias


# ─── Condition flattening ─────────────────────────────────────────────────────


def _flatten_and(cond: exp.Expression) -> list[exp.Expression]:
    """Recursively flatten AND-chains into a flat list of predicates."""
    if isinstance(cond, exp.And):
        return _flatten_and(cond.left) + _flatten_and(cond.right)
    return [cond]


def _collect_safe_cast_constraints(
    select: exp.Select,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
) -> None:
    """Add safe_cast_not_null constraints for each SAFE_CAST in *select*.

    Scans SELECT projections, WHERE, and JOIN-ON expressions.
    Does not descend into subqueries or CTE definitions — those are handled
    by the recursive _walk_tree / CTE loop in extract_constraints.
    """

    def _iter_try_casts(node: exp.Expression):
        """Yield TryCast nodes, not descending into Subquery or With subtrees."""
        for child in node.args.values():
            items = child if isinstance(child, list) else [child]
            for item in items:
                if not isinstance(item, exp.Expression):
                    continue
                if isinstance(item, (exp.Subquery, exp.With)):
                    continue
                if isinstance(item, exp.TryCast):
                    yield item
                yield from _iter_try_casts(item)

    seen: set[tuple[str, str, str]] = set()
    for try_cast in _iter_try_casts(select):
        inner_col = _find_column_in(try_cast.this)
        if inner_col is None:
            continue
        ref = _col_ref(inner_col, alias_map)
        if ref is None:
            continue
        src_cols = resolver.resolve_all(ref)
        ref = resolver.resolve(ref)
        to_type = try_cast.args.get("to")
        type_str = to_type.sql(dialect="bigquery").upper() if to_type else ""
        key = (ref.table, ref.column, type_str)
        if key not in seen:
            seen.add(key)
            filters.append(
                FilterConstraint(
                    column=ref,
                    op="safe_cast_not_null",
                    value=type_str,
                    source_columns=src_cols,
                )
            )


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
        elif isinstance(inner, exp.Like) and _is_column(inner.this):
            raw = _col_ref(inner.this, alias_map)
            pat_node = inner.args.get("expression") or inner.args.get("pattern")
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
        ref_derived = _col_ref(left, alias_map)
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
                            func=right_func,
                        )
                    )
        return

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


# ─── Walk entire AST for JOINs + WHEREs ───────────────────────────────────────


def _walk_select(
    select: exp.Select,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
    equalities: list[tuple[ColumnRef, ColumnRef]],
    functional: list[FunctionalConstraint],
    col_inequalities: list[tuple[ColumnRef, ColumnRef]],
) -> None:
    """Extract constraints from a single SELECT node."""
    _collect_aliases(select, alias_map)

    # WHERE — collect IS NULL columns first for anti-join detection below
    where = select.args.get("where")
    where_is_null_cols: set[ColumnRef] = set()
    if where:
        where_is_null_cols = _collect_is_null_cols(where.this, alias_map, resolver)
        _extract_from_condition(
            where.this, alias_map, resolver, filters, equalities, functional
        )

    # ON clauses of every JOIN
    for join in select.args.get("joins") or []:
        on = join.args.get("on")
        if not on:
            continue

        join_side = (join.args.get("side") or "").upper()
        is_outer_join = join_side in {"LEFT", "RIGHT", "FULL"}

        if is_outer_join and where_is_null_cols:
            # Anti-join pattern: a LEFT/RIGHT/FULL JOIN whose nullable side has an
            # IS NULL filter in WHERE means "rows with no match in the other table".
            # The ON equality is NOT a true value equality — skip those pairs.
            # Filters from ON (e.g. ON … AND b.status = 'active') are kept.
            tmp_eq: list[tuple[ColumnRef, ColumnRef]] = []
            tmp_func: list[FunctionalConstraint] = []
            _extract_from_condition(on, alias_map, resolver, filters, tmp_eq, tmp_func)
            for a, b in tmp_eq:
                if a not in where_is_null_cols and b not in where_is_null_cols:
                    equalities.append((a, b))
                else:
                    col_inequalities.append((a, b))
            for fc in tmp_func:
                if (
                    fc.derived not in where_is_null_cols
                    and fc.source not in where_is_null_cols
                ):
                    functional.append(fc)
        else:
            _extract_from_condition(
                on, alias_map, resolver, filters, equalities, functional
            )

    # Scan SELECT projections, WHERE, and JOIN-ON for SAFE_CAST occurrences
    _collect_safe_cast_constraints(select, alias_map, resolver, filters)


def _walk_tree(
    node: exp.Expression,
    alias_map: dict[str, str],
    resolver: _LineageResolver,
    filters: list[FilterConstraint],
    equalities: list[tuple[ColumnRef, ColumnRef]],
    functional: list[FunctionalConstraint],
    col_inequalities: list[tuple[ColumnRef, ColumnRef]],
) -> None:
    """Recursively walk SELECT / UNION trees."""
    if isinstance(node, exp.Select):
        _walk_select(
            node,
            dict(alias_map),
            resolver,
            filters,
            equalities,
            functional,
            col_inequalities,
        )
    elif isinstance(node, exp.Union):
        _walk_tree(
            node.left,
            alias_map,
            resolver,
            filters,
            equalities,
            functional,
            col_inequalities,
        )
        _walk_tree(
            node.right,
            alias_map,
            resolver,
            filters,
            equalities,
            functional,
            col_inequalities,
        )
    elif isinstance(node, exp.Subquery):
        _walk_tree(
            node.this,
            alias_map,
            resolver,
            filters,
            equalities,
            functional,
            col_inequalities,
        )


# ─── Main extractor ───────────────────────────────────────────────────────────


def extract_constraints(
    sql: str,
    dialect: str = "bigquery",
    schema: list[dict] | None = None,
) -> tuple[
    list[FilterConstraint],
    list[tuple[ColumnRef, ColumnRef]],
    list[FunctionalConstraint],
    list[tuple[ColumnRef, ColumnRef]],
]:
    """
    Parse *sql* and return (filters, equalities, functional, col_inequalities).

    - filters          : FilterConstraint list
    - equalities       : list of (ColumnRef, ColumnRef) pairs that must be equal
    - functional       : FunctionalConstraint list
    - col_inequalities : anti-join pairs — (a, b) means "rows where a has no match on b"
                         (LEFT/RIGHT/FULL JOIN … ON a = b WHERE b IS NULL patterns)

    Args:
        schema: project schema from get_schemas(), used by sqlglot lineage for
                accurate CTE resolution and type-aware column tracking.
    """
    statement = sqlglot.parse_one(sql, dialect=dialect)
    sqlglot_schema = _schemas_to_sqlglot(schema) if schema else None
    resolver = _LineageResolver(statement, sqlglot_schema, dialect)
    alias_map: dict[str, str] = {}

    filters: list[FilterConstraint] = []
    equalities: list[tuple[ColumnRef, ColumnRef]] = []
    functional: list[FunctionalConstraint] = []
    col_inequalities: list[tuple[ColumnRef, ColumnRef]] = []

    # Walk each CTE body first (constraints inside CTEs are real constraints)
    with_clause = statement.args.get("with_") if hasattr(statement, "args") else None
    if with_clause:
        for cte in with_clause.expressions or []:
            inner = cte.this
            if inner is not None:
                _walk_tree(
                    inner,
                    {},
                    resolver,
                    filters,
                    equalities,
                    functional,
                    col_inequalities,
                )

    # Walk the main query body (outer SELECT / UNION)
    _walk_tree(
        statement,
        alias_map,
        resolver,
        filters,
        equalities,
        functional,
        col_inequalities,
    )

    return filters, equalities, functional, col_inequalities


# ─── Simplifier ───────────────────────────────────────────────────────────────


def simplify(
    sql: str, dialect: str = "bigquery", schema: list[dict] | None = None
) -> SimplificationResult:
    """
    Analyse *sql* and return a SimplificationResult:

        result.source_columns     – {ColumnRef: [FilterConstraint, ...]}
        result.derived_columns    – {ColumnRef: (source_ColumnRef, "FUNC(source)")}
        result.equivalence_classes – [frozenset({ColumnRef, ...}), ...]

    Algorithm
    ---------
    1. Extract filters, equalities, functional dependencies.
    2. Build a Union-Find over all columns mentioned in equalities.
    3. For each functional dependency derived = func(source):
       - source is a source column (will be generated).
       - derived is NOT unioned with source; it is tracked as derived.
    4. For each equivalence class:
       - Pick a representative: prefer a column with a filter constraint.
       - Mark all others as derived from the representative (propagation).
    5. Functional-derived columns are marked as derived (not generated).
    """
    filters, equalities, functional, col_inequalities = extract_constraints(
        sql, dialect, schema
    )

    uf = _UnionFind()

    # Register all columns from equalities
    for a, b in equalities:
        uf.union(a, b)

    # Register columns that only appear in filters (not in any equality)
    for f in filters:
        uf.add(f.column)

    # Register functional columns (do NOT union — they are derived, not equivalent)
    func_sources: set[ColumnRef] = set()
    func_derived: set[ColumnRef] = set()
    for fc in functional:
        uf.add(fc.source)
        uf.add(fc.derived)
        func_sources.add(fc.source)
        func_derived.add(fc.derived)

    # Build filter index: col → [FilterConstraint]
    filter_index: dict[ColumnRef, list[FilterConstraint]] = {}
    for f in filters:
        filter_index.setdefault(f.column, []).append(f)

    result = SimplificationResult(
        filters=filters, functional=functional, col_inequalities=col_inequalities
    )

    # Collect equivalence classes (non-singleton groups)
    result.equivalence_classes = uf.groups()

    # For each equivalence class, pick a representative and mark others as derived
    processed: set[ColumnRef] = set()

    for group in uf.all_groups():
        # Choose representative: prefer column with a filter; otherwise pick smallest
        candidates_with_filter = [c for c in group if c in filter_index]
        rep = min(candidates_with_filter) if candidates_with_filter else min(group)

        for col in group:
            if col == rep:
                continue
            # Mark as derived from rep (simple propagation, no function)
            if col not in func_derived:
                result.derived_columns[col] = (rep, str(rep))
            processed.add(col)

        # The representative becomes a source (if not already a functional derived)
        if rep not in func_derived:
            result.source_columns[rep] = filter_index.get(rep, [])
            processed.add(rep)

    # Handle functional constraints
    for fc in functional:
        src = uf.find(fc.source)
        # Ensure source has an entry in source_columns
        if src not in result.source_columns and src not in result.derived_columns:
            result.source_columns[src] = filter_index.get(src, [])
        # Mark derived as derived with the function expression
        expr_str = f"{fc.func}({fc.source})"
        result.derived_columns[fc.derived] = (fc.source, expr_str)

    return result
