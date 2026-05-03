"""
profiler.py — Schema profiling engine.

Public API:
    profile_schema(schema, sql_executor, options) -> dict
    profile_joins_for_query(schema, sql_query, sql_executor, options) -> list[dict]

    # UI-oriented workflow (user executes the query themselves):
    build_profile_query(schema, used_columns, dialect, options) -> str
    parse_profile_query_result(rows, schema, used_columns) -> dict
"""

from __future__ import annotations

from typing import Any, Callable, Optional

import sqlglot
from sqlglot import expressions as exp


# ─── Schema normalisation ────────────────────────────────────────────────────


def normalize_schema(schema: dict) -> dict:
    """Validate *schema* and build fast-access lookup dicts.

    Args:
        schema: Dict with a ``"tables"`` key containing a list of table
                descriptors, each having ``"name"`` and ``"columns"`` fields.

    Returns:
        Dict with three keys:

        * ``"tables"``          — original list, unchanged.
        * ``"tables_by_name"``  — ``{table_name: table_dict}``.
        * ``"columns_by_table"``— ``{table_name: {col_name: col_dict}}``.

    Raises:
        ValueError: if *schema* has no tables, a table has no name, a table
                    name is duplicated, a column has no name, or a column
                    name is duplicated within its table.

    Examples:
        >>> s = {
        ...     "tables": [
        ...         {"name": "orders", "columns": [
        ...             {"name": "id",     "type": "INTEGER"},
        ...             {"name": "status", "type": "STRING"},
        ...         ]}
        ...     ]
        ... }
        >>> n = normalize_schema(s)
        >>> sorted(n.keys())
        ['columns_by_table', 'tables', 'tables_by_name']
        >>> "orders" in n["tables_by_name"]
        True
        >>> list(n["columns_by_table"]["orders"].keys())
        ['id', 'status']

        Missing tables list raises:

        >>> normalize_schema({})
        Traceback (most recent call last):
            ...
        ValueError: schema must contain at least one table

        Duplicate table name raises:

        >>> normalize_schema({"tables": [{"name": "t", "columns": []}, {"name": "t", "columns": []}]})
        Traceback (most recent call last):
            ...
        ValueError: Duplicate table name: 't'
    """
    tables = schema.get("tables", [])
    if not tables:
        raise ValueError("schema must contain at least one table")

    tables_by_name: dict[str, dict] = {}
    columns_by_table: dict[str, dict[str, dict]] = {}

    for table in tables:
        name = table.get("name")
        if not name:
            raise ValueError("Every table entry must have a 'name' field")
        if name in tables_by_name:
            raise ValueError(f"Duplicate table name: {name!r}")
        tables_by_name[name] = table

        cols: dict[str, dict] = {}
        for col in table.get("columns", []):
            col_name = col.get("name")
            if not col_name:
                raise ValueError(
                    f"A column in table {name!r} is missing a 'name' field"
                )
            if col_name in cols:
                raise ValueError(f"Duplicate column {col_name!r} in table {name!r}")
            cols[col_name] = col
        columns_by_table[name] = cols

    return {
        "tables": tables,
        "tables_by_name": tables_by_name,
        "columns_by_table": columns_by_table,
    }


# ─── Type helpers ─────────────────────────────────────────────────────────────

_ORDERABLE_TYPES = frozenset(
    {
        "INTEGER",
        "INT64",
        "INT",
        "FLOAT",
        "FLOAT64",
        "NUMERIC",
        "DECIMAL",
        "BIGINT",
        "SMALLINT",
        "DOUBLE",
        "DATE",
        "TIMESTAMP",
        "DATETIME",
        "TIME",
    }
)


def _is_unprofilable(col_type: str) -> bool:
    """Return True if *col_type* cannot be used with MIN/MAX/DISTINCT/GROUP BY.

    Applies to ARRAY, STRUCT, and RECORD types (BigQuery nested/repeated fields).

    Examples:
        >>> _is_unprofilable("RECORD")
        True
        >>> _is_unprofilable("STRUCT")
        True
        >>> _is_unprofilable("ARRAY")
        True
        >>> _is_unprofilable("ARRAY<STRUCT<x INT64>>")
        True
        >>> _is_unprofilable("STRUCT<x STRING>")
        True
        >>> _is_unprofilable("STRING")
        False
        >>> _is_unprofilable("INTEGER")
        False
    """
    upper = col_type.upper().strip()
    return upper in ("RECORD", "STRUCT", "ARRAY") or upper.startswith(
        ("ARRAY<", "STRUCT<")
    )


def _is_orderable(col_type: str) -> bool:
    """Return True if *col_type* supports MIN/MAX (i.e. is numeric or temporal).

    Examples:
        >>> _is_orderable("INTEGER")
        True
        >>> _is_orderable("FLOAT64")
        True
        >>> _is_orderable("TIMESTAMP")
        True
        >>> _is_orderable("STRING")
        False
        >>> _is_orderable("BOOLEAN")
        False
        >>> _is_orderable("timestamp")   # case-insensitive
        True
    """
    return col_type.upper() in _ORDERABLE_TYPES


# ─── Per-column profiling ─────────────────────────────────────────────────────


def _quote(name: str) -> str:
    """Wrap *name* in backticks to avoid SQL keyword collisions (BigQuery style).

    Examples:
        >>> _quote("order")
        '`order`'
        >>> _quote("my_col")
        '`my_col`'
    """
    return f"`{name}`"


def build_column_profile_queries(
    table_name: str, col: dict, options: dict, dialect: str = "bigquery"
) -> dict[str, str]:
    """Return a dict of named SQL strings for profiling one column.

    Always includes ``"basic"``, ``"duplicates"``, and ``"top_values"`` keys.
    Adds ``"minmax"`` only when the column type is orderable (numeric /
    temporal).

    Args:
        table_name: Fully-qualified or bare table name as it appears in SQL.
        col:        Column descriptor dict with at least ``"name"`` and
                    optionally ``"type"``.
        options:    Knobs dict; recognises ``"top_k_values"`` (default 20).
        dialect:    SQL dialect for identifier quoting (default ``"bigquery"``).

    Returns:
        Dict mapping query name → SQL string.

    Examples:
        Numeric column → four queries including ``"minmax"``:

        >>> col = {"name": "amount", "type": "FLOAT"}
        >>> q = build_column_profile_queries("orders", col, {})
        >>> sorted(q.keys())
        ['basic', 'duplicates', 'minmax', 'top_values']

        String column → three queries, no ``"minmax"``:

        >>> col2 = {"name": "status", "type": "STRING"}
        >>> q2 = build_column_profile_queries("orders", col2, {})
        >>> sorted(q2.keys())
        ['basic', 'duplicates', 'top_values']

        ``top_k_values`` option is respected:

        >>> col3 = {"name": "id", "type": "INTEGER"}
        >>> q3 = build_column_profile_queries("t", col3, {"top_k_values": 5})
        >>> "LIMIT 5" in q3["top_values"]
        True
    """
    col_name = col["name"]
    top_k = options.get("top_k_values", 20)

    def _cref() -> exp.Column:
        return exp.Column(this=exp.Identifier(this=col_name, quoted=True))

    def _is_null() -> exp.Expression:
        return exp.Is(this=_cref(), expression=exp.Null())

    def _not_null() -> exp.Expression:
        return exp.Not(this=_is_null())

    basic = (
        exp.select(
            exp.alias_(exp.Count(this=exp.Star()), "total_count"),
            exp.alias_(
                exp.Anonymous(this="COUNTIF", expressions=[_is_null()]), "null_count"
            ),
            exp.alias_(
                exp.Count(this=exp.Distinct(expressions=[_cref()])), "distinct_count"
            ),
        )
        .from_(table_name)
        .sql(dialect=dialect)
    )

    dup_inner = (
        exp.select(_cref(), exp.alias_(exp.Count(this=exp.Star()), "cnt"))
        .from_(table_name)
        .where(_not_null())
        .group_by(exp.Literal.number(1))
        .having(
            exp.GT(this=exp.Count(this=exp.Star()), expression=exp.Literal.number(1))
        )
    )
    duplicates = (
        exp.select(exp.alias_(exp.Count(this=exp.Star()), "duplicate_value_count"))
        .from_(dup_inner.subquery())
        .sql(dialect=dialect)
    )

    top_values = (
        exp.select(
            exp.alias_(_cref(), "val"),
            exp.alias_(exp.Count(this=exp.Star()), "cnt"),
        )
        .from_(table_name)
        .group_by(exp.Literal.number(1))
        .order_by(exp.Ordered(this=exp.column("cnt"), desc=True))
        .limit(top_k)
        .sql(dialect=dialect)
    )

    queries: dict[str, str] = {
        "basic": basic,
        "duplicates": duplicates,
        "top_values": top_values,
    }

    if _is_orderable(col.get("type", "")):
        queries["minmax"] = (
            exp.select(
                exp.alias_(exp.Min(this=_cref()), "min_value"),
                exp.alias_(exp.Max(this=_cref()), "max_value"),
            )
            .from_(table_name)
            .where(_not_null())
            .sql(dialect=dialect)
        )

    return queries


def build_column_profile(
    table_name: str,
    col: dict,
    sql_executor: Callable[[str], list[dict]],
    options: dict,
    dialect: str = "bigquery",
) -> dict:
    """Execute profile queries for one column and return structured statistics.

    Runs the queries produced by :func:`build_column_profile_queries` via
    *sql_executor* and aggregates the results into a single profile dict.

    Args:
        table_name:   Fully-qualified or bare table name.
        col:          Column descriptor with ``"name"`` and optional ``"type"``.
        sql_executor: Callable ``(sql: str) -> list[dict]``.  Each dict maps
                      column-name → value.
        options:      Knobs dict; recognises ``"categorical_distinct_threshold"``
                      (default 20) and ``"top_k_values"`` (default 20).

    Returns:
        Dict with the following keys:

        =====================  =================================================
        Key                    Description
        =====================  =================================================
        ``type``               SQL type string from the schema.
        ``nullable_ratio``     Fraction of NULL rows (0.0–1.0, 4 d.p.).
        ``null_count``         Absolute NULL count.
        ``non_null_count``     Absolute non-NULL count.
        ``is_always_null``     True if every row is NULL.
        ``is_never_null``      True if no row is NULL.
        ``distinct_count``     Number of distinct non-NULL values.
        ``duplicate_count``    Number of values that appear more than once.
        ``is_unique``          True if all rows are non-NULL and distinct.
        ``is_unique_non_null`` True if non-NULL values are all distinct.
        ``is_categorical``     True if distinct_count ≤ threshold.
        ``top_values``         List of most-frequent values (strings excluded
                               NULLs), ordered by frequency descending.
        ``top_values_frequency`` Relative frequency for each top value.
        ``min_value``          Minimum value (orderable types only, else None).
        ``max_value``          Maximum value (orderable types only, else None).
        =====================  =================================================

    Invariants:
        ``null_count + non_null_count == total_count``

    Examples:
        Build a simple executor from pre-canned results:

        >>> def _exec(sql):
        ...     if "total_count" in sql:
        ...         return [{"total_count": 10, "null_count": 2, "distinct_count": 5}]
        ...     if "duplicate_value_count" in sql:
        ...         return [{"duplicate_value_count": 1}]
        ...     if "MIN" in sql:
        ...         return [{"min_value": 1, "max_value": 9}]
        ...     # top_values query
        ...     return [{"val": "a", "cnt": 6}, {"val": "b", "cnt": 2}]
        >>> col = {"name": "score", "type": "INTEGER"}
        >>> p = build_column_profile("events", col, _exec, {})
        >>> p["null_count"]
        2
        >>> p["non_null_count"]
        8
        >>> p["null_count"] + p["non_null_count"] == 10
        True
        >>> p["is_categorical"]
        True
        >>> p["min_value"]
        1
    """
    queries = build_column_profile_queries(table_name, col, options, dialect=dialect)
    col_type = col.get("type", "STRING")
    threshold = options.get("categorical_distinct_threshold", 20)

    # Basic stats
    basic_rows = sql_executor(queries["basic"])
    if not basic_rows:
        return {"type": col_type, "error": "no rows returned from basic query"}
    row = basic_rows[0]
    total = int(row.get("total_count") or 0)
    null_count = int(row.get("null_count") or 0)
    distinct_count = int(row.get("distinct_count") or 0)
    non_null = total - null_count

    nullable_ratio = round(null_count / total, 4) if total > 0 else 0.0

    # Duplicates
    dup_rows = sql_executor(queries["duplicates"])
    dup_count = int((dup_rows[0].get("duplicate_value_count") or 0)) if dup_rows else 0

    # Uniqueness
    is_unique = distinct_count == total and null_count == 0
    is_unique_non_null = distinct_count == non_null and non_null > 0

    # Top values
    top_rows = sql_executor(queries["top_values"])
    total_non_null = max(non_null, 1)
    top_values = [r["val"] for r in top_rows if r.get("val") is not None]
    top_values_frequency = [
        round(int(r.get("cnt") or 0) / total_non_null, 4)
        for r in top_rows
        if r.get("val") is not None
    ]

    is_categorical = 0 < distinct_count <= threshold

    # Min / max
    min_value: Any = None
    max_value: Any = None
    if "minmax" in queries:
        mm_rows = sql_executor(queries["minmax"])
        if mm_rows:
            min_value = mm_rows[0].get("min_value")
            max_value = mm_rows[0].get("max_value")

    return {
        "type": col_type,
        "nullable_ratio": nullable_ratio,
        "null_count": null_count,
        "non_null_count": non_null,
        "is_always_null": null_count == total and total > 0,
        "is_never_null": null_count == 0,
        "distinct_count": distinct_count,
        "duplicate_count": dup_count,
        "is_unique": is_unique,
        "is_unique_non_null": is_unique_non_null,
        "is_categorical": is_categorical,
        "top_values": top_values,
        "top_values_frequency": top_values_frequency,
        "min_value": min_value,
        "max_value": max_value,
    }


# ─── Correlation detection ────────────────────────────────────────────────────


def detect_correlations(
    table_name: str,
    table_profile: dict,
    sql_executor: Callable[[str], list[dict]],
    options: dict,
    dialect: str = "bigquery",
) -> list[dict]:
    """Detect simple functional correlations between columns.

    For each pair *(driver, target)* where the driver column is categorical
    (low cardinality) and the target is sometimes NULL, checks whether the
    target is **always NULL** or **always constant** when the driver equals a
    particular value.

    Args:
        table_name:    Table to query.
        table_profile: Partial profile dict ``{"columns": {col_name: stats}}``,
                       as produced by :func:`build_column_profile` for each
                       column.
        sql_executor:  Callable ``(sql: str) -> list[dict]``.
        options:       Knobs; recognises
                       ``"max_correlation_columns_per_table"`` (default 30).

    Returns:
        List of correlation dicts, each with keys:

        * ``"driver_column"``  — column whose value triggers the rule.
        * ``"driver_value"``   — the specific value.
        * ``"target_column"``  — column affected by the rule.
        * ``"rule_type"``      — ``"always_null"`` or ``"constant"``.

    Note:
        SQL errors for any individual pair are silently skipped so that a
        bad column doesn't abort the entire detection pass.

    Examples:
        When the executor says target is always NULL for driver_val = "X":

        >>> def _exec(sql):
        ...     return [{"driver_val": "X", "row_count": 5,
        ...              "target_null_count": 5, "target_distinct_count": 0}]
        >>> profile = {
        ...     "columns": {
        ...         "status": {"is_categorical": True, "distinct_count": 2},
        ...         "refund_amount": {"nullable_ratio": 0.5, "is_always_null": False},
        ...     }
        ... }
        >>> corrs = detect_correlations("orders", profile, _exec, {})
        >>> len(corrs) == 1
        True
        >>> corrs[0]["rule_type"]
        'always_null'
        >>> corrs[0]["driver_value"]
        'X'
    """
    max_cols = options.get("max_correlation_columns_per_table", 30)
    columns = table_profile.get("columns", {})

    driver_cols = [
        name
        for name, cp in columns.items()
        if cp.get("is_categorical") and 0 < cp.get("distinct_count", 0) <= 20
    ][:max_cols]

    target_cols = [
        name
        for name, cp in columns.items()
        if cp.get("nullable_ratio", 0) > 0 and not cp.get("is_always_null")
    ][:max_cols]

    correlations: list[dict] = []

    for driver in driver_cols:
        for target in target_cols:
            if driver == target:
                continue
            query = (
                exp.select(
                    exp.alias_(
                        exp.Column(this=exp.Identifier(this=driver, quoted=True)),
                        "driver_val",
                    ),
                    exp.alias_(exp.Count(this=exp.Star()), "row_count"),
                    exp.alias_(
                        exp.Anonymous(
                            this="COUNTIF",
                            expressions=[
                                exp.Is(
                                    this=exp.Column(
                                        this=exp.Identifier(this=target, quoted=True)
                                    ),
                                    expression=exp.Null(),
                                )
                            ],
                        ),
                        "target_null_count",
                    ),
                    exp.alias_(
                        exp.Count(
                            this=exp.Distinct(
                                expressions=[
                                    exp.Column(
                                        this=exp.Identifier(this=target, quoted=True)
                                    )
                                ]
                            )
                        ),
                        "target_distinct_count",
                    ),
                )
                .from_(table_name)
                .group_by(exp.Literal.number(1))
                .sql(dialect=dialect)
            )
            try:
                rows = sql_executor(query)
            except Exception:
                continue

            for r in rows:
                driver_val = r.get("driver_val")
                row_count = int(r.get("row_count") or 0)
                null_count = int(r.get("target_null_count") or 0)
                distinct_count = int(r.get("target_distinct_count") or 0)

                if driver_val is None or row_count == 0:
                    continue

                if null_count == row_count:
                    correlations.append(
                        {
                            "driver_column": driver,
                            "driver_value": driver_val,
                            "target_column": target,
                            "rule_type": "always_null",
                        }
                    )
                elif distinct_count == 1:
                    correlations.append(
                        {
                            "driver_column": driver,
                            "driver_value": driver_val,
                            "target_column": target,
                            "rule_type": "constant",
                        }
                    )

    return correlations


# ─── Main profiling function ──────────────────────────────────────────────────


def profile_schema(
    schema: dict,
    sql_executor: Callable[[str], list[dict]],
    options: dict | None = None,
    dialect: str = "bigquery",
) -> dict:
    """Build and execute profiling queries for every table/column in *schema*.

    This is the **backend-side** profiling function.  It runs all queries
    directly via *sql_executor* and returns a complete profile.  For a
    user-facing, single-query variant see :func:`build_profile_query` /
    :func:`parse_profile_query_result`.

    Args:
        schema:       ``{"tables": [{"name": ..., "columns": [...]}]}``.
        sql_executor: Callable ``(sql: str) -> list[dict]``.
        options:      Optional tuning knobs:

                      * ``"top_k_values"`` (int, default 20)
                      * ``"categorical_distinct_threshold"`` (int, default 20)
                      * ``"compute_pairwise_correlations"`` (bool, default False)
                      * ``"max_correlation_columns_per_table"`` (int, default 30)

    Returns:
        Profile dict::

            {
                "tables": {
                    "<table_name>": {
                        "row_count": int,
                        "columns": {
                            "<col_name>": { ...stats... }
                        },
                        "correlations": [ ...rules... ],
                    }
                },
                "joins": [],
            }

    Postconditions:
        * Every table and column in *schema* has a corresponding entry.
        * For each column: ``null_count + non_null_count == row_count``.
        * For each column: ``distinct_count <= non_null_count``.

    Examples:
        >>> def _exec(sql):
        ...     if "row_count" in sql:        return [{"row_count": 4}]
        ...     if "total_count" in sql:      return [{"total_count": 4, "null_count": 0, "distinct_count": 4}]
        ...     if "duplicate" in sql:        return [{"duplicate_value_count": 0}]
        ...     if "MIN" in sql:              return [{"min_value": 1, "max_value": 4}]
        ...     return [{"val": i, "cnt": 1} for i in range(4)]
        >>> schema = {"tables": [{"name": "t", "columns": [{"name": "id", "type": "INTEGER"}]}]}
        >>> p = profile_schema(schema, _exec)
        >>> list(p.keys())
        ['tables', 'joins']
        >>> p["tables"]["t"]["row_count"]
        4
        >>> p["tables"]["t"]["columns"]["id"]["is_unique"]
        True
    """
    options = options or {}
    norm = normalize_schema(schema)

    result: dict = {"tables": {}, "joins": []}

    for table in norm["tables"]:
        table_name: str = table["name"]
        columns: list[dict] = table.get("columns", [])

        # Row count
        try:
            rc_rows = sql_executor(
                exp.select(exp.alias_(exp.Count(this=exp.Star()), "row_count"))
                .from_(table_name)
                .sql(dialect=dialect)
            )
            row_count = int((rc_rows[0].get("row_count") or 0)) if rc_rows else 0
        except Exception:
            row_count = 0

        # Column profiles
        col_profiles: dict[str, dict] = {}
        for col in columns:
            try:
                col_profiles[col["name"]] = build_column_profile(
                    table_name, col, sql_executor, options, dialect=dialect
                )
            except Exception as exc:
                col_profiles[col["name"]] = {
                    "type": col.get("type", "UNKNOWN"),
                    "error": str(exc),
                }

        # Correlations (opt-in)
        correlations: list[dict] = []
        if options.get("compute_pairwise_correlations", False):
            try:
                correlations = detect_correlations(
                    table_name,
                    {"columns": col_profiles},
                    sql_executor,
                    options,
                    dialect=dialect,
                )
            except Exception:
                pass

        result["tables"][table_name] = {
            "row_count": row_count,
            "columns": col_profiles,
            "correlations": correlations,
        }

    return result


# ─── Join profiling ───────────────────────────────────────────────────────────


def _collect_join_specs(sql_query: str, dialect: str = "bigquery") -> list[dict]:
    """Parse *sql_query* and return one spec dict per JOIN ON equality condition.

    Each spec contains:

    * ``"left_table"``     — resolved real table name for the left side.
    * ``"right_table"``    — resolved real table name for the right side.
    * ``"left_expr_sql"``  — SQL expression on the left of the ``=``.
    * ``"right_expr_sql"`` — SQL expression on the right of the ``=``.
    * ``"left_keys"``      — list of source column names in the left expression.
    * ``"right_keys"``     — list of source column names in the right expression.

    Table aliases are resolved to their real names.  Compound ``AND``
    conditions are split into individual specs.  Returns ``[]`` on parse
    failure.

    Examples:
        >>> specs = _collect_join_specs(
        ...     "SELECT * FROM orders o JOIN users u ON o.user_id = u.id"
        ... )
        >>> len(specs)
        1
        >>> specs[0]["left_table"]
        'orders'
        >>> specs[0]["right_table"]
        'users'
        >>> specs[0]["left_keys"]
        ['user_id']
        >>> specs[0]["right_keys"]
        ['id']

        Malformed SQL returns an empty list:

        >>> _collect_join_specs("NOT VALID SQL !!!!")
        []
    """
    try:
        tree = sqlglot.parse_one(
            sql_query, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return []

    def _full_table_name(tbl: exp.Table) -> str:
        """Return fully-qualified dotted name (project.dataset.table)."""
        parts = []
        catalog = tbl.args.get("catalog")
        db = tbl.args.get("db")
        if catalog:
            parts.append(catalog.name if hasattr(catalog, "name") else str(catalog))
        if db:
            parts.append(db.name if hasattr(db, "name") else str(db))
        parts.append(tbl.name)
        return ".".join(parts)

    # Build alias → full-qualified-table map
    alias_map: dict[str, str] = {}
    for tbl in tree.find_all(exp.Table):
        full = _full_table_name(tbl)
        if tbl.alias:
            alias_map[tbl.alias] = full
        alias_map[tbl.name] = full  # short name → full name

    def resolve(name: str) -> str:
        return alias_map.get(name, name)

    def source_cols(expr: exp.Expression) -> list[str]:
        return [c.name for c in expr.find_all(exp.Column)]

    # Identify the primary FROM table
    from_node = tree.find(exp.From)
    primary_table: str = ""
    if from_node:
        tbl = from_node.find(exp.Table)
        if tbl:
            primary_table = _full_table_name(tbl)

    specs: list[dict] = []

    for join_node in tree.find_all(exp.Join):
        on_expr = join_node.args.get("on")
        if not on_expr:
            continue

        # Right-side table
        jt = join_node.this
        if isinstance(jt, exp.Alias):
            right_table = (
                resolve(jt.alias) or _full_table_name(jt.this)
                if isinstance(jt.this, exp.Table)
                else jt.this.name
            )
        elif isinstance(jt, exp.Table):
            right_table = _full_table_name(jt)
        else:
            continue

        # Walk AND-separated conditions
        def collect_eq(node: exp.Expression):
            if isinstance(node, exp.And):
                collect_eq(node.left)
                collect_eq(node.right)
            elif isinstance(node, exp.EQ):
                left = node.left
                right = node.right
                left_tbl = resolve(left.table) if isinstance(left, exp.Column) else ""
                right_tbl = (
                    resolve(right.table) if isinstance(right, exp.Column) else ""
                )

                # Determine which side belongs to which table.
                # right_table is the JOIN node's table; the other side is the FROM/left table.
                if right_tbl == right_table or (not left_tbl and not right_tbl):
                    l_expr, r_expr = left, right
                else:
                    l_expr, r_expr = right, left

                # Derive left_table from l_expr's resolved table qualifier after the
                # potential swap — this is more accurate than using left_tbl directly,
                # since the swap can change which expression is on the left side.
                l_resolved = (
                    resolve(l_expr.table)
                    if isinstance(l_expr, exp.Column) and l_expr.table
                    else ""
                )

                specs.append(
                    {
                        "left_table": l_resolved or primary_table,
                        "right_table": right_table,
                        "left_expr_sql": l_expr.sql(),
                        "right_expr_sql": r_expr.sql(),
                        "left_keys": source_cols(l_expr),
                        "right_keys": source_cols(r_expr),
                    }
                )

        collect_eq(on_expr)

    return specs


def profile_joins_for_query(
    schema: dict,
    sql_query: str,
    sql_executor: Callable[[str], list[dict]],
    options: dict | None = None,
    dialect: str = "bigquery",
) -> list[dict]:
    """Parse *sql_query*, extract JOIN conditions, and measure their cardinality.

    For each ``JOIN ON`` equality condition found in the query, runs a
    ``WITH l AS / WITH r AS`` cardinality sub-query to determine whether the
    relationship is one-to-one, one-to-many, many-to-one, or many-to-many.

    Args:
        schema:       Full schema dict (used for normalisation / validation).
        sql_query:    Raw SQL string to parse.
        sql_executor: Callable ``(sql: str) -> list[dict]``.
        options:      Reserved for future knobs (currently unused).

    Returns:
        List of join dicts, each containing:

        * ``"left_table"``        — real table name.
        * ``"right_table"``       — real table name.
        * ``"left_expr"``         — SQL expression used as left join key.
        * ``"right_expr"``        — SQL expression used as right join key.
        * ``"left_keys"``         — source column names on the left side.
        * ``"right_keys"``        — source column names on the right side.
        * ``"join_type_profiled"``— ``"one-to-one"`` | ``"one-to-many"`` |
                                    ``"many-to-one"`` | ``"many-to-many"``.

        Falls back to ``"many-to-many"`` on any SQL execution error.

    Examples:
        >>> schema = {
        ...     "tables": [
        ...         {"name": "orders",  "columns": [{"name": "user_id", "type": "INTEGER"}]},
        ...         {"name": "users",   "columns": [{"name": "id",      "type": "INTEGER"}]},
        ...     ]
        ... }
        >>> def _exec(sql):
        ...     return [{"join_type": "many-to-one"}]
        >>> results = profile_joins_for_query(
        ...     schema,
        ...     "SELECT * FROM orders o JOIN users u ON o.user_id = u.id",
        ...     _exec,
        ... )
        >>> len(results)
        1
        >>> results[0]["join_type_profiled"]
        'many-to-one'
        >>> results[0]["left_keys"]
        ['user_id']
    """
    options = options or {}
    specs = _collect_join_specs(sql_query, dialect=dialect)
    results: list[dict] = []

    for spec in specs:
        left_table = spec["left_table"]
        right_table = spec["right_table"]
        left_expr = spec["left_expr_sql"]
        right_expr = spec["right_expr_sql"]

        # Build cardinality query using sqlglot expressions
        left_expr_node = sqlglot.parse_one(left_expr) if left_expr else exp.Star()
        right_expr_node = sqlglot.parse_one(right_expr) if right_expr else exp.Star()

        l_cte = (
            exp.select(
                exp.alias_(left_expr_node, "join_key"),
                exp.alias_(exp.Count(this=exp.Star()), "cnt"),
            )
            .from_(left_table)
            .group_by(exp.Literal.number(1))
        )
        r_cte = (
            exp.select(
                exp.alias_(right_expr_node, "join_key"),
                exp.alias_(exp.Count(this=exp.Star()), "cnt"),
            )
            .from_(right_table)
            .group_by(exp.Literal.number(1))
        )

        def _lcnt() -> exp.Column:
            return exp.column("cnt", table="l")

        def _rcnt() -> exp.Column:
            return exp.column("cnt", table="r")

        join_case = exp.Case(
            ifs=[
                exp.If(
                    this=exp.And(
                        this=exp.EQ(
                            this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                        ),
                        expression=exp.EQ(
                            this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                        ),
                    ),
                    true=exp.Literal.string("one-to-one"),
                ),
                exp.If(
                    this=exp.And(
                        this=exp.GT(
                            this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                        ),
                        expression=exp.EQ(
                            this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                        ),
                    ),
                    true=exp.Literal.string("many-to-one"),
                ),
                exp.If(
                    this=exp.And(
                        this=exp.EQ(
                            this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                        ),
                        expression=exp.GT(
                            this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                        ),
                    ),
                    true=exp.Literal.string("one-to-many"),
                ),
            ],
            default=exp.Literal.string("many-to-many"),
        )

        main_q = (
            exp.select(exp.alias_(join_case, "join_type"))
            .from_("l")
            .join(
                "r",
                on=exp.EQ(
                    this=exp.column("join_key", table="l"),
                    expression=exp.column("join_key", table="r"),
                ),
            )
        )
        cardinality_sql = (
            main_q.with_("l", as_=l_cte).with_("r", as_=r_cte).sql(dialect=dialect)
        )
        try:
            rows = sql_executor(cardinality_sql)
            join_type = (
                rows[0]["join_type"]
                if rows and rows[0].get("join_type")
                else "many-to-many"
            )
        except Exception:
            join_type = "many-to-many"

        results.append(
            {
                "left_table": left_table,
                "right_table": right_table,
                "left_expr": left_expr,
                "right_expr": right_expr,
                "left_keys": spec["left_keys"],
                "right_keys": spec["right_keys"],
                "join_type_profiled": join_type,
            }
        )

    return results


# ─── Single-query profile builder (UI workflow) ───────────────────────────────
#
# One SELECT per (table, col), UNION ALL between them.
# Each SELECT produces ONE row with ALL metrics in named columns:
#
#   table_name     VARCHAR  — fully-qualified table name
#   col_name       VARCHAR  — column name
#   total_count    BIGINT   — COUNT(*)
#   null_count     BIGINT   — COUNT(*) - COUNT(col)
#   non_null_count BIGINT   — COUNT(col)
#   distinct_count BIGINT   — COUNT(DISTINCT col)
#   dup_count      BIGINT   — # of distinct values appearing more than once
#   min_val        VARCHAR  — CAST(MIN(col) AS STRING/VARCHAR)  — all types
#   max_val        VARCHAR  — CAST(MAX(col) AS STRING/VARCHAR)  — all types
#   top_values     VARCHAR  — top-K most frequent values, comma-separated
#
# Identifier quoting is delegated to sqlglot so dialect differences
# (backtick vs double-quote) are handled automatically.


def _col_id(col: str, dialect: str) -> str:
    """Return a dialect-appropriate quoted identifier for *col* using sqlglot.

    Delegates quoting to ``sqlglot.exp.to_identifier`` so dialect differences
    (BigQuery backticks vs ANSI double-quotes) are handled automatically.

    Examples:
        >>> _col_id("my_col", "bigquery")
        '`my_col`'
        >>> _col_id("my_col", "duckdb")
        '"my_col"'
        >>> _col_id("my_col", "postgres")
        '"my_col"'
        >>> _col_id("order", "bigquery")
        '`order`'
    """
    from sqlglot import exp as _exp

    return _exp.to_identifier(col, quoted=True).sql(dialect=dialect)


def _str_type(dialect: str) -> str:
    """Return the string/varchar type keyword for the given dialect.

    Examples:
        >>> _str_type("bigquery")
        'STRING'
        >>> _str_type("duckdb")
        'VARCHAR'
        >>> _str_type("postgres")
        'VARCHAR'
    """
    return "STRING" if dialect == "bigquery" else "VARCHAR"


def _table_expr(table: str) -> exp.Table:
    """Build a quoted ``exp.Table`` node from a (possibly dotted) table name.

    Handles BigQuery-style ``project.dataset.table`` names where project may
    contain hyphens that confuse sqlglot's string parser.
    """
    parts = table.split(".")
    if len(parts) == 3:
        return exp.Table(
            this=exp.Identifier(this=parts[2], quoted=True),
            db=exp.Identifier(this=parts[1], quoted=True),
            catalog=exp.Identifier(this=parts[0], quoted=True),
        )
    if len(parts) == 2:
        return exp.Table(
            this=exp.Identifier(this=parts[1], quoted=True),
            db=exp.Identifier(this=parts[0], quoted=True),
        )
    return exp.Table(this=exp.Identifier(this=parts[0], quoted=True))


def _build_col_query(
    table: str,
    col: str,
    col_type: str,
    dialect: str,
    top_k: int,
    idx: int,
) -> str:
    """Build one SELECT that computes all profile metrics for a single column.

    Produces exactly **one row** with human-readable column names:

    ===============  =======  =============================================
    Column           Type     Meaning
    ===============  =======  =============================================
    table_name       VARCHAR  Fully-qualified table name
    col_name         VARCHAR  Column name
    total_count      BIGINT   COUNT(*)
    null_count       BIGINT   COUNT(*) - COUNT(col)
    non_null_count   BIGINT   COUNT(col)
    distinct_count   BIGINT   COUNT(DISTINCT col)
    dup_count        BIGINT   # distinct values appearing more than once
    min_val          VARCHAR  CAST(MIN(col) AS STRING) — works for all types incl. strings
    max_val          VARCHAR  CAST(MAX(col) AS STRING) — works for all types incl. strings
    top_values       VARCHAR  Top-k values comma-joined via STRING_AGG
    ===============  =======  =============================================

    Args:
        table:    Fully-qualified table name (used verbatim in FROM clause).
        col:      Column name (will be quoted with :func:`_col_id`).
        col_type: Column data type string (used to decide min/max inclusion).
        dialect:  ``"bigquery"`` | ``"duckdb"`` | ``"postgres"``.
        top_k:    Maximum number of top values to include in ``top_values``.
        idx:      Unique integer to avoid sub-query alias collisions.

    Examples:
        >>> sql = _build_col_query("orders", "status", "STRING", "bigquery", 5, 0)
        >>> "table_name" in sql and "col_name" in sql
        True
        >>> "total_count" in sql and "null_count" in sql
        True
        >>> "distinct_count" in sql and "dup_count" in sql
        True
        >>> "top_values" in sql
        True
        >>> "STRING_AGG" in sql
        True
        >>> "`status`" in sql
        True
        >>> sql = _build_col_query("orders", "amount", "INTEGER", "duckdb", 5, 1)
        >>> '"amount"' in sql
        True
        >>> "min_val" in sql and "max_val" in sql
        True
        >>> "_dup_1" in sql and "_top_1" in sql
        True
    """
    str_t = _str_type(dialect)
    str_dtype = exp.DataType.build(str_t)
    complex_col = _is_unprofilable(col_type)

    def _cref() -> exp.Column:
        return exp.Column(this=exp.Identifier(this=col, quoted=True))

    def _not_null() -> exp.Expression:
        return exp.Not(this=exp.Is(this=_cref(), expression=exp.Null()))

    if not complex_col:
        dup_inner = (
            exp.select(_cref())
            .from_(_table_expr(table))
            .where(_not_null())
            .group_by(exp.Literal.number(1))
            .having(
                exp.GT(
                    this=exp.Count(this=exp.Star()), expression=exp.Literal.number(1)
                )
            )
        )
        dup_expr: exp.Expression = exp.select(exp.Count(this=exp.Star())).from_(
            dup_inner.subquery(f"_dup_{idx}")
        )

        top_alias = f"_top_{idx}"
        top_inner = (
            exp.select(_cref(), exp.alias_(exp.Count(this=exp.Star()), "_cnt"))
            .from_(_table_expr(table))
            .where(_not_null())
            .group_by(exp.Literal.number(1))
            .order_by(exp.Ordered(this=exp.column("_cnt"), desc=True))
            .limit(top_k)
        )
        top_expr: exp.Expression = exp.select(
            exp.Anonymous(
                this="STRING_AGG", expressions=[exp.Cast(this=_cref(), to=str_dtype)]
            )
        ).from_(top_inner.subquery(top_alias))

        distinct_expr: exp.Expression = exp.Count(
            this=exp.Distinct(expressions=[_cref()])
        )
        min_expr: exp.Expression = exp.Cast(
            this=exp.Min(this=_cref()), to=str_dtype
        )
        max_expr: exp.Expression = exp.Cast(
            this=exp.Max(this=_cref()), to=str_dtype
        )
    else:
        dup_expr = exp.Null()
        top_expr = exp.Null()
        distinct_expr = exp.Null()
        min_expr = exp.Null()
        max_expr = exp.Null()

    return (
        exp.select(
            exp.alias_(exp.Literal.string("column"), "row_type"),
            exp.alias_(exp.Literal.string(table), "table_name"),
            exp.alias_(exp.Literal.string(col), "col_name"),
            exp.alias_(exp.Count(this=exp.Star()), "total_count"),
            exp.alias_(
                exp.Sub(
                    this=exp.Count(this=exp.Star()), expression=exp.Count(this=_cref())
                ),
                "null_count",
            ),
            exp.alias_(exp.Count(this=_cref()), "non_null_count"),
            exp.alias_(distinct_expr, "distinct_count"),
            exp.alias_(
                dup_expr.subquery() if not complex_col else dup_expr, "dup_count"
            ),
            exp.alias_(min_expr, "min_val"),
            exp.alias_(max_expr, "max_val"),
            exp.alias_(
                top_expr.subquery() if not complex_col else top_expr, "top_values"
            ),
            exp.alias_(exp.Null(), "left_table"),
            exp.alias_(exp.Null(), "right_table"),
            exp.alias_(exp.Null(), "left_expr"),
            exp.alias_(exp.Null(), "right_expr"),
            exp.alias_(exp.Null(), "join_type"),
            exp.alias_(exp.Null(), "left_match_rate"),
            exp.alias_(exp.Null(), "avg_right_per_left_key"),
            exp.alias_(exp.Null(), "max_right_per_left_key"),
            exp.alias_(exp.Null(), "left_key_sample"),
        )
        .from_(_table_expr(table))
        .sql(dialect=dialect)
    )


def _resolve_cte_source(
    cte_name: str,
    col_names: list[str],
    cte_map: dict[str, str],
    dialect: str = "bigquery",
) -> dict | None:
    """Use sqlglot lineage to trace CTE join-key columns back to the real source table.

    Parses the CTE body, traces each *col_name* to its origin via lineage, then
    extracts the CTE's WHERE conditions (with table aliases stripped) so profiling
    subqueries can run against the real source table instead of the CTE name.

    Returns::

        {
            "source_table": "fully.qualified.table",
            "source_cols":  ["col", ...],   # resolved column names in source
            "where_sql":    str | None,     # WHERE conditions (aliases stripped)
        }

    or ``None`` if resolution fails (unknown CTE, lineage error, columns from
    multiple source tables, …).
    """
    cte_body = cte_map.get(cte_name)
    if not cte_body:
        return None

    try:
        from sqlglot.lineage import lineage as _lineage

        source_table: str | None = None
        source_cols: list[str] = []

        for col_name in col_names:
            try:
                node = _lineage(col_name, cte_body, dialect=dialect)
            except Exception:
                continue

            downstream = node.downstream
            if not downstream:
                continue

            src_node = downstream[0]
            src = src_node.source

            # Resolve the actual Table node
            tbl: exp.Table | None = None
            if isinstance(src, exp.Table):
                tbl = src
            elif isinstance(src, exp.Subquery):
                inner = src.find(exp.Table)
                if inner:
                    tbl = inner

            if tbl is None:
                continue

            # Build fully-qualified name
            parts: list[str] = []
            catalog = tbl.args.get("catalog")
            db = tbl.args.get("db")
            if catalog:
                parts.append(catalog.name if hasattr(catalog, "name") else str(catalog))
            if db:
                parts.append(db.name if hasattr(db, "name") else str(db))
            parts.append(tbl.name)
            tbl_full = ".".join(parts)

            if source_table is None:
                source_table = tbl_full
            elif source_table != tbl_full:
                return None  # keys from different source tables — give up

            raw = src_node.name
            source_cols.append(raw.split(".")[-1] if "." in raw else raw)

        if not source_table:
            return None

        # Extract WHERE conditions from the CTE body (strip table aliases)
        where_sql: str | None = None
        try:
            tree = sqlglot.parse_one(
                cte_body, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
            )
            where_node = tree.args.get("where")
            if where_node:
                where_expr = where_node.this.copy()
                for col_node in where_expr.find_all(exp.Column):
                    if col_node.table:
                        col_node.replace(exp.Column(this=col_node.this.copy()))
                where_sql = where_expr.sql(dialect=dialect)
        except Exception:
            pass

        return {
            "source_table": source_table,
            "source_cols": source_cols,
            "where_sql": where_sql,
        }

    except Exception:
        return None


def _build_join_query(
    pair_specs: list[dict],
    dialect: str,
    idx: int,
    l_source: dict | None = None,
    r_source: dict | None = None,
) -> str:
    """Build one SELECT that profiles the cardinality of a join between two tables.

    Accepts *pair_specs* — all equality conditions for a single ``(left_table,
    right_table)`` pair.  When multiple conditions exist (compound AND key), the
    individual key columns are concatenated with ``\\x01`` so a single join_key
    value uniquely identifies a row combination.

    When *l_source* / *r_source* are provided (from :func:`_resolve_cte_source`),
    the subqueries are built against the real source tables with the CTE's WHERE
    conditions applied, rather than querying the CTE alias directly.

    Extra output columns:

    * ``left_match_rate``        — fraction of left key values found in right (0.0–1.0).
    * ``avg_right_per_left_key`` — average right-table row count per matched left key.
    * ``max_right_per_left_key`` — maximum right-table row count for any left key.
    * ``left_key_sample``        — STRING_AGG of up to 100 distinct left key values.
    """
    left_table = pair_specs[0]["left_table"]
    right_table = pair_specs[0]["right_table"]

    # Combined expression strings for the metadata columns
    left_expr_str = " AND ".join(s["left_expr_sql"] for s in pair_specs)
    right_expr_str = " AND ".join(s["right_expr_sql"] for s in pair_specs)

    str_t = _str_type(dialect)
    str_dtype = exp.DataType.build(str_t)
    float_t = "FLOAT"

    l_src_table = l_source["source_table"] if l_source else left_table
    r_src_table = r_source["source_table"] if r_source else right_table

    # Build one key expression node per spec, stripped of table qualifiers.
    # If CTE lineage resolved a source column, use that; otherwise strip the
    # table alias from the original expression.
    def _spec_key(i: int, side: str, source: dict | None) -> exp.Expression:
        spec = pair_specs[i]
        src_cols = (source or {}).get("source_cols", [])
        if src_cols and i < len(src_cols):
            return exp.Column(this=exp.to_identifier(src_cols[i], quoted=True))
        raw = spec[f"{side}_expr_sql"]
        node = sqlglot.parse_one(raw) if raw else exp.Star()
        if isinstance(node, exp.Column) and node.table:
            node = exp.Column(this=node.this)
        return node

    l_key_nodes = [_spec_key(i, "left", l_source) for i in range(len(pair_specs))]
    r_key_nodes = [_spec_key(i, "right", r_source) for i in range(len(pair_specs))]

    # For a single condition: use the column directly.
    # For compound AND keys: CONCAT all parts with \x01 as separator so the
    # combined value uniquely identifies each row combination.
    def _make_key(nodes: list[exp.Expression]) -> exp.Expression:
        if len(nodes) == 1:
            return nodes[0]
        parts: list[exp.Expression] = []
        sep = exp.Literal.string("\x01")
        for i, n in enumerate(nodes):
            if i > 0:
                parts.append(sep)
            parts.append(exp.Cast(this=n, to=str_dtype))
        return exp.Anonymous(this="CONCAT", expressions=parts)

    l_key_expr = _make_key(l_key_nodes)
    r_key_expr = _make_key(r_key_nodes)

    def _apply_where(sel: exp.Select, source: dict | None) -> exp.Select:
        if source and source.get("where_sql"):
            try:
                where_expr = sqlglot.parse_one(
                    source["where_sql"],
                    dialect=dialect,
                    error_level=sqlglot.ErrorLevel.WARN,
                )
                return sel.where(where_expr)
            except Exception:
                pass
        return sel

    l_alias = f"_jl_{idx}"
    r_alias = f"_jr_{idx}"
    ks_alias = f"_ks_{idx}"

    l_sub = _apply_where(
        exp.select(
            exp.alias_(l_key_expr, "join_key"),
            exp.alias_(exp.Count(this=exp.Star()), "cnt"),
        )
        .from_(_table_expr(l_src_table))
        .group_by(exp.Literal.number(1)),
        l_source,
    )
    r_sub = _apply_where(
        exp.select(
            exp.alias_(r_key_expr, "join_key"),
            exp.alias_(exp.Count(this=exp.Star()), "cnt"),
        )
        .from_(_table_expr(r_src_table))
        .group_by(exp.Literal.number(1)),
        r_source,
    )

    # Key-sample subquery: up to 100 distinct left key values, STRING_AGG'd into one string
    ks_inner = _apply_where(
        exp.select(exp.alias_(l_key_expr, "join_key"))
        .from_(_table_expr(l_src_table))
        .group_by(exp.Literal.number(1))
        .limit(100),
        l_source,
    )
    ks_outer = exp.select(
        exp.Anonymous(
            this="STRING_AGG",
            expressions=[exp.Cast(this=exp.column("join_key"), to=str_dtype)],
        )
    ).from_(ks_inner.subquery(ks_alias))

    def _lcnt() -> exp.Column:
        return exp.column("cnt", table=l_alias)

    def _rcnt() -> exp.Column:
        return exp.column("cnt", table=r_alias)

    def _rkey() -> exp.Column:
        return exp.column("join_key", table=r_alias)

    join_case = exp.Case(
        ifs=[
            exp.If(
                this=exp.And(
                    this=exp.EQ(
                        this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                    ),
                    expression=exp.EQ(
                        this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                    ),
                ),
                true=exp.Literal.string("one-to-one"),
            ),
            exp.If(
                this=exp.And(
                    this=exp.GT(
                        this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                    ),
                    expression=exp.EQ(
                        this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                    ),
                ),
                true=exp.Literal.string("many-to-one"),
            ),
            exp.If(
                this=exp.And(
                    this=exp.EQ(
                        this=exp.Max(this=_lcnt()), expression=exp.Literal.number(1)
                    ),
                    expression=exp.GT(
                        this=exp.Max(this=_rcnt()), expression=exp.Literal.number(1)
                    ),
                ),
                true=exp.Literal.string("one-to-many"),
            ),
        ],
        default=exp.Literal.string("many-to-many"),
    )

    # left_match_rate = CAST(COUNT(r.join_key) AS FLOAT) / NULLIF(COUNT(*), 0)
    # After a LEFT JOIN, r.join_key is NULL for non-matching left rows,
    # so COUNT(r.join_key) counts only matched rows.
    # NULLIF prevents division by zero when the left table is empty.
    left_match_rate = exp.Div(
        this=exp.Cast(
            this=exp.Count(this=_rkey()),
            to=exp.DataType.build(float_t),
        ),
        expression=exp.Nullif(
            this=exp.Count(this=exp.Star()),
            expression=exp.Literal.number(0),
        ),
    )

    return (
        exp.select(
            exp.alias_(exp.Literal.string("join"), "row_type"),
            exp.alias_(exp.Null(), "table_name"),
            exp.alias_(exp.Null(), "col_name"),
            exp.alias_(exp.Null(), "total_count"),
            exp.alias_(exp.Null(), "null_count"),
            exp.alias_(exp.Null(), "non_null_count"),
            exp.alias_(exp.Null(), "distinct_count"),
            exp.alias_(exp.Null(), "dup_count"),
            exp.alias_(exp.Null(), "min_val"),
            exp.alias_(exp.Null(), "max_val"),
            exp.alias_(exp.Null(), "top_values"),
            exp.alias_(exp.Literal.string(left_table), "left_table"),
            exp.alias_(exp.Literal.string(right_table), "right_table"),
            exp.alias_(exp.Literal.string(left_expr_str), "left_expr"),
            exp.alias_(exp.Literal.string(right_expr_str), "right_expr"),
            exp.alias_(join_case, "join_type"),
            exp.alias_(left_match_rate, "left_match_rate"),
            exp.alias_(exp.Avg(this=_rcnt()), "avg_right_per_left_key"),
            exp.alias_(exp.Max(this=_rcnt()), "max_right_per_left_key"),
            exp.alias_(ks_outer.subquery(), "left_key_sample"),
        )
        .from_(l_sub.subquery(l_alias))
        .join(
            r_sub.subquery(r_alias),
            on=exp.EQ(
                this=exp.column("join_key", table=l_alias),
                expression=exp.column("join_key", table=r_alias),
            ),
            join_type="left",
        )
        .sql(dialect=dialect)
    )


def _extract_ctes(sql_query: str, dialect: str = "bigquery") -> list[tuple[str, str]]:
    """Return (cte_name, cte_body_sql) pairs from a WITH clause, in definition order.

    Returns ``[]`` if the query has no CTEs or cannot be parsed.

    Examples:
        >>> ctes = _extract_ctes(
        ...     "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a JOIN b ON a.x = b.y"
        ... )
        >>> [name for name, _ in ctes]
        ['a', 'b']
    """
    try:
        tree = sqlglot.parse_one(
            sql_query, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
        with_node = tree.args.get("with_")
        if not with_node:
            return []
        return [
            (cte.alias or cte.name, cte.this.sql(dialect=dialect))
            for cte in with_node.expressions
        ]
    except Exception:
        return []


def _is_one_side(table: str, join_keys: set, cte_grain_map: dict) -> Optional[bool]:
    """Return True if *join_keys* cover the CTE grain (→ 'one' side), False if not, None if *table* is not a CTE.

    The short name (last dotted component) is tried first so that
    ``project.dataset.cte_name`` still matches ``cte_name`` in *cte_grain_map*.

    Examples:
        >>> _is_one_side("my_cte", {"date", "merchant"}, {"my_cte": ["date", "merchant"]})
        True
        >>> _is_one_side("my_cte", {"date"}, {"my_cte": ["date", "merchant"]})
        False
        >>> _is_one_side("real_table", {"id"}, {})
        >>> # None — not a CTE
    """
    short = table.split(".")[-1]
    for key in (short, table):
        if key in cte_grain_map:
            grain = set(cte_grain_map[key] or [])
            if not grain:
                return None  # grain unknown — can't infer
            return grain.issubset(join_keys)
    return None  # not a CTE


def _build_static_join_row(
    left_table: str,
    right_table: str,
    left_expr: str,
    right_expr: str,
    join_type: str,
    dialect: str,
) -> str:
    """Build a one-row SELECT (no FROM) with a statically-inferred join cardinality.

    Used when one or both join sides are CTEs and therefore cannot be queried
    directly.  Produces the same column schema as :func:`_build_join_query` so
    the row can participate in the same UNION ALL.
    """
    return exp.select(
        exp.alias_(exp.Literal.string("join"), "row_type"),
        exp.alias_(exp.Null(), "table_name"),
        exp.alias_(exp.Null(), "col_name"),
        exp.alias_(exp.Null(), "total_count"),
        exp.alias_(exp.Null(), "null_count"),
        exp.alias_(exp.Null(), "non_null_count"),
        exp.alias_(exp.Null(), "distinct_count"),
        exp.alias_(exp.Null(), "dup_count"),
        exp.alias_(exp.Null(), "min_val"),
        exp.alias_(exp.Null(), "max_val"),
        exp.alias_(exp.Null(), "top_values"),
        exp.alias_(exp.Literal.string(left_table), "left_table"),
        exp.alias_(exp.Literal.string(right_table), "right_table"),
        exp.alias_(exp.Literal.string(left_expr), "left_expr"),
        exp.alias_(exp.Literal.string(right_expr), "right_expr"),
        exp.alias_(exp.Literal.string(join_type), "join_type"),
        exp.alias_(exp.Null(), "left_match_rate"),
        exp.alias_(exp.Null(), "avg_right_per_left_key"),
        exp.alias_(exp.Null(), "max_right_per_left_key"),
        exp.alias_(exp.Null(), "left_key_sample"),
    ).sql(dialect=dialect)


def build_profile_query(
    schema: dict,
    used_columns: list[dict],
    dialect: str = "bigquery",
    options: dict | None = None,
    sql_query: str | None = None,
) -> str:
    """Build a single UNION ALL SQL query that profiles every used column.

    The user pastes this into their data-warehouse console, executes it, and
    passes the resulting rows (as JSON) to :func:`parse_profile_query_result`.

    Each UNION ALL branch produces **one row per column** with human-readable
    names:

    ===============  =======  =============================================
    Column           Type     Meaning
    ===============  =======  =============================================
    table_name       VARCHAR  Fully-qualified table name
    col_name         VARCHAR  Column name
    total_count      BIGINT   COUNT(*)
    null_count       BIGINT   COUNT(*) - COUNT(col)
    non_null_count   BIGINT   COUNT(col)
    distinct_count   BIGINT   COUNT(DISTINCT col)
    dup_count        BIGINT   # distinct values appearing more than once
    min_val          VARCHAR  CAST(MIN(col) AS STRING) — works for all types incl. strings
    max_val          VARCHAR  CAST(MAX(col) AS STRING) — works for all types incl. strings
    top_values       VARCHAR  Top-k values comma-joined via STRING_AGG
    ===============  =======  =============================================

    Identifier quoting is handled by sqlglot (backticks for BigQuery,
    double-quotes for everything else).

    Args:
        schema:       Full schema dict ``{"tables": [...]}``.
        used_columns: List of ``{"table": str, "used_columns": [str, ...]}``,
                      matching the format produced by the validator's
                      ``extract_used_columns`` step.
        dialect:      ``"bigquery"`` (default) | ``"duckdb"`` | ``"postgres"``.
        options:      Optional knobs:
                        ``"top_k_values"`` (int, default 10) — top values per
                        column.

    Returns:
        A single SQL string ready to execute.

    Raises:
        ValueError: If no column in *used_columns* matches a table in *schema*.

    Examples:
        Basic usage — result is a UNION ALL of one SELECT per column:

        >>> schema = {"tables": [{"name": "t", "columns": [
        ...     {"name": "x", "type": "STRING"},
        ...     {"name": "n", "type": "INTEGER"},
        ... ]}]}
        >>> sql = build_profile_query(schema, [{"table": "t", "used_columns": ["x", "n"]}])
        >>> "UNION ALL" in sql
        True

        Output columns use human-readable names:

        >>> "table_name" in sql and "col_name" in sql
        True
        >>> "total_count" in sql and "null_count" in sql
        True
        >>> "top_values" in sql
        True

        Unknown table raises:

        >>> build_profile_query(schema, [{"table": "unknown", "used_columns": ["x"]}])
        Traceback (most recent call last):
            ...
        ValueError: No matching columns found in schema for the given used_columns.

        Dialect affects identifier quoting:

        >>> sql_bq = build_profile_query(schema, [{"table": "t", "used_columns": ["x"]}], dialect="bigquery")
        >>> "`x`" in sql_bq
        True
        >>> sql_ddb = build_profile_query(schema, [{"table": "t", "used_columns": ["x"]}], dialect="duckdb")
        >>> '"x"' in sql_ddb
        True
    """
    options = options or {}
    top_k = options.get("top_k_values", 10)
    norm = normalize_schema(schema)

    type_map: dict[str, dict[str, str]] = {
        tbl: {
            col["name"]: col.get("bq_ddl_type") or col.get("type", "STRING")
            for col in norm["tables_by_name"][tbl].get("columns", [])
        }
        for tbl in norm["tables_by_name"]
    }

    parts: list[str] = []
    idx = 0

    print("<<<<<<<<<<<<<<<<<<<<<<<<used_columns")
    print(used_columns)

    for entry in used_columns:
        table = entry.get("table", "")
        col_list = entry.get("used_columns", [])
        if table not in norm["tables_by_name"]:
            continue

        for col in col_list:
            col_type = type_map.get(table, {}).get(col, "STRING")
            parts.append(_build_col_query(table, col, col_type, dialect, top_k, idx))
            idx += 1

    if not parts:
        raise ValueError(
            "No matching columns found in schema for the given used_columns."
        )

    # Append join cardinality branches if a SQL query was provided
    ctes: list[tuple[str, str]] = []
    cte_map: dict[str, str] = {}
    if sql_query:
        # Extract CTE definitions so SQL-based join branches can reference CTE names.
        ctes = _extract_ctes(sql_query, dialect=dialect)
        cte_map = dict(ctes)

        # Build CTE grain map for static inference.
        # If cte1 has grain (a, b) and the outer join is ON (a, b), the cte1 side is
        # definitively "one" — no need to run an expensive GROUP BY query.
        cte_grain_map: dict = {}
        try:
            from utils.find_grains import determine_query_grain

            _tables_and_columns = [
                {"table_name": t["name"], "columns": t.get("columns", [])}
                for t in schema.get("tables", [])
            ]
            _grain = determine_query_grain(sql_query, _tables_and_columns)
            cte_grain_map = _grain.get("cte_grain_map") or {}
        except Exception:
            cte_grain_map = {}

        # Group specs by (left_table, right_table) so compound AND conditions
        # (e.g. ON a.date = b.date AND a.merchant = b.merchant) are evaluated together
        # against the grain before deciding cardinality.
        # Filter out literal-equality specs (e.g. table.year = 2011) — those are
        # filters, not join key relationships: one side would have no column refs.
        from collections import defaultdict

        _grouped: dict = defaultdict(list)
        for spec in _collect_join_specs(sql_query, dialect=dialect):
            if spec.get("left_keys") and spec.get("right_keys"):
                _grouped[(spec["left_table"], spec["right_table"])].append(spec)

        for (left_table, right_table), pair_specs in _grouped.items():
            all_left_keys: set = set()
            all_right_keys: set = set()
            for s in pair_specs:
                all_left_keys.update(s.get("left_keys") or [])
                all_right_keys.update(s.get("right_keys") or [])

            left_is_one = _is_one_side(left_table, all_left_keys, cte_grain_map)
            right_is_one = _is_one_side(right_table, all_right_keys, cte_grain_map)

            if left_is_one is not None or right_is_one is not None:
                # At least one CTE side has a deterministic cardinality from grain analysis.
                # Unknown/real-table sides default to "many" (conservative).
                l_one = bool(left_is_one)
                r_one = bool(right_is_one)
                if l_one and r_one:
                    join_type = "one-to-one"
                elif not l_one and r_one:
                    join_type = "many-to-one"
                elif l_one and not r_one:
                    join_type = "one-to-many"
                else:
                    join_type = "many-to-many"
                left_expr_str = " AND ".join(s["left_expr_sql"] for s in pair_specs)
                right_expr_str = " AND ".join(s["right_expr_sql"] for s in pair_specs)
                try:
                    parts.append(
                        _build_static_join_row(
                            left_table,
                            right_table,
                            left_expr_str,
                            right_expr_str,
                            join_type,
                            dialect,
                        )
                    )
                    idx += 1
                except Exception:
                    pass
            else:
                # Both sides are real tables (or CTEs whose grain is unknown) —
                # measure cardinality via SQL.  One query per (left, right) pair
                # using all compound key conditions together.
                # For CTE sides, use lineage to resolve the real source table and apply
                # the CTE's WHERE conditions so the profiling query runs against actual data.
                l_src: dict | None = None
                r_src: dict | None = None
                if cte_map:
                    l_cte = left_table.split(".")[-1]
                    r_cte = right_table.split(".")[-1]
                    all_l_keys = [
                        k for s in pair_specs for k in (s.get("left_keys") or [])
                    ]
                    all_r_keys = [
                        k for s in pair_specs for k in (s.get("right_keys") or [])
                    ]
                    if l_cte in cte_map:
                        l_src = _resolve_cte_source(l_cte, all_l_keys, cte_map, dialect)
                    if r_cte in cte_map:
                        r_src = _resolve_cte_source(r_cte, all_r_keys, cte_map, dialect)
                try:
                    parts.append(
                        _build_join_query(
                            pair_specs, dialect, idx, l_source=l_src, r_source=r_src
                        )
                    )
                    idx += 1
                except Exception:
                    pass

    union_sql = "\n\nUNION ALL\n\n".join(parts)

    # Prepend CTE definitions so that any SQL-based join branches can reference them.
    if ctes:
        cte_defs = ",\n".join(f"{name} AS ({body})" for name, body in ctes)
        return f"WITH {cte_defs}\n\n{union_sql}"

    return union_sql


def detect_fk_candidates(profile: dict, used_columns: list[dict]) -> list[dict]:
    """Detect implicit FK / PK relationships from column-name matches + uniqueness.

    A column that appears under the **same name** in two different tables is a
    FK candidate when it is ``is_unique`` in one of those tables (the PK side).

    Returns a list of dicts::

        {
            "pk_table":  str,   # table where the column is unique (PK side)
            "pk_column": str,
            "fk_table":  str,   # table where duplicates exist (FK side)
            "fk_column": str,
        }

    Examples:
        >>> profile = {
        ...     "tables": {
        ...         "country_summary":  {"columns": {"country_code": {"is_unique": True}}},
        ...         "indicators_data":  {"columns": {"country_code": {"is_unique": False}}},
        ...     }
        ... }
        >>> used = [
        ...     {"table": "country_summary",  "used_columns": ["country_code"]},
        ...     {"table": "indicators_data",  "used_columns": ["country_code"]},
        ... ]
        >>> cands = detect_fk_candidates(profile, used)
        >>> len(cands)
        1
        >>> cands[0]["pk_table"]
        'country_summary'
        >>> cands[0]["fk_table"]
        'indicators_data'
    """
    from collections import defaultdict

    col_to_tables: dict = defaultdict(list)
    for entry in used_columns:
        tbl = entry.get("table", "")
        for col in entry.get("used_columns", []):
            if tbl not in col_to_tables[col]:
                col_to_tables[col].append(tbl)

    candidates: list[dict] = []
    seen: set = set()

    for col, tables in col_to_tables.items():
        if len(tables) < 2:
            continue
        for i, t1 in enumerate(tables):
            for t2 in tables[i + 1 :]:
                col1 = (
                    profile.get("tables", {})
                    .get(t1, {})
                    .get("columns", {})
                    .get(col, {})
                )
                col2 = (
                    profile.get("tables", {})
                    .get(t2, {})
                    .get("columns", {})
                    .get(col, {})
                )
                t1_unique = col1.get("is_unique", False)
                t2_unique = col2.get("is_unique", False)
                if not t1_unique and not t2_unique:
                    continue
                pk_table = t1 if t1_unique else t2
                fk_table = t2 if t1_unique else t1
                key = (pk_table, fk_table, col)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(
                    {
                        "pk_table": pk_table,
                        "pk_column": col,
                        "fk_table": fk_table,
                        "fk_column": col,
                    }
                )

    return candidates


def parse_profile_query_result(
    rows: list[dict],
    schema: dict,
    used_columns: list[dict],
) -> dict:
    """Convert the flat row list from :func:`build_profile_query` into a Profile dict.

    Reads the new one-row-per-column format produced by :func:`build_profile_query`
    (columns: ``table_name``, ``col_name``, ``total_count``, ``null_count``,
    ``non_null_count``, ``distinct_count``, ``dup_count``, ``min_val``,
    ``max_val``, ``top_values``).

    Args:
        rows:         List of dicts returned by the warehouse, each with keys
                      ``table_name``, ``col_name``, ``total_count``,
                      ``null_count``, ``non_null_count``, ``distinct_count``,
                      ``dup_count``, ``min_val``, ``max_val``, ``top_values``.
        schema:       Full schema dict.
        used_columns: Same list passed to :func:`build_profile_query`.

    Returns:
        Profile dict::

            {
                "tables": {
                    "<table>": {
                        "row_count": int,
                        "columns": {"<col>": { ...stats... }},
                        "correlations": [],
                    }
                },
                "joins": [],
            }

    Note:
        ``correlations`` is always ``[]`` because the single-query approach
        does not run cross-column group-by queries.

    Postconditions:
        * ``null_count + non_null_count == row_count`` for every column.
        * ``distinct_count <= non_null_count``.

    Examples:
        >>> schema = {"tables": [{"name": "t", "columns": [
        ...     {"name": "x", "type": "STRING"},
        ... ]}]}
        >>> rows = [
        ...     {
        ...         "table_name": "t", "col_name": "x",
        ...         "total_count": 10, "null_count": 2, "non_null_count": 8,
        ...         "distinct_count": 5, "dup_count": 1,
        ...         "min_val": "aaa", "max_val": "zzz",
        ...         "top_values": "foo,bar",
        ...     },
        ... ]
        >>> p = parse_profile_query_result(rows, schema, [{"table": "t", "used_columns": ["x"]}])
        >>> p["tables"]["t"]["row_count"]
        10
        >>> col = p["tables"]["t"]["columns"]["x"]
        >>> col["null_count"]
        2
        >>> col["non_null_count"]
        8
        >>> col["null_count"] + col["non_null_count"] == p["tables"]["t"]["row_count"]
        True
        >>> col["nullable_ratio"]
        0.2
        >>> col["top_values"]
        ['foo', 'bar']
        >>> col["is_categorical"]   # 5 distinct values ≤ threshold of 20
        True
        >>> col["min_value"]
        'aaa'
        >>> col["max_value"]
        'zzz'
        >>> p["joins"]
        []
    """
    norm = normalize_schema(schema)
    type_map: dict[str, dict[str, str]] = {
        tbl: {
            col["name"]: col.get("type", "STRING")
            for col in norm["tables_by_name"][tbl].get("columns", [])
        }
        for tbl in norm["tables_by_name"]
    }

    # Separate join rows from column rows
    col_rows = [r for r in rows if r.get("row_type") != "join"]
    join_rows = [r for r in rows if r.get("row_type") == "join"]

    # Index column rows by (table_name, col_name)
    row_map: dict[tuple, dict] = {}
    for row in col_rows:
        key = (row.get("table_name", ""), row.get("col_name", ""))
        row_map[key] = row

    def _f(v: Any) -> Optional[float]:
        return round(float(v), 4) if v is not None else None

    def _i(v: Any) -> Optional[int]:
        return int(v) if v is not None else None

    def _sa(v: Any) -> list[str]:
        return [x.strip() for x in str(v).split(",") if x.strip()] if v else []

    profile: dict = {
        "tables": {},
        "joins": [
            {
                "left_table": r["left_table"],
                "right_table": r["right_table"],
                "left_expr": r["left_expr"],
                "right_expr": r["right_expr"],
                "join_type_profiled": r["join_type"],
                # How many FK values exist in the PK table (1.0 = perfect integrity)
                "left_match_rate": _f(r.get("left_match_rate")),
                # Average right-table row count per left key value (fanout)
                "avg_right_per_left_key": _f(r.get("avg_right_per_left_key")),
                # Max right-table row count for any single left key value
                "max_right_per_left_key": _i(r.get("max_right_per_left_key")),
                # Sample of actual left key values → FK value pool for the generator
                "left_key_sample": _sa(r.get("left_key_sample")),
            }
            for r in join_rows
            if r.get("join_type")
        ],
        "fk_candidates": [],
    }
    threshold = 20  # categorical distinct threshold

    for entry in used_columns:
        table = entry.get("table", "")
        col_list = entry.get("used_columns", [])
        if table not in norm["tables_by_name"]:
            continue

        col_profiles: dict[str, dict] = {}
        row_count = 0

        for col in col_list:
            col_type = type_map.get(table, {}).get(col, "STRING")
            row = row_map.get((table, col), {})

            total = int(row.get("total_count") or 0)
            null_cnt = int(row.get("null_count") or 0)
            non_null = int(row.get("non_null_count") or 0)
            distinct = int(row.get("distinct_count") or 0)
            dup_cnt = int(row.get("dup_count") or 0)
            row_count = max(row_count, total)

            min_value = row.get("min_val")
            max_value = row.get("max_val")

            # top_values is a comma-separated STRING_AGG result
            top_raw = row.get("top_values") or ""
            top_values = (
                [v.strip() for v in top_raw.split(",") if v.strip()] if top_raw else []
            )

            # ── derived fields ───────────────────────────────────────────────
            nullable_ratio = round(null_cnt / total, 4) if total > 0 else 0.0
            is_unique = distinct == total and null_cnt == 0 and total > 0
            is_unique_non_null = distinct == non_null and non_null > 0
            is_categorical = 0 < distinct <= threshold

            col_profiles[col] = {
                "type": col_type,
                "nullable_ratio": nullable_ratio,
                "null_count": null_cnt,
                "non_null_count": non_null,
                "is_always_null": null_cnt == total and total > 0,
                "is_never_null": null_cnt == 0,
                "distinct_count": distinct,
                "duplicate_count": dup_cnt,
                "is_unique": is_unique,
                "is_unique_non_null": is_unique_non_null,
                "is_categorical": is_categorical,
                "top_values": top_values,
                "min_value": min_value,
                "max_value": max_value,
            }

        profile["tables"][table] = {
            "row_count": row_count,
            "columns": col_profiles,
            "correlations": [],  # not available from single-query profile
        }

    profile["fk_candidates"] = detect_fk_candidates(profile, used_columns)
    return profile
