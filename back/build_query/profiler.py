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

import logging
from typing import Any, Callable, Optional

import sqlglot
from sqlglot import expressions as exp

try:
    import utils.logger  # noqa: F401 — registers DIAG level (15)
except ImportError:
    pass

logger = logging.getLogger(__name__)


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

    Applies to ARRAY, STRUCT, RECORD (BigQuery nested/repeated fields) and
    GEOGRAPHY / GEOMETRY types which do not support aggregate functions.

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
        >>> _is_unprofilable("GEOGRAPHY")
        True
        >>> _is_unprofilable("GEOMETRY")
        True
        >>> _is_unprofilable("STRING")
        False
        >>> _is_unprofilable("INTEGER")
        False
    """
    upper = col_type.upper().strip()
    return upper in (
        "RECORD",
        "STRUCT",
        "ARRAY",
        "GEOGRAPHY",
        "GEOMETRY",
    ) or upper.startswith(("ARRAY<", "STRUCT<"))


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


# ─── Temporal regularity detection ───────────────────────────────────────────

_TEMPORAL_TYPES = frozenset({"DATE", "TIMESTAMP", "DATETIME", "TIME"})


def _is_temporal(col_type: str) -> bool:
    """Return True if *col_type* is a date or time type.

    Examples:
        >>> _is_temporal("DATE")
        True
        >>> _is_temporal("TIMESTAMP")
        True
        >>> _is_temporal("STRING")
        False
        >>> _is_temporal("timestamp")
        True
    """
    return col_type.upper().strip() in _TEMPORAL_TYPES


def _regularity_case_days(dialect: str) -> str:
    """Return a SQL CASE expression classifying a day-diff into a regularity label."""
    if dialect == "bigquery":
        cast_s = "CAST(CAST(mode_diff AS INT64) AS STRING)"
    else:
        cast_s = "CAST(CAST(mode_diff AS INTEGER) AS VARCHAR)"
    return (
        "CASE"
        "  WHEN ratio < 0.8 THEN 'irregular'"
        "  WHEN mode_diff = 1 THEN 'daily'"
        "  WHEN mode_diff = 7 THEN 'weekly'"
        "  WHEN mode_diff BETWEEN 28 AND 31 THEN 'monthly'"
        "  WHEN mode_diff BETWEEN 89 AND 92 THEN 'quarterly'"
        "  WHEN mode_diff BETWEEN 365 AND 366 THEN 'yearly'"
        f"  ELSE CONCAT('every_', {cast_s}, 'd')"
        " END"
    )


def _regularity_case_seconds(dialect: str) -> str:
    """Return a SQL CASE expression classifying a second-diff into a regularity label."""
    if dialect == "bigquery":
        cast_s = "CAST(CAST(mode_diff AS INT64) AS STRING)"
        cast_m = "CAST(CAST(CAST(mode_diff AS INT64) / 60 AS INT64) AS STRING)"
        cast_h = "CAST(CAST(CAST(mode_diff AS INT64) / 3600 AS INT64) AS STRING)"
    else:
        cast_s = "CAST(CAST(mode_diff AS INTEGER) AS VARCHAR)"
        cast_m = "CAST(CAST(mode_diff / 60 AS INTEGER) AS VARCHAR)"
        cast_h = "CAST(CAST(mode_diff / 3600 AS INTEGER) AS VARCHAR)"
    return (
        "CASE"
        "  WHEN ratio < 0.8 THEN 'irregular'"
        "  WHEN mode_diff BETWEEN 59 AND 61 THEN 'minutely'"
        "  WHEN mode_diff BETWEEN 3599 AND 3601 THEN 'hourly'"
        "  WHEN mode_diff BETWEEN 86399 AND 86401 THEN 'daily'"
        "  WHEN mode_diff BETWEEN 604799 AND 604801 THEN 'weekly'"
        "  WHEN mode_diff BETWEEN 2419200 AND 2678400 THEN 'monthly'"
        "  WHEN mode_diff BETWEEN 31536000 AND 31622400 THEN 'yearly'"
        f"  WHEN mode_diff < 60 THEN CONCAT('every_', {cast_s}, 's')"
        f"  WHEN mode_diff < 3600 THEN CONCAT('every_', {cast_m}, 'min')"
        f"  ELSE CONCAT('every_', {cast_h}, 'h')"
        " END"
    )


def _build_regularity_query(
    table_name: str, col: str, col_type: str, dialect: str = "bigquery"
) -> str | None:
    """Build a SQL query that returns a single ``date_regularity`` string.

    Uses LAG to compute consecutive diffs between sorted non-null values, finds
    the modal diff, and classifies it (``"daily"``, ``"weekly"``, ``"monthly"``,
    ``"hourly"``, ``"irregular"``, …).  Returns ``None`` for non-temporal types.

    Examples:
        >>> q = _build_regularity_query("events", "event_date", "DATE")
        >>> q is not None and "DATE_DIFF" in q and "date_regularity" in q
        True
        >>> _build_regularity_query("events", "name", "STRING") is None
        True
    """
    if not _is_temporal(col_type):
        return None

    # These helpers are defined later in the file but resolved at call time.
    col_q = exp.to_identifier(col, quoted=True).sql(dialect=dialect)
    tbl_sql = _table_expr(table_name).sql(dialect=dialect)  # type: ignore[name-defined]
    upper = col_type.upper().strip()
    is_date_only = upper == "DATE"

    if is_date_only:
        if dialect == "bigquery":
            diff_expr = f"DATE_DIFF({col_q}, LAG({col_q}) OVER (ORDER BY {col_q}), DAY)"
        elif dialect in ("duckdb", "snowflake"):
            diff_expr = (
                f"DATEDIFF('day', LAG({col_q}) OVER (ORDER BY {col_q}), {col_q})"
            )
        else:
            diff_expr = f"({col_q} - LAG({col_q}) OVER (ORDER BY {col_q}))"
        case_sql = _regularity_case_days(dialect)
    else:
        if dialect == "bigquery":
            diff_expr = (
                f"TIMESTAMP_DIFF(CAST({col_q} AS TIMESTAMP),"
                f" LAG(CAST({col_q} AS TIMESTAMP)) OVER (ORDER BY {col_q}), SECOND)"
            )
        elif dialect in ("duckdb", "snowflake"):
            diff_expr = (
                f"DATEDIFF('second', LAG({col_q}) OVER (ORDER BY {col_q}), {col_q})"
            )
        else:
            diff_expr = (
                f"EXTRACT(EPOCH FROM ({col_q} - LAG({col_q}) OVER (ORDER BY {col_q})))"
            )
        case_sql = _regularity_case_seconds(dialect)

    if dialect == "bigquery":
        float_t = "FLOAT64"
    elif dialect == "snowflake":
        float_t = "FLOAT"
    else:
        float_t = "DOUBLE"

    return (
        f"SELECT {case_sql} AS date_regularity"
        f" FROM ("
        f"  SELECT mode_diff,"
        f"         CAST(mode_count AS {float_t}) / NULLIF(total_count, 0) AS ratio"
        f"  FROM ("
        f"    SELECT diff AS mode_diff, COUNT(*) AS mode_count,"
        f"           SUM(COUNT(*)) OVER () AS total_count,"
        f"           ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn"
        f"    FROM ("
        f"      SELECT {diff_expr} AS diff"
        f"      FROM {tbl_sql}"
        f"      WHERE {col_q} IS NOT NULL"
        f"    ) _d"
        f"    WHERE diff IS NOT NULL AND diff > 0"
        f"    GROUP BY 1"
        f"  ) _m"
        f"  WHERE rn = 1"
        f" ) _r"
    )


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
    table_name: str,
    col: dict,
    options: dict,
    dialect: str = "bigquery",
    partition_where: str | None = None,
) -> dict[str, str]:
    """Return a dict of named SQL strings for profiling one column.

    Always includes ``"basic"``, ``"duplicates"``, and ``"top_values"`` keys.
    Adds ``"minmax"`` only when the column type is orderable (numeric /
    temporal).

    Args:
        table_name:      Fully-qualified or bare table name as it appears in SQL.
        col:             Column descriptor dict with at least ``"name"`` and
                         optionally ``"type"``.
        options:         Knobs dict; recognises ``"top_k_values"`` (default 20).
        dialect:         SQL dialect for identifier quoting (default ``"bigquery"``).
        partition_where: Optional SQL WHERE fragment (e.g. from
                         :func:`_build_partition_where`) applied to every query
                         to restrict the scan to recent partitions.

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

    pw_expr: exp.Expression | None = None
    if partition_where:
        try:
            pw_expr = sqlglot.parse_one(partition_where, dialect=dialect)
        except Exception:
            pw_expr = None

    def _apply_pw(q: exp.Select) -> exp.Select:
        return q.where(pw_expr.copy()) if pw_expr is not None else q

    basic_q = exp.select(
        exp.alias_(exp.Count(this=exp.Star()), "total_count"),
        exp.alias_(
            exp.Anonymous(this="COUNTIF", expressions=[_is_null()]), "null_count"
        ),
        exp.alias_(
            exp.Count(this=exp.Distinct(expressions=[_cref()])), "distinct_count"
        ),
    ).from_(_table_expr(table_name))
    basic = _apply_pw(basic_q).sql(dialect=dialect)

    dup_base = (
        exp.select(_cref(), exp.alias_(exp.Count(this=exp.Star()), "cnt"))
        .from_(_table_expr(table_name))
        .where(_not_null())
    )
    dup_inner = (
        _apply_pw(dup_base)
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

    top_q = exp.select(
        exp.alias_(_cref(), "val"),
        exp.alias_(exp.Count(this=exp.Star()), "cnt"),
    ).from_(_table_expr(table_name))
    top_values = (
        _apply_pw(top_q)
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
        mm_q = (
            exp.select(
                exp.alias_(exp.Min(this=_cref()), "min_value"),
                exp.alias_(exp.Max(this=_cref()), "max_value"),
            )
            .from_(_table_expr(table_name))
            .where(_not_null())
        )
        queries["minmax"] = _apply_pw(mm_q).sql(dialect=dialect)

    if _is_temporal(col.get("type", "")):
        reg_q = _build_regularity_query(
            table_name, col_name, col.get("type", ""), dialect
        )
        if reg_q:
            queries["regularity"] = reg_q

    return queries


def build_column_profile(
    table_name: str,
    col: dict,
    sql_executor: Callable[[str], list[dict]],
    options: dict,
    dialect: str = "bigquery",
    partition_where: str | None = None,
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
    queries = build_column_profile_queries(
        table_name, col, options, dialect=dialect, partition_where=partition_where
    )
    col_type = col.get("type", "STRING")
    threshold = options.get("categorical_distinct_threshold", 20)
    logger.diag("[profiler] basic query: %s", queries["basic"])

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

    # Temporal regularity
    date_regularity: str | None = None
    if "regularity" in queries:
        try:
            reg_rows = sql_executor(queries["regularity"])
            if reg_rows:
                date_regularity = reg_rows[0].get("date_regularity") or None
        except Exception:
            pass

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
        "date_regularity": date_regularity,
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
                .from_(_table_expr(table_name))
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
    partition_limit: int | None = options.get("partition_limit", 3)
    norm = normalize_schema(schema)

    result: dict = {"tables": {}, "joins": []}

    for table in norm["tables"]:
        table_name: str = table["name"]
        columns: list[dict] = table.get("columns", [])
        logger.diag("[profiler] table=%s cols=%d", table_name, len(columns))

        # Compute partition WHERE once per table
        pw: str | None = None
        if partition_limit:
            pw = _build_partition_where(
                table_name, table.get("partition") or {}, dialect, partition_limit
            )
            if pw:
                logger.diag("[profiler] partition_where for %s: %s", table_name, pw)

        # Row count (restricted to recent partitions when available)
        rc_q = exp.select(exp.alias_(exp.Count(this=exp.Star()), "row_count")).from_(
            _table_expr(table_name)
        )
        if pw:
            try:
                rc_q = rc_q.where(sqlglot.parse_one(pw, dialect=dialect))
            except Exception:
                pass
        rc_sql = rc_q.sql(dialect=dialect)
        logger.diag("[profiler] row_count query: %s", rc_sql)
        try:
            rc_rows = sql_executor(rc_sql)
            row_count = int((rc_rows[0].get("row_count") or 0)) if rc_rows else 0
            logger.diag("[profiler] row_count=%d", row_count)
        except Exception as rc_exc:
            logger.warning("[profiler] row_count failed for %s: %s", table_name, rc_exc)
            row_count = 0

        # Column profiles
        col_profiles: dict[str, dict] = {}
        for col in columns:
            col_type = col.get("type", "UNKNOWN")
            if _is_unprofilable(col_type) or "." in col["name"]:
                col_profiles[col["name"]] = {
                    "type": col_type,
                    "skipped": "unprofilable type",
                }
                continue
            logger.diag("[profiler] col=%s type=%s", col["name"], col_type)
            try:
                col_profiles[col["name"]] = build_column_profile(
                    table_name,
                    col,
                    sql_executor,
                    options,
                    dialect=dialect,
                    partition_where=pw,
                )
            except Exception as exc:
                logger.warning(
                    "[profiler] col=%s table=%s failed: %s",
                    col["name"],
                    table_name,
                    exc,
                )
                col_profiles[col["name"]] = {
                    "type": col_type,
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

    def source_cols(expr: exp.Expression) -> list[str]:
        return [c.name for c in expr.find_all(exp.Column)]

    def _direct_alias_map(select: exp.Select) -> dict[str, str]:
        """Return alias→full_table_name for the direct FROM and JOIN tables only.

        Unlike find_all(exp.Table), this never descends into scalar subqueries,
        LATERAL views, or other nested scopes — so an alias reused in a WHERE
        scalar subquery (e.g. ``WHERE x = (SELECT y FROM t AS b WHERE …)``)
        cannot overwrite the outer JOIN alias ``b``.
        """
        m: dict[str, str] = {}

        def _register(node: exp.Expression) -> None:
            if isinstance(node, exp.Table):
                full = _full_table_name(node)
                if node.alias:
                    m[node.alias] = full
                m[node.name] = full
            elif isinstance(node, exp.Alias) and isinstance(node.this, exp.Table):
                full = _full_table_name(node.this)
                m[node.alias] = full
                m[node.this.name] = full
            elif isinstance(node, (exp.Subquery, exp.Alias)):
                # Inline subquery that survived eliminate_subqueries — keep alias as-is
                # so build_profile_query can decide whether to profile it.
                alias = node.alias
                if alias:
                    m[alias] = alias

        from_clause = select.args.get("from") or select.args.get("from_")
        if from_clause:
            _register(from_clause.this)
        for join in select.args.get("joins") or []:
            _register(join.this)
        return m

    # Global alias map (all tables in the tree) — used only for primary_table fallback.
    _global_alias_map: dict[str, str] = {}
    for _t in tree.find_all(exp.Table):
        _full = _full_table_name(_t)
        if _t.alias:
            _global_alias_map[_t.alias] = _full
        _global_alias_map[_t.name] = _full

    # Identify the primary FROM table of the outermost SELECT.
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

        # Walk up to the containing SELECT and build a scope-local alias map
        # from its direct FROM/JOIN only — no nested-subquery traversal.
        _scope: exp.Expression | None = join_node.parent
        while _scope is not None and not isinstance(_scope, exp.Select):
            _scope = _scope.parent
        local_alias_map = (
            _direct_alias_map(_scope) if _scope is not None else _global_alias_map
        )

        # Local primary FROM table (fallback when l_expr has no table qualifier).
        local_primary: str = primary_table
        if _scope is not None:
            _local_from = _scope.args.get("from") or _scope.args.get("from_")
            if _local_from:
                _lf_node = _local_from.this
                if isinstance(_lf_node, exp.Table):
                    _lf_key = _lf_node.alias or _lf_node.name
                    local_primary = local_alias_map.get(
                        _lf_key, _full_table_name(_lf_node)
                    )

        def resolve(name: str, _m: dict = local_alias_map) -> str:
            return _m.get(name, name)

        # Right-side table
        jt = join_node.this
        if isinstance(jt, exp.Alias):
            right_alias = jt.alias
            if isinstance(jt.this, exp.Table):
                right_table = resolve(jt.alias) or _full_table_name(jt.this)
            else:
                # CTE or inline subquery — keep the alias; build_profile_query resolves it
                right_table = jt.alias or ""
            if not right_table:
                continue
        elif isinstance(jt, exp.Table):
            right_table = _full_table_name(jt)
            # Use the alias (e.g. "i1") if present so two JOINs on the same physical
            # table with different aliases are kept as separate grouping keys.
            right_alias = jt.alias or jt.name
        elif isinstance(jt, exp.Subquery):
            # sqlglot represents JOIN (SELECT ...) AS alias as exp.Subquery with .alias set
            right_table = jt.alias or ""
            right_alias = jt.alias or ""
            if not right_table:
                continue
        else:
            continue

        # Walk AND-separated conditions
        def collect_eq(
            node: exp.Expression,
            _rt: str = right_table,
            _lp: str = local_primary,
            _ra: str = right_alias,
        ):
            if isinstance(node, exp.And):
                collect_eq(node.left, _rt, _lp, _ra)
                collect_eq(node.right, _rt, _lp, _ra)
            elif isinstance(node, exp.EQ):
                left = node.left
                right = node.right
                left_tbl = resolve(left.table) if isinstance(left, exp.Column) else ""
                right_tbl = (
                    resolve(right.table) if isinstance(right, exp.Column) else ""
                )

                # Determine which side belongs to which table.
                # right_table is the JOIN node's table; the other side is the FROM/left table.
                if right_tbl == _rt or (not left_tbl and not right_tbl):
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
                        "left_table": l_resolved or _lp,
                        "right_table": _rt,
                        "right_alias": _ra,
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
            .from_(_table_expr(left_table))
            .group_by(exp.Literal.number(1))
        )
        r_cte = (
            exp.select(
                exp.alias_(right_expr_node, "join_key"),
                exp.alias_(exp.Count(this=exp.Star()), "cnt"),
            )
            .from_(_table_expr(right_table))
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


def _build_partition_where(
    table: str, partition: dict, dialect: str, limit: int
) -> str | None:
    """Return a SQL WHERE clause string that restricts to the last *limit* partitions.

    Only time-partitioned tables are supported (range partitioning is skipped).
    For ingestion-time partitioning (field=None) the pseudo-column _PARTITIONDATE
    is used.  For column-based partitioning the named field is used.
    """
    if not partition or partition.get("type") != "time":
        return None
    field = partition.get("field")
    col = field if field else "_PARTITIONDATE"
    col_q = _col_id(col, dialect)
    tbl_q = _table_expr(table).sql(dialect=dialect)
    return f"{col_q} IN (SELECT DISTINCT {col_q} FROM {tbl_q} ORDER BY {col_q} DESC LIMIT {limit})"


def _build_col_query(
    table: str,
    col: str,
    col_type: str,
    dialect: str,
    top_k: int,
    idx: int,
    partition_where: str | None = None,
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

    pw_expr: exp.Expression | None = None
    if partition_where:
        try:
            pw_expr = sqlglot.parse_one(partition_where, dialect=dialect)
        except Exception:
            pw_expr = None

    def _cref() -> exp.Column:
        return exp.Column(this=exp.Identifier(this=col, quoted=True))

    def _not_null() -> exp.Expression:
        return exp.Not(this=exp.Is(this=_cref(), expression=exp.Null()))

    if not complex_col:
        dup_base = exp.select(_cref()).from_(_table_expr(table)).where(_not_null())
        if pw_expr is not None:
            dup_base = dup_base.where(pw_expr.copy())
        dup_inner = dup_base.group_by(exp.Literal.number(1)).having(
            exp.GT(this=exp.Count(this=exp.Star()), expression=exp.Literal.number(1))
        )
        dup_expr: exp.Expression = exp.select(exp.Count(this=exp.Star())).from_(
            dup_inner.subquery(f"_dup_{idx}")
        )

        top_alias = f"_top_{idx}"
        top_base = (
            exp.select(_cref(), exp.alias_(exp.Count(this=exp.Star()), "_cnt"))
            .from_(_table_expr(table))
            .where(_not_null())
        )
        if pw_expr is not None:
            top_base = top_base.where(pw_expr.copy())
        top_inner = (
            top_base.group_by(exp.Literal.number(1))
            .order_by(exp.Ordered(this=exp.column("_cnt"), desc=True))
            .limit(top_k)
        )
        top_expr: exp.Expression = exp.select(
            exp.Anonymous(
                this="STRING_AGG",
                expressions=[
                    exp.Cast(this=_cref(), to=str_dtype),
                    exp.Literal.string("|||"),
                ],
            )
        ).from_(top_inner.subquery(top_alias))

        distinct_expr: exp.Expression = exp.Count(
            this=exp.Distinct(expressions=[_cref()])
        )
        min_expr: exp.Expression = exp.Cast(this=exp.Min(this=_cref()), to=str_dtype)
        max_expr: exp.Expression = exp.Cast(this=exp.Max(this=_cref()), to=str_dtype)
    else:
        dup_expr = exp.Null()
        top_expr = exp.Null()
        distinct_expr = exp.Null()
        min_expr = exp.Null()
        max_expr = exp.Null()

    # Temporal regularity scalar subquery
    reg_expr: exp.Expression = exp.Null()
    if _is_temporal(col_type) and not complex_col:
        try:
            _reg_raw = _build_regularity_query(table, col, col_type, dialect)
            if _reg_raw:
                reg_expr = sqlglot.parse_one(f"({_reg_raw})", dialect=dialect)
        except Exception:
            pass

    outer = exp.select(
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
        exp.alias_(dup_expr.subquery() if not complex_col else dup_expr, "dup_count"),
        exp.alias_(min_expr, "min_val"),
        exp.alias_(max_expr, "max_val"),
        exp.alias_(top_expr.subquery() if not complex_col else top_expr, "top_values"),
        exp.alias_(exp.Null(), "left_table"),
        exp.alias_(exp.Null(), "right_table"),
        exp.alias_(exp.Null(), "left_expr"),
        exp.alias_(exp.Null(), "right_expr"),
        exp.alias_(exp.Null(), "join_type"),
        exp.alias_(exp.Null(), "left_match_rate"),
        exp.alias_(exp.Null(), "avg_right_per_left_key"),
        exp.alias_(exp.Null(), "max_right_per_left_key"),
        exp.alias_(exp.Null(), "left_key_sample"),
        exp.alias_(exp.Null(), "right_where_sql"),
        exp.alias_(reg_expr, "date_regularity"),
    ).from_(_table_expr(table))
    if pw_expr is not None:
        outer = outer.where(pw_expr.copy())
    return outer.sql(dialect=dialect)


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
    cte_names: set[str] | None = None,
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

    def _table_ref(name: str) -> exp.Expression:
        # CTE names resolved via the WITH clause must not be backtick-quoted in
        # BigQuery — quoted identifiers are treated as table literals requiring a
        # dataset qualifier, while unquoted names are resolved as CTE references.
        if cte_names and name in cte_names:
            return exp.Table(this=exp.Identifier(this=name, quoted=False))
        return _table_expr(name)

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
        .from_(_table_ref(l_src_table))
        .group_by(exp.Literal.number(1)),
        l_source,
    )
    r_sub = _apply_where(
        exp.select(
            exp.alias_(r_key_expr, "join_key"),
            exp.alias_(exp.Count(this=exp.Star()), "cnt"),
        )
        .from_(_table_ref(r_src_table))
        .group_by(exp.Literal.number(1)),
        r_source,
    )

    # Key-sample subquery: up to 100 distinct left key values, STRING_AGG'd into one string
    ks_inner = _apply_where(
        exp.select(exp.alias_(l_key_expr, "join_key"))
        .from_(_table_ref(l_src_table))
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

    right_where_val: exp.Expression = (
        exp.Literal.string(r_source["where_sql"])
        if r_source and r_source.get("where_sql")
        else exp.Null()
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
            exp.alias_(exp.Literal.string(l_src_table), "left_table"),
            exp.alias_(exp.Literal.string(r_src_table), "right_table"),
            exp.alias_(exp.Literal.string(left_expr_str), "left_expr"),
            exp.alias_(exp.Literal.string(right_expr_str), "right_expr"),
            exp.alias_(join_case, "join_type"),
            exp.alias_(left_match_rate, "left_match_rate"),
            exp.alias_(exp.Avg(this=_rcnt()), "avg_right_per_left_key"),
            exp.alias_(exp.Max(this=_rcnt()), "max_right_per_left_key"),
            exp.alias_(ks_outer.subquery(), "left_key_sample"),
            exp.alias_(right_where_val, "right_where_sql"),
            exp.alias_(exp.Null(), "date_regularity"),
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


def _extract_subquery_aliases(
    sql_query: str, dialect: str = "bigquery"
) -> dict[str, str]:
    """Return {alias: body_sql} for every inline subquery in a FROM or JOIN clause.

    Examples:
        >>> q = "SELECT * FROM t JOIN (SELECT id FROM s WHERE active = 1) AS sub ON t.id = sub.id"
        >>> aliases = _extract_subquery_aliases(q, dialect="duckdb")
        >>> list(aliases.keys())
        ['sub']
        >>> "SELECT" in aliases["sub"]
        True

        FROM-clause subqueries are also captured:

        >>> q2 = "SELECT * FROM (SELECT id FROM s WHERE active = 1) src JOIN t ON src.id = t.id"
        >>> aliases2 = _extract_subquery_aliases(q2, dialect="duckdb")
        >>> "src" in aliases2
        True
    """
    try:
        tree = sqlglot.parse_one(
            sql_query, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return {}
    result: dict[str, str] = {}

    # FROM-clause subqueries: FROM (SELECT ...) AS alias
    from_node = tree.find(exp.From)
    if from_node:
        src = from_node.this
        if isinstance(src, exp.Subquery) and src.alias:
            result[src.alias] = src.this.sql(dialect=dialect)

    for join_node in tree.find_all(exp.Join):
        jt = join_node.this
        # sqlglot represents JOIN (SELECT ...) AS alias as exp.Subquery (jt.this = Select)
        if isinstance(jt, exp.Subquery) and jt.alias:
            result[jt.alias] = jt.this.sql(dialect=dialect)
        elif isinstance(jt, exp.Alias) and isinstance(jt.this, exp.Subquery):
            alias = jt.alias
            if alias:
                result[alias] = jt.this.this.sql(dialect=dialect)
    return result


def _resolve_alias_to_table(
    alias: str, alias_map: dict[str, str], dialect: str = "bigquery"
) -> str:
    """Return the base real-table name for a CTE or subquery alias.

    Parses the alias body and returns the first real table found in its FROM
    clause.  Falls back to *alias* on any parse error.

    Examples:
        >>> body = "SELECT id FROM myproject.mydataset.stations WHERE status = 'active'"
        >>> _resolve_alias_to_table("s", {"s": body})
        'myproject.mydataset.stations'

        >>> _resolve_alias_to_table("unknown", {})
        'unknown'
    """
    body_sql = alias_map.get(alias)
    if not body_sql:
        return alias
    try:
        tree = sqlglot.parse_one(
            body_sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
        from_node = tree.find(exp.From)
        if from_node:
            tbl = from_node.find(exp.Table)
            if tbl:
                parts: list[str] = []
                catalog = tbl.args.get("catalog")
                db = tbl.args.get("db")
                if catalog:
                    parts.append(
                        catalog.name if hasattr(catalog, "name") else str(catalog)
                    )
                if db:
                    parts.append(db.name if hasattr(db, "name") else str(db))
                parts.append(tbl.name)
                return ".".join(parts)
    except Exception:
        pass
    return alias


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
        exp.alias_(exp.Null(), "right_where_sql"),
        exp.alias_(exp.Null(), "date_regularity"),
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
    partition_limit: int | None = options.get("partition_limit", 3)
    norm = normalize_schema(schema)

    type_map: dict[str, dict[str, str]] = {
        tbl: {
            col["name"]: col.get("bq_ddl_type") or col.get("type", "STRING")
            for col in norm["tables_by_name"][tbl].get("columns", [])
        }
        for tbl in norm["tables_by_name"]
    }

    # Pre-build partition WHERE clauses per table (None when no partition info or limit disabled).
    partition_where_map: dict[str, str | None] = {}
    if partition_limit:
        for tbl_name, tbl_dict in norm["tables_by_name"].items():
            partition_where_map[tbl_name] = _build_partition_where(
                tbl_name, tbl_dict.get("partition") or {}, dialect, partition_limit
            )

    parts: list[str] = []
    idx = 0

    for entry in used_columns:
        table = entry.get("table", "")
        col_list = entry.get("used_columns", [])
        if table not in norm["tables_by_name"]:
            continue

        pw = partition_where_map.get(table) if partition_limit else None
        for col in col_list:
            col_type = type_map.get(table, {}).get(col, "STRING")
            parts.append(
                _build_col_query(
                    table, col, col_type, dialect, top_k, idx, partition_where=pw
                )
            )
            idx += 1

    if not parts:
        raise ValueError(
            "No matching columns found in schema for the given used_columns."
        )

    # Append join cardinality branches if a SQL query was provided
    ctes: list[tuple[str, str]] = []
    cte_map: dict[str, str] = {}
    if sql_query:
        # Hoist inline FROM-clause subqueries (e.g. FROM (...) a inside a CTE body)
        # into top-level CTEs before any further processing.  This makes local aliases
        # like `a` globally referenceable as CTE names, fixing queries that would
        # otherwise generate FROM `a` (invalid in BigQuery — requires dataset qualification).
        # Duplicate aliases get unique suffixes (a → a_2) automatically.
        try:
            from sqlglot.optimizer.eliminate_subqueries import (
                eliminate_subqueries as _elim_subq,
            )

            _parsed = sqlglot.parse_one(
                sql_query, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
            )
            sql_query = _elim_subq(_parsed).sql(dialect=dialect)
        except Exception:
            pass

        # Extract CTE definitions so SQL-based join branches can reference CTE names.
        ctes = _extract_ctes(sql_query, dialect=dialect)
        cte_map = dict(ctes)

        # Also extract inline JOIN subquery aliases, e.g. JOIN (...) AS s.
        # Merged with cte_map so _resolve_cte_source handles both transparently.
        subquery_alias_map = _extract_subquery_aliases(sql_query, dialect=dialect)
        full_alias_map = {**cte_map, **subquery_alias_map}

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

        # Group specs by (left_table, right_table, right_alias) so compound AND
        # conditions (e.g. ON a.date = b.date AND a.merchant = b.merchant) are
        # evaluated together, while two separate JOINs on the same physical table
        # with different aliases (e.g. JOIN items i1 … JOIN items i2) stay distinct.
        # Filter out literal-equality specs (e.g. table.year = 2011) — those are
        # filters, not join key relationships: one side would have no column refs.
        from collections import defaultdict

        _grouped: dict = defaultdict(list)
        for spec in _collect_join_specs(sql_query, dialect=dialect):
            if spec.get("left_keys") and spec.get("right_keys"):
                _grouped[
                    (
                        spec["left_table"],
                        spec["right_table"],
                        spec.get("right_alias", ""),
                    )
                ].append(spec)

        for (left_table, right_table, _), pair_specs in _grouped.items():
            all_left_keys: set = set()
            all_right_keys: set = set()
            for s in pair_specs:
                all_left_keys.update(s.get("left_keys") or [])
                all_right_keys.update(s.get("right_keys") or [])

            left_is_one = _is_one_side(left_table, all_left_keys, cte_grain_map)
            right_is_one = _is_one_side(right_table, all_right_keys, cte_grain_map)

            # Resolve CTE/subquery aliases to real base table names for storage
            l_short = left_table.split(".")[-1]
            r_short = right_table.split(".")[-1]
            l_real = (
                _resolve_alias_to_table(l_short, full_alias_map, dialect)
                if l_short in full_alias_map
                else left_table
            )
            r_real = (
                _resolve_alias_to_table(r_short, full_alias_map, dialect)
                if r_short in full_alias_map
                else right_table
            )

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
                            l_real,
                            r_real,
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
                # Both sides are real tables (or CTEs/subqueries whose grain is unknown) —
                # measure cardinality via SQL.  One query per (left, right) pair
                # using all compound key conditions together.
                # For CTE/subquery sides, use lineage to resolve the real source table and
                # apply the WHERE conditions so the profiling query runs against actual data.
                l_src: dict | None = None
                r_src: dict | None = None
                if full_alias_map:
                    all_l_keys = [
                        k for s in pair_specs for k in (s.get("left_keys") or [])
                    ]
                    all_r_keys = [
                        k for s in pair_specs for k in (s.get("right_keys") or [])
                    ]
                    if l_short in full_alias_map:
                        l_src = _resolve_cte_source(
                            l_short, all_l_keys, full_alias_map, dialect
                        )
                        if l_src is None:
                            real = _resolve_alias_to_table(
                                l_short, full_alias_map, dialect
                            )
                            if real != l_short:
                                l_src = {
                                    "source_table": real,
                                    "source_cols": all_l_keys,
                                    "where_sql": None,
                                }
                    if r_short in full_alias_map:
                        r_src = _resolve_cte_source(
                            r_short, all_r_keys, full_alias_map, dialect
                        )
                        if r_src is None:
                            real = _resolve_alias_to_table(
                                r_short, full_alias_map, dialect
                            )
                            if real != r_short:
                                r_src = {
                                    "source_table": real,
                                    "source_cols": all_r_keys,
                                    "where_sql": None,
                                }
                try:
                    parts.append(
                        _build_join_query(
                            pair_specs,
                            dialect,
                            idx,
                            l_source=l_src,
                            r_source=r_src,
                            cte_names=set(full_alias_map) if full_alias_map else None,
                        )
                    )
                    idx += 1
                except Exception:
                    pass

        # Derived-expression profile branches (SAFE_CAST, REGEXP_EXTRACT, COALESCE, …)
        derived_branches = _build_derived_expr_profile_branches(
            sql_query,
            used_columns,
            dialect,
            top_k=5,
            start_idx=idx,
        )
        parts.extend(derived_branches)
        idx += len(derived_branches)

    union_sql = "\n\nUNION ALL\n\n".join(parts)

    # Prepend CTE definitions so that any SQL-based join branches can reference them.
    if ctes:
        cte_defs = ",\n".join(f"{name} AS ({body})" for name, body in ctes)
        return f"WITH {cte_defs}\n\n{union_sql}"

    return union_sql


# ─── Derived-expression profiling ────────────────────────────────────────────


def _sql_table_alias_map(sql: str, dialect: str) -> dict[str, str]:
    """Return {alias: full_table_name} for all tables referenced in *sql*.

    Handles BigQuery-style ``project.dataset.table`` names.  Both the alias
    and the bare table name (last segment) are included as keys so callers
    can look up by either.

    Examples:
        >>> m = _sql_table_alias_map("SELECT * FROM proj.ds.orders AS o", "bigquery")
        >>> m.get("o")
        'proj.ds.orders'
        >>> m = _sql_table_alias_map("SELECT * FROM t", "duckdb")
        >>> m.get("t")
        't'
    """
    try:
        tree = sqlglot.parse_one(
            sql, dialect=dialect, error_level=sqlglot.ErrorLevel.WARN
        )
    except Exception:
        return {}

    result: dict[str, str] = {}
    for tbl in tree.find_all(exp.Table):
        parts: list[str] = []
        catalog = tbl.args.get("catalog")
        db = tbl.args.get("db")
        if catalog:
            parts.append(catalog.name if hasattr(catalog, "name") else str(catalog))
        if db:
            parts.append(db.name if hasattr(db, "name") else str(db))
        parts.append(tbl.name)
        full = ".".join(parts)
        if tbl.alias:
            result[tbl.alias] = full
        result[tbl.name] = full
    return result


def _build_derived_expr_profile_branches(
    sql_query: str,
    used_columns: list[dict],
    dialect: str,
    top_k: int = 5,
    start_idx: int = 0,
) -> list[str]:
    """Build UNION ALL branches profiling derived expressions found in *sql_query*.

    Delegates expression detection with CTE-lineage resolution to
    :func:`~build_query.constraint_simplifier.detect_select_derived_expressions`,
    then constructs one SQL branch per expression that computes the top-k distinct
    output values using only the joins needed to connect the expression's source
    tables.

    The row uses ``row_type='derived_expr'``, ``table_name``=comma-joined
    resolved base-table names, ``col_name``=expression SQL,
    ``top_values``=STRING_AGG of top-k non-null values.  Other stat columns
    are NULL.

    Never raises — per-expression failures are silently skipped.

    Examples:
        >>> branches = _build_derived_expr_profile_branches(
        ...     "SELECT TRY_CAST(t.v AS INT) FROM tbl t",
        ...     [{"table": "tbl", "used_columns": ["v"]}],
        ...     "duckdb",
        ... )
        >>> len(branches) == 1
        True
        >>> "derived_expr" in branches[0]
        True
    """
    from build_query.constraint_simplifier import detect_select_derived_expressions

    expressions = detect_select_derived_expressions(sql_query, dialect)
    if not expressions:
        return []

    # Build {alias: full_table_name} from the SQL for query construction.
    # This is separate from 'source_tables' (which uses resolved lineage) and is
    # needed to reconstruct the correct FROM / JOIN aliases in the inner query.
    alias_map = _sql_table_alias_map(sql_query, dialect)

    # Reverse alias map: full_table → first alias seen (for FROM clause)
    table_to_alias: dict[str, str] = {}
    for alias, full_tbl in alias_map.items():
        if full_tbl not in table_to_alias:
            table_to_alias[full_tbl] = alias

    # Build join graph: {(left_full, right_full): [condition_str, ...]} from SQL
    from collections import defaultdict as _dd

    join_conds: dict[tuple[str, str], list[str]] = _dd(list)
    join_nbrs: dict[str, set[str]] = _dd(set)
    for spec in _collect_join_specs(sql_query, dialect):
        lt = spec.get("left_table", "")
        rt = spec.get("right_table", "")
        le = spec.get("left_expr_sql", "")
        re_ = spec.get("right_expr_sql", "")
        if lt and rt and le and re_:
            join_conds[(lt, rt)].append(f"{le} = {re_}")
            join_conds[(rt, lt)].append(f"{re_} = {le}")
            join_nbrs[lt].add(rt)
            join_nbrs[rt].add(lt)

    str_t = _str_type(dialect)
    parts: list[str] = []
    idx = start_idx

    for expr_info in expressions:
        try:
            branch = _build_one_derived_expr_branch(
                expr_info,
                alias_map,
                table_to_alias,
                join_conds,
                join_nbrs,
                dialect,
                str_t,
                top_k,
                idx,
            )
            if branch:
                parts.append(branch)
                idx += 1
        except Exception:
            pass

    return parts


def _build_one_derived_expr_branch(
    expr_info: dict,
    alias_map: dict[str, str],
    table_to_alias: dict[str, str],
    join_conds: dict[tuple[str, str], list[str]],
    join_nbrs: dict[str, set[str]],
    dialect: str,
    str_t: str,
    top_k: int,
    idx: int,
) -> str | None:
    """Build one UNION ALL row for a single derived expression.

    Returns a raw SQL SELECT string or None if the expression can't be profiled
    (source tables unresolvable, join path missing, …).

    ``expr_info`` has keys: ``expr_sql`` (str), ``source_tables`` (list[str] of
    resolved base-table names), ``col_refs`` (list[(alias, col_name)]).
    """
    expr_sql = expr_info["expr_sql"]
    # source_tables: base-table names resolved via lineage (for table_name column)
    source_tables: list[str] = expr_info.get("source_tables") or []
    # col_refs: original alias+col pairs from the expression (for query construction)
    col_refs: list[tuple[str, str]] = expr_info.get("col_refs") or []

    if not source_tables:
        return None

    # Determine the query-level tables to JOIN (using original aliases from the SQL)
    # These may be CTE names or base-table aliases — the WITH preamble handles CTEs.
    query_tables: set[str] = set()
    for alias, _col in col_refs:
        if alias:
            tbl = alias_map.get(alias, alias)
            query_tables.add(tbl)
    if not query_tables:
        # Fallback: use source_tables directly
        query_tables = set(source_tables)

    # Build minimal FROM + JOIN chain to connect all query_tables
    tables_list = list(query_tables)
    primary = tables_list[0]
    primary_alias = table_to_alias.get(primary, primary.split(".")[-1])
    primary_sql = _table_expr(primary).sql(dialect=dialect)
    from_clause = f"{primary_sql} AS {primary_alias}"
    join_parts: list[str] = []

    if len(tables_list) > 1:
        connected: set[str] = {primary}
        remaining = set(tables_list[1:])

        # BFS: expand from connected set, following join graph edges
        for _ in range(min(len(tables_list) * 3, 9)):
            if not remaining:
                break
            progress = False
            for src in list(connected):
                for tgt in list(join_nbrs.get(src, set())):
                    if tgt in connected:
                        continue
                    conds = (
                        join_conds.get((src, tgt)) or join_conds.get((tgt, src)) or []
                    )
                    if not conds:
                        continue
                    tgt_alias = table_to_alias.get(tgt, tgt.split(".")[-1])
                    tgt_sql = _table_expr(tgt).sql(dialect=dialect)
                    join_parts.append(
                        f"JOIN {tgt_sql} AS {tgt_alias} ON {' AND '.join(conds)}"
                    )
                    connected.add(tgt)
                    remaining.discard(tgt)
                    progress = True
            if not progress:
                break

        if remaining:
            return None  # can't connect all query-level tables

    join_clause = "\n".join(join_parts)
    join_part = f"\n{join_clause}" if join_clause else ""

    inner = (
        f"SELECT DISTINCT CAST(({expr_sql}) AS {str_t}) AS _v"
        f"\nFROM {from_clause}"
        f"{join_part}"
        f"\nWHERE ({expr_sql}) IS NOT NULL"
        f"\nLIMIT {top_k}"
    )

    if dialect == "postgres":
        top_values_scalar = (
            f"(SELECT STRING_AGG(_v, '|||') FROM ({inner}) AS _tv_{idx})"
        )
    elif dialect == "snowflake":
        top_values_scalar = f"(SELECT LISTAGG(_v, '|||') FROM ({inner}) AS _tv_{idx})"
    else:
        top_values_scalar = (
            f"(SELECT STRING_AGG(_v, '|||') FROM ({inner}) AS _tv_{idx})"
        )

    src_str = ",".join(sorted(source_tables))
    expr_lit = exp.Literal.string(expr_sql).sql(dialect=dialect)
    src_lit = exp.Literal.string(src_str).sql(dialect=dialect)

    # Encode source column lineage as "col@table,col@table,..." in left_key_sample
    src_cols_items: list[str] = []
    for alias, col_name in col_refs:
        tbl = alias_map.get(alias, alias) if alias else None
        src_cols_items.append(f"{col_name}@{tbl}" if tbl else col_name)
    src_cols_lit = exp.Literal.string(",".join(src_cols_items)).sql(dialect=dialect)

    return (
        f"SELECT 'derived_expr' AS row_type,"
        f" {src_lit} AS table_name,"
        f" {expr_lit} AS col_name,"
        f" COUNT(*) AS total_count,"
        f" SUM(CASE WHEN ({expr_sql}) IS NULL THEN 1 ELSE 0 END) AS null_count,"
        f" SUM(CASE WHEN ({expr_sql}) IS NOT NULL THEN 1 ELSE 0 END) AS non_null_count,"
        f" COUNT(DISTINCT ({expr_sql})) AS distinct_count,"
        f" SUM(CASE WHEN ({expr_sql}) IS NOT NULL THEN 1 ELSE 0 END)"
        f" - COUNT(DISTINCT ({expr_sql})) AS dup_count,"
        f" MIN(CAST(({expr_sql}) AS {str_t})) AS min_val,"
        f" MAX(CAST(({expr_sql}) AS {str_t})) AS max_val,"
        f" {top_values_scalar} AS top_values,"
        f" NULL AS left_table,"
        f" NULL AS right_table,"
        f" NULL AS left_expr,"
        f" NULL AS right_expr,"
        f" NULL AS join_type,"
        f" NULL AS left_match_rate,"
        f" NULL AS avg_right_per_left_key,"
        f" NULL AS max_right_per_left_key,"
        f" {src_cols_lit} AS left_key_sample,"
        f" NULL AS right_where_sql,"
        f" NULL AS date_regularity"
        f"\nFROM {from_clause}"
        f"{join_part}"
    )


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


def describe_join(join_info: dict) -> str:
    """Return a concise natural-language sentence describing a join's cardinality.

    Replaces raw numeric fields with an actionable sentence for the LLM prompt
    and the UI.  Uses only the last dotted component of table names.

    Examples:
        >>> describe_join({
        ...     "left_table": "project.dataset.orders",
        ...     "right_table": "project.dataset.users",
        ...     "join_type_profiled": "many-to-one",
        ...     "left_match_rate": 1.0,
        ...     "avg_right_per_left_key": 1.0,
        ...     "max_right_per_left_key": 1,
        ... })
        'Multiple orders rows can share the same users row'

        >>> describe_join({
        ...     "left_table": "trips",
        ...     "right_table": "stations",
        ...     "join_type_profiled": "one-to-many",
        ...     "left_match_rate": 0.95,
        ...     "avg_right_per_left_key": 2.3,
        ...     "max_right_per_left_key": 5,
        ... })
        'Each trips row can match up to 5 rows in stations (95.0% match rate, avg 2.3 per key)'

        >>> describe_join({
        ...     "left_table": "t",
        ...     "right_table": "s",
        ...     "join_type_profiled": "one-to-one",
        ...     "left_match_rate": 1.0,
        ...     "avg_right_per_left_key": 1.0,
        ...     "max_right_per_left_key": 1,
        ... })
        'Each t row matches exactly 1 row in s'
    """
    left = join_info.get("left_table", "").split(".")[-1]
    right = join_info.get("right_table", "").split(".")[-1]
    jt = join_info.get("join_type_profiled", "many-to-many")
    avg_r = join_info.get("avg_right_per_left_key")
    max_r = join_info.get("max_right_per_left_key")
    match_rate = join_info.get("left_match_rate")

    if jt == "one-to-one":
        desc = f"Each {left} row matches exactly 1 row in {right}"
    elif jt == "many-to-one":
        desc = f"Multiple {left} rows can share the same {right} row"
    elif jt == "one-to-many":
        if max_r and max_r > 1:
            desc = f"Each {left} row can match up to {max_r} rows in {right}"
        else:
            desc = f"Each {left} row can match multiple rows in {right}"
    else:
        desc = f"Multiple {left} rows can match multiple {right} rows"

    extras: list[str] = []
    if match_rate is not None and match_rate < 0.99:
        extras.append(f"{round(match_rate * 100, 1)}% match rate")
    if jt in ("one-to-many", "many-to-many") and avg_r and avg_r > 1.0:
        extras.append(f"avg {round(avg_r, 1)} per key")

    if extras:
        desc += f" ({', '.join(extras)})"
    right_filter = join_info.get("right_filter")
    if right_filter:
        desc += f" — right side filtered: {right_filter}"
    return desc


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

    # Separate row types: column stats, join stats, derived-expression stats
    col_rows = [r for r in rows if r.get("row_type") not in ("join", "derived_expr")]
    join_rows = [r for r in rows if r.get("row_type") == "join"]
    derived_rows = [r for r in rows if r.get("row_type") == "derived_expr"]

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

    _joins: list[dict] = []
    for r in join_rows:
        if not r.get("join_type"):
            continue
        j: dict = {
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
        if r.get("right_where_sql"):
            # WHERE filter applied to the right side (e.g. "status = 'active'")
            # Tells the generator why left_match_rate < 1.0
            j["right_filter"] = r["right_where_sql"]
        j["description"] = describe_join(j)
        _joins.append(j)

    profile: dict = {
        "tables": {},
        "joins": _joins,
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

            top_raw = row.get("top_values") or ""
            if top_raw:
                sep = "|||" if "|||" in top_raw else ","
                top_values = [v.strip() for v in top_raw.split(sep) if v.strip()]
            else:
                top_values = []

            # ── derived fields ───────────────────────────────────────────────
            nullable_ratio = round(null_cnt / total, 4) if total > 0 else 0.0
            is_unique = distinct == total and null_cnt == 0 and total > 0
            is_unique_non_null = distinct == non_null and non_null > 0
            is_categorical = 0 < distinct <= threshold

            date_regularity = row.get("date_regularity") or None

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
                "date_regularity": date_regularity,
            }

        profile["tables"][table] = {
            "row_count": row_count,
            "columns": col_profiles,
            "correlations": [],  # not available from single-query profile
        }

    profile["fk_candidates"] = detect_fk_candidates(profile, used_columns)

    # Attach derived-expression stats to each source table's profile
    for row in derived_rows:
        src_str = row.get("table_name") or ""
        expr_sql_stored = row.get("col_name") or ""
        top_raw = row.get("top_values") or ""
        if not (src_str and expr_sql_stored):
            continue
        sep = "|||" if "|||" in top_raw else ","
        top_vals = [v.strip() for v in top_raw.split(sep) if v.strip()]

        # Parse source column lineage from left_key_sample ("col@table,col@table,...")
        src_cols_raw = row.get("left_key_sample") or ""
        source_columns: list[dict] = []
        for item in src_cols_raw.split(","):
            item = item.strip()
            if not item:
                continue
            if "@" in item:
                col, tbl_ref = item.split("@", 1)
                source_columns.append({"table": tbl_ref.strip(), "column": col.strip()})
            else:
                source_columns.append({"column": item})

        derived_entry: dict = {
            "expr_sql": expr_sql_stored,
            "top_values": top_vals,
            "source_columns": source_columns,
            "total_count": _i(row.get("total_count")),
            "null_count": _i(row.get("null_count")),
            "non_null_count": _i(row.get("non_null_count")),
            "distinct_count": _i(row.get("distinct_count")),
            "dup_count": _i(row.get("dup_count")),
            "min_val": row.get("min_val"),
            "max_val": row.get("max_val"),
        }

        for tbl in src_str.split(","):
            tbl = tbl.strip()
            if not tbl:
                continue
            tbl_key = next(
                (k for k in (tbl, tbl.split(".")[-1]) if k in profile["tables"]),
                None,
            )
            if tbl_key:
                profile["tables"][tbl_key].setdefault("derived_expressions", []).append(
                    derived_entry
                )

    return profile
