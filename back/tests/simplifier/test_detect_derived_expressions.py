"""
Unit tests for detect_select_derived_expressions.

All tests are pure-Python, no DB, no LLM.
"""

from build_query.constraint_simplifier import detect_select_derived_expressions


# ─── Helpers ──────────────────────────────────────────────────────────────────


def expr_sqls(results: list[dict]) -> list[str]:
    return [r["expr_sql"].upper() for r in results]


def find_expr(results: list[dict], fragment: str) -> dict | None:
    fragment = fragment.upper()
    for r in results:
        if fragment in r["expr_sql"].upper():
            return r
    return None


# ─── Basic detection ──────────────────────────────────────────────────────────


class TestBasicDetection:
    def test_coalesce_detected(self):
        sql = "SELECT COALESCE(t.a, t.b) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert any("COALESCE" in e.upper() for e in expr_sqls(results))

    def test_safe_cast_detected(self):
        sql = "SELECT SAFE_CAST(t.v AS INT64) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert any("SAFE_CAST" in e or "TRY_CAST" in e for e in expr_sqls(results))

    def test_regexp_extract_detected(self):
        sql = "SELECT REGEXP_EXTRACT(t.s, r'[0-9]+') FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert len(results) >= 1

    def test_date_parse_detected(self):
        sql = "SELECT PARSE_DATE('%Y-%m-%d', t.dt) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert len(results) >= 1

    def test_if_detected(self):
        sql = "SELECT IF(t.x > 0, t.a, t.b) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert len(results) >= 1

    def test_date_diff_detected(self):
        sql = "SELECT DATE_DIFF(t.end_date, t.start_date, DAY) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert len(results) >= 1


# ─── Trivial functions excluded ───────────────────────────────────────────────


class TestTrivialFunctionsExcluded:
    def test_upper_excluded(self):
        sql = "SELECT UPPER(t.name) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert not any("UPPER" in e for e in expr_sqls(results))

    def test_lower_excluded(self):
        sql = "SELECT LOWER(t.name) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert not any("LOWER" in e for e in expr_sqls(results))

    def test_trim_excluded(self):
        sql = "SELECT TRIM(t.name) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert not any("TRIM" in e for e in expr_sqls(results))

    def test_round_excluded(self):
        sql = "SELECT ROUND(t.amount, 2) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert not any("ROUND" in e for e in expr_sqls(results))

    def test_concat_excluded(self):
        sql = "SELECT CONCAT(t.first_name, ' ', t.last_name) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert not any("CONCAT" in e for e in expr_sqls(results))

    def test_plain_column_not_detected(self):
        sql = "SELECT t.a, t.b FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert results == []


# ─── Source table resolution ──────────────────────────────────────────────────


class TestSourceTableResolution:
    def test_alias_stripped_to_real_table(self):
        sql = "SELECT COALESCE(t.x, t.y) FROM my_table t"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "my_table" in r["source_tables"]

    def test_no_alias_uses_table_name(self):
        sql = "SELECT COALESCE(orders.amount, orders.fallback) FROM orders"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "orders" in r["source_tables"]

    def test_multi_table_source(self):
        sql = "SELECT COALESCE(a.x, b.y) FROM ta a JOIN tb b ON a.id = b.id"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "ta" in r["source_tables"]
        assert "tb" in r["source_tables"]

    def test_mixed_case_dataset_preserved_qualified(self):
        # BigQuery dataset names are case-sensitive: the resolved source table must
        # keep the original case, not be lowercased (else the profiling FROM 404s).
        sql = (
            "SELECT COALESCE(t.x, 'Global') "
            "FROM `MyProject.MARKETING_Reporting.daily_sales` t"
        )
        results = detect_select_derived_expressions(sql, "bigquery")
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "MyProject.MARKETING_Reporting.daily_sales" in r["source_tables"]

    def test_mixed_case_dataset_preserved_unqualified(self):
        # Unqualified column, single base table → inferred source must keep case.
        sql = (
            "SELECT COALESCE(libelle, 'Global') "
            "FROM `MARKETING_Reporting.datamart_commercants`"
        )
        results = detect_select_derived_expressions(sql, "bigquery")
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "MARKETING_Reporting.datamart_commercants" in r["source_tables"]


# ─── CTE lineage resolution ───────────────────────────────────────────────────


class TestCteLineageResolution:
    def test_cte_column_resolves_to_base_table(self):
        # Without re-aliasing the CTE, the lineage resolver traces back to the base table.
        sql = """
        WITH cte AS (SELECT id, val FROM base_tbl)
        SELECT COALESCE(cte.val, 0) FROM cte
        """
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "base_tbl" in r["source_tables"]

    def test_chained_cte_resolves_to_root(self):
        sql = """
        WITH
          c1 AS (SELECT id, v FROM source_table),
          c2 AS (SELECT id, v FROM c1)
        SELECT COALESCE(c2.v, 0) FROM c2
        """
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert "source_table" in r["source_tables"]


# ─── Column refs ──────────────────────────────────────────────────────────────


class TestColRefs:
    def test_col_refs_present(self):
        sql = "SELECT COALESCE(t.x, t.y) FROM tbl t"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        col_names = [name for _, name in r["col_refs"]]
        assert "x" in col_names
        assert "y" in col_names

    def test_col_refs_empty_for_constant(self):
        # COALESCE with only a constant — no column refs → should not appear
        sql = "SELECT COALESCE(NULL, 'default') FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert results == []


# ─── Constants skipped ────────────────────────────────────────────────────────


class TestConstantsSkipped:
    def test_function_on_literals_only_skipped(self):
        sql = "SELECT IF(1 = 1, 'yes', 'no') FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert results == []


# ─── Deduplication ────────────────────────────────────────────────────────────


class TestDeduplication:
    def test_same_expression_twice_deduplicated(self):
        sql = """
        SELECT COALESCE(t.x, t.y), COALESCE(t.x, t.y)
        FROM tbl t
        """
        results = detect_select_derived_expressions(sql)
        coalesce_hits = [r for r in results if "COALESCE" in r["expr_sql"].upper()]
        assert len(coalesce_hits) == 1


# ─── Cap at 10 ────────────────────────────────────────────────────────────────


class TestCapAt10:
    def test_more_than_10_capped(self):
        # Build a SELECT with 12 distinct COALESCE expressions
        cols = ", ".join(f"COALESCE(t.col{i}, t.col{i + 1})" for i in range(12))
        sql = f"SELECT {cols} FROM tbl t"
        results = detect_select_derived_expressions(sql)
        assert len(results) <= 10


# ─── Non-projection clauses (WHERE / QUALIFY / GROUP BY / HAVING) ─────────────


class TestNonProjectionClauses:
    def test_regexp_in_where_detected(self):
        sql = "SELECT t.a FROM tbl t WHERE REGEXP_CONTAINS(t.s, r'[0-9]+')"
        results = detect_select_derived_expressions(sql)
        assert len(results) >= 1

    def test_coalesce_in_where_detected(self):
        sql = "SELECT t.a FROM tbl t WHERE COALESCE(t.x, 0) > 5"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None

    def test_qualify_regexp_detected(self):
        sql = """
        SELECT t.a, ROW_NUMBER() OVER (PARTITION BY t.id ORDER BY t.ts DESC) AS rn
        FROM tbl t
        QUALIFY REGEXP_CONTAINS(t.s, r'pattern')
        """
        results = detect_select_derived_expressions(sql)
        assert any("REGEXP" in e for e in expr_sqls(results))

    def test_group_by_date_trunc_detected(self):
        sql = "SELECT DATE_TRUNC(t.created_at, MONTH), COUNT(*) FROM tbl t GROUP BY DATE_TRUNC(t.created_at, MONTH)"
        results = detect_select_derived_expressions(sql)
        assert any("DATE_TRUNC" in e or "DATETRUNC" in e for e in expr_sqls(results))

    def test_trivial_in_where_excluded(self):
        sql = "SELECT t.a FROM tbl t WHERE UPPER(t.name) = 'FOO'"
        results = detect_select_derived_expressions(sql)
        assert not any("UPPER" in e for e in expr_sqls(results))

    def test_same_func_in_select_and_where_deduplicated(self):
        sql = "SELECT COALESCE(t.x, 0) FROM tbl t WHERE COALESCE(t.x, 0) > 5"
        results = detect_select_derived_expressions(sql)
        hits = [r for r in results if "COALESCE" in r["expr_sql"].upper()]
        assert len(hits) == 1


# ─── Unqualified columns (no table prefix) ────────────────────────────────────


class TestUnqualifiedColumns:
    def test_unqualified_single_table_resolves(self):
        # CoC_Number has no table qualifier — must resolve to the only FROM table.
        # sqlglot parses SUBSTR as exp.Substring (named type, not Anonymous), so it
        # is NOT caught by _TRIVIAL_FUNC_NAMES and IS detected as a derived expression.
        sql = "SELECT SUBSTR(CoC_Number, 0, 2) FROM my_table"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "SUBSTR")
        assert r is not None
        assert r["source_tables"] == ["my_table"]

    def test_unqualified_single_table_nontrivial(self):
        sql = "SELECT REGEXP_EXTRACT(CoC_Number, r'[0-9]+') FROM my_table"
        results = detect_select_derived_expressions(sql)
        assert len(results) == 1
        assert results[0]["source_tables"] == ["my_table"]

    def test_unqualified_coalesce_single_table(self):
        sql = "SELECT COALESCE(amount, fallback_amount) FROM orders"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert r["source_tables"] == ["orders"]

    def test_unqualified_with_alias_resolves_to_real_table(self):
        # Even when the FROM table has an alias, unqualified col → real table name
        sql = "SELECT COALESCE(amount, 0) FROM orders o"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert r["source_tables"] == ["orders"]

    def test_unqualified_with_ctes_resolves_to_base_table(self):
        # CTEs are virtual — only hud_pit_by_coc is a real base table.
        # CoC_Number unqualified → must resolve to hud_pit_by_coc, not stay unknown.
        sql = """
        WITH
          homeless_2018 AS (SELECT CoC_Number, total FROM hud_pit_by_coc WHERE year = 2018),
          homeless_2012 AS (SELECT CoC_Number, total FROM hud_pit_by_coc WHERE year = 2012)
        SELECT REGEXP_EXTRACT(CoC_Number, r'[A-Z]+') FROM hud_pit_by_coc
        """
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "REGEXP")
        assert r is not None
        assert r["source_tables"] == ["hud_pit_by_coc"]


# ─── UNNEST exclusion ─────────────────────────────────────────────────────────


class TestUnnestExcluded:
    def test_unnest_in_from_clause_excluded(self):
        # UNNEST in a FROM clause (BigQuery array expansion) must not be picked up
        # as a derived expression — it is a table-valued function, not a scalar one,
        # and using it as a column expression produces invalid SQL.
        sql = """
        SELECT v2ProductName, SUM(productQuantity) AS qty
        FROM `project.dataset.ga_sessions`,
        UNNEST(hits) AS hits,
        UNNEST(hits.product) AS product
        GROUP BY v2ProductName
        """
        results = detect_select_derived_expressions(sql, dialect="bigquery")
        assert not any("UNNEST" in e for e in expr_sqls(results))

    def test_unnest_in_in_subquery_excluded(self):
        # UNNEST inside an IN subquery (BigQuery): _scan_clause on the outer WHERE
        # recursively descends into the subquery's FROM clause and would previously
        # find UNNEST(hits.product) AS a non-trivial exp.Func.
        sql = """
        WITH cte AS (
            SELECT DISTINCT v2ProductName, SUM(productQuantity) AS qty
            FROM `project.dataset.ga_sessions`,
            UNNEST(hits) AS hits,
            UNNEST(hits.product) AS product
            WHERE fullVisitorID IN (
                SELECT DISTINCT fullVisitorId
                FROM `project.dataset.ga_sessions`,
                UNNEST(hits) AS hits,
                UNNEST(hits.product) AS product
                WHERE REGEXP_CONTAINS(LOWER(v2ProductName), 'youtube')
            )
            GROUP BY v2ProductName
        )
        SELECT v2ProductName FROM cte ORDER BY qty DESC LIMIT 1
        """
        results = detect_select_derived_expressions(sql, dialect="bigquery")
        assert not any("UNNEST" in e for e in expr_sqls(results))

    def test_unqualified_multi_table_ambiguous(self):
        # With a JOIN and no table qualifier, we can't determine the source → empty
        sql = (
            "SELECT COALESCE(amount, 0) FROM orders o JOIN items i ON o.id = i.order_id"
        )
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        assert r["source_tables"] == []

    def test_mixed_qualified_and_unqualified(self):
        # a.col1 is qualified (→ ta), col2 is not (→ ambiguous with JOIN → ignored)
        sql = "SELECT COALESCE(a.col1, col2) FROM ta a JOIN tb b ON a.id = b.id"
        results = detect_select_derived_expressions(sql)
        r = find_expr(results, "COALESCE")
        assert r is not None
        # col2 unqualified with multiple tables → not added; a.col1 → "ta"
        assert "ta" in r["source_tables"]
        assert "tb" not in r["source_tables"]


# ─── Invalid SQL ──────────────────────────────────────────────────────────────


class TestInvalidSql:
    def test_unparseable_returns_empty(self):
        results = detect_select_derived_expressions("NOT VALID SQL !!!!")
        assert results == []

    def test_empty_string_returns_empty(self):
        results = detect_select_derived_expressions("")
        assert results == []


# ─── Multiple non-trivial functions in one query ──────────────────────────────


class TestMultipleFunctions:
    def test_coalesce_and_date_diff_both_detected(self):
        sql = """
        SELECT
            COALESCE(t.a, t.b),
            DATE_DIFF(t.end_date, t.start_date, DAY)
        FROM tbl t
        """
        results = detect_select_derived_expressions(sql)
        sqls = expr_sqls(results)
        assert any("COALESCE" in s for s in sqls)
        assert any("DATE_DIFF" in s or "DATEDIFF" in s for s in sqls)

    def test_trivial_mixed_with_nontrivial(self):
        sql = """
        SELECT
            UPPER(t.name),
            COALESCE(t.a, t.b)
        FROM tbl t
        """
        results = detect_select_derived_expressions(sql)
        sqls = expr_sqls(results)
        assert any("COALESCE" in s for s in sqls)
        assert not any("UPPER" in s for s in sqls)
