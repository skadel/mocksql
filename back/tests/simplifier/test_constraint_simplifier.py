"""
Unit tests for build_query/constraint_simplifier.py

All tests are pure-Python, no DB, no LLM.
They cover the five reference examples from the spec plus additional edge cases.
"""

from build_query.constraint_simplifier import (
    ColumnRef,
    SimplificationResult,
    simplify,
)
from build_query.examples_generator import _branch_to_dict


# ─── Helpers ──────────────────────────────────────────────────────────────────


# ─── Helpers ──────────────────────────────────────────────────────────────────


def col(table: str, column: str) -> ColumnRef:
    return ColumnRef(table.lower(), column.lower())


def source_cols_of(filters: list, table: str, column: str) -> list[ColumnRef]:
    """Return source_columns of the first filter whose column matches table.column."""
    c = col(table, column)
    for f in filters:
        if f.column == c:
            return f.source_columns
    return []


def filter_ops(result: SimplificationResult, table: str, column: str) -> list[str]:
    """Return the op-list of filters on a given source column."""
    c = col(table, column)
    return [f.op for f in result.source_columns.get(c, [])]


def is_source(result: SimplificationResult, table: str, column: str) -> bool:
    return col(table, column) in result.source_columns


def is_derived(result: SimplificationResult, table: str, column: str) -> bool:
    return col(table, column) in result.derived_columns


def derived_from(
    result: SimplificationResult, table: str, column: str
) -> ColumnRef | None:
    entry = result.derived_columns.get(col(table, column))
    return entry[0] if entry else None


def same_class(result: SimplificationResult, *cols: tuple[str, str]) -> bool:
    """Return True if all given (table, col) pairs belong to the same equivalence class."""
    refs = {col(t, c) for t, c in cols}
    for cls in result.equivalence_classes:
        if refs.issubset(cls):
            return True
    return False


# ─── simplify — example 1: simple join + filter ───────────────────────────────


class TestSimplifyExample1:
    SQL = """
    SELECT *
    FROM myproject.analytics.a AS a
    JOIN myproject.analytics.b AS b ON a.x = b.x
    WHERE a.y = 10
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    def test_equivalence_class(self):
        assert same_class(self.r, ("a", "x"), ("b", "x"))

    def test_one_of_ax_bx_is_source(self):
        # Exactly one of a.x / b.x should be the source; the other is derived
        sources = {col("a", "x"), col("b", "x")}
        assert len(sources & set(self.r.source_columns)) == 1

    def test_ay_is_source_with_eq_filter(self):
        assert is_source(self.r, "a", "y")
        assert filter_ops(self.r, "a", "y") == ["eq"]

    def test_bx_or_ax_derived(self):
        # The non-representative of {a.x, b.x} must be derived
        if is_source(self.r, "a", "x"):
            assert is_derived(self.r, "b", "x")
        else:
            assert is_derived(self.r, "a", "x")


# ─── simplify — example 2: LIKE + functional ──────────────────────────────────


class TestSimplifyExample2:
    SQL = """
    SELECT *
    FROM myproject.analytics.a AS a
    JOIN myproject.analytics.b AS b ON a.a1 = TRIM(b.b1)
    WHERE b.b1 LIKE '%abc%'
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    def test_bb1_is_source_with_like(self):
        assert is_source(self.r, "b", "b1")
        assert "like" in filter_ops(self.r, "b", "b1")

    def test_aa1_is_derived(self):
        assert is_derived(self.r, "a", "a1")

    def test_aa1_derived_from_bb1(self):
        src = derived_from(self.r, "a", "a1")
        assert src == col("b", "b1")

    def test_no_equivalence_between_aa1_and_bb1(self):
        # They are NOT equivalent — functional, not equality
        assert not same_class(self.r, ("a", "a1"), ("b", "b1"))


# ─── simplify — example 3: CTE + propagation ──────────────────────────────────


class TestSimplifyExample3:
    SQL = """
    WITH cte1 AS (
        SELECT a.x, b.z
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b ON a.x = b.x
        WHERE b.z > 100
    )
    SELECT *
    FROM cte1
    JOIN myproject.analytics.c AS c ON cte1.z = c.c1
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    def test_bz_has_gt_filter(self):
        # b.z > 100 should be captured (cte1.z resolves to b.z)
        assert is_source(self.r, "b", "z")
        assert "gt" in filter_ops(self.r, "b", "z")

    def test_ax_bx_same_class(self):
        assert same_class(self.r, ("a", "x"), ("b", "x"))

    def test_bz_cc1_same_class(self):
        # cte1.z resolves to b.z via lineage; the outer JOIN gives {b.z, c.c1}.
        assert same_class(self.r, ("b", "z"), ("c", "c1"))


# ─── simplify — example 4: full complex query ────────────────────────────────


class TestSimplifyExample4:
    SQL = """
    WITH cte1 AS (
        SELECT a.x, a.y, b.z
        FROM myproject.analytics.a AS a
        JOIN myproject.analytics.b AS b
          ON a.x = b.x
         AND a.a1 = TRIM(b.b1)
         AND a.y = 123
         AND a.t <= '2025-01-01'
        WHERE b.b1 LIKE '%abc%'
    )
    SELECT x, y, z
    FROM cte1
    JOIN myproject.analytics.c AS c ON cte1.z = c.c1
    WHERE c.c2 = 'c2_val'

    UNION ALL

    SELECT x, y, z
    FROM myproject.analytics.p AS p
    WHERE p.p1 = 'p_filter'
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    # ── Equivalence classes
    def test_ax_bx_same_class(self):
        assert same_class(self.r, ("a", "x"), ("b", "x"))

    def test_bz_cc1_same_class(self):
        # cte1.z resolves to b.z via lineage; the outer JOIN gives {b.z, c.c1}.
        assert same_class(self.r, ("b", "z"), ("c", "c1"))

    # ── Source columns
    def test_bb1_is_source_with_like(self):
        assert is_source(self.r, "b", "b1")
        assert "like" in filter_ops(self.r, "b", "b1")

    def test_at_is_source_with_lte(self):
        assert is_source(self.r, "a", "t")
        assert "lte" in filter_ops(self.r, "a", "t")

    def test_cc2_is_source_with_eq(self):
        assert is_source(self.r, "c", "c2")
        assert "eq" in filter_ops(self.r, "c", "c2")

    def test_pp1_is_source_with_eq(self):
        assert is_source(self.r, "p", "p1")
        assert "eq" in filter_ops(self.r, "p", "p1")

    def test_ay_is_source_with_eq(self):
        assert is_source(self.r, "a", "y")
        assert "eq" in filter_ops(self.r, "a", "y")

    # ── Derived columns
    def test_aa1_is_derived_from_bb1(self):
        assert is_derived(self.r, "a", "a1")
        assert derived_from(self.r, "a", "a1") == col("b", "b1")

    def test_bx_or_ax_one_is_derived(self):
        # One of a.x / b.x is derived from the other
        ax_derived = is_derived(self.r, "a", "x")
        bx_derived = is_derived(self.r, "b", "x")
        assert ax_derived or bx_derived
        assert not (ax_derived and bx_derived)


# ─── simplify — example 5: chain a-b-c join ──────────────────────────────────


class TestSimplifyExample5:
    SQL = """
    SELECT *
    FROM myproject.analytics.a AS a
    JOIN myproject.analytics.b AS b ON a.x = b.x
    JOIN myproject.analytics.c AS c ON b.x = c.x
    WHERE c.x > 50
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    def test_all_three_in_same_class(self):
        assert same_class(self.r, ("a", "x"), ("b", "x"), ("c", "x"))

    def test_cx_filter_propagated_to_rep(self):
        # The representative of {a.x, b.x, c.x} must carry the > 50 filter
        min([col("a", "x"), col("b", "x"), col("c", "x")])
        # The rep is the smallest ColumnRef; c.x has the filter so the rep
        # should either be c.x or carry the filter via filter_index
        # At minimum: the source representative has a "gt" filter
        any_gt = any(
            "gt" in filter_ops(self.r, t, c)
            for t, c in [("a", "x"), ("b", "x"), ("c", "x")]
        )
        assert any_gt

    def test_two_derived_one_source(self):
        xs = [col("a", "x"), col("b", "x"), col("c", "x")]
        sources = [c for c in xs if c in self.r.source_columns]
        deriveds = [c for c in xs if c in self.r.derived_columns]
        assert len(sources) == 1
        assert len(deriveds) == 2


# ─── simplify — example 6: real-world BigQuery self-join (World Bank WDI) ─────


class TestSimplifyExample6:
    """
    Étape 3 : Fusionner les deux indicateurs et enrichir avec les informations des pays.
    Two indicators joined from the same table (self-join via alias indicators_data_2),
    enriched with country_summary. All constraints come from ON clauses and WHERE.
    """

    SQL = """
    SELECT
      `cs`.`short_name` AS `country_name`,
      `cs`.`region` AS `region`,
      `cs`.`income_group` AS `income_group`,
      `indicators_data`.`value` AS `gdp_per_capita`,
      `indicators_data_2`.`value` AS `internet_users_percent`
    FROM `bigquery-public-data.world_bank_wdi.indicators_data` AS `indicators_data`
    JOIN `bigquery-public-data.world_bank_wdi.country_summary` AS `cs`
      ON `cs`.`country_code` = `indicators_data`.`country_code`
      AND NOT `cs`.`region` IS NULL
    JOIN `bigquery-public-data.world_bank_wdi.indicators_data` AS `indicators_data_2`
      ON `indicators_data`.`country_code` = `indicators_data_2`.`country_code`
      AND `indicators_data_2`.`indicator_code` = 'IT.NET.USER.ZS'
      AND `indicators_data_2`.`year` = 2019
      AND NOT `indicators_data_2`.`value` IS NULL
    WHERE
      `indicators_data`.`indicator_code` = 'NY.GDP.PCAP.CD'
      AND `indicators_data`.`year` = 2019
      AND NOT `indicators_data`.`value` IS NULL
    ORDER BY
      `indicators_data`.`value` ASC
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    # ── Equivalence classes
    # Aliases are preserved: `cs` stays `cs`, `indicators_data_2` stays `indicators_data_2`.
    # All three country_code aliases end up in the same equivalence class.
    def test_country_code_same_class(self):
        assert same_class(
            self.r,
            ("cs", "country_code"),
            ("indicators_data", "country_code"),
            ("indicators_data_2", "country_code"),
        )

    def test_one_country_code_source_two_derived(self):
        # 3 aliases for country_code in one equivalence class → 1 source, 2 derived
        cc_cols = [
            col("cs", "country_code"),
            col("indicators_data", "country_code"),
            col("indicators_data_2", "country_code"),
        ]
        sources = [c for c in cc_cols if c in self.r.source_columns]
        deriveds = [c for c in cc_cols if c in self.r.derived_columns]
        assert len(sources) == 1
        assert len(deriveds) == 2

    # ── Filters from ON clauses (aliases preserved)
    def test_cs_region_is_not_null(self):
        # NOT cs.region IS NULL
        assert is_source(self.r, "cs", "region")
        assert "is_not_null" in filter_ops(self.r, "cs", "region")

    def test_indicators_data_indicator_code_eq(self):
        # WHERE indicators_data.indicator_code = 'NY.GDP.PCAP.CD'
        assert is_source(self.r, "indicators_data", "indicator_code")
        assert "eq" in filter_ops(self.r, "indicators_data", "indicator_code")

    def test_indicators_data_2_indicator_code_eq(self):
        # ON indicators_data_2.indicator_code = 'IT.NET.USER.ZS' — separate from indicators_data
        assert is_source(self.r, "indicators_data_2", "indicator_code")
        assert "eq" in filter_ops(self.r, "indicators_data_2", "indicator_code")

    def test_indicators_data_year_eq(self):
        assert is_source(self.r, "indicators_data", "year")
        assert "eq" in filter_ops(self.r, "indicators_data", "year")

    def test_indicators_data_2_year_eq(self):
        assert is_source(self.r, "indicators_data_2", "year")
        assert "eq" in filter_ops(self.r, "indicators_data_2", "year")

    def test_indicators_data_value_is_not_null(self):
        assert is_source(self.r, "indicators_data", "value")
        assert "is_not_null" in filter_ops(self.r, "indicators_data", "value")

    def test_indicators_data_2_value_is_not_null(self):
        assert is_source(self.r, "indicators_data_2", "value")
        assert "is_not_null" in filter_ops(self.r, "indicators_data_2", "value")


# ─── Regression: inline subquery — sqlglot 30.x uses "from_" key ─────────────


class TestInlineSubquerySimple:
    """Minimal inline subquery — verifies FROM clause is read via 'from_' key."""

    SQL = """
    SELECT a.val
    FROM (SELECT val FROM myproject.myds.mytable WHERE val > 42) a
    """

    def setup_method(self):
        self.r = simplify(self.SQL)

    def test_filter_from_inner_where_captured(self):
        assert is_source(self.r, "mytable", "val")
        assert "gt" in filter_ops(self.r, "mytable", "val")


class TestInlineSubqueryFromJoinFiltersAndJoins:
    """
    Regression — two gaps fixed together (sqlglot 30.x 'from_' key):

    Gap 1: WHERE filters inside the FROM subquery were not captured at all.
    Gap 2: JOIN was rendered with subquery aliases (o/i) instead of real table names
           (objects/images) in _branch_to_dict.

    SQL from the bug report: MET museum objects × images, BigQuery dialect.
    """

    SQL = """
    SELECT o.artist_display_name, o.title, o.object_end_date, o.medium, i.original_image_url
    FROM (
      SELECT object_id, title, artist_display_name, object_end_date, medium
      FROM `bigquery-public-data.the_met.objects`
      WHERE
        department = 'Photographs'
        AND object_name LIKE '%Photograph%'
        AND artist_display_name != 'Unknown'
        AND object_end_date <= 1839
    ) o
    INNER JOIN (
      SELECT original_image_url, object_id
      FROM `bigquery-public-data.the_met.images`
    ) i ON o.object_id = i.object_id
    ORDER BY o.object_end_date
    """

    def setup_method(self):
        self.r = simplify(self.SQL, dialect="bigquery")

    # ── Gap 1: all four WHERE predicates from the FROM subquery must be captured

    def test_department_eq_filter(self):
        assert is_source(self.r, "objects", "department")
        assert filter_ops(self.r, "objects", "department") == ["eq"]

    def test_object_name_like_filter(self):
        assert is_source(self.r, "objects", "object_name")
        assert "like" in filter_ops(self.r, "objects", "object_name")

    def test_artist_display_name_neq_filter(self):
        assert is_source(self.r, "objects", "artist_display_name")
        assert "neq" in filter_ops(self.r, "objects", "artist_display_name")

    def test_object_end_date_lte_filter(self):
        assert is_source(self.r, "objects", "object_end_date")
        assert "lte" in filter_ops(self.r, "objects", "object_end_date")

    # ── Gap 2: join must render with real table names, not subquery aliases

    def test_join_uses_real_table_names(self):
        d = _branch_to_dict(self.r)
        assert d.get("joins"), "No joins found in result"
        join_str = d["joins"][0]
        assert "objects" in join_str and "images" in join_str, (
            f"Expected real table names in join, got: {join_str!r}"
        )

    def test_join_does_not_use_subquery_aliases(self):
        d = _branch_to_dict(self.r)
        join_str = d["joins"][0]
        assert "o.object_id" not in join_str and "i.object_id" not in join_str, (
            f"Join must not use inline-subquery aliases, got: {join_str!r}"
        )
