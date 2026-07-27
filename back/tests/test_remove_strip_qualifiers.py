from utils.examples import strip_qualifiers_with_scope


def test_remove_project_dataset_basic():
    query = "SELECT * FROM `my_project`.`my_ds`.`my_table`"
    expected = "SELECT * FROM my_table"
    assert strip_qualifiers_with_scope(query, "bigquery") == expected


def test_remove_dataset_without_project():
    query = "SELECT col FROM my_ds.my_table"
    expected = "SELECT col FROM my_table"
    assert strip_qualifiers_with_scope(query, "bigquery") == expected


def test_suffix_applied():
    query = "SELECT * FROM project1.ds1.table1"
    expected = "SELECT * FROM ds1_table1_tmp"
    assert strip_qualifiers_with_scope(query, "bigquery", suffix="tmp") == expected


def test_suffix_applied_to_unqualified_physical_table():
    query = "SELECT * FROM orders"
    expected = "SELECT * FROM orders_tmp"
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_same_name_cte_only_suffixes_physical_table_in_cte_body():
    query = "WITH orders AS (SELECT id FROM orders) SELECT * FROM orders"
    expected = "WITH orders AS (SELECT id FROM orders_tmp) SELECT * FROM orders"
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_unqualified_table_qualifier_is_preserved_as_alias():
    query = "SELECT orders.id FROM orders"
    expected = "SELECT orders.id FROM orders_tmp AS orders"
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_unpivot_on_cte_is_not_mistaken_for_physical_table():
    query = (
        "WITH orders AS (SELECT id, a, b FROM orders) "
        "SELECT * FROM orders UNPIVOT(value FOR key IN (a, b))"
    )
    expected = (
        "WITH orders AS (SELECT id, a, b FROM orders_tmp) "
        "SELECT * FROM orders UNPIVOT(value FOR key IN (a, b))"
    )
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_pivot_on_cte_is_not_mistaken_for_physical_table():
    query = (
        "WITH orders AS (SELECT category, amount FROM orders) "
        "SELECT * FROM orders PIVOT(SUM(amount) FOR category IN ('a'))"
    )
    expected = (
        "WITH orders AS (SELECT category, amount FROM orders_tmp) "
        "SELECT * FROM orders PIVOT(SUM(amount) FOR category IN ('a'))"
    )
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_chained_ctes_with_pivot_only_suffix_physical_source():
    query = (
        "WITH src AS (SELECT category, amount FROM orders), "
        "pivoted AS ("
        "SELECT * FROM src PIVOT(SUM(amount) FOR category IN ('a'))"
        ") SELECT * FROM pivoted"
    )
    expected = (
        "WITH src AS (SELECT category, amount FROM orders_tmp), "
        "pivoted AS ("
        "SELECT * FROM src PIVOT(SUM(amount) FOR category IN ('a'))"
        ") SELECT * FROM pivoted"
    )
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_chained_ctes_with_unpivot_only_suffix_physical_source():
    query = (
        "WITH src AS (SELECT id, a, b FROM orders), "
        "unpivoted AS ("
        "SELECT * FROM src UNPIVOT(value FOR key IN (a, b))"
        ") SELECT * FROM unpivoted"
    )
    expected = (
        "WITH src AS (SELECT id, a, b FROM orders_tmp), "
        "unpivoted AS ("
        "SELECT * FROM src UNPIVOT(value FOR key IN (a, b))"
        ") SELECT * FROM unpivoted"
    )
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_recursive_cte_reference_is_not_suffixed():
    query = (
        "WITH RECURSIVE nums(n) AS ("
        "SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3"
        ") SELECT * FROM nums"
    )
    expected = (
        "WITH RECURSIVE nums(n) AS ("
        "SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3"
        ") SELECT * FROM nums"
    )
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == expected


def test_table_function_is_not_suffixed():
    query = "SELECT * FROM READ_CSV('orders.csv')"
    assert strip_qualifiers_with_scope(query, "duckdb", suffix="tmp") == query


def test_multiple_tables_and_aliases():
    query = (
        "SELECT a.col, b.col FROM `proj1`.`ds1`.`tbl1` a JOIN ds2.tbl2 b ON a.id = b.id"
    )
    expected = "SELECT a.col, b.col FROM tbl1 AS a JOIN tbl2 AS b ON a.id = b.id"
    assert strip_qualifiers_with_scope(query, "bigquery") == expected


def test_subquery_and_cte():
    query = (
        "WITH cte AS (SELECT id FROM proj.ds.src_tbl) "
        "SELECT * FROM cte JOIN proj.ds.src_tbl2 USING(id)"
    )
    expected = (
        "WITH cte AS (SELECT id FROM ds_src_tbl_suff) "
        "SELECT * FROM cte JOIN ds_src_tbl2_suff USING (id)"
    )
    assert strip_qualifiers_with_scope(query, "bigquery", suffix="suff") == expected


# --- Régression : qualificateur de colonne = chemin complet de la table ---
# Avant le fix, la table était renommée en `ds_tbl_xxx` mais les colonnes
# gardaient `tbl` comme qualificateur sans alias correspondant → DuckDB error.


def test_col_qualifier_full_path_3part():
    """SELECT `project.dataset.table`.col → colonnes doivent rester résolvables."""
    query = "SELECT `proj.ds.tbl`.col1, `proj.ds.tbl`.col2 FROM `proj.ds.tbl` WHERE `proj.ds.tbl`.col3 = 1"
    result = strip_qualifiers_with_scope(query, "bigquery", suffix="s1")
    # La table est renommée et reçoit un alias pour que tbl.col reste valide
    # sqlglot génère les identifiants avec backticks en dialecte bigquery
    assert "ds_tbl_s1` AS tbl" in result or "ds_tbl_s1 AS tbl" in result
    assert "`tbl`" in result or "tbl." in result


def test_col_qualifier_full_path_2part():
    """SELECT `dataset.table`.col → même logique pour un chemin 2 parties."""
    query = "SELECT `ds.tbl`.col FROM `ds.tbl`"
    result = strip_qualifiers_with_scope(query, "bigquery", suffix="s1")
    assert "ds_tbl_s1` AS tbl" in result or "ds_tbl_s1 AS tbl" in result


def test_col_qualifier_no_alias_needed_when_no_ref():
    """SELECT * n'utilise pas de qualificateur → pas d'alias ajouté."""
    query = "SELECT * FROM `proj.ds.tbl`"
    result = strip_qualifiers_with_scope(query, "bigquery", suffix="s1")
    assert "AS tbl" not in result
    assert "ds_tbl_s1" in result


def test_col_qualifier_case_insensitive_match():
    """Régression (c3.sql) : la table garde sa casse d'origine (MAJUSCULE) mais
    `qualify_columns` écrit certains qualificateurs de colonne en minuscule. Le
    rapprochement table↔colonne doit être insensible à la casse, sinon le
    qualificateur `db.table` survit et DuckDB lève « Referenced table … not found »."""
    query = (
        "SELECT `MARKETING_RENTABILITE_PBI`.`FAITS_PRE`.id AS id "
        "FROM `MARKETING_RENTABILITE_PBI`.`FAITS_PRE` AS faits_pre "
        "WHERE marketing_rentabilite_pbi.faits_pre.partition_date = 1"
    )
    result = strip_qualifiers_with_scope(query, "bigquery", suffix="s1")
    # plus aucun qualificateur dataset.table résiduel, quelle que soit la casse
    assert "marketing_rentabilite_pbi" not in result.lower().replace(
        "marketing_rentabilite_pbi_faits_pre_s1", ""
    )
    assert "faits_pre.partition_date" in result.lower()


def test_col_qualifier_ignores_project_on_table():
    """Régression (c3.sql) : la table source porte le project (`proj.ds.tbl`) mais
    le qualificateur de colonne ne porte que `ds.tbl` (jamais le project). Le
    rapprochement doit IGNORER le catalog, sinon `ds.tbl`.col survit → DuckDB error.
    Reproduit la forme `dataset.table` dans un seul identifiant backtické."""
    query = (
        "SELECT `marketing_rentabilite_pbi.faits_pre`.`id` AS id "
        "FROM `pipetalk-493612.marketing_rentabilite_pbi.faits_pre` AS faits_pre "
        "WHERE `marketing_rentabilite_pbi.faits_pre`.`partition_date` = '2026-01-01'"
    )
    result = strip_qualifiers_with_scope(query, "bigquery", suffix="s1")
    # le qualificateur dataset.table de la colonne est réécrit en alias
    assert "`faits_pre`.`id`" in result
    assert "`faits_pre`.`partition_date`" in result
    # plus aucune référence résiduelle au dataset hors du nom de table renommé
    assert "marketing_rentabilite_pbi" not in result.lower().replace(
        "marketing_rentabilite_pbi_faits_pre_s1", ""
    )
