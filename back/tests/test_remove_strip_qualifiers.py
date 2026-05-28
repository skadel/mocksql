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
