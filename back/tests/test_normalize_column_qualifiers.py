from build_query.validator import _normalize_column_qualifiers


def test_aliases_table_and_rewrites_col_qualifier():
    """`dataset.table`.col → alias.col, avec un alias explicite sur la table."""
    sql = (
        "SELECT `ds.tbl`.id FROM `ds.tbl` WHERE `ds.tbl`.partition_date = '2026-01-01'"
    )
    out = _normalize_column_qualifiers(sql, "bigquery")
    # plus aucun qualificateur de colonne pointé
    assert "`ds.tbl`.id" not in out
    assert "`ds.tbl`.`id`" not in out
    # la table reçoit un alias explicite et les colonnes le référencent
    assert "AS tbl" in out
    assert "tbl.id" in out or "`tbl`.`id`" in out


def test_aliases_table_when_project_prepended():
    """Régression : table qualifiée 3 parties (project préfixé en amont) mais
    qualificateur de colonne en chemin 2 parties. BigQuery rejetait aussi bien
    `dataset.table` que le segment final `table` — il faut un alias explicite."""
    sql = (
        "SELECT `myproj.ds.tbl`.id "
        "FROM `myproj.ds.tbl` "
        "WHERE `ds.tbl`.partition_date = '2026-01-01'"
    )
    out = _normalize_column_qualifiers(sql, "bigquery")
    assert "`ds.tbl`.id" not in out
    assert "`myproj.ds.tbl`.id" not in out
    # alias explicite ajouté ; le qualificateur de colonne pointe dessus
    assert "AS tbl" in out
    assert "tbl.id" in out or "`tbl`.`id`" in out


def test_case_insensitive_table_vs_col_qualifier():
    """Régression : table en casse d'origine (MAJUSCULE) et qualificateur de colonne
    en casse normalisée (minuscule) — le rapprochement doit être insensible à la casse,
    sinon le qualificateur `dataset.table` n'est pas réécrit en alias."""
    sql = "SELECT `DS.TBL`.id FROM `DS.TBL` WHERE ds.tbl.partition_date = '2026-01-01'"
    out = _normalize_column_qualifiers(sql, "bigquery")
    assert "ds.tbl.partition_date" not in out.lower()
    assert "AS tbl" in out or "AS TBL" in out


def test_no_change_when_no_dotted_qualifier():
    """Aucune réécriture si les colonnes ne portent pas de qualificateur pointé."""
    sql = "SELECT t.id FROM `ds.tbl` AS t WHERE t.x = 1"
    out = _normalize_column_qualifiers(sql, "bigquery")
    assert "t.id" in out or "`t`.`id`" in out
    # pas d'alias en double ajouté
    assert out.count(" AS ") == 1
