import sqlglot

from app.api.endpoints.query import _qualify_two_part_refs


def _tables(sql: str):
    return list(sqlglot.parse_one(sql, dialect="bigquery").find_all(sqlglot.exp.Table))


def test_dashed_project_is_backtick_quoted():
    """Régression : un project id à tiret (ex. `pipetalk-493612`) doit être quoté.

    Sans backticks, BigQuery lit `pipetalk-493612` comme une soustraction et le
    dry-run échoue. On vérifie que le segment projet préfixé est bien entre
    backticks et que la réécriture reste sémantiquement stable (round-trip)."""
    sql = "SELECT * FROM Marketing_Referentiel.banques"
    out = _qualify_two_part_refs(sql, _tables(sql), "pipetalk-493612", "bigquery")
    assert "`pipetalk-493612`" in out
    assert "pipetalk-493612.Marketing_Referentiel" not in out

    # round-trip : le projet est bien reconnu comme catalog, pas comme soustraction
    tbl = sqlglot.parse_one(out, dialect="bigquery").find(sqlglot.exp.Table)
    assert tbl.catalog == "pipetalk-493612"
    assert tbl.db == "Marketing_Referentiel"
    assert tbl.name == "banques"


def test_already_qualified_ref_is_left_untouched():
    """Une table déjà en 3 parties (avec catalog) ne doit pas être re-préfixée."""
    sql = "SELECT * FROM other_proj.ds.tbl"
    out = _qualify_two_part_refs(sql, _tables(sql), "pipetalk-493612", "bigquery")
    assert "pipetalk-493612" not in out
