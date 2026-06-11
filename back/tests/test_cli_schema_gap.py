"""Régression : détection des colonnes référencées dans le SQL mais absentes du
schéma en cache (schéma périmé). Sans cette garde, la colonne est droppée
silencieusement → table de test sans la colonne → DuckDB "column not found".
"""

import json

from cli.generate import find_used_columns_missing_from_schema


def _uc(database, table, cols):
    return json.dumps(
        {"project": "", "database": database, "table": table, "used_columns": cols}
    )


def _schema(table_name, cols):
    return {"table_name": table_name, "columns": [{"name": c} for c in cols]}


def test_flags_column_absent_from_cached_schema():
    used = [_uc("MARKETING_Referentiels", "banques", ["code_banque", "libelle_banque"])]
    schemas = [
        _schema("p.MARKETING_Referentiels.banques", ["code_banque", "reseau"]),
    ]
    problems = find_used_columns_missing_from_schema(used, schemas)
    assert problems == [("p.MARKETING_Referentiels.banques", ["libelle_banque"])]


def test_no_problem_when_schema_covers_all():
    used = [_uc("d", "t", ["a", "b"])]
    schemas = [_schema("p.d.t", ["a", "b", "c"])]
    assert find_used_columns_missing_from_schema(used, schemas) == []


def test_case_insensitive_match():
    used = [_uc("D", "T", ["COL_A"])]
    schemas = [_schema("p.d.t", ["col_a"])]
    assert find_used_columns_missing_from_schema(used, schemas) == []


def test_ignores_struct_subfields():
    # Les sous-champs (avec un '.') sont portés par la colonne racine, pas
    # vérifiés individuellement contre le schéma plat.
    used = [_uc("d", "t", ["root", "root.child"])]
    schemas = [_schema("p.d.t", ["root"])]
    assert find_used_columns_missing_from_schema(used, schemas) == []


def test_table_absent_from_schema_is_skipped():
    # Une table sans entrée de schéma (CTE mal classée, etc.) n'est pas flaggée.
    used = [_uc("d", "cte_like", ["x"])]
    schemas = [_schema("p.d.other", ["x"])]
    assert find_used_columns_missing_from_schema(used, schemas) == []


def test_accepts_dict_entries_not_only_json_strings():
    used = [{"database": "d", "table": "t", "used_columns": ["missing"]}]
    schemas = [_schema("p.d.t", ["present"])]
    problems = find_used_columns_missing_from_schema(used, schemas)
    assert problems == [("p.d.t", ["missing"])]
