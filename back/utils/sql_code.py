import json

import sqlglot as sg
from sqlglot.optimizer.scope import traverse_scope

from utils.examples import strip_qualifiers_with_scope


def extract_real_table_refs(sql: str, dialect: str) -> list[sg.exp.Table]:
    """
    Extrait les références de tables réelles (physiques ou logiques) d'une requête SQL.

    Cette fonction analyse le SQL et parcourt ses différentes portées (scopes).
    Elle ignore volontairement :
    - Les Common Table Expressions (CTEs)
    - Les sous-requêtes
    - Les fonctions de table (ex: UNNEST, générateurs de tableaux, fonctions personnalisées)
    - Les artefacts syntaxiques (ex: références CTE masquées dans des nœuds PIVOT/UNPIVOT)

    Args:
        sql (str): La requête SQL à analyser.
        dialect (str): Le dialecte SQL ciblé (ex: "bigquery", "snowflake").

    Returns:
        list[sg.exp.Table]: Une liste d'objets Table représentant les dépendances réelles.
    """
    parsed = sg.parse_one(sql, dialect=dialect)
    real_tables: list[sg.exp.Table] = []

    for scope in traverse_scope(parsed):
        # 1. Extraction des CTE actives dans la portée (scope) courante.
        # Gestion de la rétrocompatibilité : selon les versions de sqlglot,
        # scope.ctes peut être un dictionnaire ou une liste.
        active_cte_names = set()
        if isinstance(scope.ctes, dict):
            active_cte_names.update(scope.ctes.keys())
        elif isinstance(scope.ctes, list):
            for item in scope.ctes:
                if isinstance(item, str):
                    active_cte_names.add(item)
                elif hasattr(item, "alias"):
                    active_cte_names.add(item.alias)

        # 2. Analyse des sources de données de la portée courante.
        for source in scope.sources.values():
            # Ne conserver que les objets de type Table
            if not isinstance(source, sg.exp.Table):
                continue

            # Ignorer les artefacts de parsing sans nom
            if not source.name:
                continue

            # Ignorer les fonctions de table (ex: UNNEST, my_function()).
            # Une table réelle possède un nœud parent 'this' de type Identifier.
            if not isinstance(source.this, sg.exp.Identifier):
                continue

            # Gestion des artefacts (PIVOT/UNPIVOT) et du masquage (shadowing) :
            # Si une table n'est pas qualifiée (aucun schéma/dataset renseigné)
            # ET que son nom correspond à une CTE active dans ce scope, il s'agit
            # d'une référence interne à la CTE. On l'ignore.
            # (Les tables réelles pleinement qualifiées comme 'dataset.table' sont conservées).
            is_unqualified = not source.db and not source.catalog
            if is_unqualified and source.name in active_cte_names:
                continue

            real_tables.append(source)

    return real_tables


def build_sql(sub_questions: list[dict] | None, final_sql: str) -> str:
    """
    Construit une requête SQL du type :

        WITH
          cte1 AS (...),
          cte2 AS (...),
          ...
        <final_sql>

    Si sub_questions est None ou vide, on retourne simplement final_sql.

    Parameters
    ----------
    sub_questions : list[dict] | None
        Chaque dict doit contenir au moins les clés
        - "sub_question_name" : nom de la CTE
        - "sub_query"         : texte SQL de la sous-requête
    final_sql : str
        Le SELECT final qui exploite les CTE précédentes.

    Returns
    -------
    str
        La requête SQL complète prête à exécuter.
    """
    # Si pas de sous-questions, on renvoie directement le SQL final
    if not sub_questions:
        return final_sql.strip()

    # Sinon, on formate chaque sous-requête comme une CTE
    ctes_parts = []
    for q in sub_questions:
        # Sécurité : s’assurer que q contient bien les deux clés attendues
        name = q.get("sub_question_name", "").strip()
        query_text = q.get("sub_query", "").strip()
        if not name or not query_text:
            # On peut soit lever une exception, soit sauter cette sous-question…
            # Ici on choisit de sauter si le format est incorrect
            continue

        ctes_parts.append(f"{name} AS (\n{query_text}\n)")

    # Si, après filtrage, il n’y a plus rien à mettre en CTE, on retourne le final_sql
    if not ctes_parts:
        return final_sql.strip()

    # On joint les CTE par virgule et on ajoute le SELECT final
    with_clause = ",\n".join(ctes_parts)
    return f"WITH\n{with_clause}\n{final_sql.strip()}"


def extract_used_columns(last_query_decomposed: list):
    """
    Extrait les colonnes utilisées pour chaque table avec le projet et la base à partir de la décomposition de la requête.
    Si plusieurs sources se réfèrent à la même table, leurs colonnes utilisées sont fusionnées (union).

    Args:
        last_query_decomposed (list): Liste de dictionnaires contenant les différentes étapes (queries)
                                      avec une clé 'sources' pour les sources de données. Chaque source
                                      doit contenir les clés 'project', 'database', 'table' et 'used_columns'.

    Returns:
        list: Liste de dictionnaires avec pour chaque table le projet, la base et la liste des colonnes utilisées.
    """
    used_columns_by_table = {}
    table_info = {}  # Pour stocker project et database par table

    # Parcourir chaque requête dans last_query_decomposed
    for query in last_query_decomposed:
        sources = query.get("sources", [])
        for source in sources:
            table = source.get("table")
            if not table:
                continue

            # Utiliser uniquement le nom de la table comme clé
            if table not in used_columns_by_table:
                used_columns_by_table[table] = set()
                # On initialise avec les valeurs rencontrées (si non vides)
                table_info[table] = {
                    "project": source.get("project", ""),
                    "database": source.get("database", ""),
                }
            # Mettre à jour l'ensemble des colonnes utilisées
            used_columns_by_table[table].update(source.get("used_columns", []))
            # Si les informations de project/database ne sont pas encore renseignées, on les met à jour
            if not table_info[table]["project"] and source.get("project"):
                table_info[table]["project"] = source.get("project")
            if not table_info[table]["database"] and source.get("database"):
                table_info[table]["database"] = source.get("database")

    # Construire la liste finale en fusionnant les colonnes et en conservant les infos projet et base
    used_columns_list = [
        {
            "project": table_info[table]["project"],
            "database": table_info[table]["database"],
            "table": table,
            "used_columns": sorted(list(used_columns_by_table[table])),
        }
        for table in used_columns_by_table
    ]

    return used_columns_list


def process_sql(sql_data, dialect, remove_db=False) -> str:
    if isinstance(sql_data, list):
        # Check if the only element in sql_data is 'final_query'
        if all(step["name"] == "final_query" for step in sql_data):
            return next(
                (step["code"] for step in sql_data if step["name"] == "final_query"), ""
            )

        # Construct the WITH clause by filtering out 'final_query'
        with_clause = "WITH " + ",\n".join(
            f"{step['name']} AS (\n{step['code']}\n)"
            for step in sql_data
            if step["name"] != "final_query"
        )

        # Add the code of 'final_query' separately
        final_query = next(
            (step["code"] for step in sql_data if step["name"] == "final_query"), ""
        )
        all_sql_code = f"{with_clause}\n{final_query}\n"
        if remove_db:
            used_columns_list = extract_used_columns(last_query_decomposed=sql_data)
            table_names = []
            for entry in used_columns_list:
                if entry["project"] != "":
                    table_names.append(f"{entry['project']}.{entry['table']}")
                else:
                    table_names.append(entry["table"])
            if len(table_names) == 0:
                raise ValueError("This script has no used columns.")
            all_sql_code = strip_qualifiers_with_scope(all_sql_code, dialect)
        # Combine the WITH clause and the final query
        return all_sql_code
    elif isinstance(sql_data, str):
        return sql_data
    else:
        raise ValueError("mocksql sql Expression should be list or str.")


def safe_process_sql(content, dialect, remove_db=False):
    try:
        data = json.loads(content)
        return process_sql(data, dialect, remove_db)
    except (json.JSONDecodeError, TypeError):
        # Si le content n'est pas un JSON valide, on renvoie simplement le contenu original
        return content
