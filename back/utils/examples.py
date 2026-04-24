import re
from typing import Dict, Any, Optional, Type, List, Tuple

import duckdb
import sqlglot
from pandas import DataFrame
from pydantic import BaseModel, Field, create_model
from sqlglot import expressions as exp
from sqlglot.optimizer import traverse_scope, find_all_in_scope

from common_vars import type_mapping
from models.env_variables import PROJECT_ID as BQ_TEST_PROJECT

# Initialize DuckDB connection
DB_PATH = ":memory:"


# FOR DEBUG we can store the results in .db file
# DB_PATH = "my_database1.db"


def filter_columns(schemas, used_columns):
    filtered_tables = []
    for table in schemas:
        parts = table["table_name"].split(".")

        # Assurer qu'il y a au moins 2 parties (projet.database.table ou database.table) pour extraire correctement
        if len(parts) < 2:
            print(
                f"Warning: Skipping table '{table['table_name']}' due to unexpected format."
            )
            continue  # Saute cette table si le format n'est pas comme attendu

        # Extraire le nom de la base de données et le nom de la table de la structure 'schemas'
        db_name_from_schema = parts[-2]
        table_name_from_schema = parts[-1]

        # Construire le nom de table qualifié pour le résultat final (ex: MARKETING_Referentiels_correspondance_cartes)
        # On prend les deux dernières parties pour formater le 'table_name' final
        qualified_under_parts = parts[-2:]
        qualified_under_name = "_".join(qualified_under_parts)

        # Rechercher dans used_columns en comparant à la fois la base de données et le nom de la table
        used_table_entry = next(
            (
                item
                for item in used_columns
                if item.get("database") == db_name_from_schema
                and item.get("table") == table_name_from_schema
            ),
            None,
        )

        if used_table_entry:
            filtered_columns = [
                col
                for col in table["columns"]
                if col["name"].lower()
                in [uc.lower() for uc in used_table_entry["used_columns"]]
            ]
            filtered_tables.append(
                {"table_name": qualified_under_name, "columns": filtered_columns}
            )

    return filtered_tables


def parse_field_type(field_type_str: str) -> Type | List | Dict:
    """
    Parses a BigQuery field type string into a Python type.

    Handles ARRAY and STRUCT types recursively.
    """
    if field_type_str.startswith("ARRAY<"):
        inner_type_str = field_type_str[6:-1]
        inner_type = parse_field_type(inner_type_str)
        return List[inner_type]
    elif field_type_str.startswith("STRUCT<"):
        fields_str = field_type_str[7:-1]
        fields = {}
        for field_part in parse_struct_fields(fields_str):  # Use the helper function
            name, type_str = field_part.split(" ", 1)
            # Handle nested struct names containing spaces

            type_str = type_str.strip()
            fields[name] = parse_field_type(type_str)
        return Dict[str, Any]

    else:
        return type_mapping.get(field_type_str, str)


def parse_struct_fields(fields_str: str) -> List[str]:
    """
    Helper function to parse the fields inside a STRUCT definition.

    Handles commas within nested types correctly.
    """
    fields = []
    start = 0
    bracket_level = 0
    for i, char in enumerate(fields_str):
        if char == "<":
            bracket_level += 1
        elif char == ">":
            bracket_level -= 1
        elif char == "," and bracket_level == 0:
            fields.append(fields_str[start:i].strip())
            start = i + 1
    fields.append(fields_str[start:].strip())  # Add the last field

    # Handling cases where a field name may have been split due to the presence of a space before a type that isn't STRUCT or ARRAY
    cleaned_fields = []
    i = 0
    while i < len(fields):
        field_parts = fields[i].split(" ", 1)
        if len(field_parts) == 2 and not any(
            type_str in field_parts[1] for type_str in ("STRUCT", "ARRAY")
        ):
            field_name = field_parts[0]
            type_str = field_parts[1]
            # Check if the next field starts with a valid type, if not, it means the current field's name was split

            if i + 1 < len(fields):
                next_part = fields[i + 1].split(" ", 1)[0]
                if (
                    not any(
                        type_str in next_part
                        for type_str in (
                            "STRUCT",
                            "ARRAY",
                            "INT",
                            "INT64",
                            "STRING",
                            "FLOAT",
                            "FLOAT64",
                            "TIMESTAMP",
                            "BOOL",
                        )
                    )
                    and next_part not in type_mapping
                ):
                    field_name += " " + fields[i + 1].split(" ", 1)[0]
                    type_str = fields[i + 1].split(" ", 1)[1]
                    cleaned_fields.append(f"{field_name} {type_str}")
                    i += 2
                    continue

            cleaned_fields.append(fields[i])

        else:
            cleaned_fields.append(fields[i])
        i += 1

    return cleaned_fields


def create_pydantic_models(filtered_tables_and_columns: list) -> Type[BaseModel]:
    models = {}
    for table in filtered_tables_and_columns:
        table_name = table["table_name"]
        fields = {}

        for column in table["columns"]:
            col_name = column["name"].lower()
            col_type_str = column["type"]
            col_type = parse_field_type(col_type_str)
            col_description = column.get("description", None)

            if isinstance(col_type, type) and issubclass(col_type, dict):
                # Handle STRUCT types by creating a nested model
                struct_model_fields = {}
                for struct_field_name, struct_field_type in col_type.items():
                    struct_model_fields[struct_field_name] = (
                        Optional[struct_field_type],
                        Field(None, description=None),
                    )
                struct_model = create_model(
                    f"{table_name}_{col_name}", **struct_model_fields
                )
                fields[col_name] = (
                    Optional[struct_model],
                    Field(None, description=col_description),
                )

            elif isinstance(col_type, list):
                # Handle ARRAY of STRUCT
                if isinstance(col_type[0], dict):
                    array_struct_fields = {}
                    for struct_field_name, struct_field_type in col_type[0].items():
                        array_struct_fields[struct_field_name] = (
                            Optional[struct_field_type],
                            Field(None, description=None),
                        )
                    array_struct_model = create_model(
                        f"{table_name}_{col_name}_item", **array_struct_fields
                    )
                    fields[col_name] = (
                        Optional[List[array_struct_model]],
                        Field(None, description=col_description),
                    )
                else:
                    fields[col_name] = (
                        Optional[col_type],
                        Field(None, description=col_description),
                    )
            else:
                fields[col_name] = (
                    Optional[col_type],
                    Field(None, description=col_description),
                )

        # Create the model for the table
        model = create_model(table_name, **fields)
        models[table_name] = (list[model], Field(None, description="Model for table "))

    # Create a combined model
    CombinedModel = create_model("CombinedModel", **models)
    return CombinedModel


def create_test_tables(
    tables: list,
    suffix: str,
    con: duckdb.DuckDBPyConnection,
    dialect: str,
    used_columns: list = None,
    overwrite=True,
):
    """
    Create test tables in DuckDB based on the given table definitions.

    Parameters:
        tables (list): A list of table definitions, each containing 'table_name' and 'columns'.
        suffix (str): A suffix to append to table names.
        con (DuckDBPyConnection): The connection to the duckdb database.
        dialect (str): bigquery, postgres, ...
        used_columns (list): The columns used in the sql script (or None to utiliser toutes les colonnes).
        overwrite (bool): Whether to overwrite existing tables with the same name.

    Returns:
        list: A list of created DuckDB table definitions with column type mappings.
    """
    duckdb_typed_tables = []
    errors = []

    # Si used_columns est None, on crée un dictionnaire vide afin de ne pas filtrer les colonnes.
    if used_columns is not None:
        used_columns_dict = {uc["table"]: uc["used_columns"] for uc in used_columns}
    else:
        used_columns_dict = {}

    for table in tables:
        try:
            parts = table["table_name"].split(".")
            qualified_key = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            duckdb_base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            table_name = f"{duckdb_base}_{suffix.replace('-', '_')}"

            # Filtrage en tenant compte de la casse si des colonnes utilisées sont définies
            if qualified_key in used_columns_dict:
                filtered_columns = [
                    col
                    for col in table["columns"]
                    if col["name"].lower()
                    in [c.lower() for c in used_columns_dict[qualified_key]]
                ]
            else:
                filtered_columns = table["columns"]

            # Construction de la requête
            columns_def = ", ".join(
                [f"{col['name']} {col['type']}" for col in filtered_columns]
            )
            create_table_query = f"CREATE TABLE {table_name} ({columns_def});"
            print(f"Creating table {table_name}...")

            # Parse and convert SQL to DuckDB dialect
            create_test_table_query = sqlglot.parse_one(
                create_table_query, dialect=dialect
            ).sql(dialect="duckdb")

            # Handle unsupported types (e.g., GEOGRAPHY)
            create_test_table_query = create_test_table_query.replace(
                "GEOGRAPHY", "GEOMETRY"
            )

            # Drop the table if overwrite is enabled
            if overwrite:
                con.execute(f"DROP TABLE IF EXISTS {table_name};")

            # Execute the table creation query
            con.execute(create_test_table_query)
            print(f"Table {table_name} created successfully.")

            # Enregistre la définition de la table créée
            duckdb_typed_tables.append(
                {"table_name": table_name, "columns": filtered_columns}
            )

        except Exception as e:
            errors.append({"table_name": table["table_name"], "error": str(e)})

    # Gestion des erreurs éventuelles
    if errors:
        for err in errors:
            print(f"Erreur création table {err['table_name']}: {err['error']}")

    return duckdb_typed_tables


def execute_queries(queries: list[str], con: duckdb.DuckDBPyConnection):
    try:
        for idx, query in enumerate(queries, start=1):
            try:
                result = con.execute(query).fetchall()  # Fetch results for verification
                print(f"Result for query {idx}: {result}")
            except Exception as e:
                print(f"Error executing query {idx}: {e}")
    except Exception as e:
        print(f"Error establishing database connection: {e}")


def fix_duck_db_sql(duckdb_sql: str) -> str:
    """
    Applique des corrections et des traductions sémantiques pour les requêtes DuckDB
    transpilées par sqlglot, notamment depuis BigQuery.

    Cette fonction corrige les incompatibilités de syntaxe et de sémantique entre
    les dialectes SQL, particulièrement pour les fonctions de date, géospatiales,
    et les fonctions "safe" de BigQuery.

    Args:
        duckdb_sql (str): Requête SQL générée par sqlglot pour DuckDB

    Returns:
        str: Requête SQL corrigée et compatible avec DuckDB

    Examples:
        >>> # Correction DATE_TRUNC avec jours de la semaine
        >>> fix_duck_db_sql('SELECT DATE_TRUNC(WEEK(MONDAY), my_col)')
        "SELECT DATE_TRUNC('week', my_col)"

        >>> fix_duck_db_sql('SELECT DATE_TRUNC(WEEK(FRIDAY), my_col)')
        "SELECT (DATE_TRUNC('week', my_col) + INTERVAL '4' DAY)"

        >>> fix_duck_db_sql('SELECT DATE_DIFF(DAY, start_date, end_date)')
        "SELECT (end_date::DATE - start_date::DATE)"

        >>> # Correction WEEK sans jour spécifié
        >>> fix_duck_db_sql('SELECT DATE_TRUNC(WEEK, my_col)')
        "SELECT DATE_TRUNC('week', my_col)"

        >>> # Correction fonctions géospatiales
        >>> fix_duck_db_sql('SELECT ST_GEOGPOINT(lon, lat)')
        "SELECT ST_POINT(lon, lat)"

        >>> # Correction SAFE_CAST
        >>> fix_duck_db_sql('SELECT SAFE_CAST(my_col AS INTEGER)')
        "SELECT TRY_CAST(my_col AS INTEGER)"

        >>> # Correction EXTRACT(DATE FROM ...)
        >>> fix_duck_db_sql('SELECT EXTRACT(DATE FROM my_timestamp)')
        "SELECT CAST(my_timestamp AS DATE)"

    Note:
        Cette fonction traite les cas les plus courants d'incompatibilité.
        Pour des cas complexes ou spécifiques, une vérification manuelle
        peut être nécessaire.

    Categories handled:
        - SAFE functions: "safe".* -> TRY_*
        - Geospatial functions: ST_GEOG* -> ST_GEOM*
        - Date/Time functions: DATE_TRUNC, EXTRACT, FORMAT_DATE, etc.
        - Type casting: SAFE_CAST -> TRY_CAST
        - Date differences: DATE_DIFF -> arithmetic operations
    """
    s = duckdb_sql

    # === DATE_TRUNC avec WEEK(jour) ===
    # sqlglot produit : DATE_TRUNC('WEEK(MONDAY)', col)
    # DuckDB attend   : DATE_TRUNC('week', col)
    # Pour les jours autres que lundi, un offset est ajouté à l'intérieur de l'expression.

    _day_offsets = {
        "MONDAY": 0,
        "TUESDAY": 1,
        "WEDNESDAY": 2,
        "THURSDAY": 3,
        "FRIDAY": 4,
        "SATURDAY": 5,
        "SUNDAY": 6,
    }

    def _fix_date_trunc_week(match):
        jour = match.group(1).upper()
        date_expr = match.group(2).strip()
        offset = _day_offsets.get(jour, 0)
        if offset == 0:
            return f"DATE_TRUNC('week', {date_expr})"
        return f"(DATE_TRUNC('week', {date_expr}) + INTERVAL '{offset}' DAY)"

    s = re.sub(
        r"DATE_TRUNC\('WEEK\((\w+)\)',\s*([^)]+)\)",
        _fix_date_trunc_week,
        s,
        flags=re.IGNORECASE,
    )

    # DATE_TRUNC('WEEK', ...) sans jour spécifié
    s = re.sub(r"DATE_TRUNC\('WEEK',", "DATE_TRUNC('week',", s, flags=re.IGNORECASE)

    # === SAFE.PARSE_DATE / SAFE.PARSE_TIMESTAMP ===
    # sqlglot <30 : SAFE.PARSE_DATE('%fmt', col)
    # sqlglot 30+ : SAFE.CAST(STRPTIME(col, '%fmt') AS DATE)
    # DuckDB attend : TRY_STRPTIME(col, '%fmt')

    s = re.sub(
        r"SAFE\.CAST\s*\(\s*STRPTIME\s*\(\s*([^,]+?)\s*,\s*'([^']+)'\s*\)\s*AS\s+\w+\s*\)",
        r"TRY_STRPTIME(\1, '\2')",
        s,
        flags=re.IGNORECASE,
    )

    s = re.sub(
        r"SAFE\.PARSE_DATE\s*\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        r"TRY_STRPTIME(\2, '\1')",
        s,
        flags=re.IGNORECASE,
    )

    s = re.sub(
        r"SAFE\.PARSE_TIMESTAMP\s*\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        r"TRY_STRPTIME(\2, '\1')",
        s,
        flags=re.IGNORECASE,
    )

    # === PARSE_DATETIME ===
    # sqlglot <30 : PARSE_DATETIME('%fmt', col)  — format first
    # sqlglot 30+ : PARSE_DATETIME(col, '%fmt')  — col first
    # DuckDB attend : TRY_STRPTIME(col, '%fmt')

    s = re.sub(
        r"PARSE_DATETIME\s*\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        r"TRY_STRPTIME(\2, '\1')",
        s,
        flags=re.IGNORECASE,
    )

    s = re.sub(
        r"PARSE_DATETIME\s*\(\s*([^',][^,]*?)\s*,\s*'([^']+)'\s*\)",
        r"TRY_STRPTIME(\1, '\2')",
        s,
        flags=re.IGNORECASE,
    )

    # === EXTRACT(DATE FROM ...) ===
    s = re.sub(
        r"EXTRACT\s*\(\s*DATE\s+FROM\s+([^)]+)\)",
        r"CAST(\1 AS DATE)",
        s,
        flags=re.IGNORECASE,
    )

    # === EXTRACT(DAYOFWEEK FROM ...) ===
    def _fix_dayofweek(match):
        expr = match.group(1).strip()
        return (
            f"(CASE WHEN EXTRACT(ISODOW FROM {expr}) = 7 "
            f"THEN 1 ELSE EXTRACT(ISODOW FROM {expr}) + 1 END)"
        )

    s = re.sub(
        r"EXTRACT\s*\(\s*DAYOFWEEK\s+FROM\s+([^)]+)\)",
        _fix_dayofweek,
        s,
        flags=re.IGNORECASE,
    )

    # === FORMAT_DATE ===
    s = re.sub(
        r"FORMAT_DATE\s*\(\s*'([^']+)'\s*,\s*([^)]+)\)",
        r"STRFTIME(\2, '\1')",
        s,
        flags=re.IGNORECASE,
    )

    # === SAFE_CAST → TRY_CAST (sqlglot le fait déjà, correction défensive) ===
    s = s.replace("SAFE_CAST", "TRY_CAST")

    # === Fonctions géospatiales ===
    s = s.replace("ST_GEOGPOINT", "ST_POINT")
    s = s.replace("ST_GEOGFROMTEXT", "ST_GEOMFROMTEXT")
    s = s.replace("ST_GEOGFROMWKT", "ST_GEOMFROMTEXT")

    return s


async def run_query_on_test_dataset(
    query: str, session: str, project: str, dialect: str, con: duckdb.DuckDBPyConnection
) -> tuple[DataFrame, str]:
    """
    Run a query on the test dataset in DuckDB.

    Parameters:
        query (str): The SQL query to execute.
        session (str): The session ID for query context.
        project (str): The project name for query context.
        dialect (str): bigquery, postgres ...
        con (DuckDBPyConnection): The connection to the duckdb database

    Returns:
        tuple[DataFrame, str]: The result of the query execution as a Pandas DataFrame,
            and the DuckDB SQL that was actually executed.
    """
    # Parse the query to DuckDB SQL format
    duckdb_sql = await parse_test_query(query, session, dialect)

    # Workaround to fix potential issues with the generated DuckDB SQL
    fixed_duckdb_sql = fix_duck_db_sql(duckdb_sql)

    try:
        result = con.execute(fixed_duckdb_sql).fetchdf()
        return result, fixed_duckdb_sql
    except Exception as e:
        print(f"Failed to run query: {e}")
        print("<<<<<<<<<<fixed_duckdb_sql>>>>>>>>>>")
        print(fixed_duckdb_sql)
        raise


async def create_tables_on_test_dataset(
    query: str,
    table_name: str,
    suffix: str,
    project: str,
    dialect: str,
    con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Create a table on the test dataset in DuckDB using the provided query.

    Parameters:
        query (str): The SQL query used to populate the table.
        table_name (str): The name of the table to be created.
        suffix (str): suffix on the tables name.
        project (str): The project name for query context.
        dialect (str): postgres, bigquery, ...
        con (DuckDBPyConnection): The connection to the duckdb database
    """
    # Parse the query to DuckDB SQL format
    duckdb_sql = await parse_test_query(query, suffix, dialect)

    # Apply fixes to the generated DuckDB SQL
    fixed_duckdb_sql = fix_duck_db_sql(duckdb_sql)

    # Modify the query to create a table
    create_table_sql = (
        f"CREATE OR REPLACE TABLE {table_name}_{suffix} AS {fixed_duckdb_sql}"
    )
    try:
        # Execute the table creation SQL
        con.execute(create_table_sql)
        print(f"Table {table_name}_{suffix} created successfully.")

        # # Fetch and display table schema information
        # result = con.execute(f"PRAGMA table_info('{table_name}_{suffix}')").fetchall()
        # print(f"Schema for table {table_name}_{suffix}: {result}")
    except Exception as e:
        print(f"Failed to create table {table_name}: {e}")
        raise


async def parse_test_query(query, suffix, dialect):
    query_on_test_ds = strip_qualifiers_with_scope(
        sql_query=query, suffix=suffix, dialect=dialect
    )
    sqlglot.parse_one(query_on_test_ds, dialect=dialect)
    duckdb_sql = sqlglot.parse_one(query_on_test_ds, dialect=dialect).sql(
        dialect="duckdb"
    )
    return duckdb_sql


def extract_projects_datasets_from_tables(tables: List[str]) -> List[Tuple[str, str]]:
    projects_datasets = []

    for table in tables:
        table = table.replace("`", "")
        parts = table.split(".")
        if len(parts) == 3:
            project, dataset, _ = parts
            projects_datasets.append((project, dataset))
        elif len(parts) == 2:
            dataset, _ = parts
            projects_datasets.append((None, dataset))
        else:
            raise ValueError(f"Invalid table name: {table}")

    return projects_datasets


def strip_qualifiers_with_scope(
    sql_query: str, dialect: str, suffix: str = None
) -> str:
    """
    Enlève systématiquement les qualifiers project.dataset
    de toutes les tables, en utilisant traverse_scope + find_all_in_scope.
    """

    # 1. Parser la requête en AST (dialecte BigQuery pour gérer les backticks)
    tree = sqlglot.parse_one(sql_query, read=dialect)

    # 2. Parcourir chaque Scope (chaque SELECT ou sous‑requête)
    for scope in traverse_scope(tree):
        # 3. Récupérer toutes les tables dans ce scope
        for table in find_all_in_scope(scope.expression, exp.Table):
            if table.db and table.db != "":
                db = table.db
                original = table.this.name
                if suffix:
                    new_name = (
                        f"{db}_{original}_{suffix.replace('-', '_')}"
                        if db
                        else f"{original}_{suffix.replace('-', '_')}"
                    )
                else:
                    new_name = original
                # 5. Supprimer project et dataset, et renommer la table
                table.set("catalog", None)
                table.set("db", None)
                table.set("this", exp.to_identifier(new_name))

    # 6. Regénérer la requête nettoyée avec backticks si besoin
    return tree.sql(dialect=dialect)


def modify_test_dataset_for_bigquery_exec(
    sql_query: str,
    session_id: str,
    dialect: str,
    test_dataset: str = None,
    test_project: str = BQ_TEST_PROJECT,
) -> str:
    """
    Pour chaque table QUALIFIÉE (catalog.db.table) de la requête,
    remplace par test_project.test_dataset.table_suffix.
    Les tables sans db (ex. CTEs ou tables locales) sont préservées.
    """
    # 1. Parse en AST
    tree = sqlglot.parse_one(sql_query, read=dialect)

    # 2. Suffixe safe pour le nom de table
    suffix = session_id.replace("-", "_") if session_id else ""

    # 3. Parcours de chaque scope (SELECT / sous-requêtes)
    for scope in traverse_scope(tree):
        # 4. Pour chaque nœud Table
        for table in find_all_in_scope(scope.expression, exp.Table):
            # On ne modifie QUE si un dataset (db) est défini
            if (
                table.db
                and table.db != ""
                and (not suffix or suffix not in table.this.name)
            ):
                db = table.db
                orig_tbl = table.this.name
                qualified_tbl = f"{db}_{orig_tbl}" if db else orig_tbl
                new_name = f"{qualified_tbl}_{suffix}" if suffix else qualified_tbl
                table.set("this", exp.to_identifier(new_name))
                if test_project is None:
                    table.set("catalog", None)
                else:
                    table.set("catalog", exp.to_identifier(test_project))
                if test_dataset is None:
                    table.set("db", None)
                else:
                    table.set("db", exp.to_identifier(test_dataset))

    # 7. Génération de la SQL modifiée
    return tree.sql(dialect=dialect)


def verify_tables_in_list(
    sql_query: str,
    tables: List[str],
    dialect: str,
) -> None:
    """
    Vérifie que toutes les tables QUALIFIÉES utilisées dans `sql_query`
    figurent dans la liste `tables`.

    - `tables` peut contenir des chaînes de la forme "proj.ds.tbl" ou "ds.tbl".
    - Ne considère que les tables ayant un dataset (`table.db` non vide).
    - Lève ValueError si des tables référencées ne sont pas dans la liste.

    Args:
        sql_query: Requête SQL à analyser.
        tables: Liste des tables autorisées (format projet.dataset.table ou dataset.table).
        dialect: Dialecte SQL pour le parsing (ex. 'bigquery').

    Raises:
        ValueError: avec le détail des tables manquantes.
    """
    # 1. Parser la requête en AST
    tree = sqlglot.parse_one(sql_query, read=dialect)

    # 2. Extraire les tables référencées (dataset, table)
    referenced: set[Tuple[str, str]] = set()
    for scope in traverse_scope(tree):
        for table in scope.expression.find_all(exp.Table):
            if table.db and table.db != "":
                referenced.add((table.db, table.this.name))

    # 3. Normaliser la liste fournie en tuples (dataset, table)
    normalized_allowed: set[Tuple[str, str]] = set()
    for tbl in tables:
        parts = tbl.split(".")
        # on prend les deux derniers éléments pour dataset et table
        ds, name = parts[-2], parts[-1]
        normalized_allowed.add((ds, name))

    # 4. Détecter les manquantes
    missing = {
        f"{ds}.{tbl}" for ds, tbl in referenced if (ds, tbl) not in normalized_allowed
    }

    # 5. Remonter une erreur si besoin
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Tables non autorisées ou manquantes : {missing_list}")


def is_step_examples(state):
    return "tests" in state.get("route", "").lower()


def transform_timestamp(sql_query):
    # Transformer TIMESTAMP_ADD
    pattern_add = r"TIMESTAMP_ADD\(\s*([^\s,]+)\s*,\s*([^\s,]+)\s*,\s*([^\s\)]+)\s*\)"
    replacement_add = r"\1 + INTERVAL \2 \3"
    sql_query = re.sub(pattern_add, replacement_add, sql_query, flags=re.IGNORECASE)

    # Transformer TIMESTAMP_SUB
    pattern_sub = r"TIMESTAMP_SUB\(\s*([^\s,]+)\s*,\s*([^\s,]+)\s*,\s*([^\s\)]+)\s*\)"
    replacement_sub = r"\1 - INTERVAL \2 \3"
    sql_query = re.sub(pattern_sub, replacement_sub, sql_query, flags=re.IGNORECASE)

    return sql_query


def initialize_duckdb(db_path: str):
    return duckdb.connect(db_path)
