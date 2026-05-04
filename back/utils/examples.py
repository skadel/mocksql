import logging
import re
from typing import Dict, Any, Optional, Type, List, Tuple

import duckdb
import sqlglot
from pandas import DataFrame
from pydantic import BaseModel, Field, create_model
from sqlglot import expressions as exp
from sqlglot.optimizer import traverse_scope, find_all_in_scope

from common_vars import type_mapping
from models.env_variables import BQ_TEST_PROJECT

logger = logging.getLogger(__name__)

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
            logger.warning(
                "Skipping table '%s' due to unexpected format.", table["table_name"]
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
            used_cols = {uc.lower() for uc in used_table_entry["used_columns"]}
            used_ids = {
                ui.lower() for ui in used_table_entry.get("used_identifiers", [])
            }
            filtered_columns = []
            for col in table["columns"]:
                if col["name"].lower() in used_cols:
                    if used_ids and "STRUCT<" in col.get("bq_ddl_type", ""):
                        col = {
                            **col,
                            "bq_ddl_type": _filter_struct_type(
                                col["bq_ddl_type"], used_ids
                            ),
                        }
                    filtered_columns.append(col)
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


def _split_ddl_struct_fields(struct_inner: str) -> List[tuple]:
    """Split STRUCT inner content into (field_name, field_type) pairs.

    Uses bracket-level counting so commas inside nested STRUCT<> or ARRAY<> are
    not treated as field separators.  Field names in BQ DDL never contain spaces.
    """
    raw: List[str] = []
    start = 0
    level = 0
    for i, c in enumerate(struct_inner):
        if c == "<":
            level += 1
        elif c == ">":
            level -= 1
        elif c == "," and level == 0:
            raw.append(struct_inner[start:i].strip())
            start = i + 1
    raw.append(struct_inner[start:].strip())

    result = []
    for field in raw:
        parts = field.split(" ", 1)
        if len(parts) == 2:
            result.append((parts[0], parts[1].strip()))
    return result


def _filter_struct_type(bq_ddl_type: str, used_ids: set) -> str:
    """Trim a BQ DDL type to only keep STRUCT fields whose names appear in used_ids."""
    if not used_ids:
        return bq_ddl_type

    is_array = bq_ddl_type.startswith("ARRAY<")
    inner = bq_ddl_type[6:-1] if is_array else bq_ddl_type

    if not inner.startswith("STRUCT<"):
        return bq_ddl_type

    struct_inner = inner[7:-1]
    try:
        pairs = _split_ddl_struct_fields(struct_inner)
    except Exception:
        return bq_ddl_type

    kept = []
    for fname, ftype in pairs:
        if fname.lower() in used_ids:
            kept.append(f"{fname} {_filter_struct_type(ftype, used_ids)}")

    if not kept:
        return bq_ddl_type

    struct_type = f"STRUCT<{', '.join(kept)}>"
    return f"ARRAY<{struct_type}>" if is_array else struct_type


def _bq_ddl_to_pydantic(model_name: str, bq_ddl_type: str):
    """Recursively convert a BQ DDL type string to a Python/Pydantic type."""
    if bq_ddl_type.startswith("ARRAY<"):
        inner = bq_ddl_type[6:-1]
        inner_type = _bq_ddl_to_pydantic(f"{model_name}_item", inner)
        return List[inner_type]

    if bq_ddl_type.startswith("STRUCT<"):
        struct_inner = bq_ddl_type[7:-1]
        try:
            pairs = _split_ddl_struct_fields(struct_inner)
        except Exception:
            return Dict[str, Any]

        fields_def = {}
        for fname, ftype in pairs:
            sub_type = _bq_ddl_to_pydantic(f"{model_name}_{fname}", ftype)
            fields_def[fname.lower()] = (Optional[sub_type], Field(None))

        if not fields_def:
            return Dict[str, Any]

        return create_model(model_name, **fields_def)

    return type_mapping.get(bq_ddl_type.upper(), str)


def create_pydantic_models(filtered_tables_and_columns: list) -> Type[BaseModel]:
    models = {}
    for table in filtered_tables_and_columns:
        table_name = table["table_name"]
        fields = {}

        for column in table["columns"]:
            col_name = column["name"].lower()
            col_description = column.get("description", None)
            bq_ddl = column.get("bq_ddl_type", "")

            if bq_ddl:
                col_type = _bq_ddl_to_pydantic(f"{table_name}_{col_name}", bq_ddl)
            else:
                col_type = parse_field_type(column["type"])

            fields[col_name] = (
                Optional[col_type],
                Field(None, description=col_description),
            )

        model = create_model(table_name, **fields)
        models[table_name] = (
            Optional[list[model]],
            Field(None, description="Model for table "),
        )

    CombinedModel = create_model("CombinedModel", **models)
    return CombinedModel


def _uc_key(uc: dict) -> str:
    """Compute a lookup key matching duckdb_base for a used_columns entry."""
    if uc.get("database"):
        return f"{uc['database']}_{uc['table']}"
    parts = uc["table"].split(".")
    return "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]


def _resolve_duck_type(bq_ddl_type: str, dialect: str) -> str:
    """Convert a BigQuery DDL type string to DuckDB DDL type string via sqlglot."""
    try:
        dummy = sqlglot.parse_one(
            f"CREATE TABLE _t (_c {bq_ddl_type})", dialect=dialect
        )
        col_def = dummy.find(exp.ColumnDef)
        return col_def.args["kind"].sql(dialect="duckdb")
    except Exception:
        return bq_ddl_type


def _get_ddl_type(col_name: str, filtered_columns: list) -> str:
    """Recursively resolve the DuckDB DDL type for a column, handling STRUCT/ARRAY."""
    col = next(c for c in filtered_columns if c["name"] == col_name)
    if bq_ddl_type := col.get("bq_ddl_type"):
        return bq_ddl_type
    base = col["type"].upper()
    mode = col.get("mode", "NULLABLE").upper()
    if base in ("RECORD", "STRUCT"):
        depth = col_name.count(".")
        children = [
            c
            for c in filtered_columns
            if c["name"].startswith(f"{col_name}.")
            and c["name"].count(".") == depth + 1
        ]
        if children:
            inner = ", ".join(
                f"{c['name'].split('.')[-1]} {_get_ddl_type(c['name'], filtered_columns)}"
                for c in children
            )
            struct_type = f"STRUCT<{inner}>"
            return f"ARRAY<{struct_type}>" if mode == "REPEATED" else struct_type
    return f"ARRAY<{base}>" if mode == "REPEATED" else base


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
        used_columns_dict = {_uc_key(uc): uc["used_columns"] for uc in used_columns}
    else:
        used_columns_dict = {}

    for table in tables:
        create_table_query = "(non générée)"
        try:
            parts = table["table_name"].split(".")
            duckdb_base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            qualified_key = duckdb_base  # même format que used_columns_dict
            table_name = f"{duckdb_base}_{suffix.replace('-', '_')}"

            # Filtrage avec sous-champs inclus pour pouvoir reconstruire les types STRUCT
            if qualified_key in used_columns_dict:
                wanted = [c.lower() for c in used_columns_dict[qualified_key]]
                filtered_columns = [
                    col
                    for col in table["columns"]
                    if col["name"].lower() in wanted
                    or any(col["name"].lower().startswith(f"{w}.") for w in wanted)
                ]
            else:
                filtered_columns = table["columns"]

            root_columns = [col for col in filtered_columns if "." not in col["name"]]
            columns_def = ", ".join(
                f"{col['name']} {_get_ddl_type(col['name'], filtered_columns)}"
                for col in root_columns
            )
            create_table_query = f"CREATE TABLE {table_name} ({columns_def});"
            logger.debug("Creating table %s ...", table_name)

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
            logger.debug("Table %s created successfully.", table_name)

            # Enregistre la définition avec les types DuckDB résolus (plus de RECORD brut)
            duckdb_typed_tables.append(
                {
                    "table_name": table_name,
                    "columns": [
                        {
                            **col,
                            "type": _resolve_duck_type(
                                _get_ddl_type(col["name"], filtered_columns), dialect
                            ),
                        }
                        for col in root_columns
                    ],
                }
            )

        except Exception as e:
            errors.append(
                {
                    "table_name": table["table_name"],
                    "query": create_table_query,
                    "error": str(e),
                }
            )

    if errors:
        for err in errors:
            logger.error(
                "Erreur création table %s: %s\n  Requête : %s",
                err["table_name"],
                err["error"],
                err["query"],
            )
        raise RuntimeError(
            f"Échec création de {len(errors)} table(s) : "
            + "; ".join(e["table_name"] for e in errors)
        )

    return duckdb_typed_tables


def execute_queries(queries: list[str], con: duckdb.DuckDBPyConnection):
    try:
        for idx, query in enumerate(queries, start=1):
            try:
                result = con.execute(query).fetchall()  # Fetch results for verification
                logger.debug("Result for query %d: %s", idx, result)
            except Exception as e:
                logger.error("Error executing query %d: %s", idx, e)
    except Exception as e:
        logger.error("Error establishing database connection: %s", e)


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
        logger.error("Failed to run query: %s\nSQL:\n%s", e, fixed_duckdb_sql)
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
        logger.debug("Table %s_%s created successfully.", table_name, suffix)
    except Exception as e:
        logger.error("Failed to create table %s: %s", table_name, e)
        raise


def _fix_unnest_alias_conflicts(tree: exp.Expression) -> exp.Expression:
    """
    Rename UNNEST column aliases that conflict with their source column name.

    BigQuery allows CROSS JOIN UNNEST(hits) AS hits where the alias matches the
    source column.  DuckDB raises "Ambiguous reference" in that case.
    Fix: rename the column alias to _{name}_u and patch all Column refs in the
    query that point to that alias (excluding refs inside the Unnest node itself).
    """
    renames: dict[str, str] = {}

    for unnest_node in tree.find_all(exp.Unnest):
        tbl_alias = unnest_node.find(exp.TableAlias)
        if not tbl_alias:
            continue
        alias_cols = tbl_alias.args.get("columns") or []
        src_names = {
            e.name.lower()
            for e in unnest_node.expressions
            if hasattr(e, "name") and e.name
        }
        for alias_col in alias_cols:
            old = alias_col.name
            if old and old.lower() in src_names:
                new = f"_{old}_u"
                renames[old] = new
                alias_col.set("this", new)

    if not renames:
        return tree

    # Source columns inside UNNEST (e.g. `hits` in UNNEST(hits)) have no table
    # qualifier, so they are naturally immune to the rename below.
    # References to an unnested alias inside another UNNEST (e.g. `hits.product`
    # in UNNEST(hits.product)) have the alias as their table qualifier and must
    # be renamed — so we do not skip columns that appear inside Unnest nodes.
    for col in tree.find_all(exp.Column):
        # hits.type   → Column(table="hits", this="type")            → col.table matches
        # hits.page.pagePath → Column(db="hits", table="page", this="pagePath") → col.db matches
        for attr in ("catalog", "db", "table"):
            val = col.text(attr)
            if val and val in renames:
                col.set(attr, exp.to_identifier(renames[val]))
                break

    return tree


async def parse_test_query(query, suffix, dialect):
    query_on_test_ds = strip_qualifiers_with_scope(
        sql_query=query, suffix=suffix, dialect=dialect
    )
    tree = sqlglot.parse_one(query_on_test_ds, dialect=dialect)
    tree = _fix_unnest_alias_conflicts(tree)
    duckdb_sql = tree.sql(dialect="duckdb")
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
