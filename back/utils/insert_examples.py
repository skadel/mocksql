import copy
import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


def parse_struct_definition(schema: str) -> List[tuple]:
    """
    Parse a DuckDB struct definition of the form:
    STRUCT(field1 TYPE1, field2 TYPE2, field3 STRUCT(...), ...)
    into a list of (field_name, field_type).
    Handles array types like STRUCT(...)[] correctly.
    """
    schema = schema.strip()
    if schema.endswith("[]"):
        schema = schema[:-2].strip()
    if not (schema.startswith("STRUCT(") and schema.endswith(")")):
        raise ValueError(f"Invalid STRUCT schema: {schema}")
    inside = schema[len("STRUCT(") : -1].strip()
    fields = []
    start = 0
    bracket_level = 0
    for i, ch in enumerate(inside):
        if ch == "(":
            bracket_level += 1
        elif ch == ")":
            bracket_level -= 1
        elif ch == "," and bracket_level == 0:
            field_str = inside[start:i].strip()
            if field_str:
                fields.append(field_str)
            start = i + 1
    if start < len(inside):
        field_str = inside[start:].strip()
        if field_str:
            fields.append(field_str)
    parsed = []
    for f_str in fields:
        m = re.match(r"^(\w+)\s+(.+)$", f_str)
        if not m:
            raise ValueError(f"Cannot parse field definition: '{f_str}'")
        field_name, field_type = m.groups()
        parsed.append((field_name, field_type.strip()))
    return parsed


def null_struct_expr(duck_type: str) -> str:
    duck_type = duck_type.strip()
    if duck_type.endswith("[]"):
        return f"[]::{duck_type}"
    if duck_type.startswith("STRUCT("):
        fields = parse_struct_definition(duck_type)
        field_exprs = [null_struct_expr(subtype) for _, subtype in fields]
        return f"ROW({', '.join(field_exprs)})::{duck_type}"
    return "NULL"


def dict_to_struct_expr(d: dict, schema: str) -> str:
    fields = parse_struct_definition(schema)
    row_values = []
    for field_name, field_type in fields:
        value = next(
            (val for k, val in d.items() if k.lower() == field_name.lower()), None
        )
        if value is None:
            row_values.append(null_struct_expr(field_type))
        else:
            row_values.append(to_duck_expr(value, field_type))
    return f"ROW({', '.join(row_values)})::{schema}"


def to_duck_expr(value: Any, duck_type: str) -> str:
    duck_type = duck_type.strip()
    if value is None:
        return null_struct_expr(duck_type)

    # STRUCT simple
    if duck_type.startswith("STRUCT(") and not duck_type.endswith("[]"):
        if isinstance(value, dict):
            return dict_to_struct_expr(value, duck_type)
        else:
            return null_struct_expr(duck_type)

    # STRUCT(...)[] => array de structs
    if duck_type.endswith("[]"):
        element_type = duck_type[:-2].strip()
        if isinstance(value, list):
            if not value:
                return f"[]::{duck_type}"
            elements = [
                dict_to_struct_expr(elem, element_type)
                if isinstance(elem, dict)
                else null_struct_expr(element_type)
                for elem in value
            ]
            return f"[{', '.join(elements)}]::{duck_type}"
        return f"[]::{duck_type}"

    # Types texte
    if duck_type.upper() in ["TEXT", "STRING"]:
        escaped_value = str(value).replace("'", "''")
        return f"'{escaped_value}'"

    # Types booléens
    if duck_type.upper() in ["BOOL", "BOOLEAN"]:
        return "TRUE" if value else "FALSE"

    # Types entiers
    if duck_type.upper() in ["INT", "INT64", "BIGINT"]:
        return str(int(value)) if value is not None else "NULL"

    # Types flottants
    if duck_type.upper() in ["FLOAT", "DOUBLE"]:
        return str(float(value)) if value is not None else "NULL"

    # Par défaut, on échappe en string
    escaped_value = str(value).replace("'", "''")
    return f"'{escaped_value}'"


def build_insert_statement(
    table_name: str,
    records: List[Dict],
    schema: Dict,
    used_cols: Optional[List[str]] = None,
) -> str:
    """
    Build SQL INSERT statement with support for nested structs and arrays of structs.
    - used_cols: liste des noms de colonnes à insérer (None => toutes).
    """
    all_cols = [col["name"] for col in schema["columns"]]
    logger.debug(
        "build_insert_statement table=%s  all_cols=%s  used_cols=%s",
        table_name,
        all_cols,
        used_cols,
    )

    # Si on a une liste de colonnes utilisées, on ne garde que celles qui matchent (insensible à la casse).
    if used_cols is not None:
        used_lower = [c.lower() for c in used_cols]
        columns = [c for c in all_cols if c.lower() in used_lower]
    else:
        columns = all_cols

    logger.debug("colonnes finales pour INSERT: %s", columns)

    column_list = ", ".join(columns)
    values_str_list = []

    for record in records:
        row_exprs = []
        for column in columns:
            # Recherche la bonne définition de colonne dans le schema
            col_schema = next(
                (
                    col
                    for col in schema["columns"]
                    if col["name"].lower() == column.lower()
                ),
                None,
            )
            if col_schema:
                duck_type = col_schema["type"]
                logger.debug("  col=%s  duck_type=%s", column, duck_type)

                # Recherche insensible à la casse dans record
                value = next(
                    (val for k, val in record.items() if k.lower() == column.lower()),
                    None,
                )

                # Cas struct array
                if duck_type.endswith("[]") and duck_type.startswith("STRUCT("):
                    element_type = duck_type[:-2].strip()
                    if value is None or not isinstance(value, list):
                        row_exprs.append(f"[]::{duck_type}")
                    else:
                        elements = [
                            dict_to_struct_expr(elem, element_type) for elem in value
                        ]
                        row_exprs.append(f"[{', '.join(elements)}]::{duck_type}")
                else:
                    row_exprs.append(to_duck_expr(value, duck_type))
            else:
                # Colonne non trouvée dans schema => on insère NULL
                row_exprs.append("NULL")
        values_str_list.append(f"({', '.join(row_exprs)})")

    stmt = (
        f"INSERT INTO {table_name} ({column_list}) VALUES {', '.join(values_str_list)};"
    )
    logger.debug("INSERT généré (tronqué) : %.200s", stmt)
    return stmt


def insert_examples(
    data_dict: Dict[str, List[Dict]],
    schemas: List[Dict],
    suffix: str,
    used_columns: Optional[List[Dict[str, List[str]]]] = None,
):
    """
    Génère des instructions SQL INSERT pour chaque table.
    """
    logger.debug("insert_examples appelé avec used_columns=%s", used_columns)
    logger.debug("Schemas : %s", schemas)

    # Transformer la liste used_columns en dict pour accès rapide
    if used_columns is not None:

        def _uc_key(uc: dict) -> str:
            if uc.get("database"):
                return f"{uc['database']}_{uc['table']}"
            parts = uc["table"].split(".")
            return "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]

        used_columns_dict = {_uc_key(uc): uc["used_columns"] for uc in used_columns}
        logger.debug("used_columns_dict keys: %s", list(used_columns_dict.keys()))
    else:
        used_columns_dict = {}
        logger.info(
            "Aucune colonne spécifique utilisée (used_columns est None), insertion de toutes les colonnes."
        )

    def get_schema_for_table(table_name: str, schemas: List[Dict]) -> Optional[Dict]:
        t_parts = table_name.replace("`", "").split(".")
        t_qualified = ".".join(t_parts[-2:]) if len(t_parts) >= 2 else t_parts[-1]
        t_base = t_parts[-1].lower()
        logger.debug(
            "Recherche du schema pour la table '%s' (qualifié: '%s')",
            table_name,
            t_qualified,
        )
        suffix_key = f"_{suffix.replace('-', '_')}"
        for schema in schemas:
            s_name = schema.get("table_name", "")
            if s_name.endswith(suffix_key):
                s_name = s_name[: -len(suffix_key)]
            s_parts = s_name.replace("`", "").split(".")
            s_qualified = ".".join(s_parts[-2:]) if len(s_parts) >= 2 else s_parts[-1]
            if (
                s_qualified.lower() == t_qualified.lower()
                or s_name.lower() == t_base
                or s_name.lower().endswith(f"_{t_base}")
            ):
                return schema
        logger.debug(
            "Aucun schema trouvé pour la table '%s' (qualifié: '%s')",
            table_name,
            t_qualified,
        )
        return None

    for table_name, records in data_dict.items():
        logger.info(
            "Traitement de la table: %s (%d enregistrements)", table_name, len(records)
        )
        table_schema = get_schema_for_table(table_name, schemas)
        logger.debug(f"table schema {table_schema}")
        if table_schema and len(records) > 0:
            t_parts = table_name.split(".")
            suffix_key = suffix.replace("-", "_")
            schema_tname = table_schema["table_name"]
            if schema_tname.endswith(f"_{suffix_key}"):
                duckdb_base = schema_tname[: -len(f"_{suffix_key}")]
            else:
                duckdb_base = (
                    "_".join(t_parts[-2:]) if len(t_parts) >= 2 else t_parts[-1]
                )

            table_name_with_suffix = f"{duckdb_base}_{suffix_key}"
            qualified_key = duckdb_base  # même format que used_columns_dict

            logger.debug(
                "Traitement table=%s  qualified_key=%s", table_name, qualified_key
            )
            if qualified_key in used_columns_dict:
                cols_to_use = used_columns_dict[qualified_key]
                logger.debug("Match used_columns_dict  cols_to_use=%s", cols_to_use)
            else:
                cols_to_use = None
                logger.debug(
                    "Aucun match dans used_columns_dict pour %s (fallback None)",
                    qualified_key,
                )

            insert_stmt = build_insert_statement(
                table_name_with_suffix, records, table_schema, used_cols=cols_to_use
            )
            yield insert_stmt
        elif len(records) == 0:
            logger.debug("Table '%s' ignorée car elle est vide.", table_name)
        else:
            logger.debug(
                "Table '%s' ignorée car aucun schema correspondant n'a été trouvé.",
                table_name,
            )


def replace_missing_with_null(data, schema):
    """
    Cette fonction n'est pas modifiée, elle gère déjà le remplacement par NULL au besoin.
    On la laisse telle quelle.
    """

    def get_schema_for_column(column_name, schema):
        for column in schema["columns"]:
            if column["name"].lower() == column_name.lower():
                return column
        return None

    def parse_struct(struct_data, struct_schema):
        if not struct_schema.startswith("STRUCT<"):
            return struct_data  # Not a STRUCT, return as-is

        # Parse fields in STRUCT
        struct_fields = re.findall(
            r"(\w+) (\w+(?:<.*?>)?)", struct_schema[len("STRUCT<") : -1]
        )
        struct_dict = {}

        for field_name, field_type in struct_fields:
            # Case-insensitive lookup for fields in the struct
            value = (
                next(
                    (
                        v
                        for k, v in struct_data.items()
                        if k.lower() == field_name.lower()
                    ),
                    None,
                )
                if isinstance(struct_data, dict)
                else None
            )
            if field_type.startswith("STRUCT<"):
                struct_dict[field_name] = parse_struct(value, field_type)
            else:
                struct_dict[field_name] = value if value is not None else None

        return struct_dict

    def parse_array_struct(array_data, struct_schema):
        if not isinstance(array_data, list):
            return []  # If not a list, return an empty list
        return [parse_struct(item, struct_schema) for item in array_data]

    def process_row(row, schema):
        processed_row = {}
        for key, value in row.items():
            column_schema = get_schema_for_column(key, schema)

            if column_schema is None:
                processed_row[key] = value  # If no schema exists, keep as-is
            elif column_schema["type"].startswith("STRUCT<"):
                processed_row[key] = parse_struct(value, column_schema["type"])
            elif column_schema["type"].startswith("ARRAY<STRUCT<"):
                # Extract the struct part from ARRAY<...>
                inner_struct = column_schema["type"][len("ARRAY<") : -1]
                processed_row[key] = parse_array_struct(value, inner_struct)
            else:
                processed_row[key] = value

        return processed_row

    processed_data = copy.deepcopy(data)
    for table_name, rows in processed_data.items():
        # Skip if rows is None or not a list
        if not isinstance(rows, list):
            continue
        schema_for_table = next(
            (
                s
                for s in schema
                if s["table_name"].split(".")[-1].lower() == table_name.lower()
            ),
            None,
        )
        if schema_for_table:
            processed_data[table_name] = [
                process_row(row, schema_for_table) for row in rows
            ]

    return processed_data
