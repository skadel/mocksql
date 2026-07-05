import logging
import re
from typing import Dict, Any, Optional, Type, List, Tuple

import duckdb
import sqlglot
from pandas import DataFrame
from pydantic import BaseModel, ConfigDict, Field, create_model
from sqlglot import expressions as exp
from sqlglot.optimizer import traverse_scope, find_all_in_scope

import datetime

from common_vars import FlexibleDatetime, type_mapping
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

        # Rechercher dans used_columns en comparant base de données + table.
        # Comparaison INSENSIBLE À LA CASSE : la qualification sqlglot de certains
        # dialectes (Trino…) met les identifiants de used_columns en minuscules,
        # alors que le schema_cache conserve la casse d'origine de l'entrepôt
        # (BigQuery). Sans .lower() des deux côtés, aucune table ne matche → le
        # schéma de génération se vide, le LLM ne produit aucune donnée, et seul
        # Faker survit (tables de fait manquantes → Catalog Error à l'exécution).
        used_table_entry = next(
            (
                item
                for item in used_columns
                if (item.get("database") or "").lower() == db_name_from_schema.lower()
                and (item.get("table") or "").lower() == table_name_from_schema.lower()
            ),
            None,
        )

        if used_table_entry:
            # Nom de table final aligné sur la casse de used_columns (source de
            # vérité du pipeline : faker_cols et l'executor construisent tous la
            # clé f"{db}_{table}" à partir de used_columns). En BigQuery la casse
            # coïncide avec le schéma → sortie inchangée ; en Trino elle suit
            # used_columns (minuscules) → cohérence des clés en aval.
            db_key = used_table_entry.get("database") or db_name_from_schema
            tbl_key = used_table_entry.get("table") or table_name_from_schema
            qualified_under_name = f"{db_key}_{tbl_key}"

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
        used_keys: set[str] = set()
        has_alias = False
        for fname, ftype in pairs:
            sub_type = _bq_ddl_to_pydantic(f"{model_name}_{fname}", ftype)
            fname_lower = fname.lower()
            # Même garde qu'au niveau colonne : un champ de STRUCT à underscore
            # initial (ex. STRUCT<_dt DATE>) ferait planter create_model.
            if fname_lower.startswith("_"):
                key = _safe_field_name(fname_lower, used_keys)
                fields_def[key] = (Optional[sub_type], Field(None, alias=fname_lower))
                has_alias = True
            else:
                used_keys.add(fname_lower)
                fields_def[fname_lower] = (Optional[sub_type], Field(None))

        if not fields_def:
            return Dict[str, Any]

        return create_model(
            model_name,
            __config__=_ALIASED_MODEL_CONFIG if has_alias else None,
            **fields_def,
        )

    return type_mapping.get(bq_ddl_type.upper(), str)


# Config partagée : `populate_by_name` accepte le nom réel ET la clé assainie en
# entrée ; `serialize_by_alias` fait que .dict()/model_dump() ré-émet le nom réel
# (l'alias) — indispensable car les clés du dump deviennent les noms de colonnes
# DuckDB en aval. Sans ça, une colonne à underscore initial sortirait assainie.
_ALIASED_MODEL_CONFIG = ConfigDict(populate_by_name=True, serialize_by_alias=True)


def _safe_field_name(name: str, used: set[str]) -> str:
    """Nom de champ Pydantic valide et unique pour `name`.

    Pydantic rejette les noms à underscore initial (détectés comme attributs
    privés) — fréquents en dbt (`_line_number`, `_dt`, `_feed_valid_from`). On
    retire les underscores de tête et on garde le nom réel via un alias ; en cas
    de collision (ex. `_dt` vs `dt`) on suffixe pour rester injectif.
    """
    candidate = name.lstrip("_") or "field"
    if candidate[0].isdigit():
        candidate = f"f_{candidate}"
    base, i = candidate, 1
    while candidate in used:
        candidate = f"{base}_{i}"
        i += 1
    used.add(candidate)
    return candidate


def _iso_format_hint(col_type) -> str:
    """Rappel de format ISO pour un champ TYPÉ date/timestamp, à coller à sa description.

    Un champ `date`/`timestamp` du schéma de sortie s'écrit TOUJOURS en ISO, quel que soit
    le format des littéraux du SQL (un `PARSE_DATE('%d-%m-%Y', col)` ne concerne QUE les
    colonnes TEXTE). Sans ce rappel, le LLM recopie le format du SQL (ex. `01-01-2026`) →
    `OutputParserException` Pydantic → retry coûteux (incident c2). Porté par la DESCRIPTION
    du champ pour survivre au retry sans contexte (schéma + erreur seuls). Cf. consigne 5.
    Chaîne vide pour tout autre type (identité stricte : `datetime.datetime` sous-classe
    `datetime.date`, mais `type_mapping` mappe TIMESTAMP → FlexibleDatetime, jamais date).
    """
    if col_type is datetime.date:
        return " (⚠️ champ typé date : littéral ISO obligatoire, format YYYY-MM-DD)"
    if col_type is FlexibleDatetime:
        return " (⚠️ champ typé timestamp : littéral ISO obligatoire, format YYYY-MM-DDTHH:MM:SS)"
    return ""


def create_pydantic_models(filtered_tables_and_columns: list) -> Type[BaseModel]:
    models = {}
    for table in filtered_tables_and_columns:
        table_name = table["table_name"]
        fields = {}
        has_alias = False

        # Les colonnes « normales » réservent leur nom tel quel : une clé assainie
        # d'underscore (ex. `_dt`→`dt`) ne doit jamais écraser une vraie colonne `dt`.
        used_keys: set[str] = {
            c["name"].lower()
            for c in table["columns"]
            if not c["name"].lower().startswith("_")
        }

        for column in table["columns"]:
            col_name = column["name"].lower()
            col_description = column.get("description", None)
            bq_ddl = column.get("bq_ddl_type", "")

            if bq_ddl:
                col_type = _bq_ddl_to_pydantic(f"{table_name}_{col_name}", bq_ddl)
            else:
                col_type = parse_field_type(column["type"])

            # Champ typé date/timestamp → rappel ISO dans la description (survit au retry
            # Pydantic sans contexte). No-op pour tout autre type. Cf. _iso_format_hint.
            iso_hint = _iso_format_hint(col_type)
            if iso_hint:
                col_description = (
                    (str(col_description) + iso_hint)
                    if col_description
                    else iso_hint.strip()
                )

            # Underscore initial → clé assainie + alias sur le nom réel.
            if col_name.startswith("_"):
                field_key = _safe_field_name(col_name, used_keys)
                field = Field(None, alias=col_name, description=col_description)
                has_alias = True
            else:
                field_key = col_name
                field = Field(None, description=col_description)

            fields[field_key] = (Optional[col_type], field)

        model = create_model(
            table_name,
            __config__=_ALIASED_MODEL_CONFIG if has_alias else None,
            **fields,
        )
        models[table_name] = (
            Optional[list[model]],
            Field(None, description="Model for table "),
        )

    # serialize_by_alias sur le modèle combiné : le générateur appelle
    # `.data.dict()` à ce niveau, et le flag doit se propager aux modèles imbriqués.
    CombinedModel = create_model(
        "CombinedModel", __config__=_ALIASED_MODEL_CONFIG, **models
    )
    return CombinedModel


def _uc_key(uc: dict) -> str:
    """Compute a lookup key matching duckdb_base for a used_columns entry."""
    if uc.get("database"):
        return f"{uc['database']}_{uc['table']}"
    parts = uc["table"].split(".")
    return "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]


def _resolve_duck_type(bq_ddl_type: str) -> str:
    """Convert a BigQuery DDL type string to DuckDB DDL type string via sqlglot.

    Le type d'entrée est toujours en syntaxe BigQuery (STRING / STRUCT<> /
    ARRAY<>), donc on parse comme bigquery quel que soit le dialect source.
    """
    try:
        dummy = sqlglot.parse_one(
            f"CREATE TABLE _t (_c {bq_ddl_type})", dialect="bigquery"
        )
        _widen_bare_decimals(dummy)
        col_def = dummy.find(exp.ColumnDef)
        return col_def.args["kind"].sql(dialect="duckdb")
    except Exception:
        return bq_ddl_type


def _widen_bare_decimals(tree: exp.Expression) -> exp.Expression:
    """Donne une précision large aux décimaux sans précision (NUMBER/DECIMAL/NUMERIC).

    Un `NUMBER`/`DECIMAL` sans `(p, s)` (typique d'un import Snowflake où la
    précision a été perdue, ou d'un BigQuery NUMERIC) se résout en DuckDB par le
    défaut `DECIMAL(18, 3)`, qui **déborde** sur tout grand entier (timestamps en
    µs, valeurs wei blockchain, ids…) :
    `Could not convert string "2000000000000000" to DECIMAL(18,3)`.

    On élargit donc tout décimal sans précision à `DECIMAL(38, 9)` — assez large
    pour les grands entiers comme pour les fractions. Les décimaux qui portent
    déjà une précision explicite (`NUMBER(12, 2)`) sont laissés intacts.
    """
    for dt in tree.find_all(exp.DataType):
        if dt.this == exp.DataType.Type.DECIMAL and not dt.expressions:
            dt.set(
                "expressions",
                [
                    exp.DataTypeParam(this=exp.Literal.number(38)),
                    exp.DataTypeParam(this=exp.Literal.number(9)),
                ],
            )
    return tree


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
    # Clés en minuscules (cf. filter_schemas_by_used_columns) : used_columns peut être
    # en minuscules (qualification Trino) alors que le nom de table du schéma garde la
    # casse d'origine → sans normalisation le filtrage retombe silencieusement sur
    # « toutes les colonnes ».
    if used_columns is not None:
        used_columns_dict = {
            _uc_key(uc).lower(): uc["used_columns"] for uc in used_columns
        }
    else:
        used_columns_dict = {}

    for table in tables:
        create_table_query = "(non générée)"
        try:
            parts = table["table_name"].split(".")
            duckdb_base = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            qualified_key = (
                duckdb_base.lower()
            )  # même format que used_columns_dict (minuscule)
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
                f"`{col['name']}` {_get_ddl_type(col['name'], filtered_columns)}"
                for col in root_columns
            )
            create_table_query = f"CREATE TABLE {table_name} ({columns_def});"
            logger.debug("Creating table %s ...", table_name)

            # Le DDL ci-dessus est TOUJOURS en syntaxe BigQuery (backticks +
            # STRING/STRUCT<>/ARRAY<>, issus de bq_ddl_type / _get_ddl_type),
            # indépendamment du dialect SOURCE du projet. On le parse donc comme
            # bigquery, jamais comme `dialect` : sinon, pour un projet
            # dialect=duckdb/postgres, les backticks font échouer le parse
            # ("Expecting )"). La cible d'exécution reste DuckDB.
            create_test_table_tree = sqlglot.parse_one(
                create_table_query, dialect="bigquery"
            )
            _widen_bare_decimals(create_test_table_tree)
            create_test_table_query = create_test_table_tree.sql(dialect="duckdb")

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
                                _get_ddl_type(col["name"], filtered_columns)
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


def fix_duck_db_sql(duckdb_sql: str, source_dialect: str = "bigquery") -> str:
    """
    Applique des corrections et des traductions sémantiques pour les requêtes DuckDB
    transpilées par sqlglot.

    Les corrections BigQuery (fonctions SAFE.*, géospatiales ST_GEOG*, DATE_TRUNC WEEK,
    SUBSTR position 0…) ne sont appliquées que si source_dialect == "bigquery".

    Args:
        duckdb_sql (str): Requête SQL générée par sqlglot pour DuckDB
        source_dialect (str): Dialecte source de la transpilation ("bigquery", "postgres"…).
            Défaut : "bigquery".

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

    if source_dialect == "bigquery":
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
        # sqlglot 30+ : PARSE_DATETIME(col, '%fmt')  — col first (ou littéral first)
        # DuckDB attend : TRY_STRPTIME(col, '%fmt')
        #
        # Stratégie : l'arg format est identifié par son préfixe '%'.
        # Cela couvre les deux ordres sans dépendre de la version de sqlglot.

        def _fix_parse_datetime(m):
            a1, a2 = m.group(1).strip(), m.group(2).strip()
            if a1.startswith("'%"):  # format en 1er (sqlglot <30)
                return f"TRY_STRPTIME({a2}, {a1})"
            elif a2.startswith("'%"):  # valeur en 1er (sqlglot 30+)
                return f"TRY_STRPTIME({a1}, {a2})"
            return m.group(0)  # indéterminable, laisser tel quel

        s = re.sub(
            r"PARSE_DATETIME\s*\(\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            _fix_parse_datetime,
            s,
            flags=re.IGNORECASE,
        )

        # === EXTRACT(DATE FROM ...) ===
        # sqlglot 30 enveloppe le littéral timestamp dans un CAST(...AS TIMESTAMPTZ),
        # ce qui ajoute des parens imbriquées. Le pattern gère un niveau d'imbrication.
        s = re.sub(
            r"EXTRACT\s*\(\s*DATE\s+FROM\s+((?:[^()]+|\([^()]*\))+)\)",
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

        # === SUBSTR(expr, 0, n) → SUBSTR(expr, 1, n) ===
        # BigQuery : position 0 est clampée à 1 → SUBSTR('ABCD', 0, 2) = 'AB'
        # DuckDB   : position 0 = avant le 1er char → SUBSTR('ABCD', 0, 2) = 'A' (un char perdu)
        # Limitation : ne couvre pas les premiers arguments contenant une virgule (ex: CONCAT(a, b)).
        s = re.sub(
            r"\bSUBSTR(?:ING)?\s*\(([^,]+),\s*0\s*,",
            lambda m: f"SUBSTR({m.group(1).strip()}, 1,",
            s,
            flags=re.IGNORECASE,
        )

        # === SAFE_CAST → TRY_CAST (sqlglot le fait déjà, correction défensive) ===
        s = s.replace("SAFE_CAST", "TRY_CAST")

        # === Fonctions géospatiales ===
        s = s.replace("ST_GEOGPOINT", "ST_POINT")
        s = s.replace("ST_GEOGFROMTEXT", "ST_GEOMFROMTEXT")
        s = s.replace("ST_GEOGFROMWKT", "ST_GEOMFROMTEXT")

    if source_dialect == "snowflake":
        # IFF(cond, a, b) → IF(cond, a, b)
        s = re.sub(r"\bIFF\s*\(", "IF(", s, flags=re.IGNORECASE)

        # ZEROIFNULL(x) → COALESCE(x, 0)
        s = re.sub(
            r"\bZEROIFNULL\s*\(([^)]+)\)",
            lambda m: f"COALESCE({m.group(1)}, 0)",
            s,
            flags=re.IGNORECASE,
        )

        # NULLIFZERO(x) → NULLIF(x, 0)
        s = re.sub(
            r"\bNULLIFZERO\s*\(([^)]+)\)",
            lambda m: f"NULLIF({m.group(1)}, 0)",
            s,
            flags=re.IGNORECASE,
        )

        # LISTAGG(x, sep) → STRING_AGG(x, sep)
        s = re.sub(r"\bLISTAGG\s*\(", "STRING_AGG(", s, flags=re.IGNORECASE)

        # EQUAL_NULL(a, b) → (a IS NOT DISTINCT FROM b)
        s = re.sub(
            r"\bEQUAL_NULL\s*\(([^,]+),\s*([^)]+)\)",
            lambda m: (
                f"({m.group(1).strip()} IS NOT DISTINCT FROM {m.group(2).strip()})"
            ),
            s,
            flags=re.IGNORECASE,
        )

        # DATEADD(unit, n, date) → date + INTERVAL n UNIT
        s = re.sub(
            r"\bDATEADD\s*\(\s*(\w+)\s*,\s*(-?\d+)\s*,\s*([^)]+)\)",
            lambda m: (
                f"({m.group(3).strip()} + INTERVAL '{m.group(2)}' {m.group(1).upper()})"
            ),
            s,
            flags=re.IGNORECASE,
        )

        # NB: TO_TIMESTAMP / TO_TIMESTAMP_NTZ/LTZ/TZ est traité plus en amont sur
        # l'AST (_fix_snowflake_idioms dans parse_test_query) car le rendu dépend du
        # type de l'argument (numérique epoch → to_timestamp ; sinon → CAST AS
        # TIMESTAMP). Surtout PAS de regex `TO_TIMESTAMP → CAST AS TIMESTAMP` ici :
        # elle écraserait le `to_timestamp(epoch)` légitime émis par l'AST en un
        # `CAST(DOUBLE AS TIMESTAMP)` que DuckDB rejette.

        # TO_DATE(x) → CAST(x AS DATE) — filet de sécurité si TO_DATE a survécu en
        # Anonymous (sqlglot le rend déjà en CAST AS DATE dans la plupart des cas).
        s = re.sub(
            r"\bTO_DATE\s*\(([^)]+)\)",
            lambda m: f"CAST({m.group(1)} AS DATE)",
            s,
            flags=re.IGNORECASE,
        )

        # STRTOK_TO_ARRAY(str, delim) → STRING_SPLIT(str, delim)
        s = re.sub(r"\bSTRTOK_TO_ARRAY\s*\(", "STRING_SPLIT(", s, flags=re.IGNORECASE)

    return s


def _fix_bare_unnest_col_refs(sql: str, error_msg: str) -> str | None:
    """
    Piloted by DuckDB BinderException: finds the exact bare column name from the
    error message (e.g. productrevenue) and injects the full struct path from the
    UNNEST alias.  Only touches the one reported column — never corrupts other
    references like fullvisitorid.

    Scope-aware: a bare column is only rewritten to ``<unnest_tbl>.<unnest_col>.<field>``
    when an UNNEST is defined in the **same scope** as that column.  Without this,
    a multi-CTE query where the UNNEST lives in CTE A and the bare column in CTE B
    would get ``_t0.value.<col>`` injected into B, where ``_t0`` is out of scope —
    producing a worse "Referenced table _t0 not found" error (see c2.sql regression).
    """
    from sqlglot.optimizer.scope import traverse_scope

    m = re.search(
        r'(?:Referenced )?[Cc]olumn "([^"]+)" (?:not found|referenced that exists)',
        error_msg,
    )
    if not m:
        return None

    bare_col = m.group(1).lower()

    try:
        tree = sqlglot.parse_one(sql, dialect="duckdb")
    except Exception:
        return None

    patched = False
    for scope in traverse_scope(tree):
        # UNNEST aliases defined directly as sources of THIS scope only.
        local_unnest: list[tuple[str, str]] = []
        for source in scope.sources.values():
            unnest = (
                source.expression
                if hasattr(source, "expression")
                and isinstance(source.expression, exp.Unnest)
                else source
                if isinstance(source, exp.Unnest)
                else None
            )
            if unnest is None:
                continue
            table_alias = unnest.args.get("alias")
            if isinstance(table_alias, exp.TableAlias):
                t_name = table_alias.name
                cols = table_alias.args.get("columns", [])
                if t_name and cols:
                    local_unnest.append((t_name, cols[0].name))

        if not local_unnest:
            continue

        target_t, target_c = local_unnest[-1]

        # Only columns directly in this scope's expression — never reach into a
        # nested Subquery, which traverse_scope visits as its own scope.
        subquery_col_ids = {
            id(c)
            for sq in scope.expression.find_all(exp.Subquery)
            for c in sq.find_all(exp.Column)
        }
        for col in scope.expression.find_all(exp.Column):
            if id(col) in subquery_col_ids:
                continue
            if (
                col.name.lower() == bare_col
                and not col.args.get("table")
                and not col.args.get("db")
            ):
                col.set("table", exp.to_identifier(target_c))
                col.set("db", exp.to_identifier(target_t))
                patched = True

    if not patched:
        return None

    return tree.sql(dialect="duckdb")


_MISSING_EXTENSION_RE = re.compile(
    r"but it exists in the (\w+) extension", re.IGNORECASE
)


def _missing_extension_hint(err: str) -> str | None:
    """Si l'erreur DuckDB pointe une extension non chargée, renvoie un message
    actionnable expliquant comment l'activer dans mocksql.yml. Sinon None."""
    m = _MISSING_EXTENSION_RE.search(err or "")
    if not m:
        return None
    ext = m.group(1).lower()
    return (
        f"Cette requête utilise une fonction de l'extension DuckDB '{ext}', "
        f"non chargée. Active-la dans mocksql.yml :\n\n"
        f"duckdb:\n  extensions:\n    - {ext}\n"
    )


async def run_query_on_test_dataset(
    query: str,
    session: str,
    project: str,
    dialect: str,
    con: duckdb.DuckDBPyConnection,
    precompiled_sql: str | None = None,
) -> tuple[DataFrame, str]:
    """
    Run a query on the test dataset in DuckDB.

    Parameters:
        query (str): The SQL query to execute.
        session (str): The session ID for query context.
        project (str): The project name for query context.
        dialect (str): bigquery, postgres ...
        con (DuckDBPyConnection): The connection to the duckdb database
        precompiled_sql (str | None): DuckDB SQL déjà transpilé (suffixe injecté). Quand
            fourni, on saute `parse_test_query` + `fix_duck_db_sql` — utilisé par le replay
            (`mocksql test`) qui transpile une seule fois par modèle puis substitue le
            suffixe par cas, au lieu de re-parser le SQL identique à chaque cas (sqlglot).

    Returns:
        tuple[DataFrame, str]: The result of the query execution as a Pandas DataFrame,
            and the DuckDB SQL that was actually executed.
    """
    if precompiled_sql is not None:
        current_sql = precompiled_sql
    else:
        duckdb_sql = await parse_test_query(query, session, dialect)
        current_sql = fix_duck_db_sql(duckdb_sql, dialect)

    for _ in range(10):
        try:
            result = con.execute(current_sql).fetchdf()
            return result, current_sql
        except duckdb.BinderException as e:
            patched = _fix_bare_unnest_col_refs(current_sql, str(e))
            if (
                patched is None
                and "Referenced table" in str(e)
                and "not found" in str(e)
            ):
                try:
                    tree = sqlglot.parse_one(current_sql, dialect="duckdb")
                    fixed = _fix_unnest_scope_leak(tree).sql(dialect="duckdb")
                    patched = fixed if fixed != current_sql else None
                except Exception:
                    patched = None
            if patched is None:
                logger.error("Failed to run query: %s\nSQL:\n%s", e, current_sql)
                raise
            current_sql = patched
        except Exception as e:
            hint = _missing_extension_hint(str(e))
            if hint:
                logger.error("%s\nSQL:\n%s", hint, current_sql)
                raise RuntimeError(hint) from e
            logger.error("Failed to run query: %s\nSQL:\n%s", e, current_sql)
            raise

    result = con.execute(current_sql).fetchdf()
    return result, current_sql


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
    fixed_duckdb_sql = fix_duck_db_sql(duckdb_sql, dialect)

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


def _fix_unnest_scope_leak(tree: exp.Expression) -> exp.Expression:
    """
    Fix column refs that reference a _tN alias from an inner scope.

    qualify_columns may resolve `product.v2productname` in the outer scope to _t3
    (the inner subquery's UNNEST alias) instead of _t1 (the outer one).  DuckDB
    then raises "Referenced table '_t3' not found".

    Two Column representations handled:
      Case 1: Column(table="_t3", this="_product_u")           → table is wrong _tN
      Case 2: Column(db="_t3", table="_product_u", this="field") → db is wrong _tN

    Algorithm (per scope):
      1. Build {col_alias → _tN} from UNNEST sources defined in THIS scope only.
      2. For every Column in scope.columns:
         Case 1: table_ref not in local scope → look up col.name in local_unnest
         Case 2: db_ref not in local scope → look up col.table in local_unnest
    """
    from sqlglot.optimizer.scope import traverse_scope

    for scope in traverse_scope(tree):
        local_unnest: dict[str, str] = {}
        local_tables: set[str] = set(scope.sources.keys())

        for alias, source in scope.sources.items():
            unnest = (
                source.expression
                if hasattr(source, "expression")
                and isinstance(source.expression, exp.Unnest)
                else source
                if isinstance(source, exp.Unnest)
                else None
            )
            if unnest is None:
                continue
            tbl_alias_node = unnest.find(exp.TableAlias)
            if not tbl_alias_node:
                continue
            for col_id in tbl_alias_node.args.get("columns") or []:
                local_unnest[col_id.name.lower()] = alias

        if not local_unnest:
            continue

        # scope.columns may include Column refs from nested IN-subqueries (because
        # sqlglot treats them as correlated outer refs when the table is not found
        # in the inner scope).  We must exclude them: only fix columns that appear
        # directly in this scope's expression, not inside a Subquery node.
        subquery_col_ids: set[int] = {
            id(col)
            for sq in scope.expression.find_all(exp.Subquery)
            for col in sq.find_all(exp.Column)
        }
        direct_cols = [
            col
            for col in scope.expression.find_all(exp.Column)
            if id(col) not in subquery_col_ids
        ]

        for col in direct_cols:
            # Case 1: Column(table="_t3", this="_product_u") — table is wrong _tN
            table_ref = col.text("table")
            if table_ref and table_ref not in local_tables:
                correct = local_unnest.get(col.name.lower())
                if correct:
                    col.set("table", exp.to_identifier(correct))
                    continue

            # Case 2: Column(db="_t3", table="_product_u", this="field")
            # — db is the wrong _tN; table is the UNNEST column alias
            db_ref = col.text("db")
            if db_ref and db_ref not in local_tables:
                correct = local_unnest.get(col.text("table").lower())
                if correct:
                    col.set("db", exp.to_identifier(correct))

    return tree


def _fix_unnest_alias_conflicts(tree: exp.Expression) -> exp.Expression:
    """
    Rename UNNEST column aliases that conflict with their source column name.

    BigQuery allows CROSS JOIN UNNEST(hits) AS hits where the alias matches the
    source column.  DuckDB raises "Ambiguous reference" in that case.
    Fix: rename the column alias to _{name}_u and patch all Column refs in the
    query that point to that alias.

    Two representation cases handled (depends on whether qualify_columns ran first):
      Pre-qualify:  hits.type  → Column(table="hits", this="type")       → qualifier renamed
      Post-qualify: _t0.hits.type → Dot(Column(table="_t0", this="hits"), "type")
                    → col.name == "hits" and col.table is the UNNEST table alias → col.this renamed
    """
    renames: dict[str, str] = {}
    # table_alias → {old_col_alias → new_col_alias}, populated after qualify_tables
    unnest_table_col_renames: dict[str, dict[str, str]] = {}

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
        table_alias_name = tbl_alias.name
        for alias_col in alias_cols:
            old = alias_col.name
            if old and old.lower() in src_names:
                new = f"_{old}_u"
                renames[old] = new
                alias_col.set("this", new)
                if table_alias_name:
                    unnest_table_col_renames.setdefault(table_alias_name, {})[old] = new

    if not renames:
        return tree

    for col in tree.find_all(exp.Column):
        renamed = False
        # Pre-qualify case: qualifier is the UNNEST column alias (hits.type, product.v2name)
        for attr in ("catalog", "db", "table"):
            val = col.text(attr)
            if val and val in renames:
                col.set(attr, exp.to_identifier(renames[val]))
                renamed = True
                break

        if not renamed:
            # Post-qualify case: qualify_columns replaced the UNNEST table alias with _tN
            # and turned struct access into Dot(Column(table="_tN", this="old_alias"), field).
            # The column name itself is the UNNEST alias — rename col.this.
            col_table = col.text("table")
            if col_table and col_table in unnest_table_col_renames:
                old_name = col.name
                if old_name in unnest_table_col_renames[col_table]:
                    col.set(
                        "this",
                        exp.to_identifier(
                            unnest_table_col_renames[col_table][old_name]
                        ),
                    )

    return tree


def _fix_group_by_strict_mode(tree: exp.Expression) -> None:
    """
    Add to GROUP BY any SELECT column not wrapped in an aggregate and not already present.
    DuckDB requires strict GROUP BY (no functional-dependency shortcut like BigQuery).
    Modifies the tree in-place.
    """
    for select in tree.find_all(exp.Select):
        group = select.args.get("group")
        if not group:
            continue
        if (
            group.args.get("all")
            or group.args.get("grouping_sets")
            or group.args.get("rollup")
            or group.args.get("cube")
        ):
            continue

        grouped_sqls = {g.sql(dialect="duckdb").lower() for g in group.expressions}

        to_add = []
        for sel_expr in select.expressions:
            inner = sel_expr.this if isinstance(sel_expr, exp.Alias) else sel_expr
            if isinstance(inner, (exp.Star, exp.Literal)):
                continue
            if inner.find(exp.AggFunc):
                continue
            for col in inner.find_all(exp.Column):
                col_sql = col.sql(dialect="duckdb").lower()
                if col_sql not in grouped_sqls:
                    to_add.append(col.copy())
                    grouped_sqls.add(col_sql)

        if to_add:
            group.set("expressions", list(group.expressions) + to_add)


def _qualify_group_order_by_aliases(tree: exp.Expression) -> None:
    """
    Replace bare column refs in GROUP BY / ORDER BY with their qualified form
    when a SELECT alias maps to a qualified column, to avoid DuckDB ambiguity
    errors when the alias name also exists in multiple joined tables.
    e.g. SELECT t.year AS year ... JOIN y ON t.year = y.year GROUP BY year
      → GROUP BY t.year

    Workaround for sqlglot not resolving GROUP BY aliases during BQ→DuckDB
    transpilation. See test_alert_when_sqlglot_fixes_group_by_aliases_natively.
    """
    for select in tree.find_all(exp.Select):
        alias_map: dict[str, exp.Column] = {}
        for expr in select.expressions:
            if (
                isinstance(expr, exp.Alias)
                and isinstance(expr.this, exp.Column)
                and expr.this.args.get("table")
            ):
                alias_map[expr.alias.lower()] = expr.this

        if not alias_map:
            continue

        group = select.args.get("group")
        if group:
            new_exprs = []
            for expr in group.expressions:
                if (
                    isinstance(expr, exp.Column)
                    and not expr.args.get("table")
                    and expr.name.lower() in alias_map
                ):
                    new_exprs.append(alias_map[expr.name.lower()].copy())
                else:
                    new_exprs.append(expr)
            group.set("expressions", new_exprs)

            for special_key in ("rollup", "cube", "grouping_sets"):
                special_list = group.args.get(special_key)
                if not special_list:
                    continue
                for item in special_list:
                    if not isinstance(item, exp.Expression):
                        continue
                    for col in list(item.find_all(exp.Column)):
                        if not col.args.get("table") and col.name.lower() in alias_map:
                            col.replace(alias_map[col.name.lower()].copy())

        order = select.args.get("order")
        if order:
            for ordered in order.expressions:
                inner = ordered.this
                if (
                    isinstance(inner, exp.Column)
                    and not inner.args.get("table")
                    and inner.name.lower() in alias_map
                ):
                    ordered.set("this", alias_map[inner.name.lower()].copy())


_COL_N_RE = re.compile(r"^_col_\d+$")


def _sanitize_alias(name: str) -> str:
    """Lowercase + collapse non-alphanumerics to single underscores."""
    name = re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()
    return name or "col"


def _derive_projection_alias(expr: exp.Expression) -> Optional[str]:
    """Readable alias for an unnamed/opaque final-SELECT projection, or None to
    leave it as-is. Functions become `<fn>_<firstcol>` (e.g. AVG(corr) -> avg_corr);
    bare columns and complex expressions are left untouched."""
    inner = expr.this if isinstance(expr, exp.Alias) else expr
    if isinstance(inner, (exp.Column, exp.Star)):
        return None  # already carries a meaningful name
    if isinstance(inner, exp.Func):
        fname = (inner.sql_name() or "").lower()
        if not fname:
            return None
        col = inner.find(exp.Column)
        return _sanitize_alias(f"{fname}_{col.name}" if col is not None else fname)
    return None  # arithmetic / CASE / etc. — don't guess, keep _col_N


def _alias_unnamed_final_projections(tree: exp.Expression) -> None:
    """Give the OUTERMOST select's unnamed or `_col_N`-aliased projections a
    readable alias derived from the expression, so the result schema and the
    assertions generated on it use business-legible names instead of `_col_1`.

    Inner CTE/subquery projections are left alone (referenced elsewhere). ORDER BY
    / GROUP BY / HAVING / QUALIFY references to a renamed `_col_N` alias in the same
    select are updated to keep the query valid.
    """
    if not isinstance(tree, exp.Select):
        return  # UNION / non-select roots: leave untouched

    used: set[str] = set()
    for proj in tree.expressions:
        if isinstance(proj, exp.Alias) and not _COL_N_RE.match(proj.alias):
            used.add(proj.alias.lower())
        elif isinstance(proj, exp.Column):
            used.add(proj.name.lower())

    renames: dict[str, str] = {}  # old _col_N name -> new name
    for proj in tree.expressions:
        is_col_n = isinstance(proj, exp.Alias) and bool(_COL_N_RE.match(proj.alias))
        is_unnamed = not isinstance(proj, (exp.Alias, exp.Column, exp.Star))
        if not (is_col_n or is_unnamed):
            continue
        base = _derive_projection_alias(proj)
        if not base:
            continue
        new = base
        i = 2
        while new.lower() in used:
            new = f"{base}_{i}"
            i += 1
        used.add(new.lower())
        if is_col_n:
            renames[proj.alias] = new
            proj.set("alias", exp.to_identifier(new))
        else:
            proj.replace(exp.alias_(proj.copy(), new))

    # Keep references to renamed _col_N aliases in sync (same select only).
    if renames:
        for key in ("order", "group", "having", "qualify"):
            clause = tree.args.get(key)
            if clause is None:
                continue
            for col in clause.find_all(exp.Column):
                if not col.table and col.name in renames:
                    col.set("this", exp.to_identifier(renames[col.name]))


def _fix_unnest_with_offset(tree: exp.Expression) -> exp.Expression:
    """Rends ``UNNEST(arr) WITH OFFSET AS offset`` (BigQuery) exécutable sur DuckDB.

    sqlglot transpile ``WITH OFFSET AS offset`` en ``WITH ORDINALITY`` mais **perd
    le nom de la colonne** : DuckDB nomme la colonne d'ordinalité ``ordinality`` et
    la référence ``offset`` échoue ("Referenced column offset not found"). De plus,
    l'``OFFSET`` BigQuery est 0-based alors que l'``ORDINALITY`` DuckDB est 1-based.

    On réécrit chaque ``UNNEST`` porteur d'un offset nommé en sous-requête corrélée
    qui réexpose un ``offset`` 0-based sous son nom d'origine :

        ... CROSS JOIN UNNEST(arr) WITH OFFSET AS offset
        →
        ... CROSS JOIN (
              SELECT u._ord_N - 1 AS offset
              FROM UNNEST(arr) WITH ORDINALITY AS u(_uval_N, _ord_N)
            ) AS _unnest_off_N

    La valeur dépliée n'est pas projetée — cohérent avec l'expansion ``SELECT *``
    de sqlglot qui n'expose que ``offset`` (et non l'élément du tableau). DuckDB
    résout la corrélation sur ``arr`` via un lateral implicite (jointure virgule).
    """
    n = 0
    for unnest_node in list(tree.find_all(exp.Unnest)):
        offset = unnest_node.args.get("offset")
        if not isinstance(offset, exp.Identifier):
            continue
        n += 1
        offset_name = offset.name
        ord_id, val_id = f"_ord_{n}", f"_uval_{n}"
        synth_alias = f"_unnest_off_{n}"
        inner = exp.Unnest(
            expressions=[unnest_node.expressions[0].copy()],
            alias=exp.TableAlias(
                this=exp.to_identifier("u"),
                columns=[exp.to_identifier(val_id), exp.to_identifier(ord_id)],
            ),
            offset=True,
        )
        zero_based = exp.alias_(
            exp.Sub(this=exp.column(ord_id, "u"), expression=exp.Literal.number(1)),
            offset_name,
        )
        subquery = exp.Subquery(
            this=exp.Select(expressions=[zero_based]).from_(inner),
            alias=exp.TableAlias(this=exp.to_identifier(synth_alias)),
        )
        unnest_node.replace(subquery)
    return tree


# Tokens de format Snowflake TO_CHAR → tokens strftime DuckDB (longest-first :
# l'alternation regex est leftmost, donc YYYY avant YY, HH24 avant HH, etc.).
_SNOW_TO_CHAR_TOKENS: list[tuple[str, str]] = [
    ("YYYY", "%Y"),
    ("YY", "%y"),
    ("MMMM", "%B"),
    ("MON", "%b"),
    ("MM", "%m"),
    ("DD", "%d"),
    ("DY", "%a"),
    ("HH24", "%H"),
    ("HH12", "%I"),
    ("HH", "%H"),
    ("MI", "%M"),
    ("SS", "%S"),
    ("AM", "%p"),
    ("PM", "%p"),
]
_SNOW_TO_CHAR_RE = re.compile(
    "|".join(re.escape(k) for k, _ in _SNOW_TO_CHAR_TOKENS), re.IGNORECASE
)
_SNOW_TO_CHAR_MAP = {k.upper(): v for k, v in _SNOW_TO_CHAR_TOKENS}


def _snow_fmt_to_strftime(snow_fmt: str) -> str | None:
    """Traduit un format date Snowflake en format strftime DuckDB.

    Retourne ``None`` si AUCUN token de date n'est reconnu (ex. format numérique
    '999,999.00') — dans ce cas on laisse sqlglot rendre `TO_CHAR` en CAST AS TEXT.
    """
    matched = False

    def _repl(m: re.Match) -> str:
        nonlocal matched
        matched = True
        return _SNOW_TO_CHAR_MAP[m.group(0).upper()]

    out = _SNOW_TO_CHAR_RE.sub(_repl, snow_fmt)
    return out if matched else None


def _fix_snowflake_to_char(tree: exp.Expression) -> exp.Expression:
    """Réécrit ``TO_CHAR(x, '<fmt date>')`` en ``strftime(x, '<fmt duckdb>')``.

    sqlglot transpile `TO_CHAR(x, fmt)` snowflake→duckdb en **abandonnant** l'argument
    de format (→ `CAST(x AS TEXT)`, avec un warning), ce qui perd le formatage. On
    réécrit donc sur l'AST snowflake AVANT la transpilation, tant que le format est
    encore présent.
    """
    for node in list(tree.find_all(exp.ToChar)):
        fmt = node.args.get("format")
        if not isinstance(fmt, exp.Literal) or not fmt.is_string:
            continue
        duck_fmt = _snow_fmt_to_strftime(fmt.this)
        if duck_fmt is None:
            continue
        node.replace(
            exp.Anonymous(
                this="strftime",
                expressions=[node.this.copy(), exp.Literal.string(duck_fmt)],
            )
        )
    return tree


def _is_numeric_expr(node: exp.Expression) -> bool:
    """Heuristique : l'expression dénote-t-elle un nombre (epoch) plutôt qu'une chaîne ?"""
    if isinstance(node, exp.Paren):
        return _is_numeric_expr(node.this)
    if isinstance(node, (exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod)):
        return True
    if isinstance(node, exp.Literal):
        return not node.is_string
    if isinstance(node, exp.Cast):
        target = node.to
        return isinstance(target, exp.DataType) and target.this in (
            exp.DataType.Type.INT,
            exp.DataType.Type.BIGINT,
            exp.DataType.Type.DECIMAL,
            exp.DataType.Type.DOUBLE,
            exp.DataType.Type.FLOAT,
        )
    return False


_SNOW_TO_TIMESTAMP_FNS = {
    "TO_TIMESTAMP",
    "TO_TIMESTAMP_NTZ",
    "TO_TIMESTAMP_LTZ",
    "TO_TIMESTAMP_TZ",
}


def _fix_snowflake_to_timestamp(tree: exp.Expression) -> exp.Expression:
    """Réécrit les ``TO_TIMESTAMP[_NTZ/LTZ/TZ](x)`` laissés en Anonymous par sqlglot.

    sqlglot rend déjà `TO_TIMESTAMP('<chaîne>')` en `CAST AS TIMESTAMP`, mais laisse
    les variantes `_NTZ/_LTZ/_TZ` et `TO_TIMESTAMP(<numérique|colonne>)` en Anonymous.
    DuckDB n'a pas `to_timestamp_ntz` ; et `to_timestamp(epoch)` attend un nombre.
    Le rendu dépend donc du type de l'argument :
    - numérique (epoch en secondes, ex. ``"bt" / 1000000``) → ``to_timestamp(x)``
      (DuckDB ne sait PAS faire `CAST(DOUBLE AS TIMESTAMP)`) ;
    - sinon (chaîne, colonne) → ``CAST(x AS TIMESTAMP)``.
    """
    for node in list(tree.find_all(exp.Anonymous)):
        name = (node.this or "").upper()
        if name not in _SNOW_TO_TIMESTAMP_FNS or len(node.expressions) != 1:
            continue
        arg = node.expressions[0]
        if _is_numeric_expr(arg):
            replacement: exp.Expression = exp.Anonymous(
                this="to_timestamp", expressions=[arg.copy()]
            )
        else:
            replacement = exp.cast(arg.copy(), "TIMESTAMP")
        node.replace(replacement)
    return tree


def _strip_hex_prefix(concat: exp.Expression) -> exp.Expression | None:
    """Retire le littéral ``'0x'`` en tête d'une chaîne de concaténations.

    Retourne le reste de la concaténation (copie), ou ``None`` si l'expression
    n'est pas une concaténation préfixée par ``'0x'``. Les ``||`` chaînés sont
    imbriqués à gauche par sqlglot (``('0x' || a) || b``) : on descend donc
    récursivement jusqu'à la feuille la plus à gauche.
    """

    def _is_0x(n: exp.Expression) -> bool:
        return isinstance(n, exp.Literal) and n.is_string and n.this.lower() == "0x"

    if isinstance(concat, exp.DPipe):
        if _is_0x(concat.this):
            return concat.expression.copy()
        stripped = _strip_hex_prefix(concat.this)
        if stripped is not None:
            return exp.DPipe(this=stripped, expression=concat.expression.copy())
        return None
    if isinstance(concat, exp.Concat):
        exprs = concat.expressions
        if len(exprs) >= 2 and _is_0x(exprs[0]):
            rest = [e.copy() for e in exprs[1:]]
            if len(rest) == 1:
                return rest[0]
            return exp.Concat(
                expressions=rest,
                safe=concat.args.get("safe"),
                coalesce=concat.args.get("coalesce"),
            )
    return None


def _fix_snowflake_hex_cast(tree: exp.Expression) -> exp.Expression:
    """Réécrit ``CAST('0x' || h AS FLOAT)`` en ``hexstr_to_double(h)``.

    Snowflake interprète une chaîne ``'0x…'`` castée en FLOAT comme de
    l'hexadécimal ; DuckDB refuse (il ne parse l'hexa que comme littéral).
    Le macro ``hexstr_to_double`` est enregistré sur chaque connexion par
    ``storage.config:apply_duckdb_extensions``. Couvre CAST et TRY_CAST
    (sous-classe), cibles FLOAT/DOUBLE uniquement — un cast texte ou décimal
    n'est pas touché.
    """
    for node in list(tree.find_all(exp.Cast)):
        target = node.to
        if not isinstance(target, exp.DataType) or target.this not in (
            exp.DataType.Type.FLOAT,
            exp.DataType.Type.DOUBLE,
        ):
            continue
        inner = node.this
        while isinstance(inner, exp.Paren):
            inner = inner.this
        stripped = _strip_hex_prefix(inner)
        if stripped is None:
            continue
        node.replace(exp.Anonymous(this="hexstr_to_double", expressions=[stripped]))
    return tree


def _fix_snowflake_idioms(tree: exp.Expression) -> exp.Expression:
    """Réécritures d'idiomes Snowflake que sqlglot ne transpile pas correctement.

    Appliquées sur l'AST snowflake AVANT ``.sql(dialect="duckdb")``.
    """
    _fix_snowflake_to_char(tree)
    _fix_snowflake_to_timestamp(tree)
    _fix_snowflake_hex_cast(tree)
    return tree


# Tokens de format Joda-Time (Trino format_datetime) → tokens strftime DuckDB.
# CASSE-SENSIBLE : en Joda, MM=mois et mm=minute, HH=24h et hh=12h — surtout PAS
# de IGNORECASE. Alternation longest-first (yyyy avant yy, MMMM avant MM…).
_TRINO_JODA_TOKENS: list[tuple[str, str]] = [
    ("yyyy", "%Y"),
    ("YYYY", "%Y"),
    ("MMMM", "%B"),
    ("EEEE", "%A"),
    ("MMM", "%b"),
    ("EEE", "%a"),
    ("yy", "%y"),
    ("YY", "%y"),
    ("MM", "%m"),
    ("dd", "%d"),
    ("HH", "%H"),
    ("hh", "%I"),
    ("mm", "%M"),
    ("ss", "%S"),
]
_TRINO_JODA_RE = re.compile(
    "|".join(
        re.escape(k) for k, _ in sorted(_TRINO_JODA_TOKENS, key=lambda kv: -len(kv[0]))
    )
)
_TRINO_JODA_MAP = {k: v for k, v in _TRINO_JODA_TOKENS}


def _trino_joda_to_strftime(joda_fmt: str) -> str | None:
    """Traduit un format Joda-Time (Trino) en format strftime DuckDB.

    Retourne ``None`` si AUCUN token de date n'est reconnu — dans ce cas on laisse
    l'expression telle quelle plutôt que de fabriquer un format vide.
    """
    matched = False

    def _repl(m: re.Match) -> str:
        nonlocal matched
        matched = True
        return _TRINO_JODA_MAP[m.group(0)]

    out = _TRINO_JODA_RE.sub(_repl, joda_fmt)
    return out if matched else None


def _fix_trino_format_datetime(tree: exp.Expression) -> exp.Expression:
    """Réécrit ``format_datetime(x, '<fmt Joda>')`` en ``strftime(x, '<fmt duckdb>')``.

    sqlglot laisse ``format_datetime`` en Anonymous (DuckDB n'a pas cette fonction →
    Catalog Error). On réécrit sur l'AST avant le rendu DuckDB en traduisant le
    format Joda-Time vers strftime.
    """
    for node in list(tree.find_all(exp.Anonymous)):
        if (node.this or "").lower() != "format_datetime":
            continue
        args = node.expressions
        if len(args) != 2 or not (
            isinstance(args[1], exp.Literal) and args[1].is_string
        ):
            continue
        duck_fmt = _trino_joda_to_strftime(args[1].this)
        if duck_fmt is None:
            continue
        node.replace(
            exp.Anonymous(
                this="strftime",
                expressions=[args[0].copy(), exp.Literal.string(duck_fmt)],
            )
        )
    return tree


def _fix_trino_reduce_finish(tree: exp.Expression) -> exp.Expression:
    """Préserve la lambda de finition de ``reduce(arr, init, merge, finish)``.

    sqlglot transpile Trino ``reduce`` vers DuckDB ``list_reduce`` en **abandonnant
    silencieusement** la 4ᵉ lambda (finition), avec un simple warning : le résultat
    devient faux sans erreur dès que la finition n'est pas l'identité (ex.
    ``s -> s / cardinality(arr)`` pour une moyenne). On inline donc la finition :
    ``finish(reduce_sans_finish(...))``, ce qui reste correct y compris pour
    l'identité (``s -> s`` → juste le reduce). Perte silencieuse → résultat exact.
    """
    for node in list(tree.find_all(exp.Reduce)):
        finish = node.args.get("finish")
        if not isinstance(finish, exp.Lambda) or len(finish.expressions) != 1:
            continue
        param_name = finish.expressions[0].name
        body = finish.this.copy()
        inner = exp.Reduce(
            this=node.this.copy(),
            initial=node.args["initial"].copy(),
            merge=node.args["merge"].copy(),
        )

        def _is_param(n: exp.Expression) -> bool:
            # Dans une lambda, une référence de paramètre est un Identifier nu ;
            # une vraie colonne est un Column (qui enveloppe son Identifier — exclu).
            if isinstance(n, exp.Identifier):
                return n.name == param_name and not isinstance(n.parent, exp.Column)
            if isinstance(n, exp.Column):
                return not n.table and n.name == param_name
            return False

        if _is_param(body):
            new_expr: exp.Expression = inner
        else:
            for n in list(body.find_all(exp.Column, exp.Identifier)):
                if _is_param(n):
                    n.replace(inner.copy())
            new_expr = body
        node.replace(new_expr)
    return tree


def _fix_trino_idioms(tree: exp.Expression) -> exp.Expression:
    """Réécritures d'idiomes Trino que sqlglot ne transpile pas correctement.

    Appliquées sur l'AST trino AVANT ``.sql(dialect="duckdb")``.
    """
    _fix_trino_format_datetime(tree)
    _fix_trino_reduce_finish(tree)
    return tree


async def parse_test_query(query, suffix, dialect):
    query_on_test_ds = strip_qualifiers_with_scope(
        sql_query=query, suffix=suffix, dialect=dialect
    )
    tree = sqlglot.parse_one(query_on_test_ds, dialect=dialect)
    _qualify_group_order_by_aliases(tree)
    _fix_group_by_strict_mode(tree)
    # _fix_unnest_with_offset traduit la sémantique 0-based de BigQuery WITH OFFSET.
    # Trino WITH ORDINALITY est déjà 1-based comme DuckDB (et la forme t(x, i) est
    # native DuckDB) : appliquer le fix décale l'ordinal et perd la colonne valeur.
    if dialect != "trino":
        _fix_unnest_with_offset(tree)
    _alias_unnamed_final_projections(tree)
    if dialect == "snowflake":
        _fix_snowflake_idioms(tree)
    elif dialect == "trino":
        _fix_trino_idioms(tree)
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
        # Noms courts de tables utilisés comme qualificateurs de colonnes dans ce scope.
        # On collecte `col.table` (la partie la plus proche du nom de colonne) qu'il
        # s'agisse d'un qualificateur 1-part déjà simplifié (objects.col) ou d'un
        # qualificateur multi-part (project.dataset.objects.col) qui sera nettoyé :
        # dans les deux cas, `col.table = "objects"` après le nettoyage.
        # Lowercasé : sqlglot peut écrire le qualificateur de colonne en casse
        # normalisée (minuscule) alors que la table garde sa casse d'origine —
        # comparer en minuscule évite de rater le rapprochement (cf. step 6).
        col_table_names: set[str] = {
            col.text("table").lower()
            for col in scope.expression.find_all(exp.Column)
            if col.text("table")
        }

        # Collecte les tables renommées : (db, name) EN MINUSCULE pour corriger les
        # colonnes. On IGNORE le catalog (project) : un qualificateur de colonne s'écrit
        # toujours `dataset.table`.col (jamais le project), alors que la table source a pu
        # être enrichie d'un catalog en amont (`pipetalk-493612.dataset.table`). Comparer
        # avec le catalog ferait rater le rapprochement. La casse est aussi normalisée
        # (DuckDB/BigQuery sont insensibles à la casse).
        tables_being_renamed: set[tuple] = set()

        # 3. Récupérer toutes les tables dans ce scope
        for table in find_all_in_scope(scope.expression, exp.Table):
            if table.db and table.db != "":
                db = table.db
                original = table.this.name
                existing_alias = table.alias
                if suffix:
                    new_name = (
                        f"{db}_{original}_{suffix.replace('-', '_')}"
                        if db
                        else f"{original}_{suffix.replace('-', '_')}"
                    )
                else:
                    new_name = original
                tables_being_renamed.add((db.lower(), original.lower()))
                # 5. Supprimer project et dataset, et renommer la table
                table.set("catalog", None)
                table.set("db", None)
                table.set("this", exp.to_identifier(new_name))
                # Quand la table est réellement renommée (suffix fourni) et que des
                # colonnes du scope utilisent le nom court original comme qualificateur
                # (ex: objects.col), forcer un alias explicite pour que DuckDB puisse
                # les résoudre.  On ne touche pas les tables qui ont déjà un alias
                # explicite (l'utilisateur a écrit FROM ... AS alias).
                if (
                    not existing_alias
                    and new_name != original
                    and original.lower() in col_table_names
                ):
                    table.set(
                        "alias",
                        exp.TableAlias(this=exp.to_identifier(original)),
                    )

        # 6. Supprimer catalog/db des colonnes qui référencent une table renommée.
        # Sans ça, une colonne qualifiée `project.dataset.table`.col reste avec ses
        # qualificateurs après le renommage de la table, ce qui produit une référence
        # invalide en DuckDB (ex: "bigquery-public-data"."the_met"."objects"."col").
        for col in scope.expression.find_all(exp.Column):
            col_db = col.text("db")
            if col_db:
                col_table = col.text("table")
                # Match sur (db, table) en ignorant le catalog (cf. construction de
                # tables_being_renamed) — le qualificateur de colonne ne porte jamais
                # le project.
                if (col_db.lower(), col_table.lower()) in tables_being_renamed:
                    col.set("catalog", None)
                    col.set("db", None)

    # 7. Regénérer la requête nettoyée avec backticks si besoin
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
    from storage.config import apply_duckdb_extensions

    con = duckdb.connect(db_path)
    apply_duckdb_extensions(con)
    return con
