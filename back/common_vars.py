import datetime
import re
from typing import Annotated

from pydantic import BeforeValidator


def _normalize_datetime_str(v):
    if isinstance(v, str):
        v = re.sub(r"^(\d{4}-\d{2}-\d{2}) ", r"\1T", v)
        v = re.sub(r"\+(\d{2})$", r"+\1:00", v)
    return v


# Accepts both strict ISO 8601 and common LLM variants ("2026-01-01 00:00:00+00")
FlexibleDatetime = Annotated[
    datetime.datetime, BeforeValidator(_normalize_datetime_str)
]


COMMON_HISTORY_TABLE_NAME = "common_history"
SESSIONS_TABLE_NAME = "sessions"
LITERALS_TABLE_NAME = "lit"
EXAMPLES_TABLE_NAME = "examples"
USERS_TABLE_NAME = "users"
MODELS_TABLE_NAME = "models"
PROJECTS_TABLE_NAME = "projects"
USER_PROJECTS_TABLE_NAME = "user_projects"
PROJECT_TABLES_TABLE_NAME = "project_tables"
USER_SETTINGS_TABLE_NAME = "user_settings"

type_mapping = {
    "INTEGER": int,
    "INT64": int,
    "INT": int,
    "BIGINT": int,
    "SMALLINT": int,
    "TINYINT": int,
    "BYTEINT": int,
    "STRING": str,
    "FLOAT": float,
    "FLOAT64": float,
    "DOUBLE": float,
    "DOUBLE PRECISION": float,
    "REAL": float,
    "DATE": datetime.date,
    "TIMESTAMP": FlexibleDatetime,
}


async def get_table_details(project_id: str, **_):
    from models.schemas import get_schemas

    content = []

    def _esc(s: str) -> str:
        return s.replace("{", "{{").replace("}", "}}")

    schema = await get_schemas(project_id)

    for table in schema:
        lines = [f"Table: {table['table_name']}"]

        # Description de la table
        table_desc = (table.get("description") or "").strip()
        if table_desc:
            lines.append(f"Description: {_esc(table_desc)}")

        # Primary keys (optionnel)
        pks = table.get("primary_keys") or []
        if pks:
            lines.append(f"Primary keys: {', '.join(pks)}")

        for col in table.get("columns", []):
            parts = []

            col_type = (col.get("type") or "").strip()
            if col_type:
                parts.append(f"Type: {_esc(col_type)}")

            description = (col.get("description") or "").strip()
            if description:
                parts.append(f"Description: {_esc(description)}")

            # Règles d'affichage des exemples
            if col.get("is_categorical"):
                parts.append(
                    f"Exemple de 10 valeurs possibles pour ce champ : {_esc((col.get('examples') or '').strip())}"
                )
                # sinon: ne rien afficher (spécification inchangée)
            else:
                examples = (col.get("examples") or "").strip()
                if examples:
                    parts.append(
                        f"Exemples de valeurs possibles pour ce champ : {_esc(examples)}"
                    )

            lines.append(
                "Column name: {name}".format(name=col["name"])
                + (", " + ", ".join(parts) if parts else "")
            )

        if table.get("columns"):
            content.append("\n".join(lines))

    return "\n\n".join(content)


async def get_tables_mapping(project_id: str, **_):
    from models.schemas import get_schemas

    schema = await get_schemas(project_id)
    mapping = {}

    for table in schema:
        cols = {}
        for col in table["columns"]:
            # 1. On ignore les sous-colonnes aplaties (celles avec un '.')
            # car le type imbriqué de la colonne racine contient déjà tout.
            if "." not in col["name"]:
                # 2. On prend bq_ddl_type en priorité, sinon type par défaut
                # (Aussi, cols[key] = value est beaucoup plus performant que **cols)
                cols[col["name"]] = col.get("bq_ddl_type") or col["type"]

        parts = table["table_name"].split(".")
        key = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        mapping[key] = cols

    return mapping
