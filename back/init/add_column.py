import asyncio
import re
from typing import Dict

from common_vars import (
    MODELS_TABLE_NAME,
    SESSIONS_TABLE_NAME,
    USER_PROJECTS_TABLE_NAME,
    USERS_TABLE_NAME,
    PROJECTS_TABLE_NAME,
    COMMON_HISTORY_TABLE_NAME,
)
from models.database import execute
from models.env_variables import DB_MODE

# Tables autorisées, avec un "clé logique" simplifiée pour le CLI/paramètres
ALLOWED_TABLES: Dict[str, str] = {
    "models": MODELS_TABLE_NAME,
    "sessions": SESSIONS_TABLE_NAME,
    "user_projects": USER_PROJECTS_TABLE_NAME,
    "users": USERS_TABLE_NAME,
    "projects": PROJECTS_TABLE_NAME,
    "chat_history": COMMON_HISTORY_TABLE_NAME,
}

# Types logiques autorisés
ALLOWED_TYPES = {"varchar", "text", "int", "bool", "timestamp", "json"}


def validate_identifier(name: str) -> None:
    """
    Vérifie que le nom de colonne est un identifiant SQL simple (pas d'injection).
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(f"Nom invalide pour un identifiant SQL : {name!r}")


def get_sql_type(type_key: str) -> str:
    """
    Mappe un type logique (varchar, int, json, ...) vers le type SQL concret
    en fonction du DB_MODE.
    """
    type_key = type_key.lower()
    if type_key not in ALLOWED_TYPES:
        raise ValueError(
            f"Type {type_key!r} non autorisé. Types autorisés : {sorted(ALLOWED_TYPES)}"
        )

    pg_like = DB_MODE in ("postgres", "cloudsql")

    if type_key == "varchar":
        return "VARCHAR(255)" if pg_like else "VARCHAR"
    if type_key == "text":
        return "TEXT"
    if type_key == "int":
        return "INT" if pg_like else "INTEGER"
    if type_key == "bool":
        return "BOOLEAN"
    if type_key == "timestamp":
        return "TIMESTAMP"
    if type_key == "json":
        return "JSONB" if pg_like else "JSON"

    # Ne doit pas arriver à cause du check ALLOWED_TYPES
    raise ValueError(f"Type {type_key!r} non géré")


async def add_column_to_table(
    table_key: str,
    column_name: str,
    type_key: str,
) -> None:
    """
    Ajoute une colonne à une table autorisée, si elle n'existe pas déjà.
    - table_key : clé logique ('models', 'users', ...)
    - column_name : nom de colonne SQL (vérifié)
    - type_key : type logique ('varchar', 'int', 'json', ...)
    """
    table_key = table_key.strip().lower()
    if table_key not in ALLOWED_TABLES:
        raise ValueError(
            f"Table {table_key!r} non autorisée. Tables autorisées : {sorted(ALLOWED_TABLES.keys())}"
        )

    table_name = ALLOWED_TABLES[table_key]
    validate_identifier(column_name)
    column_type = get_sql_type(type_key)

    sql = f"""
    ALTER TABLE {table_name}
    ADD COLUMN IF NOT EXISTS {column_name} {column_type};
    """

    await execute(sql)
    print(
        f"✔ Colonne {column_name!r} ({column_type}) ajoutée (ou déjà existante) sur la table {table_name!r}."
    )


def main():
    print("=== Ajout de colonne dans une table ===")
    print("DB_MODE :", DB_MODE)
    print("Tables disponibles :")
    for key, name in ALLOWED_TABLES.items():
        print(f"  - {key:12s} -> {name}")

    while True:
        table_key = (
            input("Choisis la table (clé logique, ex: 'models', 'users') : ")
            .strip()
            .lower()
        )
        if table_key in ALLOWED_TABLES:
            break
        print(
            f"Table inconnue. Tables autorisées : {', '.join(sorted(ALLOWED_TABLES.keys()))}"
        )

    while True:
        column_name = input("Nom de la colonne à ajouter : ").strip()
        try:
            validate_identifier(column_name)
            break
        except ValueError as e:
            print(e)

    print("Types disponibles :", ", ".join(sorted(ALLOWED_TYPES)))
    while True:
        type_key = (
            input("Type de la colonne (ex: varchar, int, json) : ").strip().lower()
        )
        if type_key in ALLOWED_TYPES:
            break
        print(f"Type inconnu. Types autorisés : {', '.join(sorted(ALLOWED_TYPES))}")

    # Confirmation
    table_real_name = ALLOWED_TABLES[table_key]
    sql_type = get_sql_type(type_key)
    print()
    print("Résumé :")
    print(f"  Table   : {table_key} -> {table_real_name}")
    print(f"  Colonne : {column_name}")
    print(f"  Type    : {type_key} -> {sql_type}")
    confirm = input("Confirmer ? (o/N) : ").strip().lower()
    if confirm != "o":
        print("Opération annulée.")
        return

    asyncio.run(add_column_to_table(table_key, column_name, type_key))


if __name__ == "__main__":
    main()
