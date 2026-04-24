import argparse
import asyncio
import json
from datetime import datetime

from common_vars import PROJECTS_TABLE_NAME
from models.database import execute, query
from models.schemas import invalidate_project_cache
from utils.schema_utils import update_schema


async def add_table_to_project(project_id: str, new_tables: list) -> None:
    """
    Ajoute une ou plusieurs tables au json_schema d'un projet existant.
    """
    rows = await query(
        f"SELECT json_schema FROM {PROJECTS_TABLE_NAME} WHERE project_id = $1",
        (project_id,),
    )
    if not rows:
        raise ValueError(f"Projet introuvable : {project_id!r}")

    existing_schema = json.loads(rows[0].get("json_schema") or "[]")
    merged = update_schema(existing_schema, new_tables)

    await execute(
        f"UPDATE {PROJECTS_TABLE_NAME} SET json_schema = $1, updated_at = $2 WHERE project_id = $3",
        json.dumps(merged, ensure_ascii=False),
        datetime.now().isoformat(),
        project_id,
    )
    invalidate_project_cache(project_id)
    print(f"{len(new_tables)} table(s) ajoutee(s) au projet {project_id!r}.")


def _prompt_table() -> dict:
    """Mode interactif : saisie d'une definition de table."""
    print("\n--- Nouvelle table ---")
    table_catalog = input(
        "Catalog (ex: my-bq-project, laisser vide si inutile): "
    ).strip()
    table_schema_name = input(
        "Schema/Dataset (ex: my_dataset, laisser vide si inutile): "
    ).strip()
    table_name = input("Nom de table: ").strip()
    if not table_name:
        raise ValueError("Le nom de table est obligatoire.")

    parts = [p for p in [table_catalog, table_schema_name, table_name] if p]
    full_name = ".".join(parts)
    description = input("Description de la table (optionnel): ").strip()

    columns = []
    print("Colonnes (laisser le nom vide pour terminer):")
    while True:
        col_name = input("  Nom de colonne: ").strip()
        if not col_name:
            break
        col_type = input(f"  Type de '{col_name}': ").strip()
        col_desc = input(f"  Description de '{col_name}' (optionnel): ").strip()
        columns.append({"name": col_name, "type": col_type, "description": col_desc})

    return {"table_name": full_name, "description": description, "columns": columns}


def _load_tables_from_file(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, dict):
        return [data]
    if isinstance(data, list):
        return data
    raise ValueError("Le fichier JSON doit contenir un objet ou un tableau d'objets.")


def main():
    parser = argparse.ArgumentParser(
        description="Ajoute une ou plusieurs tables au schema d'un projet MockSQL.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Format JSON attendu (objet ou tableau d'objets) :
  {
    "table_name": "catalog.dataset.ma_table",
    "description": "Description de la table",
    "columns": [
      {"name": "id",   "type": "INTEGER", "description": "Cle primaire"},
      {"name": "name", "type": "STRING",  "description": "Nom"}
    ]
  }

Exemples :
  add_table --project-id my-proj --file tables.json
  add_table --project-id my-proj --json '{"table_name":"ds.t","columns":[{"name":"id","type":"INT"}]}'
  add_table --project-id my-proj          # mode interactif
        """,
    )
    parser.add_argument("--project-id", required=True, help="ID du projet cible")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--file", "-f", help="Chemin vers un fichier JSON decrivant la/les tables"
    )
    group.add_argument(
        "--json",
        "-j",
        dest="json_str",
        metavar="JSON",
        help="JSON inline decrivant la/les tables",
    )
    args = parser.parse_args()

    if args.file:
        new_tables = _load_tables_from_file(args.file)
    elif args.json_str:
        data = json.loads(args.json_str)
        new_tables = [data] if isinstance(data, dict) else data
    else:
        new_tables = []
        while True:
            table = _prompt_table()
            new_tables.append(table)
            again = input("\nAjouter une autre table ? (o/N): ").strip().lower()
            if again != "o":
                break

    asyncio.run(add_table_to_project(args.project_id, new_tables))


if __name__ == "__main__":
    main()
