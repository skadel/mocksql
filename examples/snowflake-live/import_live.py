"""Reproduit le flux d'import schema du SERVEUR pour Snowflake, en CLI.

Le CLI `mocksql refresh-schemas` est BigQuery-only ; l'import Snowflake live
(fetch_tables_schema_snowflake) n'est cable que dans l'endpoint serveur. Ce script
appelle exactement la meme chaine que le serveur :
  refs (depuis le SQL) -> fetch_tables_schema_snowflake (INFORMATION_SCHEMA live)
  -> generate_tables_and_columns_from_project_schema -> .mocksql/schema_cache.json

=> teste le VRAI chemin d'import Snowflake sur un compte live.
"""

import asyncio
import glob
import json
import os
from pathlib import Path

from dotenv import load_dotenv

BACK = Path(r"C:/Users/skhir/workspace/mocksql/back")
load_dotenv(BACK / ".env", override=True)  # avant l'import des modules MockSQL

import sqlglot  # noqa: E402
from sqlglot import exp  # noqa: E402

HERE = Path(__file__).parent
MODELS = HERE / "models"
OUT = HERE / ".mocksql" / "schema_cache.json"


def model_refs() -> list[str]:
    refs: set[str] = set()
    for f in glob.glob(str(MODELS / "*.sql")):
        p = sqlglot.parse_one(Path(f).read_text(encoding="utf-8"), read="snowflake")
        ctes = {c.alias_or_name.upper() for c in p.find_all(exp.CTE)}
        for t in p.find_all(exp.Table):
            parts = [x.name for x in (t.args.get("catalog"), t.args.get("db"), t.this) if x]
            if len(parts) == 3:
                refs.add(".".join(parts))
            elif len(parts) == 1 and parts[0].upper() not in ctes:
                print(f"  [warn] table non qualifiee ignoree: {parts[0]}")
    return sorted(refs)


async def main() -> None:
    from build_query.schema_fetcher import fetch_tables_schema_snowflake
    from utils.schema_utils import generate_tables_and_columns_from_project_schema

    refs = model_refs()
    print(f"refs a importer ({len(refs)}): {refs}")

    schema_rows, failed = await fetch_tables_schema_snowflake(refs)
    print(f"INFORMATION_SCHEMA rows: {len(schema_rows)} | failed: {failed}")
    if not schema_rows:
        print("[ERREUR] aucune colonne renvoyee par Snowflake")
        return

    cache = generate_tables_and_columns_from_project_schema({"data": schema_rows})
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(cache, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[OK] cache ecrit: {OUT} ({len(cache)} tables)")

    # apercu types tels qu'importes (focus NUMBER)
    for tbl in cache:
        print(f"\n== {tbl['table_name']} ==")
        for col in tbl["columns"][:6]:
            print(f"   {col['name']:16} type={col['type']:14} bq_ddl={col.get('bq_ddl_type','<absent>')}")


if __name__ == "__main__":
    asyncio.run(main())
