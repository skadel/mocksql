"""Construit examples/spider2-snow/.mocksql/schema_cache.json depuis les DDL.csv
de Spider2-snow, en gardant les types **Snowflake natifs** (tels qu'un
introspecteur INFORMATION_SCHEMA de MockSQL les produirait) — pas de
transpilation vers BigQuery.

Pour chaque modèle .sql de models/, on extrait les tables 3-parties
(DB.SCHEMA.TABLE), on retrouve leur DDL dans resource/databases/<DB>/<SCHEMA>/
DDL.csv, et on émet une entrée de cache au format MockSQL :
    {table_name: "DB.SCHEMA.TABLE", description, columns: [{name, type, mode}]}
où `type` est le DATA_TYPE Snowflake canonique (TEXT/NUMBER/FLOAT/VARIANT/...),
sans `bq_ddl_type` — exactement ce que fetch_tables_schema_snowflake stocke.
"""

import csv
import glob
import json
import os
import sys

import sqlglot
from sqlglot import exp

csv.field_size_limit(10**7)

SPIDER_ROOT = r"C:/Users/skhir/workspace/Spider2/spider2-snow"
HERE = os.path.dirname(os.path.abspath(__file__))
MODELS_DIR = os.path.join(HERE, "models")
OUT = os.path.join(HERE, ".mocksql", "schema_cache.json")
DB_ROOT = os.path.join(SPIDER_ROOT, "resource", "databases")

# sqlglot DataType.Type -> DATA_TYPE canonique Snowflake (INFORMATION_SCHEMA).
# Resolu par NOM pour survivre aux variations de membres entre versions sqlglot.
T = exp.DataType.Type
_NAME_TO_SNOW = {
    "TEXT": "TEXT", "VARCHAR": "TEXT", "CHAR": "TEXT", "NCHAR": "TEXT",
    "NVARCHAR": "TEXT", "STRING": "TEXT",
    "INT": "NUMBER", "BIGINT": "NUMBER", "SMALLINT": "NUMBER",
    "TINYINT": "NUMBER", "MEDIUMINT": "NUMBER",
    "DECIMAL": "NUMBER", "BIGDECIMAL": "NUMBER",
    "FLOAT": "FLOAT", "DOUBLE": "FLOAT",
    "BOOLEAN": "BOOLEAN",
    "DATE": "DATE", "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP_NTZ", "DATETIME": "TIMESTAMP_NTZ",
    "TIMESTAMPNTZ": "TIMESTAMP_NTZ",
    "TIMESTAMPLTZ": "TIMESTAMP_LTZ", "TIMESTAMPTZ": "TIMESTAMP_TZ",
    "VARIANT": "VARIANT", "JSON": "VARIANT",
    "STRUCT": "OBJECT", "OBJECT": "OBJECT", "MAP": "OBJECT",
    "ARRAY": "ARRAY",
    "BINARY": "BINARY", "VARBINARY": "BINARY",
    "GEOGRAPHY": "GEOGRAPHY", "GEOMETRY": "GEOMETRY",
}
SNOW_TYPE = {
    getattr(T, name): snow
    for name, snow in _NAME_TO_SNOW.items()
    if hasattr(T, name)
}

unmapped: set = set()


def snow_data_type(kind: exp.DataType | None) -> str:
    if kind is None:
        return "TEXT"
    mapped = SNOW_TYPE.get(kind.this)
    if mapped is None:
        unmapped.add(str(kind.this))
        return kind.this.name
    return mapped


def model_tables(sql: str) -> set[tuple[str, str, str]]:
    p = sqlglot.parse_one(sql, read="snowflake")
    ctes = {c.alias_or_name.upper() for c in p.find_all(exp.CTE)}
    out: set[tuple[str, str, str]] = set()
    for t in p.find_all(exp.Table):
        parts = [x.name for x in (t.args.get("catalog"), t.args.get("db"), t.this) if x]
        if len(parts) == 3:
            out.add(tuple(parts))  # garde la casse d'origine
        elif len(parts) == 1 and parts[0].upper() not in ctes:
            print(f"  [warn] table 1-partie non qualifiee, ignoree: {parts[0]}")
    return out


def build_ddl_index() -> dict[tuple[str, str, str], str]:
    """(DB,SCHEMA,TABLE) majuscules -> DDL string."""
    idx: dict[tuple[str, str, str], str] = {}
    for ddl_csv in glob.glob(os.path.join(DB_ROOT, "*", "*", "DDL.csv")):
        parts = ddl_csv.replace("\\", "/").split("/")
        db, sch = parts[-3], parts[-2]
        with open(ddl_csv, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                key = (db.upper(), sch.upper(), row["table_name"].upper())
                idx[key] = row["DDL"]
    return idx


def columns_from_ddl(ddl: str) -> list[dict]:
    parsed = sqlglot.parse_one(ddl, read="snowflake")
    cols = []
    for c in parsed.find_all(exp.ColumnDef):
        cols.append(
            {
                "name": c.this.name,  # casse d'origine (quoted => preservee)
                "type": snow_data_type(c.args.get("kind")),
                "mode": "NULLABLE",
                "description": "",
            }
        )
    return cols


def main() -> None:
    ddl_idx = build_ddl_index()
    print(f"DDL index: {len(ddl_idx)} tables")

    wanted: dict[tuple[str, str, str], tuple[str, str, str]] = {}  # upper -> orig
    for sql_path in sorted(glob.glob(os.path.join(MODELS_DIR, "*.sql"))):
        name = os.path.basename(sql_path)
        sql = open(sql_path, encoding="utf-8").read()
        try:
            tabs = model_tables(sql)
        except Exception as e:
            print(f"[PARSE-ERR] {name}: {e}")
            continue
        for orig in tabs:
            wanted[tuple(p.upper() for p in orig)] = orig
        print(f"{name}: {len(tabs)} tables")

    cache = []
    missing = []
    for up, orig in sorted(wanted.items()):
        ddl = ddl_idx.get(up)
        if ddl is None:
            missing.append(".".join(orig))
            continue
        cache.append(
            {
                "table_name": ".".join(orig),
                "description": "",
                "columns": columns_from_ddl(ddl),
            }
        )

    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

    print(f"\n-> {len(cache)} tables ecrites dans {OUT}")
    if missing:
        print(f"[MISSING DDL] {len(missing)}: {missing}")
    if unmapped:
        print(f"[UNMAPPED TYPES] {sorted(unmapped)}")
    if missing:
        sys.exit(1)


if __name__ == "__main__":
    main()
