from typing import List, Optional

from pydantic import BaseModel

from utils.sqlglot_patches import apply_duckdb_date_parse_patches

# Le générateur DuckDB de sqlglot est un singleton de process : le patcher ici
# garantit qu'il l'est avant le premier rendu `write="duckdb"`, quel que soit le
# point d'entrée (serveur FastAPI ou CLI). Tous les modules qui rendent du
# DuckDB importent `utils`, donc passent par cet `__init__`.
apply_duckdb_date_parse_patches()


class ColumnSchema(BaseModel):
    table_catalog: str
    table_schema: str
    table_name: str
    field_path: str
    primary_key: bool = False
    data_type: str
    bq_ddl_type: str = ""
    mode: str = "NULLABLE"
    description: Optional[str] = ""
    categorical: bool = False
    table_description: Optional[str] = None


class ProjectSchema(BaseModel):
    data: List[ColumnSchema]
