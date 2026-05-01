from typing import List, Optional

from pydantic import BaseModel


class ColumnSchema(BaseModel):
    table_catalog: str
    table_schema: str
    table_name: str
    field_path: str
    primary_key: bool = False
    data_type: str
    mode: str = "NULLABLE"
    description: Optional[str] = ""
    categorical: bool = False
    table_description: Optional[str] = None


class ProjectSchema(BaseModel):
    data: List[ColumnSchema]
