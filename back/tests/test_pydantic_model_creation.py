from typing import get_args


from utils.examples import create_pydantic_models


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _table(name, *cols):
    """Build a minimal table dict."""
    return {"table_name": name, "columns": list(cols)}


def _col(name, type_=None, bq_ddl_type=None, description=None):
    c = {"name": name}
    if type_ is not None:
        c["type"] = type_
    if bq_ddl_type is not None:
        c["bq_ddl_type"] = bq_ddl_type
    if description is not None:
        c["description"] = description
    return c


# ---------------------------------------------------------------------------
# Basic scalar types (type field, no bq_ddl_type)
# ---------------------------------------------------------------------------


def test_basic_scalar_types():
    model = create_pydantic_models(
        [_table("T", _col("id", "INTEGER"), _col("name", "STRING"))]
    )
    instance = model(T=[{"id": 1, "name": "alice"}])
    assert len(instance.T) == 1
    assert instance.T[0].name == "alice"


def test_two_tables_combined_model():
    model = create_pydantic_models(
        [
            _table("T1", _col("id", "INTEGER")),
            _table("T2", _col("code", "STRING"), _col("value", "INTEGER")),
        ]
    )
    data = {"T1": [{"id": 1}], "T2": [{"code": "A", "value": 10}]}
    inst = model(**data)
    assert hasattr(inst, "T1") and hasattr(inst, "T2")
    assert inst.T2[0].code == "A"


# ---------------------------------------------------------------------------
# bq_ddl_type — scalar
# ---------------------------------------------------------------------------


def test_bq_ddl_scalar_int64():
    model = create_pydantic_models([_table("T", _col("qty", bq_ddl_type="INT64"))])
    inst = model(T=[{"qty": 42}])
    assert inst.T[0].qty == 42


def test_bq_ddl_scalar_string():
    model = create_pydantic_models([_table("T", _col("label", bq_ddl_type="STRING"))])
    inst = model(T=[{"label": "hello"}])
    assert inst.T[0].label == "hello"


# ---------------------------------------------------------------------------
# bq_ddl_type — ARRAY<scalar>
# ---------------------------------------------------------------------------


def test_bq_ddl_array_of_string():
    model = create_pydantic_models(
        [_table("T", _col("tags", bq_ddl_type="ARRAY<STRING>"))]
    )
    inst = model(T=[{"tags": ["a", "b"]}])
    assert inst.T[0].tags == ["a", "b"]


def test_bq_ddl_array_of_int64():
    model = create_pydantic_models(
        [_table("T", _col("scores", bq_ddl_type="ARRAY<INT64>"))]
    )
    inst = model(T=[{"scores": [1, 2, 3]}])
    assert inst.T[0].scores == [1, 2, 3]


# ---------------------------------------------------------------------------
# bq_ddl_type — STRUCT
# ---------------------------------------------------------------------------


def test_bq_ddl_struct_creates_nested_model():
    model = create_pydantic_models(
        [_table("T", _col("address", bq_ddl_type="STRUCT<city STRING, zip INT64>"))]
    )
    inst = model(T=[{"address": {"city": "Paris", "zip": 75001}}])
    assert inst.T[0].address.city == "Paris"
    assert inst.T[0].address.zip == 75001


def test_bq_ddl_struct_subfields_are_optional():
    model = create_pydantic_models(
        [_table("T", _col("info", bq_ddl_type="STRUCT<a STRING, b INT64>"))]
    )
    # All subfields Optional — partial data must work
    inst = model(T=[{"info": {"a": "x"}}])
    assert inst.T[0].info.a == "x"
    assert inst.T[0].info.b is None


# ---------------------------------------------------------------------------
# bq_ddl_type — ARRAY<STRUCT<...>>
# ---------------------------------------------------------------------------


def test_bq_ddl_array_of_struct():
    model = create_pydantic_models(
        [_table("T", _col("items", bq_ddl_type="ARRAY<STRUCT<id INT64, name STRING>>"))]
    )
    inst = model(T=[{"items": [{"id": 1, "name": "foo"}, {"id": 2, "name": "bar"}]}])
    assert len(inst.T[0].items) == 2
    assert inst.T[0].items[0].name == "foo"
    assert inst.T[0].items[1].id == 2


def test_bq_ddl_nested_struct():
    model = create_pydantic_models(
        [
            _table(
                "T",
                _col(
                    "meta", bq_ddl_type="STRUCT<outer STRING, inner STRUCT<val INT64>>"
                ),
            )
        ]
    )
    inst = model(T=[{"meta": {"outer": "x", "inner": {"val": 7}}}])
    assert inst.T[0].meta.outer == "x"
    assert inst.T[0].meta.inner.val == 7


# ---------------------------------------------------------------------------
# Optional / None behaviour
# ---------------------------------------------------------------------------


def test_all_fields_optional_allows_none():
    model = create_pydantic_models(
        [_table("T", _col("id", "INTEGER"), _col("name", "STRING"))]
    )
    # Every field is Optional — None is valid
    inst = model(T=[{"id": None, "name": None}])
    assert inst.T[0].id is None
    assert inst.T[0].name is None


def test_table_list_itself_is_optional():
    model = create_pydantic_models([_table("T", _col("id", "INTEGER"))])
    # Omitting a table entirely is valid (Optional[list[...]])
    inst = model()
    assert inst.T is None


# ---------------------------------------------------------------------------
# Column names are lowercased
# ---------------------------------------------------------------------------


def test_column_names_lowercased():
    model = create_pydantic_models(
        [_table("T", _col("MyCol", "STRING"), _col("UPPER", "INTEGER"))]
    )
    inst = model(T=[{"mycol": "v", "upper": 1}])
    assert inst.T[0].mycol == "v"
    assert inst.T[0].upper == 1


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_input_returns_model_with_no_tables():
    model = create_pydantic_models([])
    inst = model()
    assert inst is not None


def test_table_with_no_columns():
    model = create_pydantic_models([_table("Empty")])
    inst = model(Empty=[{}])
    assert isinstance(inst.Empty, list)


def test_description_attached_to_field():
    model = create_pydantic_models(
        [_table("T", _col("id", "INTEGER", description="Primary key"))]
    )
    field_info = model.model_fields["T"]
    assert field_info.description == "Model for table "
    # Inner row model carries the column description
    row_model = model.model_fields["T"].annotation
    # unwrap Optional[list[RowModel]] → list[RowModel] → RowModel
    inner = get_args(row_model)[0]  # list[RowModel]
    row_cls = get_args(inner)[0]  # RowModel
    assert row_cls.model_fields["id"].description == "Primary key"
