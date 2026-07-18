from typing import get_args

import pytest
from pydantic import ValidationError

from utils.examples import create_pydantic_models


def _row_cls(model, table):
    """Descend Optional[list[RowModel]] → RowModel."""
    row_model = model.model_fields[table].annotation
    inner = get_args(row_model)[0]  # list[RowModel]
    return get_args(inner)[0]  # RowModel


def _field_description(model, table, col):
    """Descend Optional[list[RowModel]] → RowModel et rend la description d'un champ."""
    row_model = model.model_fields[table].annotation
    inner = get_args(row_model)[0]  # list[RowModel]
    row_cls = get_args(inner)[0]  # RowModel
    return row_cls.model_fields[col].description


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
# Leading-underscore columns (dbt metadata cols : _line_number, _dt, …)
# ---------------------------------------------------------------------------


def test_leading_underscore_column_does_not_crash():
    """Pydantic refuse les noms de champ à underscore initial (attribut privé).
    Les projets dbt en débordent (`_line_number`, `_dt`, `_feed_valid_from`).
    create_pydantic_models doit les accepter via alias, sans NameError."""
    model = create_pydantic_models(
        [_table("T", _col("_line_number", "INTEGER"), _col("agency_id", "STRING"))]
    )
    inst = model(T=[{"_line_number": 5, "agency_id": "AC"}])
    assert len(inst.T) == 1


def test_leading_underscore_column_roundtrips_real_name_on_dump():
    """Le générateur fait `model.dict()` puis utilise les clés comme noms de
    colonnes DuckDB : le dump DOIT ré-émettre `_line_number`, pas un nom assaini."""
    model = create_pydantic_models([_table("T", _col("_line_number", "INTEGER"))])
    dumped = model(T=[{"_line_number": 5}]).model_dump()
    assert dumped["T"][0] == {"_line_number": 5}


def test_underscore_and_plain_variant_coexist():
    """`_x` et `x` dans la même table ne doivent pas s'écraser (collision d'alias)."""
    model = create_pydantic_models(
        [_table("T", _col("_dt", "STRING"), _col("dt", "STRING"))]
    )
    dumped = model(T=[{"_dt": "a", "dt": "b"}]).model_dump()
    assert dumped["T"][0] == {"_dt": "a", "dt": "b"}


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


# ---------------------------------------------------------------------------
# Rappel ISO sur les champs typés date/timestamp (P1-3, retry Pydantic c2)
# ---------------------------------------------------------------------------


def test_date_field_carries_iso_hint():
    """Un champ typé DATE porte un rappel ISO dans sa description — il survit au retry
    Pydantic (schéma + erreur seuls, sans system prompt)."""
    model = create_pydantic_models([_table("T", _col("partition_date", "DATE"))])
    desc = _field_description(model, "T", "partition_date")
    assert desc and "ISO" in desc and "YYYY-MM-DD" in desc


def test_timestamp_field_carries_iso_hint():
    model = create_pydantic_models([_table("T", _col("event_ts", "TIMESTAMP"))])
    desc = _field_description(model, "T", "event_ts")
    assert desc and "ISO" in desc


def test_string_field_has_no_iso_hint():
    """Une colonne TEXTE (que le SQL peut parser via PARSE_DATE) ne reçoit PAS le rappel
    ISO : sa valeur doit respecter le format attendu par le SQL, pas ISO."""
    model = create_pydantic_models([_table("T", _col("date_str", "STRING"))])
    desc = _field_description(model, "T", "date_str")
    assert not desc or "ISO" not in desc


def test_date_hint_appended_to_existing_description():
    model = create_pydantic_models(
        [_table("T", _col("d", "DATE", description="Date d'ouverture"))]
    )
    desc = _field_description(model, "T", "d")
    assert desc.startswith("Date d'ouverture")
    assert "YYYY-MM-DD" in desc


# ---------------------------------------------------------------------------
# Colonne NUMBER au nom horodaté = epoch, pas une date ISO (incident sf_bq028)
#
# Un type Snowflake `NUMBER` n'est pas dans `type_mapping` → le champ retombe sur
# `str`, donc un littéral date ISO ('2024-01-01T00:00:00') passe la validation
# Pydantic sans broncher… puis DuckDB (colonne DECIMAL) le rejette :
# « Could not convert string "2024-01-01T00:00:00" to DECIMAL(38,9) » → INSERT
# échoués, données dégradées en NULL. Le nom (`SnapshotAt`, `UpstreamPublishedAt`)
# induit le LLM en erreur ; on injecte un rappel « epoch → entier » dans la
# description (seul canal survivant au retry).
# ---------------------------------------------------------------------------


def test_number_timestamp_named_col_carries_epoch_hint():
    """Colonne NUMBER dont le nom implique un horodatage → rappel epoch (entier),
    surtout PAS un rappel date ISO."""
    model = create_pydantic_models([_table("T", _col("SnapshotAt", "NUMBER"))])
    desc = _field_description(model, "T", "snapshotat")
    assert desc and "epoch" in desc.lower()
    # Pas le rappel DATE ISO (dont le marqueur est le format YYYY-MM-DD).
    assert "YYYY-MM-DD" not in desc


def test_number_camelcase_published_at_carries_epoch_hint():
    model = create_pydantic_models([_table("T", _col("UpstreamPublishedAt", "NUMBER"))])
    desc = _field_description(model, "T", "upstreampublishedat")
    assert desc and "epoch" in desc.lower()


def test_number_snake_created_at_carries_epoch_hint():
    model = create_pydantic_models([_table("T", _col("created_at", "NUMBER(38,0)"))])
    desc = _field_description(model, "T", "created_at")
    assert desc and "epoch" in desc.lower()


def test_number_non_temporal_name_has_no_epoch_hint():
    """Un NUMBER au nom non-horodaté (StarsCount) ne reçoit AUCUN rappel epoch."""
    model = create_pydantic_models([_table("T", _col("StarsCount", "NUMBER"))])
    desc = _field_description(model, "T", "starscount")
    assert not desc or "epoch" not in desc.lower()


def test_string_ending_in_at_no_epoch_hint():
    """Garde anti-faux-positif : `format` (TEXTE) se termine par 'at' mais n'est ni
    numérique ni un token temporel → aucun rappel epoch."""
    model = create_pydantic_models([_table("T", _col("format", "STRING"))])
    desc = _field_description(model, "T", "format")
    assert not desc or "epoch" not in desc.lower()


def test_date_typed_at_col_keeps_iso_hint_not_epoch():
    """Une colonne TYPÉE date/timestamp nommée `...At` garde le rappel ISO : le rappel
    epoch est réservé aux colonnes NUMÉRIQUES (gate sur le type, pas seulement le nom)."""
    model = create_pydantic_models([_table("T", _col("EventAt", "TIMESTAMP"))])
    desc = _field_description(model, "T", "eventat")
    assert desc and "ISO" in desc and "YYYY-MM-DD" in desc
    assert "epoch" not in desc.lower()


def test_sql_format_directive_suppresses_epoch_hint():
    """Arbitrage : quand le SQL impose un format sur la colonne (directive posée par
    le générateur, ex. TO_DATE(...,'YYYYMMDD') sur sf_bq216), le hint epoch — simple
    heuristique de nom — doit s'effacer. Sinon le prompt porte deux prescriptions
    contradictoires et le LLM tranche au hasard (coin-flip constaté en éval)."""
    col = _col(
        "filing_date",
        "NUMBER",
        description="(⚠️ format imposé par la requête : TO_DATE('%Y%m%d'))",
    )
    col["sql_format_directive"] = True
    model = create_pydantic_models([_table("T", col)])
    desc = _field_description(model, "T", "filing_date")
    assert "epoch" not in desc.lower()
    assert "%Y%m%d" in desc


def test_no_directive_keeps_epoch_hint():
    """Sans directive SQL, le hint epoch reste le défaut des NUMBER temporels."""
    col = _col("filing_date", "NUMBER")
    col["sql_format_directive"] = False
    model = create_pydantic_models([_table("T", col)])
    desc = _field_description(model, "T", "filing_date")
    assert desc and "epoch" in desc.lower()


def test_epoch_hint_appended_to_existing_description():
    model = create_pydantic_models(
        [_table("T", _col("SnapshotAt", "NUMBER", description="Instant du snapshot"))]
    )
    desc = _field_description(model, "T", "snapshotat")
    assert desc.startswith("Instant du snapshot")
    assert "epoch" in desc.lower()


# ---------------------------------------------------------------------------
# Typage NUMBER/NUMERIC/DECIMAL (root-cause spider2-snow) : le contrat Pydantic
# doit porter un signal NUMÉRIQUE. Avant : ces types (absents de `type_mapping`)
# retombaient sur `str` → schéma JSON sans signal → le LLM collait des ids
# alphanumériques ("M001") ou des mots dans des colonnes DECIMAL, et l'INSERT
# DuckDB était rejeté (silencieusement, cf. test_insert_failure_surfacing).
# ---------------------------------------------------------------------------


def test_number_field_rejects_alphanumeric_id():
    """`"M001"` dans un `match_id NUMBER` doit échouer dès la validation Pydantic —
    plus jamais au moment de l'INSERT DuckDB (rejet tardif et jadis silencieux)."""
    model = create_pydantic_models([_table("T", _col("match_id", "NUMBER"))])
    with pytest.raises(ValidationError):
        model(T=[{"match_id": "M001"}])


def test_number_field_annotation_carries_numeric_signal():
    """Le schéma envoyé au LLM doit annoncer un champ numérique, pas `string`."""
    model = create_pydantic_models([_table("T", _col("match_id", "NUMBER"))])
    args = get_args(_row_cls(model, "T").model_fields["match_id"].annotation)
    assert str not in args
    assert int in args and float in args


def test_bare_number_accepts_int_and_float():
    model = create_pydantic_models([_table("T", _col("x", "NUMBER"))])
    inst = model(T=[{"x": 1}, {"x": 2.5}])
    assert inst.T[0].x == 1
    assert inst.T[1].x == 2.5


def test_bare_number_keeps_large_int_exact():
    """Epoch µs / wei : forcer float perdrait la précision au-delà de 2^53 —
    la forme sans précision reste `int | float` (l'entier gagne s'il est entier)."""
    model = create_pydantic_models([_table("T", _col("wei_value", "NUMBER"))])
    big = 2_000_000_000_000_000_003
    inst = model(T=[{"wei_value": big}])
    assert inst.T[0].wei_value == big
    assert isinstance(inst.T[0].wei_value, int)


def test_number_scale_zero_is_int():
    model = create_pydantic_models([_table("T", _col("n", "NUMBER(38,0)"))])
    assert model(T=[{"n": 42}]).T[0].n == 42
    with pytest.raises(ValidationError):
        model(T=[{"n": 2.5}])


def test_number_precision_only_is_int():
    """`NUMBER(10)` (Snowflake) = scale 0 implicite → int."""
    model = create_pydantic_models([_table("T", _col("n", "NUMBER(10)"))])
    assert model(T=[{"n": 7}]).T[0].n == 7
    with pytest.raises(ValidationError):
        model(T=[{"n": "abc"}])


def test_number_with_scale_is_float():
    model = create_pydantic_models([_table("T", _col("price", "NUMBER(12,2)"))])
    assert model(T=[{"price": 10.5}]).T[0].price == 10.5
    with pytest.raises(ValidationError):
        model(T=[{"price": "dix"}])


def test_decimal_family_variants_reject_text():
    """NUMERIC/DECIMAL/BIGNUMERIC (+ casse Trino, + précision) : tous numériques."""
    for type_str in ("NUMERIC", "DECIMAL", "BIGNUMERIC", "number(18,3)", "decimal"):
        model = create_pydantic_models([_table("T", _col("v", type_str))])
        with pytest.raises(ValidationError):
            model(T=[{"v": "cost"}])
        assert model(T=[{"v": 1.5}]).T[0].v == 1.5


def test_iso_date_literal_rejected_on_number_column():
    """Symétrique de l'incident sf_bq028 : un littéral date ISO sur une colonne NUMBER
    au nom horodaté est maintenant rejeté dès la validation (avant : champ `str`,
    rejet tardif à l'INSERT DuckDB)."""
    model = create_pydantic_models([_table("T", _col("SnapshotAt", "NUMBER"))])
    with pytest.raises(ValidationError):
        model(T=[{"snapshotat": "2024-01-01T00:00:00"}])
    assert model(T=[{"snapshotat": 1704067200000000}]).T[0].snapshotat


def test_bq_ddl_numeric_is_numeric_too():
    """Le chemin bq_ddl_type partage le même résolveur scalaire."""
    model = create_pydantic_models([_table("T", _col("amount", bq_ddl_type="NUMERIC"))])
    with pytest.raises(ValidationError):
        model(T=[{"amount": "beaucoup"}])
    assert model(T=[{"amount": 12.34}]).T[0].amount == 12.34


def test_unknown_type_still_falls_back_to_str():
    """Types inconnus (GEOGRAPHY, VARIANT…) : fallback `str` inchangé."""
    model = create_pydantic_models([_table("T", _col("geo", "GEOGRAPHY"))])
    assert model(T=[{"geo": "POINT(1 2)"}]).T[0].geo == "POINT(1 2)"
