import pytest

from utils.examples import create_pydantic_models


def test_create_pydantic_models():
    # Sample input data
    filtered_tables_and_columns = [
        {
            "table_name": "Table1",
            "columns": [
                {"name": "id", "type": "integer", "description": "The ID of the row"},
                {
                    "name": "name",
                    "type": "string",
                    "description": "The name of the entity",
                },
            ],
        },
        {
            "table_name": "Table2",
            "columns": [
                {
                    "name": "code",
                    "type": "string",
                    "description": "The code of the item",
                },
                {
                    "name": "value",
                    "type": "integer",
                    "description": "The value of the item",
                },
            ],
        },
    ]

    # Call the function
    CombinedModel = create_pydantic_models(filtered_tables_and_columns)
    data = {
        "Table1": [{"id": "1", "name": "Entity1"}, {"id": "2", "name": "Entity2"}],
        "Table2": [{"code": "A1", "value": "100"}, {"code": "B2", "value": "200"}],
    }

    combined_model = CombinedModel(**data)
    # Validate the CombinedModel
    assert hasattr(combined_model, "Table1"), (
        "CombinedModel should have attribute 'Table1'"
    )
    assert hasattr(combined_model, "Table2"), (
        "CombinedModel should have attribute 'Table2'"
    )
    assert isinstance(combined_model.Table1, list), "Table1 should be a list"
    assert isinstance(combined_model.Table2, list), "Table2 should be a list"
    assert len(combined_model.Table1) == 2, "Table1 should have 2 items"
    assert len(combined_model.Table2) == 2, "Table2 should have 2 items"
    assert combined_model.Table1[0].id == "1", "Table1[0].id should be 1"
    assert combined_model.Table1[0].name == "Entity1", (
        "Table1[0].name should be 'Entity1'"
    )
    assert combined_model.Table1[1].id == "2", "Table1[1].id should be 2"
    assert combined_model.Table1[1].name == "Entity2", (
        "Table1[1].name should be 'Entity2'"
    )


if __name__ == "__main__":
    pytest.main()
