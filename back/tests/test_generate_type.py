import unittest
from typing import get_args

from pydantic import BaseModel

from utils.examples import filter_columns, create_pydantic_models

# Assuming the functions and models from the previous code are available here
# filter_columns and CombinedModel creation

# Sample data for testing
tables_and_columns = [
    {
        "table_name": "bigquery-public-data.thelook_ecommerce.products",
        "columns": [
            {
                "name": "id",
                "type": "INTEGER",
                "description": "Unique identifier for each product.",
                "example": "1",
            },
            {
                "name": "cost",
                "type": "FLOAT",
                "description": "Cost of the product.",
                "example": "10.0",
            },
            {
                "name": "category",
                "type": "STRING",
                "description": "Category of the product.",
                "example": "Electronics",
            },
            {
                "name": "name",
                "type": "STRING",
                "description": "Name of the product.",
                "example": "Laptop",
            },
            {
                "name": "brand",
                "type": "STRING",
                "description": "Brand of the product.",
                "example": "Brand A",
            },
            {
                "name": "retail_price",
                "type": "FLOAT",
                "description": "Retail price of the product.",
            },
            {
                "name": "department",
                "type": "STRING",
                "description": "Department associated with the product.",
                "example": "Electronics",
            },
            {
                "name": "sku",
                "type": "STRING",
                "description": "Stock Keeping Unit of the product.",
                "example": "SKU123",
            },
            {
                "name": "distribution_center_id",
                "type": "INTEGER",
                "description": "Identifier of the distribution center that stocks the product.",
                "example": "1",
            },
        ],
    },
    {
        "table_name": "bigquery-public-data.thelook_ecommerce.orders",
        "columns": [
            {
                "name": "order_id",
                "type": "INTEGER",
                "description": "Unique identifier for each order.",
                "example": "1",
            },
            {
                "name": "user_id",
                "type": "INTEGER",
                "description": "Identifier of the user who placed the order.",
                "example": "123",
            },
            {
                "name": "status",
                "type": "STRING",
                "description": "Current status of the order (e.g., processing, shipped).",
                "example": "shipped",
            },
            {
                "name": "gender",
                "type": "STRING",
                "description": "Gender of the user who placed the order.",
                "example": "Male",
            },
            {
                "name": "created_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order was created.",
                "example": "2022-01-01 12:00:00",
            },
            {
                "name": "returned_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order was returned, if applicable.",
                "example": "2022-01-02 12:00:00",
            },
            {
                "name": "shipped_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order was shipped.",
                "example": "2022-01-03 12:00:00",
            },
            {
                "name": "delivered_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order was delivered.",
                "example": "2022-01-04 12:00:00",
            },
            {
                "name": "num_of_item",
                "type": "INTEGER",
                "description": "Number of items included in the order.",
                "example": "2",
            },
        ],
    },
    {
        "table_name": "bigquery-public-data.thelook_ecommerce.order_items",
        "columns": [
            {
                "name": "id",
                "type": "INTEGER",
                "description": "Unique identifier for each order item.",
                "example": "1",
            },
            {
                "name": "order_id",
                "type": "INTEGER",
                "description": "Identifier of the order associated with the order item.",
                "example": "123",
            },
            {
                "name": "user_id",
                "type": "INTEGER",
                "description": "Identifier of the user who placed the order.",
                "example": "123",
            },
            {
                "name": "product_id",
                "type": "INTEGER",
                "description": "Identifier of the product ordered.",
                "example": "123",
            },
            {
                "name": "inventory_item_id",
                "type": "INTEGER",
                "description": "Identifier of the inventory item sold.",
                "example": "1",
            },
            {
                "name": "status",
                "type": "STRING",
                "description": "Current status of the order item (e.g., shipped, delivered).",
                "example": "shipped",
            },
            {
                "name": "created_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order item was created.",
                "example": "2022-01-01 12:00:00",
            },
            {
                "name": "shipped_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order item was shipped.",
                "example": "2022-01-02 12:00:00",
            },
            {
                "name": "delivered_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order item was delivered.",
                "example": "2022-01-03 12:00:00",
            },
            {
                "name": "returned_at",
                "type": "TIMESTAMP",
                "description": "Timestamp when the order item was returned, if applicable.",
                "example": "2022-01-04 12:00:00",
            },
            {
                "name": "sale_price",
                "type": "FLOAT",
                "description": "Sale price of the order item.",
                "example": "15.0",
            },
        ],
    },
]

used_columns = [
    {
        "database": "thelook_ecommerce",
        "table": "products",
        "used_columns": ["brand", "category", "id", "name"],
    },
    {
        "database": "thelook_ecommerce",
        "table": "orders",
        "used_columns": ["created_at", "gender", "order_id"],
    },
    {
        "database": "thelook_ecommerce",
        "table": "order_items",
        "used_columns": ["order_id", "product_id", "sale_price"],
    },
]


class TestFunctions(unittest.TestCase):
    def test_filter_columns(self):
        expected_filtered_tables = [
            {
                "columns": [
                    {
                        "description": "Unique identifier for each product.",
                        "example": "1",
                        "name": "id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Category of the product.",
                        "example": "Electronics",
                        "name": "category",
                        "type": "STRING",
                    },
                    {
                        "description": "Name of the product.",
                        "example": "Laptop",
                        "name": "name",
                        "type": "STRING",
                    },
                    {
                        "description": "Brand of the product.",
                        "example": "Brand A",
                        "name": "brand",
                        "type": "STRING",
                    },
                ],
                "table_name": "thelook_ecommerce_products",
            },
            {
                "columns": [
                    {
                        "description": "Unique identifier for each order.",
                        "example": "1",
                        "name": "order_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Gender of the user who placed the order.",
                        "example": "Male",
                        "name": "gender",
                        "type": "STRING",
                    },
                    {
                        "description": "Timestamp when the order was created.",
                        "example": "2022-01-01 12:00:00",
                        "name": "created_at",
                        "type": "TIMESTAMP",
                    },
                ],
                "table_name": "thelook_ecommerce_orders",
            },
            {
                "columns": [
                    {
                        "description": "Identifier of the order associated with the "
                        "order item.",
                        "example": "123",
                        "name": "order_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Identifier of the product ordered.",
                        "example": "123",
                        "name": "product_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Sale price of the order item.",
                        "example": "15.0",
                        "name": "sale_price",
                        "type": "FLOAT",
                    },
                ],
                "table_name": "thelook_ecommerce_order_items",
            },
        ]

        actual_filtered_tables = filter_columns(tables_and_columns, used_columns)
        self.assertEqual(actual_filtered_tables, expected_filtered_tables)

    def test_combined_model_fields(self):
        filtered_tables = [
            {
                "columns": [
                    {
                        "description": "Unique identifier for each product.",
                        "example": "1",
                        "name": "id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Category of the product.",
                        "example": "Electronics",
                        "name": "category",
                        "type": "STRING",
                    },
                    {
                        "description": "Name of the product.",
                        "example": "Laptop",
                        "name": "name",
                        "type": "STRING",
                    },
                    {
                        "description": "Brand of the product.",
                        "example": "Brand A",
                        "name": "brand",
                        "type": "STRING",
                    },
                ],
                "table_name": "products",
            },
            {
                "columns": [
                    {
                        "description": "Unique identifier for each order.",
                        "example": "1",
                        "name": "order_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Gender of the user who placed the order.",
                        "example": "Male",
                        "name": "gender",
                        "type": "STRING",
                    },
                    {
                        "description": "Timestamp when the order was created.",
                        "example": "2022-01-01 12:00:00",
                        "name": "created_at",
                        "type": "TIMESTAMP",
                    },
                ],
                "table_name": "orders",
            },
            {
                "columns": [
                    {
                        "description": "Identifier of the order associated with the "
                        "order item.",
                        "example": "123",
                        "name": "order_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Identifier of the product ordered.",
                        "example": "123",
                        "name": "product_id",
                        "type": "INTEGER",
                    },
                    {
                        "description": "Sale price of the order item.",
                        "example": "15.0",
                        "name": "sale_price",
                        "type": "FLOAT",
                    },
                ],
                "table_name": "order_items",
            },
        ]
        combined_model = create_pydantic_models(filtered_tables)
        expected_fields = ["products", "orders", "order_items"]
        self.assertTrue(
            all(field in combined_model.model_fields for field in expected_fields)
        )

        # Get inner model types from Optional[list[Model]] annotation
        # get_args(Optional[list[M]]) = (list[M], NoneType); then get_args(list[M]) = (M,)
        def _inner_model(field_name):
            outer = get_args(combined_model.model_fields[field_name].annotation)[0]
            return get_args(outer)[0]

        products_model = _inner_model("products")
        orders_model = _inner_model("orders")
        order_items_model = _inner_model("order_items")

        # Assert each model has the correct fields and types
        self.assertTrue(issubclass(products_model, BaseModel))
        self.assertEqual(
            set(products_model.model_fields.keys()), {"id", "category", "name", "brand"}
        )

        self.assertTrue(issubclass(orders_model, BaseModel))
        self.assertEqual(
            set(orders_model.model_fields.keys()), {"order_id", "gender", "created_at"}
        )

        self.assertTrue(issubclass(order_items_model, BaseModel))
        self.assertEqual(
            set(order_items_model.model_fields.keys()),
            {"order_id", "product_id", "sale_price"},
        )


if __name__ == "__main__":
    unittest.main()
