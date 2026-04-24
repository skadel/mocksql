import unittest

from utils.find_grains import determine_query_grain


class TestDetermineQueryGrainWithSamePrimaryKeyNames(unittest.TestCase):
    def setUp(self):
        self.tables_and_columns = [
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.distribution_centers",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each distribution center.",
                        "is_categorical": False,
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name of the distribution center.",
                        "is_categorical": True,
                    },
                    {
                        "name": "latitude",
                        "type": "FLOAT",
                        "description": "Latitude coordinate of the distribution center.",
                        "is_categorical": False,
                    },
                    {
                        "name": "longitude",
                        "type": "FLOAT",
                        "description": "Longitude coordinate of the distribution center.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.events",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each event.",
                        "is_categorical": False,
                    },
                    {
                        "name": "user_id",
                        "type": "INTEGER",
                        "description": "Identifier of the user associated with the event.",
                        "is_categorical": False,
                    },
                    {
                        "name": "sequence_number",
                        "type": "INTEGER",
                        "description": "Sequence number of the event in a session.",
                        "is_categorical": False,
                    },
                    {
                        "name": "session_id",
                        "type": "STRING",
                        "description": "Identifier of the session during which the event occurred.",
                        "is_categorical": False,
                    },
                    {
                        "name": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the event was created.",
                        "is_categorical": False,
                    },
                    {
                        "name": "ip_address",
                        "type": "STRING",
                        "description": "IP address of the user when the event occurred.",
                        "is_categorical": False,
                    },
                    {
                        "name": "city",
                        "type": "STRING",
                        "description": "City from which the event was logged.",
                        "is_categorical": True,
                    },
                    {
                        "name": "state",
                        "type": "STRING",
                        "description": "State from which the event was logged.",
                        "is_categorical": True,
                    },
                    {
                        "name": "postal_code",
                        "type": "STRING",
                        "description": "Postal code from which the event was logged.",
                        "is_categorical": False,
                    },
                    {
                        "name": "browser",
                        "type": "STRING",
                        "description": "Browser used to access the service.",
                        "is_categorical": True,
                    },
                    {
                        "name": "traffic_source",
                        "type": "STRING",
                        "description": "Source of traffic that led to the event.",
                        "is_categorical": True,
                    },
                    {
                        "name": "uri",
                        "type": "STRING",
                        "description": "Uniform Resource Identifier accessed during the event.",
                        "is_categorical": True,
                    },
                    {
                        "name": "event_type",
                        "type": "STRING",
                        "description": "Type of event recorded.",
                        "is_categorical": True,
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.inventory_items",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each inventory item.",
                        "is_categorical": False,
                    },
                    {
                        "name": "product_id",
                        "type": "INTEGER",
                        "description": "Identifier of the product associated with the inventory item.",
                        "is_categorical": False,
                    },
                    {
                        "name": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the inventory item was created.",
                        "is_categorical": False,
                    },
                    {
                        "name": "sold_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the inventory item was sold.",
                        "is_categorical": False,
                    },
                    {
                        "name": "cost",
                        "type": "FLOAT",
                        "description": "Cost of the inventory item.",
                        "is_categorical": False,
                    },
                    {
                        "name": "product_category",
                        "type": "STRING",
                        "description": "Category of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "product_name",
                        "type": "STRING",
                        "description": "Name of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "product_brand",
                        "type": "STRING",
                        "description": "Brand of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "product_retail_price",
                        "type": "FLOAT",
                        "description": "Retail price of the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "product_department",
                        "type": "STRING",
                        "description": "Department associated with the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "product_sku",
                        "type": "STRING",
                        "description": "Stock Keeping Unit of the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "product_distribution_center_id",
                        "type": "INTEGER",
                        "description": "Identifier of the distribution center where the product is stored.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.order_items",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each order item.",
                        "is_categorical": False,
                    },
                    {
                        "name": "order_id",
                        "type": "INTEGER",
                        "description": "Identifier of the order associated with the order item.",
                        "is_categorical": False,
                    },
                    {
                        "name": "user_id",
                        "type": "INTEGER",
                        "description": "Identifier of the user who placed the order.",
                        "is_categorical": False,
                    },
                    {
                        "name": "product_id",
                        "type": "INTEGER",
                        "description": "Identifier of the product ordered.",
                        "is_categorical": False,
                    },
                    {
                        "name": "inventory_item_id",
                        "type": "INTEGER",
                        "description": "Identifier of the inventory item sold.",
                        "is_categorical": False,
                    },
                    {
                        "name": "status",
                        "type": "STRING",
                        "description": "Current status of the order item (e.g., shipped, delivered).",
                        "is_categorical": True,
                    },
                    {
                        "name": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order item was created.",
                        "is_categorical": False,
                    },
                    {
                        "name": "shipped_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order item was shipped.",
                        "is_categorical": False,
                    },
                    {
                        "name": "delivered_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order item was delivered.",
                        "is_categorical": False,
                    },
                    {
                        "name": "returned_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order item was returned, if applicable.",
                        "is_categorical": False,
                    },
                    {
                        "name": "sale_price",
                        "type": "FLOAT",
                        "description": "Sale price of the order item.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.orders",
                "columns": [
                    {
                        "name": "order_id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each order.",
                        "is_categorical": False,
                    },
                    {
                        "name": "user_id",
                        "type": "INTEGER",
                        "description": "Identifier of the user who placed the order.",
                        "is_categorical": False,
                    },
                    {
                        "name": "status",
                        "type": "STRING",
                        "description": "Current status of the order (e.g., processing, shipped).",
                        "is_categorical": True,
                    },
                    {
                        "name": "gender",
                        "type": "STRING",
                        "description": "Gender of the user who placed the order.",
                        "is_categorical": True,
                    },
                    {
                        "name": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order was created.",
                        "is_categorical": False,
                    },
                    {
                        "name": "returned_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order was returned, if applicable.",
                        "is_categorical": False,
                    },
                    {
                        "name": "shipped_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order was shipped.",
                        "is_categorical": False,
                    },
                    {
                        "name": "delivered_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the order was delivered.",
                        "is_categorical": False,
                    },
                    {
                        "name": "num_of_item",
                        "type": "INTEGER",
                        "description": "Number of items included in the order.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["order_id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.products",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "cost",
                        "type": "FLOAT",
                        "description": "Cost of the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "category",
                        "type": "STRING",
                        "description": "Category of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "brand",
                        "type": "STRING",
                        "description": "Brand of the product.",
                        "is_categorical": True,
                    },
                    {
                        "name": "retail_price",
                        "type": "FLOAT",
                        "description": "Retail price of the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "department",
                        "type": "STRING",
                        "description": "Department associated with the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "sku",
                        "type": "STRING",
                        "description": "Stock Keeping Unit of the product.",
                        "is_categorical": False,
                    },
                    {
                        "name": "distribution_center_id",
                        "type": "INTEGER",
                        "description": "Identifier of the distribution center that stocks the product.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["id"],
            },
            {
                "table_name": "bigquery-public-data.thelook_ecommerce.users",
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Unique identifier for each user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "first_name",
                        "type": "STRING",
                        "description": "First name of the user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "last_name",
                        "type": "STRING",
                        "description": "Last name of the user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "email",
                        "type": "STRING",
                        "description": "Email address of the user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "age",
                        "type": "INTEGER",
                        "description": "Age of the user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "gender",
                        "type": "STRING",
                        "description": "Gender of the user.",
                        "is_categorical": True,
                    },
                    {
                        "name": "state",
                        "type": "STRING",
                        "description": "State of residence of the user.",
                        "is_categorical": True,
                    },
                    {
                        "name": "street_address",
                        "type": "STRING",
                        "description": "Street address of the user.",
                        "is_categorical": False,
                    },
                    {
                        "name": "postal_code",
                        "type": "STRING",
                        "description": "Postal code of the user's address.",
                        "is_categorical": False,
                    },
                    {
                        "name": "city",
                        "type": "STRING",
                        "description": "City of residence of the user.",
                        "is_categorical": True,
                    },
                    {
                        "name": "country",
                        "type": "STRING",
                        "description": "Country of residence of the user.",
                        "is_categorical": True,
                    },
                    {
                        "name": "latitude",
                        "type": "FLOAT",
                        "description": "Latitude coordinate of the user's location.",
                        "is_categorical": False,
                    },
                    {
                        "name": "longitude",
                        "type": "FLOAT",
                        "description": "Longitude coordinate of the user's location.",
                        "is_categorical": False,
                    },
                    {
                        "name": "traffic_source",
                        "type": "STRING",
                        "description": "Source from which the user originated.",
                        "is_categorical": True,
                    },
                    {
                        "name": "created_at",
                        "type": "TIMESTAMP",
                        "description": "Timestamp when the user's record was created.",
                        "is_categorical": False,
                    },
                ],
                "primary_keys": ["id"],
            },
        ]

    def test_simple_select_with_composed_table_name(self):
        query = """
SELECT
    distribution_centers.id,
    distribution_centers.name,
    distribution_centers.latitude,
    distribution_centers.longitude
  FROM
    `bigquery-public-data`.thelook_ecommerce.distribution_centers AS distribution_centers;
        """
        # Both table_a and table_b have a primary key named "id".
        # The expected grain should include both "a_id" and "b_id" due to the aliases.
        expected_grain = ["id"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)


if __name__ == "__main__":
    unittest.main()
