import asyncio
import unittest

import pytest
import sqlglot

from build_query.validator import optimize_and_extract_info, evaluate_and_fix_query


class TestExtractUsedColumns(unittest.TestCase):
    async def test_single_table(self):
        query = "SELECT id, name FROM users WHERE age > 20"
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        expected = [{"table": "users", "used_columns": ["age", "id", "name"]}]
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    def test_table_alias(self):
        query = "/* Select Adult users */ SELECT u.id, u.name FROM users AS u WHERE u.age > 20"
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        expected = [
            {
                "project": "",
                "database": "",
                "table": "users",
                "used_columns": ["age", "id", "name"],
                "used_identifiers": ["age", "name", "id", "u"],
            }
        ]
        query, _, used_columns, literals = asyncio.run(
            evaluate_and_fix_query(query, "dummy", mapping=tables, dialect="bigquery")
        )
        for entry in used_columns:
            entry["used_identifiers"] = sorted(entry.get("used_identifiers", []))
        for entry in expected:
            entry["used_identifiers"] = sorted(entry.get("used_identifiers", []))
        self.assertEqual(used_columns, expected)

    async def test_table_mix_alias(self):
        query = "SELECT u.id, u.name FROM users AS u WHERE age > 20"
        expected = [{"table": "users", "used_columns": ["age", "id", "name"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_join_tables(self):
        query = (
            "SELECT u.id, p.title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        )
        expected = [
            {"table": "posts", "used_columns": ["title", "user_id"]},
            {"table": "users", "used_columns": ["id"]},
        ]
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT"},
        }
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_join_tables_without_qualification(self):
        query = "SELECT id, title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        expected = [
            {"table": "posts", "used_columns": ["title", "user_id"]},
            {"table": "users", "used_columns": ["id"]},
        ]
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT", "post": "STRING"},
        }
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_unresolvable_column(self):
        query = "SELECT id, title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT", "id": "INT"},
        }
        with pytest.raises(sqlglot.errors.OptimizeError) as excinfo:
            await optimize_and_extract_info(query, tables)

        assert "Column 'id' could not be resolved" in str(excinfo.value)

    async def test_subquery(self):
        query = "SELECT name FROM (SELECT name FROM users) AS u"
        expected = [{"table": "users", "used_columns": ["name"]}]
        tables = {"users": {"name": "STRING"}}
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_columns_no_table(self):
        query = "SELECT name, age FROM some_view"
        expected = [{"table": "some_view", "used_columns": ["age", "name"]}]
        tables = {"some_view": {"age": "INT", "name": "STRING"}}
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_simple_cte(self):
        query = """
        WITH filtered_users AS (
            SELECT id, name FROM users WHERE age > 20
        )
        SELECT name FROM filtered_users
        """

        expected = [{"table": "users", "used_columns": ["age", "name"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_nested_cte(self):
        query = """
        WITH filtered_users AS (
            SELECT * FROM users WHERE age > 20
        ), adult_users AS (
            SELECT id FROM filtered_users WHERE age >= 18
        )
        SELECT id FROM adult_users
        """
        expected = [{"table": "users", "used_columns": ["age", "id"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_complex_query(self):
        query = """
                WITH filtered_users AS (
                    SELECT id, name FROM users WHERE age > 20
                ),
                extended_info AS (
                    SELECT user_id, email FROM user_details WHERE email LIKE '%@example.com'
                ),
                joined_data AS (
                    SELECT fu.name, ei.email 
                    FROM filtered_users fu
                    JOIN extended_info ei ON fu.id = ei.user_id
                ),
                additional_data AS (
                    SELECT name FROM users WHERE age BETWEEN 15 AND 20
                )

                SELECT name, email FROM joined_data
                UNION
                SELECT name, NULL AS email FROM additional_data
                """
        tables = {
            "users": {"id": "INT", "name": "STRING", "age": "INT"},
            "user_details": {"user_id": "INT", "email": "STRING"},
        }

        expected = [
            {"table": "user_details", "used_columns": ["email", "user_id"]},
            {"table": "users", "used_columns": ["age", "id", "name"]},
        ]

        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_complex_query_with_wildcard(self):
        query = """
                WITH filtered_users AS (
                    SELECT id, name FROM users WHERE age > 20
                ),
                extended_info AS (
                    SELECT * FROM user_details WHERE email LIKE '%@example.com'
                ),
                joined_data AS (
                    SELECT fu.name, ei.email 
                    FROM filtered_users fu
                    JOIN extended_info ei ON fu.id = ei.user_id
                ),
                additional_data AS (
                    SELECT name FROM users WHERE age BETWEEN 15 AND 20
                )

                SELECT name, email FROM joined_data
                UNION
                SELECT name, NULL AS email FROM additional_data
                """
        tables = {
            "users": {"id": "INT", "name": "STRING", "age": "INT"},
            "user_details": {
                "user_id": "INT",
                "email": "STRING",
                "other_col": "STRING",
            },
        }

        expected = [
            {"table": "user_details", "used_columns": ["email", "user_id"]},
            {"table": "users", "used_columns": ["age", "id", "name"]},
        ]
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_same_table_name_in_different_cte(self):
        query = """
WITH
  `oi` AS (
  SELECT
    `oi`.`order_id` AS `order_id`,
    `oi`.`product_id` AS `product_id`,
    `oi`.`sale_price` AS `sale_price`
  FROM
    `order_items` AS `oi`),
  `p` AS (
  SELECT
    `p`.`id` AS `id`,
    `p`.`name` AS `name`,
    `p`.`brand` AS `brand`
  FROM
    `products` AS `p`
  WHERE
    `p`.`brand` = 'MG'),
  `productrevenue2021` AS (
  SELECT
    `p`.`name` AS `product_name`,
    SUM(`oi`.`sale_price`) AS `revenue_2021`
  FROM
    `orders` AS `o`
  JOIN
    `oi` AS `oi`
  ON
    `o`.`order_id` = `oi`.`order_id`
  JOIN
    `p` AS `p`
  ON
    `oi`.`product_id` = `p`.`id`
  WHERE
    `o`.`gender` = 'F'
    AND EXTRACT(YEAR
    FROM
      `o`.`created_at`) = 2021
  GROUP BY
    `p`.`name`),
  `productrevenue2022` AS (
  SELECT
    `p`.`name` AS `product_name`,
    SUM(`oi`.`sale_price`) AS `revenue_2022`
  FROM
    `orders` AS `o`
  JOIN
    `oi` AS `oi`
  ON
    `o`.`order_id` = `oi`.`order_id`
  JOIN
    `p` AS `p`
  ON
    `oi`.`product_id` = `p`.`id`
  WHERE
    `o`.`gender` = 'F'
    AND EXTRACT(YEAR
    FROM
      `o`.`created_at`) = 2022
  GROUP BY
    `p`.`name`)
SELECT
  `p1`.`product_name` AS `product_name`
FROM
  `productrevenue2021` AS `p1`
JOIN
  `productrevenue2022` AS `p2`
ON
  `p1`.`product_name` = `p2`.`product_name`
  AND `p2`.`revenue_2022` < 0.9 * `p1`.`revenue_2021`
"""
        tables = {
            "distribution_centers": {
                "id": "INTEGER",
                "name": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
            },
            "events": {
                "id": "INTEGER",
                "user_id": "INTEGER",
                "sequence_number": "INTEGER",
                "session_id": "STRING",
                "created_at": "TIMESTAMP",
                "ip_address": "STRING",
                "city": "STRING",
                "state": "STRING",
                "postal_code": "STRING",
                "browser": "STRING",
                "traffic_source": "STRING",
                "uri": "STRING",
                "event_type": "STRING",
            },
            "inventory_items": {
                "id": "INTEGER",
                "product_id": "INTEGER",
                "created_at": "TIMESTAMP",
                "sold_at": "TIMESTAMP",
                "cost": "FLOAT",
                "product_category": "STRING",
                "product_name": "STRING",
                "product_brand": "STRING",
                "product_retail_price": "FLOAT",
                "product_department": "STRING",
                "product_sku": "STRING",
                "product_distribution_center_id": "INTEGER",
            },
            "order_items": {
                "id": "INTEGER",
                "order_id": "INTEGER",
                "user_id": "INTEGER",
                "product_id": "INTEGER",
                "inventory_item_id": "INTEGER",
                "status": "STRING",
                "created_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "sale_price": "FLOAT",
            },
            "orders": {
                "order_id": "INTEGER",
                "user_id": "INTEGER",
                "status": "STRING",
                "gender": "STRING",
                "created_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "num_of_item": "INTEGER",
            },
            "products": {
                "id": "INTEGER",
                "cost": "FLOAT",
                "category": "STRING",
                "name": "STRING",
                "brand": "STRING",
                "retail_price": "FLOAT",
                "department": "STRING",
                "sku": "STRING",
                "distribution_center_id": "INTEGER",
            },
            "users": {
                "id": "INTEGER",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "age": "INTEGER",
                "gender": "STRING",
                "state": "STRING",
                "street_address": "STRING",
                "postal_code": "STRING",
                "city": "STRING",
                "country": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
                "traffic_source": "STRING",
                "created_at": "TIMESTAMP",
            },
        }

        expected = [
            {
                "table": "order_items",
                "used_columns": ["order_id", "product_id", "sale_price"],
            },
            {"table": "orders", "used_columns": ["created_at", "gender", "order_id"]},
            {"table": "products", "used_columns": ["brand", "id", "name"]},
        ]
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)

    async def test_same_column_name_in_different_cte(self):
        query = """
    WITH
      `inventory` AS (
      SELECT
        `ii`.`product_id` AS `product_id`,
        `ii`.`cost` AS `cost`,
        `ii`.`sold_at` AS `sold_at`
      FROM
        `inventory_items` AS `ii`),
      `order_data` AS (
      SELECT
        `oi`.`order_id` AS `order_id`,
        `oi`.`product_id` AS `product_id`,
        `oi`.`sale_price` AS `sale_price`
      FROM
        `order_items` AS `oi`),
      `sales2021` AS (
      SELECT
        `od`.`product_id` AS `product_id`,
        SUM(`od`.`sale_price`) AS `total_sales_2021`
      FROM
        `orders` AS `o`
      JOIN
        `order_data` AS `od`
      ON
        `o`.`order_id` = `od`.`order_id`
      WHERE
        EXTRACT(YEAR FROM `o`.`created_at`) = 2021
      GROUP BY
        `od`.`product_id`),
      `sales2022` AS (
      SELECT
        `od`.`product_id` AS `product_id`,
        SUM(`od`.`sale_price`) AS `total_sales_2022`
      FROM
        `orders` AS `o`
      JOIN
        `order_data` AS `od`
      ON
        `o`.`order_id` = `od`.`order_id`
      WHERE
        EXTRACT(YEAR FROM `o`.`created_at`) = 2022
      GROUP BY
        `od`.`product_id`)
    SELECT
      `s21`.`product_id` AS `product_id`,
      `s21`.`total_sales_2021` AS `total_sales_2021`,
      `s22`.`total_sales_2022` AS `total_sales_2022`
    FROM
      `sales2021` AS `s21`
    JOIN
      `sales2022` AS `s22`
    ON
      `s21`.`product_id` = `s22`.`product_id`
    WHERE
      `s22`.`total_sales_2022` < 0.75 * `s21`.`total_sales_2021`
    """
        tables = {
            "inventory_items": {
                "product_id": "INTEGER",
                "cost": "FLOAT",
                "sold_at": "TIMESTAMP",
            },
            "order_items": {
                "order_id": "INTEGER",
                "product_id": "INTEGER",
                "sale_price": "FLOAT",
            },
            "orders": {"order_id": "INTEGER", "created_at": "TIMESTAMP"},
        }

        expected = [
            {
                "table": "order_items",
                "used_columns": ["order_id", "product_id", "sale_price"],
            },
            {"table": "orders", "used_columns": ["created_at", "order_id"]},
        ]
        result = await optimize_and_extract_info(query, tables)
        self.assertEqual(result["used_columns"], expected)


if __name__ == "__main__":
    unittest.main()
