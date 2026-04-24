import unittest
from utils.find_grains import determine_query_grain


class TestDetermineQueryGrainWithSamePrimaryKeyNames(unittest.TestCase):
    def setUp(self):
        self.tables_and_columns = [
            {
                "table_name": "table_a",
                "primary_keys": ["id"],
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Primary key for table_a",
                        "example": "1",
                    },
                    {
                        "name": "a_name",
                        "type": "STRING",
                        "description": "Name in table_a",
                        "example": "Item A",
                    },
                ],
            },
            {
                "table_name": "table_b",
                "primary_keys": ["id"],
                "columns": [
                    {
                        "name": "id",
                        "type": "INTEGER",
                        "description": "Primary key for table_b",
                        "example": "2",
                    },
                    {
                        "name": "b_name",
                        "type": "STRING",
                        "description": "Name in table_b",
                        "example": "Item B",
                    },
                    {
                        "name": "a_id",
                        "type": "INTEGER",
                        "description": "Foreign key reference to table_a",
                        "example": "1",
                    },
                ],
            },
            {
                "table_name": "table_c",
                "primary_keys": ["c_id"],
                "columns": [
                    {
                        "name": "c_id",
                        "type": "INTEGER",
                        "description": "Primary key for table_c",
                        "example": "3",
                    },
                    {
                        "name": "b_id",
                        "type": "INTEGER",
                        "description": "Foreign key referencing table_b",
                        "example": "2",
                    },
                ],
            },
        ]

    def test_simple_select_with_qualified_same_named_primary_keys(self):
        query = """
        SELECT a.id AS a_id, b.id AS b_id
        FROM table_a a
        JOIN table_b b ON a.id = b.id;
        """
        # Both table_a and table_b have a primary key named "id".
        # The expected grain should include both "a_id" and "b_id" due to the aliases.
        expected_grain = ["a_id", "b_id"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_with_foreign_key_reference(self):
        query = """
        SELECT b.id AS b_id, c.c_id
        FROM table_b b
        JOIN table_c c ON b.id = c.b_id;
        """
        # The primary key "id" from table_b is joined with the foreign key "b_id" from table_c.
        # The expected grain should include "c_id"
        expected_grain = ["c_id"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)

    def test_self_join_on_same_named_primary_key_with_alias(self):
        query = """
        SELECT a1.id AS id1, a2.id AS id2
        FROM table_a a1
        JOIN table_a a2 ON a1.id = a2.id;
        """
        # Self join on the same table with the same primary key name "id", but with aliases.
        # The expected grain should correctly identify both "id1" and "id2".
        expected_grain = ["id2"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_with_cte_involving_same_named_primary_keys(self):
        query = """
        WITH cte AS (
            SELECT id AS cte_id, a_name FROM table_a
        )
        SELECT cte.cte_id, b.id AS b_id
        FROM cte
        JOIN table_b b ON cte.cte_id = b.a_id;
        """
        # CTE includes the primary key "id" from table_a, aliased as "cte_id".
        # It is joined with "id" from table_b, aliased as "b_id" which is the pk of table_b.
        # The expected grain should be b_id because the cte_id
        expected_grain = ["b_id"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_with_different_columns_with_same_names(self):
        query = """
        SELECT a.id AS a_id, a.a_name, b.id AS b_id, b.b_name
        FROM table_a a
        JOIN table_b b ON a.id = b.id;
        """
        # Here, "a.a_name" and "b.b_name" are both aliased as "name", but they are different columns.
        # The expected grain should only include "a_id" and "b_id" because "name" isn't a primary key.
        expected_grain = ["a_id", "b_id"]
        result = determine_query_grain(query, self.tables_and_columns)["grains"]
        self.assertEqual(result, expected_grain)


if __name__ == "__main__":
    unittest.main()
