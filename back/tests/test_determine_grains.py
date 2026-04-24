import unittest

from utils.find_grains import determine_query_grain


class TestDetermineQueryGrain(unittest.TestCase):
    def setUp(self):
        self.source_table_grains = [
            {
                "table_name": "table_a",
                "primary_keys": ["a_id"],
                "columns": [
                    {
                        "name": "a_id",
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
                    {
                        "name": "a_title",
                        "type": "STRING",
                        "description": "Title in table_a",
                        "example": "Title A",
                    },
                    {
                        "name": "value",
                        "type": "FLOAT",
                        "description": "Some value in table_a",
                        "example": "10.5",
                    },
                    {
                        "name": "parent_id",
                        "type": "INTEGER",
                        "description": "Parent ID reference",
                        "example": "100",
                    },
                    {
                        "name": "a_date",
                        "type": "DATE",
                        "description": "Parent ID reference",
                        "example": "2022-01-01",
                    },
                    {
                        "name": "price",
                        "type": "FLOAT",
                        "description": "Price of the item in table_a",
                        "example": "19.99",
                    },
                ],
            },
            {
                "table_name": "table_b",
                "primary_keys": ["b_id"],
                "columns": [
                    {
                        "name": "b_id",
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
                        "name": "value",
                        "type": "FLOAT",
                        "description": "Some value in table_b",
                        "example": "15.0",
                    },
                    {
                        "name": "a_id",
                        "type": "INTEGER",
                        "description": "Foreign key reference to table_a",
                        "example": "1",
                    },
                    {
                        "name": "e_id1",
                        "type": "INTEGER",
                        "description": "Foreign key reference to table_e",
                        "example": "200",
                    },
                    {
                        "name": "e_id2",
                        "type": "INTEGER",
                        "description": "Foreign key reference to table_e",
                        "example": "300",
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
                        "name": "a_id",
                        "type": "INTEGER",
                        "description": "Foreign key reference to table_a",
                        "example": "3",
                    },
                    {
                        "name": "c_name",
                        "type": "STRING",
                        "description": "Name in table_c",
                        "example": "Item C",
                    },
                ],
            },
            {
                "table_name": "table_d",
                "primary_keys": ["d_id1", "d_id2"],
                "columns": [
                    {
                        "name": "d_id1",
                        "type": "INTEGER",
                        "description": "First part of composite primary key for table_d",
                        "example": "4",
                    },
                    {
                        "name": "d_id2",
                        "type": "INTEGER",
                        "description": "Second part of composite primary key for table_d",
                        "example": "5",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name in table_d",
                        "example": "Item D",
                    },
                ],
            },
            {
                "table_name": "table_e",
                "primary_keys": ["e_id1", "e_id2"],
                "columns": [
                    {
                        "name": "e_id1",
                        "type": "INTEGER",
                        "description": "First part of composite primary key for table_e",
                        "example": "6",
                    },
                    {
                        "name": "e_id2",
                        "type": "INTEGER",
                        "description": "Second part of composite primary key for table_e",
                        "example": "7",
                    },
                    {
                        "name": "name",
                        "type": "STRING",
                        "description": "Name in table_e",
                        "example": "Item E",
                    },
                    {
                        "name": "some_value",
                        "type": "FLOAT",
                        "description": "Some value in table_e",
                        "example": "20.0",
                    },
                    {
                        "name": "d_id1",
                        "type": "INTEGER",
                        "description": "First part of composite primary key for table_d",
                        "example": "4",
                    },
                    {
                        "name": "d_id2",
                        "type": "INTEGER",
                        "description": "Second part of composite primary key for table_d",
                        "example": "4",
                    },
                ],
            },
        ]

    def test_simple_select_without_join(self):
        query = "SELECT a_id FROM table_a;"
        expected_grain = ["a_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_join(self):
        query = """
        SELECT a.a_id, b.b_id, a.a_name, b.value, a.price
        FROM table_a a
        JOIN table_b b ON a.a_id = b.a_id;
        """
        expected_grain = ["b_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_no_grain(self):
        query = """
        SELECT d.d_id1, e.name
        FROM table_d d
        JOIN table_e e ON d.d_id1 = e.d_id1;
        """
        expected_grain = None
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_simple_agg(self):
        query = """
        SELECT MAX(table_b.value) as max FROM table_a JOIN table_b on table_b.a_id = table_a.a_id
        """
        expected_grain = [0]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_complex_query_with_cte_subquery_and_aggregates(self):
        query = """
        WITH cte AS (
            SELECT a_id, SUM(value) AS total_value FROM table_a GROUP BY a_id
        )
        SELECT cte.a_id, c.c_id, (SELECT MAX(b.value) FROM table_b b WHERE b.a_id = cte.a_id) AS max_value
        FROM cte
        JOIN table_c c ON cte.a_id = c.a_id;
        """
        expected_grain = [
            "a_id",
            "c_id",
        ]  # Grain includes a_id from CTE and c_id from table_c.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_not_all_selected(self):
        query = """
        WITH cte AS (
            SELECT a_name, a_title, SUM(value) AS total_value FROM table_a GROUP BY a_name, a_title
        )
        SELECT a_name, total_value AS total_value
        FROM cte;
        """
        expected_grain = None  # Grain includes a_id from CTE and c_id from table_c.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_with_multiple_columns_but_one_grain(self):
        query = "SELECT a_id, a_name, price FROM table_a;"
        expected_grain = ["a_id"]  # Only a_id should be part of the grain.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_rename_id(self):
        query = """
        SELECT a_id as id, a_name as name FROM table_a;
        """
        expected_grain = ["id"]  # Union of grains from both SELECT statements.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_rename_id(self):
        query = """
        WITH cte AS (
            SELECT a_id as id, SUM(value) AS total_value 
            FROM table_a 
            GROUP BY a_id
        )
        SELECT cte.id, cte.total_value 
        FROM cte;
        """
        expected_grain = ["id"]  # Grain should reflect the alias 'id' from the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_group_by_with_non_useful_keys(self):
        query = """
        SELECT a_id as id, a_name as name, SUM(value) AS total_value 
            FROM table_a 
            GROUP BY a_id, name
        """
        expected_grain = ["id"]  # Grain should reflect the alias 'id' from the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_group_by_with_use_of_id_aliased(self):
        query = """
        SELECT a_id as id, a_name as name, SUM(value) AS total_value 
            FROM table_a a
            GROUP BY id, name
            
        """
        expected_grain = ["id"]  # Grain should reflect the alias 'id' from the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_group_by_with_function_alias(self):
        query = """
        SELECT EXTRACT(YEAR FROM a_date) as year, a_name as name, SUM(value) AS total_value 
            FROM table_a a
            GROUP BY EXTRACT(YEAR FROM a_date), name
        """
        expected_grain = [
            "name",
            "year",
        ]  # Grain should reflect the alias 'id' from the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_union_query(self):
        query = """
        SELECT a_id as id, a_name as name FROM table_a
        UNION DISTINCT
        SELECT b_id as id, b_name as name FROM table_b;
        """
        expected_grain = ["id"]  # Union of grains from both SELECT statements.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_with_group_by(self):
        query = """
        WITH cte AS (
            SELECT a_id, COUNT(*) AS cnt FROM table_a GROUP BY a_id
        )
        SELECT a_id, cnt FROM cte;
        """
        expected_grain = [
            "a_id"
        ]  # The grain is determined by the GROUP BY clause within the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cast(self):
        query = """
        SELECT CAST(a_id as STRING) as id, COUNT(*) AS cnt FROM table_a;
        """
        expected_grain = [
            "id"
        ]  # The grain is determined by the GROUP BY clause within the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cast_with_group_by(self):
        query = """
        WITH cte AS (
            SELECT CAST(a_id as STRING) as id, COUNT(*) AS cnt FROM table_a GROUP BY CAST(a_id as STRING)
        )
        SELECT id, cnt FROM cte;
        """
        expected_grain = [
            "id"
        ]  # The grain is determined by the GROUP BY clause within the CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cte_with_join(self):
        query = """
        WITH cte AS (
            SELECT a.a_id, a.a_name FROM table_a a
        )
        SELECT cte.a_id, b_id, b.value FROM cte JOIN table_b b ON cte.a_id = b.a_id;
        """
        expected_grain = [
            "b_id"
        ]  # The grain is determined by the join, where b_id is on the "many" side.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_subquery_in_from_clause(self):
        query = """
        SELECT sq.a_id, sq.a_name FROM (
            SELECT a_id, a_name FROM table_a
        ) sq;
        """
        expected_grain = ["a_id"]  # The grain is d
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_subquery_in_select_clause(self):
        query = """
        SELECT a_id, (SELECT COUNT(*) FROM table_b b WHERE b.a_id = a.a_id) AS order_count
        FROM table_a a;
        """
        expected_grain = [
            "a_id"
        ]  # The grain is determined by the primary key in the main query.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_subquery_in_where_clause(self):
        query = """
        SELECT a_id, a_name FROM table_a WHERE a_id IN (
            SELECT b_id FROM table_b WHERE value > 100
        );
        """
        expected_grain = [
            "a_id"
        ]  # The grain is determined by the primary key in the main query.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_self_join(self):
        query = "SELECT a1.a_id, a2.a_id, a2.a_name FROM table_a a1 JOIN table_a a2 ON a1.parent_id = a2.a_id;"
        expected_grain = ["a_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_window_function_with_partition_by(self):
        query = "SELECT a_id, RANK() OVER (PARTITION BY a_title ORDER BY value DESC) as rank FROM table_a;"
        expected_grain = [
            "a_id"
        ]  # The grain is determined by the primary key in the main query.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_multiple_primary_keys_and_join(self):
        query = """
        SELECT e.e_id1, b.b_id
        FROM table_e e
        JOIN table_b b ON e.e_id1 = b.e_id1 AND e.e_id2 = b.e_id2;
        """
        expected_grain = ["b_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_aggregate_function_without_group_by(self):
        query = "SELECT SUM(value) FROM table_a;"
        expected_grain = [0]  # Single row result, so no grain.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_cross_join(self):
        query = "SELECT a.a_id, b.b_id FROM table_a a CROSS JOIN table_b b;"
        expected_grain = [
            "a_id",
            "b_id",
        ]  # Cartesian product means both a_id and b_id contribute to the grain.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_recursive_cte(self):
        query = """
        WITH cte AS (
            SELECT a_id, parent_id FROM table_a WHERE parent_id IS NULL
            UNION ALL
            SELECT a.a_id, a.parent_id FROM table_a a INNER JOIN cte ON cte.a_id = a.parent_id
        )
        SELECT a_id, parent_id FROM cte;
        """
        expected_grain = [
            "a_id"
        ]  # The grain is determined by the primary key in the recursive CTE.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_distinct_with_group_by(self):
        query = "SELECT DISTINCT a_id, COUNT(value) FROM table_a GROUP BY a_id;"
        expected_grain = ["a_id"]  # The grain is determined by the GROUP BY clause.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_union_all_with_no_group_by(self):
        query = (
            "SELECT a_id as id FROM table_a UNION ALL SELECT b_id as id FROM table_b;"
        )
        expected_grain = ["id"]  # Union of grains from both SELECT statements.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_group_by_indexed_columns(self):
        query = """
        SELECT a.a_id, SUM(b.value), SUM(e.some_value), d.d_id1, d.d_id2, e.e_id1, e.e_id2, SUM(e.some_value)
        FROM table_a a
        JOIN table_b b ON a.a_id = b.a_id
        JOIN table_d d ON b.b_id = d.d_id1
        JOIN table_e e ON d.d_id1 = e.e_id1 AND d.d_id2 = e.e_id2
        GROUP BY 1, 4, 5, 6, 7;
        """
        expected_grain = [
            "a_id",
            "d_id1",
            "d_id2",
            "e_id1",
            "e_id2",
        ]  # Columns corresponding to indices 1, 5, and 6.
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_distinct_with_aggregation(self):
        query = """
        SELECT DISTINCT a_name as name, parent_id, a_title
        FROM table_a;
        """
        expected_grain = ["a_title", "name", "parent_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_distinct_with_id(self):
        query = """
        SELECT DISTINCT a_id, a_name as name, parent_id, a_title
        FROM table_a;
        """
        expected_grain = ["a_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_all(self):
        query = """
        SELECT *
        FROM table_a;
        """
        expected_grain = ["a_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_select_except(self):
        query = """
        SELECT distinct * except(a_name, a_date, parent_id)
        FROM table_a;
        """
        expected_grain = ["a_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)

    def test_self_join_ctes(self):
        query = """
        WITH NewCustomers AS (
            SELECT
                t.a_id AS user_id
            FROM
                `your_project.your_dataset.table_a` AS t
            WHERE CAST(t.a_date as DATE) BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), QUARTER)
             AND DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
        ),
        
        OrdersByNewCustomer AS (
            SELECT
                nc.user_id,
                COUNT(b.b_id) AS order_count,
                SUM(b.value) AS total_amount
            FROM
                NewCustomers nc
            LEFT JOIN
                `your_project.your_dataset.table_b` AS b ON nc.user_id = b.a_id
            GROUP BY
                nc.user_id
        )
        
        SELECT
            nc.user_id,
            COALESCE(obnc.order_count, 0) AS order_count,
            COALESCE(obnc.total_amount, 0) AS total_amount
        FROM
            NewCustomers nc
        LEFT JOIN
            OrdersByNewCustomer obnc ON nc.user_id = obnc.user_id;
        """
        expected_grain = ["user_id"]
        result = determine_query_grain(query, self.source_table_grains)["grains"]
        self.assertEqual(result, expected_grain)


if __name__ == "__main__":
    unittest.main()
