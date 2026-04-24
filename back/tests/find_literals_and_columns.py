import unittest

import sqlglot

from build_query.validator import find_literals_and_columns


class TestFindLiteralsAndColumns(unittest.TestCase):
    def test_simple_equality(self):
        query = "SELECT * FROM table WHERE column1 = 'value1';"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["'value1'"]},
            "predicate": "column1 = 'value1'",
        }
        self.assertEqual(result, expected)

    def test_greater_than(self):
        query = "SELECT * FROM table WHERE column1 > 42;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {"columns_values": {"column1": ["42"]}, "predicate": "column1 > 42"}
        self.assertEqual(result, expected)

    def test_less_than_string(self):
        query = "SELECT * FROM table WHERE column1 < 'value2';"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["'value2'"]},
            "predicate": "column1 < 'value2'",
        }
        self.assertEqual(result, expected)

    def test_not_equal(self):
        query = "SELECT * FROM table WHERE column1 != 100;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["100"]},
            "predicate": "column1 <> 100",
        }
        self.assertEqual(result, expected)

    def test_in_clause(self):
        query = "SELECT * FROM table WHERE column1 IN ('value1', 'value2');"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["'value1'", "'value2'"]},
            "predicate": "column1 IN ('value1', 'value2')",
        }
        self.assertEqual(result, expected)

    def test_between_clause(self):
        # BETWEEN is not a binary expression, so literals won't be captured
        query = "SELECT * FROM table WHERE column1 BETWEEN 1 AND 10;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {"columns_values": {}, "predicate": "column1 BETWEEN 1 AND 10"}
        self.assertEqual(result, expected)

    def test_combined_conditions(self):
        query = """
        SELECT * FROM table
        WHERE column1 = 'value1' AND column2 > 50 OR column3 <= 'value3';
        """
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {
                "column1": ["'value1'"],
                "column2": ["50"],
                "column3": ["'value3'"],
            },
            "predicate": "column1 = 'value1' AND column2 > 50 OR column3 <= 'value3'",
        }
        self.assertEqual(result, expected)

    def test_function_in_condition(self):
        query = "SELECT * FROM table WHERE LENGTH(column1) = 5;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {"columns_values": {}, "predicate": "LENGTH(column1) = 5"}
        self.assertEqual(result, expected)

    def test_literal_on_left_side(self):
        query = "SELECT * FROM table WHERE 'value1' = column1;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["'value1'"]},
            "predicate": "'value1' = column1",
        }
        self.assertEqual(result, expected)

    def test_non_string_int_literals(self):
        query = "SELECT * FROM table WHERE column1 = 3.14;"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {"columns_values": {}, "predicate": "column1 = 3.14"}
        self.assertEqual(result, expected)

    def test_subquery(self):
        query = """
        SELECT * FROM table1
        WHERE column1 IN (SELECT column2 FROM table2 WHERE column3 = 'value3');
        """
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column3": ["'value3'"]},
            "predicate": "column1 IN (SELECT column2 FROM table2 WHERE column3 = 'value3')",
        }
        self.assertEqual(result, expected)

    def test_duplicate_literals(self):
        query = "SELECT * FROM table WHERE column1 = 'value1' OR column1 = 'value1';"
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {
            "columns_values": {"column1": ["'value1'"]},
            "predicate": "column1 = 'value1' OR column1 = 'value1'",
        }
        self.assertEqual(result, expected)

    def test_no_literals(self):
        query = """
        DECLARE
    Sys STRING DEFAULT 'PYPI';

WITH HighestReleases AS (
    SELECT
        Name,
        Version,
    FROM (
        SELECT
            Name,
            Version,
            ROW_NUMBER() OVER (
                PARTITION BY Name
                ORDER BY VersionInfo.Ordinal DESC
            ) AS RowNumber
        FROM
            `spider2-public-data.deps_dev_v1.PackageVersions`
        WHERE
            System = Sys
            AND VersionInfo.IsRelease
    )
    WHERE RowNumber = 1
)

SELECT
    D.Dependency.Name,
    D.Dependency.Version
FROM
    `spider2-public-data.deps_dev_v1.Dependencies` AS D
JOIN
    HighestReleases AS H
USING
    (Name, Version)
WHERE
    D.System = Sys
GROUP BY
    D.Dependency.Name,
    D.Dependency.Version
ORDER BY
    COUNT(*) DESC
LIMIT 1;
"""
        parsed_query = sqlglot.parse_one(query, dialect="bigquery")
        result = find_literals_and_columns(parsed_query)
        expected = {"columns_values": {}, "predicate": "column1 = column2"}
        self.assertEqual(result, expected)


if __name__ == "__main__":
    unittest.main()
