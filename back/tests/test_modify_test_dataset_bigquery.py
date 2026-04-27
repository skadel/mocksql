import pytest

from utils.examples import modify_test_dataset_for_bigquery_exec

TEST_PROJECT = "p1"
TEST_DATASET = "ds_test"
SESSION_ID = "sess-4-1-3"


@pytest.mark.parametrize(
    "query, expected",
    [
        # Basic replacement in simple SELECT (db prefix included in table name)
        ("SELECT a FROM `ds1.t1`", "SELECT a FROM `p1.ds_test.ds1_t1_sess_4_1_3`"),
        # Case insensitivity and DISTINCT keyword
        (
            "select distinct x from `myds.mytable`",
            "SELECT DISTINCT x FROM `p1.ds_test.myds_mytable_sess_4_1_3`",
        ),
        # Multiple tables and CTEs
        (
            """
WITH first AS (
    SELECT * FROM `dataset.table1`
),
second AS (
    SELECT * FROM `dataset.table2`
)
SELECT f.*, s.*
FROM first f
JOIN second s ON f.id = s.id
""",
            "WITH first AS (SELECT * FROM `p1.ds_test.dataset_table1_sess_4_1_3`), second AS (SELECT * FROM `p1.ds_test.dataset_table2_sess_4_1_3`) SELECT f.*, s.* FROM first AS f JOIN second AS s ON f.id = s.id",
        ),
        # Quoted dataset and table separately
        (
            "SELECT * FROM `ds1`.`t1` WHERE col=1",
            "SELECT * FROM p1.ds_test.ds1_t1_sess_4_1_3 WHERE col = 1",
        ),
        # Idempotent: already modified should remain unchanged
        (
            "SELECT * FROM `p1.ds_test.t1_sess_4_1_3`",
            "SELECT * FROM `p1.ds_test.t1_sess_4_1_3`",
        ),
        # No backticks: should modify to test dataset reference
        ("SELECT * FROM ds1.t1", "SELECT * FROM p1.ds_test.ds1_t1_sess_4_1_3"),
        # Extra whitespace and newlines
        (
            "  SELECT  b  FROM  `schema.ds.table3`  ",
            "SELECT b FROM `p1.ds_test.ds_table3_sess_4_1_3`",
        ),
        # Multiple occurrences in same query
        (
            "SELECT * FROM `ds1.t1`, `ds2.t2`",
            "SELECT * FROM `p1.ds_test.ds1_t1_sess_4_1_3` CROSS JOIN `p1.ds_test.ds2_t2_sess_4_1_3`",
        ),
        # Fully qualified project.dataset.table
        (
            "SELECT * FROM `proj1.ds1.tbl`",
            "SELECT * FROM `p1.ds_test.ds1_tbl_sess_4_1_3`",
        ),
        # Subquery
        (
            "SELECT * FROM (SELECT * FROM `orig.ds.t4`)",
            "SELECT * FROM (SELECT * FROM `p1.ds_test.ds_t4_sess_4_1_3`)",
        ),
        # Table alias with AS
        (
            "SELECT x FROM `ds1.t5` AS alias",
            "SELECT x FROM `p1.ds_test.ds1_t5_sess_4_1_3` AS alias",
        ),
        # Comments preserved
        (
            "-- comment\nSELECT * FROM `ds1.t7` -- end",
            "/* comment */ SELECT * FROM `p1.ds_test.ds1_t7_sess_4_1_3`",
        ),
    ],
)
def test_modify_dataset_various(query, expected):
    result = modify_test_dataset_for_bigquery_exec(
        query,
        session_id=SESSION_ID,
        dialect="bigquery",
        test_dataset=TEST_DATASET,
        test_project=TEST_PROJECT,
    )
    assert result == expected


def test_nested_subqueries_no_double_prefix():
    """Regression: tables in nested subqueries were renamed twice when session_id is empty."""
    query = """
SELECT Provider_Name
FROM (
  SELECT OP.provider_name AS Provider_Name
  FROM (
    SELECT provider_id, provider_name
    FROM `bigquery-public-data.cms_medicare.outpatient_charges_2014`
  ) AS OP
  INNER JOIN (
    SELECT provider_id, provider_name
    FROM `bigquery-public-data.cms_medicare.inpatient_charges_2014`
  ) AS IP
  ON OP.provider_id = IP.provider_id
)
"""
    result = modify_test_dataset_for_bigquery_exec(
        query,
        session_id="",
        dialect="bigquery",
        test_dataset="test_dataset",
        test_project="mocksql-493612",
    )
    assert "test_dataset_test_dataset_" not in result
    assert "cms_medicare_outpatient_charges_2014" in result
    assert "cms_medicare_inpatient_charges_2014" in result
