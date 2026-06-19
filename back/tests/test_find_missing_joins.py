from build_query.profile_checker import _find_missing_joins

PROFILE = {
    "tables": {},
    "joins": [
        {
            "left_table": "proj.ds.orders",
            "right_table": "proj.ds.users",
            "left_expr": "user_id",
            "right_expr": "id",
        }
    ],
}


def test_join_covered_same_order():
    expected = [{"left_table": "proj.ds.orders", "right_table": "proj.ds.users"}]
    assert _find_missing_joins(PROFILE, expected) == []


def test_join_covered_reversed_order():
    # The key is the set of tables, so a profiled "orders JOIN users" covers
    # an expected "users JOIN orders".
    expected = [{"left_table": "proj.ds.users", "right_table": "proj.ds.orders"}]
    assert _find_missing_joins(PROFILE, expected) == []


def test_join_covered_short_vs_full_names():
    # Matching is on the unqualified, lowercased table name.
    expected = [{"left_table": "ORDERS", "right_table": "users"}]
    assert _find_missing_joins(PROFILE, expected) == []


def test_missing_join_detected():
    expected = [{"left_table": "proj.ds.orders", "right_table": "proj.ds.payments"}]
    missing = _find_missing_joins(PROFILE, expected)
    assert len(missing) == 1
    assert missing[0]["right_table"] == "proj.ds.payments"


def test_partial_coverage():
    expected = [
        {"left_table": "users", "right_table": "orders"},  # covered (reversed)
        {"left_table": "orders", "right_table": "payments"},  # missing
    ]
    missing = _find_missing_joins(PROFILE, expected)
    assert len(missing) == 1
    assert missing[0]["right_table"] == "payments"


def test_no_expected_joins():
    assert _find_missing_joins(PROFILE, []) == []
    assert _find_missing_joins(PROFILE, None) == []


def test_empty_profile_all_missing():
    expected = [{"left_table": "orders", "right_table": "users"}]
    assert _find_missing_joins({"joins": []}, expected) == expected
