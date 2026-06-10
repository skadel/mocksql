"""Regression tests for _resolve_target_key.

Bug: ``test_index`` is a stable slot label minted 1-based (``str(len+1)`` →
"1", "2", …) and surfaced to the frontend/agent, but ``_resolve_target_key``
indexed ``existing_list`` with it 0-based (``existing_list[test_index_int]``).
The two conventions disagree, so:
  - targeting test "1" overwrote the 2nd test (off-by-one),
  - targeting test "2" went out of range → None → a NEW (duplicate) test.

These tests pin value-matching: the passed test_index must select the test
whose stored test_index equals it.
"""

from build_query.examples_generator import _next_test_index, _resolve_target_key


def _tests():
    return [
        {"test_index": "1", "test_uid": "ddbd"},
        {"test_index": "2", "test_uid": "ef73"},
    ]


def test_targets_second_test_by_value_not_position():
    """test_index "2" → the 2nd test, not out-of-range/None."""
    assert _resolve_target_key({"test_index": "2"}, _tests()) == "2"


def test_targets_first_test_without_off_by_one():
    """test_index "1" → the 1st test, NOT the 2nd (the off-by-one bug)."""
    assert _resolve_target_key({"test_index": "1"}, _tests()) == "1"


def test_unknown_test_index_creates_new():
    """A test_index that matches no test → None (create a new test)."""
    assert _resolve_target_key({"test_index": "99"}, _tests()) is None


def test_no_test_index_creates_new():
    assert _resolve_target_key({}, _tests()) is None


def test_empty_results_retries_first_test():
    """During an empty_results retry without an explicit match → first test."""
    assert _resolve_target_key({"status": "empty_results"}, _tests()) == "1"


def test_int_test_index_matches_string_stored_value():
    """Frontend may pass test_index as an int; stored value is a string."""
    assert _resolve_target_key({"test_index": 2}, _tests()) == "2"


def test_targets_by_test_uid():
    """test_uid (stable identity) selects the matching test."""
    assert _resolve_target_key({"test_uid": "ef73"}, _tests()) == "2"


def test_test_uid_takes_priority_over_test_index():
    """When both are present, test_uid wins (the stable identity)."""
    state = {"test_uid": "ddbd", "test_index": "2"}
    assert _resolve_target_key(state, _tests()) == "1"


def test_unknown_test_uid_falls_back_to_test_index():
    state = {"test_uid": "zzzz", "test_index": "2"}
    assert _resolve_target_key(state, _tests()) == "2"


def test_next_test_index_is_collision_free_after_delete():
    """After deleting "1" from ["1","2"], the next slot must be "3", not "2"
    (len+1 == "2" would collide with the surviving test)."""
    survivors = [{"test_index": "2", "test_uid": "ef73"}]
    assert _next_test_index(survivors) == "3"


def test_next_test_index_empty():
    assert _next_test_index([]) == "1"


def test_next_test_index_ignores_non_numeric():
    tests = [{"test_index": "2"}, {"test_index": ""}, {"test_index": None}]
    assert _next_test_index(tests) == "3"
