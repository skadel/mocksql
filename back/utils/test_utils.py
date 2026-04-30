"""Shared test context utilities used by test_evaluator and assertion_modifier."""

import json
from typing import Optional

from utils.msg_types import MsgType
from utils.saver import get_message_type


def build_test_detail(current_test: dict) -> dict:
    """Build the test detail dict passed to LLM prompts."""
    try:
        result_rows = json.loads(current_test.get("results_json") or "[]")
    except Exception:
        result_rows = []

    detail = {
        "description": current_test.get("unit_test_description", ""),
        "reasoning": current_test.get("unit_test_build_reasoning", ""),
        "tags": current_test.get("tags", []),
        "input_data": current_test.get("data", {}),
        "status": current_test.get("status"),
        "row_count": len(result_rows),
        "result_rows": result_rows[:20],
    }
    if current_test.get("error"):
        detail["error"] = current_test["error"]
    if current_test.get("failing_cte"):
        detail["failing_cte"] = current_test["failing_cte"]
    return detail


def find_current_test(all_tests: list, test_index=None) -> Optional[dict]:
    """Return the test matching test_index, or the last test if index is None."""
    if test_index is not None:
        found = next(
            (t for t in all_tests if t.get("test_index") == test_index),
            None,
        )
        if found is not None:
            return found
    return all_tests[-1] if all_tests else None


def extract_tests_from_results(messages: list) -> list:
    """Extract the test list from the last RESULTS message in state.messages."""
    results_msgs = [m for m in messages if get_message_type(m) == MsgType.RESULTS]
    if not results_msgs:
        return []
    try:
        data = json.loads(results_msgs[-1].content)
        if isinstance(data, list):
            return data
        return [data]
    except Exception:
        return []
