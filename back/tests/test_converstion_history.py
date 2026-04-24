import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.converstion_history import (
    _SYNTHETIC_NOMINAL_HUMAN,
    _format_execution_results,
    _format_unit_tests_for_generator,
    format_history,
)
from utils.msg_types import MsgType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _msg(content: str, msg_type: str):
    return HumanMessage(content=content, additional_kwargs={"type": msg_type})


_SAMPLE_TEST = {
    "unit_test_description": "Vérifie que X retourne Y.",
    "unit_test_build_reasoning": "Logique de génération.",
    "tags": ["Logique métier"],
    "suggestions": ["Vérifie que A.", "S'assure que B."],
    "data": {"Table1": [{"col": "val"}]},
    "status": "complete",
    "test_index": 0,
    "results_json": '[{"result": 1}]',
}

_EXPECTED_KEYS = {
    "unit_test_description",
    "unit_test_build_reasoning",
    "tags",
    "suggestions",
    "data",
}


def _results_msg(tests=None):
    tests = tests or [_SAMPLE_TEST]
    return _msg(json.dumps(tests), MsgType.RESULTS)


def _examples_msg(tests=None):
    tests = tests or [_SAMPLE_TEST]
    return _msg(json.dumps(tests), MsgType.EXAMPLES)


def _instruction_msg(text="Modifie le test."):
    return _msg(text, MsgType.EXAMPLES_INSTRUCTION)


def _query_msg(text="Génère un test."):
    return _msg(text, MsgType.QUERY)


def _make_result(idx, desc, results_json):
    return {
        "test_index": idx,
        "unit_test_description": desc,
        "results_json": results_json,
    }


def _run_format(history, excluded_agents=None):
    return format_history(
        history,
        dialect="bigquery",
        output_format="generator",
        excluded_agents=excluded_agents or [],
    )


# ---------------------------------------------------------------------------
# _format_unit_tests_for_generator
# ---------------------------------------------------------------------------


class TestFormatUnitTestsForGenerator:
    def test_single_test_returns_object_not_array(self):
        result = json.loads(_format_unit_tests_for_generator([_SAMPLE_TEST]))
        assert isinstance(result, dict)

    def test_multiple_tests_returns_array(self):
        result = json.loads(
            _format_unit_tests_for_generator([_SAMPLE_TEST, _SAMPLE_TEST])
        )
        assert isinstance(result, list)
        assert len(result) == 2

    def test_strips_runtime_fields(self):
        result = json.loads(_format_unit_tests_for_generator([_SAMPLE_TEST]))
        assert set(result.keys()) == _EXPECTED_KEYS
        assert "status" not in result
        assert "test_index" not in result
        assert "results_json" not in result

    def test_preserves_data_content(self):
        result = json.loads(_format_unit_tests_for_generator([_SAMPLE_TEST]))
        assert result["data"] == _SAMPLE_TEST["data"]

    def test_missing_optional_fields_ignored(self):
        minimal = {"unit_test_description": "Vérifie X.", "data": {}}
        result = json.loads(_format_unit_tests_for_generator([minimal]))
        assert "unit_test_description" in result
        assert "tags" not in result


# ---------------------------------------------------------------------------
# _format_execution_results
# ---------------------------------------------------------------------------


class TestFormatExecutionResults:
    def test_single_result(self):
        r = _make_result(0, "Vérifie X.", '[{"date": "2016-01-01"}]')
        text = _format_execution_results([r])
        assert "Test 0 (Vérifie X.)" in text
        assert "2016-01-01" in text

    def test_multiple_results_separated(self):
        results = [
            _make_result(0, "Desc A", '[{"a": 1}]'),
            _make_result(1, "Desc B", '[{"b": 2}]'),
        ]
        text = _format_execution_results(results)
        assert "Test 0 (Desc A)" in text
        assert "Test 1 (Desc B)" in text

    def test_invalid_results_json_kept_raw(self):
        r = _make_result(0, "Desc.", "not-valid-json")
        text = _format_execution_results([r])
        assert "not-valid-json" in text

    def test_empty_results_json_default(self):
        r = {"test_index": 0, "unit_test_description": "X."}
        text = _format_execution_results([r])
        assert "Test 0" in text


# ---------------------------------------------------------------------------
# format_history — output_format="generator"
# ---------------------------------------------------------------------------


class TestFormatHistoryGenerator:
    def test_empty_history_returns_empty_list(self):
        assert _run_format([]) == []

    def test_results_first_injects_synthetic_human(self):
        msgs = _run_format([_results_msg()])
        assert len(msgs) == 2
        assert isinstance(msgs[0], HumanMessage)
        assert msgs[0].content == _SYNTHETIC_NOMINAL_HUMAN

    def test_examples_first_injects_synthetic_human(self):
        msgs = _run_format([_examples_msg()])
        assert len(msgs) == 2
        assert isinstance(msgs[0], HumanMessage)
        assert msgs[0].content == _SYNTHETIC_NOMINAL_HUMAN

    def test_instruction_then_results_produces_human_ai(self):
        msgs = _run_format([_instruction_msg(), _results_msg()])
        assert len(msgs) == 2
        assert isinstance(msgs[0], HumanMessage)
        assert isinstance(msgs[1], AIMessage)

    def test_query_instruction_not_wrapped_in_tag(self):
        msgs = _run_format([_query_msg("Ma question"), _results_msg()])
        assert "Ma question" in msgs[0].content
        assert "<examples_update>" not in msgs[0].content

    def test_examples_instruction_wrapped_in_tag(self):
        msgs = _run_format([_instruction_msg("Modifie X."), _results_msg()])
        assert "<demande de modification/rajout de test>" in msgs[0].content
        assert "Modifie X." in msgs[0].content

    def test_ai_message_contains_unit_test_fields(self):
        msgs = _run_format([_instruction_msg(), _results_msg()])
        ai_content = json.loads(msgs[1].content)
        assert set(ai_content.keys()) == _EXPECTED_KEYS

    def test_two_turns_produces_four_messages(self):
        history = [
            _instruction_msg("Instruction 1"),
            _results_msg(),
            _instruction_msg("Instruction 2"),
            _results_msg(),
        ]
        msgs = _run_format(history)
        assert len(msgs) == 4

    def test_results_prepended_to_next_human_message(self):
        history = [
            _instruction_msg("Instruction 1"),
            _results_msg([{**_SAMPLE_TEST, "unit_test_description": "Vérifie A."}]),
            _instruction_msg("Instruction 2"),
            _results_msg(),
        ]
        msgs = _run_format(history)
        second_human = msgs[2]
        assert isinstance(second_human, HumanMessage)
        assert "Voici ce que j'ai obtenu" in second_human.content
        assert "Vérifie A." in second_human.content
        assert "Instruction 2" in second_human.content

    def test_first_human_has_no_results_prefix(self):
        history = [_instruction_msg("Instruction 1"), _results_msg()]
        msgs = _run_format(history)
        assert "Voici ce que j'ai obtenu" not in msgs[0].content

    def test_examples_then_results_no_duplicate_ai(self):
        """EXAMPLES → AIMessage ; RESULTS only stores pending_results, no second AIMessage."""
        msgs = _run_format([_instruction_msg(), _examples_msg(), _results_msg()])
        types = [type(m).__name__ for m in msgs]
        assert types == ["HumanMessage", "AIMessage"]

    def test_examples_then_results_pending_forwarded(self):
        history = [
            _instruction_msg("Tour 1"),
            _examples_msg(),
            _results_msg([{**_SAMPLE_TEST, "unit_test_description": "Résultat A."}]),
            _instruction_msg("Tour 2"),
        ]
        msgs = _run_format(history)
        last_human = msgs[-1]
        assert "Résultat A." in last_human.content

    def test_excluded_results_skipped(self):
        history = [_instruction_msg(), _results_msg()]
        msgs = _run_format(history, excluded_agents=[MsgType.RESULTS])
        assert len(msgs) == 1
        assert isinstance(msgs[0], HumanMessage)

    def test_excluded_instruction_skipped(self):
        msgs = _run_format(
            [_instruction_msg(), _results_msg()],
            excluded_agents=[MsgType.EXAMPLES_INSTRUCTION],
        )
        assert isinstance(msgs[0], HumanMessage)
        assert msgs[0].content == _SYNTHETIC_NOMINAL_HUMAN

    def test_invalid_output_format_raises(self):
        with pytest.raises(ValueError):
            format_history([], dialect="bigquery", output_format="unknown")
