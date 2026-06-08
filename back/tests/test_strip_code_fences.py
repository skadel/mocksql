import json

import pytest
from langchain_core.utils.json import parse_partial_json
from pydantic import BaseModel, Field

from utils.prompt_utils import _strip_code_fences, _strip_fences


def test_strip_three_backticks_with_json_tag():
    text = '```json\n{"a": 1}\n```'
    assert _strip_fences(text) == '{"a": 1}'


def test_strip_three_backticks_no_tag():
    text = '```\n{"a": 1}\n```'
    assert _strip_fences(text) == '{"a": 1}'


def test_strip_four_backticks():
    # Régression : certains modèles émettent 4 backticks au lieu de 3.
    # L'ancien regex `^```(?:json)?` n'en strippait que 3, laissant un
    # backtick parasite devant `json`, ce qui cassait le parse JSON.
    text = '````json\n{"a": 1}\n````'
    stripped = _strip_fences(text)
    assert stripped == '{"a": 1}'
    assert json.loads(stripped) == {"a": 1}


def test_strip_no_fence():
    text = '{"a": 1}'
    assert _strip_fences(text) == '{"a": 1}'


def test_strip_code_fences_on_aimessage_four_backticks():
    class _Msg:
        content = '````json\n{"a": 1}\n````'

    result = _strip_code_fences(_Msg())
    assert json.loads(result.content) == {"a": 1}


# ---------------------------------------------------------------------------
# Regression: UnitTestData field order — reasoning last prevents Pydantic
# validation failure when the LLM hits its output token limit mid-JSON.
# ---------------------------------------------------------------------------


class _UnitTestDataNew(BaseModel):
    """Field order matching get_generation_output_type() after the fix."""

    test_name: str
    unit_test_description: str
    tags: list[str]
    data: dict
    unit_test_build_reasoning: str


class _UnitTestDataOld(BaseModel):
    """Field order BEFORE the fix — reasoning first."""

    unit_test_build_reasoning: str
    test_name: str
    unit_test_description: str
    tags: list[str]
    data: dict


def _truncated_json_old() -> str:
    """Simulates a token-limit truncation when reasoning is the FIRST field."""
    return '{"unit_test_build_reasoning": "Very long reasoning about the banques CTE and all its'


def _truncated_json_new() -> str:
    """Simulates a token-limit truncation when reasoning is the LAST field."""
    return (
        '{"test_name": "Banques actives", '
        '"unit_test_description": "Vérifie que les banques actives sont retournées.", '
        '"tags": ["Logique métier"], '
        '"data": {}, '
        '"unit_test_build_reasoning": "Very long reasoning about the banques CTE and all its'
    )


def test_truncated_json_old_field_order_fails_pydantic():
    """Before fix: truncated JSON (reasoning first) → Pydantic fails because core fields missing."""
    recovered = parse_partial_json(_truncated_json_old())
    assert recovered is not None, "parse_partial_json should recover partial JSON"
    with pytest.raises(Exception):
        _UnitTestDataOld.model_validate(recovered)


def test_truncated_json_new_field_order_passes_pydantic():
    """After fix: truncated JSON (reasoning last) → all core fields present → Pydantic succeeds."""
    recovered = parse_partial_json(_truncated_json_new())
    assert recovered is not None, "parse_partial_json should recover partial JSON"
    result = _UnitTestDataNew.model_validate(recovered)
    assert result.test_name == "Banques actives"
    assert result.data == {}
    assert isinstance(result.unit_test_build_reasoning, str)
