import json

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
