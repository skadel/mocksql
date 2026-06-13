"""Troncature de la chaîne `conditions` du hint (P2b — dégraissage du prompt).

Au-delà du budget, `conditions` est tronqué avec un renvoi vers `<query>` ; les
autres clés du hint (`anti_joins`, `format_constraints`…) restent intactes.
"""

import json

from build_query.examples_generator import _CONDITIONS_MAX_CHARS, _serialize_hint


def test_short_conditions_untouched():
    hint = {"conditions": "a.x = 'y'", "anti_joins": []}
    out = json.loads(_serialize_hint(hint))
    assert out["conditions"] == "a.x = 'y'"
    assert "tronqué" not in _serialize_hint(hint)


def test_long_conditions_truncated_with_marker():
    long_cond = "a.x = 'y' AND " * 500  # bien au-delà du budget
    hint = {"conditions": long_cond, "anti_joins": ["t.k"]}
    serialized = _serialize_hint(hint)
    out = json.loads(serialized)
    # le marqueur renvoie vers <query>
    assert out["conditions"].endswith(" … (tronqué — voir <query>)")
    # le corps conservé est borné par le budget
    body = out["conditions"].removesuffix(" … (tronqué — voir <query>)")
    assert len(body) == _CONDITIONS_MAX_CHARS
    # les autres clés ne sont pas affectées
    assert out["anti_joins"] == ["t.k"]


def test_empty_hint_yields_empty_string():
    assert _serialize_hint({}) == ""
    assert _serialize_hint(None) == ""


def test_conditions_at_budget_not_truncated():
    cond = "x" * _CONDITIONS_MAX_CHARS
    out = json.loads(_serialize_hint({"conditions": cond}))
    assert out["conditions"] == cond
    assert "tronqué" not in out["conditions"]
