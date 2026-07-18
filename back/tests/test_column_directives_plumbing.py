"""Plumbing générateur des `column_directives` (format date / structure JSON).

Le simplifier prouve le format via l'AST (cf. tests/simplifier/
test_variant_structure_directives.py) ; côté générateur il faut que :

1. `_serialize_hint` retire `column_directives` du hint prompt (l'info passe par
   les descriptions Pydantic, pas par le JSON de contraintes) ;
2. `_run_simplify_and_hint` retourne les directives — y compris depuis le cache ;
3. `_directive_hint_text` rende un texte prescriptif AVEC exemple concret
   (c'est l'exemple qui ancre le LLM — incident sf_bq216, coin-flip epoch).
"""

import json

from build_query.examples_generator import (
    _directive_hint_text,
    _run_simplify_and_hint,
    _serialize_hint,
)

# ── _serialize_hint ────────────────────────────────────────────────────────────


def test_serialize_hint_strips_column_directives():
    hint = {
        "conditions": "t.a = 1",
        "format_constraints": ["t.a : TO_DATE('%Y%m%d')"],
        "column_directives": {"t.a": [{"kind": "date_format", "format": "%Y%m%d"}]},
    }
    out = json.loads(_serialize_hint(hint))
    assert "column_directives" not in out
    assert out["format_constraints"] == ["t.a : TO_DATE('%Y%m%d')"]
    # l'appelant garde son dict intact (pas de mutation)
    assert "column_directives" in hint


# ── _run_simplify_and_hint : directives retournées, cache compris ─────────────

_SQL_216_SHAPE = """
SELECT 1
FROM "P"."P"."PUBLICATIONS" p
WHERE EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), 'YYYYMMDD')) = 2016
"""


def test_run_simplify_and_hint_returns_directives_and_caches():
    _, hint_str, directives = _run_simplify_and_hint(
        _SQL_216_SHAPE, dialect="snowflake"
    )
    assert "column_directives" not in (hint_str or "")
    entry = directives.get("publications.filing_date")
    assert entry and entry[0]["kind"] == "date_format", directives

    # 2ᵉ appel : chemin cache — mêmes directives, hint toujours sans la clé
    _, hint_str2, directives2 = _run_simplify_and_hint(
        _SQL_216_SHAPE, dialect="snowflake"
    )
    assert directives2 == directives
    assert "column_directives" not in (hint_str2 or "")


# ── _directive_hint_text ──────────────────────────────────────────────────────


def test_date_format_text_carries_concrete_example():
    txt = _directive_hint_text(
        {"kind": "date_format", "fn": "TO_DATE", "format": "%Y%m%d"}
    )
    assert "TO_DATE" in txt and "%Y%m%d" in txt
    assert "20160315" in txt  # exemple calculé, pas un placeholder
    assert "epoch" in txt.lower()  # contre-indication explicite


def test_date_format_text_survives_bad_strftime():
    txt = _directive_hint_text({"kind": "date_format", "format": None})
    assert txt  # pas d'exception, texte sans exemple


def test_json_object_array_text_lists_fields():
    txt = _directive_hint_text({"kind": "json_object_array", "fields": ["code"]})
    assert "'code'" in txt and '[{"code"' in txt.replace(" ", "")


def test_json_object_text_lists_fields():
    txt = _directive_hint_text({"kind": "json_object", "fields": ["name"]})
    assert "'name'" in txt and "objet JSON" in txt


def test_unknown_kind_yields_empty():
    assert _directive_hint_text({"kind": "???"}) == ""
