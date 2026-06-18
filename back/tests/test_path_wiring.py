"""Régression du câblage path-slicing (hors fonctions pures de path_slicer)."""

import json

from build_query.query_chain import route_agent_output
from build_query.suggestions_node import _build_path_suggestions


_PLANS = {
    "daily": {"sliced_sql": "SELECT 1", "used_columns": [], "branch_index": 0},
    "weekly": {"sliced_sql": "SELECT 2", "used_columns": [], "branch_index": 1},
    "all": {"sliced_sql": "SELECT *", "used_columns": [], "branch_index": None},
}


def test_path_suggestions_exclude_covered_include_uncovered_and_all():
    state = {"path_plans": json.dumps(_PLANS), "target_path": "daily"}
    # 'daily' est couvert par le test fraîchement généré (state.target_path),
    # 'weekly' par un test existant → seuls les non couverts + all restent.
    tests = [{"target_path": "daily"}]
    texts, rationales = _build_path_suggestions(state, tests)
    assert any("weekly" in t for t in texts)
    assert any("assemblage complet" in t for t in texts)
    assert not any("daily" in t for t in texts)  # couvert → pas reproposé
    assert all(t in rationales for t in texts)


def test_path_suggestions_all_covered_returns_only_remaining():
    state = {"path_plans": json.dumps(_PLANS), "target_path": None}
    tests = [
        {"target_path": "daily"},
        {"target_path": "weekly"},
        {"target_path": "all"},
    ]
    texts, _ = _build_path_suggestions(state, tests)
    assert texts == []


def test_path_suggestions_no_catalogue_returns_empty():
    assert _build_path_suggestions({}, []) == ([], {})


def test_route_set_target_path_goes_to_generator():
    assert route_agent_output({"agent_tool_call": "set_target_path"}) == "generator"
