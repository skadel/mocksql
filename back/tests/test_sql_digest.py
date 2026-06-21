"""Pré-digestion structurelle partagée `build_sql_digest`.

Produit un aperçu compact du pipeline de CTEs à partir de `query_decomposed` (déjà
produit par le validator), réutilisable par plusieurs prompts. Heuristique regex,
aucun re-parse sqlglot.
"""

import json

from build_query.prompt_tools import build_sql_digest


def _steps() -> list[dict]:
    return [
        {
            "name": "daily",
            "code": "SELECT d, SUM(amount) AS total FROM raw.sales WHERE d >= '2024-01-01' GROUP BY d",
            "dependencies": [],
            "sources": [{"table": "raw.sales"}],
        },
        {
            "name": "ranked",
            "code": "SELECT d, total, ROW_NUMBER() OVER (ORDER BY total DESC) AS rk FROM daily",
            "dependencies": ["daily"],
            "sources": [],
        },
        {
            "name": "final_query",
            "code": "SELECT r.d, r.total, dim.label FROM ranked r JOIN ref.dim dim ON dim.d = r.d WHERE r.rk <= 10",
            "dependencies": ["ranked"],
            "sources": [{"table": "ref.dim"}],
        },
    ]


def test_digest_lists_steps_in_order_with_inputs_and_ops():
    out = build_sql_digest(_steps())
    assert "Structure de la requête" in out
    lines = [ln for ln in out.splitlines() if ln.startswith("- ")]
    assert len(lines) == 3
    # Ordre préservé.
    assert lines[0].startswith("- `daily`")
    assert lines[1].startswith("- `ranked`")
    assert lines[2].startswith("- `final_query`")


def test_digest_detects_operations():
    out = build_sql_digest(_steps())
    assert "agrège" in out  # GROUP BY / SUM dans daily
    assert "fenêtre" in out  # OVER( dans ranked
    assert "jointure" in out  # JOIN dans final_query
    assert "filtre" in out  # WHERE


def test_digest_shows_inputs_cte_and_base_table():
    out = build_sql_digest(_steps())
    # daily lit une table de base ; ranked lit la CTE daily.
    assert "table `sales`" in out
    assert "`daily`" in out
    assert "**résultat final**" in out


def test_digest_accepts_json_string():
    out = build_sql_digest(json.dumps(_steps()))
    assert "`ranked`" in out


def test_digest_empty_for_trivial_or_missing():
    assert build_sql_digest(None) == ""
    assert build_sql_digest([]) == ""
    # Une seule étape : le SQL brut se suffit → pas de digest.
    assert build_sql_digest([{"name": "final_query", "code": "SELECT 1"}]) == ""


def test_digest_unnest_not_reported_as_join():
    steps = [
        {
            "name": "a",
            "code": "SELECT id, arr FROM t",
            "dependencies": [],
            "sources": [{"table": "t"}],
        },
        {
            "name": "final_query",
            "code": "SELECT a.id, item FROM a CROSS JOIN UNNEST(a.arr) AS item",
            "dependencies": ["a"],
            "sources": [],
        },
    ]
    out = build_sql_digest(steps)
    assert "jointure" not in out
    assert "déplie un tableau" in out


def test_digest_malformed_json_returns_empty():
    assert build_sql_digest("{not json") == ""
