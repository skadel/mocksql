"""Qualité du prompt de suggestions (cf. docs/spec-suggestions-robustesse.md) :

- W2 : le SQL brut noie le prompt sous les projections passthrough (``col AS col`` × N × CTEs)
  alors que ``build_sql_digest`` porte déjà la structure. ``compact_passthrough_sql`` compacte
  ces projections avant injection (échantillon + résumé chiffré), en gardant intactes les
  projections porteuses de logique (calculs, casts, renommages, agrégats).
- W3 : la règle « zéro jargon » était trop rigide pour les requêtes statistiques. Le prompt
  tolère désormais un terme d'analyse indispensable au sens et fournit un exemple analytique
  (une anomalie qui en masque une autre via la contamination de la référence de calcul).
"""

import json

import sqlglot

import pytest
from langchain_core.runnables import RunnableLambda

from build_query import prompt_tools, suggestions_node
from build_query.suggestions_node import (  # alias : évite la collecte pytest des noms Test*
    TestSuggestion as Suggestion,
    TestSuggestionsOutput as SuggestionsOutput,
)


# --- W2 : compaction des projections passthrough (helper pur) ------------------------------


def _noisy_sql(n_passthrough: int = 12) -> str:
    cols = ",\n         ".join(
        f"noise_col_{i} AS noise_col_{i}" for i in range(n_passthrough)
    )
    return f"""WITH stg AS (
  SELECT {cols},
         CAST(amount AS FLOAT64) AS amount_f
  FROM raw.events
)
SELECT region, SUM(amount_f) AS total
FROM stg
GROUP BY region"""


def test_compact_collapses_passthrough_keeps_logic():
    out = prompt_tools.compact_passthrough_sql(_noisy_sql(12), "bigquery")
    # Échantillon conservé, colonnes au-delà résumées (12 passthrough − 3 gardées = 9).
    assert "noise_col_0" in out
    assert "noise_col_11" not in out
    assert "autres colonnes transmises" in out
    # La projection porteuse de logique (le CAST) reste intacte.
    assert "amount_f" in out
    # Le résultat reste un SQL parseable (commentaire sqlglot, pas du texte cassé).
    sqlglot.parse_one(out, read="bigquery")


def test_compact_is_noop_below_threshold():
    sql = "SELECT a AS a, b AS b, SUM(x) AS s FROM t GROUP BY a, b"
    assert prompt_tools.compact_passthrough_sql(sql, "bigquery") == sql


def test_compact_invalid_sql_returns_original():
    garbage = "NOT @@@ VALID SQL"
    assert prompt_tools.compact_passthrough_sql(garbage, "bigquery") == garbage


def test_compact_handles_all_passthrough_select():
    cols = ", ".join(f"c{i} AS c{i}" for i in range(10))
    sql = f"WITH s AS (SELECT {cols} FROM t) SELECT c0 FROM s"
    out = prompt_tools.compact_passthrough_sql(sql, "bigquery")
    assert "autres colonnes transmises" in out
    assert "c0" in out  # au moins un échantillon → SELECT jamais vide
    sqlglot.parse_one(out, read="bigquery")  # reste valide


# --- Câblage dans generate_suggestions (W2 + W3) -------------------------------------------


def _install_capture(monkeypatch, captured: dict):
    def _capture(prompt_value):
        captured["system"] = prompt_value.to_messages()[0].content
        captured["user"] = prompt_value.to_messages()[1].content
        return SuggestionsOutput(
            analyse_des_manques="ok",
            suggestions=[Suggestion(text="Vérifie un cas", rationale="")],
        )

    class _FakeLLM:
        def with_structured_output(self, _model):
            return RunnableLambda(_capture)

    monkeypatch.setattr(suggestions_node, "make_llm", lambda *a, **k: _FakeLLM())
    monkeypatch.setattr(suggestions_node, "get_test", lambda *a, **k: {})
    monkeypatch.setattr(suggestions_node, "update_test", lambda *a, **k: None)
    monkeypatch.setattr(suggestions_node, "is_native_thinking_active", lambda: True)
    monkeypatch.setattr(
        "utils.saver.persist_completed_tests", lambda *a, **k: 0, raising=False
    )

    async def _fake_retrieve(session, state):
        return [
            {
                "test_index": 0,
                "test_name": "t",
                "unit_test_description": "desc",
                "status": "pass",
                "results_json": '[{"x": 1}]',
                "data": {"t": [{"x": 1}]},
            }
        ]

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _fake_retrieve)


async def _run_suggestions(monkeypatch, *, sql: str) -> dict:
    captured: dict = {}
    _install_capture(monkeypatch, captured)
    state = {
        "session": "s1",
        "query": sql,
        "messages": [],
        "agent_tool_args": {},
        "query_decomposed": json.dumps(
            [
                {
                    "name": "stg",
                    "code": sql,
                    "dependencies": [],
                    "sources": [{"table": "t"}],
                },
                {
                    "name": "final_query",
                    "code": sql,
                    "dependencies": ["stg"],
                    "sources": [],
                },
            ]
        ),
    }
    await suggestions_node.generate_suggestions(state)
    return captured


@pytest.mark.asyncio
async def test_suggestions_prompt_injects_compacted_sql(monkeypatch):
    """W2 — le SQL injecté dans le prompt est compacté : marqueur de résumé présent, colonnes
    passthrough au-delà de l'échantillon absentes."""
    captured = await _run_suggestions(monkeypatch, sql=_noisy_sql(12))
    user = captured["user"]
    assert "autres colonnes transmises" in user
    assert "noise_col_11" not in user


@pytest.mark.asyncio
async def test_suggestions_prompt_has_analytical_masking_example(monkeypatch):
    """W3 — le prompt fournit un exemple analytique (anomalie qui en masque une autre via la
    contamination de la référence de calcul)."""
    captured = await _run_suggestions(
        monkeypatch, sql="SELECT region, SUM(x) AS s FROM t GROUP BY region"
    )
    assert "une anomalie en masque une autre" in captured["user"]


@pytest.mark.asyncio
async def test_suggestions_prompt_tolerates_indispensable_technical_term(monkeypatch):
    """W3 — la règle jargon est assouplie : un terme d'analyse indispensable au sens est
    toléré (la prohibition vise le détail d'implémentation, pas le vocabulaire d'analyse)."""
    captured = await _run_suggestions(
        monkeypatch, sql="SELECT region, SUM(x) AS s FROM t GROUP BY region"
    )
    user = captured["user"]
    assert "indispensable" in user
