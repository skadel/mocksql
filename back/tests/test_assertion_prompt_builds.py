"""Régression : la construction du prompt de `_generate_assertions_and_evaluate`
ne doit jamais lever (ex. `NameError` sur un nom non importé).

Le `system_content` est bâti EN TÊTE de la fonction, AVANT le `try/except` qui
garde l'appel LLM (lequel retombe sur un verdict "Bon" en cas d'erreur). Une
référence non importée dans ce bloc (incident : `tag_labels()` appelé sans import)
lève donc une exception NON attrapée qui remonte hors du nœud `assertion_generator`,
avorte le graphe LangGraph (~30 ms, aucun appel LLM) et casse le flux SSE côté front.

Ce test mocke le LLM : le seul point de défaillance restant est la construction du
prompt. Il échoue (NameError non attrapée) sur le code d'avant le fix, passe après.
"""

import pandas as pd

from build_query.examples_executor import (
    _Assertion,
    _AssertionsAndEvaluation,
    _generate_assertions_and_evaluate,
)


class _FakeStructured:
    async def ainvoke(self, _messages):
        return _AssertionsAndEvaluation(
            reasoning="ok",
            assertions=[
                _Assertion(
                    description="La ligne d'ouverture existe.",
                    expected_condition="typ_client = 'OUVERTURE'",
                )
            ],
            verdict="Bon",
            explanation="ok",
        )


class _FakeLLM:
    def with_structured_output(self, _schema):
        return _FakeStructured()


class _RaisingStructured:
    async def ainvoke(self, _messages):
        raise RuntimeError("boom — LLM indisponible")


class _RaisingLLM:
    def with_structured_output(self, _schema):
        return _RaisingStructured()


async def test_assertions_prompt_builds_without_nameerror(monkeypatch):
    """Le prompt (qui interpole `tag_labels()`) doit se construire sans lever.

    Avant le fix (import `tag_labels` manquant), l'interpolation levait un
    `NameError` non attrapé — pas de fallback "Bon", exception propagée.
    """
    monkeypatch.setattr("build_query.examples_executor.make_llm", lambda: _FakeLLM())

    result_df = pd.DataFrame([{"total": 150, "typ_client": "OUVERTURE"}])

    # con=None + view_name="" → schéma lu depuis les dtypes pandas (pas de moteur
    # requis). Le corps traverse tout le bloc de construction du system prompt.
    result = await _generate_assertions_and_evaluate(
        duckdb_sql="SELECT total, typ_client FROM t",
        test_data=[{"total": 150, "typ_client": "OUVERTURE"}],
        result_df=result_df,
        test_description="Cas nominal : une ouverture de compte.",
        con=None,
        view_name="",
    )

    assert result.verdict == "Bon"


async def test_llm_failure_fallback_is_schema_valid(monkeypatch):
    """Quand l'appel LLM lève, le repli doit renvoyer un `_AssertionsAndEvaluation`
    VALIDE (schéma : `reasoning` + ≥1 assertion), pas lever de `ValidationError`.

    Régression : l'ancien repli construisait `assertions=[]` sans `reasoning` → une
    ValidationError NON attrapée remontait hors du nœud et cassait le graphe, exactement
    le crash que le repli est censé absorber. Le repli émet un pin `COUNT(*) = N` brut,
    dédupliqué en aval par `_is_bare_rowcount_pin`.
    """
    monkeypatch.setattr("build_query.examples_executor.make_llm", lambda: _RaisingLLM())

    result_df = pd.DataFrame([{"a": 1}, {"a": 2}, {"a": 3}])

    result = await _generate_assertions_and_evaluate(
        duckdb_sql="SELECT a FROM t",
        test_data=[{"a": 1}],
        result_df=result_df,
        test_description="Cas nominal.",
        con=None,
        view_name="",
    )

    assert result.verdict == "Bon"
    assert len(result.assertions) == 1
    # Pin de cardinalité brut aligné sur le nombre de lignes réel (dédupliqué en aval).
    assert result.assertions[0].expected_condition == "COUNT(*) = 3"
