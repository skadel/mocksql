"""Checkpoint incrémental des tests terminés (robustesse aux coupures).

Avant ce mécanisme, TOUTE la persistance (test_cases + historique) se faisait dans
``history_saver``, le dernier nœud du graph. Une coupure réseau en cours de boucle
multi-tests (crash d'un appel LLM avant la clôture) faisait perdre même les tests déjà
terminés. ``persist_completed_tests`` sauve sur disque le dernier RESULTS dès qu'un test
est réglé, aux frontières de la boucle — au pire on perd le test en cours, jamais les
précédents. C'est idempotent (merge par test_index), donc rejouable sans dommage.
"""

import json

import pytest
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda

from utils.msg_types import MsgType


def _results_msg(test_index, test_name):
    return AIMessage(
        content=json.dumps(
            [
                {
                    "test_index": test_index,
                    "test_uid": f"uid-{test_index}",
                    "test_name": test_name,
                    "data": {"t": [{"x": 1}]},
                    "assertion_results": [],
                }
            ]
        ),
        additional_kwargs={"type": MsgType.RESULTS},
    )


@pytest.fixture
def model_on_disk(tmp_path, monkeypatch):
    """Crée un modèle vide sur disque sous un .mocksql temporaire et renvoie sa session."""
    monkeypatch.setenv("MOCKSQL_BASE_DIR", str(tmp_path))
    import storage.config as config

    config.load_config.cache_clear()
    from storage.test_repository import create_test, update_test

    test = create_test("my_model")
    # Flush en mémoire → disque (create_test garde en _pending tant qu'aucun update).
    update_test(test["test_id"], {"sql": "SELECT 1"})
    return test["test_id"]


def test_persist_completed_tests_writes_last_results(model_on_disk):
    from utils.saver import persist_completed_tests
    from storage.test_repository import get_test

    state = {"session": model_on_disk, "messages": [_results_msg(0, "nominal")]}

    n = persist_completed_tests(state)

    assert n == 1
    stored = get_test(model_on_disk)
    assert [c["test_index"] for c in stored["test_cases"]] == [0]
    assert stored["test_cases"][0]["test_name"] == "nominal"


def test_persist_completed_tests_is_idempotent(model_on_disk):
    from utils.saver import persist_completed_tests
    from storage.test_repository import get_test

    state = {"session": model_on_disk, "messages": [_results_msg(0, "nominal")]}
    persist_completed_tests(state)
    persist_completed_tests(state)  # rejoué : ne doit pas dupliquer

    stored = get_test(model_on_disk)
    assert len(stored["test_cases"]) == 1


def test_persist_completed_tests_noop_without_results(model_on_disk):
    from utils.saver import persist_completed_tests

    assert persist_completed_tests({"session": model_on_disk, "messages": []}) == 0


@pytest.mark.asyncio
async def test_loop_checkpoints_previous_test_before_next(model_on_disk, monkeypatch):
    """Cœur du scénario utilisateur : pendant la boucle multi-tests, le test précédent
    est persisté AVANT de générer le suivant. Si une coupure survient ensuite (avant
    ``history_saver``), le test 0 est déjà sur disque."""
    from build_query import suggestions_node
    from build_query.suggestions_node import generate_single_suggestion
    from storage.test_repository import get_test

    async def _no_tests(_session, _state):
        return []

    monkeypatch.setattr(suggestions_node, "retrieve_existing_tests", _no_tests)
    fake = RunnableLambda(
        lambda _pv: suggestions_node.TestSuggestionsOutput(
            analyse_des_manques="x",
            suggestions=[suggestions_node.TestSuggestion(text="Vérifie le cas NULL")],
        )
    )

    class _FakeLLM:
        def with_structured_output(self, _schema):
            return fake

    monkeypatch.setattr(suggestions_node, "make_llm", lambda: _FakeLLM())

    # Le test 0 vient d'être réglé : son RESULTS est dans le state, mais history_saver
    # n'a pas encore tourné → rien sur disque pour l'instant.
    assert get_test(model_on_disk)["test_cases"] == []

    state = {
        "session": model_on_disk,
        "query": "SELECT 1",
        "dialect": "bigquery",
        "messages": [_results_msg(0, "nominal")],
        "auto_tests_built": 0,
    }
    await generate_single_suggestion(state)

    # Le test 0 a été checkpointé avant la construction du test suivant.
    stored = get_test(model_on_disk)
    assert [c["test_index"] for c in stored["test_cases"]] == [0]
