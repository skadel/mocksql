"""
TICKET-1 (P0, trust-critical) — Protection d'une prémisse utilisateur dans la
boucle bad_data.

Péché cardinal d'un outil de test : l'utilisateur affirme « pour Y j'attends X »,
l'exécution donne Z (vide/cassé), et la boucle d'auto-correction mute SILENCIEUSEMENT
la valeur énoncée vers ce qui rend le test vert — sans jamais le signaler. L'ingé
perd le seul signal qui comptait.

Détection : authorship EXPLICITE — le test porte un marqueur `user_premise` (la
prémisse en langage naturel tracée à la création). Pas d'heuristique sur le texte.

Enforcement : INSTRUCTION DE PROMPT — quand le marqueur est présent, le trigger
`bad_data` envoyé à l'agent doit l'orienter vers `request_reevaluation` /
`ask_clarification` (→ délégation VALIDATION_PROMPT) plutôt que vers un patch muet.
Sans le marqueur, le trigger reste la correction ciblée normale (on ne sur-déclenche
pas le stop-and-ask sur des données purement auto-générées).
"""

import json

import pytest
from langchain_core.messages import AIMessage, HumanMessage

from build_query.conversational_agent import conversational_agent
from build_query.examples_generator import _resolve_user_premise
from utils.msg_types import MsgType


class FakeLLM:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def bind_tools(self, _tools):
        return self

    async def ainvoke(self, messages):
        self.calls.append(messages)
        return self._responses.pop(0)


def _patch_batch():
    """Un lot de patch quelconque : sert juste à laisser l'agent terminer son tour.
    Le test n'observe que le TRIGGER envoyé (fake.calls[0]), pas la sortie."""
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "patch_test_field",
                "args": {
                    "test_uid": "a3f9",
                    "table": "ds_correspondance",
                    "row_index": 0,
                    "field": "code_produit",
                    "value_json": json.dumps("'PROD1'"),
                },
                "id": "call_0",
            }
        ],
    )


def _state_auto_correct(user_premise=None):
    test = {
        "test_uid": "a3f9",
        "test_index": "1",
        "test_name": "Cartes",
        "unit_test_description": "un client avec 2 cartes ouvre un compte",
        "data": {
            "ds_correspondance": [
                {"code_produit": "PROD1"},
                {"code_produit": "PROD2"},
            ]
        },
        "status": "empty_results",
        "failing_cte": "temp_carte",
        "results_json": "[]",
        "assertion_results": [],
    }
    if user_premise is not None:
        test["user_premise"] = user_premise
    results_msg = AIMessage(
        content=json.dumps([test]),
        id="r1",
        additional_kwargs={"type": MsgType.RESULTS},
    )
    eval_msg = AIMessage(
        content="**Insuffisant** — temp_carte vide",
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": "1",
            "parent": "r1",
        },
    )
    return {
        "session": "sess1",
        "messages": [results_msg, eval_msg],
        "dialect": "duckdb",
        "query": "SELECT 1",
        "optimized_sql": "",
        "query_decomposed": "[]",
        "input": "",
        "evaluation_feedback": "bad_data",
        "auto_correct": True,
        "gen_retries": 2,
    }


def _trigger_text(messages):
    """Concatène le contenu des HumanMessage du tour : c'est là que vit le trigger."""
    return "\n".join(str(m.content) for m in messages if isinstance(m, HumanMessage))


@pytest.mark.asyncio
async def test_user_premise_routes_to_delegation_not_silent_patch(monkeypatch):
    """Test AUTHORED par l'user (user_premise présent) en échec bad_data : le trigger
    doit orienter vers la délégation (request_reevaluation / ask_clarification) plutôt
    que d'inviter à muter en silence la valeur énoncée."""
    fake = FakeLLM([_patch_batch()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(
        _state_auto_correct(user_premise="un client avec 2 cartes")
    )

    trigger = _trigger_text(fake.calls[0])
    # L'agent doit se voir proposer la sortie « déléguer à l'humain ».
    assert "request_reevaluation" in trigger
    # Et le trigger doit nommer la prémisse à protéger (pas une instruction générique).
    assert "2 cartes" in trigger


@pytest.mark.asyncio
async def test_no_premise_keeps_normal_targeted_correction(monkeypatch):
    """Données purement auto-générées (pas de user_premise) : le trigger reste la
    correction ciblée habituelle — on ne sur-déclenche PAS le stop-and-ask, sinon la
    boucle bad_data ne corrigerait plus rien toute seule (régression fdp 11/11)."""
    fake = FakeLLM([_patch_batch()])
    monkeypatch.setattr("build_query.conversational_agent.make_llm", lambda: fake)

    await conversational_agent(_state_auto_correct(user_premise=None))

    trigger = _trigger_text(fake.calls[0])
    # Pas de bascule délégation dans la branche non-debug sans prémisse à protéger.
    assert "request_reevaluation" not in trigger


# ── Population de `user_premise` à la génération (_resolve_user_premise) ──────


def test_premise_traced_for_explicit_new_test():
    """Nouveau test issu d'une instruction explicite de l'user (input présent, tests
    déjà existants, pas de retry) → la prémisse est tracée."""
    state = {"input": "un client avec 2 cartes", "evaluation_feedback": None}
    existing = [{"test_index": "1", "test_uid": "a3f9"}]
    assert (
        _resolve_user_premise(state, existing, existing_tc=None)
        == "un client avec 2 cartes"
    )


def test_no_premise_on_initial_bulk_generation():
    """Génération initiale en masse (aucun test existant) : pas de prémisse par test,
    même si `input` est renseigné — ce n'est pas une affirmation concrète par test."""
    state = {"input": "génère des tests pour ce modèle", "evaluation_feedback": None}
    assert _resolve_user_premise(state, existing_tests=[], existing_tc=None) is None


def test_no_premise_during_bad_data_retry():
    """Boucle de retry bad_data : `input` peut être périmé → on ne trace PAS une
    nouvelle prémisse depuis lui."""
    state = {"input": "stale retry input", "evaluation_feedback": "bad_data"}
    existing = [{"test_index": "1", "test_uid": "a3f9"}]
    assert _resolve_user_premise(state, existing, existing_tc=None) is None


def test_no_premise_on_machine_suggestion():
    """Suggestion machine (boucle batch ou clic sur le panneau, `suggestion_intent`) :
    le texte est écrit par MockSQL, pas affirmé par l'utilisateur → jamais de prémisse.
    Sans ce garde, le premise_guard de la boucle bad_data refuse de patcher les données
    et pousse vers ask_clarification — sans destinataire en batch."""
    state = {
        "input": "Un client PRO→PART n'apparaît pas dans le résultat final.",
        "evaluation_feedback": None,
        "suggestion_intent": True,
    }
    existing = [{"test_index": "1", "test_uid": "a3f9"}]
    assert _resolve_user_premise(state, existing, existing_tc=None) is None


def test_premise_preserved_on_regeneration():
    """Régénération/retry d'un test existant : l'authorship déjà tracée est reportée,
    jamais perdue."""
    state = {"input": "", "evaluation_feedback": "bad_data"}
    existing_tc = {"test_index": "1", "test_uid": "a3f9", "user_premise": "2 cartes"}
    assert (
        _resolve_user_premise(state, [existing_tc], existing_tc=existing_tc)
        == "2 cartes"
    )


def test_no_premise_when_targeting_existing_without_one():
    """Cibler un test existant qui n'a pas de prémisse n'en invente pas une depuis
    l'input courant (sinon une simple modif deviendrait une prémisse protégée)."""
    state = {"input": "ajoute une ligne", "evaluation_feedback": None}
    existing_tc = {"test_index": "1", "test_uid": "a3f9"}
    assert _resolve_user_premise(state, [existing_tc], existing_tc=existing_tc) is None
