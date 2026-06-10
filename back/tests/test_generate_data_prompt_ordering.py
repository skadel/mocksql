"""Ordre des messages de generate_data_prompt.

La référence (schéma/SQL/contraintes) doit précéder le few-shot pour l'ancrer,
et la tâche (`<task>`) doit rester dans le DERNIER message (recency).
"""

import json

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

from build_query.prompt_tools import generate_data_prompt
from utils.msg_types import MsgType

_UC = [
    {
        "database": "MARKETING",
        "table": "banques",
        "used_columns": ["code_banque", "reseau"],
    }
]
_SQL = "SELECT code_banque FROM MARKETING.banques WHERE reseau = 'BP'"


def _hist(content, msg_type):
    return HumanMessage(content=content, additional_kwargs={"type": msg_type})


def _build(history):
    return generate_data_prompt(
        history=history,
        dialect="bigquery",
        format_instructions="FORMAT_INSTRUCTIONS",
        used_columns=_UC,
        sql=_SQL,
    ).format_messages()


# Les balises <schema>/<task>/<constraints> sont aussi *mentionnées* dans le texte
# du system et de la tâche → on identifie les vrais blocs par le DÉBUT du message.
def _ref_idx(msgs):
    return next(i for i, m in enumerate(msgs) if m.content.startswith("<schema>"))


def _ask_idx(msgs):
    return next(
        i for i, m in enumerate(msgs) if m.content.lstrip().startswith("<task>")
    )


def test_system_first():
    msgs = _build([])
    assert isinstance(msgs[0], SystemMessage)


def test_reference_precedes_task_no_history():
    msgs = _build([])
    assert _ref_idx(msgs) < _ask_idx(msgs)


def test_task_is_in_last_message():
    msgs = _build([])
    assert msgs[-1].content.lstrip().startswith("<task>")
    # le vrai bloc schéma (header inclus) n'est PAS dans le message d'ask
    assert "**Tables sources à peupler**" not in msgs[-1].content


def test_fewshot_grounded_between_reference_and_task():
    test_case = {
        "unit_test_description": "Pour le réseau BP → 1 ligne.",
        "tags": ["Logique métier"],
        "data": {"MARKETING_banques": [{"code_banque": "001", "reseau": "BP"}]},
        "test_index": 0,
        "results_json": '[{"code_banque": "001"}]',
    }
    history = [
        _hist("Génère un test.", MsgType.QUERY),
        _hist(json.dumps([test_case]), MsgType.EXAMPLES),
        _hist(json.dumps([test_case]), MsgType.RESULTS),
    ]
    msgs = _build(history)
    ai_idx = next(i for i, m in enumerate(msgs) if isinstance(m, AIMessage))
    # référence (schéma/requête) → exemple few-shot → tâche
    assert _ref_idx(msgs) < ai_idx < _ask_idx(msgs)
