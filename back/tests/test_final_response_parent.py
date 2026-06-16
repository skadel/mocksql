"""Le message de clôture (final_response) doit se rattacher au dernier message
*persisté* de la requête courante.

Piège : dans le flux 1ʳᵉ génération, l'ordre est
``... → EVALUATION → SUGGESTIONS → final_response``. Or les messages SUGGESTIONS
ne sont **pas** persistés dans l'historique (ils vivent dans un panneau dédié, cf.
``history_saver``). Si final_response prend la SUGGESTIONS comme parent, le parent
est orphelin au rechargement → chaîne cassée. Il doit donc sauter les types
non-persistés et se rattacher à l'EVALUATION.
"""

from langchain_core.messages import AIMessage

from build_query.final_response_node import _parent_for_summary
from utils.msg_types import MsgType


def _msg(msg_type, msg_id, request_id="req-1", parent=None):
    return AIMessage(
        content="x",
        id=msg_id,
        additional_kwargs={
            "type": msg_type,
            "request_id": request_id,
            "parent": parent,
        },
    )


def test_parent_skips_suggestions_message():
    """final_response doit chaîner sur l'EVALUATION, pas sur la SUGGESTIONS non-persistée."""
    state = {
        "request_id": "req-1",
        "parent_message_id": "pmsg-1",
        "messages": [
            _msg(MsgType.RESULTS, "res-1"),
            _msg(MsgType.EVALUATION, "eval-1", parent="res-1"),
            _msg(MsgType.SUGGESTIONS, "sugg-1", parent="eval-1"),
        ],
    }
    assert _parent_for_summary(state) == "eval-1"


def test_parent_skips_examples_message():
    """Les EXAMPLES non plus ne sont pas persistés → on les saute aussi."""
    state = {
        "request_id": "req-1",
        "parent_message_id": "pmsg-1",
        "messages": [
            _msg(MsgType.EVALUATION, "eval-1"),
            _msg(MsgType.EXAMPLES, "ex-1", parent="eval-1"),
        ],
    }
    assert _parent_for_summary(state) == "eval-1"


def test_parent_is_last_persisted_message_normally():
    """Cas nominal : le dernier message persisté de la requête est bien le parent."""
    state = {
        "request_id": "req-1",
        "parent_message_id": "pmsg-1",
        "messages": [
            _msg(MsgType.RESULTS, "res-1"),
            _msg(MsgType.EVALUATION, "eval-1", parent="res-1"),
        ],
    }
    assert _parent_for_summary(state) == "eval-1"


def test_parent_falls_back_when_no_request_message():
    """Aucun message de la requête courante → repli sur parent_message_id."""
    state = {
        "request_id": "req-1",
        "parent_message_id": "pmsg-1",
        "messages": [_msg(MsgType.EVALUATION, "eval-0", request_id="other-req")],
    }
    assert _parent_for_summary(state) == "pmsg-1"
