"""Store des « instructions supplémentaires » saisies pendant une génération en cours.

Garantit le contrat utilisé par le run en vol (peek non-destructif) et le replay de
fin de run (flush des non-consommées) : cf. build_query/pending_instructions et la
consultation à chaud côté assertion_generator / conversational_agent.
"""

from build_query.pending_instructions import (
    add_instruction,
    consume_instructions,
    flush_instructions,
    peek_count,
    peek_instructions,
)


def test_add_peek_is_non_destructive():
    session = "sess-peek"
    add_instruction(session, "couvre le cas date NULL")
    add_instruction(session, "ajoute un client sans commande")

    # peek renvoie dans l'ordre et ne retire rien
    assert peek_instructions(session) == [
        "couvre le cas date NULL",
        "ajoute un client sans commande",
    ]
    assert peek_instructions(session) == [
        "couvre le cas date NULL",
        "ajoute un client sans commande",
    ]
    assert peek_count(session) == 2

    flush_instructions(session)  # cleanup


def test_consume_then_flush_returns_empty():
    session = "sess-consume"
    add_instruction(session, "instruction A")
    consumed = consume_instructions(session)
    assert consumed == ["instruction A"]
    # Consommée en vol → ni peek ni flush ne la rejouent (pas de double-application)
    assert peek_instructions(session) == []
    assert flush_instructions(session) == []


def test_flush_returns_unconsumed_and_clears():
    session = "sess-flush"
    add_instruction(session, "non consommée 1")
    add_instruction(session, "non consommée 2")
    assert flush_instructions(session) == ["non consommée 1", "non consommée 2"]
    # session vidée
    assert flush_instructions(session) == []
    assert peek_count(session) == 0


def test_blank_and_missing_session_are_noops():
    assert add_instruction("", "x") == 0
    assert add_instruction("sess-blank", "   ") == 0
    assert peek_instructions("sess-blank") == []
    assert peek_instructions("") == []
