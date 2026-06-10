"""Régression : le JSON produit par le LLM contient souvent des apostrophes
échappées « à la C » (`\\'`) à l'intérieur d'une string SQL embarquée, ce qui est
un échappement illégal en JSON. `json.loads` plante alors avec « Invalid \\escape »
et chaque correction d'assertion (regen_assertion / assertion_fixer) est jetée."""

import json

import pytest

from utils.llm_errors import loads_lenient_json


# Reproduit exactement le payload des logs (Caisse d\'Epargne dans un NOT IN).
BROKEN_PAYLOAD = (
    "{\n"
    '  "description": "Vérifie que toutes les transactions appartiennent à BP ou CE.",\n'
    '  "sql": "SELECT * FROM __result__ WHERE groupe_bancaire_principal '
    "NOT IN ('BPCE', 'Banque Populaire', 'Caisse d\\'Epargne')\"\n"
    "}"
)


def test_stdlib_json_rejects_the_broken_payload():
    """Garde-fou : confirme que le payload réel casse bien json.loads standard."""
    with pytest.raises(json.JSONDecodeError):
        json.loads(BROKEN_PAYLOAD)


def test_lenient_parses_invalid_single_quote_escape():
    parsed = loads_lenient_json(BROKEN_PAYLOAD)
    assert parsed["sql"].endswith("'Caisse d'Epargne')")
    assert "BPCE" in parsed["sql"]


def test_lenient_preserves_valid_escapes():
    """Les échappements JSON légaux (\\n, \\", \\\\) ne doivent pas être altérés."""
    raw = '{"sql": "SELECT \\"a\\\\b\\"\\nFROM t"}'
    parsed = loads_lenient_json(raw)
    assert parsed["sql"] == 'SELECT "a\\b"\nFROM t'


def test_lenient_is_noop_on_clean_json():
    raw = '{"description": "ok", "sql": "SELECT 1"}'
    assert loads_lenient_json(raw) == {"description": "ok", "sql": "SELECT 1"}


def test_lenient_reraises_on_genuinely_broken_json():
    with pytest.raises(json.JSONDecodeError):
        loads_lenient_json('{"sql": "SELECT 1"')  # accolade non fermée
