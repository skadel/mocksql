"""Store éphémère des « instructions supplémentaires » saisies par l'utilisateur
PENDANT qu'une génération est déjà en cours.

Le run en vol les CONSULTE (peek, non-destructif) au niveau du test_evaluator et du
conversational_agent ; ce qui n'a pas été appliqué en vol est rejoué après coup
(flush → replay côté front). Process-level et non persisté : c'est un tampon
transitoire de session, pas un état durable.

Chaque entrée : {"id": str, "text": str, "consumed": bool}.
Le flag ``consumed`` évite la double-application : le conversational_agent qui
applique une instruction en vol la marque consommée, et le flush de fin de run ne
la rejoue alors plus.
"""

import threading
import uuid

_LOCK = threading.Lock()
_STORE: dict[str, list[dict]] = {}


def add_instruction(session: str, text: str) -> int:
    """Enregistre une instruction et renvoie le nombre de non-consommées pour la session."""
    text = (text or "").strip()
    if not session or not text:
        return peek_count(session)
    with _LOCK:
        _STORE.setdefault(session, []).append(
            {"id": str(uuid.uuid4()), "text": text, "consumed": False}
        )
        return sum(1 for e in _STORE[session] if not e["consumed"])


def peek_instructions(session: str) -> list[str]:
    """Textes des instructions non consommées, dans l'ordre — SANS les retirer."""
    if not session:
        return []
    with _LOCK:
        return [e["text"] for e in _STORE.get(session, []) if not e["consumed"]]


def consume_instructions(session: str) -> list[str]:
    """Marque toutes les instructions non consommées comme consommées et renvoie leur texte.

    Appelé par le conversational_agent quand il les a effectivement prises en compte
    en vol → elles ne seront pas rejouées par le flush de fin de run.
    """
    if not session:
        return []
    with _LOCK:
        pending = [e for e in _STORE.get(session, []) if not e["consumed"]]
        for e in pending:
            e["consumed"] = True
        return [e["text"] for e in pending]


def flush_instructions(session: str) -> list[str]:
    """Renvoie les instructions non consommées puis VIDE entièrement la session.

    Appelé en fin de run (front) pour le replay de ce qui n'a pas été consommé en vol.
    """
    if not session:
        return []
    with _LOCK:
        entries = _STORE.pop(session, [])
        return [e["text"] for e in entries if not e["consumed"]]


def peek_count(session: str) -> int:
    """Nombre d'instructions non consommées pour la session."""
    if not session:
        return 0
    with _LOCK:
        return sum(1 for e in _STORE.get(session, []) if not e["consumed"])
