"""Régression : le travail sqlglot lourd de la génération (qualify + simplify + hint)
doit tourner HORS de la boucle asyncio, sinon un onglet qui analyse une requête large
gèle les streams SSE de tous les autres onglets (perte de parallélisme multi-onglets).

Cf. recommandation #1 « sortir le travail bloquant de la boucle » — le coût dominant
mesuré est sqlglot (~10s sur requêtes larges), pas le LLM (déjà en ``ainvoke``).
"""

import asyncio
import threading
import time

from build_query import examples_generator


async def test_aprepare_constraints_does_not_block_event_loop(monkeypatch):
    """Pendant l'exécution du passage sqlglot, une autre coroutine sur la même boucle
    (ex. le stream SSE d'un autre onglet) doit continuer à progresser."""

    def _blocking(sql, schema, dialect):
        time.sleep(0.3)  # tient lieu du qualify/simplify multi-secondes
        return ("SENTINEL", "hint")

    monkeypatch.setattr(
        examples_generator, "_prepare_generation_constraints", _blocking
    )

    progressed = 0

    async def ticker():
        nonlocal progressed
        while True:
            await asyncio.sleep(0.01)
            progressed += 1

    tk = asyncio.create_task(ticker())
    try:
        result = await examples_generator._aprepare_generation_constraints(
            "SELECT 1", None, "duckdb"
        )
    finally:
        tk.cancel()

    assert result == ("SENTINEL", "hint")
    # Si le travail bloquant avait tourné sur le thread de la boucle, le sleep de 0.3s
    # aurait affamé le ticker (progressed ~0). Hors-boucle, il avance librement.
    assert progressed >= 10, f"boucle asyncio bloquée (progressed={progressed})"


async def test_aprepare_constraints_runs_on_worker_thread(monkeypatch):
    """Délègue au helper sync, transmet les args tels quels, et tourne sur un thread
    worker (≠ thread de la boucle)."""
    loop_thread = threading.get_ident()
    captured = {}

    def _spy(sql, schema, dialect):
        captured["thread"] = threading.get_ident()
        captured["args"] = (sql, schema, dialect)
        return ("sim", "h")

    monkeypatch.setattr(examples_generator, "_prepare_generation_constraints", _spy)

    out = await examples_generator._aprepare_generation_constraints(
        "SELECT 42", [{"t": "x"}], "bigquery"
    )

    assert out == ("sim", "h")
    assert captured["args"] == ("SELECT 42", [{"t": "x"}], "bigquery")
    assert captured["thread"] != loop_thread
