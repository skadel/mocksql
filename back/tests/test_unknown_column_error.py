"""Régression : un `OptimizeError: Unknown column: X` (schéma en cache périmé —
typiquement un `SELECT *` sur une table dont le cache ne contient qu'une partie
des colonnes) ne doit PAS être renvoyé au solver LLM comme « please fix the
following error » (le SQL est valide, l'inviter à le « réparer » est inutile).

Il doit produire un message terminal actionnable poussant vers `refresh-schemas`.
"""

from sqlglot.errors import OptimizeError

from utils.errors import handle_post_compile_exceptions


def _flatten(payload) -> str:
    parts = [str(payload.get("error", ""))]
    for m in payload.get("messages", []) or []:
        parts.append(str(getattr(m, "content", "")))
    for m in payload.get("solver_messages", []) or []:
        parts.append(str(getattr(m, "content", "")))
    return "\n".join(parts)


class TestUnknownColumnOptimizeError:
    def test_unknown_column_points_to_refresh_schemas(self):
        exc = OptimizeError("Unknown column: no_contrat_commercant")
        payload = handle_post_compile_exceptions(exc=exc, code="SELECT 1")
        text = _flatten(payload)

        assert "refresh-schemas" in text
        assert "no_contrat_commercant" in text

    def test_unknown_column_not_routed_to_llm_solver(self):
        exc = OptimizeError("Unknown column: no_contrat_commercant")
        payload = handle_post_compile_exceptions(exc=exc, code="SELECT 1")

        # Pas de boucle de correction LLM : ni solver_messages, ni « please fix ».
        assert "solver_messages" not in payload
        assert "please fix" not in _flatten(payload).lower()
        # Un message terminal de type erreur est présent.
        assert payload.get("messages")

    def test_other_optimize_error_still_goes_to_solver(self):
        """Une OptimizeError sans « Unknown column » garde le comportement existant."""
        exc = OptimizeError("Cannot automatically join: no common columns")
        payload = handle_post_compile_exceptions(exc=exc, code="SELECT 1")

        assert "solver_messages" in payload
