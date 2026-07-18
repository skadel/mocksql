"""Garde de containment : joindre un segment relatif contrôlé par l'appelant
(model_name issu d'une requête HTTP, full_path du catch-all SPA…) à une racine
sans laisser `../` ou un chemin absolu s'échapper de cette racine.

Toute jointure `root / user_input` du codebase doit passer par ``safe_join`` :
un `model_name = "../../secret"` reçu par un endpoint lisait/écrivait/supprimait
sinon hors de la racine prévue (audit sécurité 2026-07).
"""

from pathlib import Path
from typing import Optional


def safe_join(root: Path, *parts: str, suffix: str = "") -> Optional[Path]:
    """Retourne ``root/parts…(+suffix)`` résolu si le résultat reste sous ``root``,
    sinon ``None``.

    ``root`` est résolu ; chaque élément de ``parts`` est traité comme un segment
    relatif. Un composant absolu, un `..` qui remonte au-dessus de ``root``, ou tout
    résultat hors racine donne ``None`` (jamais d'exception — l'appelant décide du
    code d'erreur : 404 pour une lecture, ValueError pour une écriture).
    """
    base = root.resolve()
    # Un segment absolu écrase le join côté pathlib → refus explicite.
    for part in parts:
        if part is None or Path(part).is_absolute():
            return None
    try:
        candidate = base.joinpath(*parts)
        if suffix:
            candidate = candidate.with_suffix(suffix)
        resolved = candidate.resolve()
    except (ValueError, OSError):
        return None
    if resolved != base and base not in resolved.parents:
        return None
    return resolved
