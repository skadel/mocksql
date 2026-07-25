from pathlib import Path

from storage.config import get_models_path
from utils.path_guard import safe_join


def load_model_context(model_name: str) -> str:
    """Collect mocksql.md files for a model, ordered global → specific.

    Resolution order for model_name = "finance/revenue":
      1. <models_path>/mocksql.md          (project-wide context)
      2. <models_path>/finance/mocksql.md  (folder-level context)
      3. <models_path>/finance/revenue.md  (file-specific context)

    All found files are concatenated with a separator. Returns "" if none exist.
    """
    if not model_name:
        return ""

    models_path = get_models_path()
    parts = Path(model_name).parts  # e.g. ("finance", "revenue")

    # Garde traversal : `model_name` vient de l'appelant HTTP — un `../` ne doit pas
    # aspirer un `.md` hors de models_path (audit sécu 2026-07).
    if safe_join(models_path, model_name, suffix=".md") is None:
        return ""

    fragments: list[str] = []

    # Walk from models_path root down to the file's parent directory
    for i in range(len(parts)):
        level_dir = models_path.joinpath(*parts[:i])  # empty tuple → models_path itself
        candidate = level_dir / "mocksql.md"
        if candidate.exists():
            text = candidate.read_text(encoding="utf-8").strip()
            if text:
                fragments.append(text)

    # File-specific: <model_name>.md alongside the .sql file
    file_md = models_path / f"{model_name}.md"
    if file_md.exists():
        text = file_md.read_text(encoding="utf-8").strip()
        if text:
            fragments.append(text)

    return "\n\n---\n\n".join(fragments)
