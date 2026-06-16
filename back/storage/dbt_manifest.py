"""Connecteur dbt — résout un modèle dbt et fournit son SQL compilé.

Rôle (volontairement étroit) : le connecteur dbt fait **deux** choses, rien de plus.
1. **Résolution** : retrouver le nœud dbt d'un modèle (par chemin relatif).
2. **Compile** : fournir le SQL **compilé** (`dbt compile` a déjà retiré tout le Jinja —
   `ref()`/`source()`/`var()`/`this`/macros → SQL plat avec noms de tables réels). Ça
   remplace le préprocesseur regex `replace_vars`.

La **récupération du schéma reste côté MockSQL** : une fois le SQL compilé fourni, le
flux `generate` classique extrait les refs et importe les schémas via son chemin normal
(`fetch_tables_schema` BigQuery aujourd'hui, connecteurs warehouse à venir). Le connecteur
dbt n'infère PAS de colonnes (rejeté : fabrication fragile, sans types, contraire au
correctness-first) et ne lit PAS `catalog.json`.

Note : `compiled_code`/`compiled_path` sont souvent vides dans le manifest selon la
commande dbt utilisée → on lit le fichier `target/compiled/<package>/<original_file_path>`.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Optional


class DbtProject:
    """Vue d'un projet dbt compilé (manifest + fichiers SQL compilés)."""

    def __init__(self, project_dir: Path, target_path: str = "target") -> None:
        self.project_dir = Path(project_dir).resolve()
        self.target_dir = self.project_dir / target_path
        self.manifest_path = self.target_dir / "manifest.json"

    @property
    def manifest(self) -> dict:
        return _load_manifest(str(self.manifest_path))

    def find_node(self, model_name: str) -> Optional[dict]:
        """Trouve le nœud modèle pour un identifiant MockSQL (chemin relatif depuis
        models_path, ex. ``marts/core/sales``).

        On matche par nom de modèle (= stem du fichier) et on désambiguïse via le
        suffixe de `original_file_path` quand plusieurs modèles partagent un nom.
        """
        stem = Path(model_name).stem
        want = Path(model_name).with_suffix(".sql").as_posix().lower()
        candidates = [
            n
            for n in self.manifest.get("nodes", {}).values()
            if n.get("resource_type") == "model" and n.get("name") == stem
        ]
        if not candidates:
            return None
        if len(candidates) == 1:
            return candidates[0]
        for n in candidates:
            ofp = (n.get("original_file_path") or "").replace("\\", "/").lower()
            if ofp.endswith(want):
                return n
        return candidates[0]

    def is_dbt_model(self, model_name: str) -> bool:
        return self.find_node(model_name) is not None

    def compiled_sql(self, node: dict) -> str:
        """Lit le SQL compilé sur disque (`target/compiled/<package>/<file>`).

        On ne se fie pas au champ `compiled_code` du manifest : il est souvent vide
        selon la commande dbt utilisée. Le fichier compilé est la source fiable.
        """
        rel = (node.get("original_file_path") or "").replace("\\", "/")
        path = self.target_dir / "compiled" / node["package_name"] / rel
        if not path.exists():
            raise FileNotFoundError(
                f"SQL compilé introuvable pour '{node.get('name')}': {path}. "
                f"Lance `dbt compile` (sélectionne au moins ce modèle)."
            )
        return path.read_text(encoding="utf-8")

    def compiled_sql_for_model(self, model_name: str) -> Optional[str]:
        node = self.find_node(model_name)
        return self.compiled_sql(node) if node else None


@lru_cache(maxsize=8)
def _load_manifest(manifest_path: str) -> dict:
    p = Path(manifest_path)
    if not p.exists():
        raise FileNotFoundError(
            f"manifest.json introuvable : {p}. Lance `dbt compile` dans le projet dbt."
        )
    with open(p, encoding="utf-8") as f:
        return json.load(f)
