"""Connecteur dbt — résout un modèle dbt depuis son `manifest.json` + SQL compilé.

Pourquoi ce module : sans lui, MockSQL ne sait tester que des fichiers `.sql` plats
(éventuellement passés par un préprocesseur regex fragile). Avec un projet dbt, les
`ref()`/`source()` ne résolvent pas vers des tables réelles → schémas introuvables,
marts non testables. Le manifest dbt porte la résolution exacte (`relation_name`,
`depends_on`) et le SQL compilé (macros/`var()`/`this` déjà rendus). Les colonnes des
modèles amont viennent du yml (`nodes[].columns`), ce qui permet de les **mocker comme
tables de base sans jamais interroger l'entrepôt** — c'est ce qui rend les marts
évaluables offline (0 € facturé), quel que soit le warehouse derrière dbt.

Découvertes terrain (cf. spikes) qui dictent l'implémentation :
- `compiled_code`/`compiled_path` sont souvent **vides** dans le manifest → on lit les
  fichiers `target/compiled/<package>/<original_file_path>` sur disque.
- Les **sources** n'ont fréquemment **aucune** colonne yml → fallback inférence par usage.
- Les colonnes yml n'ont pas de `data_type` ici → défaut `STRING`, descriptions propagées.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

import sqlglot
from sqlglot import exp


def _strip_relation(relation_name: str) -> str:
    """`"scratch"."mart_gtfs"."dim_agency"` → `scratch.mart_gtfs.dim_agency`."""
    return relation_name.replace('"', "").replace("`", "")


def _yml_columns_to_schema_cols(columns: dict[str, Any]) -> list[dict]:
    """Convertit `nodes[].columns` (yml dbt) au format colonne MockSQL.

    Le yml dbt ne porte pas toujours de `data_type` → défaut `STRING` (MockSQL
    génère les données ; le type n'est qu'un indice). La description est propagée
    car elle améliore la qualité de génération du LLM.
    """
    cols: list[dict] = []
    for name, meta in (columns or {}).items():
        meta = meta or {}
        cols.append(
            {
                "name": name,
                "type": (meta.get("data_type") or "STRING").upper(),
                "mode": "NULLABLE",
                "description": (meta.get("description") or "").strip(),
            }
        )
    return cols


class DbtProject:
    """Vue indexée d'un projet dbt compilé (manifest + fichiers compilés)."""

    def __init__(self, project_dir: Path, target_path: str = "target") -> None:
        self.project_dir = Path(project_dir).resolve()
        self.target_dir = self.project_dir / target_path
        self.manifest_path = self.target_dir / "manifest.json"

    # ── chargement / index ────────────────────────────────────────────────
    @property
    def manifest(self) -> dict:
        return _load_manifest(str(self.manifest_path))

    @property
    def _relation_index(self) -> dict[str, dict]:
        """`scratch.mart_gtfs.dim_agency` (lowercase) → nœud (model ou source)."""
        return _build_relation_index(str(self.manifest_path))

    # ── résolution d'un modèle ────────────────────────────────────────────
    def find_node(self, model_name: str) -> Optional[dict]:
        """Trouve le nœud modèle pour un identifiant MockSQL (chemin relatif depuis
        models_path, ex. ``mart/gtfs_schedule_latest/dim_agency_latest``).

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

    # ── schémas amont (sans warehouse) ────────────────────────────────────
    def schemas_for_sql(
        self, compiled_sql: str, dialect: str = "bigquery"
    ) -> tuple[list[dict], list[str]]:
        """Construit les schémas des tables référencées dans `compiled_sql`.

        Retourne ``(schemas, unresolved)`` au format cache MockSQL
        (``{table_name, description, columns}``). Pour chaque ref :
        - nœud modèle/source avec colonnes yml → schéma depuis le yml ;
        - source sans yml → inférence des colonnes par usage (best-effort) ;
        - ref non trouvée dans le manifest → ajoutée à ``unresolved``.
        """
        from utils.sql_code import extract_real_table_refs

        refs = extract_real_table_refs(compiled_sql, dialect)
        index = self._relation_index
        schemas: list[dict] = []
        unresolved: list[str] = []
        seen: set[str] = set()

        sources_without_cols: list[tuple[str, dict]] = []

        for r in refs:
            qualified = ".".join(p for p in [r.catalog, r.db, r.name] if p)
            key = qualified.lower()
            if key in seen:
                continue
            seen.add(key)

            node = index.get(key)
            if node is None:
                unresolved.append(qualified)
                continue

            cols = _yml_columns_to_schema_cols(node.get("columns") or {})
            if cols:
                schemas.append(
                    {
                        "table_name": qualified,
                        "description": (node.get("description") or "").strip(),
                        "columns": cols,
                    }
                )
            else:
                # Source/modèle non documenté → inférence par usage en 2ème passe.
                sources_without_cols.append((qualified, node))

        # 2ème passe : inférer les colonnes des tables non documentées depuis l'usage.
        for qualified, node in sources_without_cols:
            inferred = _infer_columns_from_usage(compiled_sql, qualified, dialect)
            schemas.append(
                {
                    "table_name": qualified,
                    "description": (node.get("description") or "").strip(),
                    "columns": [
                        {
                            "name": c,
                            "type": "STRING",
                            "mode": "NULLABLE",
                            "description": "",
                        }
                        for c in inferred
                    ],
                }
            )

        return schemas, unresolved


def _infer_columns_from_usage(sql: str, qualified: str, dialect: str) -> list[str]:
    """Best-effort : colonnes attribuables à `qualified` quand son schéma est inconnu.

    Un `SELECT *` depuis la table empêche sqlglot d'attribuer précisément les colonnes ;
    on retombe alors sur l'ensemble des colonnes nues référencées dans la requête
    (sur-ensemble inoffensif : des colonnes en trop élargissent juste la table mockée).
    """
    try:
        ast = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return []
    names = {
        c.name
        for c in ast.find_all(exp.Column)
        if c.name and not c.name.startswith("_col")
    }
    return sorted(names)


@lru_cache(maxsize=8)
def _load_manifest(manifest_path: str) -> dict:
    p = Path(manifest_path)
    if not p.exists():
        raise FileNotFoundError(
            f"manifest.json introuvable : {p}. Lance `dbt compile` dans le projet dbt."
        )
    with open(p, encoding="utf-8") as f:
        return json.load(f)


@lru_cache(maxsize=8)
def _build_relation_index(manifest_path: str) -> dict[str, dict]:
    manifest = _load_manifest(manifest_path)
    index: dict[str, dict] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        rel = node.get("relation_name")
        if rel:
            index[_strip_relation(rel).lower()] = node
    for src in manifest.get("sources", {}).values():
        rel = src.get("relation_name")
        if rel:
            index[_strip_relation(rel).lower()] = src
    return index
