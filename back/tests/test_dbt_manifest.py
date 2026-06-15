"""Tests for storage/dbt_manifest.py — résolution dbt + schémas amont offline.

Tests hermétiques (manifest synthétique + arborescence temporaire), plus un test
d'intégration optionnel sur le vrai manifest calitp (data-infra/warehouse) s'il existe.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from storage.dbt_manifest import (
    DbtProject,
    _build_relation_index,
    _load_manifest,
    _strip_relation,
    _yml_columns_to_schema_cols,
)


def _write_project(root: Path) -> None:
    """Crée un mini-projet dbt compilé : 1 mart -> 1 modèle parent + 1 source."""
    target = root / "target"
    (target).mkdir(parents=True)

    manifest = {
        "nodes": {
            "model.pkg.dim_agency_latest": {
                "resource_type": "model",
                "name": "dim_agency_latest",
                "package_name": "pkg",
                "original_file_path": "models\\mart\\dim_agency_latest.sql",
                "relation_name": '"scratch"."mart"."dim_agency_latest"',
                "description": "Latest agencies",
                "columns": {},
                "depends_on": {"nodes": ["model.pkg.dim_agency"]},
            },
            "model.pkg.dim_agency": {
                "resource_type": "model",
                "name": "dim_agency",
                "package_name": "pkg",
                "original_file_path": "models\\mart\\dim_agency.sql",
                "relation_name": '"scratch"."mart"."dim_agency"',
                "description": "Agencies",
                "columns": {
                    "agency_id": {"description": "PK"},
                    "feed_key": {"data_type": "string", "description": "FK"},
                },
            },
            # homonyme dans un autre dossier (pour tester la désambiguïsation)
            "model.pkg.dim_agency_other": {
                "resource_type": "model",
                "name": "dim_agency",
                "package_name": "pkg",
                "original_file_path": "models\\other\\dim_agency.sql",
                "relation_name": '"scratch"."other"."dim_agency"',
                "columns": {},
            },
        },
        "sources": {
            "source.pkg.raw.agency": {
                "resource_type": "source",
                "name": "agency",
                "relation_name": '"raw_db"."raw"."agency"',
                "description": "raw feed",
                "columns": {},  # source non documentée -> inférence par usage
            }
        },
    }
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    compiled = target / "compiled" / "pkg" / "models" / "mart"
    compiled.mkdir(parents=True)
    (compiled / "dim_agency_latest.sql").write_text(
        'SELECT * FROM "scratch"."mart"."dim_agency" t '
        'WHERE EXISTS (SELECT 1 FROM "raw_db"."raw"."agency" s WHERE s.agency_id = t.agency_id)',
        encoding="utf-8",
    )


class TestHelpers(unittest.TestCase):
    def test_strip_relation(self):
        self.assertEqual(
            _strip_relation('"scratch"."mart"."dim_agency"'),
            "scratch.mart.dim_agency",
        )

    def test_yml_columns_default_type_and_description(self):
        cols = _yml_columns_to_schema_cols(
            {"a": {"description": "x"}, "b": {"data_type": "int64"}}
        )
        by = {c["name"]: c for c in cols}
        self.assertEqual(by["a"]["type"], "STRING")  # défaut
        self.assertEqual(by["a"]["description"], "x")  # propagée
        self.assertEqual(by["b"]["type"], "INT64")  # depuis yml, uppercased


class TestDbtProject(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        _write_project(self.root)
        # caches lru sont mémoïsés par chemin -> chemins temporaires uniques, ok.
        _load_manifest.cache_clear()
        _build_relation_index.cache_clear()
        self.proj = DbtProject(self.root)

    def tearDown(self):
        self._tmp.cleanup()

    def test_find_node_unique(self):
        node = self.proj.find_node("mart/dim_agency_latest")
        self.assertIsNotNone(node)
        self.assertEqual(node["name"], "dim_agency_latest")

    def test_find_node_disambiguates_homonyms_by_path(self):
        node = self.proj.find_node("other/dim_agency")
        self.assertEqual(node["original_file_path"], "models\\other\\dim_agency.sql")
        node2 = self.proj.find_node("mart/dim_agency")
        self.assertEqual(node2["original_file_path"], "models\\mart\\dim_agency.sql")

    def test_find_node_missing(self):
        self.assertIsNone(self.proj.find_node("nope/missing"))
        self.assertFalse(self.proj.is_dbt_model("nope/missing"))

    def test_compiled_sql_read_from_disk(self):
        sql = self.proj.compiled_sql_for_model("mart/dim_agency_latest")
        self.assertIn("dim_agency", sql)
        self.assertIn("EXISTS", sql)

    def test_compiled_sql_missing_raises(self):
        node = self.proj.find_node("mart/dim_agency")  # pas de fichier compilé écrit
        with self.assertRaises(FileNotFoundError):
            self.proj.compiled_sql(node)

    def test_schemas_for_sql_model_uses_yml_columns(self):
        sql = self.proj.compiled_sql_for_model("mart/dim_agency_latest")
        schemas, unresolved = self.proj.schemas_for_sql(sql, dialect="bigquery")
        self.assertEqual(unresolved, [])
        by_name = {s["table_name"]: s for s in schemas}
        self.assertIn("scratch.mart.dim_agency", by_name)
        cols = {c["name"] for c in by_name["scratch.mart.dim_agency"]["columns"]}
        self.assertEqual(cols, {"agency_id", "feed_key"})

    def test_schemas_for_sql_source_without_yml_infers_from_usage(self):
        sql = self.proj.compiled_sql_for_model("mart/dim_agency_latest")
        schemas, _ = self.proj.schemas_for_sql(sql, dialect="bigquery")
        by_name = {s["table_name"]: s for s in schemas}
        self.assertIn("raw_db.raw.agency", by_name)
        cols = {c["name"] for c in by_name["raw_db.raw.agency"]["columns"]}
        # agency_id apparaît dans l'usage -> inféré
        self.assertIn("agency_id", cols)


@unittest.skipUnless(
    Path("C:/Users/skhir/workspace/data-infra/warehouse/target/manifest.json").exists(),
    "manifest calitp réel absent — test d'intégration sauté",
)
class TestRealCalitpManifest(unittest.TestCase):
    """Verrouille le comportement sur le vrai manifest (le banc d'essai des spikes)."""

    def setUp(self):
        _load_manifest.cache_clear()
        _build_relation_index.cache_clear()
        self.proj = DbtProject(Path("C:/Users/skhir/workspace/data-infra/warehouse"))

    def test_resolves_dim_agency_latest_and_parents(self):
        node = self.proj.find_node("mart/gtfs_schedule_latest/dim_agency_latest")
        self.assertIsNotNone(node)
        sql = self.proj.compiled_sql(node)
        schemas, unresolved = self.proj.schemas_for_sql(sql, dialect="bigquery")
        by_name = {s["table_name"]: s for s in schemas}
        # les deux modèles parents sont résolus avec colonnes yml
        self.assertIn("scratch.mart_gtfs.dim_agency", by_name)
        self.assertIn("scratch.mart_gtfs.dim_schedule_feeds", by_name)
        feeds_cols = {
            c["name"]
            for c in by_name["scratch.mart_gtfs.dim_schedule_feeds"]["columns"]
        }
        self.assertIn("_is_current", feeds_cols)
        self.assertIn("key", feeds_cols)


if __name__ == "__main__":
    unittest.main()
