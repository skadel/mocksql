"""Tests for storage/dbt_manifest.py — résolution dbt + lecture du SQL compilé.

Tests hermétiques (manifest synthétique + arborescence temporaire), plus un test
d'intégration optionnel sur le vrai manifest calitp (data-infra/warehouse) s'il existe.
"""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from storage.dbt_manifest import DbtProject, _load_manifest


def _write_project(root: Path) -> None:
    """Crée un mini-projet dbt compilé : 1 mart + un homonyme dans un autre dossier."""
    target = root / "target"
    target.mkdir(parents=True)

    manifest = {
        "nodes": {
            "model.pkg.sales": {
                "resource_type": "model",
                "name": "sales",
                "package_name": "pkg",
                "original_file_path": "models\\marts\\sales.sql",
                "relation_name": '"db"."marts"."sales"',
            },
            "model.pkg.dim_agency": {
                "resource_type": "model",
                "name": "dim_agency",
                "package_name": "pkg",
                "original_file_path": "models\\mart\\dim_agency.sql",
            },
            # homonyme dans un autre dossier (désambiguïsation par chemin)
            "model.pkg.dim_agency_other": {
                "resource_type": "model",
                "name": "dim_agency",
                "package_name": "pkg",
                "original_file_path": "models\\other\\dim_agency.sql",
            },
        },
    }
    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    compiled = target / "compiled" / "pkg" / "models" / "marts"
    compiled.mkdir(parents=True)
    (compiled / "sales.sql").write_text(
        'SELECT * FROM "db"."marts"."int_orders"', encoding="utf-8"
    )


class TestDbtProject(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        _write_project(self.root)
        _load_manifest.cache_clear()
        self.proj = DbtProject(self.root)

    def tearDown(self):
        self._tmp.cleanup()

    def test_find_node_unique(self):
        node = self.proj.find_node("marts/sales")
        self.assertIsNotNone(node)
        self.assertEqual(node["name"], "sales")

    def test_find_node_disambiguates_homonyms_by_path(self):
        node = self.proj.find_node("other/dim_agency")
        self.assertEqual(node["original_file_path"], "models\\other\\dim_agency.sql")
        node2 = self.proj.find_node("mart/dim_agency")
        self.assertEqual(node2["original_file_path"], "models\\mart\\dim_agency.sql")

    def test_find_node_missing(self):
        self.assertIsNone(self.proj.find_node("nope/missing"))
        self.assertFalse(self.proj.is_dbt_model("nope/missing"))
        self.assertTrue(self.proj.is_dbt_model("marts/sales"))

    def test_compiled_sql_read_from_disk(self):
        sql = self.proj.compiled_sql_for_model("marts/sales")
        self.assertIn("int_orders", sql)

    def test_compiled_sql_missing_raises(self):
        node = self.proj.find_node("mart/dim_agency")  # pas de fichier compilé écrit
        with self.assertRaises(FileNotFoundError):
            self.proj.compiled_sql(node)


@unittest.skipUnless(
    Path("C:/Users/skhir/workspace/data-infra/warehouse/target/manifest.json").exists(),
    "manifest calitp réel absent — test d'intégration sauté",
)
class TestRealCalitpManifest(unittest.TestCase):
    """Verrouille la résolution + lecture du compilé sur le vrai manifest."""

    def setUp(self):
        _load_manifest.cache_clear()
        self.proj = DbtProject(Path("C:/Users/skhir/workspace/data-infra/warehouse"))

    def test_resolves_and_reads_compiled_dim_agency_latest(self):
        node = self.proj.find_node("mart/gtfs_schedule_latest/dim_agency_latest")
        self.assertIsNotNone(node)
        sql = self.proj.compiled_sql(node)
        # le Jinja a été rendu → refs réels présents, pas de {{ }}
        self.assertNotIn("{{", sql)
        self.assertIn("dim_agency", sql)


if __name__ == "__main__":
    unittest.main()
