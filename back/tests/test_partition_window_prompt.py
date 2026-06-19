"""Tests that the partition window is surfaced in the generation prompt block."""

import unittest

from build_query.prompt_tools import (
    _format_partition_window,
    _format_profile_block,
)


class TestFormatPartitionWindow(unittest.TestCase):
    def test_exact_window_mentions_range_and_scope(self):
        note = _format_partition_window(
            {
                "field": "event_date",
                "limit": 3,
                "exact": True,
                "min": "2026-06-17",
                "max": "2026-06-19",
            }
        )
        self.assertIn("3 dernières partitions", note)
        self.assertIn("2026-06-17", note)
        self.assertIn("2026-06-19", note)
        self.assertIn("PAS l'historique complet", note)

    def test_inexact_window_omits_dates(self):
        note = _format_partition_window(
            {"field": "_PARTITIONDATE", "limit": 2, "exact": False}
        )
        self.assertIn("_PARTITIONDATE", note)
        self.assertIn("2 dernières partitions", note)
        self.assertNotIn("→", note)

    def test_no_window_returns_empty(self):
        self.assertEqual(_format_partition_window(None), "")
        self.assertEqual(_format_partition_window({}), "")


class TestProfileBlockWindowNote(unittest.TestCase):
    def test_window_note_in_table_header(self):
        profile = {
            "tables": {
                "project.dataset.events": {
                    "row_count": 3,
                    "columns": {
                        "event_date": {
                            "type": "DATE",
                            "min_value": "2026-06-17",
                            "max_value": "2026-06-19",
                            "distinct_count": 3,
                        }
                    },
                    "partition_window": {
                        "field": "event_date",
                        "limit": 3,
                        "exact": True,
                        "min": "2026-06-17",
                        "max": "2026-06-19",
                    },
                }
            },
            "joins": [],
        }
        used = [{"table": "project.dataset.events", "used_columns": ["event_date"]}]
        block = _format_profile_block(profile, used)
        self.assertIn("table `events`", block)
        self.assertIn("profilé sur les 3 dernières partitions", block)
        self.assertIn("2026-06-17", block)

    def test_no_window_no_note(self):
        profile = {
            "tables": {
                "project.dataset.events": {
                    "row_count": 3,
                    "columns": {
                        "event_date": {"type": "DATE", "distinct_count": 3},
                    },
                }
            },
            "joins": [],
        }
        used = [{"table": "project.dataset.events", "used_columns": ["event_date"]}]
        block = _format_profile_block(profile, used)
        self.assertIn("table `events`", block)
        self.assertNotIn("partitions", block)


if __name__ == "__main__":
    unittest.main()
