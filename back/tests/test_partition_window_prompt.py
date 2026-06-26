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


class TestProfileBlockCrossProjectLeak(unittest.TestCase):
    """W6 — une table absente de `used_columns` ne doit JAMAIS apparaître dans le bloc,
    même quand elle porte des `derived_expressions`. Régression : un profil contaminé par
    des tables d'autres projets (cache PII partagé) fuitait leurs expressions via la boucle
    derived_expressions, qui sautait son filtre quand `wanted_cols` était vide."""

    def _profile(self) -> dict:
        return {
            "tables": {
                # Table pertinente : référencée par la requête.
                "proj.MARKETING.datamart_commercants": {
                    "columns": {
                        "valeur": {
                            "min_value": 10,
                            "max_value": 99,
                            "distinct_count": 40,
                        },
                    },
                },
                # Table ÉTRANGÈRE : absente de used_columns, mais porte une expr dérivée.
                "other_proj.genomics.quant_proteome_ccrcc": {
                    "derived_expressions": [
                        {
                            "expr_sql": "CAST(quant.protein_abundance_log2ratio AS FLOAT64)",
                            "top_values": [-0.1822, -0.3095, -0.5247],
                        }
                    ],
                },
            },
            "joins": [],
        }

    def test_foreign_table_with_derived_expr_is_not_leaked(self):
        used = [
            {"table": "proj.MARKETING.datamart_commercants", "used_columns": ["valeur"]}
        ]
        block = _format_profile_block(self._profile(), used)
        # La table pertinente est présente…
        self.assertIn("datamart_commercants", block)
        self.assertIn("`valeur`", block)
        # …mais aucune trace de la table étrangère ni de son expression.
        self.assertNotIn("quant_proteome_ccrcc", block)
        self.assertNotIn("protein_abundance_log2ratio", block)


class TestProfileBlockJoinPartitionHint(unittest.TestCase):
    """Le bloc profil annote les match_rate de jointures bornés par fenêtre, et
    masque le 0% trompeur quand les fenêtres sont disjointes."""

    @staticmethod
    def _two_partitioned_tables() -> dict:
        return {
            "project.dataset.events": {
                "columns": {},
                "partition_window": {
                    "field": "event_date",
                    "limit": 3,
                    "exact": False,
                },
            },
            "project.dataset.sessions": {
                "columns": {},
                "partition_window": {
                    "field": "session_date",
                    "limit": 3,
                    "exact": False,
                },
            },
        }

    def test_disjoint_join_masks_zero_and_warns(self):
        profile = {
            "tables": self._two_partitioned_tables(),
            "joins": [
                {
                    "left_table": "project.dataset.events",
                    "right_table": "project.dataset.sessions",
                    "left_expr": "e.user_id",
                    "right_expr": "s.user_id",
                    "left_match_rate": 0.0,
                    "window_disjoint": True,
                }
            ],
        }
        block = _format_profile_block(profile, [])
        self.assertIn("Jointures profilées", block)
        self.assertIn("match indéterminé", block)
        self.assertNotIn("match=0%", block)

    def test_partitioned_join_with_match_gets_bounded_note(self):
        profile = {
            "tables": self._two_partitioned_tables(),
            "joins": [
                {
                    "left_table": "project.dataset.events",
                    "right_table": "project.dataset.sessions",
                    "left_expr": "e.user_id",
                    "right_expr": "s.user_id",
                    "left_match_rate": 0.6,
                }
            ],
        }
        block = _format_profile_block(profile, [])
        self.assertIn("match=60%", block)
        self.assertIn("stats bornées aux dernières partitions", block)

    def test_unpartitioned_join_no_bounded_note(self):
        profile = {
            "tables": {
                "project.dataset.events": {"columns": {}},
                "project.dataset.sessions": {"columns": {}},
            },
            "joins": [
                {
                    "left_table": "project.dataset.events",
                    "right_table": "project.dataset.sessions",
                    "left_expr": "e.user_id",
                    "right_expr": "s.user_id",
                    "left_match_rate": 0.6,
                }
            ],
        }
        block = _format_profile_block(profile, [])
        self.assertIn("match=60%", block)
        self.assertNotIn("stats bornées", block)


class TestFlagDisjointPartitionJoins(unittest.TestCase):
    """flag_disjoint_partition_joins ne marque un join que si les DEUX côtés sont
    partitionnés et que le match mesuré est 0."""

    @staticmethod
    def _profile(left_window: bool, right_window: bool, rate) -> dict:
        events: dict = {"columns": {}}
        sessions: dict = {"columns": {}}
        if left_window:
            events["partition_window"] = {"field": "event_date", "limit": 3}
        if right_window:
            sessions["partition_window"] = {"field": "session_date", "limit": 3}
        return {
            "tables": {
                "project.dataset.events": events,
                "project.dataset.sessions": sessions,
            },
            "joins": [
                {
                    "left_table": "project.dataset.events",
                    "right_table": "project.dataset.sessions",
                    "left_match_rate": rate,
                }
            ],
        }

    def test_both_partitioned_zero_match_flagged(self):
        from build_query.profile_checker import flag_disjoint_partition_joins

        p = flag_disjoint_partition_joins(self._profile(True, True, 0.0))
        self.assertTrue(p["joins"][0].get("window_disjoint"))

    def test_one_side_unpartitioned_not_flagged(self):
        from build_query.profile_checker import flag_disjoint_partition_joins

        p = flag_disjoint_partition_joins(self._profile(True, False, 0.0))
        self.assertNotIn("window_disjoint", p["joins"][0])

    def test_both_partitioned_nonzero_match_not_flagged(self):
        from build_query.profile_checker import flag_disjoint_partition_joins

        p = flag_disjoint_partition_joins(self._profile(True, True, 0.42))
        self.assertNotIn("window_disjoint", p["joins"][0])

    def test_no_joins_returned_unchanged(self):
        from build_query.profile_checker import flag_disjoint_partition_joins

        self.assertEqual(
            flag_disjoint_partition_joins({"tables": {}, "joins": []}),
            {"tables": {}, "joins": []},
        )


if __name__ == "__main__":
    unittest.main()
