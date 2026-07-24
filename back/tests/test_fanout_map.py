"""Phase 1.5a — carte « où est le fan-out » (localisation de la multiplicité).

But : donner au générateur (et à l'agent) une carte ACTIONNABLE des cardinalités de
jointure — nommer explicitement les joins un-à-plusieurs, l'axe où placer la
multiplicité pour un test de repro (Phase 2) et où NE PAS en fabriquer par erreur
(anti-faux-bug, cf. PLAN §1.5). Deux sources normalisées vers la même carte :

- ``fanout_cards_from_profile`` : cardinalités RÉELLES du profil (nécessite un scan
  DWH ; déjà collectées via ``profile_joins_for_query`` / ``describe_join``) ;
- ``fanout_cards_from_probes`` : sondes SYNTHÉTIQUES d'``inspect`` (comptages DuckDB
  sur les données générées — gratuit, zéro DWH).

Le formateur est pur (aucune I/O) → testable hors-ligne.
"""

from build_query.prompt_tools import (
    fanout_cards_from_probes,
    fanout_cards_from_profile,
    format_fanout_map,
)


# ── fanout_cards_from_probes (source synthétique inspect) ────────────────────


def test_probe_fanout_verdict_is_flagged():
    cards = fanout_cards_from_probes(
        [
            {
                "left": "orders",
                "right": "p.d.order_items",
                "join_type": "INNER",
                "left_rows": 90,
                "result_rows": 340,
                "verdict": "fan_out",
            }
        ]
    )
    assert len(cards) == 1
    c = cards[0]
    assert c["left"] == "orders"
    assert c["right"] == "order_items"  # dernier segment
    assert c["is_fanout"] is True
    assert "90" in c["detail"] and "340" in c["detail"]


def test_probe_preserves_is_not_fanout():
    cards = fanout_cards_from_probes(
        [
            {
                "left": "a",
                "right": "b",
                "left_rows": 10,
                "result_rows": 10,
                "verdict": "preserves",
            }
        ]
    )
    assert cards[0]["is_fanout"] is False


def test_probe_empty_input():
    assert fanout_cards_from_probes([]) == []
    assert fanout_cards_from_probes(None) == []


# ── fanout_cards_from_profile (source réelle profil) ─────────────────────────


def test_profile_one_to_many_is_flagged():
    profile = {
        "joins": [
            {
                "left_table": "p.d.orders",
                "right_table": "p.d.order_items",
                "join_type_profiled": "one-to-many",
                "avg_right_per_left_key": 3.8,
                "max_right_per_left_key": 5,
                "left_match_rate": 1.0,
            }
        ]
    }
    cards = fanout_cards_from_profile(profile)
    assert len(cards) == 1
    assert cards[0]["is_fanout"] is True
    # describe_join fournit une phrase actionnable en detail.
    assert "order_items" in cards[0]["detail"]


def test_profile_one_to_one_is_not_fanout():
    profile = {
        "joins": [
            {
                "left_table": "t",
                "right_table": "s",
                "join_type_profiled": "one-to-one",
                "avg_right_per_left_key": 1.0,
                "max_right_per_left_key": 1,
                "left_match_rate": 1.0,
            }
        ]
    }
    assert fanout_cards_from_profile(profile)[0]["is_fanout"] is False


def test_profile_many_to_many_max_gt_1_is_fanout():
    profile = {
        "joins": [
            {
                "left_table": "a",
                "right_table": "b",
                "join_type_profiled": "many-to-many",
                "max_right_per_left_key": 4,
            }
        ]
    }
    assert fanout_cards_from_profile(profile)[0]["is_fanout"] is True


def test_profile_no_joins_or_incomplete_skipped():
    assert fanout_cards_from_profile({}) == []
    assert fanout_cards_from_profile(None) == []
    # jointure sans tables → ignorée (pas de crash).
    assert (
        fanout_cards_from_profile({"joins": [{"join_type_profiled": "one-to-many"}]})
        == []
    )


# ── format_fanout_map (rendu du bloc) ────────────────────────────────────────


def test_format_empty_is_empty_string():
    assert format_fanout_map([]) == ""


def test_format_flags_fanout_join_with_actionable_tag():
    block = format_fanout_map(
        [
            {
                "left": "orders",
                "right": "order_items",
                "join_type": "INNER",
                "is_fanout": True,
                "detail": "90 → 340 lignes",
            },
            {
                "left": "orders",
                "right": "products",
                "join_type": "INNER",
                "is_fanout": False,
                "detail": None,
            },
        ]
    )
    # Les deux jointures sont listées.
    assert "`orders` ⋈ `order_items`" in block
    assert "`orders` ⋈ `products`" in block
    # Seule la 1-to-N porte le marqueur FAN-OUT actionnable.
    fanout_line = next(ln for ln in block.splitlines() if "order_items" in ln)
    other_line = next(ln for ln in block.splitlines() if "products" in ln)
    assert "FAN-OUT" in fanout_line
    assert "FAN-OUT" not in other_line
    # Le detail est rendu.
    assert "90 → 340" in fanout_line


def test_format_skips_cards_without_tables():
    assert format_fanout_map([{"left": "", "right": "x", "is_fanout": True}]) == ""
