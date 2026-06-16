"""Régression — hint « agrégat statistique exige ≥2 points par groupe ».

Cas bq143 : `CORR(protein, expression)` groupé par `(gene, sample_type)`. Une
corrélation (comme STDDEV_SAMP / VAR_SAMP / COVAR_SAMP) renvoie NULL sur une seule
ligne → la CTE suivante filtre le NULL → résultat VIDE. Le générateur, faute de
consigne, met 1 ligne par groupe et la boucle bad_data épuise ses retries.

On détecte ces agrégats et on injecte un hint qui pousse le générateur vers ≥2 (voire
≥3 si un filtre de magnitude suit) lignes VARIÉES par groupe GROUP BY.
"""

from build_query.constraint_simplifier import detect_min_points_aggregates
from build_query.prompt_tools import _build_min_points_agg_hint_block

_CORR_SQL = """
WITH c AS (
  SELECT gene, sample_type, CORR(a, b) AS corr
  FROM t JOIN u ON t.id = u.id
  GROUP BY gene, sample_type
)
SELECT sample_type, AVG(corr) FROM c WHERE ABS(corr) <= 0.5 GROUP BY sample_type
"""

_PLAIN_SQL = "SELECT region, SUM(amount) FROM sales GROUP BY region"


def test_detects_corr_as_min_points_aggregate():
    assert "CORR" in detect_min_points_aggregates(_CORR_SQL, "duckdb")


def test_plain_sum_is_not_flagged():
    """SUM/COUNT/AVG tiennent sur une seule ligne → pas concernés."""
    assert detect_min_points_aggregates(_PLAIN_SQL, "duckdb") == []


def test_hint_block_warns_about_min_rows_per_group():
    block = _build_min_points_agg_hint_block(_CORR_SQL, "duckdb")
    assert block  # non vide quand un agrégat est détecté
    low = block.lower()
    assert "corr" in low
    # pousse vers plusieurs lignes par groupe et prévient du résultat vide
    assert "≥2" in block or "2 ligne" in low or "deux ligne" in low
    assert "null" in low or "vide" in low


def test_hint_block_empty_without_stat_aggregate():
    assert _build_min_points_agg_hint_block(_PLAIN_SQL, "duckdb") == ""
