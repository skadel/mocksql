"""Régression — hint « requête DÉTECTEUR (seuil sur valeur calculée) ».

Cas c6 : pipeline de détection d'anomalies (winsorisation + moyenne mobile pondérée
+ écart-type roulant → z-score modifié comparé à un seuil `> 1.2`). Le « cas nominal »
de cette classe de requêtes n'est PAS des données lisses (qui donnent un résultat VIDE)
mais l'événement de détection lui-même : une série de référence + un point déviant.

On détecte la forme (statistique fenêtrée/percentile/écart-type ET un seuil numérique
dans un WHERE/QUALIFY/HAVING) et on injecte un hint qui pousse vers ce contraste. Le
gate est volontairement large ; le guard `find_ancestor` évite de déclencher sur un
seuil porté par un `JOIN … ON`.
"""

from build_query.prompt_tools import _build_detector_query_hint_block

# Détecteur : écart-type fenêtré comparé à un seuil terminal.
_STDDEV_DETECTOR_SQL = """
WITH s AS (
  SELECT id, v, STDDEV(v) OVER (PARTITION BY id ORDER BY d) AS sd
  FROM t
)
SELECT * FROM s WHERE sd > 1.2
"""

# Détecteur : percentile fenêtré (winsorisation) + seuil.
_PERCENTILE_DETECTOR_SQL = """
WITH s AS (
  SELECT id, v, PERCENTILE_CONT(v, 0.95) OVER (PARTITION BY id) AS p95
  FROM t
)
SELECT * FROM s WHERE v > p95 AND v > 1000
"""

# Transformation : seuil présent mais dans un JOIN ON, aucune stat fenêtrée.
_JOIN_THRESHOLD_SQL = (
    "SELECT a.id, SUM(b.x) AS tot "
    "FROM a JOIN b ON a.id = b.id AND b.x > 0 GROUP BY a.id"
)

# Transformation pure : agrégat simple, aucun seuil sur valeur calculée.
_PLAIN_AGG_SQL = "SELECT id, COUNT(*) AS n FROM t WHERE statut = 'actif' GROUP BY id"

# Déduplication : fenêtre présente mais filtre d'égalité (`rn = 1`), pas un seuil.
_DEDUP_SQL = """
WITH r AS (
  SELECT id, ROW_NUMBER() OVER (PARTITION BY id ORDER BY d DESC) AS rn FROM t
)
SELECT * FROM r WHERE rn = 1
"""


def test_windowed_stddev_threshold_is_flagged():
    block = _build_detector_query_hint_block(_STDDEV_DETECTOR_SQL, "bigquery")
    assert block  # non vide
    low = block.lower()
    assert "détecteur" in low
    assert "vide" in low  # prévient du résultat vide sur données plates
    assert "déviant" in low or "contraste" in low or "déviation" in low


def test_percentile_threshold_is_flagged():
    assert _build_detector_query_hint_block(_PERCENTILE_DETECTOR_SQL, "bigquery")


def test_threshold_in_join_on_is_not_flagged():
    """Seuil porté par un JOIN ON, sans stat fenêtrée → pas un détecteur."""
    assert _build_detector_query_hint_block(_JOIN_THRESHOLD_SQL, "duckdb") == ""


def test_plain_aggregate_is_not_flagged():
    assert _build_detector_query_hint_block(_PLAIN_AGG_SQL, "duckdb") == ""


def test_dedup_equality_filter_is_not_flagged():
    """Fenêtre + `WHERE rn = 1` : égalité, pas un seuil → pas un détecteur."""
    assert _build_detector_query_hint_block(_DEDUP_SQL, "duckdb") == ""


def test_empty_sql_returns_empty():
    assert _build_detector_query_hint_block("", "duckdb") == ""
