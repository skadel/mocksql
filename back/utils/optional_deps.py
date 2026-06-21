"""Imports paresseux des dépendances optionnelles (connecteurs sources).

Les connecteurs vers les entrepôts (BigQuery, Snowflake) sont des dépendances
lourdes (pyarrow, grpc…) inutiles au cœur du produit (génération + exécution
DuckDB). Ils sont déclarés en `extras` dans pyproject.toml et importés à la
demande via ces helpers, qui lèvent un message d'installation clair si l'extra
correspondant n'est pas installé.
"""

from __future__ import annotations


def import_bigquery():
    """Retourne le module `google.cloud.bigquery` ou lève un message clair."""
    try:
        from google.cloud import bigquery

        return bigquery
    except ImportError as e:
        raise ImportError(
            "Le connecteur BigQuery n'est pas installé. "
            "Installez l'extra correspondant : pip install mocksql[bigquery]"
        ) from e
