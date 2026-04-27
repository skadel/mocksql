import os
import sys

from dotenv import load_dotenv

from storage.config import (
    get_bq_test_dataset,
    get_langchain_api_key,
    get_langchain_tracing,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Projet GCP pour Vertex AI / LLM (obligatoire)
#   Priorité : VERTEX_PROJECT > GOOGLE_CLOUD_PROJECT (var standard GCP SDK)
# ---------------------------------------------------------------------------
VERTEX_PROJECT = os.getenv("VERTEX_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
if VERTEX_PROJECT:
    os.environ.setdefault("GOOGLE_CLOUD_PROJECT", VERTEX_PROJECT)

# ---------------------------------------------------------------------------
# Projet GCP pour BigQuery — dry-runs, schémas, import de tables (optionnel)
#   Défaut : VERTEX_PROJECT (ou GOOGLE_CLOUD_PROJECT si déjà configuré GCP)
# ---------------------------------------------------------------------------
BQ_TEST_PROJECT = os.getenv("BQ_TEST_PROJECT") or VERTEX_PROJECT

# ---------------------------------------------------------------------------
# Validation des variables obligatoires
# ---------------------------------------------------------------------------
_REQUIRED: list[tuple[str, str]] = [
    ("VERTEX_PROJECT", "Projet GCP pour Vertex AI / LLM (ou GOOGLE_CLOUD_PROJECT)"),
]

_missing = [
    f"  • {name}  — {desc}"
    for name, desc in _REQUIRED
    if not os.getenv(name)
    and not (name == "VERTEX_PROJECT" and os.getenv("GOOGLE_CLOUD_PROJECT"))
]

if _missing:
    print(
        "\n[MockSQL] Variables d'environnement obligatoires manquantes :\n"
        + "\n".join(_missing)
        + "\n\nDéfinissez-les dans votre .env ou via l'environnement shell, puis relancez.\n"
        "Exemple minimal :\n"
        "  VERTEX_PROJECT=my-gcp-project          # projet Vertex AI\n"
        "  BQ_TEST_PROJECT=my-bq-project           # optionnel — défaut : VERTEX_PROJECT\n",
        file=sys.stderr,
    )
    sys.exit(1)

# ---------------------------------------------------------------------------
# Variables optionnelles
# ---------------------------------------------------------------------------
DB_MODE = "duckdb"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/mocksql.duckdb")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION")
EMBEDDING = os.getenv("EMBEDDING_MODEL", "text-multilingual-embedding-002")
DEFAULT_MODEL_NAME = os.getenv("DEFAULT_MODEL_NAME", "gemini-2.0-flash-lite")
ROUTING_MODEL = os.getenv("ROUTING_MODEL", DEFAULT_MODEL_NAME)
SOLVER_MODEL = os.getenv("SOLVER_MODEL", DEFAULT_MODEL_NAME)
ANALYSIS_MODEL = os.getenv("ANALYSIS_MODEL", DEFAULT_MODEL_NAME)
GENERATOR_MODEL = os.getenv("SOLVER_MODEL", DEFAULT_MODEL_NAME)
EXPLAIN_MODEL = os.getenv("EXPLAIN_MODEL", DEFAULT_MODEL_NAME)
OTHER_MODEL = os.getenv("SOLVER_MODEL", DEFAULT_MODEL_NAME)
SQLMESH_META_DB = os.getenv("SQLMESH_META_DB")
BQ_TEST_DATASET = get_bq_test_dataset()
BQ_SANDBOX_MODE = os.getenv("BQ_SANDBOX_MODE", "false").lower() == "true"
BQ_SCHEMA_BILLING_PROJECT = os.getenv("BQ_SCHEMA_BILLING_PROJECT") or BQ_TEST_PROJECT
SCHEMA_CACHE_PATH = os.getenv("SCHEMA_CACHE_PATH", ".mocksql/schema_cache.json")
AUTO_SCHEMA_IMPORT = True
AUTO_PROFILING = True

LANGCHAIN_API_KEY = get_langchain_api_key()
LANGCHAIN_TRACING = get_langchain_tracing()
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true" if LANGCHAIN_TRACING else "false")
if LANGCHAIN_API_KEY:
    os.environ.setdefault("LANGCHAIN_API_KEY", LANGCHAIN_API_KEY)
