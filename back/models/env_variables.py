import os
import sys

from dotenv import load_dotenv

from storage.config import (
    get_duckdb_path,
    get_langchain_api_key,
    get_langchain_tracing,
    get_profile_cache_path,
)

load_dotenv()

# ---------------------------------------------------------------------------
# Projet GCP pour Vertex AI / LLM (obligatoire)
#   Priorité : VERTEX_PROJECT > GOOGLE_CLOUD_PROJECT (var standard GCP SDK)
# ---------------------------------------------------------------------------
VERTEX_PROJECT = os.getenv("VERTEX_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
if VERTEX_PROJECT:
    os.environ.setdefault("VERTEX_PROJECT", VERTEX_PROJECT)
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


def validate_required_env() -> None:
    missing = [
        f"  • {name}  — {desc}" for name, desc in _REQUIRED if not os.getenv(name)
    ]
    if missing:
        print(
            "\n[MockSQL] Variables d'environnement obligatoires manquantes :\n"
            + "\n".join(missing)
            + "\n\nDéfinissez-les dans votre .env ou via l'environnement shell, puis relancez.\n"
            "Exemple minimal :\n"
            "  VERTEX_PROJECT=my-gcp-project    # ou GOOGLE_CLOUD_PROJECT si déjà configuré\n"
            "  BQ_TEST_PROJECT=my-bq-project    # optionnel — défaut : VERTEX_PROJECT\n",
            file=sys.stderr,
        )
        sys.exit(1)


# ---------------------------------------------------------------------------
# Variables optionnelles
# ---------------------------------------------------------------------------
DB_MODE = "duckdb"
DUCKDB_PATH = get_duckdb_path()
SCHEMA_CACHE_PATH = os.getenv("SCHEMA_CACHE_PATH", ".mocksql/schema_cache.json")
# Profil + sample_values (valeurs brutes issues de l'entrepôt → PII) : cache local
# SÉPARÉ et gitignoré, jamais commité. Le réplay CI n'en a pas besoin (cf. test_runner).
# Précédence : env PROFILE_CACHE_PATH > mocksql.yml `profile_cache` > .mocksql/profile.json.
PROFILE_CACHE_PATH = os.getenv("PROFILE_CACHE_PATH") or get_profile_cache_path()
AUTO_SCHEMA_IMPORT = True
AUTO_PROFILING = True

LANGCHAIN_API_KEY = get_langchain_api_key()
LANGCHAIN_TRACING = get_langchain_tracing()
os.environ.setdefault("LANGCHAIN_TRACING_V2", "true" if LANGCHAIN_TRACING else "false")
if LANGCHAIN_API_KEY:
    os.environ.setdefault("LANGCHAIN_API_KEY", LANGCHAIN_API_KEY)

# ---------------------------------------------------------------------------
# Snowflake (source optionnelle)
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "")
SNOWFLAKE_SCHEMA_NAME = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE", "")

# ---------------------------------------------------------------------------
# Trino (source optionnelle)
#   L'exécution reste DuckDB : Trino ne sert qu'à l'import de schéma, la
#   validation dry-run (EXPLAIN TYPE VALIDATE) et le profiling.
# ---------------------------------------------------------------------------
TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "mocksql")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
TRINO_HTTP_SCHEME = os.getenv("TRINO_HTTP_SCHEME", "http")
