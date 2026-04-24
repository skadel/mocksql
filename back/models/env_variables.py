import os

from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = os.getenv("PROJECT_ID")
DB_MODE = "duckdb"
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "data/mocksql.duckdb")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
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
BQ_TEST_DATASET = os.getenv("BQ_TEST_DATASET", "test_dataset")
BQ_TEST_PROJECT = os.getenv("BQ_TEST_PROJECT") or os.getenv("PROJECT_ID")
BQ_SANDBOX_MODE = os.getenv("BQ_SANDBOX_MODE", "false").lower() == "true"
BQ_SCHEMA_BILLING_PROJECT = os.getenv("BQ_SCHEMA_BILLING_PROJECT") or os.getenv(
    "PROJECT_ID"
)
SCHEMA_CACHE_PATH = os.getenv("SCHEMA_CACHE_PATH", ".mocksql/schema_cache.json")
AUTO_SCHEMA_IMPORT = True
AUTO_PROFILING = True
