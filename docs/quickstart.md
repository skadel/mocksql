# Quickstart

## Prerequisites

- Python 3.11+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- Poetry (`pip install poetry`) — for development from source
- Node.js 18+ — only to build the frontend

---

## 1. Google Cloud authentication

MockSQL uses Google application credentials for Vertex AI and BigQuery calls:

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

---

## 2. IAM permissions

The account in use must have the following roles:

| Role | Purpose |
|------|---------|
| `roles/bigquery.dataViewer` | Read table schemas |
| `roles/bigquery.user` | Run jobs / dry-run |
| `roles/aiplatform.user` | Call Vertex AI models (Gemini) |

> **Enabling Gemini (one-time step per project)**
> IAM roles are not enough: open the [Model Garden](https://console.cloud.google.com/vertex-ai/model-garden), search for a Gemini model and accept the terms of use. This is a one-time operation per GCP project and cannot be done via `gcloud`.

### Option A — User account (local development)

```bash
for ROLE in roles/bigquery.dataViewer roles/bigquery.user roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member='user:<your-email@domain.com>' \
    --role="${ROLE}"
done
```

### Option B — Service account (CI/CD, Cloud Run)

```bash
SA_EMAIL="mocksql-sa@<PROJECT_ID>.iam.gserviceaccount.com"

gcloud iam service-accounts create mocksql-sa \
  --project=<PROJECT_ID> \
  --display-name="MockSQL service account"

for ROLE in roles/bigquery.dataViewer roles/bigquery.user roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}"
done

gcloud iam service-accounts keys create ~/keys/mocksql-sa.json \
  --iam-account="${SA_EMAIL}"
```

Locally, in the `.env` at your project root (see next section):
```dotenv
GOOGLE_APPLICATION_CREDENTIALS=/path/to/mocksql-sa.json
```

In CI/CD, inject `GOOGLE_APPLICATION_CREDENTIALS` as a secret environment variable.

> **Common error**: `Forbidden: Access Denied: bigquery.jobs.create` → the `bigquery.user` role is missing.

---

## 3. CLI installation

```bash
pip install mocksql
```

The base install is intentionally lightweight: data generation and execution run entirely on **DuckDB**, with no warehouse client. The source-warehouse connectors are heavy (`pyarrow`, `grpc`, …) and only needed to **profile or import** real tables, so they ship as optional extras:

```bash
pip install mocksql[bigquery]    # + profiling/import from BigQuery
pip install mocksql[snowflake]   # + profiling/import from Snowflake
pip install mocksql[all]         # all connectors
```

If you trigger a profiling/import step without the matching extra installed, MockSQL fails fast with the exact `pip install mocksql[…]` command to run. Since the default `dialect` is `bigquery`, profiling against BigQuery sources requires `mocksql[bigquery]`.

### Environment variables

MockSQL reads its GCP configuration from environment variables. The priority is:

```
system / CI variable  >  local .env file  >  error
```

`mocksql.yml` describes the project structure (paths, dialect) — not the credentials or GCP projects, which change per environment.

#### In local development

Create a **gitignored** `.env` at your project root:

```dotenv
# .env — do not commit
VERTEX_PROJECT=my-project-dev
GOOGLE_CLOUD_LOCATION=us-central1

# Optional — default: VERTEX_PROJECT
BQ_TEST_PROJECT=my-project-dev

# Optional: explicit service account (otherwise: Application Default Credentials)
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
```

MockSQL loads this file automatically at startup (`load_dotenv()`). Add `.env` to your `.gitignore`:

```
.env
```

#### In CI/CD (GitHub Actions, Cloud Build…)

Inject the variables directly — they take priority over the local `.env`:

```yaml
# GitHub Actions
env:
  VERTEX_PROJECT: my-project-preprod
  GOOGLE_CLOUD_LOCATION: us-central1
  BQ_TEST_PROJECT: my-project-preprod   # if different from VERTEX_PROJECT
```

```yaml
# Cloud Build
substitutions:
  _VERTEX_PROJECT: my-project-prod
env:
  - VERTEX_PROJECT=$_VERTEX_PROJECT
  - GOOGLE_CLOUD_LOCATION=us-central1
```

`GOOGLE_CLOUD_LOCATION` is required for Vertex AI calls. The DuckDB path is configured via `duckdb_path` in `mocksql.yml` (default: `data/mocksql.duckdb`).

### `mocksql init`

Initializes a project and generates `mocksql.yml`:

```bash
mocksql init
# or in a subfolder
mocksql init --path ./my_project
```

Example generated `mocksql.yml`:

```yaml
version: "2"
dialect: bigquery          # bigquery | postgres | duckdb
models_path: ./models
duckdb_path: data/mocksql.duckdb   # path to the local DuckDB database
llm:
  provider: vertexai       # vertexai | openai
  model: gemini-2.0-flash  # override the default model (optional)
  streaming: false
schema_cache: .mocksql/schema_cache.json
```

#### Supported dialects

The `dialect` describes the **source** SQL: it drives validation (dry-run) and optimization. Test execution **always happens on DuckDB locally**.

| Dialect | Source | Validation (dry-run) | Schema |
|---------|--------|----------------------|--------|
| `bigquery` | BigQuery | BigQuery dry-run | fetch BigQuery → cache |
| `postgres` | PostgreSQL | Postgres dry-run | fetch Postgres → cache |
| `duckdb` | DuckDB / dbt-DuckDB | local DuckDB dry-run | pre-filled cache (see [quickstart-dbt.md](quickstart-dbt.md)) |

> In `dialect: duckdb`, MockSQL queries no remote source: the schema cache must be pre-filled (bootstrapped from a DuckDB database). This is the mode used for **dbt-DuckDB** projects — see **[quickstart-dbt.md](quickstart-dbt.md)**.

**`llm` keys**:

| Key | Default | Description |
|-----|---------|-------------|
| `provider` | `vertexai` | LLM backend (`vertexai` or `openai`) |
| `model` | `gemini-2.0-flash-lite` | Takes priority over `DEFAULT_MODEL_NAME` |
| `streaming` | `false` | Token-by-token streaming |

### `mocksql generate`

```bash
mocksql generate models/orders.sql
# with options
mocksql generate models/orders.sql --config mocksql.yml --output .mocksql/tests
```

Schemas are cached in `.mocksql/schema_cache.json` — subsequent runs no longer query BigQuery.

**Outputs** in `.mocksql/tests/`:
- `<model>_data.json` — test data (input tables)
- `<model>_results.json` — DuckDB execution results

### SQL preprocessor (variables and templates)

If your `.sql` files contain non-parsable variables (`@start_date`, `{{ ds }}`, dbt macros…):

```yaml
# mocksql.yml
preprocessor_fn: "preprocessors:replace_vars"   # module:function, relative to mocksql.yml
```

`preprocessors.py` next to `mocksql.yml`:

```python
import re

def replace_vars(sql: str) -> str:
    defaults = {"start_date": "'2024-01-01'", "end_date": "'2024-12-31'"}
    return re.sub(r"@(\w+)", lambda m: defaults.get(m.group(1), "NULL"), sql)
```

### Full example

```bash
mocksql generate examples/jaffle_shop/models/orders.sql \
  --config examples/jaffle_shop/mocksql.yml
```

---

## 4. Web UI

MockSQL ships two distinct wheels:

| Package | Contents |
|---------|----------|
| `mocksql` | CLI only |
| `mocksql-ui` | CLI + web server + React assets |

```bash
# CLI + UI
pip install mocksql mocksql-ui
```

Make sure the GCP environment variables are set (see [section 3](#3-cli-installation)) then:

```bash
mocksql ui                  # http://localhost:8080/static/
mocksql ui --port 4000
mocksql ui --no-browser
```
