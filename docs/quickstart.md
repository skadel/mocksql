# Quickstart

## Prerequisites

- Python 3.11+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) — only for Gemini (Vertex AI) and/or BigQuery sources
- Poetry (`pip install poetry`) — for development from source
- Node.js 18+ — only to build the frontend

---

## 1. Google Cloud authentication

> **Using OpenAI as LLM?** Sections 1–2 are only needed for Gemini (Vertex AI) and/or a **BigQuery source** (schema fetch, profiling, import). With OpenAI + a `postgres` / `duckdb` source, skip straight to [section 3](#3-cli-installation) — no Google Cloud setup required.

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
| `roles/aiplatform.user` | Call Vertex AI models (Gemini only — drop it if you use OpenAI) |

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

### LLM provider — Gemini or OpenAI

MockSQL picks the LLM backend from the **model name**: `gemini*` → Vertex AI, `gpt-*` / `o<N>` (o3, o4-mini…) → OpenAI. Set `llm.model` in `mocksql.yml` and provide the matching credentials below. The `llm.provider` key only breaks ties for ambiguous names (custom models, proxies).

Credentials and cloud projects come from **environment variables** — never from `mocksql.yml`, which only describes the project structure (paths, dialect, model). The priority is:

```
system / CI variable  >  local .env file  >  error
```

In local development, put them in a **gitignored** `.env` at your project root — MockSQL loads it automatically at startup (`load_dotenv()`). Add `.env` to your `.gitignore`:

```
.env
```

<details open>
<summary><b>Gemini via Vertex AI (default)</b></summary>

Requires the Google Cloud setup from [sections 1–2](#1-google-cloud-authentication) (auth + `roles/aiplatform.user` + Model Garden terms).

```dotenv
# .env — do not commit
VERTEX_PROJECT=my-project-dev
GOOGLE_CLOUD_LOCATION=us-central1     # required for Vertex AI calls

# Optional — default: VERTEX_PROJECT
BQ_TEST_PROJECT=my-project-dev

# Optional: explicit service account (otherwise: Application Default Credentials)
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account.json
```

```yaml
# mocksql.yml
llm:
  model: gemini-2.5-flash   # or gemini-2.5-pro
```

MockSQL is tuned for **gemini-2.5-flash / pro**, whose native thinking mode is on by default — prefer them over `flash-lite`.

</details>

<details>
<summary><b>OpenAI</b></summary>

No Google Cloud setup is needed for the LLM — [sections 1–2](#1-google-cloud-authentication) only remain relevant if your **source warehouse** is BigQuery.

```dotenv
# .env — do not commit
OPENAI_API_KEY=sk-...
```

```yaml
# mocksql.yml
llm:
  model: gpt-5-mini         # any gpt-* / o<N> model routes to OpenAI
```

Reasoning models (`gpt-5*`, o-series) only accept the default temperature; the optional `llm.thinking_level` key (`low` / `medium` / `high`) is forwarded to them as `reasoning_effort`. Non-reasoning models (`gpt-4.1-mini`, `gpt-5-chat-latest`) behave like classic chat models.

</details>

#### In CI/CD (GitHub Actions, Cloud Build…)

Inject the variables directly — they take priority over the local `.env`:

```yaml
# GitHub Actions — Gemini
env:
  VERTEX_PROJECT: my-project-preprod
  GOOGLE_CLOUD_LOCATION: us-central1
  BQ_TEST_PROJECT: my-project-preprod   # if different from VERTEX_PROJECT

# GitHub Actions — OpenAI
env:
  OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
```

```yaml
# Cloud Build
substitutions:
  _VERTEX_PROJECT: my-project-prod
env:
  - VERTEX_PROJECT=$_VERTEX_PROJECT
  - GOOGLE_CLOUD_LOCATION=us-central1
```

The DuckDB path is configured via `duckdb_path` in `mocksql.yml` (default: `data/mocksql.duckdb`).

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
  model: gemini-2.5-flash  # gemini* → Vertex AI · gpt-* / o<N> → OpenAI
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
| `model` | `gemini-2.5-flash` | Takes priority over `DEFAULT_MODEL_NAME`. The name picks the backend: `gemini*` → Vertex AI, `gpt-*` / `o<N>` → OpenAI |
| `provider` | `vertexai` | Only consulted for **ambiguous** model names (custom models, proxies) — the model name always wins |
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

### Profiling budget (auto-profiling)

Profiling real tables runs a BigQuery dry-run to estimate the scan, then queries
each table. To make the "click Generate and walk away" flow fully hands-off, set a
**scan budget** (in TB): tables whose estimated scan fits under the budget are
profiled automatically; tables above it are **deferred** (the profile is marked
partial and a *"Compléter le profil"* button lets you profile them on demand).

```yaml
# mocksql.yml
profile_budget_tb: 0.3   # auto-profile under 0.3 TB; defer larger tables
```

Also settable via the `PROFILE_BUDGET_TB` env var. When **unset**, the UI asks for a
budget before profiling (default 0.3 TB, remembered per browser). Set it to a value
≤ 0 / leave it out to keep the historical behaviour (no budget — profile everything).
Only applies to BigQuery (DuckDB/Postgres profiling is free, so no budget is needed).

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

Make sure the LLM provider environment variables are set (see [section 3](#3-cli-installation)) then:

```bash
mocksql ui                  # http://localhost:8080/static/
mocksql ui --port 4000
mocksql ui --no-browser
```
