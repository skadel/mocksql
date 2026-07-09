# MockSQL

[![Backend CI](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml)
[![Frontend CI](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A native unit-testing layer for data engineers.** MockSQL takes a `.sql` file, automatically generates test data via LLM, runs it locally on DuckDB (zero cost on BigQuery), assigns an argued verdict to each test, and suggests the edge cases you haven't covered.

<!-- DEMO VIDEO: éditer ce README sur github.com et glisser-déposer docs/assets/demo.mp4
     juste sous ce commentaire — GitHub héberge la vidéo et affiche un player inline. -->

<p align="center"><em>Full flow: pick a <code>.sql</code> model → MockSQL generates the input data, runs the query locally on DuckDB, and returns an argued verdict per test — plus suggestions for the edge cases you haven't covered.</em></p>

MockSQL never hands raw SQL to the LLM. It first parses the query with **SQLGlot** to extract the used columns, filters, and JOINs — then feeds those constraints to the LLM as structured context. The generated data is then executed on **DuckDB**: if a CTE returns 0 rows, MockSQL identifies which one and automatically re-runs generation until it gets non-empty results. Once the tests are generated, a **contextual chat** lets you refine, add, or edit them directly in natural language — anchored to a specific test or to the whole model.

Existing SQL mocking libraries ask you to **write the test data by hand**. MockSQL takes the opposite approach:

| | SQL mocking libraries | MockSQL |
|---|---|---|
| Test data | Written manually | **Auto-generated** by LLM |
| Coverage | No detection | **6 axes** (NULL, empty, ties…) + suggestions |
| Test quality | No evaluation | **LLM verdict** (good / weak / incorrect) |
| Interface | Python library | **Dedicated UI** (GenerateView → TestsView) |
| SQL engine | One connector per DB | **Unified DuckDB** — no BigQuery cost |

MockSQL comes in two modes:
- **CLI** (`mocksql`) — standalone use directly on your local `.sql` files
- **Web Hub** — full interface with history, verdicts, coverage, and collaboration

---

## Quick start

Whatever your warehouse, MockSQL generates data with an LLM and runs every test locally on **DuckDB** — zero warehouse cost. The base install runs entirely on DuckDB; the source-warehouse connectors are heavy (`pyarrow`, `grpc`, …) and only needed to **profile/import** real tables, so they ship as optional extras (`mocksql[bigquery]`, `mocksql[snowflake]`, `mocksql[all]`). Run an import without the matching extra and MockSQL fails fast with the exact command.

Data generation always uses an LLM (Gemini via Vertex AI by default), so set `VERTEX_PROJECT` in every setup below. Pick your source:

### BigQuery

```bash
pip install mocksql[bigquery]
gcloud auth application-default login         # GCP auth for schema import + Gemini
export VERTEX_PROJECT=<your-gcp-project>

mocksql init                                  # dialect: bigquery (the default)
mocksql generate models/orders.sql
```

Full GCP/IAM setup (roles, service accounts, CI) → **[docs/quickstart.md](docs/quickstart.md)**

### Snowflake

```bash
pip install mocksql[snowflake]
mocksql init                                  # choose dialect: snowflake
```

Put credentials in a **gitignored** `.env` at your project root:

```dotenv
VERTEX_PROJECT=<your-gcp-project>             # LLM (Gemini via Vertex AI)
SNOWFLAKE_ACCOUNT=<account_identifier>        # ORG-ACCOUNT, or <locator>.<region>.<cloud>
SNOWFLAKE_USER=<user>
SNOWFLAKE_PASSWORD=<password>
SNOWFLAKE_WAREHOUSE=<warehouse>
SNOWFLAKE_DATABASE=<database>
# SNOWFLAKE_ROLE=<role>                        # optional (required on some accounts)
```

```bash
mocksql generate models/orders.sql
```

MockSQL imports the table schemas from Snowflake (`INFORMATION_SCHEMA`), generates data, and runs the tests on DuckDB. Snowflake idioms (`IFF`, `TO_TIMESTAMP_NTZ`, `TO_CHAR`, `LISTAGG`, `NUMBER(p,s)`…) are transpiled automatically.

### dbt

A dbt model isn't flat SQL (Jinja: `{{ ref }}`, macros). MockSQL reads the **compiled** SQL via a `dbt:` block in `mocksql.yml`, then imports schemas and runs on DuckDB like any other model:

```bash
pip install mocksql[bigquery]                 # or [snowflake] — match your dbt target
cd my_dbt_project
dbt compile                                   # Jinja → flat SQL with real table names
dbt run --select +my_mart                     # (marts only) materialize parent models
mocksql generate models/marts/my_mart.sql --config mocksql.yml
```

```yaml
# mocksql.yml — set the dbt block + the dialect of your dbt target
dialect: bigquery        # bigquery | snowflake | duckdb (must match your dbt target)
models_path: ./models
dbt:
  project_dir: .         # folder containing dbt_project.yml
llm:
  provider: vertexai
```

Full recipe (compile profile, materializing parents, scratch DuckDB) → **[docs/quickstart-dbt.md](docs/quickstart-dbt.md)**

---

## Project structure

```
back/       # FastAPI + LangGraph + CLI
  cli/      # mocksql CLI (main.py, generate.py)
  ui/       # mocksql-ui package (server + React assets)
front/      # React 18 + TypeScript + Redux (Web Hub)
examples/   # Example MockSQL projects
docs/       # Documentation
  quickstart.md               # Full setup (GCP, IAM, CLI, Web UI)
  quickstart-dbt.md           # Testing a dbt-DuckDB project
  workflow-query-generation.md  # Flow frontend → backend → DuckDB
```

---

## Contributing

```bash
make check-all   # back (style + tests) + front (vitest) — full validation
```

Backend only:

```bash
cd back
make style    # lint + format check + dead code (vulture)
make format   # auto-format and auto-fix
make test     # pytest
make check    # style + test
```

Pre-commit hook (recommended):

```bash
pip install pre-commit && pre-commit install
```
