# Quickstart

MockSQL generates fixtures with an LLM and executes every generated test locally
on DuckDB. Source warehouses are used only for the capabilities listed below;
they do not run the generated synthetic data.

## Requirements

- Python `>=3.11,<3.14`
- `pip install mocksql`
- An LLM credential: Vertex AI/Gemini or OpenAI
- `mocksql[bigquery]` only when MockSQL must import or profile BigQuery tables

```bash
pip install mocksql
pip install mocksql[bigquery]       # optional BigQuery connector
mocksql --help
```

The package is version `0.2.1` and licensed under MIT; see
[back/pyproject.toml](../back/pyproject.toml).

## Initialize a project

```bash
mocksql init
mocksql init --path ./my_project
mocksql init --dialect bigquery --llm-provider openai --non-interactive
```

The documented CLI options are the options exposed by `mocksql init --help`:
`--path/-p`, `--dialect`, `--models-path`, `--llm-provider`,
`--test-dataset` (deprecated compatibility option; tests still run locally),
`--langchain-api-key`, `--force`, and `--non-interactive`.

`mocksql init` creates `mocksql.yml`, `.mocksql/schema_cache.json` on first
schema import, and a local DuckDB file under `.mocksql/data/`.

### LLM credentials

Use exactly one provider configuration.

```dotenv
# Gemini through Vertex AI
VERTEX_PROJECT=my-gcp-project
GOOGLE_CLOUD_LOCATION=us-central1
```

```dotenv
# OpenAI
OPENAI_API_KEY=sk-...
```

Set `llm.provider: vertexai` or `llm.provider: openai` in `mocksql.yml`.
Model names also route automatically: `gemini*` goes to Vertex AI; `gpt-*` and
`o<N>*` go to OpenAI. OpenAI does not need Vertex credentials unless the source
is BigQuery.

## Source connector status

| Dialect | Validation | Schema handling in `mocksql generate` |
|---|---|---|
| `bigquery` | BigQuery dry-run | Imports cache misses from BigQuery with `mocksql[bigquery]` |
| `postgres` | Postgres validation | Use a prepared `schema_cache`; no Postgres import in this flow |
| `duckdb` | Local DuckDB validation | Use a prepared `schema_cache`; no DuckDB import command in this flow |
| `snowflake` | Snowflake `EXPLAIN` validation | Refresh schemas explicitly, then generate from `schema_cache` |
| `trino` | Trino validation | Use a prepared cache for generation; `refresh-schemas` has Trino support |

`mocksql generate` needs schemas. It reads them from `schema_cache` first, then
automatically imports only BigQuery cache misses. It never guesses column types
from generated rows. `mocksql refresh-schemas` refreshes BigQuery schemas by
by default; Snowflake and Trino each have explicit branches. It is not a DuckDB
schema importer.

For Snowflake, install `mocksql[snowflake]`, set the Snowflake environment
variables, then populate the cache explicitly, for example:

```bash
mocksql refresh-schemas --table DATABASE.SCHEMA.ORDERS
mocksql generate models/orders.sql
```

## BigQuery credentials, Sandbox, and cost

For BigQuery schema import, configure an execution project and credentials:

```dotenv
BQ_TEST_PROJECT=my-billing-project  # falls back to VERTEX_PROJECT
# GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/service-account.json
```

Application Default Credentials (`gcloud auth application-default login`) are
also supported. Typical permissions are `roles/bigquery.dataViewer` for metadata
and `roles/bigquery.user` to create BigQuery jobs; Gemini additionally needs
`roles/aiplatform.user`.

BigQuery dry-runs validate a query and return an estimated `total_bytes_processed`.
They do not read table data and MockSQL uses them to validate/estimate work. A
BigQuery Sandbox can run dry-runs, read metadata, and run real queries within
its free-tier quotas and feature limits; it does not require a billing account.
`mocksql generate --profile` is a real query over source tables, so it consumes
that quota and needs a billing-enabled project once Sandbox/free-tier limits or
required capabilities are exceeded. Dry-run estimates are not charges, nor a
promise that a later real profiling query is free. `profile_budget_tb` limits
which estimated profiling queries are run; it does not make a query free.

## Generate and replay

```bash
mocksql generate models/orders.sql
mocksql generate models/orders.sql --instruction "customer with no orders"
mocksql generate models/orders.sql --overwrite
mocksql test --model orders
mocksql test --json
mocksql test --frozen
```

`generate` is additive by default. `--overwrite` rebuilds the suite. `test`
uses the live SQL file by default and makes no LLM or warehouse calls; `--frozen`
uses the SQL snapshot saved with the test.

## dbt

For dbt, run `dbt compile` first and configure the `dbt:` block. MockSQL reads
the compiled SQL, not raw Jinja, and uses the same schema-cache/BigQuery-import
rules described above. See [quickstart-dbt.md](quickstart-dbt.md) for the exact
dbt-BigQuery, dbt-DuckDB, and dbt-Snowflake status.
