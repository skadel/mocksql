# Quickstart

MockSQL generates fixtures with an LLM and executes every generated test locally
on DuckDB. Source warehouses are used only for the capabilities listed below;
they do not run the generated synthetic data.

## Requirements

- Python `>=3.11,<3.14`
- `pip install mocksql`
- An LLM credential: Vertex AI/Gemini or OpenAI
- `mocksql[bigquery]` only when MockSQL must import or profile BigQuery tables
- `mocksql[snowflake]` when MockSQL must validate or import Snowflake schemas

```bash
pip install mocksql
pip install mocksql[bigquery]       # optional BigQuery connector
pip install mocksql[snowflake]      # optional Snowflake connector
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
| `snowflake` | Snowflake `EXPLAIN` validation | Imports cache misses automatically with `mocksql[snowflake]` |
| `trino` | Trino validation | Use a prepared cache for generation; `refresh-schemas` has Trino support |

`mocksql generate` needs schemas. It reads them from `schema_cache` first, then
automatically imports BigQuery or Snowflake cache misses through the connector
selected by `dialect`. It never guesses column types from generated rows.
`mocksql refresh-schemas` explicitly preloads or refreshes BigQuery, Snowflake,
or Trino schemas. It is not a DuckDB schema importer.

For Snowflake, install `mocksql[snowflake]` and set every required connection
variable:

```dotenv
SNOWFLAKE_ACCOUNT=org-account
SNOWFLAKE_USER=mocksql
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
# SNOWFLAKE_SCHEMA=PUBLIC          # optional
# SNOWFLAKE_ROLE=ANALYST           # optional
```

`SNOWFLAKE_DATABASE` is currently required when the CLI opens the connection,
even if every table in the SQL is fully qualified. A normal generation imports
missing schemas automatically. Preloading is optional but useful in CI:

```bash
mocksql refresh-schemas --table DATABASE.SCHEMA.ORDERS
mocksql generate models/orders.sql
```

## BigQuery credentials, Sandbox, and cost

For BigQuery schema import, configure an execution project and credentials:

```dotenv
BQ_TEST_PROJECT=my-isolated-project  # explicit project for BigQuery jobs
# GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/service-account.json
```

Application Default Credentials (`gcloud auth application-default login`) are
also supported. Grant `roles/bigquery.metadataViewer` for schema metadata (or
`roles/bigquery.dataViewer` when profiling must read table data) and
`roles/bigquery.jobUser` on `BQ_TEST_PROJECT` to create dry-run/query jobs.
Gemini additionally needs `roles/aiplatform.user`; OpenAI does not.

BigQuery dry-runs validate a query and return an estimated `total_bytes_processed`.
They do not execute the query, use query slots, or incur a charge. Plain table
metadata reads do not scan source data. One nuance: for a day-partitioned table,
MockSQL queries `INFORMATION_SCHEMA.PARTITIONS` to discover representative
partitions. That is a real metadata query job; BigQuery applies a 10 MB minimum
on-demand processing amount to each `INFORMATION_SCHEMA` query.

A [BigQuery Sandbox](https://cloud.google.com/bigquery/docs/sandbox) has no
billing account and provides the free-tier limits (currently 10 GiB active
storage and 1 TiB of query data processed per month), plus Sandbox feature
restrictions. `mocksql generate --profile` executes real queries over source
tables and consumes that allowance; on a billing-enabled project it can incur
charges after the applicable free tier. Dry-run estimates are not charges, nor
a promise that a later real query is free. `profile_budget_tb` filters profiling
queries by their dry-run estimate, but does not make them free.

To avoid accidental billing, use an explicit isolated Sandbox project in
`BQ_TEST_PROJECT`, omit `--profile`, and keep `schema_cache` warm. Do not rely on
the `VERTEX_PROJECT` fallback when cost isolation matters. See Google's
[dry-run documentation](https://cloud.google.com/bigquery/docs/running-queries#dry-run),
[INFORMATION_SCHEMA pricing](https://cloud.google.com/bigquery/docs/information-schema-intro#pricing),
and [ADC setup](https://cloud.google.com/docs/authentication/provide-credentials-adc).

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
