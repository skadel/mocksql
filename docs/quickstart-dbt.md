# Quickstart dbt

MockSQL reads a dbt model's **compiled SQL**. It does not compile dbt itself and
does not derive schemas from `manifest.json`: the manifest identifies the model;
`target/compiled/` supplies the rendered SQL; schemas come from MockSQL's schema
cache or the automatic BigQuery/Snowflake cache-miss importer.

## Support matrix

| dbt target | Status | Schema source for `mocksql generate` |
|---|---|---|
| dbt-BigQuery | Supported | Automatic BigQuery import for cache misses, or `schema_cache` |
| dbt-DuckDB | Supported with a prepared cache | `schema_cache` only; no DuckDB schema-import command exists yet |
| dbt-Snowflake | Supported | Automatic Snowflake import for cache misses, or `schema_cache` |

All generated cases are executed locally in DuckDB. The warehouse is never used
to execute the synthetic test data.

## 1. Compile dbt

Run this from the dbt project. Use the target whose relation names you want
MockSQL to resolve.

```bash
cd my_dbt_project
dbt deps               # when the project uses packages
dbt compile
```

For a mart whose compiled SQL references materialized parent models, those
relations must exist in the target warehouse before BigQuery can import their
schemas:

```bash
dbt run --select +my_mart
```

This is not required for models that only reference physical sources already
present in the warehouse.

## 2. Configure MockSQL

```yaml
version: "2"
dialect: bigquery                    # use duckdb or snowflake for those targets
models_path: ./models
dbt:
  project_dir: .
  target_path: target                 # optional; default: target
schema_cache: .mocksql/schema_cache.json
llm:
  provider: vertexai                  # or openai
```

`dbt:` makes `mocksql generate models/marts/sales.sql` read the corresponding
compiled file. A `preprocessor_fn` is normally unnecessary because dbt has
already rendered Jinja.

## 3. Generate

### dbt-BigQuery

```bash
pip install mocksql[bigquery]
mocksql generate models/marts/sales.sql --config mocksql.yml
```

Set an explicit BigQuery job project (`BQ_TEST_PROJECT`) and Google application
credentials. A `VERTEX_PROJECT` fallback exists, but do not use it when cost
isolation matters. On a cache miss, MockSQL fetches the referenced table schema
and saves it in `.mocksql/schema_cache.json`.

### dbt-DuckDB

Populate `.mocksql/schema_cache.json` with the real relation schemas first,
then run the same `mocksql generate` command. MockSQL does not infer schemas
from the compiled SQL or dbt manifest. DuckDB schema import is not implemented
in the CLI generation path.

### dbt-Snowflake

Install `mocksql[snowflake]` and configure:

```dotenv
SNOWFLAKE_ACCOUNT=org-account
SNOWFLAKE_USER=mocksql
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=ANALYTICS
# SNOWFLAKE_SCHEMA=PUBLIC          # optional
# SNOWFLAKE_ROLE=ANALYST           # optional
```

`SNOWFLAKE_DATABASE` is required by the current CLI connection validation even
when compiled SQL uses fully qualified relations. Then generate directly:

```bash
mocksql generate models/marts/sales.sql --config mocksql.yml
```

On a cache miss, `generate` imports the compiled SQL's Snowflake relations
automatically. `refresh-schemas` remains recommended when CI should preload the
cache or when a warehouse schema changed:

```bash
mocksql refresh-schemas --table DATABASE.SCHEMA.PARENT_MODEL
```

## BigQuery Sandbox and billing

BigQuery dry-runs validate SQL and estimate bytes without executing or charging
for the query. Plain table metadata reads do not scan source rows; however,
MockSQL can issue a real `INFORMATION_SCHEMA.PARTITIONS` query for partition
discovery, and BigQuery applies a 10 MB minimum on-demand processing amount to
each `INFORMATION_SCHEMA` query. `--profile` runs real source-table queries.

Use an explicit isolated `BQ_TEST_PROJECT`; do not rely on `VERTEX_PROJECT` when
cost isolation matters. A Sandbox project has no billing account and currently
includes 10 GiB active storage plus 1 TiB query processing per month, subject to
Sandbox limits. Omit `--profile` and keep the schema cache warm when the goal is
zero source-query spend. ADC can be set up with
`gcloud auth application-default login`; use `roles/bigquery.metadataViewer`
(or `roles/bigquery.dataViewer` for profiling) plus
`roles/bigquery.jobUser` on the job project.
