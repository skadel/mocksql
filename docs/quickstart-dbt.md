# Quickstart dbt

MockSQL reads a dbt model's **compiled SQL**. It does not compile dbt itself and
does not derive schemas from `manifest.json`: the manifest identifies the model;
`target/compiled/` supplies the rendered SQL; schemas come from MockSQL's schema
cache or, for BigQuery, from BigQuery.

## Support matrix

| dbt target | Status | Schema source for `mocksql generate` |
|---|---|---|
| dbt-BigQuery | Supported | Automatic BigQuery import for cache misses, or `schema_cache` |
| dbt-DuckDB | Supported with a prepared cache | `schema_cache` only; no DuckDB schema-import command exists yet |
| dbt-Snowflake | Supported with an explicit schema refresh | Refresh into `schema_cache`, then generate |

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

Set a BigQuery job project (`BQ_TEST_PROJECT`, or `VERTEX_PROJECT` as its
fallback) and Google application credentials. On a cache miss, MockSQL fetches
the referenced table schema and saves it in `.mocksql/schema_cache.json`.

### dbt-DuckDB

Populate `.mocksql/schema_cache.json` with the real relation schemas first,
then run the same `mocksql generate` command. MockSQL does not infer schemas
from the compiled SQL or dbt manifest. DuckDB schema import is not implemented
in the CLI generation path.

### dbt-Snowflake

Install `mocksql[snowflake]`, configure the Snowflake connection variables, and
refresh each required relation into the cache before generation:

```bash
mocksql refresh-schemas --table DATABASE.SCHEMA.PARENT_MODEL
mocksql generate models/marts/sales.sql --config mocksql.yml
```

The dbt connector still supplies only compiled SQL; `refresh-schemas` is the
schema source. This manual step is required because `generate` auto-imports
cache misses only for BigQuery.

## BigQuery Sandbox and billing

BigQuery dry-runs validate SQL and estimate bytes processed; they do not scan
table data. Schema metadata reads are also distinct from profiling. The sandbox
can therefore be enough to compile/dry-run, read metadata, and run queries
within the Sandbox free-tier quotas and feature limits. MockSQL profiling is a
real query over the source tables: it consumes that quota and needs a
billing-enabled BigQuery project once those limits or required capabilities are
exceeded. Set `BQ_TEST_PROJECT` explicitly for any BigQuery job; dry-runs are
estimates and are not a guarantee that later profiling is free.
