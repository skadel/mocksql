# MockSQL

[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

MockSQL generates SQL unit-test fixtures with an LLM, evaluates them on local
DuckDB, and stores replayable tests. It is released under the **MIT** license.
Generated test data is never executed on BigQuery, Snowflake, or another source
warehouse.

## Install and initialize

```bash
pip install mocksql                 # CLI and local DuckDB execution
pip install mocksql[bigquery]       # BigQuery schema import/profiling
pip install mocksql[snowflake]      # Snowflake schema import/validation
pip install mocksql[all]            # BigQuery + Snowflake + Trino
mocksql init
```

`mocksql init` supports `--dialect`, `--models-path`, `--llm-provider`,
`--path`, `--force`, and `--non-interactive`. The LLM provider determines its
credentials:

```dotenv
# Vertex AI / Gemini
VERTEX_PROJECT=my-gcp-project
GOOGLE_CLOUD_LOCATION=us-central1

# Or OpenAI
OPENAI_API_KEY=sk-...
```

For a BigQuery source, set `BQ_TEST_PROJECT` explicitly and authenticate with
Application Default Credentials or `GOOGLE_APPLICATION_CREDENTIALS`. A
`VERTEX_PROJECT` fallback exists for compatibility, but should not be used when
cost isolation matters.

```bash
mocksql generate models/orders.sql
mocksql test --model orders
```

See [docs/quickstart.md](docs/quickstart.md) for credentials, cache behavior,
and BigQuery Sandbox/billing details.

## Connector status

| Source dialect | CLI generation | Notes |
|---|---|---|
| BigQuery | Supported | Imports missing schemas with `mocksql[bigquery]`; `--profile` issues real BigQuery queries. |
| DuckDB | Cache-only | Local test execution works; prepare `schema_cache` before generation. |
| PostgreSQL | Cache-only | Validation is available, but this generation flow does not import Postgres schemas. |
| Snowflake | Supported | Imports cache misses automatically with `mocksql[snowflake]`; `refresh-schemas` preloads or refreshes the cache. |
| Trino | Partial | Validation and `refresh-schemas` support exist; generation still requires cached schemas. |

## dbt status

MockSQL resolves dbt models through `manifest.json` and reads their compiled SQL
from `target/compiled/`. It never treats the dbt manifest as a schema source.

- dbt-BigQuery: supported, including BigQuery schema import.
- dbt-DuckDB: supported when `schema_cache` has been prepared.
- dbt-Snowflake: supported; cache misses in compiled SQL are imported
  automatically, and `refresh-schemas` is available for an explicit preload.

Full setup: [docs/quickstart-dbt.md](docs/quickstart-dbt.md).

## Development

The package metadata is in [back/pyproject.toml](back/pyproject.toml): version
`0.2.1`, Python `>=3.11,<3.14`, and MIT license.

```bash
cd back
poetry run mocksql --help
make check
```

### Snowflake schema import

With `dialect: snowflake` and `mocksql[snowflake]`, both `mocksql generate` and
`mocksql refresh-schemas` read schemas from Snowflake `INFORMATION_SCHEMA`. They
require `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`,
`SNOWFLAKE_WAREHOUSE`, and `SNOWFLAKE_DATABASE`; they never require or call
BigQuery. `SNOWFLAKE_DATABASE` is currently required by the CLI connection even
when every SQL relation is fully qualified. `generate` imports cache misses
automatically; use `refresh-schemas --table DATABASE.SCHEMA.TABLE` to preload or
force-refresh schemas. Snowflake profiling is not available yet:
`generate --profile` reports that limitation and continues without profiling
rather than falling back to BigQuery.
