# MockSQL

[![Backend CI](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml)
[![Frontend CI](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**A native unit-testing layer for data engineers.** MockSQL takes a `.sql` file, automatically generates test data via LLM, runs it locally on DuckDB (zero cost on BigQuery), assigns an argued verdict to each test, and suggests the edge cases you haven't covered.

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

```bash
pip install mocksql
export VERTEX_PROJECT=<your-gcp-project>

mocksql init
mocksql generate models/my_model.sql
```

For the full setup (GCP, IAM, Web UI, development) → **[docs/quickstart.md](docs/quickstart.md)**

---

## dbt projects

MockSQL tests flat `.sql` files; a dbt project uses Jinja (`{{ ref }}`, macros…). The bridge is `dbt compile` (Jinja → pure SQL) plus a schema cache bootstrapped from the DuckDB database:

```bash
cd my_dbt_project
dbt compile && dbt run        # Jinja resolved + tables materialized
# bootstrap the schema_cache from the DuckDB database, then:
mocksql generate models/my_model.sql --config mocksql.yml
```

Full recipe (compile, cache bootstrap, `dialect: duckdb`) → **[docs/quickstart-dbt.md](docs/quickstart-dbt.md)**

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
