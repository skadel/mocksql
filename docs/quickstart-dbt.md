# Quickstart dbt

MockSQL tests **flat, parsable** `.sql` files. A [dbt](https://www.getdbt.com/) project is not directly parsable: models contain Jinja (`{{ ref(...) }}`, `{{ config(...) }}`, `{% if is_incremental() %}`, `dbt_utils` macros…) that SQLGlot cannot analyze.

MockSQL's **dbt connector** bridges the gap. It has two roles, and only two:

1. **Compile** — it reads the SQL **compiled** by dbt (`target/compiled/**/*.sql`), where all Jinja is already rendered: `ref()`/`source()`/`var()`/`this`/macros → flat SQL with the **real table names**. This replaces any regex preprocessor.
2. **Resolution** — it finds the dbt model from its path and provides that compiled SQL to MockSQL.

**Schema fetching remains MockSQL's normal job**: once the compiled SQL is provided, the `generate` flow extracts the referenced tables and imports their schema like any other query (BigQuery today; other warehouses coming). Test execution stays on a **scratch DuckDB** database — **zero cost**.

```
dbt project
  │  dbt compile                       (Jinja → flat SQL, refs = real warehouse names)
  ▼
target/compiled/**/*.sql  +  manifest.json
  │  dbt connector (`dbt:` block in mocksql.yml)
  ▼
mocksql generate ──► schema import (warehouse) ──► LLM generation ──► DuckDB execution
                                                                       → .mocksql/tests/<model>.json
```

---

## 1. Declare the dbt project in `mocksql.yml`

Add a `dbt:` block to the MockSQL config. This is what **activates the connector**:

```yaml
version: "2"
dialect: bigquery                 # bigquery for a dbt-BigQuery project ; duckdb for dbt-duckdb
models_path: ./models             # the dbt project's models/ folder
dbt:
  project_dir: .                  # folder containing dbt_project.yml (relative to this mocksql.yml)
  target_path: target             # optional (default: target)
llm:
  provider: vertexai
```

When `dbt:` is present, for any model recognized as a dbt model, MockSQL reads the **compiled SQL** instead of the raw `.sql` file. Any `preprocessor_fn` becomes unnecessary (compile already does the work).

> **Dialect**: `bigquery` for a dbt-BigQuery project (the compiled SQL keeps BQ idioms, MockSQL transpiles them to DuckDB at execution). `duckdb` for a natively dbt-duckdb project.

---

## 2. Compile the dbt project

In an environment with your warehouse's dbt adapter (e.g. `dbt-bigquery`):

```bash
cd my_dbt_project           # IMPORTANT: be INSIDE the project folder
dbt deps                    # if the project has packages (dbt_utils, dbt_date…)
dbt compile                 # Jinja → target/compiled/**/*.sql
```

`dbt compile` runs nothing on the warehouse — it just renders the Jinja. The result is in `target/compiled/<project>/models/**/*.sql`, with `ref()`/`source()` resolved to **real table names**.

> **`relation_name` pitfall**: the table names in the compiled SQL depend on the **compile profile**. Compile with your warehouse's **real target** (your usual `profiles.yml`) so the refs are the real warehouse names — otherwise MockSQL's import won't find them.

---

## 3. Materialize the parent models (to test a mart)

This is the key point for **marts** and **intermediates**: a mart references **other models** (`{{ ref('products') }}`), not raw tables. To generate coherent data, MockSQL imports those parents' schema — **so they must exist in the warehouse**.

- **staging** models: their refs are **real sources** → already present, nothing to do.
- **mart / intermediate** models: their parents are derived models → you must materialize them:

```bash
dbt run --select +my_mart     # builds the mart AND all its ancestors
```

Once `dbt run` has passed, the parent tables exist and `mocksql generate` can import their schema.

> If you skip this step on a mart, `mocksql generate` will fail at import with "table not found" on a parent model.

---

## 4. Generate the tests

```bash
DUCKDB_PATH=my_dbt_project/.mocksql/scratch.duckdb \
mocksql generate my_dbt_project/models/marts/core/sales.sql \
  --config my_dbt_project/mocksql.yml \
  --output my_dbt_project/.mocksql/tests
```

Sequence:
1. `[dbt] compiled SQL from manifest` — the connector provides the flat SQL (zero Jinja).
2. `Fetching schema for: …` — MockSQL imports the referenced tables' schemas from the warehouse.
3. LLM generation of synthetic data, execution on scratch DuckDB, verdict.
4. Writes `.mocksql/tests/<model>.json` (input data, DuckDB results, assertions, verdict).

> `DUCKDB_PATH` is the **scratch** database where MockSQL creates the synthetic tables — distinct from any dbt database.

### Credentials

Warehouse + LLM credentials are read from `back/.env` (via `load_dotenv()`):

```
GOOGLE_APPLICATION_CREDENTIALS=C:\absolute\path\service-account.json
VERTEX_PROJECT=my-gcp-project
```

---

## 5. (Optional) Evaluate quality across the whole project

The `/eval-mocksql` skill generates tests for every model then scores them via an LLM judge:

```
/eval-mocksql my_dbt_project
```

Report: `data` / `test` score + per-model validity, and an overall rate.

---

## Workflow recap

```bash
# once
cd my_dbt_project && dbt deps

# on every model / schema change
dbt compile                          # updates the compiled SQL
dbt run --select +my_mart            # (marts only) materializes the parents
mocksql generate models/.../my_mart.sql --config mocksql.yml --output .mocksql/tests
```

---

## Known pitfalls & limitations

- **`relation_name` = compile profile**: compile with the real warehouse target, otherwise inconsistent refs (see §2).
- **Marts → materialized parents**: a mart is only testable if its upstream models exist in the database (see §3).
- **One mocksql project per dbt project**: each project has its own `dbt_project.yml`/`profiles.yml` and its `mocksql.yml`.
- **Date-relative logic** (`CURRENT_DATE`, rolling windows): generation may produce out-of-range data → empty result. A legitimate quality signal, not a setup bug.
- **Macros with execution-time effects** (`{% if is_incremental() %}`): `dbt compile` elides the incremental branch (false at compile time) → the test covers the non-incremental path.

---

## Limitation: warehouses other than BigQuery

Today, MockSQL's schema import can only query **BigQuery**. For a **dbt-duckdb** project or one **without warehouse access**, there is therefore no automatic import path yet.

While waiting for the **warehouse connectors** (Snowflake, Databricks, DuckDB… — on the roadmap), a workaround exists: manually pre-fill `.mocksql/schema_cache.json` (the `schema_cache` key in `mocksql.yml`) by introspecting the database materialized by `dbt run`, in `dialect: duckdb`. This is a stopgap, not the target method — the bootstrap script details are in this file's git history.
