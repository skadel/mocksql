# MockSQL

MockSQL generates SQL unit-test fixtures with an LLM, runs them locally on
DuckDB, evaluates their quality, and saves replayable tests for CI. Install the
CLI with `pip install mocksql`; install `mocksql[bigquery]`,
`mocksql[snowflake]`, or `mocksql[all]` when a source connector is needed.

Generated tests never execute against the source warehouse. See the
[project README](../README.md) for user setup and connector guidance.

## Development

> Pour l'installation, la configuration GCP et le CLI, voir le [README racine](../README.md).

**FastAPI · LangGraph · Python 3.12**

---


```bash
python -m venv .venv
source .venv/bin/activate   # Windows : .\.venv\Scripts\activate
pip install poetry && poetry install --all-extras

cp .env.example .env        # compléter les variables (voir README racine §3)
uvicorn server:app --port 8080 --reload
```

> Les connecteurs sources (BigQuery, Snowflake) sont des **extras** optionnels, exclus de l'install de base pour l'alléger. `poetry install` seul n'installe que le cœur (génération + DuckDB) ; ajoutez `--all-extras` (ou `--extras "bigquery snowflake"`) pour travailler sur le profiling/import.

---

## Commandes

```bash
make style    # ruff check + ruff format --check + vulture (code mort)
make format   # auto-format + auto-fix ruff
make test     # pytest
make check    # style + test
```

Type checking :

```bash
poetry run mypy build_query/ app/
```

---

## Packaging

```bash
poetry build --output dist
```

This produces the `mocksql` wheel and source distribution in `dist/`.

---

## License

MockSQL is released under the [MIT License](../LICENSE).
