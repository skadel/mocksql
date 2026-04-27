# MockSQL — Backend

**FastAPI + LangGraph + CLI**

Ce dossier contient le cœur de MockSQL : l'API REST, le graph LangGraph de génération de données, le CLI, et le package `mocksql-ui` (serveur web + assets React).

---

## Quickstart local

> Prérequis : Python ≥ 3.11, Poetry ≥ 1.8, un projet Google Cloud avec la facturation activée.

### 1. Authentification Google Cloud

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

### 2. Variables d'environnement

```bash
cp .env.example .env   # puis compléter les valeurs
```

Variables minimales requises :

```dotenv
PROJECT_ID=<votre-projet-gcp>
GOOGLE_CLOUD_PROJECT=<votre-projet-gcp>
BQ_SCHEMA_BILLING_PROJECT=<votre-projet-gcp>
DEFAULT_MODEL_NAME=gemini-2.0-flash-lite
DUCKDB_PATH=data/mocksql.duckdb
SECRET_KEY=<générer avec : python -c "import secrets; print(secrets.token_hex(32))">
API_SECRET_KEY=<idem>
FRONT_URL=http://127.0.0.1:3000
```

### 3. Installation & lancement

```bash
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.\.venv\Scripts\activate

pip install poetry && poetry install

# Lancer le serveur
uvicorn server:app --port 8080 --reload
```

L'API est accessible sur **http://localhost:8080**, le frontend sur **http://localhost:3000** (voir [../front/README.md](../front/README.md)).

---

## Commandes de développement

```bash
make style    # ruff check + ruff format --check + vulture (code mort)
make format   # ruff format + ruff check --fix (auto-correction)
make test     # pytest
make check    # style + test
```

Type checking :

```bash
poetry run mypy build_query/ app/
```

---

## Packaging

MockSQL produit deux wheels indépendants :

| Wheel | Contenu |
|-------|---------|
| `mocksql-*.whl` | CLI + LangGraph core (sans UI) |
| `mocksql_ui-*.whl` | Serveur web + assets React bundlés |

### Builder les wheels

```bash
# CLI uniquement
make build-cli

# CLI + UI (build React inclus — Node.js 18+ requis)
make build-ui
```

Les wheels sont générés dans `dist/`.

### Lancer l'UI depuis les wheels

```bash
pip install dist/mocksql-*.whl dist/mocksql_ui-*.whl
mocksql ui              # http://localhost:8080/static/
mocksql ui --port 4000
mocksql ui --no-browser
```

---

## Licence

Propriétaire — © 2025 Adel Skhiri. Contact : [skhiriadel92@gmail.com](mailto:skhiriadel92@gmail.com)
