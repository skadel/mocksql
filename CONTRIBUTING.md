# Contributing to MockSQL

Thank you for your interest in contributing!

## Prerequisites

- Python 3.12+ and [Poetry](https://python-poetry.org/)
- Node.js 18+ and npm
- Google Cloud SDK (for BigQuery features)

## Setup

```bash
# Backend
cd back
python -m venv .venv && source .venv/bin/activate   # Windows : .\.venv\Scripts\activate
pip install poetry && poetry install
cp .env.example .env   # ajuster DEFAULT_MODEL_NAME, LOG_LEVEL si besoin

# Frontend
cd front
npm ci
```

Variables GCP à mettre dans un `.env` à la racine du projet (chargé automatiquement via `load_dotenv()`) :

```dotenv
VERTEX_PROJECT=my-project-dev
GOOGLE_CLOUD_LOCATION=us-central1
BQ_TEST_PROJECT=my-project-dev   # optionnel, défaut : VERTEX_PROJECT
```

Variables spécifiques au backend (`back/.env`) :

```dotenv
DEFAULT_MODEL_NAME=gemini-2.0-flash-lite
FRONT_URL=http://127.0.0.1:3000
```

## Lancer le projet en local

```bash
# Backend
cd back
uvicorn server:app --port 8080 --reload

# Frontend (hot-reload, proxy vers le backend sur :8080)
cd front
npm start      # http://localhost:3000
```

## Builder les packages

```bash
cd back
make build-cli   # wheel CLI uniquement
make build-ui    # wheel CLI + UI (Node.js requis)
```

## Development workflow

```bash
# Backend — quality checks
make style     # ruff + vulture
make format    # auto-fix
make test      # pytest
make check     # style + test

# Frontend
npm test
npx eslint src/
```

## Pull requests

1. Fork the repo and create a branch from `master`.
2. Make your changes and ensure `make check` passes (backend) and `npm test` passes (frontend).
3. Open a PR — describe *what* changed and *why*.
4. A maintainer will review and merge.

## Releasing a new version (maintainers only)

Les deux packages se releasent indépendamment via deux workflows distincts.

### Releaser `mocksql` (backend + CLI)

```bash
# 1. Bump la version dans back/pyproject.toml
cd back && poetry version patch   # ou minor / major

# 2. Commit et tag — le workflow se déclenche sur v*.*.*
git add back/pyproject.toml
git commit -m "chore: release v$(cd back && poetry version -s)"
git tag "v$(cd back && poetry version -s)"
git push origin main --tags
```

Le [release workflow](.github/workflows/release.yml) :
- Lance les tests backend
- Build et publie `mocksql` sur PyPI
- Crée une GitHub Release avec le wheel

### Releaser `mocksql-ui` (web UI)

```bash
# Choisir le numéro de version souhaité pour mocksql-ui
# Le tag ui-v*.*.* déclenche le workflow — pas besoin de modifier pyproject.toml
git tag "ui-v0.1.2"
git push origin "ui-v0.1.2"
```

Le [release-ui workflow](.github/workflows/release-ui.yml) :
- Build le frontend React
- Injecte les fichiers statiques dans `mocksql_ui/static/`
- Build et publie `mocksql-ui` sur PyPI (version = tag sans le préfixe `ui-`)

### Installation utilisateur

```bash
pip install mocksql              # CLI + backend seul (CI/CD, headless)
pip install mocksql mocksql-ui   # CLI + web UI
```

### PyPI trusted publishing setup (one-time)

**Pour `mocksql`** — workflow `release.yml`, environnement `pypi` :
1. Réserver le projet `mocksql` sur [pypi.org](https://pypi.org).
2. **Account settings → Publishing → Add a new pending publisher** :
   - Repository owner / name : `skadel / mocksql`
   - Workflow : `release.yml` · Environment : `pypi`
3. Créer l'environnement `pypi` dans **GitHub → Settings → Environments**.

**Pour `mocksql-ui`** — workflow `release-ui.yml`, environnement `pypi-ui` :
1. Réserver le projet `mocksql-ui` sur [pypi.org](https://pypi.org).
2. Ajouter un second publisher :
   - Repository owner / name : `skadel / mocksql`
   - Workflow : `release-ui.yml` · Environment : `pypi-ui`
3. Créer l'environnement `pypi-ui` dans **GitHub → Settings → Environments**.

No API token needed — PyPI and GitHub exchange credentials via OIDC.

## Reporting issues

Use the [issue templates](.github/ISSUE_TEMPLATE/) to report bugs or request features.
