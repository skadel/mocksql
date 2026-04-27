# MockSQL

[![Backend CI](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml)
[![Frontend CI](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

TDD engine for Analytics Engineering — génère automatiquement des données de test unitaires pour des requêtes SQL.

MockSQL se décline en deux modes :
- **CLI** — usage standalone directement sur tes fichiers `.sql` locaux
- **Web Hub** — interface complète avec historique, profiling et collaboration

---

## Démarrage rapide (CLI)

```bash
pip install dist/mocksql-*.whl
export PROJECT_ID=<votre-projet-gcp>

# 1. Initialiser un projet
mocksql init

# 2. Générer des tests pour un modèle SQL
mocksql generate models/my_model.sql
```

Voir la section [CLI](#4-cli) pour le détail.

---

## Prérequis

- Python 3.11+
- Poetry (`pip install poetry`) — pour le développement depuis les sources
- Node.js 18+ — pour builder le frontend (pas requis à l'exécution)
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)

---

## 1. Authentification Google Cloud

Les modèles Gemini (VertexAI) et BigQuery utilisent les credentials applicatifs Google :

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

---

## 2. Permissions IAM requises

Le compte utilisé (utilisateur ou service account) doit disposer des rôles suivants sur le projet GCP :

| Rôle | Utilité |
|------|---------|
| **BigQuery Data Viewer** (`roles/bigquery.dataViewer`) | Lecture du schéma des tables |
| **BigQuery User** (`roles/bigquery.user`) | Lancement des jobs / dry-run des requêtes |
| **AI Platform Developer** (`roles/aiplatform.user`) | Appels aux modèles Vertex AI (Gemini) |

> **Activation des modèles Gemini (étape unique par projet)**
> Les rôles IAM ne suffisent pas : même avec `roles/aiplatform.user`, les modèles Gemini retournent une erreur 404 si l'accès n'a pas été activé dans le Model Garden.
> Dans la [console GCP](https://console.cloud.google.com/vertex-ai/model-garden), ouvrez **Model Garden**, cherchez un modèle Gemini (ex. *Gemini 2.0 Flash Lite*) et acceptez les conditions d'utilisation. Cette opération est **unique** par projet GCP et ne peut **pas** être réalisée via `gcloud`.

### Option A — Compte utilisateur (développement local)

```bash
gcloud projects add-iam-policy-binding <PROJECT_ID> \
  --member='user:<votre-email@domaine.com>' \
  --role='roles/bigquery.dataViewer'

gcloud projects add-iam-policy-binding <PROJECT_ID> \
  --member='user:<votre-email@domaine.com>' \
  --role='roles/bigquery.user'

gcloud projects add-iam-policy-binding <PROJECT_ID> \
  --member='user:<votre-email@domaine.com>' \
  --role='roles/aiplatform.user'
```

### Option B — Service account (CI/CD, Cloud Run)

1. Créez un service account et attribuez-lui les mêmes rôles :

   ```bash
   SA_EMAIL="mocksql-sa@<PROJECT_ID>.iam.gserviceaccount.com"

   gcloud iam service-accounts create mocksql-sa \
     --project=<PROJECT_ID> \
     --display-name="MockSQL service account"

   for ROLE in roles/bigquery.dataViewer roles/bigquery.user roles/aiplatform.user; do
     gcloud projects add-iam-policy-binding <PROJECT_ID> \
       --member="serviceAccount:${SA_EMAIL}" \
       --role="${ROLE}"
   done
   ```

2. Générez une clé JSON et pointez-y via `GOOGLE_APPLICATION_CREDENTIALS` :

   ```bash
   gcloud iam service-accounts keys create ~/keys/mocksql-sa.json \
     --iam-account="${SA_EMAIL}"
   ```

   ```dotenv
   # back/.env
   GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/mocksql-sa.json
   ```

> **Erreur fréquente** : `Forbidden: Access Denied: bigquery.jobs.create permission`
> → Le rôle **BigQuery User** est manquant sur le projet.

---

## 3. Configuration du backend

### 3.1 Créer le fichier `.env`

```bash
cp back/.env.example back/.env
```

Variables essentielles :

```dotenv
# Google Cloud
PROJECT_ID=<votre-projet-gcp>
GOOGLE_CLOUD_PROJECT=<votre-projet-gcp>
GOOGLE_CLOUD_LOCATION=us-central1

# BigQuery (import de schémas)
BQ_SCHEMA_BILLING_PROJECT=<votre-projet-gcp>
BQ_REGION=US

# LLM
DEFAULT_MODEL_NAME=gemini-2.0-flash-lite

# Base de données locale
DUCKDB_PATH=data/mocksql.duckdb

# Sécurité
SECRET_KEY=<clé-secrète>
API_SECRET_KEY=<clé-api>

# CORS
FRONT_URL=http://127.0.0.1:3000
```

### 3.2 Installer les dépendances

```bash
cd back
python -m venv .venv

# Linux / macOS
source .venv/bin/activate

# Windows
.\.venv\Scripts\activate

pip install poetry && poetry install
```

### 3.3 Lancer le serveur

```bash
uvicorn server:app --port 8080 --reload
```

Le backend est accessible sur [http://localhost:8080](http://localhost:8080).

---

## 4. CLI

### Variables d'environnement requises

Pour l'usage CLI standalone (sans `back/.env`), définir les variables suivantes dans votre shell ou un fichier `.env` à la racine de votre projet :

```dotenv
PROJECT_ID=<votre-projet-gcp>
GOOGLE_CLOUD_PROJECT=<votre-projet-gcp>
GOOGLE_CLOUD_LOCATION=us-central1
DUCKDB_PATH=data/mocksql.duckdb
```

`GOOGLE_CLOUD_LOCATION` est obligatoire pour les appels Vertex AI — sans elle, les modèles Gemini ne sont pas accessibles.

### `mocksql init`

Initialise un projet MockSQL et génère `mocksql.yml` :

```bash
mocksql init
# ou dans un sous-dossier
mocksql init --path ./mon_projet
```

Exemple de `mocksql.yml` généré :

```yaml
version: "2"
dialect: bigquery          # bigquery | postgres
models_path: ./models
llm:
  provider: vertexai       # vertexai | openai
schema_cache: .mocksql/schema_cache.json
```

### `mocksql generate`

Parse un modèle SQL, résout les schémas des tables sources et génère des données de test :

```bash
mocksql generate models/orders.sql
# options
mocksql generate models/orders.sql --config mocksql.yml --output .mocksql/tests
```

Les schémas récupérés sont mis en cache dans `.mocksql/schema_cache.json` — les runs suivants n'interrogent plus BigQuery.

**Outputs** dans `.mocksql/tests/` :
- `<model>_data.json` — données de test (tables d'entrée)
- `<model>_results.json` — résultats d'exécution DuckDB

### Exemple

Un exemple complet est disponible dans [`examples/jaffle_shop/`](examples/jaffle_shop/) :

```bash
mocksql generate examples/jaffle_shop/models/orders.sql \
  --config examples/jaffle_shop/mocksql.yml
```

---

## 5. Web UI

MockSQL distribue deux wheels distincts :

| Package | Contenu | Usage |
|---------|---------|-------|
| `mocksql` | CLI (`mocksql init`, `mocksql generate`) | CI/CD, sans UI |
| `mocksql-ui` | CLI + serveur web + assets React | Installation complète |

### Installer depuis les wheels

```bash
# CLI uniquement
pip install dist/mocksql-*.whl

# CLI + UI
pip install dist/mocksql-*.whl dist/mocksql_ui-*.whl
```

### Builder les wheels

```bash
cd back

# CLI uniquement
make build-cli

# CLI + UI (build React inclus, Node.js requis)
make build-ui   # produit dist/mocksql-*.whl et dist/mocksql_ui-*.whl
```

### Lancer l'interface

```bash
mocksql ui                  # http://localhost:8080/static/
mocksql ui --port 4000      # port personnalisé
mocksql ui --no-browser     # sans ouverture automatique du navigateur
```

Node.js n'est **pas** requis à l'exécution — uniquement pour le build.

### Développement frontend (hot-reload)

```bash
cd front
npm ci
npm start      # http://localhost:3000 (proxy vers le backend sur :8080)
```

---

## Structure du projet

```
back/       # FastAPI + LangGraph + CLI
  cli/      # mocksql CLI (main.py, generate.py)
  ui/       # package mocksql-ui (serveur + assets React)
front/      # React 18 + TypeScript + Redux (Web Hub)
examples/   # Exemples de projets MockSQL
docs/       # Documentation du workflow
```

---

## How to contribute

### Commandes backend (depuis `back/`)

| Commande | Description |
|---|---|
| `make style` | Lint (ruff) + format check + code mort (vulture) |
| `make format` | Auto-format et auto-fix ruff |
| `make test` | Tests pytest |
| `make check` | `style` + `test` |

### Hook pre-commit (recommandé)

```bash
pip install pre-commit
pre-commit install
```

`make style` sera exécuté automatiquement à chaque `git commit` sur les fichiers Python modifiés.
