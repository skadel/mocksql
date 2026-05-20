# Quickstart

## Prérequis

- Python 3.11+
- [Google Cloud SDK](https://cloud.google.com/sdk/docs/install)
- Poetry (`pip install poetry`) — pour le développement depuis les sources
- Node.js 18+ — uniquement pour builder le frontend

---

## 1. Authentification Google Cloud

MockSQL utilise les credentials applicatifs Google pour les appels Vertex AI et BigQuery :

```bash
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

---

## 2. Permissions IAM

Le compte utilisé doit disposer des rôles suivants :

| Rôle | Utilité |
|------|---------|
| `roles/bigquery.dataViewer` | Lecture du schéma des tables |
| `roles/bigquery.user` | Lancement des jobs / dry-run |
| `roles/aiplatform.user` | Appels aux modèles Vertex AI (Gemini) |

> **Activation Gemini (étape unique par projet)**
> Les rôles IAM ne suffisent pas : ouvrez le [Model Garden](https://console.cloud.google.com/vertex-ai/model-garden), cherchez un modèle Gemini et acceptez les conditions d'utilisation. Cette opération est unique par projet GCP et ne peut pas être réalisée via `gcloud`.

### Option A — Compte utilisateur (développement local)

```bash
for ROLE in roles/bigquery.dataViewer roles/bigquery.user roles/aiplatform.user; do
  gcloud projects add-iam-policy-binding <PROJECT_ID> \
    --member='user:<votre-email@domaine.com>' \
    --role="${ROLE}"
done
```

### Option B — Service account (CI/CD, Cloud Run)

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

gcloud iam service-accounts keys create ~/keys/mocksql-sa.json \
  --iam-account="${SA_EMAIL}"
```

En local, dans le `.env` à la racine de votre projet (voir section suivante) :
```dotenv
GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/mocksql-sa.json
```

En CI/CD, injecter `GOOGLE_APPLICATION_CREDENTIALS` comme variable d'environnement secrète.

> **Erreur fréquente** : `Forbidden: Access Denied: bigquery.jobs.create` → le rôle `bigquery.user` est manquant.

---

## 3. Installation CLI

```bash
pip install mocksql
```

### Variables d'environnement

MockSQL lit la configuration GCP depuis les variables d'environnement. La priorité est :

```
variable système / CI  >  fichier .env local  >  erreur
```

`mocksql.yml` décrit la structure du projet (chemins, dialect) — pas les credentials ni les projets GCP, qui changent selon l'environnement.

#### En développement local

Créez un `.env` **gitignorée** à la racine de votre projet :

```dotenv
# .env — ne pas committer
VERTEX_PROJECT=my-project-dev
GOOGLE_CLOUD_LOCATION=us-central1

# Optionnel — défaut : VERTEX_PROJECT
BQ_TEST_PROJECT=my-project-dev

# Optionnel : service account explicite (sinon : Application Default Credentials)
# GOOGLE_APPLICATION_CREDENTIALS=/chemin/vers/service_account.json
```

MockSQL charge ce fichier automatiquement au démarrage (`load_dotenv()`). Ajoutez `.env` à votre `.gitignore` :

```
.env
```

#### En CI/CD (GitHub Actions, Cloud Build…)

Injectez les variables directement — elles ont priorité sur le `.env` local :

```yaml
# GitHub Actions
env:
  VERTEX_PROJECT: my-project-preprod
  GOOGLE_CLOUD_LOCATION: us-central1
  BQ_TEST_PROJECT: my-project-preprod   # si différent de VERTEX_PROJECT
```

```yaml
# Cloud Build
substitutions:
  _VERTEX_PROJECT: my-project-prod
env:
  - VERTEX_PROJECT=$_VERTEX_PROJECT
  - GOOGLE_CLOUD_LOCATION=us-central1
```

`GOOGLE_CLOUD_LOCATION` est obligatoire pour les appels Vertex AI. Le chemin DuckDB se configure via `duckdb_path` dans `mocksql.yml` (défaut : `data/mocksql.duckdb`).

### `mocksql init`

Initialise un projet et génère `mocksql.yml` :

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
duckdb_path: data/mocksql.duckdb   # chemin de la base DuckDB locale
llm:
  provider: vertexai       # vertexai | openai
  model: gemini-2.0-flash  # override du modèle par défaut (optionnel)
  streaming: false
schema_cache: .mocksql/schema_cache.json
```

**Clés `llm`** :

| Clé | Défaut | Description |
|-----|--------|-------------|
| `provider` | `vertexai` | Backend LLM (`vertexai` ou `openai`) |
| `model` | `gemini-2.0-flash-lite` | Prioritaire sur `DEFAULT_MODEL_NAME` |
| `streaming` | `false` | Streaming token par token |

### `mocksql generate`

```bash
mocksql generate models/orders.sql
# avec options
mocksql generate models/orders.sql --config mocksql.yml --output .mocksql/tests
```

Les schémas sont mis en cache dans `.mocksql/schema_cache.json` — les runs suivants n'interrogent plus BigQuery.

**Outputs** dans `.mocksql/tests/` :
- `<model>_data.json` — données de test (tables d'entrée)
- `<model>_results.json` — résultats d'exécution DuckDB

### Préprocesseur SQL (variables et templates)

Si tes fichiers `.sql` contiennent des variables non-parsables (`@start_date`, `{{ ds }}`, macros dbt…) :

```yaml
# mocksql.yml
preprocessor_fn: "preprocessors:replace_vars"   # module:fonction, relatif au mocksql.yml
```

`preprocessors.py` à côté de `mocksql.yml` :

```python
import re

def replace_vars(sql: str) -> str:
    defaults = {"start_date": "'2024-01-01'", "end_date": "'2024-12-31'"}
    return re.sub(r"@(\w+)", lambda m: defaults.get(m.group(1), "NULL"), sql)
```

### Exemple complet

```bash
mocksql generate examples/jaffle_shop/models/orders.sql \
  --config examples/jaffle_shop/mocksql.yml
```

---

## 4. Web UI

MockSQL distribue deux wheels distincts :

| Package | Contenu |
|---------|---------|
| `mocksql` | CLI uniquement |
| `mocksql-ui` | CLI + serveur web + assets React |

```bash
# CLI + UI
pip install mocksql mocksql-ui
```

Assurez-vous que les variables d'environnement GCP sont définies (voir [section 3](#3-installation-cli)) puis :

```bash
mocksql ui                  # http://localhost:8080/static/
mocksql ui --port 4000
mocksql ui --no-browser
```

