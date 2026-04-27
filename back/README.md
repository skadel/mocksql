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

## Préprocesseur SQL (`preprocessor_fn`)

Certaines requêtes contiennent des **variables non-standard** que MockSQL ne peut pas parser directement : paramètres BigQuery (`@start_date`), variables Jinja (`{{ ds }}`), macros dbt, etc.

La clé `preprocessor_fn` dans `mocksql.yml` permet de brancher une fonction Python qui transforme le SQL brut **avant** que MockSQL ne l'analyse.

### Configuration

Dans `mocksql.yml` :

```yaml
preprocessor_fn: "preprocessors:replace_vars"
```

Format : `nom_du_module:nom_de_la_fonction`  
Le module est résolu **relativement au dossier contenant `mocksql.yml`**.

### Signature de la fonction

```python
def replace_vars(sql: str) -> str:
    ...
```

- **Entrée** : SQL brut lu depuis le fichier `.sql`
- **Sortie** : SQL valide, parseable par MockSQL (sans variables non résolues)

### Exemple — variables BigQuery `@param`

Fichier SQL :
```sql
SELECT *
FROM `my_project.my_dataset.events`
WHERE event_date BETWEEN @start_date AND @end_date
  AND country = @country
```

Fichier `preprocessors.py` (à côté de `mocksql.yml`) :
```python
import re

def replace_vars(sql: str) -> str:
    defaults = {
        "start_date": "'2024-01-01'",
        "end_date":   "'2024-12-31'",
        "country":    "'FR'",
    }
    def sub(match):
        name = match.group(1)
        return defaults.get(name, f"'__UNKNOWN_{name}__'")

    return re.sub(r"@(\w+)", sub, sql)
```

### Exemple — templates Jinja / dbt

```python
from jinja2 import Environment

def replace_vars(sql: str) -> str:
    env = Environment()
    return env.from_string(sql).render(
        ds="2024-06-01",
        ds_nodash="20240601",
        macros={"format_date": lambda fmt, d: d},
    )
```

### Quand la fonction est appelée

| Contexte | Déclenchement |
|---|---|
| `mocksql generate model.sql` | Avant l'analyse des tables et la génération des tests |
| UI — liste des modèles (`GET /models`) | À chaque lecture du fichier `.sql` |
| UI — soumission d'une requête | Idem — le SQL affiché dans l'UI est déjà préprocessé |

La fonction est chargée dynamiquement à l'exécution — pas besoin de redémarrer le serveur après modification.

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
