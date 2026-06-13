# MockSQL

[![Backend CI](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml)
[![Frontend CI](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Couche de tests unitaires native pour les data engineers.** MockSQL prend un fichier `.sql`, génère automatiquement des données de test via LLM, les exécute localement sur DuckDB (0 € facturé sur BigQuery), attribue un verdict argumenté à chaque test, et suggère les cas limites non couverts.

MockSQL ne passe pas le SQL brut au LLM. Il parse d'abord la requête avec **SQLGlot** pour extraire les colonnes utilisées, les filtres, les JOINs — puis fournit ces contraintes au LLM comme contexte structuré. Les données générées sont ensuite exécutées sur **DuckDB** : si un CTE retourne 0 lignes, MockSQL identifie lequel et relance automatiquement la génération jusqu'à obtenir des résultats non-vides. Une fois les tests générés, un **chat contextuel** permet de les affiner, en ajouter ou en modifier directement en langage naturel — ancré sur un test spécifique ou sur l'ensemble du modèle.

Les bibliothèques de mocking SQL existantes vous demandent d'**écrire les données de test à la main**. MockSQL prend le contrepied :

| | Bibliothèques de mocking SQL | MockSQL |
|---|---|---|
| Données de test | Écrites manuellement | **Générées automatiquement** par LLM |
| Couverture | Aucune détection | **6 axes** (NULL, vide, ex æquo…) + suggestions |
| Qualité du test | Pas d'évaluation | **Verdict LLM** (bon / insuffisant / incorrect) |
| Interface | Bibliothèque Python | **UI dédiée** (GenerateView → TestsView) |
| Moteur SQL | Un connecteur par DB | **DuckDB unifié** — aucun coût BigQuery |

MockSQL se décline en deux modes :
- **CLI** (`mocksql`) — usage standalone directement sur tes fichiers `.sql` locaux
- **Web Hub** — interface complète avec historique, verdicts, couverture et collaboration

---

## Démarrage rapide

```bash
pip install mocksql
export VERTEX_PROJECT=<votre-projet-gcp>

mocksql init
mocksql generate models/my_model.sql
```

Pour le setup complet (GCP, IAM, Web UI, développement) → **[docs/quickstart.md](docs/quickstart.md)**

---

## Projets dbt

MockSQL teste des `.sql` plats ; un projet dbt utilise du Jinja (`{{ ref }}`, macros…). La passerelle est `dbt compile` (Jinja → SQL pur) + un cache de schéma bootstrapé depuis la base DuckDB :

```bash
cd mon_projet_dbt
dbt compile && dbt run        # Jinja résolu + tables matérialisées
# bootstrap du schema_cache depuis la base DuckDB, puis :
mocksql generate models/mon_modele.sql --config mocksql.yml
```

Recette complète (compile, bootstrap du cache, `dialect: duckdb`) → **[docs/quickstart-dbt.md](docs/quickstart-dbt.md)**

---

## Structure du projet

```
back/       # FastAPI + LangGraph + CLI
  cli/      # mocksql CLI (main.py, generate.py)
  ui/       # package mocksql-ui (serveur + assets React)
front/      # React 18 + TypeScript + Redux (Web Hub)
examples/   # Exemples de projets MockSQL
docs/       # Documentation
  quickstart.md               # Setup complet (GCP, IAM, CLI, Web UI)
  quickstart-dbt.md           # Tester un projet dbt-DuckDB
  workflow-query-generation.md  # Flux frontend → backend → DuckDB
```

---

## Contribuer

```bash
make check-all   # back (style + tests) + front (vitest) — validation complète
```

Backend seul :

```bash
cd back
make style    # lint + format check + code mort (vulture)
make format   # auto-format et auto-fix
make test     # pytest
make check    # style + test
```

Hook pre-commit (recommandé) :

```bash
pip install pre-commit && pre-commit install
```
