# MockSQL

[![Backend CI](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/backend-ci.yml)
[![Frontend CI](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml/badge.svg)](https://github.com/skadel/mocksql/actions/workflows/frontend-ci.yml)
[![PyPI version](https://img.shields.io/pypi/v/mocksql)](https://pypi.org/project/mocksql/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Couche de tests unitaires native pour les data engineers.** MockSQL prend un fichier `.sql`, génère automatiquement des données de test via LLM, les exécute localement sur DuckDB (0 € facturé sur BigQuery), attribue un verdict argumenté à chaque test, et suggère les cas limites non couverts.

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
pip install dist/mocksql-*.whl
export PROJECT_ID=<votre-projet-gcp>

mocksql init
mocksql generate models/my_model.sql
```

Pour le setup complet (GCP, IAM, Web UI, développement) → **[docs/quickstart.md](docs/quickstart.md)**

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
  workflow-query-generation.md  # Flux frontend → backend → DuckDB
```

---

## Contribuer

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
