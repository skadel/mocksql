Lance l'évaluation LLM-as-judge sur un sous-projet MockSQL.

## Usage

```
/eval-mocksql [projet] [modèles...]
```

- `projet` : chemin vers le sous-projet (défaut : `examples/spider`)
- `modèles` : liste de noms de modèles à évaluer (défaut : tous)

Exemples :
- `/eval-mocksql` — évalue tous les modèles de `examples/spider`
- `/eval-mocksql examples/spider bq282 bq199` — évalue seulement bq282 et bq199

## Ce que fait ce skill

1. **Génère les tests manquants** via `mocksql generate` (CLI) pour chaque modèle qui n'a pas encore de fichier `.mocksql/tests/<model>.json`.
2. **Lance l'eval** (`run_eval.py`) avec le juge Gemini.
3. **Affiche le rapport** : score par modèle (cohérence_données, cohérence_test, valide) + taux global.

## Constantes (ne pas deviner)

```
VERTEX_PROJECT=pipetalk-493612
CREDS=/c/Users/skhir/pipetalk-493612-5423f51f8585.json
BACK=/c/Users/skhir/workspace/mocksql/back
GEMINI_MODEL=gemini-3.1-flash-lite-preview   ← NE PAS changer ce nom de modèle
```

> Le modèle LLM **doit** être `gemini-3.1-flash-lite-preview`. Le défaut du backend (`gemini-2.0-flash-lite`) n'existe pas sur ce projet Vertex et retourne 404.

## Étapes à exécuter

### 1. Parser les arguments

- rien → projet = `examples/spider`, tous les modèles
- un chemin → projet = ce chemin, tous les modèles
- un chemin + des noms → projet = premier token, modèles = le reste

### 2. Identifier les modèles à générer

```bash
# Lister les .sql disponibles
ls <projet>/models/*.sql | xargs -I{} basename {} .sql

# Lister les tests déjà générés
ls <projet>/.mocksql/tests/*.json | xargs -I{} basename {} .json
```

Les modèles sans `.json` correspondant → à générer à l'étape 3.

> **Tables wildcard** (`ga_sessions_*`, etc.) : le CLI ne peut pas résoudre leur schéma.
> Ces modèles afficheront `[WARN] Unqualified table refs` puis `[ERROR] No schemas available` — c'est normal, les ignorer silencieusement.

### 3. Générer les tests manquants

Pour chaque modèle sans test (sauf ceux avec tables wildcard) :

```bash
VERTEX_PROJECT=pipetalk-493612 \
GOOGLE_APPLICATION_CREDENTIALS=/c/Users/skhir/pipetalk-493612-5423f51f8585.json \
DUCKDB_PATH=<projet>/.mocksql/mocksql.duckdb \
poetry -C /c/Users/skhir/workspace/mocksql/back run mocksql generate \
  <projet>/models/<model>.sql \
  --config <projet>/mocksql.yml \
  --output <projet>/.mocksql/tests
```

### 4. Lancer l'eval

Passer **uniquement** les modèles qui ont un `.json` dans `.mocksql/tests/` :

```bash
VERTEX_PROJECT=pipetalk-493612 \
GOOGLE_APPLICATION_CREDENTIALS=/c/Users/skhir/pipetalk-493612-5423f51f8585.json \
GOOGLE_CLOUD_PROJECT=pipetalk-493612 \
poetry -C /c/Users/skhir/workspace/mocksql/back run python \
  /c/Users/skhir/workspace/mocksql/examples/eval/run_eval.py \
  --project <projet_absolu> \
  [--models <model1> <model2> ...] \
  --gcp-project pipetalk-493612 \
  --gemini-model gemini-3.1-flash-lite-preview
```

### 5. Afficher le résumé

Lire `examples/eval/results/<date>_<projet>.json` et afficher un tableau markdown :

| Modèle | données | test | exec_status | Valide | Reasoning |
|--------|---------|------|-------------|--------|-----------|
| bq282  | 5       | 4    | complete    | OK     | ...       |
| bq199  | 5       | 5    | complete    | OK     | ...       |

Terminer par le taux global : `N/M valides (X%)`.

## Notes importantes

- Le `DUCKDB_PATH` doit pointer vers `<projet>/.mocksql/mocksql.duckdb` (pas `back/data/`).
- Les chemins passés à `mocksql generate` et `run_eval.py` doivent être **absolus** (`poetry -C back` change le cwd).
- Si un modèle a une erreur DuckDB (ex: alias GROUP BY), le test est quand même écrit — l'eval peut le juger avec `exec_status=error`.
- Le rapport JSON est toujours écrit dans `examples/eval/results/<date>_<projet>.json`.
- Les scores `cohérence_données` et `cohérence_test` sont sur 5 ; `is_valid = true` si les deux >= 3.
