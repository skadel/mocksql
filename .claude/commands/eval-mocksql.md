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
GEMINI_MODEL=gemini-3.1-flash-lite-preview   ← NE PAS changer ce nom de modèle
ROOT=$(git rev-parse --show-toplevel)
BACK=$ROOT/back
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
```

Tous les modèles `.sql` sont (re)générés **de zéro** à chaque évaluation — même si un `.json` existe déjà. Le `--overwrite` est **obligatoire** : sans lui, `generate` est additif et empile une nouvelle suite par-dessus l'ancienne (cas périmés conservés → l'éval juge des tests d'une version antérieure du générateur et masque les fixes).

### 3. Générer les tests

Pour chaque modèle :

```bash
ROOT=$(git rev-parse --show-toplevel)
VERTEX_PROJECT=pipetalk-493612 \
DUCKDB_PATH=<projet>/.mocksql/mocksql.duckdb \
poetry -C $ROOT/back run mocksql generate \
  <projet>/models/<model>.sql \
  --config <projet>/mocksql.yml \
  --output <projet>/.mocksql/tests \
  --overwrite
```

> Les credentials GCP sont lus depuis `back/.env` via `load_dotenv()` au démarrage du backend. Si la commande hang sur les appels BigQuery, vérifier que `GOOGLE_APPLICATION_CREDENTIALS` est bien défini dans `back/.env` avec un chemin absolu vers la clé de service account.

### 4. Lancer l'eval

Passer **uniquement** les modèles qui ont un `.json` dans `.mocksql/tests/` :

```bash
ROOT=$(git rev-parse --show-toplevel)
VERTEX_PROJECT=pipetalk-493612 \
GOOGLE_CLOUD_PROJECT=pipetalk-493612 \
poetry -C $ROOT/back run python \
  $ROOT/examples/eval/run_eval.py \
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

## Variante : éval avec un modèle OpenAI

`make_llm` route par **nom de modèle** : `gpt-*` / `o*` → OpenAI (clé `OPENAI_API_KEY` dans `back/.env`), sinon Gemini/Vertex. Les `provider: vertexai` des mocksql.yml de projets n'interfèrent pas.

- **Génération** : préfixer la commande `mocksql generate` de l'étape 3 avec `DEFAULT_MODEL_NAME=<modèle-openai>` (les mocksql.yml des projets d'éval ne fixent pas `llm.model`, l'env s'applique donc).
- **Juge** : remplacer `--gemini-model …` par `--model <modèle-openai>` à l'étape 4 (les variables VERTEX_PROJECT/GOOGLE_CLOUD_PROJECT deviennent inutiles mais sont inoffensives).
- ⚠️ Pour comparer un run OpenAI à la baseline Gemini, **garder le même juge** (changer uniquement le modèle de génération) — sinon les scores ne sont pas comparables.

## Notes importantes

- Le `DUCKDB_PATH` doit pointer vers `<projet>/.mocksql/mocksql.duckdb` (pas `back/data/`).
- Les chemins passés à `mocksql generate` et `run_eval.py` doivent être **absolus** (`poetry -C back` change le cwd).
- Si un modèle a une erreur DuckDB (ex: alias GROUP BY), le test est quand même écrit — l'eval peut le juger avec `exec_status=error`.
- Le rapport JSON est toujours écrit dans `examples/eval/results/<date>_<projet>.json`.
- Les scores `cohérence_données` et `cohérence_test` sont sur 5 ; `is_valid = true` si les deux >= 3.
