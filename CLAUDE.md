# PipeTalk

Assistant de **test de requêtes SQL** : l'utilisateur choisit un fichier `.sql` depuis la liste de ses modèles (lus depuis `models_path` configuré localement), PipeTalk génère des jeux de données d'entrée cohérents, les exécute localement sur **DuckDB** (0 € facturé sur BigQuery), attribue un verdict argumenté à chaque test, et suggère les cas limites non couverts.

Le produit se positionne comme une **couche de tests unitaires native** pour les data engineers, avec détection de dérive entre le SQL testé et le SQL en production.

---

## Concept produit & flux UI

### Flux principal

```
1. GenerateView  → liste des fichiers .sql disponibles dans models_path (GET /models)
                   l'utilisateur choisit un fichier ; le SQL est lu côté backend
2. ImportView    → si des tables référencées n'existent pas localement : import par table
                   avec taille estimée, progression, et mention "exécution DuckDB locale"
3. TestsView     → liste des tests générés + couverture + suggestions
```

`models_path` est un chemin local configuré une fois via `pipetalk init` (stocké dans `.mocksql/config.yaml`). `GET /models` scanne ce dossier et retourne la liste des fichiers `.sql` — c'est l'unique source de vérité pour le choix de fichier.

### Tests et verdicts

Chaque test généré porte :
- **Titre** : description en langage naturel du scénario couvert
- **Tags** : `Logique métier`, `Cas limites`, `Intégration`
- **Verdict** (qualité du test) : `good` (Bon / vert), `warn` (Insuffisant / ambre), `bad` (Incorrect / rouge)
- **Texte de verdict** : explication argumentée du verdict (générée par LLM)
- **Statut d'exécution** : `pass` / `fail` (DuckDB a-t-il retourné les lignes attendues ?)
- **Données d'entrée** : lignes synthétiques injectées dans DuckDB
- **Données de sortie attendues** : colonne cible + valeurs attendues

Les deux indicateurs (verdict qualité + statut exécution) sont **distincts** — un test peut s'exécuter sans erreur mais avoir un verdict `warn` si les données ne couvrent pas vraiment le cas.

### Couverture (6 axes)

| Axe             | Clé       | Description                              |
| --------------- | --------- | ---------------------------------------- |
| Chemin nominal  | `happy`   | Le cas standard, requête correcte        |
| Valeurs NULL    | `null`    | Comportement de LAG/JOIN sur NULL        |
| Plage vide      | `empty`   | Filtre qui ne retourne aucune ligne      |
| Valeurs égales  | `equal`   | Colonnes identiques consécutives         |
| Ex æquo         | `tie`     | Résultats non-déterministes (LIMIT 1)    |
| Format de sortie| `types`   | FORMAT_DATE, CAST, patterns de formatage |

Score = somme des poids des axes couverts. Les axes manquants alimentent les **suggestions contextuelles**.

### Chat PipeTalk vs Commentaires

| Dimension           | Chat PipeTalk                              | Commentaires                              |
| ------------------- | ------------------------------------------ | ----------------------------------------- |
| Destinataire        | L'IA (modifier/générer un test)            | L'équipe (annotations humaines)           |
| Ancrage             | Global ou ancré sur un test spécifique     | Toujours ancré sur un test                |
| Persistance         | Session / historique de conversation       | Stockés avec le modèle (localStorage→DB)  |
| Traitement          | PipeTalk répond, met à jour le test        | Non traité par l'IA                       |

### DuckDB — positionnement technique

Les requêtes ne sont **jamais** exécutées sur BigQuery ou Postgres — uniquement sur DuckDB en local. L'UI doit le préciser :
- Chip topbar : `BigQuery · DuckDB local` (tooltip : "Exécution locale — 0 € facturé")
- Tooltip bouton ré-exécuter : `Exécution locale · 0 € facturé`
- Vue ImportView : "PipeTalk exécute tes requêtes en local via DuckDB — aucune requête facturée sur BigQuery."

### Connecteurs SQL (roadmap)

Le flow actuel lit les fichiers `.sql` directement depuis `models_path` (dossier local). Les connecteurs ci-dessous remplacent ou enrichissent cette source à terme.

**dbt (priorité 1)** — lit le `manifest.json` pour récupérer le SQL compilé, les sources, le DAG de dépendances, et les tests dbt existants (`schema.yml`). `models_path` pointerait vers les modèles dbt compilés. Permet l'import DuckDB automatique des tables upstream.

**Git/GitHub (priorité 2)** — enrichit le fichier local avec un suivi de SHA : stocke le SHA du commit à la création des tests. Détection de dérive : bandeau *"La requête a évolué en prod (3 commits depuis le dernier test)"* avec diff et CTA "Ré-évaluer".

**Dérive (drift detection)** — quand le SHA du fichier source change, les tests existants passent en état `stale` et un bandeau d'alerte s'affiche dans la TestsView.

---

## Stack

| Couche     | Technologie                                              |
| ---------- | -------------------------------------------------------- |
| Backend    | Python 3.12 · FastAPI · LangGraph · LangChain            |
| LLM        | VertexAI (Google Cloud)                                  |
| SQL        | DuckDB (exécution locale) · BigQuery / Postgres (source) |
| SQL parse  | `sqlglot`                                                |
| Base       | PostgreSQL (Cloud SQL)                                   |
| Frontend   | React 18 · TypeScript · Redux Toolkit · MUI              |
| Connecteurs (roadmap) | dbt (`manifest.json`) · GitHub API (SHA tracking) |

---

## Structure du projet

```
back/
  app/api/endpoints/   # routes FastAPI (query.py, models.py, projects.py, messages.py)
  app/services/        # query_service.py
  build_query/         # cœur du système : graph LangGraph
    query_chain.py     # définition du graph (nœuds + edges)
    routing.py         # logique de routage entre nœuds
    state.py           # QueryState (TypedDict)
    validator.py       # dry-run BigQuery/Postgres/DuckDB + sqlglot
    examples_generator.py  # génération LLM des données de test
    examples_executor.py   # exécution DuckDB + CTE trace
    profile_checker.py     # validation/fusion du profil statistique
    constraint_simplifier.py
  tests/               # pytest
front/
  src/
    api/               # appels HTTP vers le backend
    app/               # Redux store + hooks
    features/
      buildModel/      # feature principale
        buildModelSlice.ts
        components/    # QueryChatComponent, TestsPanel, ProfilingStep, …
                       # → GenerateView, ImportView, TestsView, TestCard
                       # → CoverageBar, ChatPanel (overlay rétractable)
                       # → Suggestions contextuelles, Commentaires
      Project/         # gestion des projets
      appBar/          # sidebar (datasets, historique des modèles)
docs/
  workflow-query-generation.md   # flux complet frontend → backend
```

---

## Commandes de développement

### Backend

```bash
cd back

# Environnement virtuel
python -m venv .venv && source .venv/bin/activate  # Linux/Mac
# .\.venv\Scripts\activate                          # Windows

pip install poetry && poetry install

# Lancer le serveur
poetry run langchain serve --port 8100

# Qualité du code (via Makefile)
make style   # ruff check + ruff format --check + vulture (code mort)
make format  # ruff format + ruff check --fix (auto-correction)
make test    # pytest
make check   # style + test

# Type checking (mypy — à installer si absent : poetry add --group dev mypy)
poetry run mypy build_query/ app/
```

> **vulture** détecte le code mort (fonctions/variables non utilisées). Configuré avec `--min-confidence 80 --exclude ".venv,venv"`. Les faux positifs (callbacks LangGraph) sont rares — vérifier avant de supprimer.

### Frontend

```bash
cd front
npm ci
npm start          # dev (port 3000)
npm test           # tests Jest
npm run build      # build de production

# Linting
npx eslint src/    # ESLint (déjà configuré via react-scripts)

# Formatage (prettier — à installer si absent : npm install --save-dev prettier)
npx prettier --write src/
```

---

## Architecture du graph LangGraph

```
START → pre_routing → routing → [route_input] → generator → executor → [route_executor] → history_saver → END
                                              ↘ executor  (used_columns vides)
                                              ↘ other     (question hors-sujet)
```

Voir le détail complet dans [docs/workflow-query-generation.md](docs/workflow-query-generation.md).

### Nœuds clés

| Nœud              | Fichier                      | Rôle                                              |
| ----------------- | ---------------------------- | ------------------------------------------------- |
| `pre_routing`     | `query_chain.py:19`          | Court-circuit si même requête (cache session)     |
| `routing`         | `routing.py`                 | Choisit le nœud suivant selon le contenu du state |
| `generator`       | `examples_generator.py`      | Génère les données de test via LLM                |
| `executor`        | `examples_executor.py`       | Exécute la requête sur DuckDB, trace les CTEs     |
| `profile_checker` | `profile_checker.py`         | Valide et fusionne le profil statistique          |
| `history_saver`   | `query_chain.py`             | Persiste l'historique en base                     |

### Boucle de retry

`executor` → `route_executor` → `generator` (si `status == "empty_results"` et `gen_retries > 0`)
— maximum 2 régénérations automatiques (`gen_retries` commence à 2).

---

## Traitement des messages — Frontend

### Type `Message` (`front/src/utils/types.ts`)

```ts
interface Message {
  id: string;
  type: 'user' | 'bot';
  contents: MessageContents;   // données affichées
  parent?: string;             // id du message parent (arbre de branches)
  children?: string[];         // ids des messages enfants
  contentType?: string | null; // type métier du message (voir tableau ci-dessous)
  request?: string | null;     // request_id associé
  testIndex?: number;          // index du test concerné (multi-test)
  context?: 'sql_update';
}

interface MessageContents {
  text?: string;           // texte libre / message utilisateur
  sql?: string;            // requête SQL brute
  optimizedSql?: string;   // requête SQL optimisée
  tables?: ...;            // données de test générées (type 'examples')
  res?: any[];             // résultats d'exécution DuckDB (type 'results')
  real_res?: any[];        // résultats réels BigQuery paginés
  meta?: DisplayTableMeta; // métadonnées de pagination
  error?: string;          // message d'erreur (type 'error')
  profileRequest?: ProfileRequest; // demande de profiling (type 'profile_query')
}
```

### `contentType` — valeurs possibles

| `contentType`     | `type` message | Contenu principal             |
| ----------------- | -------------- | ----------------------------- |
| `'examples'`      | `bot`          | `tables` + `sql` / `optimizedSql` |
| `'results'`       | `bot`          | `res` + `sql` / `optimizedSql` |
| `'error'`         | `bot`          | `error`                       |
| `'profile_query'` | `bot`          | `profileRequest`              |
| `'user_examples'` | `user`         | `tables` + `text`             |
| `'sql_update'`    | `user`         | `text` (label fixe)           |
| `'query'`         | `user`         | texte brut                    |
| `null`/autre      | selon `type`   | `text`                        |

### `formatMessage()` (`front/src/utils/messages.ts`)

Transforme un message brut renvoyé par le backend (format LangChain/LangGraph) en `Message` Redux. Lit `additional_kwargs.type` pour déterminer le `contentType`, puis `JSON.parse(message.content)` pour peupler les champs de `contents`.

### Deux chemins d'entrée des messages

#### 1. Streaming SSE — `chatQuery` (`front/src/api/query.ts`)

Pendant l'exécution du graph LangGraph, les événements `on_chain_stream` arrivent en temps réel :

```
on_chain_stream → pd.data.chunk.messages[]
  → formatMessage(m) → appendQueryComponentMessage(nm)
```

`appendQueryComponentMessage` (slice, ligne 145) :
- Insère le message dans `queryComponentGraph[msg.id]`
- Ajoute `msg.id` dans `queryComponentGraph[msg.parent].children`
- Met à jour `state.testResults` si `contents.res` est présent

**Affichage progressif** : pour les messages texte/SQL qui arrivent token par token, utiliser `appendComponentToLastMessage` au lieu de `appendQueryComponentMessage`. Cette action (slice, ligne 92) cherche un message existant avec le même `id` (ou sans id) et le même `parent`, puis **concatène** `contents.text` et `contents.sql` au lieu de remplacer. Les autres champs (`tables`, `res`, …) ne sont pas mergés — ils arrivent en une seule fois via `appendQueryComponentMessage`.

#### 2. Chargement de l'historique — `getMessages` (`front/src/api/messages.ts`)

Lors de l'ouverture d'un modèle existant, `GET /api/getMessages` renvoie tous les messages persistés :

```
getMessages.fulfilled → messages[].forEach(formatMessage)
  → queryComponentGraph[id] = message          (1er passage)
  → queryComponentGraph[parent].children.push  (2ème passage)
  → setDefaultBranchSelection(latestMessage)   (sélection de branche)
```

Le fallback (lignes 276-293 du slice) reconstruit `testResults` et `query` depuis le dernier message `results` si le modèle ne les a pas persistés séparément.

### `queryComponentGraph` — structure Redux

Dictionnaire plat `Record<string, Message>` stocké dans `buildModelState`. Les relations parent/enfant sont portées par `Message.children[]` (ids). Pour afficher l'arbre, partir de la racine (messages sans `parent`) et suivre les `children`, en utilisant `selectedChildIndices[parentId]` pour choisir la branche active quand un nœud a plusieurs enfants.

---

## Conventions importantes

### Backend (Python)

- **Tables DuckDB suffixées** : toujours `{session_id}{test_index}` pour éviter les collisions entre sessions concurrentes.
- **Qualification SQL obligatoire** : passer par `sqlglot.qualify_tables` + `qualify_columns` avant toute extraction de colonnes — les références partielles (`table.col` sans projet/dataset) provoquent des extractions incorrectes.
- **`used_columns` format** : liste de `{project, database, table, used_columns: []}` — c'est la clé de toute la logique de génération et de profiling.
- **`query_decomposed`** : JSON encodé en string (pas un dict) — toujours `json.loads()` avant usage.
- **State LangGraph** : `QueryState` est un `TypedDict` dans `state.py` — ajouter les nouveaux champs ici d'abord.
- **Streaming SSE** : le graph tourne en mode streaming via LangGraph ; les événements sont émis au fil de l'exécution des nœuds.

### Frontend (TypeScript/React)

- **Redux** : état global dans `buildModelSlice.ts` ; les sélecteurs sont dans `src/selectors/`.
- **SSE** : utilise `@microsoft/fetch-event-source` pour le stream (`chatQuery` thunk dans le slice).
- **`pendingSessionRef`** : ref qui conserve la session en cours pour permettre un retry après import de tables manquantes — ne pas réinitialiser avant la fin du flux.
- **`skipValidationRef`** : positionné à `true` lors d'une restauration depuis l'historique pour éviter une re-validation inutile.

---

## Points d'attention

- Le **dialect** (`bigquery` / `postgres` / `duckdb`) conditionne la validation et l'optimisation SQL — toujours le propager.
- Le **profil statistique** (min/max, cardinalité) est facultatif mais améliore significativement la qualité des données générées.
- La route `__fix_error__` est déclenchée automatiquement si `alwaysFix` est activé côté client (localStorage).
- `failing_cte` : quand DuckDB retourne 0 lignes, un **CTE trace** identifie la première CTE vide — le générateur cible alors uniquement cette CTE pour la correction.
- **Verdict vs statut d'exécution** : `verdict` (`good`/`warn`/`bad`) mesure la *qualité du test* (logique, assertions), `execStatus` (`pass`/`fail`) mesure si DuckDB a retourné le résultat attendu. Ne pas confondre — les deux coexistent sur le même test.
- **Coverage score** : calculé côté front à partir des titres/tags des tests ; le backend n'a pas à le connaître. Les 6 axes sont des heuristiques regex sur le texte des tests.
- **DuckDB positionné comme économie de coût** : toute mention de l'exécution dans l'UI doit souligner "0 € facturé sur BigQuery" — c'est un argument commercial, pas juste un détail technique.
- **Import de tables manquantes** : l'`ImportView` est une étape intermédiaire entre `GenerateView` et `TestsView` — elle s'affiche uniquement si des tables référencées dans le SQL ne sont pas disponibles localement. Chaque table a son propre état (`pending` / `importing` / `done`).
- **Dérive (drift)** : quand un modèle est lié à un fichier Git/dbt, stocker le `source_sha` à la création. Si le SHA change, afficher un bandeau d'alerte dans la TestsView et passer les tests en état `stale`.
