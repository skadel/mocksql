# MockSQL

Assistant de **test de requêtes SQL** : l'utilisateur choisit un fichier `.sql` depuis la liste de ses modèles (lus depuis `models_path` configuré localement), MockSQL génère des jeux de données d'entrée cohérents, les exécute localement sur **DuckDB** (0 € facturé sur BigQuery), attribue un verdict argumenté à chaque test, et suggère les cas limites non couverts.

Le produit se positionne comme une **couche de tests unitaires native** pour les data engineers, avec détection de dérive entre le SQL testé et le SQL en production.

---

## Positionnement concurrentiel

MockSQL partage le même mécanisme de base que les bibliothèques de mocking SQL (remplacer les vraies tables par des CTEs avec des données synthétiques, exécuter localement). La différence fondamentale est que ces outils **demandent à l'ingénieur d'écrire les données de test manuellement dans du code Python**. MockSQL est un **produit**, pas une bibliothèque : il génère, évalue, et guide.

| Dimension | Bibliothèques de mocking SQL | MockSQL |
|---|---|---|
| Données de test | Manuelles (code Python) | Générées automatiquement par LLM |
| Couverture | Aucune — l'ingé doit y penser | 6 axes + suggestions contextuelles |
| Qualité du test | Pas d'évaluation | Verdict LLM (good / warn / bad) |
| Interface | Bibliothèque dans du code de test | UI dédiée (GenerateView → TestsView) |
| Dérive | Inexistante |
| Moteur SQL | Connecteur par DB | DuckDB unifié — 0 € facturé sur BigQuery |

**Message clé** : "Les bibliothèques de mocking SQL vous forcent à écrire les données de test vous-même — MockSQL les génère pour vous, les évalue, et détecte ce que vous n'avez pas couvert."

Les deux valeurs différenciantes à ne jamais sacrifier lors de l'implémentation :
1. **Génération automatique** — l'ingé ne doit jamais écrire une ligne de données de test manuellement.
2. **Verdict LLM argumenté** — chaque test porte une évaluation qualitative, pas juste un pass/fail.

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

`models_path` est un chemin local configuré une fois via `mocksql init` (stocké dans `.mocksql/config.yaml`). `GET /models` scanne ce dossier et retourne la liste des fichiers `.sql` — c'est l'unique source de vérité pour le choix de fichier.

**Préprocesseur SQL (`preprocessor_fn`)** : quand les fichiers `.sql` contiennent des variables non-parsables (`@start_date`, `{{ ds }}`, macros dbt…), l'utilisateur peut déclarer dans `mocksql.yml` une fonction Python qui transforme le SQL brut avant que MockSQL ne l'analyse :

```yaml
preprocessor_fn: "preprocessors:replace_vars"  # module:fonction, résolu depuis le dossier mocksql.yml
```

La fonction reçoit le SQL brut (`str`) et retourne le SQL transformé (`str`). Elle est appelée à chaque lecture de fichier (CLI `generate` et `GET /models`). Voir `storage/config.py:load_preprocessor_fn` pour l'implémentation.

**Contexte métier (`mocksql.md`)** : l'ingé peut déposer des fichiers `mocksql.md` à n'importe quel niveau de son dossier de modèles pour annoter le contexte métier injecté dans les prompts LLM. La résolution est en cascade, du global au spécifique :

```
models_path/
  mocksql.md              ← contexte global projet
  finance/
    mocksql.md            ← contexte du dossier finance
    revenue.sql
    revenue.md            ← contexte spécifique au fichier revenue.sql
```

Les fichiers trouvés sont concaténés dans l'ordre (global → spécifique) et injectés dans le prompt du générateur sous le bloc `**Contexte métier du projet**`. Un `mocksql.md` peut contenir la description du domaine, des règles métier, des cas particuliers à couvrir (ex. "un utilisateur peut avoir 2 comptes"), ou tout autre contexte que l'ingé juge utile pour guider la génération. Voir `storage/context_loader.py:load_model_context` pour l'implémentation.

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

### Chat MockSQL vs Commentaires

| Dimension           | Chat MockSQL                              | Commentaires                              |
| ------------------- | ------------------------------------------ | ----------------------------------------- |
| Destinataire        | L'IA (modifier/générer un test)            | L'équipe (annotations humaines)           |
| Ancrage             | Global ou ancré sur un test spécifique     | Toujours ancré sur un test                |
| Persistance         | Session / historique de conversation       | Stockés avec le modèle (localStorage→DB)  |
| Traitement          | MockSQL répond, met à jour le test        | Non traité par l'IA                       |

### DuckDB — positionnement technique

Les requêtes ne sont **jamais** exécutées sur BigQuery ou Postgres — uniquement sur DuckDB en local. L'UI doit le préciser :
- Chip topbar : `BigQuery · DuckDB local` (tooltip : "Exécution locale — 0 € facturé")
- Tooltip bouton ré-exécuter : `Exécution locale · 0 € facturé`
- Vue ImportView : "MockSQL exécute tes requêtes en local via DuckDB — aucune requête facturée sur BigQuery."
---

## Stack

| Couche     | Technologie                                              |
| ---------- | -------------------------------------------------------- |
| Backend    | Python 3.12 · FastAPI · LangGraph · LangChain            |
| LLM        | VertexAI (Google Cloud) — **gemini-2.5-flash / pro** (thinking natif requis) |
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
    profile_checker.py     # validation/fusion du profil (colonnes + joins + exprs dérivées)
    profiler.py            # moteur de profiling : requêtes SQL, régularité temporelle, exprs dérivées
    test_evaluator.py      # verdict structuré (Excellent/Bon/Insuffisant + bad_data/bad_assertions)
    suggestions_node.py    # suggestions contextualisées avec données réelles + verdicts
    conversational_agent.py # agent chat utilisateur : correction guidée via outils (patch/add/remove row, run_cte)
    constraint_simplifier.py  # extraction contraintes SQL + detect_select_derived_expressions
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
START → pre_routing → routing → [route_input] → generator → executor → [route_executor] → evaluate_tests → [route_evaluator] → suggestions_generator → final_response → history_saver → END
                                  ↘ conversational_agent  (input chat utilisateur sur tests existants)
                                  ↘ other                 (question hors-sujet)

Sur verdict Insuffisant (route_evaluator) :
  bad_data (inclut empty_results) + retries>0  → bad_data_regen (décrémente gen_retries) → generator → executor   (régénération complète)
  bad_data + retries==0                         → bad_data_exhausted → history_saver
  bad_assertions                                → assertion_corrector / history_saver
  assertion_only / rerun_only / too_many_rows   → history_saver
```

Voir le détail complet dans [docs/workflow-query-generation.md](docs/workflow-query-generation.md).

### Nœuds clés

| Nœud                   | Fichier                      | Rôle                                                                |
| ---------------------- | ---------------------------- | ------------------------------------------------------------------- |
| `pre_routing`          | `query_chain.py:19`          | Court-circuit si même requête (cache session)                       |
| `routing`              | `routing.py`                 | Choisit le nœud suivant selon le contenu du state                   |
| `generator`            | `examples_generator.py`      | Génère les données de test via LLM                                  |
| `executor`             | `examples_executor.py`       | Exécute la requête sur DuckDB, trace les CTEs                       |
| `profile_checker`      | `profile_checker.py`         | Valide et fusionne le profil statistique (colonnes + joins + exprs dérivées) |
| `evaluate_tests`       | `test_evaluator.py`          | Verdict structuré : Excellent / Bon / Insuffisant + cause (bad_data / bad_assertions) |
| `suggestions_generator`| `suggestions_node.py`        | 3 suggestions contextualisées avec données réelles + verdicts       |
| `conversational_agent` | `conversational_agent.py`    | Agent chat **utilisateur** : corrige/génère via outils (`patch_test_field`, `add_test_row`, `remove_test_row`, `run_cte`). **Hors** boucle de retry auto |
| `bad_data_regen`       | `query_chain.py`             | Décrémente `gen_retries` puis re-route vers le `generator` (régénération complète sur `bad_data`) |
| `history_saver`        | `query_chain.py`             | Persiste l'historique en base                                       |

### Boucles de retry

**Boucle evaluate → generator (régénération complète)** : si `evaluation_feedback == "bad_data"` (ce qui inclut `empty_results`) et `gen_retries > 0` → `bad_data_regen` décrémente `gen_retries` puis re-route vers le `generator`, qui **régénère intégralement** les données en ciblant la CTE en échec (le `cte_trace` voyage dans le message RESULTS, et le diagnostic niveau-étape est réinjecté dans le feedback via `_build_eval_messages`). Retries épuisés → `bad_data_exhausted` → `history_saver`.

> ⚠️ Le `conversational_agent` n'est **pas** dans cette boucle automatique. C'était le cas historiquement (d'où le commentaire de `bad_data_regen`), mais le routage a été simplifié vers une régénération complète : le générateur étant **sans état**, il peut reproduire des sorties quasi identiques d'un retry à l'autre. L'agent reste réservé aux corrections **guidées par l'utilisateur** (input chat), où ses outils (`patch_test_field`, `add_test_row`, `run_cte`) permettent une correction **incrémentale**.

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

## Workflow utilisateur — Implémentation frontend

### Vue d'ensemble des composants

```
App
└── DrawerComponent (sidebar — appBarSlice)
│     ├── SqlFileList  → liste des modèles SQL
│     └── ProjectSelector
└── QueryChatComponent  (composant principal — buildModelSlice)
      ├── SQLQueryBar         → éditeur SQL + historique
      ├── MessageDisplay      → thread de conversation
      │     └── MessageBody   → rendu par type (examples, results, error, profile_query…)
      │           └── DisplayTable  → tableau paginé (résultats DuckDB)
      ├── MissingTablesAlert  → import de tables manquantes (ImportView)
      └── TestsPanel          → liste des tests générés + verdicts
```

### Flux étape par étape

#### 1. Sélection du modèle
- Sidebar (`SqlFileList`) appelle `GET /api/models` → `fetchModels` → `appBarSlice`
- Clic sur un modèle → `setCurrentId` dans appBarSlice → URL `/models/{modelId}`
- `QueryChatComponent` détecte le changement → `dispatch(getMessages({ modelId, t }))`
- `getMessages.fulfilled` → remplit `queryComponentGraph`, `testResults`, `sqlHistory`, `query`, `optimizedQuery`
- `setDefaultBranchSelection` configure la branche active dans `selectedChildIndices`

#### 2. Generate tests (première génération)
- L'utilisateur saisit un message dans `DroppableTextField` → `handleSubmit()`
- Validation optionnelle : `validateQueryApi` → retourne `valid` / erreur
- `dispatch(chatQuery(params))` → SSE stream vers `POST /api/query/build/stream_events`
- Événements SSE reçus dans l'ordre :
  - `on_chain_start` → `setLoadingMessage(loadingMap[step])` (ex: "Génération des données…")
  - `on_chain_stream` → `formatMessage(m)` → `appendQueryComponentMessage(msg)` ou `appendComponentToLastMessage(msg)` (tokens)
  - Messages `examples` (contentType) → pre-populate `testResults` avec status `pending`
  - Messages `results` → merge dans `testResults` par `test_index`
  - Messages `evaluation` → attache `verdict` au test correspondant
- `chatQuery.fulfilled` → `loading = false`, `streamingReasoning = undefined`
- Auto-save : `patchModelSql({ sql, optimizedSql, testResults })`

#### 3. Tables manquantes (ImportView)
- Si DuckDB ne connaît pas une table référencée dans le SQL → backend retourne une erreur avec `missing_tables`
- `MissingTablesAlert` s'affiche avec la liste des tables
- Clic "Importer" → `importMissingTablesApi` → `pendingSessionRef` conserve les params pour retry
- Import terminé → re-déclenche `chatQuery` avec les mêmes paramètres

#### 4. Modifier via le chat
- L'utilisateur envoie un message ancré sur un test (`selectedTestIndex`) ou global
- Même flux SSE que §2 — `parentMessageId` pointe vers le message existant
- Si plusieurs réponses pour un même message-parent → `children[]` crée une branche
- `MessageGroupComponent` rend la navigation branches (1/3, 2/3…)
- `selectedChildIndices[parentId]` mémorise la branche affichée

#### 5. Modifier le SQL directement
- `SQLQueryBar` : l'utilisateur édite le SQL → clique "Mettre à jour" → `onUpdate(editedSql)`
- `QueryChatComponent` reçoit le nouveau SQL → `pushSqlHistory(entry)` (dédup automatique)
- `setOptimizedQuery('')` → re-dispatch `chatQuery` avec `context: 'sql_update'`
- `sql_update` context → tous les tests existants passent en `pending` avant régénération

#### 6. Retry automatique (boucle executor)
- Backend : si `status == "empty_results"` et `gen_retries > 0` → `generator` re-tourne (max 2 fois)
- `failing_cte` identifie la première CTE vide → le générateur cible uniquement cette CTE
- Côté front : transparent — les messages SSE continuent d'arriver normalement

#### 7. Restauration depuis l'historique SQL
- `SQLQueryBar` affiche un popover historique (`sqlHistory[]`)
- Clic sur une entrée → `onHistorySelect(entry)` → `setRestoredMessageId(entry.parentMessageId)`
- `historyRestoreTrigger` incrémenté → `SQLQueryBar` ré-ouvre l'accordéon et affiche le SQL restauré
- `skipValidationRef = true` → évite une re-validation inutile

#### 8. État Redux — champs actifs (après nettoyage)

| Champ | Rôle |
|---|---|
| `queryComponentGraph` | Arbre de messages (id → Message) — source de vérité |
| `testResults` | Tests avec status, verdict, données — synced depuis SSE |
| `selectedChildIndices` | Branche active par parent (navigation multi-tour) |
| `sqlHistory` | Versions SQL dédupliquées |
| `streamingReasoning` | Reasoning LLM accumulé pendant le stream |
| `loading` / `loading_message` | État de chargement + texte affiché |
| `query` / `optimizedQuery` | SQL persisté (sync depuis messages, utilisé au chargement) |
| `restoredMessageId` | Message à highlighter après restauration historique |
| `lastError` | Dernière erreur backend (affiché dans UI) |

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
- **`evaluation_feedback`** : quand le verdict est `Insuffisant`, `test_evaluator` classifie la cause en `"bad_data"` (données incorrectes → régénération complète via le `generator`, nœud `bad_data_regen`) ou `"bad_assertions"` (assertions mal définies → pas de relance automatique). Ce champ est dans `QueryState` et est utilisé par `route_evaluator`.
- **Modèle LLM & raisonnement (`is_native_thinking_active`)** : MockSQL est optimisé pour **gemini-2.5-flash / pro**, où le thinking natif Gemini est activé par défaut. `storage/config.py:is_native_thinking_active()` combine le réglage explicite (`thinking_budget`/`thinking_level`) et le défaut du modèle (flash/pro = ON, flash-lite = OFF). Quand le thinking est actif, le champ `unit_test_build_reasoning` n'est qu'une **justification brève (1 phrase)** — le vrai raisonnement se fait dans le canal thinking, hors du JSON de sortie → pas de risque de troncature. Quand il est inactif (flash-lite, ou `thinking_budget: 0`), le champ porte un **chain-of-thought complet (3 phrases max)** comme seul raisonnement, et le générateur logue un warning poussant vers flash/pro. La bascule pilote à la fois le schéma Pydantic (`get_generation_output_type`) et le prompt (`generate_data_prompt`, paramètre `native_thinking`).
- **Profil statistique enrichi** : le profil stocké peut désormais contenir des `derived_expressions` (expressions SQL non-triviales profilées sur les données réelles, ex : `SAFE_CAST`, `REGEXP_EXTRACT`) injectées dans le prompt de génération via `_format_profile_block`. Les JOINs profilés peuvent inclure un `right_filter` et des `left_cte_sql`/`right_cte_sql` pour contextualiser les cardinalités.
- **Auto-import per projet** : l'auto-import silencieux de tables manquantes peut être activé globalement (`localStorage.autoImport_always`) ou par projet (`localStorage.autoImport_project_{projectId}`). Les deux flags sont vérifiés dans `QueryChatComponent`.
- **Coverage score** : calculé côté front à partir des titres/tags des tests ; le backend n'a pas à le connaître. Les 6 axes sont des heuristiques regex sur le texte des tests.
- **DuckDB positionné comme économie de coût** : toute mention de l'exécution dans l'UI doit souligner "0 € facturé sur BigQuery" — c'est un argument commercial, pas juste un détail technique.
- **Import de tables manquantes** : l'`ImportView` est une étape intermédiaire entre `GenerateView` et `TestsView` — elle s'affiche uniquement si des tables référencées dans le SQL ne sont pas disponibles localement. Chaque table a son propre état (`pending` / `importing` / `done`).
- **Dérive (drift)** : quand un modèle est lié à un fichier Git/dbt, stocker le `source_sha` à la création. Si le SHA change, afficher un bandeau d'alerte dans la TestsView et passer les tests en état `stale`.
- **Contexte métier (`mocksql.md`)** : chargé dans `pre_routing` via `load_model_context(model_name)` et stocké dans `QueryState.model_context`. Propagé à tous les appels de `generate_data_prompt` et `update_data_prompt`. La résolution est en cascade (global → dossier → fichier) — voir `storage/context_loader.py`. Le champ est `Optional[str]` : s'il est absent ou vide, aucun bloc contexte n'apparaît dans le prompt.
