# Workflow : Génération de tests unitaires à partir d'une requête SQL

Ce document décrit le flux complet, du formulaire frontend jusqu'à l'exécution des tests DuckDB, en couvrant les chemins alternatifs (tables manquantes, profiling, modification de requête, etc.).

---

## Vue d'ensemble

```
[Utilisateur saisit une requête SQL]
        │
        ▼
  validate-query ──── tables manquantes ? ──► import-missing-tables ──► (retry)
        │ valid                                (auto-import global ou par projet)
        ▼
  check-profile ──── profil incomplet ? ──► [dialog sans/avec comparison + skip]
        │ complet
        ▼
  chatQuery (stream SSE)
        │
        ▼
   [Backend : LangGraph]
        │
        ├─ pre_routing (cache hit ?)
        │       │ hit → saute validator, pré-remplit state
        │       │ miss → continue
        ▼
      routing
        │
        ├─ profile_result fourni   → profile_checker (enregistre le profil)
        ├─ user_tables fourni      → executor (re-run avec données custom)
        ├─ __fix_error__           → fixer (corrige l'erreur SQL)
        ├─ input seul (pas de SQL) → classifier LLM → other | generator
        └─ query SQL               → generator
                                          │
                                          ▼
                                       generator (génère les données de test)
                                          │
                                          ▼
                                       executor (exécute les tests sur DuckDB)
                                          │
                                 résultats vides + retries > 0 ?
                                       │ oui → generator (boucle)
                                       │ non
                                          ▼
                                    evaluate_tests (verdict structuré : bad_data / bad_assertions)
                                          │
                                 evaluation_feedback == "bad_data" + retries > 0 ?
                                       │ oui → conversational_agent (correction données)
                                       │ assertion_only → history_saver
                                       │ sinon
                                          ▼
                                    suggestions_generator → history_saver → [FIN]
```

---

## 1. Frontend : soumission initiale (`handleNewChatSubmit`)

Fichier : [QueryChatComponent.tsx](../front/src/features/buildModel/components/QueryChatComponent.tsx)

### Étapes

#### 1.1 Création du modèle
- Un `session_id` (UUID) est généré.
- `POST /models` crée l'entrée en base pour ce modèle.
- La session est conservée dans `pendingSessionRef` pour permettre un retry après import de tables.

#### 1.2 `validate-query`
- Appel : `validateQueryApi({ sql, project, dialect, session })`
- Le backend compile la requête (dry-run BigQuery / Postgres / DuckDB).
- Retourne :
  - `valid: true` + `used_columns` + `optimized_sql` si OK
  - `valid: false` + `missing_tables` si des tables sont absentes du schéma enregistré
  - `valid: false` + `error` pour toute autre erreur SQL

**Chemin alternatif — tables manquantes :**
- Si `missing_tables` est retourné → affichage du composant [MissingTablesAlert.tsx](../front/src/features/buildModel/components/MissingTablesAlert.tsx)
- Si `auto_import_available` → bouton « Importer automatiquement » disponible
  - `importMissingTablesApi` appelle le backend pour ajouter les tables au catalogue
  - Après succès → `handleNewChatSubmit()` est relancé (même `pendingSessionRef`)
- **Auto-import silencieux** : déclenché automatiquement si `localStorage.autoImport_always === 'true'` (global) **ou** `localStorage.autoImport_project_{projectId} === 'true'` (par projet). Le réglage par projet permet d'activer l'auto-import uniquement pour certains projets.

#### 1.3 `check-profile`
- Appel : `checkProfileApi({ sql, project, dialect, session, used_columns })`
- Vérifie si le profil statistique (min/max, cardinalité, etc.) des colonnes utilisées est disponible.
- Retourne :
  - `profile_complete: true` → on passe directement au stream
  - `profile_complete: false` + `profile_request` :
    - Si `auto_profile_available` → dialog [ProfilingStep.tsx](../front/src/features/buildModel/components/ProfilingStep.tsx) :
      - Le dialog affiche une **comparaison sans/avec profil** (grille à deux colonnes) pour contextualiser la valeur du profiling, et une chip discrète indiquant la taille de scan estimée (To · BigQuery).
      - **Confirmer** → `autoProfileApi` exécute la requête de profiling, puis stream
      - **Passer** → `skipProfilingApi` marque la session comme `profile_skipped`, puis stream

#### 1.4 Démarrage du stream
- `dispatch(chatQuery(...))` ouvre un flux SSE vers `POST /query/stream`.
- Les paramètres envoyés : `query` (SQL), `sessionId`, `project`, `dialect`, éventuellement `userInput`.
- L'UI affiche un stepper de progression (`submissionStep`).

---

## 2. Backend : graph LangGraph (`build_query_graph`)

Fichier : [query_chain.py](../back/build_query/query_chain.py)

```
START → pre_routing → routing → [route_input] → generator → executor → [route_executor] → history_saver → END
                                              ↘ executor (si used_columns vides)
                                              ↘ other
```

### 2.1 `pre_routing`

Fichier : [query_chain.py:19](../back/build_query/query_chain.py#L19)

- Charge depuis la DB : `optimized_sql`, `sql`, `used_columns` pour la session.
- Si la requête entrante **correspond** à celle stockée → court-circuit :
  - `validated_sql`, `optimized_sql`, `used_columns` pré-remplis
  - `profile_complete: True` (le profil projet est rechargé)
- Si la requête est différente → retourne `{}` (continue normalement).

**But :** éviter de refaire validation + profiling si l'utilisateur re-soumet la même requête.

### 2.2 `routing`

Fichier : [routing.py](../back/build_query/routing.py)

Détermine la route en fonction du contenu du state :

| Condition | Route |
|---|---|
| `profile_result` fourni | `profile_checker` |
| `user_tables` fourni | `executor` |
| `input == "__fix_error__"` | `fixer` |
| `input` seul (sans SQL), hors `test_index` | LLM classifier → `other` ou `generator` |
| Défaut | `generator` |

Le **classifier LLM** (`_classify_intent`) est appelé avec l'historique de messages pour distinguer les questions hors-sujet (`other`) des instructions de modification de tests (`generator`).

### 2.3 `route_input` (edge conditionnel)

Fichier : [query_chain.py:110](../back/build_query/query_chain.py#L110)

| Condition | Nœud suivant |
|---|---|
| `state.error` | `history_saver` (court-circuit) |
| `route` contient `"executor"` | `executor` |
| `route == "other"` | `other` |
| `used_columns` vide | `executor` (données custom) |
| Défaut | `generator` |

---

## 3. `generator` — Génération des données de test

Fichier : [examples_generator.py](../back/build_query/examples_generator.py)

### Entrées
- `used_columns` : liste des tables/colonnes réellement utilisées dans la requête
- `optimized_sql` / `query_decomposed` : SQL optimisé et décomposé en CTEs
- `profile` : statistiques de colonnes (facultatif)
- `input` : instruction utilisateur éventuelle (ex : "ajoute un test avec des nulls")
- `test_index` : si fourni, modifie uniquement ce test (0-based)

### Logique de décision (`_should_regenerate`)
| Condition | Régénère ? |
|---|---|
| Aucun test existant | Oui |
| `input` utilisateur non vide | Oui |
| `used_columns` ont changé | Oui |
| Dernier statut = `empty_results` | Oui |
| Sinon | Non (retourne les tests existants) |

### Chemin de régénération ciblée (CTE échouante)
Si l'exécution précédente a identifié une CTE dont le résultat est vide (`failing_cte`), le générateur :
1. Extrait les contraintes SQL de cette CTE spécifique
2. Utilise `debug_failing_cte_prompt` pour cibler précisément la correction
3. Fusionne le résultat dans la liste de tests existante

### Chemin standard
1. Extrait les contraintes globales (JOINs, filtres WHERE, etc.) via `constraint_simplifier`
   → voir [docs/constraint-simplifier.md](constraint-simplifier.md) pour le détail complet
2. Extrait les contraintes par CTE (`per_cte`)
3. Choisit le prompt approprié :
   - **Premier test** → `generate_data_prompt`
   - **Instruction utilisateur** → `update_data_prompt`
   - **Colonnes changées** → `query_change_data_prompt`
   - **Résultats vides** → `generate_data_prompt` (avec contraintes)
4. Appelle le LLM (VertexAI) avec un parser Pydantic structuré
5. Retourne une liste de tests fusionnée (merge sur `unit_test_index`)

### Inférence de la cible (`_infer_target_test_index`)
Quand l'utilisateur envoie un message texte, un second appel LLM détermine si ce message concerne la **modification d'un test existant** (retourne son index) ou la **création d'un nouveau test** (retourne `null`).

---

## 4. `executor` — Exécution des tests sur DuckDB

Fichier : [examples_executor.py](../back/build_query/examples_executor.py)

### Flux par test
1. Parse les unit tests depuis le state (ou `user_tables` si données custom)
2. Filtre les schémas aux seules colonnes utilisées
3. Pour chaque test :
   a. Crée des tables temporaires DuckDB suffixées par `{session_id}{test_index}`
   b. Insère les données de test
   c. Exécute la requête SQL complète
   d. Si résultat **non vide** → `status: "complete"`
   e. Si résultat **vide** :
      - Lance un **CTE trace** (exécute chaque CTE individuellement pour trouver celle qui produit 0 ligne)
      - Identifie `failing_cte` (première CTE avec 0 résultats)
      - Retourne `status: "empty_results"` + `cte_trace` + `failing_cte`

### Statut global
- Au moins un test `empty_results` → statut global `"empty_results"`
- Tous `complete` → statut global `"complete"`

### `route_executor` (boucle de retry)

```python
def route_executor(state):
    if state["status"] == "empty_results" and state["gen_retries"] > 0:
        return "generator"   # ← boucle : regenerate → re-execute
    return "history_saver"
```

- `gen_retries` commence à 2 et décrémente à chaque tentative
- Maximum 2 régénérations automatiques avant de terminer

---

## 5. `evaluate_tests` & `suggestions_generator`

### 5.1 `evaluate_tests` — Verdict structuré

Fichier : [test_evaluator.py](../back/build_query/test_evaluator.py)

Appelé après `executor` (quand tous les tests ont un statut `complete`). Évalue la qualité de chaque test via un LLM structuré (`with_structured_output`).

#### Sortie Pydantic `_EvaluationOutput`

| Champ | Type | Description |
|---|---|---|
| `verdict` | `"Excellent" \| "Bon" \| "Insuffisant"` | Qualité globale du test |
| `reason_type` | `"bad_data" \| "bad_assertions" \| null` | Cause du verdict Insuffisant uniquement |
| `explanation` | `str` | Phrase courte (≤ 20 mots) en français |

**Logique de route après évaluation (`route_after_evaluate`) :**

| Condition | Nœud suivant |
|---|---|
| `assertion_only` | `history_saver` |
| `evaluation_feedback == "bad_data"` et `gen_retries > 0` | `conversational_agent` |
| Sinon | `suggestions_generator` |

**`evaluation_feedback`** est positionné dans le state LangGraph (`Optional[str]`) et vaut `"bad_data"` ou `"bad_assertions"` uniquement si le verdict est `Insuffisant`. Cela découple la cause de l'échec du mécanisme de retry.

#### Classification `bad_data` vs `bad_assertions`

- **`bad_data`** : données d'entrée incorrectes (mauvais types, contraintes non respectées, jointures impossibles) → déclenche une correction automatique via `conversational_agent` si des retries restent
- **`bad_assertions`** : assertions/résultats attendus trop permissifs ou incorrects → les données elles-mêmes sont valides, pas de relance automatique

### 5.2 `suggestions_generator` — Suggestions contextualisées

Fichier : [suggestions_node.py](../back/build_query/suggestions_node.py)

Génère 3 suggestions de cas de tests non encore couverts. Depuis le dernier refactoring, le contexte injecté dans le prompt est **enrichi avec les données réelles** des tests exécutés :

- Descriptions + tags + statut d'exécution de chaque test
- Données d'entrée (jusqu'à 3 lignes par table)
- Résultats DuckDB (jusqu'à 3 lignes)
- Verdict LLM de chaque test (extrait des messages `EVALUATION`)

Le LLM opère en **chain-of-thought en 2 étapes** :
1. Identifie l'algorithme ou le pattern métier implémenté par le SQL (détection d'anomalie, classement, agrégation multi-niveaux…)
2. Liste les hypothèses implicites que cet algorithme fait sur les données, puis propose les cas où ces hypothèses peuvent être violées

---

## 6. Chemins alternatifs

### 5.1 Modification de requête SQL (`handleSQLUpdate`)

Fichier : [QueryChatComponent.tsx:584](../front/src/features/buildModel/components/QueryChatComponent.tsx#L584)

1. Re-validation de la nouvelle requête via `validateQueryApi`
2. Si `missing_tables` → affichage de l'alerte (sans re-créer le modèle)
3. Si valide → `sendMessage('', newSql, ...)` avec la nouvelle requête
4. Backend : `pre_routing` détecte que la requête a changé → `used_columns_changed: true`
5. `generator` choisit `query_change_data_prompt` pour adapter les données aux nouvelles colonnes

**Restore depuis l'historique :** `handleHistorySelect` positionne `skipValidationRef = true` pour éviter une re-validation inutile.

### 6.2 `profile_checker` — Upload du profil utilisateur

Déclenché quand `profile_result` est présent dans le state (résultat d'une requête de profiling exécutée manuellement par l'utilisateur).

Fichier : [profile_checker.py](../back/build_query/profile_checker.py)

1. Normalise le profil entrant (format flat BigQuery → `{tables: {...}, joins: [...]}`) via `_normalize_profile`
2. Valide qu'il couvre bien les colonnes et les paires de JOINs attendus
3. Fusionne avec le profil projet existant (`_merge_profiles`)
4. Persiste en base (`PROJECTS_TABLE_NAME`)
5. Retourne `profile_complete: True` → le générateur peut utiliser le profil enrichi

#### Nouvelles capacités de normalisation

**Expressions dérivées (`derived_expressions`)** : le profil peut contenir des lignes de type `row_type = "derived_expr"` (générées par le profiler pour les `SAFE_CAST`, `REGEXP_EXTRACT`, `COALESCE`…). `_normalize_profile` les stocke dans `tables[tbl]["derived_expressions"]` sous forme de liste `{expr_sql, top_values}`.

**Déduplication des JOINs** : `_merge_profiles` accumule les jointures par signature `(left_table, right_table, left_expr, right_expr)`. Différentes variantes entre les mêmes tables (jointure directe vs via CTE) sont conservées comme entrées distinctes dans la liste.

**Filtrage du bruit JOIN** : les champs de colonnes (`total_count`, `null_count`, `min_val`, etc.) sont filtrés des lignes de type `join` pour ne pas polluer les stats de jointure avec du bruit NULL.

### 6.3 `other` — Question hors-sujet

Fichier : [query_chain.py:68](../back/build_query/query_chain.py#L68)

- Réponse conversationnelle via `build_other_prompt`
- Inclut l'historique de messages (SQL, questions, résultats)
- Retourne un `AIMessage` de type `MsgType.OTHER`

### 6.4 `__fix_error__` — Auto-correction SQL

Déclenché automatiquement si `alwaysFix` est activé (localStorage) et que le dernier message contient une erreur.

Fichier : [QueryChatComponent.tsx:654](../front/src/features/buildModel/components/QueryChatComponent.tsx#L654)

- Envoie `input = "__fix_error__"` avec le contexte de l'erreur
- `routing` charge l'historique des messages `ERROR_SQL`, `ERROR`, `SQL`
- Route vers `fixer` (non détaillé dans ce doc)

### 6.5 Modification ciblée d'un test (`test_index`)

- L'utilisateur clique sur un test dans [TestsPanel.tsx](../front/src/features/buildModel/components/TestsPanel.tsx)
- `selectedTestIndex` est transmis dans `sendMessage`
- `state.test_index` est passé au backend
- `_resolve_target_key` utilise cet index pour écraser le bon test dans la liste mergée

### 6.6 Données utilisateur custom (`user_tables`)

- L'utilisateur peut glisser-déposer ses propres données dans le panneau de tests
- `user_tables` est envoyé directement au backend
- `routing` route vers `executor` sans passer par `generator`
- Les tables custom sont parsées dans `_parse_unit_tests_from_state`

---

## 7. State LangGraph (`QueryState`)

Fichier : [state.py](../back/build_query/state.py)

| Champ | Description |
|---|---|
| `query` | SQL brut soumis par l'utilisateur |
| `validated_sql` | SQL après validation (sauvegardé en DB) |
| `optimized_sql` | SQL optimisé (qualify tables/columns) |
| `query_decomposed` | JSON des CTEs décomposées |
| `used_columns` | `[{project, database, table, used_columns[]}]` |
| `used_columns_changed` | True si les colonnes ont changé par rapport au run précédent |
| `route` | Route déterminée par `routing` |
| `status` | `"empty_results"` ou `"complete"` |
| `gen_retries` | Compteur de tentatives restantes (défaut: 2) |
| `profile` | Profil statistique des colonnes |
| `profile_complete` | True si le profil couvre toutes les colonnes utilisées |
| `profile_result` | Résultat JSON uploadé par l'utilisateur |
| `test_index` | Index du test à modifier (0-based, optionnel) |
| `user_tables` | Données de test custom fournies par l'utilisateur |
| `evaluation_feedback` | Cause d'un verdict Insuffisant : `"bad_data"` ou `"bad_assertions"` (sinon `null`) |

---

## 8. Validation SQL (`validator.py`)

Fichier : [validator.py](../back/build_query/validator.py)

### Phase 1 — Compilation (dry-run)
- **BigQuery** : dry-run via `QueryJobConfig(dry_run=True)` → vérifie la syntaxe et l'existence des tables
- **Postgres** : `EXPLAIN` sur un dataset de test
- **DuckDB** : `EXPLAIN` local

### Phase 2 — Optimisation et extraction
1. Parse la requête avec `sqlglot`
2. `qualify_tables` + `qualify_columns` : résolution complète des références (project.dataset.table)
3. `get_source_columns` : extraction de toutes les colonnes utilisées par table source
4. `split_query` : décomposition en CTEs + `final_query`

### Sortie
```json
{
  "status": "success",
  "used_columns": [...],
  "used_columns_changed": true,
  "optimized_sql": "SELECT ...",
  "query_decomposed": "[{\"name\": \"cte1\", \"code\": \"...\"}]"
}
```

---

## 9. Résumé des API endpoints impliqués

| Endpoint | Méthode | Rôle |
|---|---|---|
| `/models` | POST | Crée un modèle (session) |
| `/query/validate` | POST | Valide + optimise la requête SQL |
| `/query/check-profile` | POST | Vérifie la complétude du profil |
| `/query/skip-profiling` | POST | Marque la session comme `profile_skipped` |
| `/query/auto-profile` | POST | Exécute la requête de profiling automatiquement |
| `/query/import-missing-tables` | POST | Importe les tables manquantes dans le catalogue |
| `/query/stream` | POST (SSE) | Lance le graph LangGraph en streaming |
