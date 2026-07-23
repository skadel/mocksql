# Spec — Injection de fixtures à la frontière d'une CTE (« boundary mocking »)

> Statut : **proposé** — 2026-07-21.
> Origine : étude concurrentielle (syntaxe `given: input: ::cte_name` — mocker une CTE
> amont au lieu des tables de base). Étude de faisabilité menée le 2026-07-20 :
> mécanisme validé empiriquement (cf. §3.1).

## 1. Constat et motivation

### 1.1 Tester une CTE profonde exige aujourd'hui de survivre à toute la chaîne amont

MockSQL injecte toujours au niveau des **tables sources**. Pour tester la logique
d'une CTE de rang N, le générateur doit produire des lignes qui traversent les N−1
étages amont (filtres, jointures, agrégats) sans s'éteindre. C'est la cause racine
des `empty_results` historiques, et la raison d'être d'un stack de mitigation
coûteux : génération focalisée (`cte_graph.isolate_cte`), boucle de correction
`bad_data` → `conversational_agent`, mémoire des leçons, breakdown par étape de la
trace CTE. Ce stack **atténue** le problème ; l'injection à la frontière le
**supprime** pour la logique profonde : les N−1 étages amont sortent du périmètre
du test.

### 1.2 La plomberie existe déjà — il ne manque que le mode d'injection

- `build_query/cte_graph.py` : DAG complet des CTEs (dépendances, graphe « requis »,
  closure transitive, tri topo).
- `utils/examples.py` : tables physiques DuckDB suffixées + réécriture du SQL
  (`strip_qualifiers_with_scope`) + insertion des lignes (`insert_examples.py`).
- Trace CTE (`_run_cte_trace`) et outil `run_cte` : inspection par étage.
- Contrat `expect` (spec validation-humaine) : côté **sortie**, orthogonal — les
  deux composent (un test frontière porte aussi son `expect`).

---

## 2. Principes

| # | Principe |
|---|---|
| P1 | Une CTE mockée est **une table comme une autre** : même chemin de création/insertion/suffixage/replay que les tables de base — pas de pipeline parallèle. |
| P2 | Le schéma de la frontière est **dérivé déterministiquement** (qualify + `annotate_types` sur `schema_cache`) — jamais inféré depuis les lignes synthétiques. Persisté dans la définition, **recalculé au replay** → détection de dérive à la frontière. |
| P3 | Syntaxe de surface `::cte_name` ; en JSON un champ **explicite** (`injection`), jamais une clé magique dans `data`. |
| P4 | Un test frontière **ne remplace pas** le test end-to-end : positionnement « tests par étage », au moins un test complet par modèle. |
| P5 | Replay CLI/CI : **déterministe, zéro LLM** (inchangé). |

---

## 3. Mécanisme d'exécution (cœur, Phase 1)

### 3.1 Réécriture — validée empiriquement

DuckDB **ne bind pas les CTEs non référencées** : `WITH a AS (SELECT * FROM
missing_table), b AS (SELECT 2 AS x), c AS (SELECT x FROM b) SELECT * FROM c`
s'exécute sans erreur (vérifié le 2026-07-20, DuckDB du venv projet). Donc :

1. Remplacer le corps de la CTE frontière par `SELECT * FROM mock.customer_agg`
   (sentinelle **qualifiée**, cf. 3.2). Le nom de CTE et toutes ses références aval
   restent intacts.
2. **Laisser l'amont en place** : les CTEs amont ne sont plus référencées → jamais
   bindées → leurs tables de base peuvent être absentes. Aucun élagage nécessaire.

### 3.2 Intégration au pipeline existant — la sentinelle qualifiée

`strip_qualifiers_with_scope` ne suffixe que les tables **qualifiées**
(`if table.db` — `utils/examples.py:2386`). En injectant `mock.customer_agg`
(qualifié), le pass standard la renomme en `mock_customer_agg_{suffix}` comme
n'importe quelle table de base. Conséquences en cascade, toutes gratuites :

- la fixture est déclarée comme entrée de table synthétique
  `{"table_name": "mock.customer_agg", "columns": <schéma dérivé §4>}` →
  `create_test_tables` (flatten `parts[-2:]` + suffixe) et `insert_examples`
  fonctionnent **inchangés** ;
- clé dans `data` : `mock_customer_agg` — même convention que les autres tables ;
- replay : `precompiled_sql` + substitution de suffixe par cas et
  `_remap_assertion_sql` fonctionnent **inchangés**.

`mock` devient un préfixe de qualificateur **réservé** (garde : refuser un projet
dont une vraie source s'appelle `mock.*`).

### 3.3 Cas limite : frontière mixte

Si une CTE amont de la frontière est **aussi** référencée en aval sans passer par
elle (ex. `final` joint `customer_agg` ET `raw_orders`), l'amont reste bindé et
exige ses sources. Détection via `build_cte_dependency_graph` : une CTE de
`deps*(frontière)` atteignable depuis `final_query` sans traverser la frontière →
**v1 refuse** cette frontière avec un message explicite (proposer les frontières
valides). Le support mixte (fixture + données sources dans le même test) est
possible plus tard, pas en v1.

Hors scope v1, refus explicite : CTEs récursives, `WITH` imbriqué portant le même
nom que la frontière.

---

## 4. Schéma de la frontière — le nœud de design

Une CTE n'a pas d'entrée dans `schema_cache.json`, et le principe replay est
« vrai schéma, zéro inférence ». Résolution :

- **Dérivation** : qualify (schémas du `schema_cache`) + `sqlglot annotate_types`
  sur la requête → colonnes + types de sortie de la CTE. Ce n'est pas de
  l'inférence depuis les lignes : c'est le schéma que l'entrepôt calculerait.
- **Persistance** : dans la **définition commitée** du test
  (`injection.schema`) — le replay ne dépend d'aucun état local.
- **Replay** : re-dériver depuis le SQL courant et **comparer** au schéma persisté.
  Mismatch = l'amont a changé le contrat de la frontière → cas `stale` (dérive
  plus fine que `source_sha` : localisée à la CTE).
- **Fail-fast** : type non résolu par `annotate_types` (dégradation silencieuse
  sqlglot connue) → erreur explicite à la création du test, pas de fallback
  d'inférence.
- `cli/test_runner._resolve_model_schemas` : les tables `mock_*` prennent leur
  schéma depuis `injection.schema`, jamais depuis le cache warehouse → pas de
  `SchemaMissingError` pour une frontière.

---

## 5. Stockage

Sur le cas de test (définition commitée, cf. split `storage/test_files.py`) :

```json
{
  "test_index": 2,
  "injection": {
    "cte": "customer_agg",
    "schema": [{"name": "customer_id", "type": "INT64"}, ...]
  },
  "data": {"mock_customer_agg": [{...}, {...}]},
  ...
}
```

- `injection` **entre dans `compute_fingerprint`** (une frontière différente = une
  identité de test différente).
- Contrat `expect` : inchangé, il porte sur la sortie finale.
- Syntaxe de surface (docs, futur contrat YAML/export) : `given: input: ::customer_agg`.

---

## 6. Impacts par chemin

| Chemin | Impact | Phase |
|---|---|---|
| `_run_cte_trace` | Skip les CTEs de `deps*(frontière)` (elles bindent des tables absentes). La frontière elle-même trace le row_count des fixtures. | 1 |
| `run_cte` (agent) | Garde : cible amont d'une frontière → message « hors périmètre de ce test (frontière ::X) ». | 2 |
| `export_dbt` | Les unit tests dbt natifs ne mockent pas les CTEs → **skip + warning explicite** pour les tests frontière. | 1 |
| `_resolve_model_schemas` | Routage `mock_*` → `injection.schema` (cf. §4). | 1 |
| `evaluate_tests` / `coherence_check` | Ph.1 : transparent (une table comme une autre). Ph.2 : labelliser le bloc `input_data` (« fixtures au niveau de la CTE X, l'amont est hors périmètre »). | 2 |
| Generator | Vue **aval** : la frontière traitée comme table feuille → `used_columns`/contraintes extraits sur le SQL réécrit (symétrique d'`isolate_cte` qui fait l'amont). | 2 |
| UI (TestCard, TestsPanel) | Chip `::customer_agg` sur le test ; sélecteur de frontière (analogue au focus `target_path`). | 2 |

---

## 7. Phasage

### Phase 1 — mécanisme + replay + promotion (zéro LLM)

Livrables :
1. Nouveau module **pur** `build_query/cte_injection.py` (même posture que
   `cte_graph.py` : zéro state, zéro réseau) :
   - `rewrite_sql_for_injection(sql, cte_name, dialect) -> str` (§3.1–3.2) ;
   - `derive_boundary_schema(sql, cte_name, schemas, dialect) -> list[dict]` (§4) ;
   - `validate_boundary(query_decomposed, cte_name) -> None | raison de refus` (§3.3).
2. Stockage : champ `injection` (définition), fingerprint, routage schémas au replay.
3. **Outil de promotion** `mocksql promote-boundary <model> --test <uid> --cte <X>` :
   capture les lignes réelles de la trace CTE d'un test existant qui passe → crée
   le test frontière équivalent. C'est le véhicule de validation : zéro LLM, corpus
   existant réutilisé.

Tests (avant le code, méthode par défaut) : `tests/test_cte_injection.py`
(réécriture — corps remplacé, aval intact, sentinelle qualifiée, pas de
double-suffixe ; amont non bindé sur DuckDB réel ; frontière mixte → refus ;
schéma dérivé — types corrects, type inconnu → erreur ; casse des colonnes) +
`tests/test_replay_injection.py` (replay vert d'un test frontière ; schéma dérivé
divergent → `stale` ; `SchemaMissingError` jamais levée pour `mock_*` ; skip
export dbt).

**Gate de sortie** : promouvoir en tests frontière une dizaine de modèles
spider2-snow à chaîne profonde (ceux des `empty_results` historiques) → replay
100 % vert.

### Phase 2 — génération LLM à la frontière

1. State : `injection_boundary: Optional[str]` dans `QueryState` ; outil agent
   `set_injection_boundary` (analogue `set_target_path`).
2. Prompt generator : bloc frontière (colonnes + types dérivés + contraintes aval
   uniquement) — le prompt montre le SQL complet mais la cible de génération est la
   frontière (même philosophie que le focus par branche).
3. Déclenchement : (a) choix explicite utilisateur (UI/CLI) ; (b) **fallback ultime
   de la boucle `bad_data`** — quand les retries s'épuisent sur une CTE bloquante
   profonde, proposer (pas imposer) la bascule frontière avant `bad_data_exhausted`.
4. UI : sélecteur + chip (cf. §6).

### Phase 3 — adjacents (hors scope, notés pour mémoire)

- Profiling de la frontière sur l'entrepôt (exécuter la sous-requête amont dans une
  requête de profil, budget To existant) → fixtures réalistes.
- Assertions/`expect` **à la frontière** (vérifier la sortie d'une CTE
  intermédiaire, façon `outputs.ctes` de SQLMesh).
- Axe de suggestion « couverture par étage ».

---

## 8. Risques et parades

| Risque | Parade |
|---|---|
| Fixtures « impossibles » (lignes que l'amont ne produirait jamais) → fausse confiance | Le juge note la cohérence narratif↔données↔SQL (philosophie existante) ; P4 : au moins un test end-to-end par modèle ; Ph.3 profiling de frontière. |
| Dérive sémantique amont **sans** changement de schéma → invisible au test frontière | Assumé : c'est le rôle du test end-to-end (P4). Le recalcul de schéma attrape la dérive structurelle ; le `source_sha` attrape le reste. |
| Collision avec une vraie source `mock.*` | Qualificateur réservé, garde à la création (§3.2). |
| `annotate_types` incomplet sur des expressions exotiques | Fail-fast + message (« ajouter la frontière plus haut ou tester end-to-end ») — jamais de schéma silencieusement faux. |

---

## 9. Questions ouvertes

1. Le fallback `bad_data` → bascule frontière (Ph.2, §7.3b) doit-il être
   automatique ou proposé à l'utilisateur ? Reco : **proposé** (cohérent avec la
   spec validation-humaine : l'humain est l'oracle du périmètre du test).
2. Faut-il exposer la frontière dans le contrat YAML/`given` dès la Ph.1 (doc
   seulement) ou attendre la convergence avec l'export dbt ? Reco : documenter la
   syntaxe `::` dès Ph.1, implémentation surface en Ph.2.
