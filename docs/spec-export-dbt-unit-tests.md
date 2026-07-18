# Spec — Export des tests MockSQL en unit tests dbt natifs

**Statut** : proposé · **Cible** : v1 CLI (`mocksql export dbt`) · **Date** : 2026-07-17

---

## 1. Objectif & positionnement

Compiler les tests MockSQL d'un modèle dbt en blocs `unit_tests:` YAML natifs (dbt ≥ 1.8,
Core **et** Fusion), écrits dans le projet dbt de l'utilisateur. L'output vit dans son repo,
tourne en CI via `dbt test --select test_type:unit`, **sans MockSQL dans la boucle CI**.

Pitch : la douleur n°1 des unit tests dbt est d'écrire les fixtures YAML à la main —
MockSQL les génère, les évalue (verdict), puis les exporte en format natif. Zéro lock-in.

Les deux runners coexistent :

| Runner | Où | Coût | Rôle |
|---|---|---|---|
| `mocksql test` | DuckDB local (SQL transpilé sqlglot) | 0 € | boucle rapide dev, pre-commit |
| `dbt test --select test_type:unit` | warehouse (adapter dbt) | fixtures minimes | CI canonique, dialecte prod |

## 2. Décisions structurantes

1. **Le format interne reste la source de vérité.** `.mocksql/tests/{model}.json` porte la
   définition riche (assertions prédicatives, scope, verdict, suggestions). L'export est une
   **compilation descendante** ; on ne ré-importe jamais depuis le YAML dbt (pas de sync bidirectionnelle).
2. **Assertions → lignes figées.** dbt `expect` n'accepte que des lignes exactes. Les
   assertions se compilent en gelant le **résultat du replay DuckDB** (`expect.rows`).
   Le verdict LLM (`Bon`/`Excellent`) est le gate qui rend ce gel non-circulaire :
   il atteste que la sortie figée reflète l'intention décrite.
3. **Replay avant gel, jamais `results_json`.** Les lignes attendues sont recalculées à
   l'export via le moteur de replay existant (`cli/test_runner.py`, zéro LLM,
   `schema_cache.json` obligatoire). `results_json` vit dans le cache gitignoré : absent
   sur un clone, potentiellement périmé. Un replay qui échoue (assertion fail, erreur)
   **exclut le cas de l'export** avec raison affichée.
4. **Un fichier YAML par modèle, possédé par MockSQL.** Écrit à côté du modèle source
   (`{dir du .sql}/{model}.mocksql.yml`), en-tête « généré par MockSQL — ne pas éditer,
   régénéré par `mocksql export dbt` ». Jamais de merge dans un `schema.yml` existant
   (trop risqué : commentaires, ordre, ancres YAML). Les unit tests doivent vivre sous
   `model-paths` (contrainte dbt) — c'est le cas.
5. **Le SQL du modèle n'est jamais touché.** L'export n'émet que du YAML (fixtures =
   données, dialecte-neutre). C'est dbt qui exécute le SQL du repo sur le warehouse.

## 3. Config & CLI

Réutilise le bloc `dbt:` existant de `mocksql.yml` (cf. `storage/config.py:get_dbt_project`) :

```yaml
dbt:
  project_dir: ../warehouse     # requis — racine du projet dbt (manifest.json compilé)
  target_path: target           # défaut
```

```
mocksql export dbt [-t <model> ...] [--all] [--check] [--dry-run]
```

- `-t/--target` répétable ; `--all` = tous les modèles de `.mocksql/tests/` qui résolvent
  vers un nœud dbt.
- `--check` : mode CI — rend le YAML en mémoire, compare au fichier sur disque,
  **exit 1 si diff** (détection de dérive tests MockSQL ↔ YAML exporté). N'écrit rien.
- `--dry-run` : affiche le YAML sur stdout sans écrire.
- Sortie : résumé par modèle — N exportés / M exclus (avec raison par cas), chemin écrit.
- Codes retour : 0 OK ; 1 = drift (`--check`), erreur de résolution, ou 0 cas exportable.

Nouveau module : `back/cli/export_dbt.py`, enregistré dans `cli/main.py`.

## 4. Pipeline d'export (par modèle)

```
lire .mocksql/tests/{model}.json (read_test_doc)
  → résoudre le nœud dbt (DbtProject.find_node) — sinon : erreur « pas un modèle dbt »
  → construire le mapping relation physique → ref()/source()   (§5)
  → filtrer les cas exportables                                 (§6)
  → replay DuckDB des cas retenus (moteur mocksql test)
      → assertions pass → geler result_df en expect.rows
      → fail/erreur    → exclu, raison « replay non-vert »
  → émettre le YAML (rendu déterministe : tri par test_uid, clés ordonnées)
  → écrire / comparer (--check) / afficher (--dry-run)
```

### Mapping relation → ref()/source()

Le manifest fournit `nodes[*].depends_on.nodes` du modèle et, pour chaque parent
(modèle ou source), `relation_name` (ex. `` `proj`.`dataset`.`table` ``). On construit
un index **insensible à la casse, quotes/backticks strippés** :

```
"proj.dataset.stg_customers"  → "ref('stg_customers')"
"proj.dataset.raw_events"     → "source('raw', 'events')"
```

Les clés de `test_case.data` (noms physiques) sont résolues via cet index — même logique
de suffixe-matching que `_schemas_from_cache` (full → `dataset.table` → `table`).
Clé non résolue → cas exclu, raison « table hors DAG dbt » (piège classique :
`used_columns` extrait une CTE ou une table en dur absente du manifest).

Ajout à `storage/dbt_manifest.py` : `DbtProject.parent_relations(node) -> dict[str, str]`
(relation physique normalisée → jinja `ref(...)`/`source(...)`).

## 5. Mapping format interne → YAML dbt

| Interne (`test_case`) | dbt `unit_tests` | Note |
|---|---|---|
| `test_name` + `test_uid` | `name: mocksql__{slug(test_name)}__{uid8}` | slug ascii ; `uid8` = 8 premiers hex du `test_uid` → stabilité au renommage, unicité par modèle |
| `unit_test_description` | `description` | tel quel |
| — | `model: {node.name}` | nom du nœud dbt |
| `data[table][rows]` | `given[].input` + `format: dict` + `rows` | fixtures **partielles** autorisées par dbt → on émet exactement les colonnes générées (alignées `used_columns`) |
| parent du DAG sans données dans le cas | `given[].input` + `rows: []` | dbt exige **tous** les `ref`/`source` du modèle dans `given` (« node not found » sinon) — les parents non peuplés reçoivent une fixture vide |
| replay `result_df` | `expect.format: dict` + `rows` | toutes les colonnes du résultat ; lignes vides `rows: []` = test « plage vide » exportable tel quel |
| `assertion_results` | **rien** (compilées dans `expect`) | conservées côté MockSQL uniquement |
| `verdict`, `verdict_text`, `tags` | **rien** | qualité = concept génération-time, pas runtime |
| — | `config: {tags: ["mocksql"]}` | permet `dbt test --select tag:mocksql` et l'exclusion en prod |

### Sérialisation des valeurs (dict YAML)

- dates/timestamps → chaînes ISO ; `Decimal`/numériques → littéraux YAML ; `None` → `null` ;
  booléens natifs.
- Types non représentables en YAML dict (BYTES, VARIANT/JSON imbriqué, GEOGRAPHY, STRUCT
  partiel BigQuery) → cas **exclu** en v1, raison explicite (v1.1 : fallback `format: sql`
  avec littéraux typés depuis `schema_cache`).

## 6. Filtres d'exportabilité

Un cas est exporté ssi **tous** les gates passent. Chaque exclusion est listée dans le
résumé avec sa raison (jamais silencieuse).

**Gates niveau cas** :
- `verdict ∈ {Excellent, Bon}` — un test `Insuffisant`/sans verdict ne doit jamais gater la CI de l'utilisateur ;
- pas mort-né (`is_deadborn_case`), pas `needs_validation` en attente ;
- `data` et `assertion_results` non vides (mêmes règles que le replay) ;
- replay vert (§4) ;
- toutes les tables de `data` résolues en `ref`/`source` (§4) ;
- valeurs sérialisables (§5).

**Gates niveau modèle** (contraintes doc dbt) :
- modèle SQL, matérialisation ∉ {`materialized_view`} ; pas de SQL récursif ni de requêtes
  introspectives (détection sqlglot best-effort : `WITH RECURSIVE`, tables `INFORMATION_SCHEMA`) ;
- matérialisation `incremental` → **hors v1** (l'`expect` dbt = delta mergé, sémantique
  différente de nos tests « table finale ») — modèle listé « exclu : incremental » ;
- parent éphémère → hors v1 (dbt exige `format: sql` pour ces inputs) ;
- joins non aliasés → **warning** (dbt l'exige pour tester la logique de join) sans bloquer.

**Warning non bloquant** : SQL potentiellement non-déterministe (`LIMIT`/`QUALIFY
ROW_NUMBER` sans tri total) → « l'expect figé peut différer sur le warehouse ».

## 7. Idempotence, drift & CI

- Rendu **déterministe** (tri par `test_uid`, ordre de clés fixe, largeur YAML fixe) →
  re-export sans changement = fichier identique, diff git minimal sinon.
- Le fichier exporté est **commité par l'utilisateur** dans son repo dbt (c'est le but).
- Boucle recommandée (documentée dans le README) :
  1. dev : `mocksql generate` / itération UI → tests verts ;
  2. `mocksql export dbt -t <model>` → commit du `.mocksql.yml` ;
  3. CI dbt : `dbt run --select <parents> --empty` puis `dbt test --select tag:mocksql` ;
  4. CI MockSQL (optionnel) : `mocksql export dbt --check` → échoue si les tests MockSQL
     et le YAML exporté ont divergé.
- Suppression d'un test MockSQL → disparaît du YAML au prochain export (le fichier est
  entièrement régénéré, jamais patché).

## 8. Hors périmètre v1 (assumé)

- Modèles incrémentaux (`overrides: is_incremental` + expect-delta) — v2.
- `format: sql` / fixtures en fichiers (`tests/fixtures/`) — v1.1 si les exclusions de
  types s'avèrent fréquentes.
- Sync inverse (YAML dbt → tests MockSQL) — jamais (décision 2.1).
- `overrides` de vars/macros/env — v2 (nécessite de tracer les vars vues à la compile).
- Export depuis l'UI (bouton TestsView) — après validation du flux CLI.

## 9. Plan de tests (`back/tests/test_export_dbt.py`)

Test avant fix, comme d'habitude — fixtures manifest minimales (2 modèles, 1 source), sans dbt installé :

1. **Mapping nominal** : doc 2 cas verts → YAML avec `given` (ref + source), `expect`
   gelé du replay, noms `mocksql__*__uid8`, parents non peuplés en `rows: []`.
2. **Gates** : cas `Insuffisant`, mort-né, replay rouge, table hors DAG, valeur BYTES →
   exclus chacun avec la bonne raison ; modèle incremental → exclu niveau modèle.
3. **Vide intentionnel** : cas PASS `empty_results` + verdict Bon → `expect.rows: []`.
4. **Idempotence** : double export → octets identiques ; `--check` vert puis rouge après
   mutation du doc.
5. **Résolution relations** : casse mixte + backticks strippés + suffixe-matching.
6. **Déterminisme YAML** : ordre des cas stable quel que soit l'ordre du JSON source.
