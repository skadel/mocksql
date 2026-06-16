# Quickstart dbt

MockSQL teste des fichiers `.sql` **plats et parsables**. Un projet [dbt](https://www.getdbt.com/) ne l'est pas directement : les modèles contiennent du Jinja (`{{ ref(...) }}`, `{{ config(...) }}`, `{% if is_incremental() %}`, macros `dbt_utils`…) que SQLGlot ne sait pas analyser.

Le **connecteur dbt** de MockSQL fait le pont. Il a deux rôles, et deux seulement :

1. **Compile** — il lit le SQL **compilé** par dbt (`target/compiled/**/*.sql`), où tout le Jinja est déjà rendu : `ref()`/`source()`/`var()`/`this`/macros → SQL plat avec les **vrais noms de tables**. Ça remplace tout préprocesseur regex.
2. **Résolution** — il retrouve le modèle dbt à partir de son chemin et fournit ce SQL compilé à MockSQL.

La **récupération du schéma reste le job normal de MockSQL** : une fois le SQL compilé fourni, le flux `generate` extrait les tables référencées et importe leur schéma comme pour n'importe quelle requête (BigQuery aujourd'hui ; autres warehouses à venir). L'exécution des tests reste sur une base **DuckDB scratch** — **0 € facturé**.

```
projet dbt
  │  dbt compile                       (Jinja → SQL plat, refs = vrais noms warehouse)
  ▼
target/compiled/**/*.sql  +  manifest.json
  │  connecteur dbt (bloc `dbt:` dans mocksql.yml)
  ▼
mocksql generate ──► import schéma (warehouse) ──► génération LLM ──► exécution DuckDB
                                                                       → .mocksql/tests/<model>.json
```

---

## 1. Déclarer le projet dbt dans `mocksql.yml`

Ajoute un bloc `dbt:` à la config MockSQL. C'est ce qui **active le connecteur** :

```yaml
version: "2"
dialect: bigquery                 # bigquery pour un projet dbt-BigQuery ; duckdb pour dbt-duckdb
models_path: ./models             # le dossier models/ du projet dbt
dbt:
  project_dir: .                  # dossier contenant dbt_project.yml (relatif à ce mocksql.yml)
  target_path: target             # optionnel (défaut : target)
llm:
  provider: vertexai
```

Quand `dbt:` est présent, pour tout modèle reconnu comme modèle dbt, MockSQL lit le **SQL compilé** au lieu du fichier `.sql` brut. Un éventuel `preprocessor_fn` devient inutile (le compile fait déjà le travail).

> **Dialect** : `bigquery` pour un projet dbt-BigQuery (le SQL compilé garde les idiomes BQ, MockSQL les transpile vers DuckDB à l'exécution). `duckdb` pour un projet nativement dbt-duckdb.

---

## 2. Compiler le projet dbt

Dans un environnement avec l'adaptateur dbt de ton warehouse (ex. `dbt-bigquery`) :

```bash
cd mon_projet_dbt           # IMPORTANT : se placer DANS le dossier du projet
dbt deps                    # si le projet a des packages (dbt_utils, dbt_date…)
dbt compile                 # Jinja → target/compiled/**/*.sql
```

`dbt compile` n'exécute rien sur le warehouse — il rend juste le Jinja. Le résultat est dans `target/compiled/<projet>/models/**/*.sql`, avec les `ref()`/`source()` résolus en **vrais noms de tables**.

> **Piège `relation_name`** : le nom des tables dans le SQL compilé dépend du **profil de compile**. Compile avec le **target réel de ton warehouse** (ton `profiles.yml` habituel) pour que les refs soient les vrais noms warehouse — sinon l'import MockSQL ne les retrouvera pas.

---

## 3. Matérialiser les modèles parents (pour tester un mart)

C'est le point clé pour les **marts** et **intermediates** : un mart référence d'**autres modèles** (`{{ ref('products') }}`), pas des tables brutes. Pour générer des données cohérentes, MockSQL importe le schéma de ces parents — **ils doivent donc exister dans le warehouse**.

- Modèles **staging** : leurs refs sont des **sources réelles** → déjà présentes, rien à faire.
- Modèles **mart / intermediate** : leurs parents sont des modèles dérivés → il faut les matérialiser :

```bash
dbt run --select +mon_mart     # construit le mart ET tous ses ancêtres
```

Une fois `dbt run` passé, les tables parentes existent et `mocksql generate` peut importer leur schéma.

> Si tu sautes cette étape sur un mart, `mocksql generate` échouera à l'import avec « table not found » sur un modèle parent.

---

## 4. Générer les tests

```bash
DUCKDB_PATH=mon_projet_dbt/.mocksql/scratch.duckdb \
mocksql generate mon_projet_dbt/models/marts/core/sales.sql \
  --config mon_projet_dbt/mocksql.yml \
  --output mon_projet_dbt/.mocksql/tests
```

Déroulé :
1. `[dbt] SQL compilé depuis le manifest` — le connecteur fournit le SQL plat (zéro Jinja).
2. `Fetching schema for: …` — MockSQL importe les schémas des tables référencées depuis le warehouse.
3. Génération LLM des données synthétiques, exécution sur DuckDB scratch, verdict.
4. Écriture de `.mocksql/tests/<model>.json` (données d'entrée, résultats DuckDB, assertions, verdict).

> `DUCKDB_PATH` est la base **scratch** où MockSQL crée les tables synthétiques — distincte de toute base dbt.

### Credentials

Les credentials warehouse + LLM sont lus depuis `back/.env` (via `load_dotenv()`) :

```
GOOGLE_APPLICATION_CREDENTIALS=C:\chemin\absolu\service-account.json
VERTEX_PROJECT=mon-projet-gcp
```

---

## 5. (Optionnel) Évaluer la qualité sur tout le projet

Le skill `/eval-mocksql` génère les tests de tous les modèles puis les note via un juge LLM :

```
/eval-mocksql mon_projet_dbt
```

Rapport : score `données` / `test` + validité par modèle, et taux global.

---

## Récapitulatif workflow

```bash
# une fois
cd mon_projet_dbt && dbt deps

# à chaque changement de modèle / schéma
dbt compile                          # met à jour le SQL compilé
dbt run --select +mon_mart           # (marts uniquement) matérialise les parents
mocksql generate models/.../mon_mart.sql --config mocksql.yml --output .mocksql/tests
```

---

## Pièges & limites connus

- **`relation_name` = profil de compile** : compiler avec le vrai target warehouse, sinon refs incohérentes (cf. §2).
- **Marts → parents matérialisés** : un mart n'est testable que si ses modèles amont existent en base (cf. §3).
- **Un projet mocksql par projet dbt** : chaque projet a son `dbt_project.yml`/`profiles.yml` et son `mocksql.yml`.
- **Logique date-relative** (`CURRENT_DATE`, fenêtres glissantes) : la génération peut produire des données hors plage → résultat vide. Signal qualité légitime, pas un bug de setup.
- **Macros à effet d'exécution** (`{% if is_incremental() %}`) : `dbt compile` élide la branche incrémentale (fausse au compile) → le test porte sur le chemin non-incrémental.

---

## Limitation : warehouses autres que BigQuery

Aujourd'hui, l'import de schéma de MockSQL ne sait interroger que **BigQuery**. Pour un projet **dbt-duckdb** ou **sans accès warehouse**, il n'y a donc pas encore de chemin d'import automatique.

En attendant les **connecteurs warehouse** (Snowflake, Databricks, DuckDB… — sur la roadmap), un contournement existe : pré-remplir manuellement `.mocksql/schema_cache.json` (clé `schema_cache` du `mocksql.yml`) par introspection de la base matérialisée par `dbt run`, en `dialect: duckdb`. C'est un palliatif, pas la méthode cible — le détail du script de bootstrap est dans l'historique git de ce fichier.
