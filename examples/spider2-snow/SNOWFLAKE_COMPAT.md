# MockSQL × Spider2-snow — rapport de compatibilité Snowflake

Test de la **compatibilité Snowflake native** de MockSQL (`dialect: snowflake` de
bout en bout) sur les requêtes gold de
[Spider2-snow](https://github.com/xlang-ai/Spider2/tree/main/spider2-snow).
Objectif : exercer le pipeline tel quel et repérer les points de casse — **pas**
contourner en transpilant le projet vers BigQuery.

## Setup

- Projet : `examples/spider2-snow/` — `dialect: snowflake`, modèles = requêtes gold
  `sf_bq*` copiées telles quelles dans `models/`.
- Schéma : `build_schema_cache.py` reconstruit `.mocksql/schema_cache.json` depuis
  les `resource/databases/*/*/DDL.csv` de Spider2, en gardant les **types Snowflake
  natifs** (`TEXT`/`NUMBER`/`FLOAT`/`VARIANT`…) sans `bq_ddl_type` — exactement le
  format que produirait `fetch_tables_schema_snowflake` (import live INFORMATION_SCHEMA).
- Pas de credentials Snowflake → l'import live n'est pas testable hors-ligne ; le
  cache offline est l'équivalent fidèle (cf. limite #1, qui touche aussi l'import live).

## Scorecard (échantillon de 5 modèles)

Avant → après les correctifs (cf. section « Correctifs appliqués ») :

| Modèle    | Avant            | Après            | Reste |
|-----------|------------------|------------------|-------|
| sf_bq037  | ✅ pass          | ✅ **Excellent** | — |
| sf_bq052  | ✅ pass          | ✅ **Excellent** | — |
| sf_bq012  | 💥 crash dur     | ✅ **Excellent** | — (backtick + NUMBER résolus) |
| sf_bq083  | ❌ error         | ⚠️ dégradé propre | `CAST('0x…' AS DOUBLE)` (hex Snowflake ≠ DuckDB) |
| sf_bq068  | ❌ error         | ⚠️ dégradé propre | `LATERAL FLATTEN`/`PARSE_JSON` (transpilation lourde) |

**3/5 passent désormais avec verdict « Excellent »** (vs 2/5 + 1 crash avant). Les 2
restants ne crashent plus : ils écrivent un test partiel et remontent un diagnostic au
lieu d'aborter le graphe.

**Ce qui marche bien** : `dialect: snowflake` accepté ; parsing snowflake + extraction
des tables 3-parties (`DB.SCHEMA.TABLE`) ; **génération de données LLM** de qualité en
contexte snowflake (données réalistes, descriptions cohérentes) ; consommation du
schema_cache ; **import live** (cf. correctifs). Le cœur produit fonctionne.

## Correctifs appliqués

| # | Finding | Correctif | Test |
|---|---------|-----------|------|
| A | Import live vide (DictCursor MAJUSCULES) | `_sf_get` lit les clés insensiblement à la casse (`schema_fetcher.py`) | `test_sf_get_uppercase_dictcursor_keys` |
| B | INFORMATION_SCHEMA non qualifié | `FROM "<db>".INFORMATION_SCHEMA.COLUMNS` par ref | (vérifié live sur TPCH) |
| C | Casse forcée en minuscules | casse Snowflake préservée (plus de `.lower()`) | (vérifié live) |
| #1 | `NUMBER` → `DECIMAL(18,3)` déborde | `_sf_snow_data_type` reconstruit `NUMBER(p,s)` + `_widen_bare_decimals` → `DECIMAL(38,9)` à la création de table | `test_bare_number_column_holds_large_integer` |
| 2a | `TO_TIMESTAMP_NTZ/LTZ/TZ` non transpilé | AST `_fix_snowflake_to_timestamp` type-aware (epoch→`to_timestamp`, sinon→CAST) | `test_to_timestamp_ntz_*` |
| 2b | `TO_CHAR(x, fmt)` perd le format | AST `_fix_snowflake_to_char` → `strftime` (tokens date), formats numériques laissés en CAST | `test_to_char_*` |
| 6 | `debug_node` re-parse en backticks BigQuery → crash | `_quote_ident` quote selon le dialecte + `run_cte` ne crash plus (renvoie un diagnostic) | `test_quote_ident_dialect_aware`, `test_snowflake_debug_sql_parses` |
| role | `get_sf_connection` n'envoie pas `role` | `SNOWFLAKE_ROLE` optionnel (env) passé à `connect()` | — |

**Limites restantes (non corrigées — gaps réels, hors scope « faire marcher »)** :
- `LATERAL FLATTEN(input => PARSE_JSON(...))` : sqlglot produit un `CROSS JOIN UNNEST`
  invalide ; transpilation lourde (sémantique FLATTEN→UNNEST de JSON).
- `CAST('0x…' AS DOUBLE)` : Snowflake interprète le préfixe hexadécimal, pas DuckDB.

## Findings (par impact)

### 1. `NUMBER` sans précision → `DECIMAL(18,3)` → débordement  ⭐ — ✅ RÉSOLU
`fetch_tables_schema_snowflake` (et donc le cache) ne lit que `DATA_TYPE`, pas
`NUMERIC_PRECISION`/`NUMERIC_SCALE`. Résultat : `NUMBER` → DuckDB `DECIMAL` → défaut
`DECIMAL(18,3)`, qui **déborde** sur tout grand entier (timestamps µs, valeurs wei) :
`Could not convert string "1686787200000000" to DECIMAL(18,3)`. Touche 3/5 modèles.
- Vérifié : `_resolve_duck_type("NUMBER")` → `DECIMAL` ; `_resolve_duck_type("NUMBER(38,0)")` → `DECIMAL(38,0)` (OK).
- **Fix** : ajouter `NUMERIC_PRECISION`/`NUMERIC_SCALE` au SELECT de
  `schema_fetcher.py:fetch_tables_schema_snowflake` et reconstruire `NUMBER(p,s)`.

### 2. Transpilation snowflake→duckdb incomplète (`utils/examples.py`) — ✅ PARTIEL
La branche `source_dialect == "snowflake"` couvre IFF/ZEROIFNULL/LISTAGG/DATEADD…
- ✅ **`TO_TIMESTAMP_NTZ/LTZ/TZ(x)`** → réécrit sur l'AST (`_fix_snowflake_to_timestamp`),
  type-aware : argument numérique (epoch) → `to_timestamp(x)`, sinon → `CAST(x AS TIMESTAMP)`.
  Le regex `TO_TIMESTAMP→CAST` de `fix_duck_db_sql` (qui écrasait le `to_timestamp`
  légitime) a été retiré.
- ✅ **`TO_CHAR(x, '<format date>')`** → `strftime(x, '<format duckdb>')` via
  `_fix_snowflake_to_char` (réécrit sur l'AST snowflake AVANT transpilation, sinon
  sqlglot abandonne le format). Les formats numériques sont laissés en CAST AS TEXT.
- ❌ **`LATERAL FLATTEN(input => PARSE_JSON(...))`** → toujours non transpilé (sqlglot
  produit un `CROSS JOIN UNNEST` invalide). Gap lourd, documenté comme limite.

### 3. Crash dur dans `debug_node` : re-parse en quoting BigQuery — ✅ RÉSOLU
Le chemin de debug (`debug_executor.py:execute_run_cte`) construisait le SQL de debug
avec des **backticks BigQuery** codés en dur → re-parse `read=snowflake` →
`ParseError: Expecting (`. Corrigé par `_quote_ident(name, dialect)` (quoting selon le
dialecte) ; de plus `run_cte` capture désormais les erreurs d'exécution et renvoie un
diagnostic au lieu de crasher le graphe. sf_bq012 passe maintenant (verdict Excellent).

### 4. `qualify` échoue sur colonnes snowflake quotées → fallback raw SQL
`[WARN] SQL qualification failed (Unknown column: from_address / id / addresses)`.
Les identifiants snowflake quotés minuscules ne se résolvent pas → on retombe sur le
SQL brut (perte de précision `used_columns`). Touche 3/5.

### Mineur
- `FLOAT` snowflake (double précision) → DuckDB `REAL` (simple précision) — perte de précision.

## Reproduire

```bash
# 1. (re)construire le cache depuis les DDL.csv Spider2
poetry -C back run python examples/spider2-snow/build_schema_cache.py
# 2. générer + exécuter un modèle
cd examples/spider2-snow
PYTHONIOENCODING=utf-8 VERTEX_PROJECT=pipetalk-493612 \
  DUCKDB_PATH=$(pwd)/.mocksql/mocksql.duckdb \
  poetry -C ../../back run mocksql generate "$(pwd)/models/sf_bq083.sql" \
  --config "$(pwd)/mocksql.yml" --output "$(pwd)/.mocksql/tests" --overwrite
```

---

## Test du chemin d'import LIVE (compte Snowflake réel)

Compte perso de test (`eh17953.europe-west3.gcp`) — les bases Spider2-snow n'y sont
pas (elles sont sur le compte hébergé Spider2 `RSRSBDK-YDB67606`, accès via form).
Test mené sur `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` (vraie base Snowflake). Harness :
`examples/snowflake-live/` (`import_live.py` reproduit le flux d'import serveur, le
CLI `refresh-schemas` étant BigQuery-only). **3 bugs bloquants découverts :**

### A. 🔴 `fetch_tables_schema_snowflake` droppe TOUTES les colonnes (import vide) — ✅ RÉSOLU
Le `DictCursor` Snowflake renvoie les clés en **MAJUSCULES** (`FIELD_PATH`,
`DATA_TYPE`…). Le code lit `row.get("field_path") or row.get("COLUMN_NAME")`
([schema_fetcher.py:449](back/build_query/schema_fetcher.py#L449)) — les deux ratent
(`COLUMN_NAME` est aliasée en `FIELD_PATH`) → `field_path=""` → chaque colonne est
filtrée par `if not name` → **cache avec `columns: []`**. Vérifié live : 33 rows
INFORMATION_SCHEMA fetchées → 0 colonne dans le cache. **L'import Snowflake live est
inutilisable en l'état.** Fix : lire en majuscules / normaliser les clés.

### B. 🔴 INFORMATION_SCHEMA non qualifié → "session does not have a current database" — ✅ RÉSOLU
La requête fait `FROM INFORMATION_SCHEMA.COLUMNS` (non préfixé par la base) →
échoue si la base de session n'est pas définie, et ne peut viser qu'**une seule base**
(celle de la connexion), pas du cross-DB — alors que chaque ref porte son `TABLE_CATALOG`.
Fix : `FROM <database>.INFORMATION_SCHEMA.COLUMNS` par ref (ou `SNOWFLAKE.ACCOUNT_USAGE`).

### C. 🟠 Casse des noms : cache en minuscules vs SQL Snowflake en majuscules — ✅ RÉSOLU
Le fetcher force `.lower()` sur catalog/schema/table → cache
`snowflake_sample_data.tpch_sf1.customer`, alors que les identifiants Snowflake quotés
sont en MAJUSCULES → mismatch à la résolution table↔cache (lié au finding #4 qualify).

### Confirmations live
- **#1 (NUMBER précision)** : `INFORMATION_SCHEMA.COLUMNS` expose bien
  `NUMERIC_PRECISION=38, NUMERIC_SCALE=0` (ex. `C_CUSTKEY`), mais la requête du fetcher
  ne sélectionne que `DATA_TYPE` → précision perdue, confirmé sur données réelles.
- **`get_sf_connection` ne passe pas `role`** — ✅ RÉSOLU : variable d'env optionnelle
  `SNOWFLAKE_ROLE` passée à `connect()` (omise si vide). Débloque le compte hébergé
  Spider2 qui impose `role=PARTICIPANT`.

---

## Résultat après correctifs

Import live re-testé sur `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1` : **33 colonnes importées**
(0 avant), types fidèles (`NUMBER(38,0)`, `NUMBER(12,2)`), casse préservée. Le pipeline
complet (génération → exécution DuckDB → verdict) tourne de bout en bout sur le schéma
importé en live : modèle `examples/snowflake-live/tpch_top_customers` → verdict
**Excellent**, 3/3 assertions, aucun débordement. Suite de tests : `back/tests/test_snowflake_compat.py`
(14 tests) ; suite complète backend verte (1540 tests).

