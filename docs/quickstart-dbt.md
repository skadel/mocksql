# Quickstart dbt

MockSQL teste des fichiers `.sql` **plats et parsables**. Un projet [dbt](https://www.getdbt.com/) ne l'est pas directement : les modèles contiennent du Jinja (`{{ ref(...) }}`, `{{ config(...) }}`, `{% if is_incremental() %}`, macros `dbt_utils`…) que SQLGlot ne sait pas analyser. De plus, MockSQL a besoin du **schéma** des tables amont pour générer des données cohérentes.

La passerelle propre est la sortie de `dbt compile`, qui résout tout le Jinja en SQL pur, combinée à un **cache de schéma** bootstrapé depuis la base DuckDB matérialisée par `dbt run`.

> **Dialect** : les projets dbt-DuckDB se testent en `dialect: duckdb` (voir [Dialects supportés](quickstart.md#dialects-supportés)). Le SQL compilé contient des idiomes DuckDB natifs (`STRFTIME`, `EXTRACT`, etc.) qui s'exécutent tels quels.

---

## Vue d'ensemble

```
projet dbt
  │  1. dbt compile + dbt run   (Jinja → SQL plat, modèles matérialisés)
  ▼
target/compiled/**/*.sql  +  <db>.duckdb
  │  2. bootstrap schema_cache.json depuis le DuckDB
  │  3. mocksql.yml (dialect: duckdb)
  ▼
mocksql generate → .mocksql/tests/<model>.json
```

L'exécuteur de MockSQL tourne sur une base DuckDB **scratch** remplie de données synthétiques — les vraies données dbt ne servent qu'à **bâtir le cache de schéma** (étape 2).

---

## 1. Compiler et matérialiser le projet dbt

Dans un environnement avec `dbt-duckdb` :

```bash
pip install dbt-duckdb
cd mon_projet_dbt          # IMPORTANT : se placer DANS le dossier du projet
dbt deps                   # si le projet a des packages (dbt_utils…)
dbt compile                # résout le Jinja → target/compiled/**/*.sql
dbt run                    # matérialise les modèles dans la base DuckDB
```

> **Piège** : `dbt` résout le chemin relatif de la base (`path: ./mon.duckdb` dans `profiles.yml`) par rapport au **répertoire d'invocation**, pas à `--project-dir`. Lancer `dbt` depuis un autre dossier crée une base vide ailleurs et `dbt run` échoue avec `Catalog Error: Table ... does not exist`. Toujours `cd` dans le projet d'abord.

Après `dbt run`, tu disposes :
- du SQL plat dans `target/compiled/<projet>/models/**/*.sql` ;
- d'une base DuckDB (`<db>.duckdb`) contenant **toutes** les tables (sources brutes + modèles matérialisés).

---

## 2. Bootstrap du cache de schéma depuis DuckDB

Le CLI `mocksql generate` lit les schémas depuis `schema_cache.json` (sinon il interroge BigQuery). On le pré-remplit par introspection de la base DuckDB :

```python
# bootstrap_schema_cache.py
import duckdb, json
from pathlib import Path

DUCKDB = Path("mon_projet_dbt/mon.duckdb")     # base matérialisée par dbt run
OUT    = Path("mon_projet_dbt/.mocksql/schema_cache.json")

def map_type(dt: str) -> str:
    dt = dt.upper()
    if any(x in dt for x in ("CHAR", "TEXT", "STRING", "UUID", "BLOB", "ENUM")): return "STRING"
    if any(x in dt for x in ("INT", "HUGEINT")):                                 return "INTEGER"
    if any(x in dt for x in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")):  return "FLOAT"
    if "BOOL" in dt:                                                             return "BOOLEAN"
    # DuckDB TIMESTAMP (naïf) → BigQuery DATETIME (naïf). Mapper vers TIMESTAMP
    # (tz-aware côté BQ) donnerait un TIMESTAMPTZ en DuckDB et casserait STRFTIME.
    if "TIMESTAMP WITH TIME ZONE" in dt or "TIMESTAMPTZ" in dt:                  return "TIMESTAMP"
    if "TIMESTAMP" in dt or "DATETIME" in dt:                                    return "DATETIME"
    if dt.startswith("DATE"):                                                    return "DATE"
    if dt.startswith("TIME"):                                                    return "TIME"
    return "STRING"

con = duckdb.connect(str(DUCKDB), read_only=True)
tables = []
for db, sch, tn in con.execute(
    "select database_name, schema_name, table_name from duckdb_tables order by 1,2,3"
).fetchall():
    cols = con.execute(
        "select column_name, data_type from information_schema.columns "
        "where table_catalog=? and table_schema=? and table_name=? order by ordinal_position",
        [db, sch, tn],
    ).fetchall()
    columns = [
        {"name": c, "type": map_type(t), "mode": "NULLABLE", "description": "", "bq_ddl_type": map_type(t)}
        for c, t in cols
    ]
    tables.append({"table_name": f"{db}.{sch}.{tn}", "description": "", "columns": columns})
con.close()

OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps({"tables": tables}, indent=2), encoding="utf-8")
print(f"{len(tables)} table(s) écrites dans {OUT}")
```

Le `table_name` doit être **qualifié comme dans le SQL compilé** : dbt-DuckDB émet `"<db>"."<schema>"."<table>"` (3 parties). Le matching du cache est insensible à la casse et accepte un suffixe (`schema.table`), donc reproduire `database.schema.table` suffit.

> **Mapping de type** : le DDL interne de MockSQL est en syntaxe BigQuery, donc `bq_ddl_type` doit l'être aussi. La subtilité courante est `TIMESTAMP` : un `TIMESTAMP` DuckDB naïf doit être mappé en `DATETIME` (BQ), pas en `TIMESTAMP` (tz-aware) — sinon il revient en `TIMESTAMPTZ` côté DuckDB et les fonctions comme `STRFTIME` échouent.

---

## 3. `mocksql.yml`

Pointe `models_path` vers le SQL **compilé** (copié à plat, ou directement le sous-dossier `target/compiled/...`) :

```yaml
version: "2"
dialect: duckdb
models_path: ./models             # SQL compilé par dbt
duckdb_path: .mocksql/scratch.duckdb   # base scratch d'exécution (≠ base dbt)
schema_cache: .mocksql/schema_cache.json
llm:
  provider: vertexai
  streaming: false
```

> `duckdb_path` est la base **scratch** où MockSQL crée les tables synthétiques. Ne pas la pointer vers la base dbt matérialisée.

---

## 4. Générer les tests

```bash
DUCKDB_PATH=mon_projet_dbt/.mocksql/scratch.duckdb \
mocksql generate mon_projet_dbt/models/mon_modele.sql \
  --config mon_projet_dbt/mocksql.yml \
  --output mon_projet_dbt/.mocksql/tests
```

Le cache étant pré-rempli, aucune requête BigQuery n'est émise. Chaque modèle produit un `.mocksql/tests/<model>.json` avec données d'entrée, résultats DuckDB, assertions et verdict.

---

## Limites connues

- **Un projet mocksql par projet dbt** : chaque exemple dbt a son propre `dbt_project.yml`/`profiles.yml` et sa base — ils ne se partagent pas.
- **Logique date-relative** (`CURRENT_DATE`, fenêtres glissantes) : la génération peut produire des données hors plage et un résultat vide. C'est un signal qualité légitime, pas un bug de setup.
- **Macros à effet d'exécution** (snapshots, `incremental` avancé) : `dbt compile` élide les branches `{% if is_incremental() %}` (faux au compile) — le test porte donc sur le chemin non-incrémental.
