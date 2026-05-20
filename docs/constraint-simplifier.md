# constraint_simplifier — Extraction et simplification des contraintes SQL

Fichier : [`back/build_query/constraint_simplifier.py`](../back/build_query/constraint_simplifier.py)

Ce module analyse une requête SQL brute pour en extraire les **contraintes implicites** (filtres, jointures, dépendances fonctionnelles) et les convertir en un ensemble minimal de colonnes à générer. Il est appelé en amont du LLM dans le nœud `generator`.

---

## Rôle dans le flux

```
validator.py  →  examples_generator.py
                       │
                       ├── simplify(optimized_sql)   ←  constraint_simplifier
                       │        │
                       │        └── SimplificationResult
                       │                ├── source_columns      → colonnes à générer
                       │                ├── derived_columns     → colonnes calculables
                       │                ├── equivalence_classes → clés de jointure
                       │                └── constraint_groups   → un groupe par chemin satisfaisant
                       │
                       └── prompt LLM (VertexAI)
```

---

## API publique

### `simplify(sql, dialect="bigquery", schema=None) → SimplificationResult`

Fonction principale. Prend un SQL complet (avec CTEs) et retourne un `SimplificationResult`.

```python
from build_query.constraint_simplifier import simplify

result = simplify("""
    WITH orders AS (
        SELECT o.id, o.user_id, o.amount
        FROM raw.orders AS o
        WHERE o.amount > 0
    )
    SELECT u.name, o.amount
    FROM orders AS o
    JOIN raw.users AS u ON o.user_id = u.id
    WHERE o.amount BETWEEN 10 AND 500
""", dialect="bigquery")

result.source_columns      # {orders.amount: [FilterConstraint(op='between', value=(10, 500))], ...}
result.derived_columns     # {orders.user_id: (users.id, "users.id")}  ← clé de join
result.equivalence_classes # [frozenset({orders.user_id, users.id})]
result.constraint_groups   # [] si chemin unique, sinon une SimplificationResult par chemin
```

### `extract_constraints(sql, dialect, schema) → list[ConstraintGroup]`

Fonction bas niveau. Retourne une liste de `ConstraintGroup` — un groupe par chemin satisfaisant indépendant. Utile pour débugger ou cibler une CTE précise.

```python
from build_query.constraint_simplifier import extract_constraints

groups = extract_constraints(sql, dialect="bigquery")
for g in groups:
    for f in g.filters:
        print(f.column, f.op, f.value)
    for a, b in g.equalities:
        print(f"  {a} = {b}")
```

### `check_having_cardinality(sql, dialect, threshold=20) → None`

Lève `ValueError` si le SQL contient un `HAVING` qui impose plus de `threshold` lignes par groupe (ex. `HAVING COUNT(*) > 150`). Appelée avant tout appel LLM pour échouer tôt avec un message clair.

### `check_correlated_aggregate_cardinality(sql, dialect, threshold=20) → None`

Lève `ValueError` si un `WHERE` filtre sur une colonne CTE calculée par agrégation (`COUNT(*)`) avec un seuil dépassant `threshold` (ex. `WHERE cte.n > 150` quand `n = (SELECT COUNT(*) FROM t WHERE ...)`).

### `extract_volume_hints(sql, dialect) → list[VolumeHint]`

Détecte les clauses structurelles qui imposent un volume minimum de lignes, sans lever d'exception :

- **`OFFSET N`** → la portée doit produire au moins N+1 lignes
- **`NTILE(N)`** → la portée doit contenir au moins N lignes
- **`RANK / ROW_NUMBER alias <= N`** → la CTE qui définit l'alias doit contenir au moins N lignes

```python
from build_query.constraint_simplifier import extract_volume_hints

hints = extract_volume_hints(sql, dialect="bigquery")
for h in hints:
    print(h.hint_type, h.context, h.min_rows, h.clause_sql)
```

### `detect_select_derived_expressions(sql, dialect) → list[dict]`

Voir [section dédiée](#detect_select_derived_expressions--détection-des-expressions-non-triviales-pour-le-profiler) plus bas.

---

## Types de données

### `ColumnRef`

Référence à une colonne dans la requête.

```python
@dataclass(frozen=True)
class ColumnRef:
    table: str      # alias tel qu'écrit dans la requête, ex: "o", "raw.orders"
    column: str     # nom de colonne, ex: "amount"
    lineage: str    # chemin de résolution CTE (compare=False, non-hashé)
    real_table: str # table réelle si alias différent (compare=False)
```

Deux `ColumnRef` avec le même `table` et `column` sont **identiques** (`__eq__`, `__hash__`) même si `lineage` diffère.

### `FilterConstraint`

Un prédicat `colonne <op> valeur` extrait d'un WHERE ou d'un ON.

```python
@dataclass
class FilterConstraint:
    column: ColumnRef
    op: str          # voir tableau ci-dessous
    value: Any       # scalaire, list (in/not_in), tuple (between), None
    source_columns: list[ColumnRef]  # colonnes de base qui alimentent cette colonne
```

| `op` | Exemple SQL | `needs_llm()` |
|---|---|---|
| `eq` | `col = 'FR'` | Non |
| `neq` | `col != 0` | Oui |
| `gt` / `gte` | `col > 100` | Oui |
| `lt` / `lte` | `col < 1000` | Oui |
| `like` | `col LIKE 'ABC%'` | Oui |
| `not_like` | `col NOT LIKE '%test%'` | Oui |
| `in` | `col IN (1, 2, 3)` | Non |
| `not_in` | `col NOT IN ('x')` | Oui |
| `between` | `col BETWEEN 10 AND 500` | Non |
| `is_null` | `col IS NULL` | Non |
| `is_not_null` | `col IS NOT NULL` | Non |
| `safe_cast_not_null` | `SAFE_CAST(col AS DATE)` | Non |

`needs_llm()` signale que la contrainte ne peut pas être satisfaite mécaniquement — le LLM doit choisir une valeur cohérente.

### `FunctionalConstraint`

Une dépendance fonctionnelle `dérivée = func(source)`.

```python
@dataclass
class FunctionalConstraint:
    derived: ColumnRef   # colonne calculée, ex: a.trimmed_name
    source: ColumnRef    # colonne source, ex: b.raw_name
    func: str            # nom de la fonction, ex: "TRIM", "UPPER"
```

### `ConstraintGroup`

Un chemin satisfaisant complet à travers le SQL. Plusieurs groupes apparaissent quand la requête contient des `UNION ALL`, des `OR` en WHERE (expansion DNF), ou des CTEs avec plusieurs chemins (produit cartésien).

```python
@dataclass
class ConstraintGroup:
    filters: list[FilterConstraint]
    equalities: list[tuple[ColumnRef, ColumnRef]]
    functional: list[FunctionalConstraint]
    col_inequalities: list[tuple[ColumnRef, ColumnRef]]
```

### `SimplificationResult`

Résultat de `simplify()`.

```python
@dataclass
class SimplificationResult:
    source_columns: dict[ColumnRef, list[FilterConstraint]]
    # Colonnes à générer. La valeur est la liste des contraintes applicables (peut être []).

    derived_columns: dict[ColumnRef, tuple[ColumnRef, str]]
    # Colonnes dérivables. Valeur = (colonne_source, "FUNC(source)").
    # Le générateur peut les omettre — elles se déduisent de source.

    equivalence_classes: list[frozenset[ColumnRef]]
    # Groupes de colonnes qui doivent partager la même valeur (clés de jointure).

    filters: list[FilterConstraint]         # contraintes brutes (union de tous les groupes)
    functional: list[FunctionalConstraint]  # dépendances fonctionnelles brutes
    col_inequalities: list[tuple[ColumnRef, ColumnRef]]
    # Paires anti-jointure : (a, b) signifie "lignes sans correspondance sur b"
    # Pattern : LEFT JOIN … ON a = b WHERE b IS NULL

    constraint_groups: list[SimplificationResult]
    # Une SimplificationResult par chemin satisfaisant indépendant.
    # Vide quand la requête n'a qu'un seul chemin (AND uniquement, pas de UNION ni OR).

    constraint_groups_truncated: bool
    # True quand le nombre de chemins dépasse _MAX_CONSTRAINT_GROUPS (32).
    # Les groupes excédentaires sont silencieusement abandonnés.
```

### `VolumeHint`

Contrainte de volume structurelle inférée depuis l'AST.

```python
@dataclass
class VolumeHint:
    hint_type: str   # "offset" | "ntile" | "rank_filter"
    context: str     # label humain : "CTE `paginated`" | "SELECT final"
    min_rows: int    # nombre minimum de lignes requises dans la portée
    clause_sql: str  # affichage : "OFFSET 3" | "NTILE(5)" | "RANK() <= 5 (via `rn`)"
```

---

## Algorithme

```
SQL
 │
 ▼
1. parse_one (sqlglot)    →  AST
 │
 ▼
2. _LineageResolver       →  résout les colonnes CTE vers les tables de base
   (sqlglot.lineage)          ex: cte1.amount → raw.orders.amount
 │
 ▼
3. CTEs (ordre définition) → _walk_tree_grouped par CTE
   → cte_groups_map[cte_name] = list[ConstraintGroup]
 │
 ▼
4. _walk_select_grouped    →  pour chaque SELECT :
   ├── JOIN ON             →  contraintes partagées (filtres, égalités, fonctionnelles)
   ├── WHERE → _to_dnf     →  expansion DNF → un ConstraintGroup par chemin AND
   │         AND(OR(A,B),C) → [[A,C],[B,C]]
   └── CTEs / sous-requêtes → _cross_multiply_groups avec les groupes du SELECT
 │
 ▼
5. UNION ALL               →  groupes des deux branches concaténés (pas multipliés)
 │
 ▼
6. Cap à 32 groupes        →  _MAX_CONSTRAINT_GROUPS = 32 (expansion OR peut exploser)
 │
 ▼
7. _UnionFind (par groupe) →  regroupe les égalités en classes d'équivalence
   union(a, b)                 ex: {orders.user_id, users.id}
 │
 ▼
8. Simplification          →  pour chaque classe, choisit un représentant
   ├── source_columns          (représentant + colonnes avec filtre)
   └── derived_columns         (les autres membres de la classe)
```

### Résolution de lignage CTE

`_LineageResolver` utilise `sqlglot.lineage.lineage()` pour tracer chaque colonne d'une CTE jusqu'à sa table de base. Les résultats sont mis en cache `(table, column) → ColumnRef`.

Pour les expressions calculées (`cte_mix = a.x1 + b.d2`), `resolve_all()` retourne **toutes** les colonnes de base qui alimentent l'expression : `[a.x1, b.d2]`.

### Expansion DNF et cross-multiply

`_to_dnf()` convertit le WHERE en Forme Normale Disjonctive : chaque chemin AND devient un `ConstraintGroup`. Les CTEs avec plusieurs chemins (via OR ou UNION) sont **multipliés** avec les groupes du SELECT extérieur (produit cartésien, cap à 32). Exception : les CTEs utilisées comme source d'anti-jointure (`LEFT JOIN … WHERE IS NULL`) ne sont pas multipliées.

### Détection des anti-jointures

Le pattern `LEFT JOIN … ON a.id = b.ref WHERE b.ref IS NULL` signifie "lignes sans correspondance". La paire `(a.id, b.ref)` va dans `col_inequalities` plutôt que dans `equalities`, pour que le générateur puisse créer des données qui ne se joignent pas.

---

## Exemple complet

**SQL :**
```sql
WITH enriched AS (
    SELECT
        o.id,
        o.amount,
        TRIM(u.email) AS clean_email
    FROM orders AS o
    JOIN users AS u ON o.user_id = u.id
    WHERE o.status = 'COMPLETED'
)
SELECT *
FROM enriched
WHERE amount BETWEEN 50 AND 1000
```

**`result.source_columns` :**
```
orders.status   → [FilterConstraint(op='eq', value='COMPLETED')]
orders.amount   → [FilterConstraint(op='between', value=(50, 1000))]
users.id        → []   ← représentant de la classe {orders.user_id, users.id}
users.email     → []   ← source de TRIM
```

**`result.derived_columns` :**
```
orders.user_id  → (users.id, "users.id")      ← jointure
enriched.clean_email → (users.email, "TRIM(users.email)")  ← fonctionnel
```

**`result.equivalence_classes` :**
```
[frozenset({orders.user_id, users.id})]
```

**`result.constraint_groups` :** `[]` — chemin unique (AND uniquement, pas de UNION/OR).

Le générateur reçoit ces informations et sait qu'il doit :
- Générer `orders.status = 'COMPLETED'`
- Générer `orders.amount` dans `[50, 1000]`
- Générer `users.id` et copier sa valeur dans `orders.user_id`
- Ne **pas** générer `clean_email` — il se déduit de `TRIM(users.email)`

---

## Exemple multi-chemins (OR / UNION)

**SQL :**
```sql
SELECT * FROM orders AS o
WHERE o.status = 'COMPLETED' OR o.status = 'REFUNDED'
```

```python
r = simplify(sql)
len(r.constraint_groups)  # 2

# Groupe 0 : status = 'COMPLETED'
r.constraint_groups[0].filters  # [FilterConstraint(op='eq', value='COMPLETED')]

# Groupe 1 : status = 'REFUNDED'
r.constraint_groups[1].filters  # [FilterConstraint(op='eq', value='REFUNDED')]

# Vue plate (union des deux) — pour compatibilité descendante
r.filters  # les deux contraintes fusionnées
```

Le générateur utilise `constraint_groups` pour créer un jeu de données par chemin satisfaisant, couvrant ainsi les deux branches de la logique métier.

---

## Cas particuliers

| Situation | Comportement |
|---|---|
| Colonne sans alias de table | `ColumnRef("__unknown__", col)` — résolution impossible |
| CTE multi-niveaux | Résolution récursive via sqlglot lineage |
| `SAFE_CAST(col AS TYPE)` | `FilterConstraint(op='safe_cast_not_null', value='TYPE')` sur la colonne interne |
| UNION ALL | Les branches sont concaténées (non multipliées) — chacune donne son propre `ConstraintGroup` |
| Sous-requêtes corrélées dans SELECT | Contraintes fusionnées dans tous les groupes existants |
| OR dans JOIN ON | Non expansé (gap connu) — traité comme un chemin unique |
| HAVING | Non capturé (gap connu) — s'applique à des groupes, pas à des lignes |
| Anti-jointure dans CTE | La CTE n'est pas cross-multipliée avec les groupes extérieurs |
| Schema fourni | Améliore la précision du lineage sqlglot (types, colonnes ambiguës) |
| > 32 chemins | `constraint_groups_truncated = True` — groupes excédentaires abandonnés |

---

## `detect_select_derived_expressions` — Détection des expressions non-triviales pour le profiler

### Rôle

Identifie dans les CTEs d'une requête les **expressions non-triviales** (appels de fonctions SQL utiles à profiler sur des données réelles) et retourne pour chacune : le SQL de l'expression, les tables sources, et les références de colonnes brutes.

Utilisé par le profiler (`profiler.py`) pour construire la requête de profiling des expressions dérivées (`SAFE_CAST`, `REGEXP_EXTRACT`, `COALESCE`, `DATE_DIFF`, `IF`, etc.) en complément du profiling colonne classique.

```python
from build_query.constraint_simplifier import detect_select_derived_expressions

exprs = detect_select_derived_expressions(sql, dialect="bigquery")
# → [
#     {
#       "expr_sql":      "SAFE_CAST(raw.orders.amount AS INT64)",
#       "source_tables": ["raw.orders"],
#       "col_refs":      [("raw.orders", "amount")]   # (alias, col_name) bruts
#     },
#     {
#       "expr_sql":      "COALESCE(t.x, t.y)",
#       "source_tables": ["raw.orders"],
#       "col_refs":      [("t", "x"), ("t", "y")]
#     }
# ]
```

Résultat limité à 10 expressions. Les expressions sans référence de colonne (constantes pures) sont ignorées. Déduplication par `expr_sql`.

### Expressions exclues (triviales)

Les transformations dont le résultat est prévisible depuis les colonnes sources déjà profilées sont ignorées :
- Fonctions de manipulation de chaînes sans sémantique : `UPPER`, `LOWER`, `TRIM`, `SUBSTR`, `CONCAT`, `LENGTH`…
- Fonctions arithmétiques pures : `ROUND`, `FLOOR`, `CEIL`, `ABS`, `MOD`…
- `CAST` / `CONVERT` directs (sans logique conditionnelle)
- Fonctions d'agrégation (`SUM`, `COUNT`, `AVG`…) — les colonnes sources sont déjà profilées
- `UNNEST` — fonction de table, non utilisable comme expression scalaire

Les expressions **retenues** incluent notamment : `SAFE_CAST`, `COALESCE`, `IF`, `IIF`, `IFNULL`, `NULLIF`, `REGEXP_EXTRACT`, `REGEXP_REPLACE`, `DATE_DIFF`, `DATE_TRUNC`, `TIMESTAMP_DIFF`, `FORMAT_DATE`, `PARSE_DATE`, `JSON_VALUE`, `STRUCT`, et toute fonction anonyme non dans la liste d'exclusion.
