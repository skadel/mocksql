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
                       │                ├── source_columns   → colonnes à générer
                       │                ├── derived_columns  → colonnes calculables
                       │                └── equivalence_classes → clés de jointure
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
```

### `extract_constraints(sql, dialect, schema) → (filters, equalities, functional, col_inequalities)`

Fonction bas niveau. Retourne les 4 listes brutes sans simplification. Utile pour débugger ou cibler une CTE précise.

```python
filters, equalities, functional, col_inequalities = extract_constraints(sql)
```

---

## Types de données

### `ColumnRef`

Référence à une colonne dans la requête.

```python
@dataclass(frozen=True)
class ColumnRef:
    table: str    # alias tel qu'écrit dans la requête, ex: "o", "raw.orders"
    column: str   # nom de colonne, ex: "amount"
    lineage: str  # chemin de résolution CTE (compare=False, non-hashé)
    real_table: str  # table réelle si alias différent (compare=False)
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

    filters: list[FilterConstraint]         # contraintes brutes (toutes)
    functional: list[FunctionalConstraint]  # dépendances fonctionnelles brutes
    col_inequalities: list[tuple[ColumnRef, ColumnRef]]
    # Paires anti-jointure : (a, b) signifie "lignes sans correspondance sur b"
    # Pattern : LEFT JOIN … ON a = b WHERE b IS NULL
```

---

## Algorithme en 5 étapes

```
SQL
 │
 ▼
1. parse_one (sqlglot)  →  AST
 │
 ▼
2. _LineageResolver     →  résout les colonnes CTE vers les tables de base
   (sqlglot.lineage)        ex: cte1.amount → raw.orders.amount
 │
 ▼
3. _walk_tree           →  extrait depuis WHERE + ON de chaque SELECT/UNION
   ├── FilterConstraint     (col <op> valeur)
   ├── equalities           (col = col)
   └── FunctionalConstraint (col = FUNC(col))
 │
 ▼
4. _UnionFind           →  regroupe les égalités en classes d'équivalence
   union(a, b)              ex: {orders.user_id, users.id}
 │
 ▼
5. Simplification       →  pour chaque classe, choisit un représentant
   ├── source_columns       (représentant + colonnes avec filtre)
   └── derived_columns      (les autres membres de la classe)
```

### Résolution de lignage CTE

`_LineageResolver` utilise `sqlglot.lineage.lineage()` pour tracer chaque colonne d'une CTE jusqu'à sa table de base. Les résultats sont mis en cache `(table, column) → ColumnRef`.

Pour les expressions calculées (`cte_mix = a.x1 + b.d2`), `resolve_all()` retourne **toutes** les colonnes de base qui alimentent l'expression : `[a.x1, b.d2]`.

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

Le générateur reçoit ces informations et sait qu'il doit :
- Générer `orders.status = 'COMPLETED'`
- Générer `orders.amount` dans `[50, 1000]`
- Générer `users.id` et copier sa valeur dans `orders.user_id`
- Ne **pas** générer `clean_email` — il se déduit de `TRIM(users.email)`

---

## Cas particuliers

| Situation | Comportement |
|---|---|
| Colonne sans alias de table | `ColumnRef("__unknown__", col)` — résolution impossible |
| CTE multi-niveaux | Résolution récursive via sqlglot lineage |
| `SAFE_CAST(col AS TYPE)` | `FilterConstraint(op='safe_cast_not_null', value='TYPE')` sur la colonne interne |
| UNION ALL | Les deux branches sont parcourues indépendamment |
| Sous-requêtes corrélées | Parcourues via `_walk_tree` récursif |
| Schema fourni | Améliore la précision du lineage sqlglot (types, colonnes ambiguës) |

---

## Debug

Le module affiche `print(">>>>>>><result\n", result)` à la fin de `simplify()` (ligne 1064) — à retirer avant la mise en production.

Pour inspecter les contraintes brutes sans simplification :

```python
filters, equalities, functional, col_inequalities = extract_constraints(sql, dialect="bigquery")
for f in filters:
    print(f.column, f.op, f.value)
for a, b in equalities:
    print(f"  {a} = {b}")
```
