# `mocksql inspect` — diagnostic déterministe d'un cas rouge

`mocksql inspect <model> -u <test_uid> --json` répond à **POURQUOI** un cas est
rouge (ou pourquoi un fix ne prend pas), **sans LLM par défaut**. C'est le verbe
« diagnose » de la boucle TDD agent : là où `mocksql test` dit *qu'un cas diverge*
(diff de lignes `expect_check`), `inspect` descend **sous** le diff — trace CTE par
CTE, sondes de cardinalité join par join — pour nommer la cause.

Tout est rejoué **en local sur DuckDB** contre le SQL **du disque** (comme `test`),
avec les données synthétiques du cas. Déterministe, zéro appel LLM, 0 € facturé.

## Signature

```bash
mocksql inspect <model> -u <test_uid> --json
mocksql inspect <model> -u <test_uid> --json --llm   # ajoute un verdict LLM (opt-in)
```

- `<model>` : nom du modèle (`orders`, `demo/payment_summary`) — comme `confirm`.
- `-u/--test-uid` : le `test_uid` du cas (issu de `mocksql test --json`).
- `--json` : sortie structurée (défaut recommandé pour un agent).
- `--llm` : **opt-in**. Ajoute un `llm_verdict` nourri de l'historique de correction.
  Absent par défaut — `inspect` est déterministe par construction.

Code de sortie : **toujours 0** (diagnostic en lecture seule, jamais un gate).

## Schéma JSON de sortie

```json
{
  "model": "customer_orders",
  "test_uid": "9a0c",
  "test_name": "Client sans commande",
  "description": "un client sans commande doit apparaître, signup_date non NULL",

  "sql_source": "disk",
  "sql_source_warning": null,

  "status": "unconfirmed",
  "review": "draft",

  "diagnosis": {
    "code": "empty_upstream_cte",
    "suspect": "orders_enriched",
    "detail": "La CTE requise `orders_enriched` produit 0 ligne (suspect n°1)."
  },

  "expect_check": {
    "passed": false, "ordered": false,
    "expected_count": 1, "actual_count": 0,
    "missing":    [ { "customer_id": 42, "signup_date": "2024-01-15" } ],
    "unexpected": [],
    "order_only_mismatch": false
  },

  "observed": { "row_count": 0, "truncated": false, "rows": [] },

  "cte_trace": [
    { "name": "customers_base",  "row_count": 3, "blocking": false,
      "sample": [ { "customer_id": 42 } ] },
    { "name": "orders_enriched", "row_count": 0, "blocking": true,
      "steps": [ "FROM orders o", "+ JOIN customers (c.id = o.customer_id) -> 0" ],
      "blocking_predicates": [
        "JOIN c — décomposition par prédicat :",
        "  c.id = o.customer_id -> 0 valeur(s) commune(s) — gauche {42}, droite {}"
      ] }
  ],

  "join_probes": [
    { "cte": "orders_enriched", "join_index": 0,
      "left": "orders o", "right": "customers c", "join_type": "INNER",
      "on": "c.id = o.customer_id",
      "left_rows": 0, "right_rows": 3, "result_rows": 0,
      "verdict": "empty" }
  ],

  "llm_verdict": null
}
```

## Les trois lentilles du diagnostic

### 1. Rejeu + `sql_source` + diff `expect_check`

Le cas est rejoué contre le SQL disque (`resolve_run_sql`, même chemin que `test`).

- `sql_source` : `disk` (défaut, le `.sql` a été lu), `frozen`, ou
  **`snapshot-fallback`** (le `.sql` est introuvable/illisible → le snapshot figé a
  tourné). Sur `snapshot-fallback`, `sql_source_warning` est une **chaîne non nulle** :
  la trace ne reflète alors pas ce que l'agent croit avoir écrit — c'est le
  garde-fou anti-silent-green (F4). L'agent DOIT vérifier ce champ avant de faire
  confiance au reste.
- `status` / `review` : identiques à `mocksql test` (`pass` · `fail` · `unconfirmed`
  · `error` · `skip` ; `confirmed` / `stale` / `draft`).
- `expect_check` : le diff de lignes (`compare_expect`, multiset ou ordonné) — `missing`
  = lignes voulues absentes, `unexpected` = lignes produites hors contrat, cappées.
  `null` si le cas ne porte pas de contrat `expect`.
- `observed` : sortie observée, `rows` cappée (`truncated: true` au-delà de la limite) ;
  le diff exact vit déjà dans `expect_check`.

### 2. Trace CTE par CTE (le suspect n°1)

`cte_trace` est une **liste ordonnée** (l'ordre du pipeline est signifiant) : chaque
entrée porte le nom de la CTE et son `row_count`. **La première CTE requise vide est
le suspect n°1** — un vide amont propage silencieusement en aval (agrégat d'un
ensemble vide → 1 ligne NULL, LEFT JOIN + COALESCE d'échafaudage) et peut masquer la
cause réelle.

- `blocking` : la CTE vide est-elle **atteignable depuis le résultat final par des
  arêtes requises** (`classify_blocking_ctes`) ? Une CTE vide seulement LEFT-jointe /
  en anti-join est un résultat métier valide, pas un défaut → `blocking: false`.
- `sample` : pour une CTE non vide à faible cardinalité (≤ 3 lignes), la **valeur**
  des lignes (pas juste le compte) — expose un pivot qui alimente un filtre en aval.
- `steps` : décomposition cumulative (FROM, puis chaque JOIN/WHERE) quand la CTE est
  vide — où exactement le compte tombe à 0.
- `blocking_predicates` : sur la CTE bloquante vide **uniquement**, la décomposition
  **par prédicat** (`_run_join_predicate_breakdown` / filtres `= (sous-requête)`) —
  la lentille *pourquoi vide* : quel prédicat n'a **aucune valeur commune** entre ses
  deux côtés (marqué `← BLOQUANT`).

### 3. Sondes de cardinalité join par join

`join_probes` est la lentille **orthogonale** : pour **chaque JOIN** de chaque CTE, on
compte les lignes en **entrée**, du **côté joint**, et en **sortie**, pour **décrire**
comment ce JOIN transforme la cardinalité (multiplie / réduit / préserve). C'est un
**fait**, pas un verdict de qualité : `inspect` ne connaît pas la cardinalité *attendue*
(un LEFT un-à-plusieurs *doit* faire grossir le compte) — l'oracle, c'est `expect`.

Dans une chaîne de JOINs (`A JOIN B JOIN C`), la mesure est **incrémentale** : le
compte est attribué au JOIN précis, pas à la CTE entière.

| champ | sens |
|---|---|
| `cte` | CTE (ou `final_query`) où vit le JOIN |
| `join_index` | index du JOIN dans la CTE (0-based) |
| `left` | libellé du côté ancre (source du `FROM`) |
| `right` | libellé de la source ajoutée par ce JOIN |
| `join_type` | `INNER` · `LEFT` · `RIGHT` · `FULL` · `CROSS` |
| `on` | prédicat `ON` rendu |
| `left_rows` | lignes **entrant** dans ce JOIN (FROM + tous les JOINs précédents de la CTE) |
| `right_rows` | lignes de la source jointe **seule** |
| `result_rows` | lignes **après** ce JOIN |
| `verdict` | voir ci-dessous |

`verdict` — descripteur **factuel** de la transformation (pas un jugement) :
- `empty` — `result_rows == 0` (le JOIN ne produit rien) ;
- `fan_out` — `result_rows > left_rows` (le côté droit multiplie les lignes) ;
- `shrinks` — `result_rows < left_rows` (le JOIN réduit le nombre de lignes) ;
- `preserves` — `result_rows == left_rows` (cardinalité inchangée).

> `left` est un **libellé d'orientation** (la source ancre) ; l'autorité du verdict
> vient des trois **comptes**. Dans une chaîne, `left_rows` est le cumul entrant
> (FROM + JOINs précédents), pas seulement la table `left`.

## `diagnosis.code` — le champ que le skill lit

Un seul champ déterministe résume la cause probable, calculé par **priorité** (première
règle qui matche gagne) :

| priorité | `code` | condition | `suspect` |
|---|---|---|---|
| 1 | `sql_source_fallback` | `sql_source == "snapshot-fallback"` (empoisonne tout le reste, F4) | `null` |
| 2 | `error` | le rejeu a levé une erreur | `null` |
| 3 | `consistent` | `expect_check.passed` (cas **vert** — rien à corriger) | `null` |
| 4 | `empty_upstream_cte` | une CTE **bloquante** produit 0 ligne (fait = mécanisme du vide) | nom de la CTE |
| 5 | `nondeterministic_order` | `expect_check.order_only_mismatch` (ex-æquo sur la clé de tri) | `null` |
| 6 | `expect_diff` | `expect_check` présent et non passé (l'**oracle** mène ; `join_probes` = évidence) | `null` |
| 7 | `join_fan_out` | **faute de contrat `expect`**, un `join_probes` a le verdict `fan_out` | `"<cte>#<join_index>"` |
| 8 | `join_shrinks` | **faute de contrat `expect`**, un `join_probes` a le verdict `shrinks` | `"<cte>#<join_index>"` |
| 9 | `consistent` | repli : ni `expect`, ni anomalie structurelle | `null` |

> **Pourquoi l'oracle prime (priorité 6 > 7-8).** Une sonde de cardinalité énonce un
> **fait** (`fan_out` = 1 → 2), pas un défaut : `inspect` ignore la cardinalité *attendue*,
> donc un JOIN un-à-plusieurs parfaitement **sain** déclenche `fan_out`. Laisser ce fait
> primer sur le diff `expect` épinglait des JOINs sans rapport comme cause racine et
> enterrait le vrai écart de valeur (le cas rouge le plus courant). `join_*` n'est donc une
> cause de **tête** que **faute d'oracle** (`expect` absent) ; sinon les comptes restent
> disponibles dans `join_probes` comme **évidence** que l'agent lit sous le diff.
> `empty_upstream_cte` (priorité 4) reste au-dessus car « 0 ligne en sortie » **est** le
> mécanisme du vide, pas une simple transformation de cardinalité.

## Verdict LLM (opt-in, `--llm`)

`llm_verdict` vaut `null` sauf si `--llm` est passé. Il n'est **jamais** le défaut :
`inspect` existe précisément pour donner un signal **déterministe et gratuit**. Le
verdict LLM (nourri de l'historique de correction) est une aide de dernier recours
quand les trois lentilles déterministes ne suffisent pas à trancher.
