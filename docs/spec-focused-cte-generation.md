# Spec — Génération focalisée par CTE (Focused CTE Generation)

> Statut : proposition · Auteur : skadel + Claude · Date : 2026-06-06
> Cible : résoudre les requêtes `empty_results` que la régénération holistique
> n'arrive pas à faire converger (ex. `examples/spider_complexified/c1.sql`).

## 1. Problème

Quand DuckDB renvoie 0 ligne, MockSQL doit régénérer des données qui satisfont
*toutes* les contraintes de la requête. Sur les requêtes larges (c1 : 18 CTEs,
~10 tables, JOINs/anti-joins/sous-requêtes corrélées `partition_date = MAX(...)`),
générer des données cohérentes pour tout en un coup est combinatoirement trop dur :

- La **régénération holistique** (chemin actuel : `empty_results → generator`) régénère
  toutes les tables avec *toutes* les contraintes dans le prompt → trop de colonnes,
  prompt énorme, le LLM noie la condition dure (regex, `PARSE_DATE`) dans le bruit.
- Le **conversational_agent** (ancien chemin) patchait un champ à la fois → incapable
  de réparer une incohérence multi-tables.

Les deux échouent sur c1 (~5 min, 10 retries, aucune convergence).

## 2. Idée

Au lieu de réparer la requête entière, **isoler la première CTE qui produit 0 ligne**,
en faire une requête autonome, et **régénérer des données uniquement pour son sous-arbre** :

1. Identifier la CTE qui fail (déjà fait : `failing_cte` / `cte_trace`).
2. Construire une requête tronquée : `WITH <dépendances transitives> SELECT * FROM <failing_cte>`.
3. Qualifier/optimiser avec sqlglot (même chemin que la validation : `optimize_query`).
4. Lancer une génération **focalisée** sur cette sous-requête : beaucoup moins de
   colonnes, contraintes locales seulement → cycle de correction rapide, le LLM
   peut raisonner sur la condition dure.
5. **Remonter le DAG** : une fois cette CTE non-vide, passer à la CTE suivante qui fail,
   en réutilisant les données déjà générées (merge), jusqu'à ce que la requête complète
   produise des lignes.

## 3. Pourquoi c'est tractable (et où ça coince)

### Ce qui marche
- **L'infra de troncature existe** : `_run_cte_trace` (`examples_executor.py:654`)
  construit déjà `WITH <ctes[0..i]> SELECT * FROM cteN` et l'exécute. On réutilise.
- **Transfert trivial** : on régénère les **mêmes tables de base**, donc les données
  reviennent directement dans la requête complète (pas de remapping).
- **Réduction de l'espace** : générer pour le sous-arbre = peu de tables/colonnes,
  prompt court, focalisé sur la contrainte qui bloque.

### Les pièges (traités dans la spec)
- **P1 — Cohérence inter-tables** : si la CTE qui fail lit une table partagée par
  d'autres CTEs, la rendre non-vide ne garantit pas les JOINs aval. → résolu par
  l'**itération DAG** (une CTE à la fois) + **merge par union de lignes**, pas par un
  combinator magique.
- **P2 — CTE vide ≠ bloquante** : une CTE vide derrière un `LEFT JOIN` ou utilisée en
  **anti-join** (`x.k IS NULL`, `NOT IN`, `NOT EXISTS`) ne bloque PAS le résultat (et la
  rendre non-vide peut être contre-productif, ex. `siret_onus` dans c1). → nécessite une
  **classification des CTEs réellement bloquantes** (§7). Corrige un bug du `cte_trace` actuel.
- **P3 — Multi-CTE indépendantes** : itérer en ordre topologique ; les CTEs sur des
  tables sources disjointes peuvent être traitées en parallèle (v2).
- **P4 — Merge destructeur** : ajouter des lignes peut violer une unicité ou exploser un
  JOIN. → merge contraint (§6.4) + re-validation systématique sur la requête complète.

## 4. Verdict & scope

Idée **retenue**. Recadrages clés vs la formulation initiale :
- **Le « combinator » est une RÉGÉNÉRATION informée par few-shot, PAS un merge de lignes.**
  Le merge (union/replace par table) crée trop de problèmes : violation d'unicité, explosion
  de JOIN, réconciliation de deux datasets générés indépendamment. À la place : la génération
  focalisée produit un **dataset validé** pour le sous-arbre (les lignes qui font passer la
  CTE — avec la regex/date/code dure ET les clés de jointure cohérentes entre elles), et on
  **régénère** le périmètre élargi en passant ce dataset comme **few-shot example** dans le
  prompt (« voici des données qui produisent des lignes pour la CTE X ; étends-les »). Pas une
  contrainte dure énumérant des valeurs : un exemple concret et cohérent que le LLM imite et
  prolonge. Guidance *soft* → préserve les relations inter-colonnes et laisse au modèle la
  marge d'aligner les nouvelles tables. On ne fusionne jamais de lignes ; on transporte le
  *savoir* (un exemple qui marche). Aligné avec la philosophie « générer, pas patcher », avec
  `update_data_prompt(existing_test=…)` et `_build_eval_context` existants.
- **Élargissement progressif du périmètre** pour ne pas retomber dans la régen holistique :
  chaque régénération n'ajoute que les tables de la couche suivante du DAG, les couches
  inférieures déjà validées étant **épinglées** (valeurs d'ancrage).
- Ajouter en préalable une **classification bloquant/non-bloquant** des CTEs vides.

MVP (Phase 1) : une seule CTE qui fail, sous-arbre isolé, génération focalisée → valeurs
d'ancrage → une régénération informée du périmètre élargi, re-run complet. Pas de
parallélisme, pas de raffinement multi-niveaux.

## 5. Architecture

### 5.1 Nouveaux nœuds LangGraph

| Nœud | Rôle |
|---|---|
| `cte_isolator` | Construit la sous-requête tronquée+qualifiée pour `failing_cte` ; calcule la fermeture transitive des dépendances et le sous-ensemble de tables/colonnes ; sélectionne la **première CTE réellement bloquante** (§7). |
| `focused_generator` | Génère des données pour la sous-requête isolée. Réutilise `generate_examples_` avec un `optimized_sql` = sous-requête, `used_columns` réduit, `constraints_hint` local. |
| `cte_combinator` | **Régénère** (pas de merge) le périmètre élargi en passant le dataset validé du sous-arbre comme **few-shot example** (§6.4) ; relance l'`executor` sur la **requête complète**. |

> Alternative légère : pas de nouveaux nœuds, mais un sous-graphe/branche réutilisant
> `generator`+`executor` avec un `focus_cte` dans le state. Voir §9.

### 5.2 Champs `QueryState` ajoutés

```python
focus_cte: Optional[str]            # CTE actuellement isolée (None = mode normal)
focus_sql: Optional[str]            # sous-requête tronquée+qualifiée
focus_used_columns: Optional[list]  # used_columns réduits au sous-arbre
focus_examples: Optional[dict]      # dataset validé du sous-arbre {table: [rows]} — passé
                                    # comme few-shot example lors de la régénération élargie
focus_scope: Optional[list]         # tables/CTEs déjà validées (couches épinglées)
blocking_ctes: Optional[list]       # CTEs vides ET sur un chemin résultant (ordre DAG)
focus_retries: int                  # budget propre à la boucle focalisée
```

## 6. Algorithme détaillé

### 6.1 Décomposition & dépendances  · **IMPLÉMENTÉ** (`cte_graph.py`)
- Source : `query_decomposed` (`{name, code, dependencies, sources}`). Le chemin validator
  réel (`split_query`/`extract_cte_dependencies`) peuple `dependencies`, mais le fallback
  `_lightweight_query_decomposed` les laisse vides → on **recalcule** dans tous les cas :
  `build_cte_dependency_graph` scanne `code` (sqlglot `find_all(exp.Table)`) et retient les
  noms qui matchent d'autres CTEs (insensible à la casse).
- `transitive_deps(graph, cte)` = fermeture transitive ; `topo_sort` = ordre + détection de cycle.
- **Deux graphes** : `build_cte_dependency_graph` (structurel — toutes les arêtes, pour la
  troncature §6.2) et `build_required_dependency_graph` (arêtes requises seulement — pour la
  classification §7 et la réduction §6.3).

### 6.2 Troncature + qualification  · **IMPLÉMENTÉ** (`build_isolated_sql` / `isolate_cte`)
```
sub_ctes = topo_sort(deps*(failing_cte)) + [failing_cte]   # graphe STRUCTUREL (validité SQL)
sub_sql  = "WITH " + join(sub_ctes) + f" SELECT * FROM {failing_cte}"
sub_sql  = optimize_query(parse_one(sub_sql), schema, dialect)  # validator.py:422 (optionnel)
```
Noms de CTE **nus** (valides DuckDB *et* BigQuery ; DuckDB rejette les backticks). La closure
utilisée ici est **structurelle** : les CTEs LEFT-optionnelles restent dans le `WITH` (sinon
le SQL casse), mais elles ne seront **pas générées** (§6.3). Qualification via `optimize_query`
si un `schema` est fourni, avec fallback sur le SQL non qualifié (R1).

### 6.3 Correction focalisée (part des données qui ont échoué)
La génération focalisée n'est **pas** une génération à partir de zéro : c'est une
**correction** des données déjà générées, restreintes au sous-arbre.
- `used_columns` restreints aux tables sources de la closure du graphe **requis**
  (`reduce_used_columns`, **IMPLÉMENTÉ**) → les tables LEFT-optionnelles / anti-jointes sont
  **exclues de la génération** (« pas d'intérêt à les générer dans le débogage »). Elles
  restent vides dans le `WITH` → leurs LEFT JOINs produisent des NULLs.
- **Données d'entrée = les données précédemment générées, filtrées** aux tables du
  sous-arbre (on jette les tables hors-périmètre). On les passe au LLM focalisé qui doit
  les *corriger* pour que `sub_sql` produise des lignes — le `constraints_hint`
  (simplify+hint, déjà caché) est petit → prompt court, cycle rapide.
- Validation immédiate : exécuter `sub_sql` sur DuckDB (réutilise `run_query_on_test_dataset`).
  Non-vide → succès local. Vide → retry focalisé.

#### 6.3bis Décision de faisabilité (après 2 échecs sur la même CTE)
Si le focus sur une CTE échoue **2 fois** (`focus_strikes[cte] == 2`), on ne boucle pas
bêtement : on demande explicitement au LLM focalisé un **verdict structuré** :
```python
class FocusOutcome(BaseModel):
    feasible: bool
    explanation: str          # si infeasible : POURQUOI (anti-join contradictoire,
                              # sous-requêtes partition_date incompatibles, filtre vide…)
    corrected_data: dict | None  # si feasible : une dernière tentative de correction
```
- `feasible=False` → on **arrête** la boucle pour cette CTE et on remonte l'`explanation`
  comme verdict (cf. §9). C'est un résultat *utile* : « cette requête ne peut pas être
  testée car X » révèle souvent un vrai bug de requête / une requête sur-contrainte. Aligné
  avec la valeur produit « verdict argumenté ».
- `feasible=True` + `corrected_data` non-vide qui passe → succès.

#### 6.3ter Dataset validé → few-shot
Une fois le sous-arbre validé, le `{table: [rows]}` qui marche est stocké tel quel dans
`focus_examples` (dataset cohérent « qui marche »), pour servir de few-shot à la
régénération élargie (§6.4). On peut surligner les colonnes *contraintes* (regex/format)
dans le few-shot, mais on ne les transforme PAS en énumération de valeurs dures.

### 6.4 Combinator = régénération few-shot (PAS de merge, PAS de contrainte dure)
Le combinator ne fusionne **aucune** ligne (le merge crée trop de problèmes : unicité,
explosion de JOIN, réconciliation de deux datasets). Il **régénère** avec un exemple :
- Construit le SQL du **périmètre élargi** : `focus_scope` ∪ tables de la couche suivante
  du DAG (les CTEs qui consomment la CTE qu'on vient de valider). Au dernier niveau =
  requête complète.
- Appelle le générateur sur ce périmètre en passant `focus_examples` comme **few-shot
  example** dans le prompt (« voici un jeu de données qui produit des lignes pour la CTE
  X — génère le jeu complet dans le même esprit, en l'étendant aux nouvelles tables »).
  Canal réutilisable : `update_data_prompt(existing_test=…)` ou un bloc dédié, PAS le canal
  `col_hints` (qui durcit en contrainte). Guidance soft → le LLM imite la structure validée
  (y compris les clés de jointure cohérentes) et l'étend.
- Re-exécute le périmètre élargi sur DuckDB. Non-vide → on épingle ces tables dans
  `focus_scope`, on passe à la couche suivante. Vide → retry focalisé sur la nouvelle
  première CTE bloquante.

> Aucune donnée « qui failait avant » n'est conservée : on transporte un *exemple qui
> marche*, pas les lignes en échec ni une liste de contraintes. C'est ce qui évite à la fois
> les pièges du merge et la sur-contrainte.

### 6.5 Boucle d'itération (élargissement progressif)
```
focus_scope, examples, strikes = {}, {}, defaultdict(int)
prev_data = current_generated_data            # données qui ont échoué
while full_query empty and focus_retries > 0:
    bcte = first_blocking_cte(cte_trace)       # §7
    if bcte is None: break                     # plus de bloqueur réel → autre cause
    sub = isolate(bcte, scope=focus_scope)
    seed = filter_tables(prev_data, sub.tables)        # §6.3 : on part des données échouées
    data = focused_correct(sub, seed)                  # corrige le seed pour faire passer la CTE
    if sub still empty:
        strikes[bcte] += 1; focus_retries -= 1
        if strikes[bcte] >= 2:                         # §6.3bis
            outcome = focused_feasibility_decision(sub, seed)
            if not outcome.feasible:
                return INFEASIBLE(bcte, outcome.explanation)   # §9 — verdict argumenté
            data = outcome.corrected_data
            if data is None or sub still empty: continue
        else:
            continue
    examples |= {t: data[t] for t in sub.tables}       # §6.3ter
    focus_scope |= sub.tables                           # couche validée → épinglée
    prev_data |= data                                   # base de départ pour la couche suivante
    rerun_full()                                        # met à jour cte_trace
    # couche suivante régénérée par few-shot (§6.4) avec `examples`
```
À chaque tour, le périmètre grandit d'une couche ; tout ce qui est sous `focus_scope` est
fourni au LLM comme few-shot → chaque correction/régénération reste **petite et locale**.
La sortie `INFEASIBLE` transforme une non-convergence en **verdict argumenté** (et borne la
latence : pas besoin d'épuiser les 10 retries).

## 7. Classification des CTEs bloquantes (préalable critique — corrige P2)

> **Statut : IMPLÉMENTÉ** (`build_query/cte_graph.py`, `tests/test_cte_graph.py`).
> Fonctions pures, non encore câblées dans le graphe LangGraph.

Le cœur est une notion d'**arête requise** : une référence d'une CTE/table vers une autre
n'est « requise » (une source vide y annule le résultat) que si elle est consommée via :
- `FROM` direct, ou `INNER`/`CROSS JOIN` → **requise**.
- `LEFT/RIGHT/FULL JOIN` **avec un prédicat forçant** sur son alias dans le WHERE
  (`x.col = …`, `x.col IS NOT NULL`, `x.col IN (...)`) → de facto INNER → **requise**.
  (ex. `RESEAU` dans c1 : `WHERE RESEAU.reseau IN ("BP","CE")`.)

N'est **pas** requise (et ne doit jamais être ciblée pour la rendre non-vide) :
- `LEFT/RIGHT/FULL JOIN` optionnel (aucun prédicat forçant) → ses lignes manquantes
  deviennent des NULLs (ex. `TMP_MR` dans c1).
- Anti-join : `LEFT JOIN … WHERE alias.col IS NULL`, `NOT IN (subquery)`, `NOT EXISTS`,
  `NOT (… IN/EXISTS …)`, `x <> ALL (subquery)` (ex. `SIRET_ONUS` dans c1).

**Classification** : `blocking_ctes` = CTEs vides (`cte_trace`) **atteignables depuis
`final_query` par des arêtes requises**, ordonnées topologiquement. L'atteignabilité depuis
le résultat propage la requiredness transitivement : une CTE seulement LEFT-jointe ne
« contamine » pas ses propres dépendances (corrige la limitation non-transitive du MVP initial).

Implémentation (sqlglot) :
- `_anti_join_table_ids` : tables sous anti-join ensembliste (`NOT IN`/`NOT EXISTS`/
  `NOT(Paren(In))`/`<> ALL`).
- `_forced_outer_aliases` : aliases OUTER référencés dans le WHERE **moins** les aliases
  anti-join `IS NULL` (réutilise `constraint_simplifier._detect_anti_join_aliases`).
- `_required_table_names` → `build_required_dependency_graph` → `classify_blocking_ctes`.

> Bénéfice transverse : le `cte_trace`/diagnostic actuel marque à tort `tmp_mr`/`siret_onus`
> comme « filtre bloquant ». Cette classification corrige le diagnostic pour tous les chemins.

**Limites connues (documentées dans les docstrings)** :
- Forçant conservateur : un alias présent à la fois en `IS NULL` *et* dans un autre prédicat
  est traité comme non forçant.
- Matching du qualifieur WHERE par nom/alias en minuscules (suffit pour c1).
- Anti-joins non couverts : `EXCEPT`, `LEFT ANTI JOIN` explicite, `!= ANY`.

## 8. Intégration LangGraph

Deux options :

**A. Branche dédiée (recommandée pour la lisibilité)**
```
executor → route_executor → test_evaluator
test_evaluator (empty_results & blocking_ctes non vide) → cte_isolator
cte_isolator → focused_generator → focused_executor(sub_sql)
   ↘ (sub non-vide) → cte_combinator → executor (requête complète)
   ↘ (sub vide, retries>0) → focused_generator
   ↘ (épuisé) → bad_data_exhausted
```

**B. Réutiliser `generator`/`executor` avec un drapeau `focus_cte`** (moins de nœuds,
mais surcharge la logique conditionnelle de `generate_examples_`).

→ Démarrer en **A** : isolation testable indépendamment, pas de régression sur le chemin normal.

## 9. Budget & terminaison
- `focus_retries` (ex. 3) **par CTE bloquante**, distinct de `gen_retries`.
- Garde globale : nombre total d'itérations focalisées borné (ex. `len(blocking_ctes) * 3`).
- Abandon anticipé : si après merge la **même** CTE reste première bloquante 2 fois →
  abandon (la condition est probablement insatisfiable, ex. conflit anti-join de c1).
- Toujours finir par `history_saver` avec le meilleur état atteint.

## 10. Risques & questions ouvertes
- **R1** : sous-requête qualifiée invalide si la CTE référence des alias résolus plus haut
  (window aliases, `QUALIFY`). Mitigation : `optimize_query` + fallback sur le SQL non
  qualifié (déjà le pattern du CLI).
- **R2** : la régénération du périmètre élargi peut, au dernier niveau, redevenir aussi
  large que la régen holistique (pas de gain). Mitigation : élargissement **une couche à la
  fois** (les couches inférieures sont épinglées via ancres, donc peu de colonnes neuves) +
  cap `focus_used_columns` (§Q2) au-delà duquel on s'arrête.
- **R2bis** : ancres **sur-contraignantes** — si on épingle trop de valeurs, le LLM n'a plus
  de marge pour aligner les clés de jointure des nouvelles tables. Mitigation : n'épingler
  que les colonnes réellement *contraintes* (filtre/regex/format), pas les clés de jointure
  libres.
- **R3** : conflit structurel insatisfiable (anti-join c1) — la spec **ne le résout pas**,
  mais l'abandon anticipé (§9) le **borne** à ~1 cycle au lieu de 10.
- **Q1** : peut-on peupler `dependencies`/`sources` dans `query_decomposed` une fois pour
  toutes (au lieu de recalculer) ? **Constat** : `split_query`/`extract_cte_dependencies`
  (`validator.py`) les peuplent **déjà** sur le chemin validator réel ; seul le fallback
  `_lightweight` les omet. On pourrait consommer `query_decomposed.dependencies` quand présent
  et ne recalculer qu'en fallback (optimisation transverse, non bloquante).
- **Q2** : faut-il un cap `focus_used_columns` (nb tables) au-delà duquel on retombe sur la
  régen holistique (sous-arbre trop large = pas de gain) ?

## 11. Tests (cf. méthode test-before-fix)

**Écrits — `tests/test_cte_graph.py` (25 tests verts)** :
- DAG : closure transitive (synthétique + c1), `topo_sort` (ordre + cycle).
- Classification : `TMP_MR` (LEFT) / `SIRET_ONUS` (anti-join) non bloquants, `RCOMP` (FROM)
  bloquant ; **LEFT forçant** bloquant (synthétique + `RESEAU` c1) ; anti-joins élargis
  (`<> ALL`, `NOT(Paren(In))`, `NOT EXISTS`) non bloquants.
- Isolation : `sub_sql` exécutable sur DuckDB ; closure exacte sur c1 ; `KeyError` si inconnu.
- Réduction : `focus_used_columns` ⊊ complet ; **table LEFT-optionnelle exclue** de la
  génération mais **conservée dans le `WITH`** (validité SQL → NULLs) ; table LEFT forçante
  conservée ; arête optionnelle absente du graphe requis.

**À écrire (étapes suivantes)** :
- `test_anchor_extraction` : valeurs des colonnes contraintes (regex/date/format) extraites ;
  clés de jointure libres exclues (R2bis).
- `test_combinator_pins_anchors` : régénération few-shot du périmètre élargi, **aucune ligne
  fusionnée**.
- `test_focused_loop_terminates` : abandon anticipé sur conflit insatisfiable (c1).
- Régression : chemin normal (requête qui converge en 1 passe) inchangé.

## 12. Phasage
- **Phase 0 (FAIT)** : briques pures `cte_graph.py` — graphes structurel & requis,
  classification bloquante (LEFT forçant + anti-joins élargis + atteignabilité), isolation
  (`build_isolated_sql`, `reduce_used_columns`, `isolate_cte`). 25 tests. Non câblé.
- **Phase 1 (MVP)** : focused_gen + extraction d'ancres + une régénération informée + re-run,
  une CTE, séquentiel, **câblage LangGraph**. Cible : faire converger une requête « vide à
  cause d'UNE condition locale dure » (regex/date) en 1-2 cycles focalisés.
- **Phase 2** : itération DAG multi-CTE avec élargissement progressif + abandon anticipé.
- **Phase 3** : parallélisation des CTEs indépendantes, peuplement de `query_decomposed.deps`.
```
