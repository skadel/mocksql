# Spécifications — Robustesse de la génération de données de test

Suite de l'audit du prompt générateur (juin 2026) mené sur la requête bancaire
« mouvements de regroupement » (~15 CTE, BPCE). Le constat : le test généré
retournait un résultat **vide** tout en se décrivant comme une « ouverture »,
à cause de trois familles de problèmes — contradictions internes du prompt,
contraintes extraites corrompues, et inversion mentale de transformations
demandée au LLM.

## Déjà livré (référence)

| Chantier | Où |
|---|---|
| Alignement des 3 voix (`unit_test_description`, reasoning-first, `test_name` documenté) | `prompt_tools.py:518-573`, `examples_generator.py:1080-1122`, `<task>` `prompt_tools.py:635-649` |
| Règles « clés de jointure dérivées » + « bornes temporelles depuis les littéraux SQL » | SYSTEM, consignes 4 et 6 |
| Filtre tautologies pré-résolution (`X = X`, `X <> X` littéraux) | `constraint_simplifier.py:1260`, branché `_serialize_cond:1508` |
| Collapse post-résolution → forme brute alias-qualifiée conservée | `_resolve_pred_node` (`constraint_simplifier.py:1304-1315`), tests `tests/simplifier/test_tautology_filter.py` |
| **P1b** — `is_identity` sur `ColumnRef` : stop remap des prédicats sur colonnes dérivées (conditions + descriptions Pydantic + Faker) | `_resolve_via_lineage` / `_identity_or_raw` (`constraint_simplifier.py`), `_all_refs_resolved` / `_compute_faker_columns` (`examples_generator.py`), tests `tests/simplifier/test_derived_predicate_no_remap.py` |
| **P1a** — recettes de jointure (CASE énuméré, vérif forward DuckDB, format CAST, fallback) injectées générateur + agent | `build_query/join_recipes.py`, injection `prompt_tools.py` (`constraints_inner`) + SYSTEM `conversational_agent.py`, tests `tests/simplifier/test_join_recipes.py` |
| **P1c** — breakdown par prédicat de JOIN + erreurs CTE complètes (message + SQL) | `_run_join_predicate_breakdown` (`examples_executor.py`), rendu `_format_cte_trace_hint`, tests `tests/test_join_predicate_breakdown.py` |
| **P1c** — garde anti-no-op + mémoire des tentatives (ledger `QueryState.correction_attempts`, conversation alternée, rejet sans consommer de retry) | `conversational_agent.py` (`_noop_batch_reason`, `_render_attempt_messages`), `data_patcher.py` (`append_correction_attempt`), `query_chain.py` (`_bad_data_to_agent`), reset `utils/saver.py`, tests `tests/test_correction_ledger.py` |
| **P3.1** — clé `anti_joins` émise systématiquement (liste vide incluse) | `build_conditions_hint`, tests `tests/simplifier/test_anti_join.py` |
| **P3.3** — garde collapse documenté comme filet de sécurité post-P1b | commentaire `_resolve_pred_node` |

Les chantiers restants, par ordre de priorité : **P0** (éval A/B via `/eval-mocksql` — runs LLM, à lancer manuellement), puis **P2a + P2b** (décision sur les chiffres de l'éval), **P3.2** (vérif du contrat sortie attendue).

---

## P0 — Éval A/B : mesurer la réécriture avant d'empiler

### Objectif
Établir la baseline chiffrée des changements déjà livrés, et le harnais de
mesure des chantiers P1. Aucun chantier P1 ne démarre sans cette baseline.

### Implémentation
Harnais existant : `examples/eval/` (`generate_tests.py`, `run_eval.py`,
`judge.py`, `compare.py`), piloté par `/eval-mocksql`.

- **Variantes comparées** : A = commit antérieur à la réécriture du prompt ;
  B = HEAD actuel (prompt réécrit + filtre tautologies + fix collapse).
- **Corpus** : le sous-ensemble spider habituel **+ la requête bancaire de
  l'audit comme fixture dédiée** (elle concentre tous les pièges : clés
  dérivées, bornes M/M-1, lineage à travers agrégats).
- **Métriques**, par variante :
  1. Taux de résultat non vide au 1ᵉʳ coup (avant toute boucle de retry) — métrique principale.
  2. Nombre moyen de tours `bad_data` consommés.
  3. Distribution des verdicts (`Excellent`/`Bon`/`Insuffisant`).
  4. `is_valid` du juge (holistique — attention : la lisibilité y pèse, cf. éval spider passée).

### Critères d'acceptation
- Rapport A/B archivé (sortie de `compare.py`) avec les 4 métriques.
- B ≥ A sur la métrique 1 ; toute régression sur 3/4 est analysée avant de poursuivre.

### Taille estimée
Aucun code neuf (hors ajout de la fixture bancaire au corpus). Coût = runs LLM.

---

## P1a — Recettes de jointure pré-calculées (clés dérivées)

### Problème
Quand une clé de JOIN est le produit d'une transformation
(`CASE`, `SUBSTR`+`SPLIT`, `CAST`), le prompt demande aujourd'hui au LLM
d'**inverser mentalement** la transformation (consigne 4 du SYSTEM). C'est la
cause directe des trois échecs de jointure de l'audit :
`cd_chef_file` attendu `'1'` (généré `'BP'`), `cd_type_carte_smp` attendu
`'ROD'` après SUBSTR (généré `'PROD1'`), `filtre_didd` `'DD'→'D'` jamais matché.
Ces transformations sont déterministes : l'inversion doit être **pré-calculée
côté Python** et injectée comme consigne concrète.

### Comportement attendu
Un nouveau bloc dans `<constraints>` :

```
**Recettes de jointure (clés dérivées) — appliquer telles quelles :**
- JOIN corr_cartes ↔ ref_port sur cd_chef_file : la clé est dérivée par
  CASE(reseau). Pour matcher : reseau='BP' → poser ref_port.cd_chef_file='1' ;
  reseau='CE' → poser '2'.
- JOIN sur code_produit_bpce_ps : la colonne source subit
  SUBSTR(col,2,LEN-2) puis SPLIT(',') puis TRIM('"'). Pour que la valeur
  dépliée soit X, écrire correspondance_cartes.code_produit_bpce_ps = "'X'"
  (vérifié : "'PROD1'" → PROD1).
- JOIN sur filtre_didd : dérivée par CASE(filtre_didd) : 'DD'→'D', 'DI'→'I',
  sinon NULL. La condition OR filtre_didd IS NULL est satisfaite avec une
  valeur hors {'DD','DI'}.
```

### Implémentation
- **Nouveau module** `back/build_query/join_recipes.py` (ne pas grossir
  `constraint_simplifier.py`, déjà ~2 800 lignes), réutilisant `_LineageResolver`
  et l'`alias_map` via le hook privé déjà prévu (`_statement`/`_resolver` de
  `build_conditions_hint`, `constraint_simplifier.py:1954`).
- **API** : `build_join_recipes(sql: str, dialect: str, schema: list[dict] | None) -> list[str]`
  — une recette par clé de JOIN dérivée, chaînes prêtes à injecter.
- **Détection** : pour chaque prédicat d'égalité de `JOIN ON`, résoudre le
  lineage de chaque côté ; si l'expression de lineage n'est pas une colonne nue
  (contient `CASE` / `Substring` / `Split` / `Trim` / `Cast` / `RegexpReplace` /
  `Concat`), produire une recette.
- **Règles d'inversion** (table extensible, une fonction par nœud sqlglot) :

  | Transformation | Recette |
  |---|---|
  | `CASE WHEN col = lit THEN lit2 …` (branches 100 % littérales) | Énumérer les couples (valeur source → valeur clé). Branches non littérales → recette générique (voir fallback) |
  | `SUBSTR` / `TRIM` / `REGEXP_REPLACE` / `SPLIT` (chaîne de fonctions sur une colonne) | Pas d'inversion symbolique : **vérification forward via DuckDB** (ci-dessous) avec gabarit d'entrée proposé |
  | `CAST` / `SAFE_CAST` | Contrainte de format des deux côtés (« valeur numérique en chaîne, identique des deux côtés ») |
  | `CONCAT(col, lit…)` | Décomposer si tous les autres opérandes sont littéraux |

- **Vérification forward DuckDB** (le point clé pour les chaînes de fonctions) :
  plutôt que d'inverser symboliquement, générer un gabarit candidat (ex. `"'X'"`),
  exécuter `SELECT <expr avec candidat>` sur DuckDB local, vérifier que la sortie
  vaut la valeur cible, et inclure le couple vérifié dans la recette
  (« vérifié : "'PROD1'" → PROD1 »). Coût : millisecondes, zéro LLM. Si la
  vérification échoue → fallback.
- **Fallback non-inversible** : recette générique « cette clé est dérivée par
  `<expr>` ; choisis la valeur source telle que `<expr>` = valeur de l'autre
  côté » — jamais pire que la consigne prose actuelle.
- **Injection — deux surfaces** :
  1. Générateur : `join_recipes_block` ajouté à `constraints_inner`
     (`prompt_tools.py:620-622`), après `constraints_block`. Mise en cache par
     `(sql, dialect)` comme le hint existant (`examples_generator.py:195-213`).
  2. **Agent conversationnel (boucle `bad_data`)** : même bloc dans le SYSTEM de
     `conversational_agent`. Validé sur incident (run du 2026-06-11) : l'agent
     avait le `run_cte` montrant `code_produit_bpce_ps = ROD` face à des données
     `PROD1`, et sans la recette SUBSTR il a interprété l'écart comme une
     inversion de lignes (patch no-op `PROD1↔PROD2`) jusqu'à épuisement des
     retries. C'est la surface où la recette est la plus rentable : un patch
     d'un champ au lieu d'une régénération.

### Critères d'acceptation
- Tests unitaires `tests/simplifier/test_join_recipes.py` : cas CASE littéral,
  chaîne SUBSTR+SPLIT+TRIM (vérif DuckDB), SAFE_CAST, cas non-inversible → fallback,
  JOIN sur colonnes nues → aucune recette (pas de bruit).
- Sur la requête bancaire de l'audit : les 3 recettes ci-dessus sont produites.
- Éval P0 rejouée : amélioration du taux non-vide 1ᵉʳ coup sur les requêtes à clés dérivées.

### Risques
- Sur-injection (recettes sur des JOINs triviaux) → diluerait le prompt :
  ne produire une recette QUE si le lineage contient une vraie transformation.
- Expressions BigQuery non supportées par DuckDB dans la vérif forward →
  try/except, fallback générique.

### Taille estimée
Le plus gros chantier : ~300-400 lignes + tests.

---

## P1b — Prédicats sur colonnes dérivées : stopper le remap vers la colonne de base

### Problème (mécanisme identifié)
`_resolve_via_lineage` (`constraint_simplifier.py:602-632`) parcourt l'arbre de
lineage et retient la **dernière feuille `Table`** rencontrée comme
`(base_table, base_col)`. Pour une colonne **dérivée** — ex. `typ_client =
CASE WHEN SUM(nb_contrats_new) > 0 …` où `nb_contrats_new = COUNT(no_carte)` —
la marche descend à travers le CASE et l'agrégat jusqu'à `ds_ref_porteur.no_carte`.
Le prédicat `temp_view_client.typ_client = 'OUVERTURE'` est alors rendu
`ds_ref_porteur.no_carte = 'OUVERTURE'` : **faux et activement nuisible**
(le garde anti-aggrégat ligne 619 ne protège que `lineage_sql`, pas
`base_table`/`base_col`).

Ce remap pollue deux surfaces : la chaîne `conditions` du hint, et les
**descriptions de champs Pydantic** (« ATTENTION contrainte SQL :
ds_ref_porteur.no_carte = 'OUVERTURE' ») — l'endroit où le LLM obéit le plus.

### Comportement attendu
1. Quand la résolution traverse une dérivation **non-identité** (agrégat, CASE,
   arithmétique, concat — tout sauf renommage/alias de colonne nue), le prédicat
   garde sa **forme CTE-qualifiée d'origine** : `temp_view_client.typ_client = 'OUVERTURE'`.
   Le détail de la dérivation reste disponible dans `lineages` (déjà le cas).
2. Aucune description de champ Pydantic n'est dérivée d'un prédicat dont la
   résolution est non-identité.

### Implémentation
- Ajouter à `ColumnRef` un indicateur `is_identity: bool` (True si la chaîne de
  lineage est un pur renommage : chaque nœud non-Table est une colonne nue ou
  un alias de colonne nue). Calculé dans `_resolve_via_lineage` pendant la
  marche existante — coût nul.
- `_resolve_pred_node._transform_fn` (`constraint_simplifier.py:1289-1302`) :
  si `resolved.is_identity` est False → retourner le nœud **original** (ne pas
  substituer). Cohérent avec le fix « collapse » déjà livré, qui devient un
  filet de sécurité.
- Collecteurs d'égalités / contraintes fonctionnelles / littéraux
  (`constraint_simplifier.py:906-1180`) : ne pousser une contrainte rattachée à
  une colonne de base que si `is_identity`. Sinon, soit dropper (descriptions
  Pydantic), soit garder la forme CTE (chaîne `conditions`).
- **Méthode** : test de régression d'abord (fixture = mini-pattern
  `CASE WHEN SUM(COUNT(col)) … = 'LITERAL'`, assert : le hint ne contient pas
  `col = 'LITERAL'` remappé), vérifié en échec, puis fix, puis **grep systémique**
  des autres consommateurs de `resolver.resolve(...)` (~25 sites, lignes
  750-1180) pour décider site par site identité requise ou non.

### Critères d'acceptation
- Sur la requête bancaire : plus aucun `no_carte = 'OUVERTURE'` / `'FERMETURE'` / `= 1`
  ni dans `conditions` ni dans les descriptions Pydantic ; la forme
  `temp_view_client.typ_client = 'OUVERTURE'` y figure à la place.
- Suite `tests/simplifier` intégralement verte (les 277 tests existants
  encadrent les régressions de résolution).

### Risques
- Trop strict (identité mal détectée → on garde des formes CTE partout et le
  hint perd ses ancres base-table). Calibrer `is_identity` sur les fixtures
  existantes de `tests/simplifier`.

### Taille estimée
~100-150 lignes + tests. À faire **avant ou avec** P1a (les recettes
s'appuient sur le même indicateur d'identité).

---

## P1c — Boucle de correction : diagnostic par prédicat, erreurs visibles, garde no-op, mémoire des tentatives

### Problème (incident du 2026-06-11, requête bancaire)
La boucle `bad_data` a épuisé ses retries alors qu'un **unique patch** suffisait
(`cd_type_carte_smp → "ROD"` ou `code_produit_bpce_ps → "'PROD1'"`). Quatre
causes, indépendantes des recettes P1a :

1. **Étiquette de jointure trompeuse** : la trace CTE affichait
   `+ JOIN (corr_cartes.cd_chef_file IS NOT NULL) → 0 ligne(s)` alors que le
   prédicat réellement bloquant était l'égalité sur `code_produit_bpce_ps`.
   L'agent du round précédent a patché `cd_chef_file` — la mauvaise colonne,
   désignée par le diagnostic lui-même.
2. **Erreurs CTE avalées** : `photo_m : erreur d'exécution` sans message ni SQL
   (contraire à la règle de logging du projet). Impossible de distinguer un
   problème de types (comparaison `'2026-02-01 00:00:00'` sur colonne DATE)
   d'une simple conséquence du 0-ligne amont.
3. **Patch no-op non détecté** : l'échange `PROD1↔PROD2` entre deux lignes
   (identiques après SUBSTR) a consommé un round complet executor+evaluator
   sans pouvoir changer le résultat.
4. **Amnésie inter-rounds** : l'historique passé à l'agent
   (`conversational_agent.py:390-399`, filtre `QUERY/OTHER/RESULTS/EXAMPLES/
   DEBUG_RUN_CTE`) ne contient **jamais** les appels d'outils des rounds
   précédents, et `eval_context` ne montre que l'état courant des données, pas
   le delta. Le round 2 ne sait pas que le round 1 a déjà patché
   `cd_chef_file` sans effet — il repart de zéro et peut répéter ou défaire
   une tentative.

### Comportement attendu
1. **Breakdown par prédicat** : quand une étape bloquante est un JOIN, le
   diagnostic évalue chaque prédicat du `ON` **indépendamment** sur les données
   réelles (requêtes DuckDB triviales) et nomme le prédicat fautif avec les
   ensembles de valeurs des deux côtés :
   ```
   JOIN corr_cartes ↔ ref_port — décomposition :
     cd_chef_file = cd_chef_file      → 1/2 lignes (row 1 : 'D' ≠ '1')
     code_produit = cd_type_carte_smp → 0 match — gauche {ROD}, droite {PROD1, PROD2} ← BLOQUANT
     filtre_didd … OR IS NULL         → 2/2 (NULL)
   ```
2. **Erreurs CTE complètes** : toute « erreur d'exécution » dans la trace porte
   le message DuckDB et la requête de l'étape (règle de logging existante).
3. **Garde anti-no-op** : avant ré-exécution, vérifier que le lot de patches
   modifie l'ensemble des valeurs d'au moins une colonne impliquée dans l'étape
   bloquante (au minimum : que le multiset de valeurs d'une colonne patchée a
   changé). Sinon, renvoyer le lot à l'agent avec le motif, **sans décrémenter
   `gen_retries`** ni relancer l'executor.
4. **Mémoire des tentatives** : chaque round de correction voit les tentatives
   précédentes et leur résultat, rendus en **conversation alternée** (le format
   naturel d'un agent à outils) entre le SYSTEM et le trigger courant :
   ```
   AI    : Tentative 1 — patch_test_field DS_REF_PORTEUR[0].cd_chef_file = "1"
   HUMAN : Résultat tentative 1 : toujours 0 ligne — étape bloquante inchangée
           (temp_carte, JOIN corr_cartes). Ne répète pas une tentative équivalente.
   AI    : Tentative 2 — patch_test_field correspondance_cartes[0/1].code_produit (swap)
   HUMAN : Résultat tentative 2 : toujours 0 ligne — étape bloquante inchangée. …
   ```
   L'agent peut ainsi raisonner « ce levier a déjà été actionné sans effet →
   le bloqueur est ailleurs », au lieu de redécouvrir le problème à chaque round.

### Implémentation
- Breakdown : dans `examples_executor.py` (construction de la trace CTE) —
  à l'endroit où le JOIN est déjà décomposé (l'étiquette `+ JOIN (...)`
  actuelle). Pour chaque conjonction du `ON` : `SELECT COUNT(*)` du semi-join
  sur ce seul prédicat + `SELECT DISTINCT` (plafonné à ~5 valeurs) de chaque côté.
- Erreurs : même fichier, propager `str(exc)` + SQL de l'étape dans le bloc
  diagnostic transmis à l'agent.
- Garde no-op : dans le nœud qui applique les patches (`data_patcher` /
  `conversational_agent`), comparaison avant/après des colonnes touchées.
- Mémoire des tentatives :
  - Nouveau champ `QueryState.correction_attempts: list[dict]` (déclaré dans
    `state.py` d'abord, convention projet). Une entrée par round :
    `{round, test_uid, ops: [{tool, table, row_index, field, value}…],
    outcome: {rows, blocking_cte, digest}}`.
  - Écriture en deux temps : `data_patcher_node` (`data_patcher.py:68`) appende
    l'entrée avec les `ops` du lot (`agent_tool_args["calls"]`) ; les chemins
    `update_test_data`/generator appendent une entrée `{tool: "regen"}`. Le
    `outcome` est complété quand la boucle re-rentre par `bad_data_to_agent`,
    à partir du diagnostic frais (une ligne : étape bloquante + nb de lignes).
  - Rendu : dans `conversational_agent`, synthétiser les paires
    AIMessage/HumanMessage depuis le ledger (pas de persistance de vrais
    messages LangChain — le ledger est la source, le rendu est reconstruit
    à chaque round). Insérées avant le trigger `auto_correct`.
  - Budget : digest d'une ligne par outcome (pas la trace complète) ; le ledger
    est borné par `gen_retries` (≤ 3 rounds). Reset : test passé, nouvelle
    génération, ou sortie de la boucle (`bad_data_exhausted`).
  - Synergie avec la garde no-op : la garde compare aussi le nouveau lot au
    ledger — un lot identique à une tentative passée est rejeté avec le motif
    « déjà tenté au round N, sans effet ».

### Critères d'acceptation
- Rejouer le scénario de l'incident : le diagnostic nomme
  `code_produit = cd_type_carte_smp` comme prédicat bloquant avec
  `{ROD}` vs `{PROD1, PROD2}` ; le patch no-op `PROD1↔PROD2` est rejeté sans
  consommer de retry.
- Les « erreur d'exécution » de la trace portent le message DuckDB.
- Au round 2, le prompt de l'agent contient la tentative 1 (ops + outcome) ;
  un lot identique à la tentative 1 est rejeté sans consommer de retry.

### Taille estimée
~250-350 lignes + tests (breakdown ~150, mémoire des tentatives ~100-150).
**Indépendant de P1a/P1b** — peut être mené en parallèle ; c'est le chantier au
meilleur ratio (il rend chaque retry utile, quel que soit le niveau du LLM).
La mémoire des tentatives et la garde no-op partagent le même ledger :
les implémenter ensemble.

---

## P2a — Exemple few-shot « clé dérivée + bornes M/M-1 »

### Problème
Le SYSTEM mentionne que les consignes s'appliquent « y compris aux exemples »,
mais aucun exemple ne couvre le piège principal. Les règles en prose se
généralisent mal ; un exemple travaillé contrastif, très bien.

### Comportement attendu
Un exemple compact dans le few-shot du générateur (entre le message de
référence et l'ask, cf. `prompt_tools.py:651-659`) : mini-requête (~15 lignes)
avec (a) un `CASE` en clé de JOIN et (b) un filtre photo M / M-1, suivi du JSON
correct, avec en regard les deux erreurs classiques annotées ✗ (valeur finale
au lieu de la valeur source ; dates satisfaisant les deux photos).

### Critères d'acceptation
- L'exemple passe la vérification forward DuckDB (l'exécuter une fois à la main).
- Éval P0 rejouée sans régression de latence notable (l'exemple ajoute ~600 tokens).

### Taille estimée
Petite (~1 h), mais à faire **après** P1a — si les recettes suffisent,
l'exemple est peut-être superflu : décider sur les chiffres de l'éval.

---

## P2b — Dégraissage du prompt

### Problème
Redondances qui diluent l'attention : la casse des tables est répétée 3 fois
(consigne 12, double ⚠️ `prompt_tools.py:571-572`, en-tête `<schema>` ligne
605-606) ; le `<task>` (lignes 635-649) re-paraphrase 6 consignes du SYSTEM ;
le bloc `conditions` peut dépasser 4 000 caractères sur les grosses requêtes.

### Comportement attendu
- Une seule occurrence de la règle de casse (la consigne 12 ; l'en-tête
  `<schema>` garde le rappel court « casse stricte » car il est collé aux clés).
- `<task>` réduit à : ce qui est volatil (instruction utilisateur, contrainte
  non-vide), le pointeur vers les consignes système, et `format_instructions`.
  Supprimer les redites (branches OR, casse, agrégats purs).
- Tronquer `conditions` au-delà d'un budget (~2 000 caractères) avec mention
  « … (tronqué — voir <query>) » : au-delà, le LLM ne lit plus, et `<query>`
  fait autorité.

### Critères d'acceptation
- Éval P0 rejouée : non-régression sur les 4 métriques (c'est un changement
  à risque nul en théorie, mais on vérifie — c'est le but du harnais).

### Taille estimée
Petite (~1-2 h). À grouper avec P2a dans un même run d'éval.

---

## P3 — Hygiène de contrat (vrac)

1. **`anti_joins` fantôme** : le SYSTEM documente la clé `anti_joins` de
   `<constraints>` mais `build_conditions_hint` ne l'émet pas toujours.
   Soit l'émettre systématiquement (liste vide incluse), soit conditionner la
   mention dans le SYSTEM à sa présence. Vérifier `_collect_anti_joins`
   (`constraint_simplifier.py:1343`).
2. **Champ sortie attendue** : le schéma de génération ne porte ni colonne
   cible ni valeurs attendues, alors que le produit définit un test comme
   « données d'entrée + sortie attendue ». Vérifier que la chaîne d'assertions
   (commit `7813192`, assertions sur valeurs concrètes) couvre bien ce contrat
   en aval ; sinon, spécifier l'ajout.
3. **Garde `_serialize_cond` post-résolution** : une fois P1b livré, vérifier
   si le fix « collapse » de `_resolve_pred_node` reste atteignable (il devrait
   devenir un filet de sécurité) — ne PAS le retirer, le documenter comme tel.

---

## Hors périmètre (décidé, ne pas rouvrir sans signal)

- **Génération en deux passes** (plan structuré puis matérialisation) :
  re-évaluer seulement si, après P1a+P1b, le taux non-vide 1ᵉʳ coup reste < 80 %
  sur les requêtes profondes. Coût : +1 appel LLM par génération.
- **Heuristique de filtrage `no_carte = 'OUVERTURE'`** : abandonnée — P1b
  corrige la cause racine.
- **Reasoning 1 phrase en mode non-native** : refusé — sans thinking natif,
  le champ est le seul canal de CoT (3 phrases conservées,
  `examples_generator.py:1065-1078`).

## Ordre d'exécution recommandé

```
P0 (baseline) → P1b (identité lineage) → P1a (recettes, s'appuie sur P1b)
                                       ↘ P1c (boucle de correction, indépendant — parallélisable)
→ éval → P2a + P2b (groupés) → éval → P3 au fil de l'eau
```
