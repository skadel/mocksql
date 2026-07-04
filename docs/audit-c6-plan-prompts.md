# Audit prompts LLM — run de génération `c6.sql`

> Plan priorisé issu de l'analyse des 6 appels LLM d'une génération complète sur `c6.sql`
> (requête CUBE(6) × UNION ALL(6) × UNNEST(14) → résultat de 1344 lignes).
> Aucun code n'a été modifié : ce document est le plan avant intervention.
> Références de ligne valables au commit `68db87d` (branche `claude/trino-dialect-support`).

## TL;DR

Le **générateur de données est le point fort** du run : il déjoue correctement le piège
winsorisation / z-score (transformations appliquées à la stat mais pas au numérateur →
z-score ≈ 8,23, détection déclenchée du premier coup). Les deux vrais problèmes sont **en
aval**, et forment une seule chaîne causale :

```
round-trip JSON du résultat  →  result_schema menteur (dates→VARCHAR, floats→int64)
   →  5 assertions écrites sur `partition_date = '2024-03-01'`  →  0 match
   →  1 assertion vacante + 4 échecs  →  2 appels de rattrapage
   →  résultat complet (1344 lignes) réinjecté 2× à ~963K tokens  →  429 Vertex (quota TPM)
```

`P0-1` + `P0-2` cassent toute la chaîne.

## Lecture des 6 appels

| # | Nœud | in / out | Latence | Constat |
|---|------|----------|---------|---------|
| 1 | `generator` | 20,8K / 19,1K | 83 s | **OK.** Piège z-score déjoué. Consignes structurelles (CUBE → pas de dimension superflue, fenêtre → compter les PRECEDING) ont porté. |
| 2 | `assertion_generator` (gen+éval fusionnés) | **963 836** / — | 62 s | `result_sample` = **les 1344 lignes complètes** (1,77 Mo sur une ligne) alors que le system prompt annonce « nombre de lignes + exemples ». 5 assertions : 1 forte, 3 redondantes (même valeur ROUND(…,9)=8.232225317 sur 3 colonnes corrélées), 1 en forme négative (`IS NULL`) malgré l'interdit du prompt. |
| 3 | réécriture assertion vacante | 25K | — | L'assertion `IS NULL` scopée `partition_date = '2024-03-01'` était vacante : la colonne est un VARCHAR `'2024-03-01T00:00:00'`. Prompt sans échantillon → le fixer a deviné (`CAST(... AS DATE)`, qui a marché). |
| 4 | fixer des 4 assertions restantes | **962 774** / — | 158 s | Trou ~2 min avant l'appel = retry post-429. Les 4 `exists` échouaient pour la **même cause unique** (date VARCHAR) mais le prompt affiche « Lignes remontées : [] » (signal vide, inévitable pour un échec `exists`). Réécrit en `LEFT(partition_date, 10)` + remplace le pin 9-décimales par `> 1.2` → plus aucune assertion ne fige la valeur calculée. |
| 5 | `suggestions_generator` | 13,4K | — | Contexte bien construit (troncature structurée, branches UNION ALL). **1 suggestion sur 3 factuellement fausse** : « Une série parfaitement stable est signalée comme anormale » — le SQL fait l'inverse (`CASE WHEN stddev <> 0 … ELSE NULL` + sentinelles `COALESCE(±999999999)` → jamais détectée). |
| 6 | `final_response` | 421 | — | Sobre, fidèle. RAS. |

**Coût total** : ~1,93M tokens facturés pour ~25K utiles.

## Réponses aux questions posées

- **Le `result_schema` infère-t-il des types cohérents ?** Non.
  `assertion_generator.py:53` reconstruit le DataFrame via `pd.read_json(...)` sur un JSON
  sérialisé ISO → les dates deviennent des chaînes (`object` → VARCHAR dans `__result__` via
  `con.register`, ligne 86) et les mesures FLOAT64 à valeurs entières deviennent `int64`.
  Le conseil temporel du system prompt (« compare directement `date = '2026-01-01'` ») devient
  un piège — c'est lui qui a tué les 5 assertions.
- **L'assertion vacante** : cause = ce problème de type, **pas** la logique de scope.
- **Les égalités à 9 décimales** : la règle floats montre `ROUND(col, 2)` en exemple mais ne
  **borne pas** la précision → le modèle a recopié la pleine précision lue dans le sample.
  Fragile, et désynchronisé de la description (« supérieur à 1.2 »).
- **Winsorisation / z-score compris ?** Oui, remarquablement — preuve que les consignes
  chirurgicales du générateur paient.
- **Le 429** : problème de **débit** provoqué par le prompt (2 appels à ~963K tokens en <4 min
  explosent le quota TPM Vertex). Correctif principal = prompt/contexte (P0-1) ; correctif
  params (retry/backoff dans `llm_factory.py`, aujourd'hui absent) = secondaire.

---

## Plan priorisé

### P0 — à faire en premier

#### P0-1 · Plafonner structurellement `<result_sample>` (gen+éval ET fixer)

- **Problème** : `in=963 836` puis `in=962 774` ; 429 entre les deux ; 62 s + 158 s de latence.
- **Cause racine** : le commentaire « petits résultats — aucun risque de budget de prompt »
  (`examples_executor.py:1883`, `:2109`, `:2349`) est invalidé par
  CUBE(6) × UNION ALL(6) × UNNEST(14) = 16 combos × 6 branches × 14 offsets = 1344 lignes.
- **Changement** : troncature par **lignes entières** (jamais `[:N]` caractères — leçon déjà
  apprise), sur le modèle de `suggestions_node.py` (`rows[:max_rows]` + `(+N autres)`).
  Budget ~50-100 lignes + `row_count`, en priorisant les lignes qui matchent le sujet du
  `test_context` (ici les lignes non-« Global ») ; si tronqué, ajouter les **valeurs distinctes
  des colonnes discriminantes** (indicator, partition_date, dimensions) pour que le juge sache
  ce qui existe hors échantillon.
- **Impact** : −~1,9M tokens sur ce type de requête, suppression probable du 429, −2 à −3 min de
  latence, attention du juge re-concentrée.

#### P0-2 · `result_schema` véridique : types DuckDB réels + valeur d'exemple par colonne

- **Problème** : `partition_date: object` masque un VARCHAR `'2024-03-01T00:00:00'` → 4 assertions
  + 1 scope écrits en `partition_date = '2024-03-01'` → 0 match → 1 vacance + 4 échecs + 2 appels
  de rattrapage. Et `rolling_stddev_mt_ope: int64` sur une mesure flottante invite une future
  égalité stricte.
- **Cause racine** : round-trip JSON (`assertion_generator.py:53`) + schéma affiché depuis
  `result_df.dtypes` (`examples_executor.py:1879`, `:2178`, `:2345`) alors que le prompt promet
  « le schéma exact de la table `__result__` ».
- **Changement (contexte)** : construire `result_schema` par **`DESCRIBE` DuckDB après
  `con.register`** (les types que les assertions affronteront vraiment) et annoter chaque colonne
  d'une valeur d'exemple : `partition_date: VARCHAR (ex. '2024-03-01T00:00:00')`. Compléter la
  règle temporelle du system prompt : « colonne temporelle en VARCHAR ISO → `LEFT(col, 10) =
  'YYYY-MM-DD'` ou `CAST` ».
  *(Alternative moteur — restaurer les vrais dtypes à la reconstruction — notée, hors périmètre.)*
- **Impact** : élimine la classe entière d'échecs « date qui ne matche pas » (5/5 dans ce run) ;
  les appels 3 et 4 disparaissent du chemin nominal.

### P1 — fort levier, après les P0

- **P1-1 · Borner la précision des pins flottants + interdire la désync description↔condition.**
  Règle floats (`examples_executor.py:1923`) : imposer « 2 décimales, jamais plus de 3 » ; exiger
  que la condition teste ce que la description affirme (« dépasse 1.2 » → `> 1.2`, + une assertion
  séparée si on veut pincer via `ROUND(…, 2)`) ; anti-redondance : ne pas pinner la même valeur sur
  des colonnes dérivées l'une de l'autre (`zscore_max = GREATEST` d'une colonne déjà pinnée).
- **P1-2 · Issue positive aux NULLs attendus + garde structurelle à la génération initiale.**
  L'interdit du `IS NULL` (`examples_executor.py:1933`) n'offre aucun moyen d'affirmer « cette
  colonne DOIT être NULL » (fait métier légitime ici). Ajouter l'idiome positif
  `COALESCE(col, sentinelle) = sentinelle`, et appliquer `_is_valid_positive_condition`
  (`examples_executor.py:112` env.) **aussi** à `_assertion_to_executable` — aujourd'hui seule la
  voie fixer est gardée.
- **P1-3 · Donner au fixer un signal utile au lieu de `[]`.** Pour un échec `exists`/scope vacant,
  « Lignes remontées : [] » (`examples_executor.py:2441`) n'apprend rien ; injecter à la place les
  lignes satisfaisant le plus grand sous-ensemble des conjoints (réutiliser la mécanique
  d'auto-scope) — ici le conjoint fautif `partition_date` aurait sauté aux yeux. Et ajouter un
  petit `result_sample` (plafond P0-1) au prompt de régénération d'assertion, qui travaille
  aujourd'hui en aveugle.
- **P1-4 · Suggestions : exiger la dérivabilité SQL de chaque affirmation.** La formulation
  « affirmation directe, jamais "Vérifier que" » + la liste de pièges priment des symptômes
  plausibles non vérifiés → suggestion 2 factuellement fausse. Ajouter : « avant d'énoncer un
  symptôme, vérifie la clause SQL qui le produirait ; si une garde (CASE, sentinelle COALESCE,
  filtre) le neutralise, énonce le symptôme réel de cette neutralisation ou abandonne le cas ».
  *Impact produit direct : les suggestions sont l'argument différenciant de MockSQL.*

### P2 — hygiène

- **P2-1 · Contradiction latente** entre le warning STDDEV « plusieurs ENTITÉS distinctes par
  groupe » (`prompt_tools.py`) et la consigne CUBE « aucune dimension superflue » : préciser « si
  la variance vient d'une fenêtre temporelle, varie dans le TEMPS, pas en entités ». Le modèle a
  bien arbitré cette fois ; un modèle plus faible pourrait suivre le mauvais conseil et faire
  exploser le CUBE.
- **P2-2 · `<input_data>` injecté en repr Python** (guillemets simples) vs JSON partout ailleurs
  → `json.dumps` pour la cohérence.
- **P2-3 · Coût output générateur** (19K tokens, 90 lignes quasi identiques × 9 clés) inhérent au
  format long ; le levier (fill-down / faker pour séries répétitives) est côté moteur, hors
  périmètre de ce plan.

### Correctifs modèle/params (distincts, secondaires)

Configurer un retry/backoff explicite (`max_retries`) dans `llm_factory.py` et loguer un warning
DIAG quand un prompt dépasse ~200K tokens — mais le 429 disparaît de lui-même avec P0-1.
