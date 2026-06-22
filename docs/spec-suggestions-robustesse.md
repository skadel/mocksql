# Spec — Robustesse des suggestions & du nommage de branches

> Issue de la revue du prompt de génération de suggestions et du prompt de nommage des
> branches `UNION ALL` (cf. `build_query/suggestions_node.py`, `build_query/prompt_tools.py`).

## Récap des faiblesses

| #  | Faiblesse                                                          | Gravité | Statut          |
| -- | ----------------------------------------------------------------- | ------- | --------------- |
| W1 | `instruction_block` : `{}` non interpolé → consignes perdues       | Moyenne | ✅ Corrigé (+ régression) |
| W6 | Le profil injecté fuit des tables d'autres projets                | **P0**  | ✅ Corrigé (a) + investigué (b) |
| W5 | Labels de branches tous identiques                                | P1      | ✅ Corrigé (+ régression) |
| W4 | Nommage des branches ré-appelé à chaque génération                | P1      | ✅ Corrigé (+ régression) |
| W2/W3 | SQL verbeux + règle « zéro jargon » mal calibrée               | P3      | Optionnel       |

---

## W1 — `instruction_block` (corrigé)

**Bug** : `instruction_block` portait un littéral `<instructions_specifiques>\n{}\n</instructions_specifiques>`.
Cette valeur étant passée comme kwarg à `.format()`, le `{}` n'était jamais réinterpolé → le
contenu réel de `agent_tool_args["instructions"]` était silencieusement perdu.

**Fix** : f-string `f"<instructions_specifiques>\n{instructions}\n</instructions_specifiques>"`.

**Régression** : `tests/test_suggestions_prompt_wiring.py`
- `test_instructions_specifiques_injected_when_present`
- `test_instructions_specifiques_absent_when_empty`

---

## P0 — W6 : fuite du profil entre projets

**Symptôme** : le prompt de nommage injecte des tables sans rapport avec la requête analysée
(`quant_proteome_cptac_ccrcc`, `rnaseq_hg38_gdc`, `creprepai_5`) sur une requête bancaire.

**Cause-racine (confirmée, double)** :
1. **Filtre cassé dans `_format_profile_block`** (`build_query/prompt_tools.py:267-268`) : une table
   n'est ignorée que si elle n'a *ni* colonne demandée *ni* `derived_expressions`. Conséquence :
   **toute table ayant des `derived_expressions` est émise, quel que soit `used_columns`**. Ça
   contredit le docstring (« Only includes columns actually used »). Les entrées fuitées sont
   précisément des `expr` (derived_expressions).
2. **Le profil lui-même contient des tables étrangères** : `.mocksql/profile.json` agrège des tables
   d'autres projets — c'est le vrai trou d'isolation PII (le profil = PII, jamais censé déborder).

**Changements** :
- (a) Dans `_format_profile_block`, filtrer aussi les `derived_expressions` par les tables présentes
  dans `used_columns` — une table sans colonne demandée ne doit rien émettre, expr comprise.
- (b) Investiguer le remplissage de `profile.json` : comment des tables étrangères atterrissent dans
  le profil ? Vérifier que `get/save_profile` est bien clé par projet/session et qu'aucune fusion
  globale n'a lieu.

**Validation** : test unitaire `_format_profile_block(profile_avec_tables_étrangères,
used_columns=[table_pertinente])` → la sortie ne contient **aucune** table hors `used_columns`,
derived_expressions incluses.

### Statut

- **(a) — Corrigé.** Garde au niveau table dans `_format_profile_block` (`prompt_tools.py`) :
  toute table absente de `used_columns` est ignorée, derived_expressions comprises.
  Régression : `tests/test_partition_window_prompt.py::TestProfileBlockCrossProjectLeak`
  (vérifié en échec sur le code buggé, puis vert).
- **(b) — Investigué, pas un bug produit.** Le profil est **délibérément partagé entre tous les
  modèles** d'un projet (`profile_checker.py:31`, « shared across all models ») et `_merge_profiles`
  (`profile_checker.py:119`) accumule les nouvelles tables. Dans un vrai projet (un entrepôt, un
  domaine), c'est voulu. Le mélange inter-domaines observé (génomique + 2 banques) est un **artefact
  d'éval** : des sous-projets hétérogènes écrivant dans un seul `.mocksql/profile.json` via un
  `MOCKSQL_BASE_DIR`/`PROFILE_CACHE_PATH` partagé. Le fix (a) neutralise toute fuite indépendamment
  de la contamination du profil. Recommandation : isoler le base dir par sous-projet côté harness
  d'éval (pas de changement produit requis).

---

## P1 — W5 : labels de branches identiques

**Cause-racine** : `sliced_sql` est tronqué en **tête** à 2500 car (`suggestions_node.py:306`). Or les
branches d'un `UNION ALL` partagent un long préfixe CTE commun (>2500) et ne diffèrent que par leur
**SELECT final** (`'nb_ope' AS indicator` + l'array `*_lags` déplié). La tête tronquée ne montre que
le tronc commun → LLM aveugle au discriminant → labels identiques.

**Changements** :
- Envoyer au labeleur la **partie discriminante** de chaque branche (son SELECT final), pas le
  préfixe commun. Soit tronquer par la **fin**, soit isoler le dernier SELECT du `sliced_sql`.
- **Filet déterministe** : pour ce pattern (unpivot d'indicateurs), le discriminant est le littéral
  `indicator` (`nb_ope`, `ouvertures`…) — il mappe directement à un label métier (« les anomalies du
  nombre d'opérations », « …des ouvertures de compte »). Si après LLM des labels collisionnent
  encore, désambiguïser via ce token extrait de `path_plans`.

**Validation** : sur le SQL de référence (6 branches), les 6 labels doivent être **distincts**.

### Statut

- **Corrigé.** `_branch_discriminant_sql` (`suggestions_node.py`) retire le `WITH` du `sliced_sql`
  pour exposer le SELECT final de la branche (qui porte le littéral `'<indicator>' AS indicator`),
  puis borne par la fin — au lieu de tronquer le SQL complet en tête. Filet déterministe
  `_disambiguate_branch_labels` : sur collision, suffixe le label avec le littéral `indicator`
  (`_branch_indicator`), à défaut le nom machine humanisé, à défaut un indice → distinction
  garantie, idempotente. Régression : `tests/test_suggestions_branch_labels.py`
  (`test_label_branches_sends_discriminant_not_common_prefix`,
  `test_disambiguate_branch_labels_yields_distinct_labels`,
  `test_disambiguate_branch_labels_is_idempotent_on_distinct_input`,
  `test_generate_suggestions_persists_distinct_labels_on_collision`).

---

## P1 — W4 : nommage une seule fois

**Cause-racine** : `_label_branches` (appel LLM) tourne à chaque `generate_suggestions`
(`suggestions_node.py:853-856`) sans relire les labels déjà persistés dans `plans[name]["label"]`.
À SQL constant, `path_plans` est rechargé **avec** labels (`query_chain.py:248`) ; à SQL changé, il
est reconstruit **sans** labels (`validator.py:166`) — le re-label automatique est donc déjà géré, il
manque juste le court-circuit côté lecture.

**Changement** : amorcer `branch_labels` depuis les labels stockés ; n'appeler `_label_branches` que
pour les branches sans label ; zéro appel LLM si toutes en ont un.

**Validation** : 2ᵉ appel à `generate_suggestions` (SQL inchangé, labels en cache) → `_label_branches`
n'est pas appelé.

### Statut

- **Corrigé.** `generate_suggestions` amorce `branch_labels` depuis les `plans[name]["label"]`
  persistés et n'appelle `_label_branches` que pour les branches sans label (`to_label`) ; zéro
  appel LLM si toutes en ont un. La persistance n'écrit `path_plans` que si le set de labels a
  changé (pas d'écriture inutile en cache). Régression :
  `tests/test_suggestions_branch_labels.py` (`test_label_branches_skipped_when_all_branches_cached`,
  `test_label_branches_called_only_for_unlabeled`).

---

## P3 — W2/W3 : qualité du prompt de suggestions (optionnel)

- **W2** : le SQL brut (passthrough `x AS x` × 40 cols × 8 CTEs) noie le prompt alors que
  `sql_digest` porte déjà la structure. Piste : compacter les projections passthrough avant injection.
- **W3** : la règle « zéro jargon » est trop rigide pour des requêtes statistiques (z-score,
  winsorization) ; les few-shot sont tous des cas BI additifs simples. Piste : ajouter un exemple
  « bon » analytique (anomalie qui en masque une autre via le baseline) et tolérer un terme technique
  s'il est indispensable au sens.

---

## Séquencement recommandé

1. **W6** (P0, isolation/PII — le plus risqué).
2. **W4 + W5** ensemble (même fonction `_label_branches` ; le run-once est trivial, la troncature est
   le gros morceau).
3. **W2/W3** si on veut pousser la qualité.

Méthode par défaut sur chaque item : test de non-régression d'abord (vérifié en échec), puis fix.
