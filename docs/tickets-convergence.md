# Tickets — Convergence vers un test *propre*

## EPIC — Convergence vers un test *propre*, sans trahir l'utilisateur

La convergence-vers-pass est résolue (dernier run fdp : 11/11 valides, `pass_rate` 1.0,
aucun `bad_data_exhausted`). Le résidu n'est plus « la boucle ne finit pas » mais : la
boucle peut converger vers un test vert mais (a) au narratif périmé, ou (b) en écrasant
silencieusement une attente que l'utilisateur a posée.

**Principe directeur transversal :** distinguer ce qui est **auto-généré** (re-sync
silencieux OK) de ce qui vient d'une **assertion utilisateur** (divergence → stop-and-ask
via `VALIDATION_PROMPT`, jamais de réécriture muette).

État de l'art existant à réutiliser :
- Desync **sortie** déjà gérée par délégation (pas de boucle) : `bad_description` /
  `needs_validation` → `history_saver` + `VALIDATION_PROMPT`
  ([query_chain.py:362-367](query_chain.py)).
- Boucle silencieuse `bad_data` (réparation de données cassées) :
  `bad_data_to_agent` → `conversational_agent` → outils de patch ciblés.
- Outils de délégation déjà disponibles à l'agent : `request_reevaluation`,
  `ask_clarification`, et le nœud `accept_validation`.

---

## TICKET-1 · `fix` · **P0 (trust-critical)** — Empêcher la boucle `bad_data` d'écraser une prémisse utilisateur en silence

**User story**
> En tant qu'ingé qui demande « valide que pour Y j'obtiens X », je veux être **averti**
> si mes données d'entrée énoncées produisent en réalité Z (vide/cassé), plutôt que de
> voir le test corrigé en douce vers une variante qui passe — sinon l'outil me cache le
> seul signal qui compte.

**Contexte / cause**
Sur le chemin `bad_data`, le trigger envoyé à l'agent
(`back/build_query/conversational_agent.py:809-815`) instruit l'agent de satisfaire les
**contraintes SQL** (`branch_plan` `must_hold`/`must_not_hold`, trace CTE) sans aucune
règle protégeant la prémisse en langage naturel. Ce chemin **boucle sans
`VALIDATION_PROMPT`**. Si la valeur que l'utilisateur a explicitement énoncée est
précisément ce qui casse la contrainte (typiquement `empty_results` ou une clé de jointure
non satisfaite), l'agent la mute silencieusement vers ce qui rend le test vert. La desync
de description n'est alors que le symptôme visible ; le vrai dégât est l'écrasement de
l'**attente**.

**Garde-fous existants mais incomplets**
- `request_reevaluation` : l'agent *peut* signaler « 0 ligne est en fait attendu » — mais
  rien ne l'**oblige** à se demander si la divergence est le point de l'utilisateur.
- Gate « vide intentionnel » (`back/build_query/examples_executor.py:1670`) : n'honore le
  vide que si `<test_context>` mentionne **explicitement** « plage vide / aucune ligne ».
- Gardes no-op : empêchent la boucle folle, pas l'écrasement silencieux.

**Approche** (réutilise l'existant, zéro nouveau nœud)
Enrichir le trigger `bad_data` : si la valeur d'entrée bloquante est explicitement énoncée
dans le scénario / la description, l'agent doit appeler `request_reevaluation` /
`ask_clarification` → délégation `VALIDATION_PROMPT` existante, au lieu de
`patch_test_field`.

**Critères d'acceptation**
- [x] Test de régression **écrit d'abord** (rouge → vert) : prémisse user explicite
      produisant un résultat vide → la boucle n'auto-mute pas la valeur énoncée, route vers
      stop-and-ask. (`back/tests/test_premise_protection.py`)
- [x] Une donnée d'entrée purement auto-générée (aucune valeur énoncée par l'user)
      continue d'être corrigée silencieusement — pas de régression sur la suite (1292 passed).
- [x] Le `VALIDATION_PROMPT` expose à l'utilisateur : « tu as énoncé Y→X, l'exécution
      donne Z — corriger les données, ou ton attente était-elle fausse ? » (via
      `request_reevaluation` / `ask_clarification` dans le trigger `bad_data`).

**Implémentation**
- Enforcement : `premise_guard` dans le trigger `bad_data`
  (`back/build_query/conversational_agent.py`), gardé par le marqueur `user_premise` du
  test en échec.
- Détection : `_resolve_user_premise(state, existing_tests, existing_tc)`
  (`back/build_query/examples_generator.py`) — signal structurel (nouveau test depuis une
  instruction user explicite ; report sur régénération).
- Persistance : `user_premise` préservé à travers la whitelist de l'executor
  (`back/build_query/examples_executor.py`, à côté de `branch_plan`) ; storage garde déjà
  le dict entier.

**Limitation connue (v1)** : si le **tout premier** message de l'utilisateur est une
prémisse concrète (aucun test existant encore), elle n'est pas tracée — la génération
initiale en masse n'attache pas de prémisse par test. À traiter si le cas se présente
(routage première-génération vs scénario explicite).

**Statut : FAIT** (les deux moitiés — enforcement + détection/population).

---

## TICKET-2 · `fix` · **P1** — Desync description ↔ **données d'entrée injectées**

**User story**
> En tant que lecteur d'un test, je veux que le scénario décrit (« on injecte 10 et 20 TiB »)
> corresponde aux valeurs réellement injectées — sinon le test ment et le juge sanctionne
> la lisibilité.

**Contexte / cause**
La desync **sortie** est couverte (`bad_description` / `needs_validation` → délégation,
`back/build_query/query_chain.py:362-367`). La desync **entrée** ne l'est pas :
`bad_description` ne compare qu'au `<result_sample>`
(`back/build_query/examples_executor.py:1635`). La boucle `bad_data` mute les valeurs
injectées sans retoucher le narratif. Observé sur fdp : `daily_verified_claims` (3/4/3,
« 10 et 20 annoncés vs 28.08/3479.61 injectés ») ; à confirmer sur `daily_network_burn`
(« 430 vs 650 »).

**Approche**
- Ajouter au juge une vérif description ↔ valeurs d'entrée injectées.
- ⚠️ **Ne PAS réécrire en silence** (option explicitement abandonnée — ce serait le bug du
  TICKET-1) : si le narratif d'entrée porte une prémisse utilisateur → délégation
  `VALIDATION_PROMPT` ; s'il est auto-généré → `corrected_description` proposé comme pour
  `bad_description`.

**Critères d'acceptation**
- [x] Test de régression : routing + handling déterministes
      (`back/tests/test_input_desync.py`) — `bad_input_description` → délégation
      `VALIDATION_PROMPT`, pas de boucle ; jamais de réécriture muette.
- [x] Narratif d'entrée auto-généré désynchronisé → `corrected_description` reflétant les
      valeurs réelles, sans relancer l'exécution (chemin `accept_validation` au clic).
- [ ] Score lisibilité fdp des cas concernés remonte (≥ 4) — **à valider à l'éval**
      (détection LLM, comme `bad_description`).

**Implémentation**
- Nouveau `reason_type: "bad_input_description"` (`examples_executor.py` : schéma
  `_AssertionsAndEvaluation` + section dédiée du prompt du juge comparant `<test_context>`
  ↔ `<input_data>`).
- Routing : `route_evaluator` + bloc de délégation de `test_evaluator` (question dédiée,
  consciente de `user_premise`) ; mapping dans `assertion_generator`.
- Persistance/validation : `accept_validation` applique `corrected_description` (déjà
  agnostique au `reason_type`) ; retire `user_premise` quand l'utilisateur valide une
  desync d'ENTRÉE (interaction T1↔T2), la conserve sur une desync de sortie.

**Lien TICKET-1** : la piste « réécrire silencieusement la description » est bien évitée —
on passe par la délégation `VALIDATION_PROMPT`, et la question pointe la prémisse
utilisateur quand elle existe.

**Statut : FAIT** côté routing/handling/détection (prompt). Reste à confirmer le gain de
lisibilité à l'éval fdp (`daily_verified_claims`).

---

## TICKET-3 · `quality` · **P2** — Durcir les assertions cosmétiquement fragiles

**User story**
> En tant qu'ingé, je veux que les assertions testent la **logique** (le tri, la règle
> métier) et non un proxy fragile (égalité stricte sur un ID), pour que le test reste
> robuste au refactoring.

**Contexte**
Résidu de qualité non bloquant relevé par le juge fdp : `warm_storage_datasets` (5/4/3,
« vérifie l'ordre par égalité stricte sur l'ID au lieu de la logique de tri ») ;
`daily_filecoin_pay_operators_metrics` (échappements inutiles dans l'ID).

**Critères d'acceptation**
- [ ] Heuristique/garde dans le générateur d'assertions contre le pattern « égalité stricte
      sur clé technique pour valider un tri ».
- [ ] Pas de régression sur les tests fdp déjà 5/5.
