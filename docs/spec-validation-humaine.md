# Spec — Validation humaine de l'output (« generate → execute → confirm »)

> Statut : **Phase 0 validée (GO) + Phase 1 implémentée** — 2026-07-19. Voir §11.
> Origine : étude comparative dbt 1.8 `unit_tests` / Rocky `[[test.given]]` (juillet 2026).

## 1. Constat et motivation

### 1.1 Le pipeline d'assertions approxime ce que l'humain confirmerait en 10 secondes

Aujourd'hui, le LLM **prédit** des assertions sur l'output, DuckDB exécute, l'évaluateur
juge, et une boucle de correction **aligne les assertions sur le réel** (by design).
À convergence, l'assertion encode la même information qu'un snapshot des lignes
observées — mais dans un langage fragile (SQL généré par LLM) qui a coûté une longue
série de garde-fous : fixer de vacuité + gardes anti-tautologie, champ `scope`,
pin de cardinalité hors-LLM, remapping des noms de tables au replay
(`test_runner._remap_assertion_sql`), taxonomie de désync à 6 causes.

### 1.2 L'oracle de la spec est l'humain, pas le LLM

Le LLM ne connaît pas le métier. Chez dbt/Rocky, l'ingénieur qui écrit les
`expect rows` **est** l'oracle. Chez MockSQL personne ne les écrit — donc quelqu'un
doit endosser ce rôle : c'est l'utilisateur (data engineer, data/business analyst),
au moment où on lui montre l'output focalisé sur le cas testé.

C'est le modèle **approval testing** (snapshots Jest, ApprovalTests) : la machine
propose, l'humain approuve, l'approbation transforme un golden test en test métier.

### 1.3 Le code y tend déjà

Trois des six causes d'`evaluation_feedback` (`bad_description`,
`bad_input_description`, `needs_validation`) aboutissent déjà à « sauver l'état et
demander à l'utilisateur » (VALIDATION_PROMPT, bouton « Je valide l'état actuel »,
nœud `accept_validation`). Cette spec **généralise l'exception en norme** : tout
test naît « à confirmer ».

### 1.4 Repositionnement produit (impact CLAUDE.md)

La valeur différenciante n°2 (« verdict LLM argumenté ») est **repositionnée**, pas
supprimée : le LLM ne *juge* plus la correction de l'output, il **prépare la revue
humaine** (focalisation, point d'attention, cohérence du scénario). La valeur n°1
(génération automatique des données — l'ingé n'écrit jamais une ligne) est inchangée
et reste le cœur.

---

## 2. Principes

| # | Principe |
|---|---|
| P1 | Le **contrat** d'un test = lignes attendues concrètes (`expect`), restreintes aux colonnes pertinentes, **confirmées par un humain**. |
| P2 | Le LLM **génère et focalise** (données qui atteignent le cas, hint de revue) — il ne juge plus la correction de l'output. |
| P3 | La confirmation doit être **cheap** : output focalisé + « la chose à vérifier » ; valider 15 tests = minutes, pas heures. |
| P4 | Replay CLI/CI et export dbt = **déterministes, zéro LLM**, sur le même contrat `expect`. |

---

## 3. Cycle de vie d'un test

```
generate (LLM) → execute (DuckDB) → [boucle bad_data si le cas n'est pas atteint]
    → coherence_check (LLM léger)
    → DRAFT  ──[Confirmer]──→  CONFIRMED  ──[SQL change / drift]──→  STALE
         │                          │                                  │
         ├─[Corriger via chat]      └── replay CI : compare expect     ├─ re-run → diff ancien
         └─[Rejeter → suppr.]           (multiset | ordered)           │  expect vs nouvel output
                                                                       └─[Re-confirmer]→ CONFIRMED
```

- **`draft`** : généré, exécuté, output présenté — en attente de confirmation.
  Rejouable mais rapporté « non confirmé » (pas un échec).
- **`confirmed`** : l'humain a approuvé → `expect` gelé = contrat de non-régression.
- **`stale`** : le SQL a changé (édition ou drift `source_sha`) → re-run → **diff**
  `expect` confirmé vs nouvel output → re-confirmation (ou bug détecté : c'est le
  moment où le test « attrape » une régression en montrant le diff).

La détection de bug « aujourd'hui » (pas seulement demain) est déplacée de
l'évaluateur LLM vers la revue humaine : l'utilisateur qui regarde l'output focalisé
et dit « non, c'est pas ça » vient de trouver un bug dans son SQL. C'est
épistémologiquement plus honnête que le verdict actuel.

---

## 4. Impact sur le graph LangGraph

### Conservé tel quel (qualité de génération — pas du jugement d'output)

| Nœud / mécanisme | Pourquoi |
|---|---|
| `generator`, `executor`, CTE-trace, `failing_cte` | cœur inchangé |
| Boucle `bad_data` / `empty_results` → `conversational_agent` | garantit que les données **atteignent le cas** (traversent JOINs/filtres). Sans elle on présente des outputs vides à l'humain. |
| Focus UNION ALL (`target_path`, `path_plans`), fallback focus→all | inchangé |
| `suggestions_generator`, axes de couverture | inchangé (heuristiques front sur titres/tags) |
| `conversational_agent` (chat édition) | inchangé — « Corriger via chat » y route |
| Leçons (`note_lesson`), profiling, préprocesseur | inchangé |

### Repositionné

| Avant | Après |
|---|---|
| `test_evaluator` (verdict Excellent/Bon/Insuffisant sur l'output + assertions) | **`coherence_check`** léger : (1) le scénario annoncé est-il réellement exercé par les données ? (test « cas NULL » sans NULL en jeu → warn) ; (2) **`review_hint`** : la chose à vérifier (« ligne 3 : amount=100 est le cas limite du seuil ») ; (3) flag non-déterminisme potentiel (LIMIT sans ORDER BY total, ex æquo). Cohérence narratif↔données↔SQL — jamais le réalisme. |
| `accept_validation` (cas `needs_validation`) | devient le **endpoint `confirm`** générique (gèle `expect`, passe `confirmed`) |
| Verdict `good/warn/bad` | devient un signal de **qualité du scénario** (sortie du coherence_check), plus un jugement de l'output |

### Supprimé (à terme, phase 3)

| Élément | Remplacé par |
|---|---|
| Génération d'assertions LLM (`assertion_generator`) | `expect` = lignes observées confirmées |
| `assertion_corrector` + gardes anti-vacuité | plus d'assertions à corriger |
| Pin de cardinalité | implicite : `len(expect.rows)` **est** la cardinalité |
| Champ `scope` | subsumé par colonnes/lignes de `expect` + `ordered` |
| Taxonomie `bad_assertions` / `bad_description` / `bad_input_description` / `needs_validation` | s'effondre : tout est « draft, à confirmer ». Ne restent que `bad_data` (boucle) et l'exécution en erreur. |
| `_remap_assertion_sql` au replay | comparaison de lignes |
| `description_proposal` (proposer/accepter une description) | la revue montre description + output côte à côte ; désaccord → chat |

---

## 5. Format de stockage (`.mocksql/tests/{model}.json`)

Par test case — la définition commitée reste lisible/éditable à la main :

```jsonc
{
  "test_uid": "…",
  "name": "…", "description": "…", "tags": ["Cas limites"],
  "tables": { /* données d'entrée générées — inchangé */ },
  "expect": {
    "columns": ["order_id", "is_high_value"],   // seules colonnes comparées
    "rows": [ {"order_id": 1, "is_high_value": true}, … ],
    "ordered": false            // true auto si ORDER BY top-level (AST sqlglot)
  },
  "review": {
    "status": "draft" | "confirmed" | "stale",
    "hint": "Vérifie la ligne order_id=3 : amount=100 est le cas limite du seuil",
    "coherence": "ok" | "warn",
    "confirmed_by": "user" | "verdict-llm-legacy",
    "confirmed_at": "2026-07-18T…"
  }
}
```

Décisions :
- **`expect.columns`** : sémantique dbt/Rocky — seules les colonnes nommées sont
  comparées. Choisies par le coherence_check (colonnes porteuses de la logique du
  scénario), éditables par l'humain à la revue.
- **`ordered`** : `false` par défaut (multiset) ; posé `true` automatiquement si le
  SQL se termine par un ORDER BY top-level (déterminisme AST, pas LLM).
- **Non-déterminisme (ex æquo)** : pas de mécanique `any_of`. Le coherence_check le
  signale en hint ; la réponse produit est de proposer de **rendre les données
  discriminantes** via le chat (c'est déjà l'axe `tie`).
- `assertion_results`, `results_json` etc. restent dans le cache gitignoré pendant la
  transition (cf. §7).

### Migration des tests existants

`verdict ∈ {Excellent, Bon}` → `expect` pré-rempli depuis `results_json` (restreint
aux colonnes des assertions existantes quand elles sont identifiables, sinon toutes),
`review.status = "confirmed"`, `confirmed_by = "verdict-llm-legacy"`. Le replay CI
continue de fonctionner sans intervention ; l'UI badge distinctement les tests
jamais confirmés par un humain. `Insuffisant`/mort-nés → `draft`.

---

## 6. UI — la carte de revue (TestsView)

- **Output focalisé** : uniquement `expect.columns`, lignes annotées (mapping
  entrée→sortie quand c'est lisible), le `review_hint` en tête.
- **3 actions** : `Confirmer` (→ endpoint confirm) · `Corriger via chat` (ancre le
  chat sur le test) · `Rejeter` (supprime).
- **Revue en lot** : navigation clavier test-à-test ; chaque carte montre UNE chose
  à vérifier. Anti-rubber-stamping : pas de bouton « tout confirmer ».
- **Stale/drift** : bandeau + **diff** ancien `expect` / nouvel output (lignes
  ajoutées/supprimées/modifiées) — c'est l'écran de détection de régression.
- Chip topbar et wording « 0 € facturé » inchangés.

---

## 7. Replay CLI / CI / export

- `mocksql test` : ré-exécute sur le `schema_cache` (inchangé, zéro inférence) puis
  **compare les lignes** (`expect.columns`, multiset ou `ordered`) — suppression du
  remapping d'assertions. Diff lisible en sortie.
- Tests `draft` : exécutés, rapportés `unconfirmed` (pas des échecs).
  Option CI : `mocksql test --require-confirmed` (gate).
- **Export dbt** : `expect` **est** le bloc `expect:` d'un unit test dbt 1.8 —
  l'export (spec `mocksql export dbt`) devient une transformation quasi triviale.
  Contrat unique replay ↔ export.

---

## 8. Risques et parades

| Risque | Parade |
|---|---|
| **Rubber-stamping** (confirmer sans regarder — LE mode d'échec des snapshots) | revue focalisée (P3) : hint unique, colonnes réduites, pas de « tout confirmer » ; badge `verdict-llm-legacy` vs `user` |
| Sorties non-déterministes → replay flaky | flag au coherence_check + proposition de rendre les données discriminantes (chat) ; `ordered` auto sur ORDER BY |
| Gros outputs illisibles à la revue | données synthétiques déjà petites ; cap existant `too_many_rows` conservé côté génération |
| Coût humain sur N tests (batch `tests_target`) | revue en lot clavier ; N par défaut reste 1–3 |
| Perte de valeur perçue (« le verdict a disparu ») | messaging : le verdict devient « prêt à confirmer » + hint — le LLM travaille *pour* la revue ; la confirmation humaine est un argument de confiance (« vos tests sont signés par votre équipe, pas par une IA ») |
| Tests legacy jamais revus par un humain | badge dédié + suggestion de re-revue opportuniste (à l'ouverture du modèle) |

---

## 9. Phasage (valider la prémisse avant de construire)

- **Phase 0 — validation cheap (shadow)** : dual-write `expect` depuis
  `results_json` sur les modèles existants + replay « comparaison de lignes » en
  parallèle des assertions sur le corpus spider2-snow. Mesure : taux d'accord,
  faux positifs de chaque approche, cas non exprimables en lignes. **Go/no-go ici.**
- **Phase 1** : statut `draft/confirmed` + endpoint `confirm` (généralisation
  d'`accept_validation`) + carte de revue UI. L'évaluateur actuel tourne encore
  (verdict affiché à titre indicatif).
- **Phase 2** : `coherence_check` remplace l'évaluation d'assertions pour les
  nouveaux tests ; replay bascule sur les lignes ; export dbt branché sur `expect`.
- **Phase 3** : suppression `assertion_generator` / `assertion_corrector` / gardes /
  taxonomie ; nettoyage state (`evaluation_feedback` réduit) ; mise à jour CLAUDE.md
  (valeur n°2 repositionnée).

---

## 10. Questions ouvertes

1. Colonnes de `expect` : choisies par le coherence_check ou par défaut *toutes*
   les colonnes de sortie (plus simple, plus de bruit au diff) ?
2. Faut-il un invariant d'appoint (ex. `rowcount >= 1` sans valeurs) pour les rares
   cas où les valeurs exactes n'ont pas de sens ? (recommandation : non en v1 —
   YAGNI, le cap `too_many_rows` et les données petites suffisent)
3. La boucle `bad_data` doit-elle être déclenchée par un check déterministe seul
   (0 lignes, tout-NULL) ou garder un déclencheur LLM (« le cas n'est pas
   exercé ») ? (recommandation : déterministe + coherence_check en warn non
   bloquant — plus de boucle sur du jugement d'output)
4. Mode CLI batch (`mocksql generate` spider2-snow) : sans humain dans la boucle,
   tout sort en `draft` — les évals LLM-as-judge devront noter des drafts
   (adapter le juge : cohérence narrative, plus de verdict à lire).

---

## 11. Résultats Phase 0 (2026-07-19) — GO, et Phase 1 livrée

### Mesure d'accord (corpus spider2-snow, 111 modèles, replay `--frozen`)

Migration : 79 cas → `confirmed` (`verdict-llm-legacy`), 9 → `draft`,
23 sans sortie exploitable (mort-nés, `no_results`).

Sur les **88 cas** portant à la fois assertions et contrat `expect` :

| Issue | Nombre |
|---|---|
| Accord (les deux passent / échouent) | **86 (97,7 %)** |
| `expect` échoue seul | 1 — `sf_local003` : mêmes lignes, ordre différent (**ex-æquo réel** sur la clé de tri `AverageSalesPerOrder`=100.0 ×2). La comparaison de lignes détecte un non-déterminisme que les assertions ne voyaient pas (§8 anticipé). |
| assertions échouent seules | 1 — `sf_bq444` : sortie **byte-identique** au contrat, mais l'assertion timestamp échoue au replay → faux positif du langage d'assertions (§1.1 démontré). |
| Cas non exprimables en lignes | 0 (hors mort-nés) |

Les deux désaccords plaident **pour** la comparaison de lignes → **GO**.

### Implémenté

- **Phase 0** — `build_query/expect_contract.py` : builder (`columns` restreintes aux
  assertions quand identifiables, `ordered` via AST sqlglot), comparaison
  multiset/ordonnée (normalisation par le même chemin pandas `to_json` que
  `results_json`, diff cappé, flag `order_only_mismatch`) ; dual-write à CHAQUE
  écriture (`write_test_doc` → `sync_expect_on_doc`) ; backfill `mocksql
  migrate-expect` (§5) ; replay shadow dans `mocksql test` (`expect_check` observé,
  jamais bloquant tant que des assertions existent).
- **Phase 1** — cycle `draft/confirmed/stale` : bascule `stale` déterministe à
  l'écriture quand le SQL change (+ report `stale` au replay sur dérive disque) ;
  `POST /api/tests/confirm` + `mocksql confirm` (gel de la sortie observée,
  `confirmed_by: user`, zéro LLM) ; garde anti-clobber (un PATCH front sans
  `review`/`expect` ne peut pas effacer une confirmation — le fichier est
  propriétaire) ; badges CLI `[à confirmer]`/`[stale]` + gate CI
  `mocksql test --require-confirmed` ; UI TestsPanel : badge de revue
  (`À confirmer` / `Confirmé` / `Confirmé (IA)` / `SQL modifié`), CTA
  « Confirmer cette sortie » (+ « Corriger via le chat »), masqué quand le prompt
  de désync est affiché.

### Reste (Phases 2-3, non engagées)

`coherence_check` + `review_hint`, bascule du replay sur les lignes (suppression du
remapping d'assertions), export dbt branché sur `expect`, écran de diff riche pour
`stale` (l'UI affiche le badge + la sortie courante ; le diff ligne-à-ligne est dans
la CLI), suppression `assertion_generator`/`assertion_corrector`/gardes/taxonomie,
mise à jour CLAUDE.md (valeur n°2 repositionnée).
