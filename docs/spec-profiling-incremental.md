# Spec — Profiling incrémental & boucle de découverte (Phase 2)

> **Statut** : proposition. Phase 1 (sortie du profil dans un cache PII gitignoré) est livrée — voir
> `models/schemas.py`, `storage/config.py:get_profile_cache_path`, CLAUDE.md « Profil = cache PII séparé ».
> Cette spec couvre la Phase 2 : transformer le profiling **one-shot** en **boucle de découverte
> incrémentale, transparente, budgétée et reprenable**.

---

## 1. Problème & intention

Aujourd'hui le profiling est tout-ou-rien : `check_profile` détecte les colonnes manquantes, `build_profile_request`
génère **une** grosse requête (`build_profile_query`), l'utilisateur l'exécute (ou auto-profile), le profil est stocké,
fin. Limites :

- **Opaque** : l'utilisateur ne voit pas *pourquoi* telle requête tourne ni ce qu'elle a appris.
- **Non incrémental** : pas de « approfondis le profil de ce champ » ; il faut tout reprofiler.
- **Coût non maîtrisé** : le profiling tape **BigQuery** (seul endroit où MockSQL dépense de l'argent — l'exécution
  des tests, elle, est gratuite sur DuckDB). Le seul garde-fou est l'estimation dry-run globale (`_estimate_profile_bytes`).
- **Non reprenable** : après le split Phase 1, un projet cloné n'a plus de `profile.json` → on redéclenche le profilage
  complet (re-prompt déjà câblé), mais on ne sait pas « augmenter » un profil partiel.

**Intention** : un profiling qui se construit **par paliers**, où le système propose des requêtes ciblées (« ça
m'aiderait de découvrir le champ `url` »), affiche leur **coût estimé avant exécution**, demande consentement selon
un **budget**, journalise tout dans un **ledger de coût**, et se **reprend** pour approfondir les champs incertains.

**Invariant conservé de Phase 1** : le profil reste dans `profile.json` gitignoré ; le ledger et l'état de découverte
y vivent aussi. Aucune PII n'est jamais commitée. Le réplay CI reste indépendant du profil.

---

## 2. Modèle conceptuel

### 2.1 Trois paliers de sondage (coût croissant)

| Palier | Contenu | Consentement |
|---|---|---|
| **0 — Basics** | counts, null ratio, cardinalité, min/max numériques, `top_values`, régularité temporelle (déjà construit par `build_column_profile`) | **Auto**, borné (`partition_limit`) |
| **1 — Probes pas chères** | ratios de match de pattern (`AVG(col ~ regexp)`), profilage d'expressions dérivées, classe de cardinalité de jointure | **Auto sous budget** (dry-run < seuil) ; sinon palier 2 |
| **2 — Sampling & probes coûteuses** | échantillon de valeurs d'un champ, probe sur gros volume | **Consentement explicite** par probe (dialogue + coût dry-run + justification) |

### 2.2 État de découverte (par champ / par jointure)

Chaque entrée du profil gagne un bloc `discovery` :

```jsonc
"tables": {
  "orders": {
    "columns": {
      "email": {
        // ... stats existantes (null_ratio, distinct_count, top_values, ...)
        "discovery": {
          "confidence": 0.4,            // 0..1 — complétude perçue de la compréhension du champ
          "value_class": "email",       // classe sémantique inférée (pattern library / agent)
          "value_pattern": "*@*.*",     // masque lisible, injecté en génération
          "probes": ["basics", "pattern_match"],  // sondes déjà passées (anti-redite)
          "updated_at": "2026-06-18T..."
        }
      }
    }
  }
}
```

Objectifs : (a) ne **jamais re-sonder** ce qui est connu (reprise/augmentation), (b) cibler le *next-best-probe*
(champ à plus faible `confidence`), (c) alimenter `_format_profile_block` avec `value_class`/`value_pattern`.

### 2.3 Ledger de coût

Frère du ledger de correction (`QueryState.correction_attempts`, `data_patcher.append_correction_attempt`).
Persisté dans `profile.json` (gitignoré) sous une clé `cost_ledger` :

```jsonc
"cost_ledger": {
  "total_bytes": 1.2e10,
  "total_eur": 0.06,
  "entries": [
    {"probe_id": "...", "target": "orders.email", "tier": 1,
     "est_bytes": 1.0e9, "actual_bytes": 1.1e9, "eur": 0.0055,
     "accepted": true, "auto": true, "ts": "..."}
  ]
}
```

Affiché en continu (« coût de profiling jusqu'ici »). Survit à la reprise.

---

## 3. Architecture technique

### 3.1 Réutilisation de l'existant

| Brique | Fichier | Rôle en Phase 2 |
|---|---|---|
| `check_profile` / `build_profile_request` | `build_query/profile_checker.py:373-487` | Point d'entrée ; étendu pour proposer des **probes ciblées** au lieu d'une requête monolithique |
| `_estimate_profile_bytes` | `profile_checker.py:444` | **Dry-run par probe** (coût AVANT exécution) — déjà BigQuery `dry_run=True` |
| `compile_query` (dry-run) | `validator.py:244` | `total_bytes_processed` pour dialects |
| `build_column_profile(_queries)` / `profile_joins_for_query` | `profiler.py` | Générateurs de requêtes agrégées (basics + probes) |
| `_merge_profiles` | `profile_checker.py:115` | Fusion incrémentale (déjà append-mergeable par colonne/jointure) |
| `get/save_profile` | `models/schemas.py` | Persistance `profile.json` ; on y ajoute `discovery` + `cost_ledger` |
| `conversational_agent` (pattern outils) | `build_query/conversational_agent.py` | **Modèle** de l'agent de découverte (boucle propose→exécute→merge) |
| `ProfileRequest` / `ProfilingStep` / dialog auto-profile | `front/.../ProfilingStep.tsx`, `QueryChatComponent.tsx:1622-1778` | UX de consentement + chip `billing_tb` réutilisés |
| flags `autoImport_always` / `autoImport_project_{id}` | `QueryChatComponent.tsx:651-659` | **Précédent** pour les flags de budget profiling |

### 3.2 Nouveaux éléments

**Backend**
- `build_query/profile_discovery.py` (nouveau) :
  - `next_probes(profile, used_columns, budget) -> list[Probe]` : choisit le *next-best-probe* (champs faible confiance,
    sans `value_class`), construit la requête agrégée + estime le dry-run, classe en palier selon le budget.
  - `apply_probe_result(profile, probe, rows) -> profile` : merge des findings + maj `discovery` + append `cost_ledger`.
  - Palier 1 **librairie de patterns déterministe** (`PATTERN_LIBRARY` : email/url/uuid/iso_date/phone/numeric) via
    `AVG(col ~ regexp)` agrégé — pas de LLM.
- `build_query/profile_discovery_agent.py` (nouveau, **derrière flag**) : agent à outils (mirroir du
  `conversational_agent`) pour les **inconnues** que la librairie ne classe pas — propose une probe regex / une
  description de champ. Outils = **constructeurs de requêtes agrégées uniquement**.
- `storage/config.py` : getters `get_profiling_budget()` (octets ou €, défaut prudent), `get_profiling_auto_always()`,
  `is_profiling_discovery_agent_enabled()` (défaut **off**).
- `state.py` : champs `profile_discovery_state`, `profile_cost_ledger`, `pending_probe`, `probe_consent`.

**Frontend**
- `ProfileRequest` étendu : `probe_rationale`, `probe_tier`, `cumulative_cost`.
- Dialogue de consentement palier 2 par probe (réutilise le chip `billing_tb` + ajoute la justification).
- **Panneau de profiling** (mirroir de `SuggestionsSection`/`TestsPanel`) : état de découverte par champ +
  coût cumulé + bouton **« Augmenter le profil »** (relance la boucle sur les champs faibles).
- Flags budget : `localStorage.autoProfile_always` (global) + `autoProfile_project_{projectId}` (par projet).

### 3.3 Flux

```
generate (SQL) → check_profile
   profil complet ────────────────────────────► génération
   profil incomplet/à approfondir
        │
        ▼
   next_probes(profile, used_columns, budget)
        │
        ├─ palier 0/1 sous budget → exécute en silence → apply_probe_result → boucle
        │                            (ledger += coût ; discovery maj)
        │
        └─ palier 2 / hors budget → ProfileRequest{rationale, est_cost}
                                     → dialogue consentement
                                        ├─ accepté → exécute → apply_probe_result → boucle
                                        └─ refusé  → skip ce champ (discovery.confidence figée)
        │
        ▼
   plus de probe utile (ou budget épuisé) → génération avec profil enrichi
```

Reprise : « Augmenter le profil » ré-entre dans `next_probes` sur un profil déjà partiel ; `discovery.probes`
empêche les redites ; le ledger continue à cumuler.

---

## 4. Découpage en livrables (chacun shippable)

- **2a — Fondations (état + ledger)** : ajouter `discovery` + `cost_ledger` au profil, **instrumenter le profiling
  existant** pour les remplir (quels champs couverts, quel coût dry-run/réel). Aucun nouveau sondage. Persistance +
  affichage du coût cumulé. *Petit, sans risque, valeur immédiate (transparence).*
- **2b — Consentement budgété** : `get_profiling_budget` + flags `autoProfile_*` + dialogue palier 2 par probe avec
  dry-run. Bouton « Augmenter le profil ». *Le cœur de l'expérience interactive.*
- **2c — Librairie de patterns** : classes sémantiques déterministes (`value_class`/`value_pattern`) via probes agrégées
  palier 1 ; injectées dans `_format_profile_block`. *Améliore la génération sans LLM ni surcoût notable.*
- **2d — Agent de découverte LLM** (flag, défaut off) : probes regex + descriptions de champ pour les inconnues.
  *Le plus riche, le plus cher en latence — réservé aux cas non couverts par 2c.*

---

## 5. Décisions déjà actées / Non-goals

- **Consentement = budgété** (pas par-probe systématique) : basics auto, probes sous seuil auto-approuvées
  (flag global/projet façon `autoImport`), question explicite au-dessus + pour le sampling. *Décidé avec l'utilisateur.*
- **Librairie de patterns d'abord, agent LLM ensuite** (pour les inconnues seulement), tout derrière flag.
- **Pas de redaction** : le profil étant gitignoré (Phase 1), le sampling de valeurs brutes est permis (palier 2,
  consentement) sans risque de commit.
- **Non-goal** : le réplay CI reste indépendant du profil — aucune dépendance nouvelle introduite.
- **Risque latence** : l'agent LLM = N allers-retours (cf. [[project_generation_latency]]) → strictement gated par 2c
  + budget + flag.

---

## 6. Vérification (par livrable)

- **2a** : tests sur `discovery`/`cost_ledger` (merge incrémental, idempotence, persistance dans `profile.json`,
  jamais dans `schema_cache.json`). Vérifier coût cumulé affiché.
- **2b** : tests budget (auto sous seuil / dialogue au-dessus), dry-run mocké, flags `autoProfile_*`. E2E : profiler un
  modèle, voir un palier 2 demander consentement avec coût, accepter/refuser.
- **2c** : tests pattern library (email/url/uuid/date) → `value_class` correct + injection dans `_format_profile_block`.
- **2d** : tests agent (propose une probe agrégée valide, merge le résultat, met à jour `discovery`), comportement off
  par défaut.
- Transversal : `make test` + `make style` ; run réel BigQuery sur un modèle, vérifier le ledger et l'absence de PII
  dans `git status` (`profile.json` gitignoré).
