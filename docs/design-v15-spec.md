# Spec d'implémentation — Design v15 (MockSQL Web Hub)

> **Statut** : proposition prête à découper en PRs.
> **Source de vérité visuelle** : `mocksql-design-system/project/ui_kits/web_hub/` (kit React standalone) + `reference_v15.html` (proposition canonique) + `colors_and_type.css` (tokens).
> **Périmètre** : porter le **langage visuel et les concepts produit** du kit v15 dans l'app réelle. Le kit est du React-sans-build (CDN + Babel + `window.MQ_DATA` + CSS brut, données figées) — **il sert de référence pixel, pas de code à copier**. La cible reste React 18 + TS + Redux Toolkit + MUI, câblée sur le state Redux/SSE existant.

---

## 0. Principes directeurs

1. **Pas de copier-coller HTML.** Chaque composant du kit est ré-écrit en composant MUI/TS branché sur `queryComponentGraph` / `testResults` / le stream SSE. Le `.html` standalone est ouvert à côté comme maquette de référence.
2. **Tokens d'abord.** On installe `colors_and_type.css` comme socle (variables CSS globales) **et** on mappe les couleurs/typo dans le thème MUI. Tout le reste en hérite.
3. **Ne jamais sacrifier les deux valeurs produit** (cf. `CLAUDE.md`) : génération automatique + verdict LLM argumenté. Le design v15 les renforce (chat « réflexion + action », décision métier figée).
4. **Le test d'intégration est hors périmètre de cette spec** (voir §8) : seulement un *stub* visible, car le design ne le définit pas.

---

## 1. Mapping des design tokens → thème MUI

Fichier cible : nouveau `front/src/style/tokens.css` (copie de `colors_and_type.css`) importé une fois dans `index.tsx`, **plus** un thème MUI enrichi dans `front/src/App.tsx` (aujourd'hui quasi vide, ne définit que `background.default: #dde3e6` — qui correspond déjà à `--mq-canvas`).

### 1.1 Palette MUI (`createTheme`)

| Token v15 | Valeur | MUI |
|---|---|---|
| `--mq-brand-500` | `#2bb0a8` | `palette.primary.main` |
| `--mq-brand-600` | `#1f948d` | `palette.primary.dark` (hover) |
| `--mq-brand-300` | `#7cc8c1` | `palette.primary.light` |
| `--mq-ink-900` | `#0f272a` | `palette.text.primary` |
| `--mq-ink-500` | `#6b8287` | `palette.text.secondary` |
| `--mq-success` / `bg` / `border` | `#23a26d` / `#e9f7f0` / `#bce6d3` | verdict **good** |
| `--mq-warning` / `bg` / `border` | `#d89323` / `#fcf3e1` / `#f0d8a8` | verdict **warn** |
| `--mq-danger` / `bg` / `border` | `#d0503f` / `#fbeceb` / `#f3c3bd` | verdict **bad** |
| `--mq-duck*` | `#f7c948` … | accent DuckDB / « fichier modifié » |
| `--mq-surface` / `--mq-surface-2` | `#f3f6f7` / `#ffffff` | cartes / inputs |
| `--mq-canvas` / `--mq-sidebar` | `#dde3e6` / `#eef3f3` | fond app / rail gauche |

### 1.2 Typo

- `typography.fontFamily` = `Inter` (sans) ; mono = `JetBrains Mono` (import Google Fonts déjà dans `colors_and_type.css`).
- Échelle : display 26 / h1 17 / h2 15 / body 13.5 / sm 12.5 / xs 11.5 / eyebrow 11 / code 12.5. Mapper sur `typography.h1/h2/body1/body2/caption/overline`.

### 1.3 Forme & élévation

- `shape.borderRadius = 8` (`--mq-r-md`). Cartes/wells = 12 (`--mq-r-lg`), pills = 999.
- Ombres quasi plates : `--mq-shadow-xs/sm/md`. Override `shadows[1]` et `shadows[2]`.
- Transition standard : `170ms cubic-bezier(0.4,0,0.2,1)` (`--mq-dur` / `--mq-ease`).

**Décision attendue** : valider qu'on remplace bien la palette MUI par défaut (boutons bleus → teal). Risque : composants MUI existants qui s'appuyaient sur le bleu par défaut.

---

## 2. Inventaire des composants (par écran)

Source kit → cible app. **C** = créer, **M** = modifier, **R** = référence visuelle.

| Bloc v15 | Fichier kit | Cible dans `front/src/` | Action |
|---|---|---|---|
| Tokens | `colors_and_type.css` | `style/tokens.css` + `App.tsx` thème | **C/M** |
| Icônes Lucide | `primitives.jsx` (`Icon`) | `lucide-react` (ou set inline `shared/Icon.tsx`) | **C** |
| Primitives Button/Pill/Tag | `primitives.jsx` | `shared/` (Pill, Tag, VerdictPill) | **C** |
| SQL viewer | `primitives.jsx` (`SqlCode`) | composant SQL existant (Suite) | **M** |
| DataTable | `primitives.jsx` (`DataTable`) | `DisplayTable` existant | **M** (style) |
| Sidebar | `Sidebar.jsx` | `features/appBar/.../DrawerComponent.tsx` | **M** |
| GenerateView | `GenerateView.jsx` | `buildModel/components/GenerateView` | **M** |
| TopBar | `App.jsx` (`TopBar`) | header de `QueryChatComponent` | **M** |
| Chat | `Chat.jsx` | `ChatColumn.tsx` / `MessageBody.tsx` | **M** (refonte) |
| Test suite + cartes | `Suite.jsx` | `TestsPanel.tsx` | **M** (refonte) |
| Couverture | `Coverage.jsx` | `TestsPanel.tsx` (`COVERAGE_AXES`) | **M** |
| Historique | `History.jsx` | nouveau `HistoryDrawer.tsx` (remplace popover `sqlHistory`) | **C/M** |
| Status bar DuckDB | `Suite.jsx` (`statusbar`) | bandeau bas de Suite | **M** |

---

## 3. Détail par bloc

### 3.1 Sidebar (`DrawerComponent.tsx`)
- En-tête : tuile teal + logo blanc (`assets/logo-white.png`) + wordmark « MockSQL » + actions (refresh, nouveau).
- Onglets `Modèles · N` / `Priorité`.
- Champ recherche pleine largeur.
- Arbre : dossier → fichier actif (nom + `taille · date`) → **sous-liste des tests** avec pastille verdict (`dot good/warn`) + horodatage.
- Pied : `Serveur local · :PORT` + sélecteur de langue.
- **Câblage** : alimenté par `GET /api/models` (déjà en place) ; la sous-liste de tests vient de `testResults`.

### 3.2 GenerateView (`GenerateView.jsx`)
- En-tête fiole + H1 « Générer des tests à partir d'une requête SQL » + sous-titre.
- **Toggle de mode** (`mode-tabs`) : *Test unitaire* (actif) / *Test d'intégration* badgé **Nouveau** → **stub, voir §8**.
- Step row « 1 · Choisir un fichier SQL » + chemin `models_path`.
- Champ recherche + ligne résultat (nom, path, `taille · modifié`).
- Footer : note DuckDB (`Exécuté sur DuckDB en local — zéro coût BigQuery`) + CTA `Générer les tests`.
- **Câblage** : liste = `GET /api/models` ; CTA = flux `chatQuery` existant.

### 3.3 TopBar
- Mini-tuile logo + « MockSQL » + `Fichier testé : <nom.sql>` + bouton `Changer` (→ retour GenerateView).

### 3.4 Chat — **refonte UX majeure** (`Chat.jsx`)
Le tour assistant n'est plus un simple message : c'est un **« turn »** structuré (style Claude Code) :
1. **`Thinking`** repliable : libellé « Réflexion · <durée> » + lignes ; lignes normales (chevron) et lignes **action** (`act`, coche verte) du type `Profilé covid19_open_data · 0 €` / `Exécuté 1 test sur DuckDB local · 220 ms`.
2. **`turn-badge`** : `Suite générée` (info) / `Test ajouté` (added) / `Test modifié` (modified) / `Note` (info).
3. **Résumé** clair en HTML (avec `<code>`, `verdict-good/warn` inline).

Autres types de message :
- **`user`** : en-tête « Vous » + bulle lilas.
- **`suggestions`** : « Cas suggérés · issus du profiling » + items `plusCircle`.
- **`prod`** (choix implicite) : bloc encadré, eyebrow `MockSQL · <titre>` + badge **PROD**, corps, note `shieldAlert`, puis zone décision avec actions `Garder (…)` / `Corriger le SQL`. → **§5/§6**.
- **`typing`** : trois points animés + label busy.
- **Composer** : textarea auto-grow (max 120px) + hint clavier + bouton envoi sombre (`--mq-send-btn`, hover teal).

**Câblage** : c'est le plus gros chantier. Il faut **mapper le stream SSE** (`on_chain_start`/`on_chain_stream`, reasoning, étapes du graph LangGraph) vers ce modèle de « turn » :
- les `loading_message` / étapes deviennent des **lignes de Réflexion** ;
- `streamingReasoning` alimente le corps repliable ;
- les nœuds `executor`/`profiler` émettent des **lignes action** (avec durée, `0 €`) ;
- le résultat final (`examples`/`results`/`evaluation`) produit le **badge** + **résumé**.
> Ne pas inventer de fausses durées : les exposer depuis le backend (cf. §6) ou les omettre.

### 3.5 Test suite + cartes (`Suite.jsx`)
- En-tête suite : fiole + « Suite de tests » + `N tests` + refresh + `Relancer`.
- Barre statut : `N tests` + pill `Tous passent` / `1 à valider`, + `Partager` + `Demander à MockSQL`.
- **Couverture** (§4) insérée ici.
- **SQL block** : barre `SQL` + nom fichier + toggle **Original / Optimisé** + coloration (`SqlCode`).
- **TestCard** :
  - `test-meta` : **VerdictPill** (good/warn/bad) + tags (`Logique métier`, `Cas limites`, `Intégration`) + numéro `#n`.
  - Titre du scénario.
  - **`decision-block`** (si présent) : question → label « Décision métier » → texte → pied (avatars `decidedBy` + `Validé · <date>` + part prod `94% des mois observés`). → **§5/§6**.
  - **`verdict-box`** (good/warn) : « Verdict · <mot> — <texte argumenté> ».
  - `scenario-label` + `passCount/total assertions passent`.
  - **Assertions** : ligne coche/croix + description + valeur réelle.
  - Données : `1 · Données d'entrée` (+ token table) → flèche → `2 · Résultat de la requête` (+ `N lignes`). `DataTable`.
  - Barre d'actions : régénérer / annuler / copier-lien / Commentaires / supprimer.
- **Status bar** : pastille + « DuckDB local · Tes requêtes BigQuery sont transpilées et exécutées sur ton poste » + `0 € facturé`.

**Câblage** : `testResults` fournit déjà verdict / assertions / données. Le `decision-block` et le `inProdShare` sont **nouveaux** (§6).

### 3.6 Historique (`History.jsx`)
- Drawer latéral (scrim + aside) déclenché par l'icône `history` du chat.
- Timeline : par version → pastille verdict + label (`Fichier modifié` / `Version sauvegardée` / `Version testée`) + badges `actuelle` / `testée` + horodatage + résumé + `Revenir à cette version`.
- Pied : « Versions surveillées en local · ré-exécutables sur DuckDB · 0 € facturé ».
- **Câblage** : remplace/enrichit le popover `sqlHistory` actuel. Le « file-watcher » (détection des modifs du `.sql` sur disque) est **nouveau** côté produit (lié à la dérive / `source_sha` du `CLAUDE.md`).

---

## 4. Couverture — décision sur les 6 axes

⚠️ **Le design redéfinit les axes.** À trancher (impacte front **et** backend).

| Actuel (`TestsPanel.tsx:73`) | v15 (`data.js:coverageAxes`) |
|---|---|
| Chemin nominal (`happy`) | — (supprimé) |
| Valeurs NULL (`null`) | Valeurs NULL (`null`) |
| Plage vide (`empty`) | Fenêtre vide (`vide`) |
| Valeurs égales (`equal`) | — (supprimé) |
| Ex æquo (`tie`) | Ex æquo (`ex_aequo`) |
| Format de sortie (`types`) | — (supprimé) |
| — | **Bornes & négatifs** (`bornes`) — nouveau |
| — | **Doublons** (`doublons`) — nouveau |
| — | **Volumétrie** (`volumetrie`) — nouveau |

**Travail si on adopte v15** :
1. Front : remplacer `COVERAGE_AXES` + les heuristiques regex (`TestsPanel.tsx:86-91`) par les 6 nouveaux axes ; revoir l'UI couverture (`cov-grid`, bouton `Tester` par axe manquant).
2. Backend : aligner les **suggestions** (`suggestions_node.py`) et, idéalement, faire **déclarer les axes par test** (`test.axes`) côté backend au lieu de deviner par regex côté front (le kit fait `t.axes` explicite — plus fiable).

**Recommandation** : passer à un modèle où **le backend tague chaque test avec ses axes** (`axes: []`), et le front ne fait plus de regex. C'est plus robuste et c'est la direction du kit.

---

## 5. Concepts produit nouveaux (au-delà du visuel)

Ces blocs ne sont pas cosmétiques — ils matérialisent la vision (dérive + décision métier + 3 états des suggestions) :

1. **Décision métier figée** (`decision-block`) : une décision validée par le métier, attachée au test, avec qui/quand et part en prod. → tout SQL futur qui change ce résultat fait échouer la suite.
2. **Choix implicite PROD** (`prod` block) : comportement non déterministe détecté **et observé en prod**, surfacé pour arbitrage (`Garder` / `Corriger`). C'est l'état **« existe-en-prod-non-couvert »**.
3. **Lignes d'action chiffrées** (`Exécuté · 220 ms · 0 €`) : rendent visible le travail de l'agent.

---

## 6. Impacts backend

| Besoin v15 | Backend |
|---|---|
| `decision` / `decidedBy` / `decidedAt` / `inProdShare` sur un test | nouveau modèle + persistance (table décisions liée au test) ; `QueryState` + `test_evaluator` |
| Bloc `prod` (choix implicite + observé en prod) | détection (non-déterminisme déjà repéré par `test_evaluator`) + **signal « observé en prod »** (profiling/connecteur) |
| Axes déclarés par test (`axes: []`) | `examples_generator` / `test_evaluator` taguent chaque test |
| Durées & coût des lignes d'action | exposer timings des nœuds (`executor`, `profiler`) dans le stream |
| Historique file-watcher + verdict par version | surveiller le `.sql` (mtime/sha) + stocker verdict par version (lié `source_sha`) |

**Aucun de ces points n'est bloquant pour la phase visuelle** : on peut livrer le front avec des champs optionnels (absents ⇒ blocs masqués), puis brancher le backend.

---

## 7. Plan de livraison (PRs)

1. **PR 1 — Socle tokens & thème** : `tokens.css` + thème MUI (palette teal, typo Inter/JetBrains, radii, ombres). Aucune régression fonctionnelle. *(visuel only)*
2. **PR 2 — Primitives & icônes** : `Icon` (lucide-react), `Pill`/`VerdictPill`/`Tag`, style `DataTable`/SQL viewer.
3. **PR 3 — Sidebar + GenerateView + TopBar** (avec **stub** mode intégration).
4. **PR 4 — Chat refonte** : modèle « turn » (Thinking repliable + badges + lignes action), composer. Mapping SSE.
5. **PR 5 — Test suite & cartes** : verdict-box, assertions, données, status bar. Champs `decision`/`prod` optionnels masqués.
6. **PR 6 — Couverture v15** (après décision §4) : nouveaux axes, idéalement tagués backend.
7. **PR 7 — Historique drawer**.
8. **PR 8+ — Backend** : décision métier, prod implicit-choice, timings, file-watcher (§6).

---

## 8. Test d'intégration — explicitement hors périmètre

Dans le kit, « Test d'intégration » = **un onglet + un libellé de bouton** (`GenerateView.jsx:24-31`, badge *Nouveau*, « Chaîne de scripts SQL »). **Aucun écran ne définit le flux** : pas de DAG, pas de tables intermédiaires, pas d'enchaînement de verdicts. Tout le kit (Suite, Coverage, Chat, données) est mono-requête.

**Décision retenue** : livrer un **stub visible mais non fonctionnel** —
- onglet présent, badge `Nouveau`/`Bientôt`, état désactivé ou ouvrant un « on te prévient » ;
- le CTA bascule le libellé mais le flux reste le mode unitaire.

**Ce qu'il faudrait pour le vrai** (à designer séparément, hors de cette spec) :
- sélection **multi-fichiers** ordonnés (chaîne) ou lecture d'un DAG (dbt `manifest.json` — déjà en roadmap `CLAUDE.md`) ;
- matérialisation des **tables intermédiaires** entre scripts dans DuckDB ;
- visualisation de l'**enchaînement** (graphe) + verdict par étape + verdict global ;
- couverture/suggestions au niveau chaîne.

> Cohérent avec la priorité mémoire *« consolider avant d'ajouter »* : on vend la roadmap (stub) sans créer de dette tant que le flux n'est pas conçu.

---

## 9. Définition de « terminé » & risques

**Done (phase visuelle, PR 1-7)** : chaque écran est pixel-cohérent avec le kit, câblé sur le vrai state Redux/SSE, `npm run build` + ESLint OK, et **la démo Playlist `demo/` passe toujours** (les `data-testid` utilisés par `full-demo.spec.ts` doivent être préservés — voir `.claude/commands/demo-review.md`).

**Risques** :
- Les `data-testid` (`test-card-1`, `coverage-bar`, `chat-input`, `send-button`, `generate-button`, `file-search-input`, `generate-file-row-*`) sont consommés par la démo → **les conserver** lors de la refonte.
- Override de la palette MUI par défaut : vérifier les composants qui supposaient le bleu.
- Mapping SSE → « turn » : risque de complexité ; livrable incrémental (d'abord rendu statique, puis streaming).
- Changement d'axes de couverture : casse les heuristiques regex actuelles → coordonner front+backend.

---

## Annexe — fichiers de référence

- Kit : `mocksql-design-system/project/ui_kits/web_hub/{App,Chat,Suite,Coverage,GenerateView,Sidebar,History,primitives}.jsx`, `data.js`, `styles.css`
- Maquette canonique : `…/web_hub/reference_v15.html`
- Tokens : `mocksql-design-system/project/colors_and_type.css`
- Cibles app : `front/src/App.tsx`, `front/src/features/appBar/components/DrawerComponent.tsx`, `front/src/features/buildModel/components/{QueryChatComponent,ChatColumn,MessageBody,TestsPanel}.tsx`
