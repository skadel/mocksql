Enregistre la démo MockSQL via Playwright, évalue chaque frame avec un juge LLM (contexte vierge), implémente les corrections si le score est insuffisant, et reboucle jusqu'à satisfaction ou 10 itérations maximum.

## Constantes

```
ROOT  = chemin absolu racine du repo  (git rev-parse --show-toplevel)
DEMO  = ROOT/demo
RECORDINGS = DEMO/recordings
FRAMES_DIR = RECORDINGS/frames
```

## Mode d'analyse (`$ARGUMENTS`)

Le skill a deux modes de jugement, sélectionnés par l'argument passé après `/demo-review` :

| `$ARGUMENTS` contient…        | Mode       | Le juge évalue…                                                  |
|-------------------------------|------------|------------------------------------------------------------------|
| _(vide)_                      | `criteres` | les **7 critères** agrégés (rubrique ci-dessous) — comportement par défaut |
| `frame` ou `frames` ou `fbf`  | `frames`   | **chaque frame individuellement** : note + ce qu'elle montre + défaut |

Déterminer `MODE` au tout début : si `$ARGUMENTS` (en minuscules) contient `frame` ou `fbf` → `MODE = frames`, sinon `MODE = criteres`. Annoncer le mode retenu à l'utilisateur avant de lancer la boucle.

Seules les **étapes 4, 5 et le rapport final** diffèrent selon `MODE` ; les étapes 1-3 (app, enregistrement, extraction) sont identiques.

## Rubrique d'évaluation — mode `criteres` (7 critères, note 1–10 chacun)

| # | Clé              | Ce qu'on vérifie                                                        |
|---|------------------|-------------------------------------------------------------------------|
| 1 | `intro_card`     | Carte intro visible et lisible — titre "MockSQL" + sous-titre nets      |
| 2 | `test_cards`     | Au moins 2 cartes de tests avec leur titre complet visible              |
| 3 | `coverage_bar`   | Barre de couverture (6 axes) visible et lisible — différenciateur clé   |
| 4 | `verdict_badge`  | Badge good/warn/bad clairement identifiable lors du zoom sur un test    |
| 5 | `chat_interaction` | Message tapé dans le chat lisible + réponse du bot apparaît          |
| 6 | `outro_duckdb`   | "0 € facturé sur BigQuery" visible sur l'écran de clôture               |
| 7 | `narrative_coherence` | **Cohérence de bout en bout** (voir ci-dessous) — le scénario tient-il la route ? |

**Le critère `narrative_coherence` est le garde-fou anti-complaisance.** Le juge note les frames isolément et passe à côté des incohérences de scénario qui sautent aux yeux quand on regarde la vidéo en entier. Pénaliser **sévèrement** (note ≤ 3) si, en lisant le texte du chat et les cartes :

- **Des IDs/hashes internes fuient** dans le texte du chat (ex. `[fa9a]`, `[3c12]`, `[a076]`) — jamais montrer ça à l'utilisateur.
- **Le chat parle de tests redondants / doublons** ou propose de *supprimer* des tests (« vous avez ajouté un nombre important de tests redondants ») → signe d'un **état accumulé** (modèle non réinitialisé entre runs).
- **La démo ré-ajoute un test qui existe déjà** (la demande en langage naturel duplique un test présent).
- **Le test mis en avant / zoomé a un verdict `bad`/`Insuffisant`** ou affiche **« les données d'entrée ne produisent aucun résultat »** / un échec d'exécution → la « money shot » finale doit être un test sain.
- **Le zoom sur un test montre le SQL** (en-tête de la suite) au lieu des détails du test (données d'entrée → résultat → assertions → verdict).

Score global = moyenne arrondie des 7 critères. **Seuil de satisfaction : 7/10.**
**Échec bloquant** : tout critère `narrative_coherence` ≤ 3 fait échouer l'itération même si la moyenne ≥ 7 (au même titre qu'une frame ≤ 3 en mode `frames`).

---

## Boucle principale (max 10 itérations)

Initialiser `iteration = 1`, `score = 0`.

Répéter les étapes 1 à 4 tant que `score < 7` et `iteration <= 10`.

---

### Étape 1 — Vérifier que l'app tourne

Vérifier que le backend répond avant de lancer Playwright :

```bash
curl -sf http://localhost:8100/api/models > /dev/null
```

Si ça échoue, afficher :
> ❌ Le backend (port 8100) ne répond pas — lance le serveur avant de relancer /demo-review.

Et s'arrêter.

### Étape 2 — Enregistrer la démo

> Le front est servi **par le backend sur `:8100`** (la `baseURL` Playwright pointe sur `:8100`) — pas besoin d'un serveur front séparé sur `:3000`. Vérifier que `http://localhost:8100/` renvoie bien la page React (`<div id="root">`).
> Prérequis navigateur : le binaire Chromium de Playwright doit être installé (`npm run install:browsers` dans `DEMO/` si absent).

> ⚠️ **Réinitialiser l'état du modèle avant d'enregistrer.** Le modèle de démo (`demo/payment_summary`) **persiste tests et historique entre les runs**. Sans reset, chaque enregistrement ré-ajoute le même test (« payment_type NULL ») → le modèle accumule des tests redondants, le chat affiche un message « vous avez ajouté un nombre important de tests redondants » avec des **IDs internes qui fuient** (`[fa9a]`, `[3c12]`…), et le test final tombe en verdict `Insuffisant`. Le `beforeAll` du spec ne nettoie que les schémas DuckDB (`/api/dev/clear-schemas`), **pas** les tests persistés. Vérifier que le spec appelle aussi `/api/dev/reset-model` (`{"model_name": "demo/payment_summary"}`) — sinon l'ajouter. Reset ⇒ la démo reprend le chemin « première génération » (60-90 s) et reste cohérente.

```bash
cd DEMO && npm run record:video
```

L'enregistrement dure ~1-2 min (il inclut une génération LLM live pour le test ajouté en langage naturel). Si la commande échoue (exit code non-zéro), afficher l'erreur et s'arrêter.

### Étape 3 — Extraire les frames

Trouver le `.webm` le plus récent dans RECORDINGS/ et en extraire des frames.

**Densité d'échantillonnage = principal levier de sévérité.** Plus on échantillonne, plus on attrape les transitions ratées (zoom/fondu à mi-course) et les frames parasites. Choisir selon `MODE` :

| MODE       | fps      | ~frames (vidéo ~75 s) | Pourquoi                                        |
|------------|----------|-----------------------|-------------------------------------------------|
| `criteres` | `1/3`    | ~25                   | suffisant pour couvrir les 6 étapes             |
| `frames`   | `1/2`    | ~38                   | plus dense → diagnostic fin, attrape les glitches |

> ⚠️ Conséquence pour la mise en scène : un « beat » qu'on veut **garantir** capturé doit durer **plus longtemps que l'intervalle d'échantillonnage** (3 s en mode critères, 2 s en mode frames). Un beat de 2,4 s peut être manqué — voir la checklist anti-régression.

```bash
# ffmpeg : voir la checklist — souvent absent du PATH. Fallbacks, dans l'ordre :
#   1) ffmpeg du PATH    2) C:\Users\skhir\bin\ffmpeg.exe    3) demo/node_modules/ffmpeg-static/ffmpeg.exe
FFMPEG=ffmpeg
command -v ffmpeg >/dev/null || FFMPEG="C:/Users/skhir/bin/ffmpeg.exe"
[ -x "$FFMPEG" ] || FFMPEG="DEMO/node_modules/ffmpeg-static/ffmpeg.exe"

# Trouver le webm le plus récent (récursif), (re)créer le dossier frames
# ⚠️ Playwright NETTOIE recordings/ à chaque run → le sous-dossier frames/ disparaît.
#    Toujours `mkdir -p` AVANT ffmpeg, sinon "Error muxing a packet" (sortie introuvable).
WEBM=$(find "RECORDINGS" -name "*.webm" | sort | tail -1)
mkdir -p "FRAMES_DIR"
rm -f "FRAMES_DIR"/frame_*.png

# fps=1/3 en mode critères, fps=1/2 en mode frames. scale=960px de large.
"$FFMPEG" -y -i "$WEBM" -vf "fps=1/3,scale=960:-1" "FRAMES_DIR/frame_%02d.png" 2>&1
```

Lister les frames extraites pour confirmer (ex: frame_01.png … frame_25.png).

### Étape 4 — Juge LLM (contexte vierge)

Spawner un subagent via l'**Agent tool**. Construire la liste des chemins absolus des frames extraites à l'étape 3 et l'injecter dans le prompt. **Choisir le prompt selon `MODE`** (§ Mode d'analyse).

Dans les deux modes, le subagent reçoit cette consigne de sévérité en préambule :

> Sois un critique **exigeant**, pas complaisant. N'accorde aucun bénéfice du doute : une frame floue, captée en pleine transition (zoom/fondu), un texte tronqué, un cadrage approximatif, une caption qui ne correspond pas à l'écran, ou une frame parasite (UI nue, écran de chargement) → note ≤ 4. Une note ≥ 8 doit être *méritée* : tout est net, lisible, intentionnel.
>
> **Ne juge pas que l'esthétique de chaque frame — lis le CONTENU (texte du chat, titres de tests, verdicts) et vérifie que le scénario est cohérent de bout en bout.** Pénalise sévèrement (note ≤ 3 sur le critère concerné) : des IDs/hashes internes qui fuient dans le chat (ex. `[fa9a]`, `[3c12]`), un chat qui parle de tests redondants / propose d'en supprimer, une démo qui ré-ajoute un test déjà présent, un test mis en avant avec un verdict `bad`/`Insuffisant` ou « les données d'entrée ne produisent aucun résultat », ou un zoom qui montre le SQL au lieu des détails du test. Ces défauts trahissent un **état accumulé** (modèle non réinitialisé) — ils sont invisibles frame par frame mais cassent la démo.

#### Étape 4a — Mode `criteres` (défaut)

**Prompt du juge (à passer tel quel, en remplaçant [FRAMES]) :**

```
Tu es un évaluateur UX EXIGEANT pour la démo vidéo du produit MockSQL (outil de test de requêtes SQL pour data engineers).
N'accorde aucun bénéfice du doute : frame floue / en transition / texte tronqué / caption incohérente / frame parasite → note ≤ 4.

Lis les images suivantes dans l'ordre en utilisant le Read tool. Ne te contente pas de l'esthétique : LIS le texte du chat, les titres des tests et les verdicts pour juger la cohérence du scénario.
[FRAMES]

Évalue chacun des 7 critères sur 10 en t'appuyant sur les frames qui montrent le mieux chaque étape :

1. intro_card       — Carte intro visible et lisible (titre "MockSQL" + sous-titre net)
2. test_cards       — Au moins 2 cartes de tests avec titre complet visible
3. coverage_bar     — Barre de couverture (6 axes) visible et lisible
4. verdict_badge    — Badge good/warn/bad clairement identifiable lors du zoom sur un test
5. chat_interaction — Message tapé dans le chat lisible + réponse du bot apparaît
6. outro_duckdb     — "0 € facturé sur BigQuery" visible sur l'écran de clôture
7. narrative_coherence — Le scénario tient-il de bout en bout ? Note ≤ 3 si : des IDs/hashes internes fuient dans le chat (ex. [fa9a], [3c12], [a076]) ; le chat parle de tests redondants ou propose d'en supprimer ; la démo ré-ajoute un test déjà présent ; le test mis en avant a un verdict bad/Insuffisant ou « les données d'entrée ne produisent aucun résultat » ; le zoom montre le SQL au lieu des détails du test (entrée → résultat → assertions → verdict).

Pour chaque critère, indique :
- La note (1–10)
- La frame de référence (ex: frame_04.png) — la plus représentative
- Un problème précis si note < 7, sinon null (ex: "badge verdict trop petit, fond trop sombre pour lire la couleur")

Retourne UNIQUEMENT le JSON suivant, sans texte avant ou après :
{
  "scores": {
    "intro_card":          { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "test_cards":          { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "coverage_bar":        { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "verdict_badge":       { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "chat_interaction":    { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "outro_duckdb":        { "note": N, "frame": "frame_XX.png", "problem": "..." | null },
    "narrative_coherence": { "note": N, "frame": "frame_XX.png", "problem": "..." | null }
  },
  "global": N,
  "summary": "phrase résumant les problèmes principaux (ou 'Aucun problème majeur' si global >= 7)"
}
```

Calcul du score : `global` = moyenne arrondie des 7 critères, **MAIS** si `narrative_coherence <= 3`, considérer le seuil non atteint même si la moyenne ≥ 7 (échec bloquant — une démo incohérente ne passe pas).

Parser la réponse JSON du subagent. Si le parsing échoue, afficher un avertissement et considérer `global = 0` pour reboucler.

Afficher le tableau de scores à l'utilisateur :

```
## Itération N/10 — Mode critères — Score : X/10
| Critère              | Note | Problème                     |
|----------------------|------|------------------------------|
| intro_card           |  X   | ...                          |
| test_cards           |  X   | ...                          |
| coverage_bar         |  X   | ...                          |
| verdict_badge        |  X   | ...                          |
| chat_interaction     |  X   | ...                          |
| outro_duckdb         |  X   | ...                          |
| narrative_coherence  |  X   | ...                          |
```

#### Étape 4b — Mode `frames` (diagnostic image par image)

Mode plus **sévère** : chaque frame est jugée isolément, sans pouvoir se « cacher » derrière une moyenne de critères. Idéal pour traquer la frame précise qui cloche.

**Prompt du juge (à passer tel quel, en remplaçant [FRAMES]) :**

```
Tu es un évaluateur UX EXIGEANT pour la démo vidéo du produit MockSQL (test de requêtes SQL pour data engineers).

Lis CHAQUE image ci-dessous, dans l'ordre, avec le Read tool, et juge-la INDÉPENDAMMENT des autres :
[FRAMES]

Pour chaque frame, demande-toi : "Si la vidéo se figeait ICI et que je la postais sur LinkedIn, est-ce net, lisible et intentionnel ?"
Pénalise sans bénéfice du doute (note ≤ 4) : image floue, captée en pleine transition (zoom/fondu à mi-course), texte tronqué/illisible, caption qui ne correspond pas à ce qui est affiché, curseur au milieu de nulle part, écran de chargement, UI nue/vide, frame de remplissage sans intérêt narratif.
LIS aussi le texte affiché (chat, titres de tests, verdicts) et pénalise ≤ 3 le contenu incohérent : IDs/hashes internes qui fuient dans le chat (ex. [fa9a], [3c12]), chat qui parle de tests redondants ou propose d'en supprimer, test mis en avant avec verdict bad/Insuffisant ou « les données d'entrée ne produisent aucun résultat », zoom qui montre le SQL au lieu des détails du test.
Une note ≥ 8 = frame nette, lisible, dont le CONTENU fait avancer l'histoire de la démo.

Retourne UNIQUEMENT ce JSON, sans texte avant/après :
{
  "frames": [
    { "frame": "frame_01.png", "note": N, "montre": "ce que la frame représente", "problem": "défaut précis si note < 7, sinon null" }
    // … une entrée par frame, dans l'ordre
  ],
  "worst": ["frame_XX.png", "frame_YY.png"],   // les 1-3 frames les plus faibles
  "global": N,        // = moyenne arrondie des notes de frames
  "summary": "phrase résumant les frames problématiques (ou 'Aucune frame faible' si global >= 7)"
}
```

Parser le JSON. Si le parsing échoue → avertissement + `global = 0` pour reboucler.

Calcul du score (plus strict qu'une simple moyenne) :
- `global` = moyenne arrondie des notes de frames **ET**
- toute frame avec `note <= 3` est un **échec bloquant** : même si la moyenne ≥ 7, considérer le seuil non atteint tant qu'il reste une frame ≤ 3.

Afficher à l'utilisateur :

```
## Itération N/10 — Mode frames — Score : X/10
Frames faibles : frame_XX.png, frame_YY.png

| Frame | Note | Montre              | Problème                |
|-------|------|---------------------|-------------------------|
| 01    |  X   | ...                 | ...                     |
| …     |  …   | …                   | …                       |
```

### Étape 5 — Implémenter les corrections (si score < 7, ou frame ≤ 3 en mode `frames`, ou `narrative_coherence` ≤ 3 en mode `criteres`)

Si `score < 7` (ou échec bloquant) et `iteration < 10` :

1. Lister les critères avec `note < 7` et leur `problem`
2. Pour chaque problème, identifier s'il relève de :
   - **l'état du modèle / la cohérence** (tests accumulés, IDs internes qui fuient, test ré-ajouté, verdict final `Insuffisant`) → le spec doit réinitialiser le modèle avant l'enregistrement (`/api/dev/reset-model` dans le `beforeAll`, cf. Étape 2) ; vérifier aussi que la demande en langage naturel (`typeMessage`) cible un cas **non déjà couvert** par la génération fraîche
   - **la mise en scène** (timing, captions, zoom, vitesse de frappe, durée d'affichage, frame finale) → corriger dans `demo/scripts/full-demo.spec.ts` ou `demo/scripts/demo-overlay.ts`
   - **le rendu de l'app** (composant illisible, badge trop petit, contraste, libellé) → corriger dans `front/src/` (React/CSS/MUI)

   Indice : un défaut "X coupé / mal cadré / capté en pleine transition / frame parasite en fin de vidéo" est presque toujours un problème de mise en scène (scripts). Un défaut "chat incohérent / tests redondants / IDs internes visibles / verdict final rouge" est presque toujours un problème d'**état accumulé** (modèle non réinitialisé), pas l'app ni la mise en scène.
3. Implémenter les corrections — uniquement les changements liés aux problèmes listés, rien d'autre
4. Incrémenter `iteration` et retourner à l'étape 1

Ne pas committer les changements — c'est l'utilisateur qui décide.

---

## Rapport final

À la fin (seuil atteint ou 10 itérations), afficher le tableau du **mode utilisé** (critères → 7 lignes de critères ; frames → liste des frames + colonne « frames faibles »), avec :

```
## Rapport /demo-review (mode : MODE)
Itérations effectuées : N/10
Score final           : X/10

[tableau selon le mode — cf. Étape 4a / 4b]

Statut : ✓ Satisfaisant (>= 7/10, narrative_coherence > 3, et aucune frame ≤ 3 en mode frames)
      ou ✗ Seuil non atteint après 10 itérations
```

Si des corrections ont été apportées, afficher la liste des fichiers modifiés (en distinguant scripts de mise en scène et fichiers app).

---

## Pièges connus — checklist anti-régression

Bugs réellement rencontrés sur cette démo. **Avant de conclure qu'une itération est bonne, revérifier ces points** ; ils ne sont pas tous visibles dans une analyse rapide.

### Cohérence narrative / état du modèle (le plus traître — invisible frame par frame)

- **Le modèle de démo accumule son état entre les runs.** `demo/payment_summary` persiste tests + historique de chat. Sans reset, chaque enregistrement ré-ajoute le test « payment_type NULL » → tests redondants, et le chat déclenche son message anti-doublon « vous avez ajouté un nombre important de tests redondants… je propose de supprimer [fa9a], [3c12], [a076] » : **des IDs internes de tests fuient dans le texte utilisateur**. ⇒ le `beforeAll` du spec doit appeler `/api/dev/reset-model` (`{"model_name": "demo/payment_summary"}`) en plus de `/api/dev/clear-schemas`. Reset ⇒ chemin « première génération » propre.
- **Le test final ne doit pas être rouge.** Sur un modèle pollué, le test ajouté reçoit un verdict `Insuffisant` (« les données d'entrée ne produisent aucun résultat ») — la « money shot » de fin tombe sur un échec. Sur un modèle frais, l'ajout d'un cas mixte donne un verdict sain. Vérifier que le test mis en avant à l'étape 5 a un verdict `good`/`Bon`.
- **La demande en langage naturel ne doit pas dupliquer un test déjà généré.** Si la génération fraîche couvre déjà l'axe « Valeurs NULL », demander « ajoute un test où payment_type est NULL » est redondant. Cibler un cas réellement non couvert (regarder la barre de couverture), sinon l'agent répond par une détection de doublon.
- **Le juge LLM note les frames isolément et RATE ces incohérences** (il a donné 8/10 à une démo avec IDs internes visibles + verdict final rouge). ⇒ critère `narrative_coherence` ajouté à la rubrique, échec bloquant si ≤ 3. Toujours **lire soi-même le texte du chat** sur 2-3 frames avant de conclure.

### Mise en scène (`demo/scripts/full-demo.spec.ts`, `demo/scripts/demo-overlay.ts`)

- **Frame finale = CTA, jamais l'UI nue.** `card()` masque la carte par défaut → la vidéo se terminait sur la TestsView nue, et la dernière frame échantillonnée tombait dessus. ⇒ la carte outro doit rester affichée jusqu'à la fin (`card(..., keep=true)` qui tient ~3,5 s sans masquer).
- **Frame_01 (t≈0) capte le fondu d'entrée de la carte intro.** Comme l'overlay s'installe après le rendu de la GenerateView, la carte « MockSQL » se fond par-dessus l'écran de sélection → frame_01 = overlay translucide sur l'UI. ⇒ tenir la carte intro longtemps (`holdMs` ≥ 4000) pour qu'une frame nette tombe à t≈3 s ; idéalement supprimer le fondu d'entrée pour que frame_01 soit propre.
- **Le zoom sur les tests peut montrer le SQL.** `zoom("tests")` cadre le haut du panneau, soit le bloc SQL (en-tête de la suite), pas les détails d'un test. ⇒ après le zoom, `scrollIntoViewIfNeeded()` sur la carte de test ciblée pour montrer entrée → résultat → assertions → verdict.
- **Caption cohérente avec l'écran.** Une caption encore visible pendant un `zoom(null)` produit une frame « caption X » sur un écran qui montre autre chose (ex. caption « verdict » sur des tables de données). ⇒ `hideCaption()` **avant** de dézoomer.
- **Tout beat à capturer doit durer > intervalle d'échantillonnage.** Sampling = 1 frame / 3 s (mode critères). Un beat de 2,4 s peut être totalement manqué. ⇒ dwell ≥ 3,5 s sur les moments clés (verdict, couverture, nouveau test). Le beat couverture à 2,4 s est passé de justesse — à surveiller.
- **Frappe rapide.** L'effet machine à écrire à 40-80 ms/car étalait la saisie sur 3 frames. ⇒ 22-42 ms/car (`typeMessage`).
- **Transitions de zoom (0,6 s) captées à mi-course = frame floue/dédoublée.** Stochastique, non éliminable à 100 % ; atténué par des dwell longs + `hideCaption` avant le dézoom. Si une frame floue de transition apparaît, c'est de la mise en scène, **pas** un bug de l'app.
- Le spec gère déjà : skip profiling (« Continuer sans profiling »), import des tables manquantes (`import-button`), bandeau « Profil non disponible » masqué par l'overlay.

### Environnement / outillage (surtout Windows)

- **ffmpeg souvent absent du PATH.** Fallbacks dans l'ordre : PATH → `C:\Users\skhir\bin\ffmpeg.exe` (copie stable) → `demo/node_modules/ffmpeg-static/ffmpeg.exe`. `encode.mjs` embarque déjà ce fallback (`resolveFfmpeg`).
- **Une modif de PATH ne se propage pas aux shells déjà lancés.** Après avoir ajouté ffmpeg au PATH, un `npm run encode` lancé dans la foulée peut encore ne pas le voir. ⇒ utiliser le chemin absolu du binaire, ou ré-exporter `$env:Path` dans la même commande.
- **PowerShell : le répertoire courant persiste entre les appels.** Un `Set-Location demo` antérieur fait que `demo\recordings\…` se résout en `demo\demo\recordings`. ⇒ **toujours des chemins absolus** dans ce skill (extraction frames, encode, lecture).
- **Front servi par le backend sur `:8100`** (pas de `:3000`). Si `:8100/` ne renvoie pas la page React, l'enregistrement filmera une page blanche.

### Évaluation (juge LLM)

- **Le juge est complaisant par défaut** — la démo « passait du premier coup » sans le mériter. ⇒ préambule de sévérité (note ≤ 4 pour flou/transition/troncature/caption incohérente/frame parasite), seuil 7 maintenu mais mérité.
- **La moyenne masque les points faibles.** En mode `frames`, toute frame ≤ 3 est un échec bloquant indépendamment de la moyenne. Pour durcir encore : passer à `fps=1/2`, ou relever le seuil à 8.
