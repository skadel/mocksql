# Vision Produit MockSQL

## Positionnement

> MockSQL transforme la recette data de "je vérifie visuellement" en "on encode ensemble ce qui doit être vrai."

MockSQL est **l'outil de recette des équipes data** — la session de 15 minutes qui transforme chaque ticket en tests qui restent dans la repo.

Les deux valeurs différenciantes à ne jamais sacrifier :
1. **Génération automatique** — le DE ou le recetteur ne doit jamais écrire une ligne de données de test manuellement.
2. **Verdict LLM argumenté** — chaque test porte une évaluation qualitative, pas juste un pass/fail.

---

## Persona

**Pair : DE + recetteur** — les deux utilisent MockSQL ensemble.

| Rôle | Profil | Apport |
|---|---|---|
| Data Engineer | Technique, écrit le SQL | Lance MockSQL, génère les tests, corrige via le chat |
| Recetteur | BA, QA, PM, ou autre DE | Connaissance métier, valide les cas, demande ce qui manque |

Le recetteur **ne lit pas le SQL** — il lit les titres des tests et les données d'entrée/sortie en langage naturel.

---

## Deux moments d'usage

### Moment 1 — Dev (DE seul)
- **Quand** : pendant le développement du modèle
- **Durée** : 5-10 min
- **Objectif** : ne pas zapper des cas, vérifier que la logique tient
- **Geste** : sélectionne le fichier `.sql`, génère les tests, itère via le chat, sauvegarde
- **Résultat** : TestsView avec tests draft, pas encore validés métier

### Moment 2 — Recette (DE + recetteur)
- **Quand** : pendant la recette, avant de fermer le ticket
- **Durée** : 10-15 min
- **Objectif** : valider que les cas business importants sont couverts
- **Geste** : DE montre la TestsView existante → recetteur lit les titres → ils enrichissent ensemble via le chat
- **Résultat** : tests committés avec le SQL = contrat DE/métier

**Trigger** : chaque nouveau modèle + chaque transformation critique ≈ chaque ticket.

---

## Deux JTBD

| # | Job | Moment | Bénéfice |
|---|---|---|---|
| 1 | Coverage / découverte | Dev + Recette | "On a pensé à tous les cas importants, on ne va pas livrer quelque chose de cassé" |
| 2 | Régression | Continu (CI) | "Si le code change, on le saura avant que ça parte en prod" |

La connaissance métier du recetteur est **encodée dans la repo** — elle ne disparaît pas quand il change d'équipe.

---

## Décisions produit figées

| Décision | Choix retenu | Raison |
|---|---|---|
| Sign-off | Implicite (commit = validation) | Moins de friction pour l'adoption |
| Format de session | Synchrone | Crée un rituel d'équipe, meilleure dynamique de dialogue |
| Fréquence | Chaque ticket (nouveau modèle ou transformation critique) | Devient une définition of done, pas une bonne pratique optionnelle |
| Exécution SQL | DuckDB local uniquement | 0 € facturé sur BigQuery — argument commercial |
| Données de test | Générées automatiquement par LLM | Valeur différenciante #1 — jamais d'écriture manuelle |

---

## Ce que MockSQL n'est pas

- Pas un outil de debug post-prod (pas de monitoring continu)
- Pas un outil utilisable par le BA sans DE (le DE doit être là)
- Pas un outil de refactoring SQL (il teste, il ne réécrit pas)
- Pas une bibliothèque à intégrer dans du code de test (c'est un produit avec une UI dédiée)

---

## Positionnement concurrentiel résumé

Les bibliothèques de mocking SQL (dbt-unit-testing, etc.) demandent au DE d'écrire les données manuellement dans du code Python. MockSQL les génère, les évalue, et implique le recetteur — sans qu'il n'écrive une ligne.
