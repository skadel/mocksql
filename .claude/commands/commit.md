Vérifie la qualité du code (back + front), puis crée un commit git avec un message en français décrivant les changements.

## Étapes

1. `make check-all` depuis la racine — si ça échoue côté back à cause du format, lance `cd back && make format` puis relance `make check-all`
2. `git diff --stat` pour voir les fichiers modifiés
3. Analyser les changements et proposer un message de commit clair et concis en français (style conventionnel : `feat:`, `fix:`, `chore:`, `refactor:`, etc.)
4. Demander confirmation avant de committer
5. `git add` des fichiers pertinents (éviter `.env`, secrets, binaires)
6. Créer le commit avec le message validé

## Règles

- Ne jamais ajouter de trailer "Co-Authored-By" au commit
- Si `make check-all` échoue pour une raison autre que le format, s'arrêter et signaler l'erreur
- Toujours afficher le diff résumé avant de proposer le message de commit
- Si l'utilisateur fournit un message de commit dans son prompt, l'utiliser directement (sans demander confirmation du message)

!make check-all 2>&1 || (cd back && poetry run make format 2>&1 && make check-all 2>&1)
