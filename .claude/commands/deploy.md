Lance le pipeline de release : lint, format, tests frontend, bump de version patch, tag git et push.

## Étapes

1. `make check` dans `back/` — si ça échoue à cause du format, lance `make format` puis relance `make check`
2. `npm test -- --run --passWithNoTests` dans `front/` (Vitest)
3. Lit la version actuelle dans `back/pyproject.toml` et l'incrémente (patch)
4. Met à jour `back/pyproject.toml` avec la nouvelle version
5. Commit tous les fichiers modifiés avec le message `chore: bump version to X.Y.Z`
6. Crée le tag git `vX.Y.Z`
7. `git push` puis `git push origin vX.Y.Z`

## Règles

- Ne jamais ajouter de trailer "Co-Authored-By" au commit
- Si `make check` ou `npm test` échouent pour une raison autre que le format, s'arrêter et signaler l'erreur
- Afficher clairement la version précédente et la nouvelle version avant de committer

!cd back && poetry run make check 2>&1 || (poetry run make format 2>&1 && poetry run make check 2>&1)
