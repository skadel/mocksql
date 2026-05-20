Lance le pipeline de release : lint, format, tests frontend, bump de version patch, tag git et push.

## Étapes

1. `make check-all` depuis la racine — si ça échoue côté back à cause du format, lance `cd back && make format` puis relance `make check-all`
2. Lit la version actuelle dans `back/pyproject.toml` et l'incrémente (patch)
3. Met à jour `back/pyproject.toml` avec la nouvelle version
4. Commit tous les fichiers modifiés avec le message `chore: bump version to X.Y.Z`
5. Crée le tag git `vX.Y.Z`
6. `git push` puis `git push origin vX.Y.Z`

## Règles

- Ne jamais ajouter de trailer "Co-Authored-By" au commit
- Si `make check-all` échoue pour une raison autre que le format, s'arrêter et signaler l'erreur
- Afficher clairement la version précédente et la nouvelle version avant de committer

!make check-all 2>&1 || (cd back && poetry run make format 2>&1 && make check-all 2>&1)
