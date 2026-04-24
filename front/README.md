# MockSQL — Frontend

**React 18 · TypeScript · Redux Toolkit · MUI**

Interface web du Web Hub MockSQL — visualisation des données de test générées, historique des requêtes, profiling et collaboration.

---

## Démarrage rapide

```bash
cd front
npm ci
npm start      # http://localhost:3000
```

Le frontend proxifie automatiquement les appels `/api/*` vers le backend sur `http://localhost:8080`.

> Le backend doit tourner sur le port 8080. Voir [../back/README.md](../back/README.md) pour le lancer.

---

## Commandes

| Commande | Description |
|----------|-------------|
| `npm start` | Dev server avec hot-reload (port 3000) |
| `npm test` | Tests Jest |
| `npm run build` | Build de production dans `build/` |
| `npx eslint src/` | Lint |
| `npx prettier --write src/` | Format |

---

## Structure

```
src/
  api/              # appels HTTP vers le backend (query.ts, models.ts, table.ts)
  app/              # Redux store + hooks
  features/
    buildModel/     # feature principale
      buildModelSlice.ts
      components/   # QueryChatComponent, TestsPanel, ProfilingStep, …
  selectors/        # sélecteurs Redux
  utils/            # types.ts, messages.ts, helpers
```

---

## Build de production

```bash
npm run build
```

Le build est généré dans `front/build/`. Il est intégré dans le wheel `mocksql-ui` via `make build-ui` (depuis `back/`) :

```bash
cd back
make build-ui   # build React + packaging Python
```

L'application servie par `mocksql ui` utilise ce build bundlé.

---

## Variables d'environnement

Le proxy de développement est configuré dans `package.json` (`"proxy": "http://localhost:8080"`). Aucune variable `.env` n'est requise pour le développement local.
