# `mocksql parity` — audit de parité DuckDB ↔ warehouse

MockSQL exécute tes tests en local sur DuckDB (« 0 € facturé »). Cette promesse repose
sur la couche de transpilation (sqlglot + adaptations maison). `mocksql parity` la rend
**auditable** : il rejoue chaque test sauvegardé **sur ta warehouse** (BigQuery /
Snowflake) avec les **mêmes données synthétiques** que le rejeu DuckDB, puis compare
les deux jeux de résultats.

```
mocksql parity [MODEL]        # tous les modèles, ou un seul
  --all                       # force le rejeu même des tests déjà vérifiés
  --json                      # sortie machine (même convention que test --json)
```

## Ce qui est exécuté côté warehouse

**Jamais de données réelles.** La requête exécutée est la requête *mockée* : chaque
table référencée est remplacée par une CTE inline contenant les lignes synthétiques du
test, typées selon le vrai schéma de l'entrepôt (`schema_cache.json`). Aucune table de
prod n'est lue, aucune PII ne transite.

**Coût quasi nul.** Une requête sans lecture de table scanne ~0 octet sur BigQuery
on-demand (gratuit) ; sur Snowflake, quelques secondes de warehouse XS.

**Pas de transpilation côté warehouse.** Le SQL du modèle s'exécute dans son dialecte
natif — la différence mesurée est donc bien celle de la couche d'émulation DuckDB.

## Lire un DIFF

Un diff n'est **pas** un échec de test (le code de sortie le distingue). Il signale
l'une de ces situations, toutes précieuses :

1. **Bug MockSQL** (transpilation incorrecte) — merci de le remonter en issue, avec la
   sortie du diff.
2. **Divergence sémantique de dialecte** (collation, division entière, tri des NULL…) —
   ton test local ment sur ce point précis, à toi de juger si c'est grave.
3. **Non-déterminisme du modèle** (`LIMIT 1` sans ordre total, ex æquo) — bug de prod
   latent : même la warehouse seule peut changer de réponse au gré du plan d'exécution.

La v1 ne classifie pas ces causes : le rapport est binaire (parité OK ou diff, avec les
deux jeux de résultats côte à côte). Tu as le contexte pour diagnostiquer.

## Règles de comparaison

- **Ordre** : insensible à l'ordre (multiset de lignes), sauf `ORDER BY` terminal →
  comparaison ordonnée.
- **Flottants** : tolérance relative `1e-9` (les moteurs n'additionnent pas dans le
  même ordre).
- **Types normalisés avant comparaison** : décimaux → forme canonique ;
  dates/timestamps → ISO 8601 UTC ; `NULL` ≡ `NULL` quel que soit le type porteur ;
  JSON/VARIANT → forme canonique.
- **Chaînes comparées telles quelles** : une différence de casse/trim EST un diff
  (c'est peut-être la collation, et tu dois le voir).
- **Noms de colonnes** : insensibles à la casse (Snowflake upper-case par défaut) ;
  les noms auto-générés par les moteurs (`_col_0` vs `f0_`) sont alignés par position.

Tout ce qui ne rentre pas dans ces règles est un diff — on préfère un diff explicable
à une normalisation trop agressive qui masquerait une vraie divergence.

## L'attestation (empreinte)

Un test dont les résultats concordent reçoit une **attestation** committée dans la
définition du test (`.mocksql/tests/{model}.json`) :

```json
"parity": {
  "fingerprint": "sha256:…",
  "verified_at": "2026-07-16T14:32:00Z",
  "dialect": "snowflake"
}
```

L'empreinte couvre le SQL normalisé, les données du test, le dialecte et la version du
transpileur (mocksql + sqlglot) : si l'un change, l'attestation devient **périmée** et
le test redevient éligible au rejeu. Relancer `mocksql parity` après un run complet ne
coûte rien et n'exécute rien (idempotent).

L'attestation voyage avec la repo : les coéquipiers et la CI en héritent sans
credentials. `mocksql test` affiche l'état par test — `[parité ✓]` (vérifié, empreinte
à jour), `[parité périmée]` (à re-auditer), rien (jamais audité) — à titre informatif,
jamais bloquant.

Un diff constaté n'écrit **rien** (pas d'attestation négative committée) : le diff est
dans la sortie CLI, point.

## Codes de sortie

| Code | Signification |
|---|---|
| `0` | tous les tests rejoués concordent (ou rien à rejouer) |
| `1` | au moins un diff |
| `2` | erreur d'exécution warehouse (credentials, réseau, SQL rejeté) |

`parity` est un audit ponctuel opt-in — jamais lancé implicitement, jamais requis dans
un pipeline. La CI reste 100 % offline (`test` + `check`), sans credentials warehouse.

## Limites v1

- **Dialectes** : BigQuery et Snowflake. Postgres est volontairement hors v1 (parité
  quasi triviale, dialectes proches).
- **Pas de classification automatique** des causes de diff (v2 possible : une
  *hypothèse* LLM clairement étiquetée comme telle).
- **Pas d'exécution sur données réelles** — `parity` compare des moteurs, pas des
  données.
- **Pas d'auto-correction sur diff** — on rapporte, on ne touche ni au test ni au SQL.
