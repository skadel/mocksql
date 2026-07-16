# Spec — `mocksql parity` : audit de parité DuckDB ↔ warehouse

> Statut : **implémentée v1** (2026-07-16) — `back/cli/parity.py`, commande `mocksql parity`,
> statut `verified`/`stale`/`unverified` dans `mocksql test`, doc utilisateur `docs/parity.md`.
> Décisions prises sur les questions ouvertes : empreinte = version de package (reco n°1) ;
> Postgres hors v1 (question n°2) ; dogfooding Spider2-snow à faire (question n°3).
> Reste hors v1 livrée : chip TestsView (UI) et badge export HTML (l'export n'existe pas encore).
> Spec d'origine issue d'une discussion produit (2026-07-16).
> Décision de nommage : `parity` (retenu contre `audit` — connotation sécurité/PII — et
> `verify` — troisième quasi-synonyme à côté de `test` et `check`).

## Résumé

`mocksql parity` rejoue les tests sauvegardés d'un modèle **sur la warehouse de
l'utilisateur** (BigQuery / Snowflake / Postgres) avec les **mêmes données synthétiques**
que le rejeu DuckDB, puis compare les deux jeux de résultats. Un test dont les résultats
concordent reçoit une **attestation de parité** (empreinte committée) affichée ensuite
par `mocksql test`, la TestsView et l'export HTML.

C'est un **audit ponctuel opt-in** — pas un mode d'exécution. Il se lance une fois par
test, puis uniquement quand l'empreinte est périmée.

---

## Motivation

La promesse centrale de MockSQL est « DuckDB émule fidèlement ton dialecte, 0 € facturé ».
Cette promesse repose sur la couche de transpilation (sqlglot + adaptations maison :
`LATERAL FLATTEN → UNNEST`, hints epoch, mapping `NUMBER`…) — qui est précisément là où
les bugs se cachent (cf. tout le chantier Spider2-snow). Aujourd'hui rien ne permet à
l'utilisateur de *vérifier* cette promesse : il doit nous croire sur parole.

`mocksql parity` rend la promesse **auditable**. Un diff détecté signale l'une de ces
situations, toutes précieuses :

1. **Bug MockSQL** (transpilation incorrecte) — gold pour nous, à remonter en issue.
2. **Divergence sémantique de dialecte** (collation, division entière, tri des NULL…) —
   le DE doit le savoir : son test local ment.
3. **Non-déterminisme du modèle** (`LIMIT 1` sans ordre total, ex æquo) — bug de prod
   latent : même la warehouse seule peut changer de réponse au gré du plan d'exécution.

**v1 ne classifie pas ces causes** (départager automatiquement est un problème de
recherche) : le rapport est binaire — parité OK ou diff, avec les deux jeux de résultats
côte à côte. Le DE diagnostique, il a le contexte.

## Cohérence avec la vision produit

La décision figée « Exécution SQL : DuckDB local uniquement — 0 € facturé »
(`docs/vision-produit.md`) reste vraie pour le cycle de vie normal (`generate`, `test`,
`check`, CI). `parity` ne la contredit pas parce que :

- **Jamais de données réelles.** On exécute la requête *mockée* : les tables sont
  remplacées par des CTEs inline contenant les lignes synthétiques du test. Aucune table
  de prod n'est lue, aucune PII ne transite.
- **Coût quasi nul.** Une requête sans lecture de table scanne ~0 octet sur BigQuery
  on-demand (gratuit) ; sur Snowflake, quelques secondes de warehouse XS. À afficher tel
  quel dans le `--help` et la doc.
- **Opt-in, hors CI.** Jamais lancé implicitement, jamais requis dans un pipeline. La CI
  reste 100 % offline (`test` + `check`), sans credentials warehouse.

Le quatuor de verbes, un par moment :

| Verbe | Moment | Coût | Credentials warehouse |
|---|---|---|---|
| `generate` | dev — créer les tests | LLM | oui (import/profil) |
| `test` | CI — rejouer, à chaque PR | 0 € | non |
| `check` | CI — gate de dérive SQL | 0 € | non |
| `parity` | ponctuel — auditer l'émulation DuckDB | ~0 € | oui |

---

## UX CLI

```
mocksql parity [MODEL]        # tous les modèles, ou un seul
  --all                       # force le rejeu même des tests déjà vérifiés
  --json                      # sortie machine (même convention que test --json)
```

**Comportement par défaut : idempotent.** Seuls les tests **non vérifiés** ou dont
l'**empreinte est périmée** sont rejoués. Relancer `mocksql parity` après un run complet
ne coûte rien et n'exécute rien.

Sortie type :

```
finance/revenue
  ✓  Remise appliquée au palier 3          parité OK (déjà vérifié, empreinte à jour)
  ✓  Client sans commande                  parité OK
  ✗  Dernier statut par client             DIFF — 2 lignes divergent
       DuckDB   : [{"client": "c1", "statut": "actif"},   …]
       Snowflake: [{"client": "c1", "statut": "suspendu"}, …]
       Causes possibles : bug de transpilation MockSQL · sémantique du dialecte ·
       non-déterminisme du modèle (ordre non total ?). Voir docs/parity.md.
  ─  Import échoué en génération           sauté (test mort-né, non exécutable)

2 vérifiés · 1 diff · 1 sauté
```

**Codes de sortie** : `0` = tous les tests rejoués concordent (ou rien à rejouer) ·
`1` = au moins un diff · `2` = erreur d'exécution warehouse (credentials, réseau, SQL
rejeté). Le diff n'est **pas** un échec de test — c'est une information d'audit.

**Sélection des tests** : tout test exécutable est éligible, indépendamment de son
verdict LLM ou de son statut pass/fail — on compare des **résultats bruts d'exécution**,
pas des assertions. Les tests morts-nés (`is_deadborn_case`) sont sautés.

---

## Empreinte de vérification (le cœur du design)

Un badge boolean « verified » mentirait : la parité est invalidée par **trois** choses,
pas une. L'attestation est donc une **empreinte** :

```
fingerprint = sha256(
    sql_normalisé            # SQL du modèle après preprocessor, AST sqlglot normalisé
  + données_d_entrée_du_test # les lignes synthétiques (tables), sérialisation canonique
  + dialecte                 # bigquery / snowflake / postgres
  + version_transpileur      # version mocksql + version sqlglot
)
```

Invalidation automatique — l'empreinte devient périmée si :

| Changement | Pourquoi ça invalide |
|---|---|
| Le SQL du modèle | évident |
| Les données du test (patch agent, régénération) | le résultat comparé n'est plus le même |
| La version de mocksql/sqlglot | un fix ou une régression de transpilation change ce que DuckDB exécute |

**Stockage** : côté **définition** de `tests/{model}.json` (committé — voir
`storage/test_files.py`, split définition/cache), par cas de test :

```json
"parity": {
  "fingerprint": "sha256:…",
  "verified_at": "2026-07-16T14:32:00Z",
  "dialect": "snowflake"
}
```

C'est une **attestation qui voyage avec la repo** : les coéquipiers et la CI en héritent
sans credentials. Un diff constaté n'écrit **rien** (pas d'attestation négative committée
— le diff est dans la sortie CLI, point).

**Affichage dans `mocksql test`** (informatif, jamais bloquant) : chaque test porte
`verified` / `stale` (empreinte présente mais périmée) / `unverified` (jamais audité).

---

## Mécanique d'exécution

1. **Construction de la requête mockée en dialecte warehouse.** Même mécanique que
   l'executor DuckDB (remplacement des refs de tables par des CTEs inline portant les
   lignes synthétiques), mais **émise dans le dialecte source** (`sqlglot` write dialect
   = `bigquery`/`snowflake`/`postgres`) — c'est le point clé : côté warehouse il n'y a
   **pas de transpilation**, le SQL du modèle s'exécute quasi-nativement. La différence
   mesurée est donc bien celle de la couche d'émulation DuckDB.
2. **Typage des CTEs.** Les valeurs inline sont castées selon le `schema_cache.json`
   (types warehouse réels), sinon l'inférence de littéraux divergerait d'elle-même et
   polluerait la mesure — même principe « vrai schéma, zéro inférence » que le réplay
   (`cli/test_runner.py`).
3. **Exécution** via les connecteurs existants (mêmes credentials que l'import/le
   profiling ; imports gardés — BigQuery/Snowflake restent des extras optionnels, cf.
   `utils/optional_deps.py`).
4. **Rejeu DuckDB** du même test (chemin `mocksql test` existant), puis comparaison.

## Comparateur de résultats

C'est là que se joue la fiabilité du badge — un faux « DIFF » détruit la confiance plus
vite qu'un vrai. Règles v1 :

- **Ordre** : comparaison **insensible à l'ordre** (multiset de lignes), sauf si le SQL
  se termine par un `ORDER BY` terminal → comparaison ordonnée.
- **Flottants** : tolérance relative (`1e-9` par défaut) — les moteurs n'additionnent pas
  dans le même ordre.
- **Normalisation de types avant comparaison** : `DECIMAL`/`DOUBLE`/`NUMBER` → décimal
  canonique ; dates/timestamps → ISO 8601 UTC ; `NULL` ≡ `NULL` quel que soit le type
  porteur ; chaînes comparées telles quelles (une différence de casse/trim EST un diff —
  c'est peut-être la collation, et le DE doit le voir).
- **Noms de colonnes** : comparés insensibles à la casse (Snowflake upper-case par défaut).

Tout ce qui ne rentre pas dans ces règles **est un diff**. On préfère un diff explicable
à une normalisation trop agressive qui masquerait une vraie divergence.

---

## Surfaces produit

| Surface | Rendu |
|---|---|
| `mocksql parity` | sortie ci-dessus, diff côte à côte |
| `mocksql test` | statut `verified` / `stale` / `unverified` par test (informatif) |
| TestsView (UI) | chip discret sur la TestCard (`✓ parité Snowflake` / `parité périmée`) |
| Export HTML | badge « ✓ vérifié contre la warehouse » par test — signal de confiance fort pour le BA et l'acheteur |

## Hors-scope v1 (explicite)

- **Classification automatique des causes de diff** (transpilation vs dialecte vs
  non-déterminisme) — v2 possible : une *hypothèse* LLM sur le diff, clairement
  étiquetée comme telle, jamais une classification ferme.
- **Exécution sur données réelles** — hors vision (« pas un outil de debug post-prod »).
  `parity` compare des moteurs, pas des données.
- **Auto-correction sur diff** — on rapporte, on ne touche ni au test ni au SQL.
- **`parity` en CI par défaut** — possible pour qui le veut (les codes de sortie le
  permettent), jamais recommandé ni documenté comme le chemin standard.
- **Monitoring continu / re-vérification périodique.**

## Questions ouvertes

1. **Granularité de l'empreinte transpileur** : version de package (simple, invalide
   large) vs hash du SQL DuckDB transpilé (précis, invalide seulement si la transpilation
   de CE modèle a changé). Reco : commencer par la version de package, affiner si les
   invalidations massives à chaque release deviennent irritantes.
2. **Postgres** : parité quasi triviale (dialectes proches) — l'inclure v1 pour la
   complétude, ou se concentrer BigQuery/Snowflake où la valeur est réelle ?
3. **Dogfooding** : remplacer le harnais manuel Spider2-snow par `mocksql parity` sur la
   suite d'éval — premier consommateur interne, à faire dès la v1 utilisable.
