# Boucle de correction pilotée par un agent de code

MockSQL expose une surface CLI permettant à un agent de code (Claude Code, etc.)
de **corriger du SQL en TDD**, en utilisant les tests MockSQL comme oracle :
génération de données synthétiques + exécution DuckDB locale + assertions.

Découpage :

- **L'agent de code = l'actionneur** : il modifie le `.sql` source, relance, lit
  le résultat, itère.
- **MockSQL = l'oracle** : il dit *ce qui doit être vrai* (les assertions) et
  *si c'est vrai* (exécution locale, 0 € facturé). C'est le signal pass/fail
  gratuit, local et sémantique qu'un agent n'a pas tout seul.

## La boucle (red → green)

```
1. assert add   → pose la spec (assertion cible), confirme qu'elle est ROUGE
2. <l'agent édite le .sql source>
3. mocksql test → relit le .sql du DISQUE, confirme le passage au VERT
                  sans casser les autres tests (régime régression)
4. répéter 2–3 jusqu'au vert
```

Deux régimes coexistent :

- **Régression** — les assertions existantes doivent **rester vertes** (« je n'ai
  rien cassé »).
- **Spec** — la nouvelle assertion-cible décrit le comportement *voulu* ; elle est
  **rouge** sur le SQL actuel et le devient **verte** une fois le code corrigé.

## Commandes

Toutes sortent du JSON sur stdout.

### `mocksql test`

Rejoue les cas sauvegardés contre DuckDB. **Par défaut, lit le `.sql` du disque**
(via le preprocessor) pour refléter les éditions de l'agent — c'est ce qui fait
fonctionner la boucle.

```bash
mocksql test -m orders            # lit models_path/orders.sql (disque)
mocksql test -m orders --frozen   # rejoue le snapshot SQL figé dans le JSON
mocksql test --json               # sortie structurée (CI / agent)
```

`sql_source` dans la sortie vaut `disk`, `frozen` ou `snapshot-fallback`. Le
fallback survient quand le `.sql` source est introuvable (suites portables type
`examples/`) : on rejoue le snapshot **avec un warning**, jamais un crash.

### `mocksql assert`

Gère les assertions (specs) d'un test, ciblées par `test_uid` (test) +
`assertion_uid` (assertion — uid court, rétro-rempli au premier `list`).

```bash
# Voir les assertions et leurs uids
mocksql assert list orders --test-uid <uid>

# Poser une spec (SQL dbt-style : SELECT les lignes ÉCHOUANTES, 0 ligne = pass)
mocksql assert add orders --test-uid <uid> \
  -d "le total carte doit valoir 150" \
  -s "SELECT * FROM __result__ WHERE payment_type='Credit Card' AND total != 150"

# Modifier / supprimer
mocksql assert update orders --test-uid <uid> --assertion-id <aid> -s "<nouveau SQL>"
mocksql assert remove orders --test-uid <uid> --assertion-id <aid>
```

`add` et `update` **ré-exécutent immédiatement** contre le SQL disque et
retournent `passed` + `failing_rows` — ce qui confirme que la cible est bien
rouge au départ (si elle est déjà verte, il n'y a rien à corriger).

### `mocksql generate`

**Additif par défaut, jamais destructif.** Si une suite existe déjà, `generate`
**ajoute** un test et **préserve** les tests + specs existants.

```bash
mocksql generate rides                       # bootstrap (1ʳᵉ fois) ; sinon ajoute un test
mocksql generate rides -i "un client a 2 cartes → trajet dupliqué"
                                             # ajoute un test CIBLÉ par le NL
mocksql generate rides --overwrite           # DESTRUCTIF : reconstruit toute la suite
```

Le mode additif route vers le `conversational_agent` (qui voit les tests existants
et ajoute un cas sans doublon). La fusion préserve toujours l'existant : un cas
régénéré avec un `test_uid` déjà présent est ignoré au profit de la version
porteuse de specs. **`--overwrite` est le seul mode qui écrase les specs `assert`.**

### `mocksql update-test`

Modifie **un** test existant (ciblé par `test_uid`) via le LLM — ajouter/changer
des lignes de données. Distinct de `generate` (qui ne fait qu'**ajouter** un test) :
`update-test` **remplace** le cas ciblé par sa version modifiée, mais **préserve les
specs `assert`** (assertions porteuses d'`assertion_uid`) — l'agent ne touche qu'aux
données et aux assertions auto-générées, jamais à ta cible rouge.

```bash
mocksql update-test rides --test-uid 9a0c \
  -i "ajoute une ligne : un client avec 2 cartes → le trajet est dupliqué"
```

C'est le primitif qui ferme le cas « mon scénario n'est pas encore dans les
données du test » : `update-test` injecte la donnée manquante sans détruire la spec.
