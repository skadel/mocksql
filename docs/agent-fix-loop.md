# Boucle de correction pilotée par un agent de code

MockSQL expose une surface CLI permettant à un agent de code (Claude Code, etc.)
de **corriger du SQL en TDD**, en utilisant les tests MockSQL comme oracle :
génération de données synthétiques + exécution DuckDB locale + contrat `expect`.

Découpage :

- **L'agent de code = l'actionneur** : il modifie le `.sql` source, relance, lit
  le résultat, itère.
- **MockSQL = l'oracle** : il dit *ce qui doit être vrai* (le contrat `expect`) et
  *si c'est vrai* (exécution locale, 0 € facturé). C'est le signal pass/fail
  gratuit, local et sémantique qu'un agent n'a pas tout seul.

## La boucle (red → green)

Deux régimes coexistent :

- **Régression** — les tests existants doivent **rester verts** (« je n'ai rien
  cassé »).
- **Spec** — un contrat *prescriptif* décrit le comportement **voulu** ; il est
  **rouge** sur le SQL actuel et devient **vert** une fois le code corrigé.

Depuis la Phase 2 de la validation humaine (contrat `expect` autoritaire au
replay), la boucle passe par l'**édition du contrat `expect`** :

```
1. mocksql generate <model> -i "repro: <prémisse>"
       → données du scénario. Le test naît VERT : son expect snapshotte la
         sortie actuelle (buggée). Ce n'est pas encore le repro.
2. Éditer expect.rows dans .mocksql/tests/<model>.json (fichier éditable à la main)
       → le contrat devient PRESCRIPTIF : la/les valeurs voulues remplacent les
         valeurs buggées. NE PAS `confirm` ici — confirm re-snapshotte l'observé
         (buggé) et écraserait l'édition.
3. mocksql test -m <model> --json
       → ROUGE QUALIFIÉ : status "unconfirmed" + expect_check.passed=false + diff
         (missing/unexpected) portant SUR la colonne de la prémisse.
         Diff ailleurs ou 0 ligne = banc d'essai à corriger (update-test / éditer
         data). expect_check.passed=true = bug NON reproduit → stop, rapporter.
4. <l'agent édite le .sql source>
5. mocksql test -m <model> --json     → VERT (status "pass") ; suite complète intacte.
6. mocksql confirm <model> --test-uid <uid>
       → gèle la sortie désormais correcte. Gate CI : `mocksql test
         --require-confirmed` (exit 1 sur stale/unconfirmed). `mocksql test` seul
         n'exit 1 QUE si le snapshot SQL est inchangé et la sortie diverge ; un edit
         du .sql bascule le contrat `stale` (rapporté, exit 0). Export dbt possible
         (`mocksql export dbt`).
```

### Installer le skill (Claude Code, Copilot, Cursor…)

Le workflow complet — rouge qualifié, faux rouges, référence du JSON — est packagé
en skill agent-ready : [`skills/mocksql-tdd/SKILL.md`](../skills/mocksql-tdd/SKILL.md).

- **Claude Code** : copier le fichier dans `.claude/skills/mocksql-tdd/SKILL.md`
  de votre projet.
- **GitHub Copilot** : coller le contenu dans
  `.github/instructions/mocksql-tdd.instructions.md` (ou un prompt file
  `.github/prompts/mocksql-tdd.prompt.md`).
- **Autres agents** : une CLI est agnostique — tout agent sachant lancer un shell
  peut suivre le fichier tel quel (règles projet, system prompt…).

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

Chaque cas de la sortie `--json` porte `test_uid` (cible pour `update-test` /
`confirm`), `review` (`confirmed` / `stale` / `draft`) et, sur les cas à contrat
`expect`, `expect_check` (diff `missing`/`unexpected` projeté sur les colonnes du
contrat). Statuts : `pass` · `fail` (contrat **confirmé** violé — seul cas qui
fait exit 1) · `unconfirmed` (contrat draft divergent — non bloquant, c'est le
« rouge » de la boucle spec) · `error` · `skip`.

> **Surface `mocksql assert` retirée** (juillet 2026). Depuis que le contrat
> `expect` est autoritaire au replay, les assertions n'étaient plus rejouées sur
> les cas migrés (`assert add` y retournait `passed: null` — spec inerte). La
> spec prescriptive s'exprime désormais en `expect.rows` : sur données
> synthétiques, la sortie est déterministe, les lignes exactes sont toujours
> calculables. Les suites **legacy sans `expect`** rejouent encore leurs
> `assertion_results` sauvegardées au replay — seule la surface de
> création/édition (`assert list/add/update/remove`) a disparu.

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
existante (contrat `expect`, statut de revue). **`--overwrite` est le seul mode
destructif — il écrase tout, contrats `expect` confirmés compris.**

### `mocksql update-test`

Modifie **un** test existant (ciblé par `test_uid`) via le LLM — ajouter/changer
des lignes de données. Distinct de `generate` (qui ne fait qu'**ajouter** un test) :
`update-test` **remplace** le cas ciblé par sa version modifiée, puis le rejoue.

> ⚠️ **Ordre des opérations en boucle repro** : à l'écriture, le contrat `expect`
> **draft** du cas est re-snapshotté depuis la nouvelle sortie observée
> (`sync_expect_on_doc` — un contrat `confirmed` n'est jamais touché). Une édition
> prescriptive d'`expect.rows` faite AVANT `update-test` est donc écrasée : édite
> le contrat APRÈS avoir ajusté les données.

```bash
mocksql update-test rides --test-uid 9a0c \
  -i "ajoute une ligne : un client avec 2 cartes → le trajet est dupliqué"
```

C'est le primitif qui ferme le cas « mon scénario n'est pas encore dans les
données du test » : `update-test` injecte la donnée manquante, puis tu poses
(ou re-poses) ta cible prescriptive en éditant `expect.rows`.
