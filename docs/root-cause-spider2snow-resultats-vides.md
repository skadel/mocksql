# Root-cause — « résultat d'exécution vide à la 1ʳᵉ génération » (éval Spider2-snow)

> Diagnostic du 2026-07-12. Périmètre : les 37 modèles marqués « KO — résultat vide »
> par le juge LLM sur les 110 modèles générés en CLI (`mocksql generate`) dans
> `C:\Users\skhir\workspace\Spider2\spider2-snow`. **Document d'analyse — aucun fix
> appliqué.** Reproduction instrumentée : `sf_local022`, log complet dans le
> scratchpad de session (`repro_sf_local022.log`).

## Résumé exécutif

Les 37 « KO — résultat vide » recouvrent deux problèmes indépendants :

- **Problème A (13 modèles) — artefact du harnais d'éval.** Les tests sont bons
  (verdict `Excellent`/`Bon`, assertions passées, résultat DuckDB non vide), mais le
  juge ne voit pas le résultat : `results_json` et `status` sont déportés par
  `write_test_doc` dans le sidecar **gitignoré** `.mocksql/cache/{model}.json`, et
  `run_eval.py` lit le fichier de tests **brut** sans fusionner ce sidecar.
- **Problème B (24 modèles) — vrais morts-nés de génération.** Trois familles :
  - **14 × `empty_results`** dont ≥ 12 causés par des **INSERT rejetés silencieusement**
    (valeurs texte dans des colonnes `NUMBER`) : `execute_queries` avale l'exception,
    les tables restent vides, la boucle anti-vide tourne mais sur un **diagnostic
    faux** (trace CTE au lieu de l'erreur d'INSERT) et ne converge jamais ;
  - **9 × `error`** (transpilation SQL : 7 FLATTEN, 1 extension spatial, 1 window-in-WHERE)
    qui court-circuitent **toute** boucle de retry par design ;
  - **1 × `bad_data_error`** (JSON malformé) — le seul cas où le circuit prévu a
    réellement vu l'erreur DuckDB.

  Dans les trois familles, la CLI écrit le fichier quand même (`rc=0`, message
  `[OK] … écrits`), sans verdict, sans assertion, et **sans statut d'erreur visible**
  (le `status` part dans le sidecar gitignoré).

L'invariant « premier test = résultat non vide » n'est donc pas violé par un défaut
de la boucle de convergence elle-même, mais par **trois angles morts autour d'elle** :
une exception avalée en amont (INSERT), une classe d'erreurs hors-boucle (transpile),
et une écriture de fichier qui ne distingue pas succès et échec en sortie.

---

## Problème A — 13 faux négatifs du harnais (PAS un bug MockSQL)

**Modèles** : `sf002 sf_bq028 sf_bq037 sf_bq043 sf_bq052 sf_bq252 sf_bq320 sf_bq321
sf_local009 sf_local015 sf_local030 sf_local038 sf_local075`

### Cause exacte

La chaîne est plus précise que « la CLI n'écrit pas `results_json` » — elle l'écrit,
mais pas là où le juge regarde :

1. La CLI produit bien `status` et `results_json` pour chaque cas (l'executor les
   pose dans le message RESULTS, la CLI les persiste).
2. `write_test_doc` (`back/storage/test_files.py:141`) **splitte** le document :
   les clés `_CACHE_CASE_KEYS` (`status`, `results_json`, `unit_test_build_reasoning`,
   `reason_type`…, `test_files.py:22`) partent dans le sidecar **gitignoré**
   `.mocksql/cache/{model}.json` ; le fichier commité `.mocksql/tests/{model}.json`
   garde la définition (data, assertions, verdict).
3. `run_eval.py:load_test_file` (`examples/eval/run_eval.py:27`) fait un
   `json.loads` **brut** du fichier de tests — il n'appelle pas `read_test_doc`,
   qui est pourtant la fonction de lecture officielle et fusionne le sidecar de
   façon transparente (`test_files.py:125`).
4. Le juge (`examples/eval/judge.py:149`, `_format_real_result`) cherche
   `results_json` / `results` / `real_res` → absent → « (non disponible) ».
   Comme `is_valid` exige « résultat réel non vide », il retombe sur `False`.
5. Même mécanique pour `exec_status` : `run_eval.py:88` lit `exec_status`/`status`
   → toujours `unknown` sur les fichiers CLI (le `status` est dans le sidecar).

### Preuves

- `cache/sf002.json` : `status='complete'`, `results_json` = **1 ligne**.
  Fichier commité : `verdict='Excellent'`, 3 assertions `passed=True` dont
  « The expected bank appears in the result » et « The result contains exactly
  1 row(s) » — la non-vacuité est prouvée deux fois.
- `cache/sf_bq028.json` : idem (`complete`, 1 ligne, `COUNT(*)=1` passée).
- Les 110 modèles ont leur sidecar présent sur la machine d'éval — la donnée
  existait, seule la lecture était incomplète.

### Quantification de l'inflation

- 13 des 37 « KO — résultat vide » = **35 % du bucket est un artefact de harnais**.
- Sur 110 modèles : **+11,8 points de pass-rate** récupérés en corrigeant la seule
  lecture (ex. un score brut de 66 % deviendrait ~78 %).
- Le rapport complet des 110 n'a pas été retrouvé
  (`examples/eval/results/2026-07-06_spider2-snow.json` est un smoke-run à
  2 modèles) — chiffres dérivés des listes de modèles ; re-runner l'éval après fix
  du harnais pour la valeur exacte.

### Où réconcilier

1. **Recommandé — côté harnais** : remplacer `json.loads` par
   `storage.test_files.read_test_doc` dans `run_eval.py` (le harnais importe déjà
   les modules `back/` via `judge.py`). Un seul point, zéro changement de format,
   `exec_status` et `results_json` réapparaissent d'un coup.
2. **Complément — robustesse clone frais** : le sidecar est *gitignoré* — sur un
   checkout sans lui (CI, autre machine), le juge retombera dans le même trou.
   Fallback dans `judge.py` : dériver la non-vacuité depuis les données commitées —
   `verdict` présent + assertion de présence/cardinalité `passed=True`
   (`exists`, `COUNT(*)=N`) ⇒ résultat non vide. Moins précis que le résultat brut
   mais suffisant pour `is_valid`.
3. **Non recommandé** : faire écrire `results_json` dans le fichier commité —
   contraire au split définition/cache voulu (lisibilité + PII), et inutile si (1).

---

## Problème B — 24 vrais résultats vides à la 1ʳᵉ génération

**Modèles** : `sf_bq236 sf_bq248 sf_bq265 sf_bq271 sf_bq291 sf_bq294 sf_bq359
sf_bq377 sf_bq390 sf_bq412 sf_bq421 sf_bq455 sf_ga001 sf_local010 sf_local019
sf_local022 sf_local026 sf_local064 sf_local209 sf_local210 sf_local283
sf_local299 sf_local311 sf_local336`

Signature commune (vérifiée sur fichiers) : `verdict=None`, `assertion_results=[]`,
`data` non vide, `rc=0`. Le `status` réel de la dernière tentative (lu dans le
sidecar cache) partitionne les 24 :

| `status` final | N | Modèles |
|---|---|---|
| `empty_results` | 14 | sf_bq265, sf_bq271, sf_bq294, sf_bq455, sf_local010, sf_local019, sf_local022, sf_local026, sf_local064, sf_local209, sf_local210, sf_local283, sf_local311, sf_local336 |
| `error` | 9 | sf_bq248, sf_bq291, sf_bq359, sf_bq377, sf_bq412, sf_bq421, sf_ga001 (Parser Error « near JOIN » = FLATTEN) · sf_bq236 (extension spatial) · sf_local299 (window function in WHERE) |
| `bad_data_error` | 1 | sf_bq390 (`Invalid Input Error: Malformed JSON`) |

### Q1 — La boucle anti-vide tourne-t-elle en CLI ? OUI, mais elle est aveugle, contournable et court-circuitable

**La boucle tourne.** `mocksql generate` exécute le graphe complet
(`build_query_graph().ainvoke`, `back/cli/generate.py:877`) avec `gen_retries: 10`
(`generate.py:242`, valeur en vigueur depuis le 2026-06-05). La reproduction DIAG de
`sf_local022` montre le circuit nominal en action :
`route_executor → test_evaluator (status=empty_results)` →
`[evaluator] empty_results → bad_data, retries=10` →
`route_evaluator → bad_data_to_agent` → agent → ré-exécution → etc.

**Mais une erreur d'INSERT est dans un angle mort total.** Le chemin prévu pour les
erreurs de données DuckDB existe : `_run_single_test_case` classe les
`Invalid Input Error`/`Conversion Error` en `status="bad_data_error"`
(`back/build_query/examples_executor.py:1996-2007`), et `test_evaluator` route alors
vers la boucle de correction **avec le message d'erreur DuckDB dans le diagnostic**
(`test_evaluator.py:355-406`). Ce chemin est le bon — sauf qu'il est
**inatteignable pour les erreurs d'insertion** :

```python
# back/utils/examples.py:670
def execute_queries(queries, con):
    try:
        for idx, query in enumerate(queries, start=1):
            try:
                result = con.execute(query).fetchall()
            except Exception as e:
                logger.error("Error executing query %d: %s", idx, e)   # ← avalé
    except Exception as e:
        logger.error("Error establishing database connection: %s", e)
```

L'exception `Conversion Error: Could not convert string "M001" to DECIMAL(38,9)`
est loggée puis **jamais relancée**. Conséquence en cascade, observée en repro :

1. Les 5 INSERT échouent (une seule instruction multi-lignes par table,
   `insert_examples.py:222` — une valeur invalide fait perdre **toute** la table) →
   tables créées mais **vides**.
2. Le SELECT tourne sur des tables vides → `0 ligne(s)` → `status="empty_results"`
   — un faux « SELECT à 0 ligne », alors que rien n'a jamais été inséré.
3. Le diagnostic transmis à la boucle est la **trace CTE**, calculée sur ces tables
   vides. Sur sf_local022 la trace elle-même échoue
   (`CTE trace PLAYER_RUNS : Expecting (. Line 1, Col: 17.` — parse sqlglot,
   conséquence du repli « raw SQL » après l'échec de qualification
   `Unknown column: match_id`), donc `failing_cte=None` et le message envoyé à
   l'agent est générique (« ses données d'entrée ne satisfont pas ses
   contraintes »). **Le vrai message d'erreur (Conversion Error, colonne, valeur)
   n'entre jamais dans le state.**
4. L'agent de correction travaille donc à l'aveugle. Verbatim de la repro :
   *« This error typically points to a SQL syntax issue rather than a data problem.
   However, as I am instructed to correct the data, I will attempt to ensure the
   join conditions … are robustly met »* — puis il patche `match_id` à… `"M001"`,
   la valeur identique. Les clés de jointure sont déjà cohérentes
   (`"M001"="M001"`) : il n'y a rien à corriger dans le référentiel qu'on lui donne.
5. **Sortie prématurée** : au 4ᵉ tour, l'agent conclut (à raison) que le problème
   n'est pas les données et appelle `ask_clarification`. En boucle `auto_correct`,
   `route_agent_output` envoie `ask_clarification → history_saver`
   (`query_chain.py:466-477` — le backstop ne couvre que
   `suggestion_intent + auto_tests_built`, pas `auto_correct`). Le graphe se
   termine **avec 5 retries restants**, alors qu'en CLI personne ne peut répondre
   à la question. Le garde-fou anti-no-op (`query_chain.py:483-491`) est
   contourné parce que `ask_clarification` est traité *avant* lui.
6. Fin de run : `[OK] 1 test case(s) écrits (reconstruction)`, `rc=0`. Fichier :
   `verdict=None`, 0 assertion, données inchangées avec `"M001"`.

À noter aussi : même quand la boucle va au bout (`gen_retries==0`), le
circuit-breaker de l'évaluateur émet un stub `FAILED_AUTO_GEN` aux données vidées
(`test_evaluator.py:561-589`) — mais dans le canal `examples`, que la CLI ne lit
pas (`_extract_test_cases` ne parcourt que `messages`/RESULTS,
`generate.py:293-313`). Le marquage d'échec n'atteint donc **jamais** le fichier :
les tags observés restent `['Business logic']`.

**Réponse courte** : la boucle tourne, mais (a) une erreur d'INSERT est
misclassée `empty_results` avec un diagnostic mensonger au lieu de
`bad_data_error` avec le vrai message ; (b) `ask_clarification` peut tuer la
boucle prématurément en mode auto ; (c) les erreurs de transpile (`status="error"`,
9 modèles) vont **directement** à `history_saver` sans aucune boucle
(`route_executor`, `query_chain.py:721-733`) — défendable (regénérer des données ne
répare pas du SQL), mais le fichier est écrit pareil. Le seul cas où le circuit
`bad_data_error` a réellement fonctionné est sf_bq390, parce que l'erreur survenait
au moment du **SELECT** (`PARSE_JSON` sur valeur insérée), pas de l'INSERT.

### Q2 — Type-mismatch (13) : où se joue la fidélité de type ?

Chaîne causale complète, chaque maillon vérifié :

**(a) Mapping Snowflake `NUMBER` → DuckDB `DECIMAL(38,9)` : correct, pas en cause.**
Le schema_cache est fidèle à l'entrepôt (`IPL.IPL.BALL_BY_BALL` : `match_id: NUMBER`,
`striker: NUMBER`, `team_batting: NUMBER`…), et l'élargissement
`NUMBER` sans précision → `DECIMAL(38,9)` (`utils/examples.py:488`) est un
anti-débordement voulu. Assouplir ce mapping vers VARCHAR pour des colonnes
« sémantiquement textuelles » masquerait un bug de type réel — exactement ce que le
principe « réplay = vrai schéma, zéro inférence » interdit. **Ne pas toucher.**

**(b) Contrat de génération LLM : le maillon faible.** `NUMBER`/`NUMERIC`/`DECIMAL`
sont absents de `type_mapping` (`back/common_vars.py:32` — seulement
INTEGER/INT64/INT/STRING/FLOAT/FLOAT64/DATE/TIMESTAMP), donc
`parse_field_type` retombe sur `str` (`utils/examples.py:114`). Le commentaire à
`utils/examples.py:326` documente ce hasard sans le résoudre. Preuve directe (log
DIAG de la repro) — le schéma JSON envoyé au LLM :

```json
"match_id": {"anyOf": [{"type": "string"}, ...], "description": "", ...}
```

**Aucun signal numérique** : type `string`, description vide. La sémantique d'ID
gagne (`"M001"`, `"P001"`) et le LLM colle même des labels d'équipe (`"RCB"`) dans
`team_batting NUMBER`. Cas extrême sf_bq271 : mots aléatoires (`'cost'`, `'fall'`,
`'including'`) dans des colonnes NUMBER. Le patch existant `_numeric_epoch_hint`
(`utils/examples.py:363`, leçon sf_bq028) ne couvre que les colonnes NUMBER au nom
*horodaté* — les `*_id` passent au travers. C'est le symétrique exact de l'incident
mémorisé « NUMBER→str→date ISO→DECIMAL reject » : même racine (fallback `str`),
autre sémantique parasite.

**(c) Validation pré-INSERT : inexistante.** `to_duck_expr` (branche par défaut,
`utils/insert_examples.py:216`) émet un littéral string quoté et délègue le cast à
DuckDB. La première vérification de type est donc l'INSERT lui-même — dont l'échec
est avalé (Q1). Personne ne rapproche jamais « valeur générée » et « type de colonne ».

**Couche responsable** : la **génération** (b) crée le mismatch, l'**exécution**
(c + `execute_queries`) le rend invisible. Les deux points d'injection sont
complémentaires, pas alternatifs : (b) fait converger la 1ʳᵉ tentative, (c) garantit
que si une valeur invalide passe quand même, la boucle reçoit le vrai diagnostic
(colonne + valeur + type) au lieu d'une trace CTE vide. Croisement
`project_trino_used_columns_case_mismatch` : même famille de défauts — un
rapprochement silencieusement raté en amont qui dégénère en symptôme illisible en
aval ; le fix d'alors (normaliser les deux côtés + test) est le bon gabarit.

### Q3 — FLATTEN (recouvrement, pas de ré-analyse)

7 des 9 `status="error"` portent la signature `Parser Error: syntax error at or
near "JOIN"` (reliquat de `LATERAL FLATTEN` transpilé en comma-join invalide) :
`sf_bq248 sf_bq291 sf_bq359 sf_bq377 sf_bq412 sf_bq421 sf_ga001`. Recouvrement
direct avec le fix déjà livré (commit `757bf72`, `LATERAL FLATTEN → UNNEST`,
`back/utils/examples.py:_fix_snowflake_idioms`) — **postérieur** à la génération des
fichiers d'éval : ces 7 modèles sont à régénérer avant tout autre diagnostic.
Les 2 autres `error` sont hors périmètre FLATTEN : `sf_bq236` (extension spatial —
résolu par `duckdb.extensions` dans mocksql.yml, config à poser côté projet
spider2-snow) et `sf_local299` (`Binder Error: WHERE clause cannot contain window
functions` — transpile résiduel non couvert, à traiter séparément).

### Q4 — Le fichier « mort-né » est-il souhaitable ? Oui, mais pas silencieux

État actuel, trois pertes de signal cumulées :

1. **CLI** : `rc=0` + `[OK] N test case(s) écrits` — indistinguable d'un succès
   (l'éval batch enchaîne sans broncher).
2. **Fichier** : `verdict=None` + 0 assertion sont les seuls indices, et le
   `status` réel (`empty_results`/`error`) part dans le sidecar gitignoré — un
   consommateur du fichier commité ne peut pas savoir que le test n'a jamais tourné.
3. **Marquage prévu jamais appliqué** : le stub `FAILED_AUTO_GEN` du
   circuit-breaker n'atteint pas le fichier (mauvais canal, cf. Q1).

**Recommandation : option (b) — écrire avec un statut d'erreur explicite,
jamais (a) ne pas écrire.** Ne pas écrire détruirait le travail de génération
(les données de sf_local022 sont à un patch de types de fonctionner — les
régénérer coûterait un run LLM complet) et casserait le mode additif. Le bon
contrat :

- exposer `exec_status: "error"` (+ `exec_error` court) dans la **définition
  commitée** — c'est un fait du test, pas un dérivable : il ne doit PAS rejoindre
  `_CACHE_CASE_KEYS` ;
- faire remonter l'échec en sortie CLI (`[FAIL]` + `rc≠0`, ou a minima un warning
  explicite par test mort-né) ;
- appliquer le marquage `FAILED_AUTO_GEN` sur le bon canal pour qu'il atteigne le
  fichier.

**Impact sur `run_eval.py`** : nul en l'état — il lit déjà
`first_test.get("exec_status") or ... "unknown"` (`run_eval.py:88`) ; si le champ
arrive dans la définition, il se met à fonctionner sans modification. Bonus : le
juge peut alors court-circuiter les morts-nés (KO-avec-cause, sans appel LLM) —
éval plus fidèle *et* moins chère, et les buckets de causes (type-mismatch /
transpile / json) deviennent des colonnes du rapport au lieu d'un grep de logs.

---

## Classement des 24 par couche responsable

| Couche | Modèles | N | Mécanisme |
|---|---|---|---|
| **Génération LLM** (contrat Pydantic `str` pour NUMBER, aucun hint numérique) — *crée* le défaut | sf_bq265, sf_bq271, sf_bq294, sf_local010, sf_local022, sf_local026, sf_local064, sf_local209, sf_local210, sf_local283, sf_local311, sf_local336 (12 confirmés déterministiquement : valeurs non-numériques dans colonnes NUMBER des données commitées) | 12–13 | `"M001"` dans `DECIMAL(38,9)` |
| **Exécution** (`execute_queries` avale l'INSERT ; misclassification `empty_results` ; diag CTE mensonger ; sortie `ask_clarification`) — *rend le défaut incorrigible* | les mêmes 12–13 + aggrave tout `empty_results` | (transverse) | INSERT échoue → tables vides → boucle aveugle |
| **Transpilation SQL** Snowflake→DuckDB | sf_bq248, sf_bq291, sf_bq359, sf_bq377, sf_bq412, sf_bq421, sf_ga001 (FLATTEN — fix `757bf72` déjà livré, à régénérer) · sf_local299 (window-in-WHERE, résiduel) | 8 | `status="error"`, aucune boucle (par design) |
| **Config projet** (extension DuckDB non chargée) | sf_bq236 (spatial — option `duckdb.extensions` existante, non posée dans le mocksql.yml spider2-snow) | 1 | `status="error"` |
| **Données LLM / JSON** | sf_bq390 (`Malformed JSON` au SELECT — seul cas passé par le vrai circuit `bad_data_error`, retries épuisés) | 1 | non-convergence réelle |
| **Résiduel à inspecter** | sf_bq455, sf_local019 (`empty_results` sans mismatch NUMBER détecté — soit mismatch sur un autre type, soit non-convergence de contraintes authentique) | 2 | ? |
| **Écriture de fichier** (mort-né silencieux : rc=0, statut dans le sidecar, stub FAILED_AUTO_GEN perdu) | **les 24** | 24 | transverse |

(Le décompte « 13 INSERT type-mismatch » des logs de génération correspond aux
12 confirmés ci-dessus + vraisemblablement un des 2 résiduels sur un type non-NUMBER.)

## Points d'injection recommandés (sans implémentation)

1. **`utils/examples.py:execute_queries` — faire remonter l'échec d'INSERT.**
   Le point d'injection le plus rentable du lot : relancer (ou collecter puis
   relancer) l'exception rend le chemin `bad_data_error` existant atteignable —
   l'évaluateur transmet alors déjà le message DuckDB verbatim au correcteur
   (`test_evaluator.py:360`). Une modification locale reconnecte un circuit
   entièrement câblé. Vérifier les autres appelants de `execute_queries` avant de
   changer le contrat (grep systémique, cf. leçon habituelle).
2. **`common_vars.py:type_mapping` / `utils/examples.py:parse_field_type` — typer
   les champs NUMBER dans le contrat Pydantic.** `NUMBER(p,0)`/`INT`-like → `int`,
   `NUMBER(p,s>0)`/`DECIMAL` → `float` (ou `Decimal`), en conservant le garde-fou
   epoch existant. C'est le fix « 1ʳᵉ tentative converge ». Attention au précédent
   sf_bq028 : tout resserrage de type doit passer par les tests
   `test_pydantic_model_creation.py` (les deux sémantiques parasites — date ISO et
   ID alphanumérique — doivent être couvertes).
3. **`query_chain.py:route_agent_output` — boucher la sortie `ask_clarification`
   en mode `auto_correct`.** Même logique que le backstop batch existant : en
   boucle automatique une question sans destinataire doit dégrader vers le
   `generator`, pas terminer le run avec des retries restants.
4. **`storage/test_files.py` + `cli/generate.py` — statut d'échec first-class.**
   `exec_status`/`exec_error` dans la définition commitée (hors `_CACHE_CASE_KEYS`),
   `rc≠0` ou `[FAIL]` en sortie CLI, et routage du stub `FAILED_AUTO_GEN` vers le
   canal que la CLI lit réellement.
5. **`examples/eval/run_eval.py:load_test_file` — lire via `read_test_doc`.**
   Corrige le Problème A en un point ; ajouter au juge le fallback
   « non-vacuité dérivée des assertions » pour les checkouts sans sidecar.
6. **Hors périmètre immédiat** : régénérer les 7 modèles FLATTEN (fix déjà livré),
   poser `duckdb.extensions: [spatial]` dans le mocksql.yml de spider2-snow,
   ouvrir un ticket transpile pour `sf_local299` (window-in-WHERE), inspecter
   sf_bq455/sf_local019.

Ordre de valeur estimé : (1) et (2) traitent 12–13 des 24 morts-nés et tout futur
projet Snowflake ; (5) récupère 13 faux négatifs d'éval immédiatement ; (4) rend
tout échec restant visible et mesurable ; (3) est petit mais supprime un mode de
défaillance sournois de la boucle.
