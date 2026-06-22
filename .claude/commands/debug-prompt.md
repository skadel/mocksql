Ouvre le dernier dump de prompt LLM d'un agent MockSQL, le résume, et pointe la section qui cloche.

## Usage

```
/debug-prompt [agent] [n]
```

- `agent` : label de l'agent (nom du nœud LangGraph). Défaut : le dump le plus récent tous agents confondus.
  Labels courants : `generator`, `suggestions_generator`, `test_evaluator`, `conversational_agent`,
  `assertion_corrector`, `routing`. Sous-labels possibles (`generator:update`…) si posés au call-site.
- `n` : combien de dumps récents lister/comparer (défaut : 1, le dernier).

Exemples :
- `/debug-prompt generator` — dernier prompt+output du générateur
- `/debug-prompt generator 3` — les 3 derniers (utile pour diffuser une boucle de retry)
- `/debug-prompt` — le dernier dump quel que soit l'agent

## Pré-requis : activer le dump (le rappeler à l'utilisateur si aucun log)

Le dump est écrit par `back/utils/prompt_dump.py` **uniquement** si l'env `MOCKSQL_DUMP_PROMPTS` est défini
au lancement du backend (serveur ou CLI). Sinon → no-op, aucun fichier. Si aucun log n'existe, dire à
l'utilisateur de relancer son scénario avec, p.ex. :

```
MOCKSQL_DUMP_PROMPTS=generator   # ou "generator,suggestions_generator" ou "*"
```

- Un token matche le label exact, sa base (avant `:`) ou son préfixe — `generator` attrape
  `generator` et `generator:update` mais pas `suggestions_generator`.
- Surcharge du dossier de sortie : `MOCKSQL_DUMP_DIR` (défaut `<base_dir>/logs/prompts`, gitignoré).

## Étapes à exécuter

### 1. Localiser les dumps

Les logs atterrissent dans `<base_dir>/logs/prompts/{label}/` — `base_dir` = cwd du backend
(souvent `back/` pour le serveur, le dossier projet pour la CLI). Chercher largement avec Glob :

```
**/logs/prompts/**/*.md
```

Trier par date de modif (Glob les renvoie déjà du plus récent au plus ancien). Filtrer par `{agent}`
si fourni : ne garder que les chemins dont le dossier parent (le label, `:` encodé en `__`) matche
le token (exact / base / préfixe, même règle que le backend).

### 2. Lire les N plus récents

Lire le(s) fichier(s) retenu(s) avec Read. Chaque dump contient : méta (label, modèle, latence,
tokens), section `## PROMPT` (system + messages rendus), section `## OUTPUT` (`text` et/ou
`tool_calls` = JSON structuré pour `with_structured_output`).

### 3. Restituer (objectif : debug rapide)

- **Méta en une ligne** : `label · modèle · latence · tokens in/out`.
- **Prompt** : structure (combien de messages, lesquels), et surtout signaler ce qui paraît
  anormal — bloc contexte métier vide/absent, profil non injecté, hint manquant, variables non
  substituées (`@start_date`, `{{ ds }}`), schéma incohérent, troncature.
- **Output** : si `tool_calls` → vérifier que le JSON structuré est cohérent avec le prompt
  (colonnes demandées présentes, assertions positives, pas de donnée tronquée). Si erreur (`-ERROR`
  dans le nom) → remonter le message d'erreur.
- **Verdict** : 1-2 phrases — où ça coince et quoi regarder ensuite. Ne pas paraphraser tout le
  prompt ; pointer la ligne/section problématique (référencer le chemin du dump en lien cliquable).

### 4. Si `n > 1` (diff de boucle)

Comparer les dumps successifs (typiquement une boucle de retry `bad_data`) : qu'est-ce qui a changé
entre les prompts/outputs ? Signaler le thrashing (sorties quasi identiques d'un retry à l'autre).

## Notes

- Le dump est best-effort : il ne casse jamais un run (toutes les écritures sont gardées par try/except).
- Ne jamais commiter les dumps : `logs/` est gitignoré (ils peuvent contenir des valeurs réelles de
  l'entrepôt = PII via le profil injecté dans le prompt).
- Pour ajouter un sous-label sur un prompt précis : au call-site, `runnable.with_config({"metadata":
  {"mocksql_label": "generator:update"}})` — il prime sur `langgraph_node`.
