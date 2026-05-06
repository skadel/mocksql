import datetime
import difflib
import json
from typing import Literal, Optional, List

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts.chat import MessageLike

from build_query.converstion_history import format_history
from utils.msg_types import MsgType
from utils.prompt_utils import escape_unescaped_placeholders
from utils.saver import get_message_type


def build_other_prompt(
    user_input: str, dialect: str, history: list[BaseMessage] = None
) -> ChatPromptTemplate:
    if history is None:
        history = []

    # Initial system message with prompt instructions
    prompt_messages = [
        (
            "system",
            """Vous êtes un data analyst expert.
Votre rôle est de répondre à l'instruction donnée après avoir analysé le schéma de la base de données.
Ne pas inclure de code dans la réponse.

**Description de la base de données**:
{descriptions}""",
        )
    ]

    user_messages = []

    # Include history of interactions with reasoning
    history_with_results = format_history(
        history,
        dialect,
        current_agent=MsgType.OTHER,
        excluded_agents=[MsgType.PROVIDED_SQL],
    )
    user_messages.extend(history_with_results)
    # Add the user input message
    user_messages.append(
        HumanMessage(content=f"<instruction>\n{user_input}\n</instruction>")
    )
    user_messages = concatenate_successive_messages(user_messages)

    # Extend the final prompt
    prompt_messages.extend(user_messages)

    final_prompt = ChatPromptTemplate.from_messages(prompt_messages)

    return final_prompt


async def fix_query_prompt(
    history: list[BaseMessage],
    descriptions: str,
    format_instruction: str,
    dialect="bigquery",
    samples: str = None,
):
    history_parsed = "\n".join(
        [
            get_message_type(m) + " : " + m.content.replace("{", "").replace("}", "")
            for m in history
        ]
    )

    return ChatPromptTemplate.from_messages(
        [
            (
                "system",
                f"""You are a {dialect.upper()} expert.
You can capture the link between the question and corresponding database and fix the error in {dialect.upper()} SQL queries.
Make sure the column names being used in the query exists in the table.

**Description de la base de données**:
{descriptions}
"""
                + (
                    f"""

Samples:
{samples}"""
                    if samples
                    else ""
                ),
            ),
            HumanMessage(
                f"""Here is the conversation history to build the Query :
{history_parsed}
Let's think step by step and generate a fix of the last {dialect.upper()} SQL query.\n"""
                f"{format_instruction}\n"
                "Réponds uniquement avec le JSON demandé sans autres explications.\n"
            ),
        ]
    )


def fix_query_line_prompt(
    query_line: str,
    compilation_error: str,
    query: str,
    history: list[BaseMessage],
    descriptions: str,
    dialect: str = "bigquery",
    samples: str = None,
):
    history_parsed = "\n".join(
        [
            get_message_type(m) + " : " + m.content.replace("{", "").replace("}", "")
            for m in history
        ]
    )

    return ChatPromptTemplate.from_messages(
        [
            (
                "system",
                f"""You are a {dialect.upper()} SQL expert. I have a query that is throwing a compilation error in specific line. 
Your task is to help fix the query by correcting the line causing the issue. 

I will provide the full query and the specific line that caused the error. 
Please respond with the corrected version of the line, or `````` if the line should be removed entirely. 
The faulty line will be replaced with your suggestion. If the fix doesn't involve the provided line, respond with an empty string "".

If the solution requires removing the line entirely, you may respond with an empty string `""` or a commented line like `-- removed`. 
Only respond with the single corrected line (or nothing/comment if removal is needed).

Database description:
{descriptions}
"""
                + (
                    f"""

Samples:
{samples}"""
                    if samples
                    else ""
                ),
            ),
            (
                "human",
                f"""Here is the conversation history to help you classify : {history_parsed}

Here's the faulty {dialect.upper()} SQL query
<sql>
{query}
</sql>

<compilation_error>
{compilation_error}
</compilation_error>

<Query line>
{query_line}
</Query line>

{{format_instruction}}
Réponds uniquement avec le JSON demandé sans autres explications.

""",
            ),
        ]
    )


def _format_profile_block(profile: Optional[dict], used_columns: list) -> str:
    """
    Formats the statistical profile into a concise block for injection into prompts.
    Only includes columns that are actually used in the query.
    """
    print(profile)
    if not profile or not profile.get("tables"):
        return ""

    requested: dict[str, set] = {}
    for entry in used_columns:
        if isinstance(entry, str):
            entry = json.loads(entry)
        tbl = entry.get("table", "")
        short = tbl.split(".")[-1]
        requested[short] = set(entry.get("used_columns", []))

    lines: List[str] = []
    for tbl_key, tbl_data in profile["tables"].items():
        short_key = tbl_key.split(".")[-1]
        wanted_cols = requested.get(tbl_key) or requested.get(short_key)
        if not wanted_cols:
            continue
        cols = tbl_data.get("columns", {})
        col_lines: List[str] = []
        for col_name, stats in cols.items():
            if col_name not in wanted_cols:
                continue
            parts: List[str] = []
            min_v, max_v = stats.get("min_value"), stats.get("max_value")
            if min_v is not None and max_v is not None:
                parts.append(f"min={min_v}, max={max_v}")
            dc = stats.get("distinct_count")
            if dc is not None:
                parts.append(f"distinct={dc}")
                if stats.get("is_categorical") and dc <= 20:
                    parts.append("(catégoriel)")
            if stats.get("is_never_null"):
                parts.append("never_null")
            elif stats.get("nullable_ratio", 0) > 0:
                pct = round(stats["nullable_ratio"] * 100, 1)
                parts.append(f"null={pct}%")
            if parts:
                col_lines.append(f"    - `{col_name}`: {', '.join(parts)}")
        if col_lines:
            lines.append(f"  table `{short_key}`:")
            lines.extend(col_lines)

    joins = profile.get("joins", [])
    join_lines: List[str] = []
    for j in joins:
        lt, rt = j.get("left_table", ""), j.get("right_table", "")
        lk, rk = j.get("left_key", ""), j.get("right_key", "")
        if lt and rt and lk and rk:
            join_lines.append(f"  - {lt}.{lk} ↔ {rt}.{rk}")

    if not lines and not join_lines:
        return ""

    result = "Profil statistique des colonnes (min/max réels) :\n"
    result += "\n".join(lines)
    if join_lines:
        result += "\nJointures profilées (cardinalités réelles) :\n"
        result += "\n".join(join_lines)
    print("formatted_profil")
    print(result)
    return result


def generate_data_prompt(
    history: list[BaseMessage],
    dialect: str,
    format_instructions: str,
    used_columns: list,
    constraints_hint: str = "",
    sql: str = "",
    user_instruction: str = "",
    profile: Optional[dict] = None,
) -> ChatPromptTemplate:
    """
    Construit un prompt pour générer un test unitaire,
    en incluant un historique de messages (system/human/ai, etc.).
    """

    if constraints_hint:
        if user_instruction:
            constraints_block = (
                f"\nContraintes extraites du SQL (types, jointures, filtres) — respecte-les pour la cohérence des données :\n"
                f"```json\n{constraints_hint}\n```\n"
            )
        else:
            constraints_block = (
                f"\nRespecte STRICTEMENT ces contraintes extraites du SQL "
                f"(en particulier les `anti_joins` : génère des données qui NE correspondent PAS à la table jointe) :\n"
                f"\n Focus dans la génération des tests sur la liste de conditions suivante pour que mes tests sur la requete SQL ci-dessous ne donnent pas un résultat vide :"
                f"```json\n{constraints_hint}\n```\n"
            )
    else:
        constraints_block = ""

    sql_block = f"\nRequête SQL :\n```sql\n{sql}\n```\n" if sql else ""

    profile_block_str = _format_profile_block(profile, used_columns)
    profile_block = (
        f"\n{profile_block_str}\n"
        "Utilise ces statistiques réelles pour générer des valeurs vraisemblables "
        "(respecte les plages min/max et la cardinalité des colonnes catégorielles).\n"
        if profile_block_str
        else ""
    )

    if user_instruction:
        consignes_1_2 = """\
1. **Respectez l'instruction utilisateur** : le test doit implémenter exactement le scénario décrit
   dans l'instruction ci-dessous, même s'il s'agit d'un cas limite, d'un résultat vide ou de valeurs NULL.
2. **Cas spécifiques permis** : contrairement au test standard, ce test peut couvrir des scénarios
   d'erreurs, de jointures défaillantes ou tout autre cas demandé par l'instruction."""
    else:
        consignes_1_2 = """\
1. **Usage nominal uniquement** : concentrez-vous sur un cas d'usage métier standard, où le résultat
   de la requête est non vide et non nul (pas de valeurs NULL ou vides).
2. **Exclusion des cas exceptionnels** : pas de scénarios d'erreurs ou de jointures défaillantes."""

    system_message_content = (
        f"""
Vous êtes un data QA, testeur de requêtes SQL et expert en génération de données de test JSON.
Analysez la requête SQL et le schéma des données sources fournis,
puis générez un unique test unitaire en format JSON.

{sql_block}{constraints_block}{profile_block}

**Consignes principales :**
"""
        + consignes_1_2
        + """
3. **Contrôle strict des types et fonctions** :
   - Si une colonne est convertie (`CAST`, `SAFE_CAST`) en entier, la valeur d'origine doit être
     un format numérique (p.ex. "123" ou "00123" acceptables, mais pas "ABC").
   - Si une fonction de date (`PARSE_DATE`, `SAFE.PARSE_DATE`, etc.) est utilisée, la chaîne
     d'entrée doit respecter exactement le format attendu.
   - Si un JOIN est fait sur un champ de type entier, assurez-vous que les valeurs correspondent
     dans les deux tables.
4. **Respect strict de la casse (majuscules/minuscules)** :
   - Le nom des tables dans la clé `data` doit **strictement** refléter la casse attendue.

5. **Aucune valeur NULL ou vide** dans les colonnes générées : fournissez des données
   complètes sur chaque colonne utilisée.

6. **Format obligatoire des clés de tables dans `data`** : chaque clé doit être
   `{dataset}_{table}` (les deux derniers segments du nom qualifié, joints par `_`).
   Exemple : pour `bigquery-public-data.covid19_open_data.covid19_open_data`,
   la clé doit être `covid19_open_data_covid19_open_data` — jamais `covid19_open_data` seul.

7. **Ne pas inclure la requête SQL** dans le résultat.
8. **Un seul test** dans la clé `unit_tests`.
9. **Conditions OR / groupes de contraintes multiples** : quand le SQL contient plusieurs branches alternatives (`condition_A OR condition_B`, `CASE WHEN … THEN … ELSE …`, plusieurs chemins de jointure), choisir **une seule branche** par test et construire des données qui satisfont uniquement cette branche. Ne pas essayer de couvrir plusieurs alternatives à la fois. La description doit nommer explicitement la branche choisie (ex. "Pour un utilisateur premium …" plutôt que "Pour un utilisateur premium ou avec cumulated_montant > 1000 …"). Les autres branches alimentent les suggestions.

**Ce que cet outil NE doit PAS tester** (laisser au moteur de warehouse) :
- Les expressions constantes dans le SELECT final (ex. `SELECT 'valeur_fixe' AS col`) : résultat trivial, aucune logique métier à valider.
- Les fonctions d'agrégation pures (SUM, AVG, COUNT, MIN, MAX) sans filtre ou condition métier : elles sont garanties correctes par le moteur SQL.
- Le comportement interne des fonctions SQL (ordre de STRING_AGG, précision de CAST, format de DATE_FORMAT…) : tester la DB, pas la logique métier.
- Les règles de calcul que toute implémentation SQL correcte produirait identiquement.
Privilégier des scénarios où **la logique métier** — les filtres, jointures, conditions temporelles, règles de déduplication — est réellement testée.

**Format de sortie obligatoire** : un objet JSON unique, sans commentaire ni texte additionnel, sous la forme :
```json
{
  "unit_test_description": "Pour [sujet avec valeurs concrètes] [condition/situation] → [résultat métier attendu]",
  "unit_test_build_reasoning": "...",
  "tags": ["Logique métier", "..."],
  "suggestions": [
    "Pour [sujet] [contexte spécifique] → [résultat attendu dans les données de sortie]",
    "Pour [sujet] [autre contexte] → [résultat attendu dans les données de sortie]"
  ],
  "data": {
    "Table1Name": [
      { "ColA": "...", "ColB": "..." }
    ],
    "Table2Name": [
      { "ColX": "...", "ColY": "..." }
    ]
  }
}
```
- `unit_test_description`: Description **métier contextualisée** au format *"Pour [sujet avec valeurs concrètes] [condition/situation] → [résultat attendu]"*. Le sujet doit mentionner des valeurs concrètes (IDs, dates, montants, statuts). Exemple : "Pour un client ayant 3 ouvertures en sept. 2025 puis toutes fermées, qui réouvre en octobre → il est compté comme nouveau PDV sur le mois d'analyse". Éviter les formulations génériques comme "Vérifie que le calcul est correct".
- `unit_test_build_reasoning`: Expliquez brièvement la logique de génération des données en vous concentrant sur les contraintes.
- `tags`: Labels décrivant les types de cas couverts. Choisir parmi : `Logique métier`, `Null checks`, `Cas limites`, `Intégration`, `Valeurs dupliquées`, `Performance`.
- `suggestions`: Exactement 2 **scénarios métier** complémentaires au format *"Pour [sujet] [contexte] → [résultat attendu]"*. Les suggestions doivent cibler des cas métier distincts avec des valeurs concrètes — pas le comportement technique des fonctions SQL. Exemple à éviter : "S'assure que STRING_AGG trie correctement les valeurs" → Exemple correct : "Pour un client NO_SIRET NS ayant souscrit aux contrats C1, C2, C3 avec des type_operation différents → le champ type_operation de sortie contient toutes les valeurs séparées par '·'".
- `data`: Données cohérentes, correctes pour la requête.

⚠️ **Toute casse incorrecte dans les noms de tables sera considérée comme une erreur.**
⚠️ **Les clés de `data` doivent être `{dataset}_{table}` (ex. `covid19_open_data_covid19_open_data`), jamais le nom court seul (ex. `covid19_open_data`).**

Ne produisez qu'une seule réponse en JSON conforme, sans texte additionnel."""
    )

    system_msg = SystemMessage(content=system_message_content)

    history_with_results = format_history(
        history,
        dialect,
        output_format="generator",
        excluded_agents=[MsgType.PROVIDED_SQL],
    )

    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M")

    instruction_block = (
        f"\n**Instruction utilisateur :** {user_instruction}\n"
        if user_instruction
        else ""
    )
    non_empty_constraint = (
        "- La requête d'entrée va être exécutée sur les données générées, donc on veut un résultat"
        " non vide et non null. Fais attention aux jointures, aux cast, safe_cast, et aux formats implicites comme les dates.\n"
        if not user_instruction
        else "- Respecte strictement l'instruction utilisateur pour ce scénario — un résultat vide est acceptable si l'instruction le demande.\n"
    )

    final_human_message_content = f"""
Génère un test unitaire conforme aux consignes ci-dessus, avec :
- Un seul test
- Résultat JSON uniquement (champs dans l'ordre : unit_test_description, unit_test_build_reasoning, tags, suggestions, data)
- `unit_test_description` : description métier au format "Pour [sujet avec valeurs concrètes] [condition] → [résultat attendu]" — mentionner des valeurs concrètes (IDs, dates, statuts), pas de formulation générique
- `tags` : labels pertinents parmi Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance
- `suggestions` : exactement 2 scénarios métier au format "Pour [sujet] [contexte] → [résultat]" — viser des cas métier distincts avec des valeurs concrètes, pas le comportement interne des fonctions SQL
- Pas de requête SQL source dans la sortie
- Données complètes, sans colonnes nulles ni vides (sauf si l'instruction le demande explicitement)
- Attention stricte à la **casse exacte** des noms de tables dans le JSON
- Ne pas tester les expressions constantes, les agrégats purs (SUM/AVG/COUNT), ni le comportement interne des fonctions SQL
- Si le SQL a des conditions OR ou plusieurs branches CASE, choisir **une seule branche** et nommer explicitement la branche dans la description (les autres branches vont dans les suggestions)
{non_empty_constraint}
Voici les colonnes qui doivent être générées (les clés `data` doivent utiliser exactement le format `{{dataset}}_{{table}}` ci-dessous) :
{[{"table_key": f"{u.get('database', '')}_{u.get('table', '')}" if u.get("database") else u.get("table", ""), "columns": u.get("used_columns", [])} for u in used_columns]}
{instruction_block}{sql_block}{constraints_block}{profile_block}
{format_instructions}

Date et heure actuelles : {formatted_datetime}
"""

    final_human_msg = ("human", final_human_message_content)

    prompt_messages = [system_msg]
    prompt_messages.extend(history_with_results)
    prompt_messages.append(final_human_msg)

    return ChatPromptTemplate.from_messages(prompt_messages, "mustache")


def update_data_prompt(
    history: list[BaseMessage],
    user_input: str,
    dialect: str,
    format_instructions: str,
    sql: str = "",
) -> ChatPromptTemplate:
    """
    Construit un prompt pour mettre à jour des données JSON (utilisées pour tester une requête SQL),
    en appliquant précisément les instructions de modification fournies, sans ajouter d'explications
    ou de commentaires superflus. Les tests qui ne sont pas concernés par la modification ne doivent
    pas être réécrits. En cas d'ajout de nouveaux tests, il convient d'utiliser des index distincts de
    ceux déjà existants pour éviter tout écrasement.

    Points d'attention :
    1. **Renforcement du cadrage** : l'objectif est de modifier les données sources JSON, pas d'expliquer
       ou de justifier les modifications.
    2. **Filtrage de l'historique** : on n'inclut que les messages utiles (sans la requête SQL).
    3. **Strict respect de l'instruction** : si une partie du test n'a pas besoin d'être modifiée, elle ne
       doit pas être réécrite.
    4. **Format JSON strict** : toute sortie doit respecter le format défini dans `format_instructions`.
    """

    # 1. Message system : rappel du rôle et des consignes principales.
    system_message_content = (
        "Vous êtes un data QA, testeur de requêtes SQL et expert en génération et modification de données de test JSON.\n"
        "Votre objectif est de mettre à jour les données sources JSON selon les instructions données,\n"
        "sans ajouter d'explications ou de justifications superflues.\n"
        "Ne modifiez que les parties explicitement concernées par les instructions, sans réécrire les tests inchangés.\n"
        "En cas d'ajout d'un nouveau test, utilisez un index qui n'existe pas encore.\n"
    )
    system_msg = SystemMessage(content=system_message_content)

    # 2. Récupération et filtrage de l'historique pour exclure la requête SQL.
    history_with_results = format_history(
        history,
        dialect,
        output_format="generator",
        excluded_agents=[MsgType.PROVIDED_SQL],
    )

    # 3. Ajout d'un horodatage (optionnel).
    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M")

    # 4. Construction du message final (Human) contenant les instructions de modification.
    sql_block = f"\nRequête SQL :\n```sql\n{sql}\n```\n" if sql else ""

    final_human_message_content = f"""
Modifie les données JSON selon l'instruction ci-dessous :
{sql_block}
<Instruction>
{user_input}
</Instruction>

Génère un unique test unitaire modifié selon l'instruction ci-dessus.
Respecte l'ordre des champs : unit_test_description, unit_test_build_reasoning, tags, suggestions, data.
- `unit_test_description` : description métier au format "Pour [sujet avec valeurs concrètes] [condition] → [résultat attendu]" — mentionner des valeurs concrètes (IDs, dates, statuts), pas de formulation générique
- `tags` : labels pertinents parmi Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance
- `suggestions` : exactement 2 scénarios métier au format "Pour [sujet] [contexte] → [résultat]" — cibler des cas métier distincts avec des valeurs concrètes, pas le comportement interne des fonctions SQL
- Ne pas tester les expressions constantes, les agrégats purs (SUM/AVG/COUNT), ni le comportement interne des fonctions SQL

{format_instructions}

Date et heure actuelles : {formatted_datetime}
"""
    final_human_msg = HumanMessage(content=final_human_message_content)

    # 5. Assemblage final des messages (system, historique filtré, humain).
    prompt_messages = [system_msg]
    prompt_messages.extend(history_with_results)
    prompt_messages.append(final_human_msg)

    # 6. Génération et retour du ChatPromptTemplate.
    return ChatPromptTemplate.from_messages(
        concatenate_successive_messages(prompt_messages), "mustache"
    )


def query_change_data_prompt(
    history, dialect: str, format_instructions: str
) -> ChatPromptTemplate:
    """
    Construit un prompt permettant d'adapter des données de test JSON existantes à une nouvelle requête SQL,
    en se basant sur l'ancienne requête. L'objectif est d'ajuster (ou générer) des données unitaires
    pour garantir qu'elles soient pertinentes pour la nouvelle requête, sans ajouter de commentaires
    ou de justifications superflues.

    Points d'attention :
    1. **Aucune sortie superflue** : la réponse doit suivre strictement le format JSON imposé par
       `format_instructions`, sans explication ou justification.
    2. **Mise à jour ciblée** : on adapte uniquement les tests qui doivent l'être pour la nouvelle requête.
    3. **Old query / new query** : la fonction doit contextualiser le changement de requête pour
       guider l'IA, mais ces requêtes ne doivent pas forcément apparaître dans la sortie finale.
    4. **Date et heure courantes** : ajoutées en fin de consigne pour contexte, à la manière
       des autres fonctions.
    """

    current_datetime = datetime.datetime.now()
    formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M")

    # Récupération et filtrage de l'historique pour exclure la requête SQL.
    history_with_results = format_history(
        history,
        dialect,
        output_format="generator",
        excluded_agents=[MsgType.PROVIDED_SQL],
    )

    system_message_content = (
        "The query has been changed. Generate a single unit test adapted to the new query.\n\n"
        f"{format_instructions}\n\n"
        "Field order: unit_test_description, unit_test_build_reasoning, tags, suggestions, data.\n"
        "- unit_test_description: business-contextualized description in the format 'Pour [subject with concrete values] [condition/situation] → [expected business result]'. Mention concrete values (IDs, dates, statuses). Avoid generic formulations like 'Vérifie que le calcul est correct'.\n"
        "- tags: relevant labels among Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance.\n"
        "- suggestions: exactly 2 business scenarios in format 'Pour [subject] [context] → [expected result]'. Target distinct business cases with concrete values — not the internal behavior of SQL functions (avoid: 'S'assure que STRING_AGG trie correctement' → prefer: 'Pour un client avec contrats C1/C2/C3 de types différents → le champ type_operation agrège toutes les valeurs séparées par \"·\"').\n"
        "- Do NOT test constant expressions, pure aggregates (SUM/AVG/COUNT), or internal SQL function behavior — these are guaranteed correct by the warehouse engine.\n"
        "- If the SQL has OR conditions or multiple CASE branches, pick ONE branch per test and name it explicitly in the description. Other branches go into suggestions.\n\n"
        "old query :\n"
        "{old_query}\n\n"
        "New query :\n"
        "{new_query}\n\n"
        "Respond with a single JSON object without any explanation or comment.\n\n"
        f"Current date time is : {formatted_datetime}"
    )

    human_message_content = (
        "Existing data for reference : {existing_data}\n"
        "Generate one adapted unit test in the format above."
    )

    # Assemblage des messages : system, historique filtré, puis le message humain final.
    prompt_messages = [("system", system_message_content)]
    prompt_messages.extend(history_with_results)
    prompt_messages.append(("human", human_message_content))

    return ChatPromptTemplate.from_messages(prompt_messages, "mustache")


def make_routing_prompt(
    granularity: Literal["coarse", "fine"],
    *,
    format_instructions: str = "",
    dialect: str = "",
    history: Optional[List[BaseMessage]] = None,
) -> ChatPromptTemplate:
    """
    'coarse' → JSON: {"title","route","reasoning"} with route in {"query","analysis","other"}
    'fine'   → JSON: {"reasoning","route"} with route in {"generator","other"}
    """
    history_parsed = format_history(
        history,
        current_agent=MsgType.ROUTE,
        output_format="text",
        excluded_agents=[],
        dialect=dialect,
    )
    history_block = (
        f"Historique de conversation :\n{history_parsed}\n" if history_parsed else ""
    )

    if granularity == "coarse":
        prompt_messages = [
            (
                "system",
                """Vous êtes un routeur qui classe la question utilisateur dans : `query`, `analysis`, `other`.

Définitions strictes :
- `query` : la demande peut être satisfaite par **une seule requête SQL** (éventuellement avec agrégats, joins, fenêtres, filtres, seuils) SANS nécessiter d'interpréter un résultat pour décider d'une requête suivante.
- `analysis` : besoin d'un **raisonnement multi-étapes** avec **itération ou branchement** après inspection des résultats (ex. diagnostic de causes, segmentation exploratoire, boucles "voir résultat → ajuster la stratégie/filtre → refaire", formulation d'hypothèses, choix de plusieurs vues/visualisations). Si une seule requête suffit, **ne classez PAS** en analysis.
- `other` : le reste.

Règles de décision (top-down) :
1) La demande est-elle résoluble par une seule requête SQL claire ? → `query`.
2) Sinon, la demande requiert-elle d'**interpréter** des résultats pour **choisir** des requêtes suivantes OU de **tester des hypothèses**/**segmentations multiples** ? → `analysis`.
3) Sinon → `other`.

Signaux forts de `query` : verbes "liste/compte/compare/vérifie/filtre", seuils simples (ex. "> 10%"), périodes précises, métriques nettes.
Signaux forts de `analysis` : "pourquoi/expliquer les causes/proposer axes d'analyse/identifier segments contributeurs", "itérer jusqu'à trouver…", "explorer".

Sortie JSON (un seul objet) :
- Toujours : "title" (2–3 mots, concrets), "route", "reasoning" (≤35 mots, expliquant la règle activée).
N'affichez rien d'autre.

**Description de la base** :
{{descriptions}}
"""
                + (format_instructions or ""),
            ),
            # Exemples positifs/négatifs pour stabiliser la frontière
            (
                "human",
                "<question>\nTop 5 des clients par chiffre d'affaires ?\n</question>",
            ),
            (
                "ai",
                '{"title": "Top 5 CA", "route": "query", "reasoning": "Une seule agrégation/tri suffit ; pas d\'itération sur résultats."}',
            ),
            (
                "human",
                "<question>\nQuel est l'évolution du chiffre d'affaires sur le dernier mois ?\n</question>",
            ),
            (
                "ai",
                '{"title": "Évolution CA", "route": "query", "reasoning": "Mesure temporelle simple ; une requête avec filtre période et agrégat."}',
            ),
            (
                "human",
                "<question>\nNotre chiffre d'affaires a baissé de 12% au T2 vs T1. Pourquoi ?\n</question>",
            ),
            (
                "ai",
                '{"title": "Explication baisse CA", "route": "analysis", "reasoning": "Recherche de causes/segments → nécessite itérations et interprétation des résultats."}',
            ),
            (
                "human",
                "<question>\nQue puis-je analyser sur les utilisateurs ?\n</question>",
            ),
            (
                "ai",
                '{"title": "Axes utilisateurs", "route": "analysis", "reasoning": "Demande ouverte d\'exploration multi-étapes, non une requête unique."}',
            ),
            (
                "human",
                "<question>\nVérifie que sur la dernière partition_date je n'ai pas de baisse du nombre de contrats de plus de 10%.\n</question>",
            ),
            (
                "ai",
                '{"title": "Check baisse 10%", "route": "query", "reasoning": "Vérification booléenne avec comparaison N/N-1 ; une seule requête suffit."}',
            ),
            ("human", history_block + "<question>\n{{input}}\n</question>\n"),
        ]
        return ChatPromptTemplate.from_messages(prompt_messages, "mustache")

    # ---- fine ----
    prompt_messages = [
        (
            "system",
            """Tu es un routeur pour MockSQL, un outil de test de requêtes SQL.

Routes :
- `generator` : l'utilisateur demande à MockSQL d'**agir** — créer, modifier, ajouter, supprimer, régénérer des tests, des données de test, des assertions ou des verdicts. Le message est **impératif** (ordre ou requête d'action), même formulé poliment ("peux-tu ajouter…").
- `other` : l'utilisateur **pose une question** ou **réfléchit à voix haute** — il veut une explication, comprendre un résultat, discuter d'une piste, ou exprime un avis sans demander à MockSQL de modifier quoi que ce soit.

Règle de décision : demande-toi "MockSQL doit-il **modifier ou générer** quelque chose ?" Si oui → `generator`. Sinon → `other`.

Signaux `generator` : "ajoute", "modifie", "génère", "change", "supprime", "mets à jour", "refais", "crée", "peux-tu faire", "je veux que tu…"
Signaux `other` : "pourquoi", "comment", "qu'est-ce que", "c'est quoi", "tu penses que", "il faudrait peut-être", "je me demande si", "est-ce normal"

Sortie JSON (unique) :
{ "reasoning": "<≤25 mots>", "route": "generator" | "other" }
N'affichez rien d'autre.
""",
        ),
        (
            "human",
            "<question>\nAjoute un test avec des valeurs NULL sur la colonne user_id.\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Demande impérative de création de test — MockSQL doit agir.", "route": "generator"}',
        ),
        (
            "human",
            "<question>\nPourquoi le test 2 échoue ?\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Question explicative sur un résultat — aucune action demandée.", "route": "other"}',
        ),
        (
            "human",
            "<question>\nPeux-tu générer un cas limite pour les ex æquo ?\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Requête de génération formulée poliment — MockSQL doit créer un test.", "route": "generator"}',
        ),
        (
            "human",
            "<question>\nJe me demande s'il faudrait tester les cas avec une plage vide.\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Reflexion a voix haute, aucune action demandee a MockSQL.", "route": "other"}',
        ),
        (
            "human",
            "<question>\nChange le verdict du test 1 en warn, les assertions sont trop permissives.\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Modification explicite de verdict existant — MockSQL doit agir.", "route": "generator"}',
        ),
        (
            "human",
            "<question>\nComment fonctionne la clause WINDOW dans cette requête ?\n</question>",
        ),
        (
            "ai",
            '{"reasoning": "Question technique sur la requete — explication attendue, pas de modification.", "route": "other"}',
        ),
        (
            "human",
            history_block + "<question>\n{{input}}\n</question>\n\nSortie JSON :",
        ),
    ]
    return ChatPromptTemplate.from_messages(prompt_messages, "mustache")


def debug_failing_cte_prompt(
    history: list[BaseMessage],
    failing_cte_name: str,
    cte_sql: str,
    cte_trace: dict,
    cte_constraints: str,
    cte_sources: list,
    existing_data: str,
    dialect: str,
    format_instructions: str,
    profile: Optional[dict] = None,
    used_columns: Optional[list] = None,
) -> ChatPromptTemplate:
    """
    Targeted prompt for fixing test data when a specific CTE returns 0 rows.
    Only asks the LLM to modify the source tables that feed the failing CTE.
    """
    history_with_results = format_history(
        history,
        dialect,
        output_format="generator",
        excluded_agents=[MsgType.PROVIDED_SQL],
    )

    cte_trace_json = json.dumps(cte_trace, ensure_ascii=False, indent=2)
    sources_str = (
        json.dumps(cte_sources, ensure_ascii=False) if cte_sources else "non spécifié"
    )
    constraints_block = (
        f"\nContraintes extraites de `{failing_cte_name}` :\n```json\n{cte_constraints}\n```\n"
        if cte_constraints
        else ""
    )

    profile_block_str = (
        _format_profile_block(profile, used_columns or []) if profile else ""
    )
    profile_section = (
        f"\n{profile_block_str}\n"
        "Respecte ces statistiques réelles pour les valeurs corrigées.\n"
        if profile_block_str
        else ""
    )

    system_message_content = f"""Vous êtes un QA engineer expert en correction de données de test SQL.

La requête complète a retourné 0 ligne. Le CTE en échec est : **{failing_cte_name}**.

Trace d'exécution par CTE :
```json
{cte_trace_json}
```

SQL de `{failing_cte_name}` :
```sql
{cte_sql}
```
{constraints_block}{profile_section}
Tables sources alimentant `{failing_cte_name}` : {sources_str}

**Règles absolues :**
1. Modifiez UNIQUEMENT les tables sources listées ci-dessus.
2. Ne touchez PAS aux tables dont les CTEs sont déjà non vides dans la trace.
3. Les données modifiées doivent satisfaire STRICTEMENT les contraintes (jointures, filtres, anti-jointures).
4. Répondez uniquement avec le JSON demandé, sans explication."""

    human_message_content = f"""Données actuelles du test :
{existing_data}

Modifiez uniquement les tables sources de `{failing_cte_name}` pour que ce CTE retourne au moins 1 ligne.

{format_instructions}"""

    prompt_messages = [SystemMessage(content=system_message_content)]
    prompt_messages.extend(history_with_results)
    prompt_messages.append(HumanMessage(content=human_message_content))

    return ChatPromptTemplate.from_messages(
        concatenate_successive_messages(prompt_messages), "mustache"
    )


def concatenate_successive_messages(messages: List[MessageLike]) -> List[BaseMessage]:
    if not messages:
        return []

    normalized: List[BaseMessage] = []
    # 1) Normalisation : transformer les tuples en vrais messages
    for m in messages:
        if isinstance(m, tuple):
            msg_type, msg_content = m
            if msg_type == "human":
                normalized.append(HumanMessage(content=msg_content))
            elif msg_type == "ai":
                normalized.append(AIMessage(content=msg_content))
            elif msg_type == "system":
                normalized.append(SystemMessage(content=msg_content))
            else:
                raise ValueError(f"Type de message inconnu : {msg_type!r}")
        else:
            normalized.append(m)

    # 2) Regroupement des messages consécutifs du même type
    result: List[BaseMessage] = []
    i = 0
    while i < len(normalized):
        current = normalized[i]
        # on stocke le contenu qu'on va concaténer
        combined_content = current.content
        j = i + 1

        # tant que le message suivant est du même type, on l'agrège
        while j < len(normalized) and normalized[j].type == current.type:
            combined_content += "\n" + normalized[j].content
            j += 1

        # on remet à jour le contenu du message courant
        current.content = combined_content
        result.append(current)
        i = j

    return result


def explain_query_prompt(script1, script2):
    if script1 and script2:
        diff = difflib.unified_diff(
            script1.splitlines(),
            script2.splitlines(),
            lineterm="",
        )
        diff_text = "\n".join(diff)
        if diff_text.strip() != "":
            query_text = (
                f"Please explain in French what exactly has changed in the initial query "
                f"without 100 words.\nDo not translate table names.\n{diff_text}"
            )
        else:
            query_text = f"Please explain in French the query without 100 words.\nDo not translate table names.\n{script1}"
    else:
        script = script1 or script2
        query_text = f"Please explain in French the query without 100 words.\nDo not translate table names.\n{script}"

    prompt_messages = [
        (
            "system",
            """As a data analyst, shortly describe the query or the query diff for a non-technical person by:
- Listing the tables it uses
- Explaining the rules used for the final result without explaining joins.
- Explain each step if multiple steps are given

Respond in markdown format.
""",
        ),
        ("human", query_text),
    ]

    final_prompt = ChatPromptTemplate.from_messages(prompt_messages)
    return final_prompt


def naming_prompt(sql: str) -> ChatPromptTemplate:
    escaped_sql = escape_unescaped_placeholders(sql)
    prompt_messages = [
        (
            "system",
            (
                "Based on the following SQL query, generate a very short name (2-4 words max) "
                "that describes what this query does. Use snake_case. "
                "Return only the name, no explanation, no punctuation."
            ),
        ),
        ("human", f"SQL:\n{escaped_sql}"),
    ]
    final_prompt = ChatPromptTemplate.from_messages(prompt_messages)

    return final_prompt
