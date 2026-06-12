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


def _format_schema_block(used_columns: list) -> str:
    """Readable 'source tables to populate' block — one line per table with its
    columns and the exact `data` key to use.

    Replaces the raw Python-list ``repr`` that previously trailed at the bottom of
    the human message: the role promises "le schéma fourni" but no schema block
    existed.
    """
    lines: List[str] = []
    for u in used_columns:
        if isinstance(u, str):
            try:
                u = json.loads(u)
            except (ValueError, TypeError):
                continue
        db = u.get("database", "")
        tbl = u.get("table", "")
        key = f"{db}_{tbl}" if db else tbl
        if not key:
            continue
        cols = u.get("used_columns", []) or []
        cols_str = ", ".join(cols) if cols else "(toutes colonnes)"
        lines.append(f"- `{key}` : {cols_str}")
    return "\n".join(lines)


def _dedup_and_parts(expr: str) -> str:
    """Deduplicate ` AND `-joined key predicates while preserving order.

    The profiler concatenates composite join keys with ` AND ` but may emit
    duplicates (e.g. ``a.x AND a.x AND a.x``). Collapse them so the join hint
    stays readable.
    """
    if not expr or " AND " not in expr:
        return expr
    seen: set[str] = set()
    uniq: List[str] = []
    for part in expr.split(" AND "):
        p = part.strip()
        if p and p not in seen:
            seen.add(p)
            uniq.append(p)
    return " AND ".join(uniq)


def _format_profile_block(
    profile: Optional[dict],
    used_columns: list,
) -> str:
    """
    Formats the statistical profile into a concise block for injection into prompts.
    Only includes columns that are actually used in the query.
    """
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
        derived_exprs = tbl_data.get("derived_expressions", [])
        if not wanted_cols and not derived_exprs:
            continue
        cols = tbl_data.get("columns", {})
        col_lines: List[str] = []
        if wanted_cols:
            for col_name, stats in cols.items():
                if col_name not in wanted_cols:
                    continue
                parts: List[str] = []
                # Support both computed field names (parse_profile_query_result)
                # and raw BigQuery field names (_normalize_profile)
                min_v = stats.get("min_value") or stats.get("min_val")
                max_v = stats.get("max_value") or stats.get("max_val")
                if min_v is not None and max_v is not None:
                    parts.append(f"min={min_v}, max={max_v}")
                dc = stats.get("distinct_count")
                total = stats.get("total_count")
                null_count = stats.get("null_count")
                nullable_ratio = stats.get("nullable_ratio") or (
                    null_count / total if null_count is not None and total else 0
                )
                is_never_null = stats.get("is_never_null") or (
                    null_count == 0 and total
                )
                is_categorical = stats.get("is_categorical") or (
                    dc is not None and dc <= 20
                )
                if dc is not None:
                    parts.append(f"distinct={dc}")
                    if is_categorical and dc <= 20:
                        parts.append("(catégoriel)")
                if is_never_null:
                    parts.append("never_null")
                elif nullable_ratio > 0:
                    pct = round(nullable_ratio * 100, 1)
                    parts.append(f"null={pct}%")
                top_values = stats.get("top_values", [])
                if top_values:
                    top_str = ", ".join(str(v) for v in top_values[:10])
                    parts.append(f"valeurs=[{top_str}]")
                if parts:
                    col_lines.append(f"    - `{col_name}`: {', '.join(parts)}")
        # Derived expressions (SAFE_CAST, REGEXP_EXTRACT, COALESCE, …) profiled on real data.
        # Keep only expressions whose source columns overlap with wanted_cols.
        for de in derived_exprs:
            if wanted_cols:
                src_cols = {sc.get("column") for sc in de.get("source_columns", [])}
                if not (src_cols & wanted_cols):
                    continue
            de_sql = de.get("expr_sql", "")
            if not de_sql:
                continue
            de_parts: List[str] = []
            de_min = de.get("min_val")
            de_max = de.get("max_val")
            if de_min is not None and de_max is not None:
                de_parts.append(f"min={de_min}, max={de_max}")
            de_distinct = de.get("distinct_count")
            if de_distinct is not None:
                de_parts.append(f"distinct={de_distinct}")
            de_null = de.get("null_count")
            de_total = de.get("total_count")
            if de_null is not None and de_total:
                pct = round(de_null / de_total * 100, 1)
                if pct > 0:
                    de_parts.append(f"null={pct}%")
            de_top = de.get("top_values", [])
            if de_top:
                tv_str = ", ".join(str(v) for v in de_top[:5])
                de_parts.append(f"top=[{tv_str}]")
            stat_str = f" ({', '.join(de_parts)})" if de_parts else ""
            col_lines.append(f"    - expr `{de_sql}`{stat_str}")
        if col_lines:
            lines.append(f"  table `{short_key}`:")
            lines.extend(col_lines)

    joins = profile.get("joins", [])
    join_lines: List[str] = []
    # CTE bodies are dumped once globally — the profiler attaches the same CTE SQL to
    # every join the CTE participates in, and that SQL is already present in the main
    # query block. Re-dumping it per-join is pure noise (cf. coface répété 3×).
    seen_cte_sql: set[str] = set()
    for j in joins:
        lt, rt = j.get("left_table", ""), j.get("right_table", "")
        lk, rk = j.get("left_expr", ""), j.get("right_expr", "")
        if not (lt and rt and lk and rk):
            continue
        # left_expr/right_expr concatènent les prédicats de clé avec " AND ", mais le
        # profiler y laisse des doublons (`a.x AND a.x AND a.x`). On déduplique en
        # préservant l'ordre. Les clés portent déjà `alias.colonne` → pas besoin du
        # préfixe table (lt/rt) qui ne fait que rallonger.
        line = f"  - {_dedup_and_parts(lk)} ↔ {_dedup_and_parts(rk)}"
        stats_parts: List[str] = []
        match_rate = j.get("left_match_rate")
        if match_rate is not None:
            stats_parts.append(f"match={match_rate:.0%}")
        fanout = j.get("avg_right_per_left_key")
        if fanout is not None:
            stats_parts.append(f"fanout_avg={fanout}")
        max_fanout = j.get("max_right_per_left_key")
        if max_fanout is not None:
            stats_parts.append(f"fanout_max={max_fanout}")
        key_sample = j.get("left_key_sample", [])
        if key_sample:
            sample_str = ", ".join(str(v) for v in key_sample[:5])
            stats_parts.append(f"exemples=[{sample_str}]")
        if stats_parts:
            line += f" ({', '.join(stats_parts)})"
        if j.get("right_filter"):
            line += f" [filtre: {j['right_filter']}]"
        join_lines.append(line)
        # CTE SQL stored at profile-build time (phase 1) — display once per unique body.
        for side_cte_key, label, side_tbl in (
            ("left_cte_sql", "gauche", lt),
            ("right_cte_sql", "droite", rt),
        ):
            cte_sql = j.get(side_cte_key)
            if cte_sql and cte_sql not in seen_cte_sql:
                seen_cte_sql.add(cte_sql)
                short = side_tbl.split(".")[-1]
                indented = "\n".join(f"      {ln}" for ln in cte_sql.splitlines())
                join_lines.append(f"    [CTE côté {label} — `{short}`]\n{indented}")

    if not lines and not join_lines:
        return ""

    result = "Profil statistique des colonnes (min/max réels) :\n"
    result += "\n".join(lines)
    if join_lines:
        result += "\nJointures profilées (cardinalités réelles) :\n"
        result += "\n".join(join_lines)
    return result


def _build_volume_hints_block(sql: str, dialect: str = "bigquery") -> str:
    """Build a warning block listing structural volume requirements (OFFSET, NTILE, RANK filters)."""
    if not sql:
        return ""
    try:
        from build_query.constraint_simplifier import extract_volume_hints

        hints = extract_volume_hints(sql, dialect)
    except Exception:
        return ""
    if not hints:
        return ""
    lines = [
        "\n⚠️ **Contraintes volumétriques** — ces clauses nécessitent un minimum de lignes "
        "pour que la requête retourne un résultat non vide :"
    ]
    for h in hints:
        scope_word = "cette " + (
            "CTE" if h.context.startswith("CTE") else "sous-requête"
        )
        lines.append(
            f"- `{h.clause_sql}` dans la {h.context} : génère **au moins {h.min_rows} ligne(s)** "
            f"dans les tables sources de {scope_word}."
        )
    return "\n".join(lines) + "\n"


def _build_fanout_hint_block(sql: str, dialect: str = "bigquery") -> str:
    """Warn the generator about fan-out risk when a JOIN feeds a row-multiplication
    sensitive aggregate (AVG/SUM/STDDEV/CORR…). Steers toward unique dimension-side
    join keys so an accidental many-to-many doesn't inflate the aggregate (bq143)."""
    if not sql:
        return ""
    try:
        from build_query.constraint_simplifier import detect_fanout_risk

        tables = detect_fanout_risk(sql, dialect)
    except Exception:
        return ""
    if not tables:
        return ""
    tables_str = ", ".join(f"`{t}`" for t in tables)
    return (
        "\n⚠️ **Risque de fan-out (multiplication de lignes)** : cette requête agrège "
        "(AVG/SUM/STDDEV/CORR…) au-dessus d'une ou plusieurs jointures "
        f"({tables_str}). Si une clé de jointure n'est pas **unique** côté table jointe, "
        "chaque ligne est dupliquée et l'agrégat est faussé (produit cartésien involontaire). "
        "Génère des clés de jointure **uniques côté table de dimension** — au plus une ligne "
        "par valeur de clé — sauf si le test vise explicitement une relation un-à-plusieurs.\n"
    )


# ── Exemple few-shot statique « clé dérivée + photos M/M-1 » (P2a) ──────────
# Mini-requête FICTIVE concentrant les deux pièges qui vident le plus souvent
# le résultat : (a) clé de JOIN dérivée d'un CASE — il faut générer la valeur
# SOURCE, pas la valeur finale ; (b) filtre photo M / M-1 — l'absence en M-1
# fait partie du scénario. Le même exemple est injecté quel que soit le modèle
# testé : il enseigne la MÉTHODE, pas des valeurs à réutiliser.
# Vérifié forward sur DuckDB par tests/test_few_shot_example.py — toute
# modification ici doit garder ce test vert.
FEW_SHOT_EXAMPLE_SQL = """\
WITH photo_m AS (
    SELECT num_carte, reseau, type_carte
    FROM cartes.stock
    WHERE dt_photo = '2024-03-31'
),
photo_m1 AS (
    SELECT num_carte
    FROM cartes.stock
    WHERE dt_photo = '2024-02-29'
)
SELECT m.num_carte, p.libelle
FROM photo_m m
JOIN ref.produits p
    ON p.code = CASE WHEN m.reseau = 'BP' THEN CONCAT('BP', m.type_carte) ELSE m.type_carte END
LEFT JOIN photo_m1 m1 ON m1.num_carte = m.num_carte
WHERE m1.num_carte IS NULL"""

FEW_SHOT_EXAMPLE_DATA = {
    "cartes_stock": [
        {
            "num_carte": "C001",
            "reseau": "BP",
            "type_carte": "GOLD",
            "dt_photo": "2024-03-31",
        }
    ],
    "ref_produits": [{"code": "BPGOLD", "libelle": "Carte Gold BP"}],
}

_FEW_SHOT_EXAMPLE_ANSWER = json.dumps(
    {
        "unit_test_build_reasoning": (
            "La clé de jointure est dérivée (CASE → CONCAT('BP', type_carte)) : "
            "type_carte='GOLD' produit 'BPGOLD' = produits.code, et C001 n'a "
            "aucune ligne au 2024-02-29 pour rester une ouverture."
        ),
        "test_name": "Ouverture carte Gold réseau BP",
        "unit_test_description": (
            "Pour la carte C001 (réseau BP, type GOLD) présente sur la photo du "
            "2024-03-31 et absente de celle du 2024-02-29 → elle ressort comme "
            "ouverture avec le libellé Carte Gold BP."
        ),
        "tags": ["Logique métier"],
        "data": FEW_SHOT_EXAMPLE_DATA,
    },
    ensure_ascii=False,
    indent=2,
)

_FEW_SHOT_EXAMPLE_HUMAN = f"""<example>
Exemple travaillé — tables et requête FICTIVES : n'en réutilisez ni les noms ni les valeurs. Seuls <schema> et <query> font autorité pour votre réponse.

Tables sources : `cartes_stock` (num_carte, reseau, type_carte, dt_photo) ; `ref_produits` (code, libelle).

```sql
{FEW_SHOT_EXAMPLE_SQL}
```

Cette requête concentre les deux pièges classiques :
✗ **Clé de jointure dérivée** — générer `type_carte = 'BPGOLD'` (la valeur FINALE de la clé) : après le CASE, la clé devient 'BPBPGOLD', la jointure ne matche jamais → résultat vide. Il faut générer la valeur SOURCE (`'GOLD'`), dont la transformation produit 'BPGOLD' = `produits.code`.
✗ **Photos M / M-1** — ajouter « par complétude » une ligne C001 datée du 2024-02-29 : la carte existe alors en M-1 et le `WHERE m1.num_carte IS NULL` l'élimine → résultat vide. L'ABSENCE en M-1 fait partie du scénario d'ouverture.
</example>"""

_FEW_SHOT_MESSAGES: list[tuple[str, str]] = [
    ("human", _FEW_SHOT_EXAMPLE_HUMAN),
    ("ai", _FEW_SHOT_EXAMPLE_ANSWER),
]


def generate_data_prompt(
    history: list[BaseMessage],
    dialect: str,
    format_instructions: str,
    used_columns: list,
    constraints_hint: str = "",
    sql: str = "",
    user_instruction: str = "",
    profile: Optional[dict] = None,
    model_context: str = "",
    trace_hint: str = "",
    eval_history: list | None = None,
    native_thinking: bool = False,
    join_recipes_block: str = "",
) -> ChatPromptTemplate:
    """
    Construit un prompt pour générer un test unitaire,
    en incluant un historique de messages (system/human/ai, etc.).

    ``native_thinking`` : quand le modèle raisonne nativement (flash/pro), le
    champ ``unit_test_build_reasoning`` n'est qu'une justification brève ; sinon
    il porte un chain-of-thought complet (cf. get_generation_output_type).
    """
    if constraints_hint:
        if user_instruction:
            constraints_block = (
                f"\nContraintes extraites du SQL (types, jointures, filtres) — respecte-les pour la cohérence des données :\n"
                f"```json\n{constraints_hint}\n```\n"
            )
        else:
            constraints_block = (
                "\n**Contraintes extraites du SQL — à respecter pour un résultat non vide :**\n"
                "Les filtres et jointures positifs ci-dessous doivent être VRAIS. "
                "⚠️ Les `anti_joins`, eux, doivent être FAUX : génère des données qui NE "
                "correspondent PAS à la table anti-jointe (sinon la ligne est supprimée).\n"
                f"```json\n{constraints_hint}\n```\n"
            )
    else:
        constraints_block = ""

    profile_block_str = _format_profile_block(profile, used_columns)
    profile_block = (
        f"\n{profile_block_str}\n"
        "Utilise ces statistiques réelles pour générer des valeurs vraisemblables "
        "(respecte les plages min/max et la cardinalité des colonnes catégorielles).\n"
        if profile_block_str
        else ""
    )

    _has_unnest = sql and "unnest" in sql.lower()
    unnest_block = (
        "\n⚠️ **Structure UNNEST détectée** : cette requête déplie des colonnes imbriquées (ARRAY/STRUCT BigQuery). "
        "Génère les données **sous forme APLATIE** — une ligne par élément déplié (ex: une ligne par hit, ou par produit). "
        "Tous les champs doivent être des scalaires (pas de tableaux ni d'objets JSON imbriqués). "
        "Les tables sources représentent les données **déjà dépliées** : ne simule pas le mécanisme UNNEST lui-même.\n"
        if _has_unnest
        else ""
    )

    volume_hints_block = _build_volume_hints_block(sql, dialect)

    fanout_hint_block = _build_fanout_hint_block(sql, dialect)

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

    if native_thinking:
        # Le modèle raisonne nativement en amont : le champ ne porte qu'une
        # justification brève (1 phrase) → JSON court, pas de troncature.
        reasoning_bullet = (
            "- `unit_test_build_reasoning` (**en premier**): **1 phrase maximum.** Citez la clause "
            "éliminatoire qui aurait pu vider le résultat (JOIN sur clé dérivée, borne de date, "
            "anti-jointure) et comment vos données la franchissent. Le raisonnement détaillé se fait "
            "dans votre canal de réflexion, pas dans le JSON."
        )
    else:
        reasoning_bullet = (
            "- `unit_test_build_reasoning`: **Ce champ doit être rempli en premier, en 3 phrases maximum.** "
            "Simulez mentalement la traversée des données à travers chaque CTE et filtre — citez les clauses "
            "éliminatoires clés (WHERE, JOIN strict, RANK/ROW_NUMBER), indiquez combien de lignes doivent "
            "survivre à chaque étape, et expliquez comment vos données le garantissent."
        )

    system_message_content = (
        """
Vous êtes un data QA, expert en test de requêtes SQL et en génération de données de test JSON.
À partir du schéma des tables sources et de la requête SQL fournis, générez UN seul test unitaire au format JSON.

La conversation contient ces sections, délimitées par des balises. Le schéma, la requête et les contraintes sont fournis **en premier** (ils s'appliquent à tout l'échange, y compris aux exemples) ; la tâche à produire arrive **en dernier message** :
- `<schema>` : les tables sources à peupler, leurs colonnes, et le profil statistique éventuel.
- `<business_context>` : contexte métier du projet (optionnel).
- `<query>` : la requête SQL à tester — **elle fait toujours autorité**.
- `<constraints>` : aides extraites du SQL — `conditions` (à rendre VRAIES), `anti_joins` (à rendre FAUSSES : générer des données qui NE matchent PAS la table anti-jointe), `format_constraints`, `lineages`. ⚠️ Ces extractions peuvent être incomplètes ou bruitées : en cas de doute, raisonnez directement sur `<query>` et ignorez toute contrainte tautologique (`X = X`) ou manifestement absurde.
- `<example>` : un exemple travaillé sur une mini-requête FICTIVE (clé de jointure dérivée + photos M/M-1) — appliquez-en la méthode, ne réutilisez jamais ses tables ni ses valeurs.
- `<diagnostic>` : diagnostic de la tentative précédente, s'il y a lieu (optionnel).
- `<task>` : ce que vous devez produire.

**Objectif** : produire des données qui parcourent réellement la LOGIQUE MÉTIER de la requête (filtres, jointures, conditions temporelles, déduplication) et qui survivent jusqu'au SELECT final (sauf si l'instruction utilisateur demande explicitement un résultat vide).

**Consignes principales :**
"""
        + consignes_1_2
        + """
3. **Une seule branche** : quand le SQL contient plusieurs alternatives (`condition_A OR condition_B`, `CASE WHEN … THEN … ELSE …`, plusieurs chemins de jointure, plusieurs `UNION ALL`), choisir **une seule branche** et construire des données qui satisfont uniquement celle-ci. Ne pas couvrir plusieurs alternatives à la fois. La description nomme explicitement la branche choisie (ex. "Pour un utilisateur premium …", pas "Pour un utilisateur premium ou avec cumulated_montant > 1000 …"). Les tables propres aux autres branches peuvent rester à null ; les tables PARTAGÉES doivent rester cohérentes avec la branche choisie. Les autres branches alimentent les suggestions.
4. **Clés de jointure DÉRIVÉES (le point le plus important)** : quand une clé de JOIN est le produit d'un `CASE` / `CAST` / `SAFE_CAST` / `SUBSTR` / `SPLIT` / `REGEXP`, générez la valeur SOURCE qui, APRÈS transformation, égale la clé de l'autre côté — **pas** la valeur finale.
   - `JOIN ON a.k = CASE WHEN t.reseau='BP' THEN '1' END` et `a.k` vient de `t2` → mettez `t2.k = '1'`.
   - `SUBSTR(col, 2, LEN(col)-2)` puis `SPLIT ','` : la colonne SOURCE doit inclure les caractères de bord qui seront retirés (ex. `"'PROD1'"` → après SUBSTR/TRIM → `PROD1`), pas `PROD1` brut.
   - `SAFE_CAST(x AS INT64)` utilisé en clé : `x` doit être numérique des deux côtés et égal.
5. **Contrôle strict des types et fonctions** :
   - colonne convertie (`CAST`, `SAFE_CAST`) en entier → valeur d'origine au format numérique ("123", "00123" acceptables, pas "ABC").
   - fonction de date (`PARSE_DATE`, `SAFE.PARSE_DATE`, …) → la chaîne d'entrée respecte exactement le format attendu.
   - JOIN sur un champ entier → les valeurs correspondent dans les deux tables.
6. **Conditions temporelles** : décodez les bornes de chaque filtre de date AVANT de générer. Les bornes viennent des **littéraux du SQL, jamais de la date courante**. Si une CTE exige `dt_deb < D1 AND dt_fin >= D2` et qu'une autre caractérise la branche par l'inverse (présent en M, absent en M-1), placez les dates dans l'intervalle qui satisfait UNIQUEMENT la branche visée. Méfiez-vous des sentinelles (`'0001-01-01'`, `'9999-12-31'`).
7. **Cohérence inter-tables** : les valeurs partagées entre tables — clés de jointure ET littéraux des filtres (année, dates, statuts, devises) — doivent être **cohérentes dans TOUTES les tables**. Si la requête filtre 2017, aucune donnée 2016. Une valeur cohérente sur une table mais incohérente avec les autres rend le test invalide.
8. **Agrégats** : si le SQL contient `GROUP BY` + `COUNT`/`SUM`/`AVG`/`STDDEV`/`CORR`, mettez plusieurs lignes partageant la **même clé de groupe** (sinon COUNT=1, STDDEV=0). Ex. : GROUP BY date → 3 lignes `date='2024-01-01'` et 2 lignes `date='2024-01-02'`, pas une date par ligne. Gardez la clé **unique côté table de dimension** pour éviter un fan-out many-to-many qui fausse l'agrégat. Si `ORDER BY` + `OFFSET` sur l'agrégat, des COUNT tous distincts (3, 2, 1 — pas 3, 1, 1, 1 qui créent un ex æquo) pour un OFFSET déterministe.
9. **LEFT JOIN** : le prédicat ON n'a PAS à matcher pour qu'une ligne survive — une table en LEFT JOIN peut rester vide si le scénario ne la requiert pas. Les INNER JOIN, eux, exigent des clés correspondantes et non nulles.
10. **Anti-jointures / `NOT IN` / `NOT EXISTS`** : générez des données qui NE matchent PAS la table anti-jointe (sinon la ligne est supprimée).
11. **NULL** : pas de valeur NULL/vide GRATUITE. Exception explicite : si la logique testée DÉPEND d'un NULL (ex. `LEFT JOIN … WHERE x IS NULL`, `segment IS NULL`), produisez ce NULL — c'est le test.
12. **Respect strict de la casse** : les clés de `data` reprennent EXACTEMENT les clés de `<schema>`, au format `{dataset}_{table}` (les deux derniers segments du nom qualifié, joints par `_`). Ex. : `bigquery-public-data.covid19_open_data.covid19_open_data` → `covid19_open_data_covid19_open_data`, jamais `covid19_open_data` seul.
13. **Un seul test**, et **ne pas inclure la requête SQL** dans le résultat.

**Ce que cet outil NE doit PAS tester** (laisser au moteur de warehouse) :
- Les expressions constantes dans le SELECT final (ex. `SELECT 'valeur_fixe' AS col`) : résultat trivial, aucune logique métier à valider.
- Les fonctions d'agrégation pures (SUM, AVG, COUNT, MIN, MAX) sans filtre ou condition métier : elles sont garanties correctes par le moteur SQL.
- Le comportement interne des fonctions SQL (ordre de STRING_AGG, précision de CAST, format de DATE_FORMAT…) : tester la DB, pas la logique métier.
- Les règles de calcul que toute implémentation SQL correcte produirait identiquement.
Privilégier des scénarios où **la logique métier** — les filtres, jointures, conditions temporelles, règles de déduplication — est réellement testée.

**Format de sortie obligatoire** : un objet JSON unique, champs dans l'ordre du schéma (reasoning d'abord).
"""
        + reasoning_bullet
        + """
- `test_name`: 3-6 mots, lecteur métier, sans jargon SQL ni noms techniques (ex. "Ouverture nouveau client janvier"). Pas de noms de CTE/colonnes.
- `unit_test_description`: Description **métier contextualisée** au format *"Pour [sujet avec valeurs concrètes : IDs, dates, montants, statuts] [condition/situation] → [résultat attendu]"*. Nommer la branche choisie quand le SQL a des alternatives. Exemple : "Pour le porteur COLLAB789 (banque 001) dont la carte démarre le 2026-01-15 → il est compté comme OUVERTURE sur le mois d'analyse". Interdits : noms de colonnes/CTE, syntaxe SQL, formulations génériques type "Vérifie que le calcul est correct".
- `tags`: Labels décrivant les types de cas couverts. Choisir parmi : `Logique métier`, `Null checks`, `Cas limites`, `Intégration`, `Valeurs dupliquées`, `Performance`.
- `data`: Données cohérentes, correctes pour la requête.

⚠️ **Toute casse incorrecte dans les noms de tables sera considérée comme une erreur.**
⚠️ **Les clés de `data` doivent être `{dataset}_{table}` (ex. `covid19_open_data_covid19_open_data`), jamais le nom court seul (ex. `covid19_open_data`).**

Répondez uniquement avec l'objet JSON brut, sans texte additionnel et **sans clôture markdown** (pas de ```json ni de backticks autour de l'objet)."""
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

    # ── Sections balisées du message human : du vocabulaire vers l'ask (recency) ──
    schema_block_str = _format_schema_block(used_columns)
    schema_section = (
        "<schema>\n"
        "**Tables sources à peupler** — utilise EXACTEMENT ces clés dans `data` "
        "(format `{dataset}_{table}`, casse stricte) :\n"
        f"{schema_block_str}\n"
        f"{profile_block}"
        "</schema>\n"
    )

    business_section = (
        f"<business_context>\n{model_context}\n</business_context>\n"
        if model_context
        else ""
    )

    query_section = f"<query>\n```sql\n{sql}\n```\n</query>\n" if sql else ""

    constraints_inner = (
        f"{constraints_block}{join_recipes_block}"
        f"{unnest_block}{volume_hints_block}{fanout_hint_block}"
    )
    constraints_section = (
        f"<constraints>\n{constraints_inner}</constraints>\n"
        if constraints_inner.strip()
        else ""
    )

    # Diagnostic = feedback d'état du tour (volatil, par retry) — près de l'ask, après
    # la requête qu'il commente (et non plus dans le system, préfixe stable).
    diagnostic_section = (
        f"<diagnostic>\n{trace_hint}\n</diagnostic>\n" if trace_hint else ""
    )

    task_section = f"""<task>
Génère un test unitaire conforme aux consignes du message système, avec :
- Un seul test
- Résultat JSON uniquement (champs dans l'ordre du schéma : `unit_test_build_reasoning` d'abord, puis `test_name`, `unit_test_description`, `tags`, `data`)
- `unit_test_description` : description métier au format "Pour [sujet avec valeurs concrètes] [condition] → [résultat attendu]" — mentionner des valeurs concrètes (IDs, dates, statuts), pas de formulation générique
- `tags` : labels pertinents parmi Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance
- Pas de requête SQL source dans la sortie
- Données complètes, sans colonnes nulles ni vides (sauf si l'instruction le demande explicitement)
- Attention stricte à la **casse exacte** des noms de tables dans le JSON (clés `data` = celles de <schema>)
- ⚠️ Respecte les `conditions` de <constraints> (à rendre VRAIES) et les `anti_joins` (à rendre FAUSSES : ne génère PAS de données qui matchent la table anti-jointe)
- Ne pas tester les expressions constantes, les agrégats purs (SUM/AVG/COUNT), ni le comportement interne des fonctions SQL
- Si le SQL a des conditions OR, plusieurs branches CASE, ou plusieurs branches UNION ALL, choisir **une seule branche** et nommer explicitement la branche dans la description — les tables spécifiques aux autres branches peuvent être laissées à null, mais les tables partagées entre branches doivent être remplies avec des valeurs cohérentes avec la branche choisie
{non_empty_constraint}{instruction_block}
{format_instructions}
</task>"""

    # Référence partagée (schéma/SQL/contraintes) : invariante pour ce modèle, elle
    # s'applique à tout l'échange. Placée AVANT le few-shot pour l'ancrer — sinon le
    # LLM lit l'exemple (réponse) sans son énoncé (schéma/SQL absents jusqu'ici).
    reference_message_content = (
        f"{schema_section}{business_section}{query_section}{constraints_section}"
    )
    reference_human_msg = ("human", reference_message_content)

    # Ask (diagnostic volatil + tâche + date) : gardé en DERNIER pour la recency.
    ask_message_content = (
        f"{diagnostic_section}{task_section}\n\n"
        f"Date et heure actuelles : {formatted_datetime}\n"
    )
    ask_human_msg = ("human", ask_message_content)

    # Ordre : system → référence → exemple travaillé (P2a, statique) →
    # few-shot history → eval retry → ask.
    prompt_messages: list = [system_msg, reference_human_msg]
    prompt_messages.extend(_FEW_SHOT_MESSAGES)
    prompt_messages.extend(history_with_results)
    if eval_history:
        prompt_messages.extend(eval_history)
    prompt_messages.append(ask_human_msg)

    return ChatPromptTemplate.from_messages(prompt_messages, "mustache")


def update_data_prompt(
    history: list[BaseMessage],
    user_input: str,
    dialect: str,
    format_instructions: str,
    sql: str = "",
    existing_test: Optional[dict] = None,
    model_context: str = "",
    eval_history: list | None = None,
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
    context_block = (
        f"\n**Contexte métier du projet (fourni par l'ingénieur data) :**\n{model_context}\n"
        if model_context
        else ""
    )
    system_message_content = (
        "Vous êtes un data QA, testeur de requêtes SQL et expert en génération et modification de données de test JSON.\n"
        "Votre objectif est de produire un test complet et correct selon les instructions données.\n\n"
        "**Règle de non-destruction des données d'environnement :**\n"
        "L'instruction utilisateur précise *ce qu'il faut ajouter ou modifier*, mais elle ne définit pas le jeu de données complet. "
        "Vous devez générer l'intégralité des lignes nécessaires dans chaque table — y compris les lignes d'environnement "
        "(lignes 'leurres') qui n'ont pas de rôle métier direct mais sont indispensables pour que les clauses structurelles "
        "du SQL (OFFSET, RANK, ROW_NUMBER, LIMIT, JOIN) retournent un résultat non vide. "
        "Ne livrez jamais uniquement les lignes explicitement demandées : vérifiez que le volume total permet à la requête "
        "de traverser toutes ses CTEs et filtres jusqu'au SELECT final.\n\n"
        "**Heuristiques Métier Obligatoires** :\n"
        "- **Règle Agrégation** : Si le SQL contient CORR(), AVG(), STDDEV() ou d'autres fonctions statistiques, générez toujours 3 à 5 lignes distinctes par groupe pour éviter un résultat constant (1.0 ou 0) ou des divisions par zéro.\n"
        "- **Règle Jointures** : Pour les clauses INNER JOIN, assurez-vous que les clés correspondent exactement ET qu'elles ne sont pas nulles.\n"
        "- **Règle Cas Limites** : Ne testez qu'une seule branche OR ou CASE WHEN à la fois.\n\n"
        "Répondez uniquement avec l'objet JSON demandé, sans texte additionnel.\n"
        + context_block
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

    existing_test_block = ""
    if existing_test:
        from build_query.converstion_history import _format_unit_tests_for_generator

        existing_test_block = f"\nTest à modifier :\n```json\n{_format_unit_tests_for_generator([existing_test])}\n```\n"

    volume_hints_block = _build_volume_hints_block(sql, dialect)
    fanout_hint_block = _build_fanout_hint_block(sql, dialect)

    final_human_message_content = f"""
Modifie les données JSON selon l'instruction ci-dessous :
{sql_block}{volume_hints_block}{fanout_hint_block}{existing_test_block}
<Instruction>
{user_input}
</Instruction>

Génère un unique test unitaire modifié selon l'instruction ci-dessus.
Respecte l'ordre des champs : unit_test_description, unit_test_build_reasoning, tags, data.
- `unit_test_description` : description métier au format "Pour [sujet avec valeurs concrètes] [condition] → [résultat attendu]" — mentionner des valeurs concrètes (IDs, dates, statuts), pas de formulation générique
- `tags` : labels pertinents parmi Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance
- Ne pas tester les expressions constantes, les agrégats purs (SUM/AVG/COUNT), ni le comportement interne des fonctions SQL

{format_instructions}

Date et heure actuelles : {formatted_datetime}
"""
    final_human_msg = HumanMessage(content=final_human_message_content)

    # 5. Assemblage final des messages (system, historique filtré, humain).
    prompt_messages = [system_msg]
    prompt_messages.extend(history_with_results)
    prompt_messages.append(final_human_msg)
    if eval_history:
        prompt_messages.extend(eval_history)

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
        "Field order: unit_test_description, unit_test_build_reasoning, tags, data.\n"
        "- unit_test_description: business-contextualized description in the format 'Pour [subject with concrete values] [condition/situation] → [expected business result]'. Mention concrete values (IDs, dates, statuses). Avoid generic formulations like 'Vérifie que le calcul est correct'.\n"
        "- tags: relevant labels among Logique métier, Null checks, Cas limites, Intégration, Valeurs dupliquées, Performance.\n"
        "- Do NOT test constant expressions, pure aggregates (SUM/AVG/COUNT), or internal SQL function behavior — these are guaranteed correct by the warehouse engine.\n"
        "- If the SQL has OR conditions or multiple CASE branches, pick ONE branch per test and name it explicitly in the description.\n\n"
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
