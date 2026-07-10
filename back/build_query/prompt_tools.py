import datetime
import difflib
import json
import re
from typing import Optional, List

from langchain_core.messages import BaseMessage, HumanMessage, SystemMessage, AIMessage
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.prompts.chat import MessageLike

from build_query.converstion_history import format_history
from build_query.lessons import format_lessons_block
from storage.config import (
    get_language,
    output_language_directive,
    output_language_name,
    tag_labels,
)
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE, escape_unescaped_placeholders
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
            MOCKSQL_PRODUCT_PREAMBLE
            + """

Ici, l'utilisateur **pose une question ou réfléchit à voix haute** — il n'a PAS demandé de générer ou modifier un test. """
            + output_language_directive()
            + """ Réponds de façon concise et naturelle, en gardant à l'esprit le contexte de test ci-dessus (la requête testée, ses tests et leurs verdicts). Aide-le à comprendre un résultat, une couverture, une redondance, ou à décider quoi tester ensuite. N'inclus pas de code dans la réponse et ne génère pas de données de test.

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


def _format_partition_window(window: Optional[dict]) -> str:
    """Return a one-line note warning that a table's profile is partition-scoped.

    Returns ``""`` when *window* is falsy. The note tells the generator that the
    min/max/cardinalities below reflect only the last N partitions scanned at
    profiling time — NOT the table's full history. Without it, a min date of
    "2026-06-17" would be misread as "this table has no older data".
    """
    if not window:
        return ""
    field = window.get("field", "")
    limit = window.get("limit")
    n = f"{limit} dernières partitions" if limit else "dernières partitions"
    if window.get("exact") and window.get("min") and window.get("max"):
        scope = f"{field} : {window['min']} → {window['max']}"
    else:
        scope = field
    return (
        f"⚠️ profilé sur les {n} uniquement ({scope}) — "
        "min/max/cardinalités ci-dessous = cette fenêtre de profilage, "
        "PAS l'historique complet de la table"
    )


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
        # Une table absente de `used_columns` ne doit JAMAIS apparaître — même via ses
        # `derived_expressions`. Sinon un profil contaminé par des tables d'autres projets
        # (cache PII partagé) fuite leurs expressions dans le prompt (W6). La boucle des
        # derived_expressions plus bas ne filtrait que `if wanted_cols:` → pour une table
        # non demandée (wanted_cols vide), elle émettait tout.
        if tbl_key not in requested and short_key not in requested:
            continue
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
            win_note = _format_partition_window(tbl_data.get("partition_window"))
            if win_note:
                lines.append(f"  table `{short_key}` — {win_note}:")
            else:
                lines.append(f"  table `{short_key}`:")
            lines.extend(col_lines)

    joins = profile.get("joins", [])
    # Tables profilées par fenêtre de partition (last-N) : le match_rate d'une
    # jointure les impliquant est une estimation intra-fenêtre, pas un taux absolu.
    partitioned_tables: set[str] = set()
    for _tk, _td in profile["tables"].items():
        if _td.get("partition_window"):
            partitioned_tables.add(_tk)
            partitioned_tables.add(_tk.split(".")[-1])

    def _side_partitioned(tbl: str) -> bool:
        return tbl in partitioned_tables or tbl.split(".")[-1] in partitioned_tables

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
        window_disjoint = bool(j.get("window_disjoint"))
        match_rate = j.get("left_match_rate")
        # Quand les deux côtés sont partitionnés et que le match mesuré est 0, c'est
        # presque sûrement un artefact de fenêtres disjointes (chaque côté borné à ses
        # propres dernières partitions) — on masque le 0% trompeur (caveat plus bas).
        if not window_disjoint and match_rate is not None:
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
        if window_disjoint:
            line += (
                " [match indéterminé : les deux tables sont profilées sur leurs"
                " dernières partitions et les fenêtres ne se recouvrent pas —"
                " ne pas conclure que la jointure est vide]"
            )
        elif stats_parts and (_side_partitioned(lt) or _side_partitioned(rt)):
            # Match ET cardinalité (fanout) sont mesurés par côté sur la fenêtre :
            # des doublons s'étalant entre partitions peuvent faire lire un
            # many-to-* comme one-to-*. On le signale pour cadrer le LLM.
            line += (
                " [stats bornées aux dernières partitions de chaque côté"
                " (match et cardinalité)]"
            )
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


def _build_min_points_agg_hint_block(sql: str, dialect: str = "bigquery") -> str:
    """Warn the generator when the query uses a statistical aggregate that returns NULL
    on a single input row (CORR/COVAR_SAMP/STDDEV_SAMP/VAR_SAMP/REGR_*). With one row per
    GROUP BY group these yield NULL → a downstream value filter drops the group → empty
    result (bq143). Steers toward ≥2 (≥3 with a magnitude filter) varied rows per group,
    and toward a multi-sample scenario in the description."""
    if not sql:
        return ""
    try:
        from build_query.constraint_simplifier import detect_min_points_aggregates

        aggs = detect_min_points_aggregates(sql, dialect)
    except Exception:
        return ""
    if not aggs:
        return ""
    aggs_str = ", ".join(aggs)
    return (
        f"\n⚠️ **Agrégat statistique exigeant plusieurs points** ({aggs_str}) : ces "
        "fonctions renvoient **NULL sur une seule ligne**. Avec une seule ligne par groupe "
        "`GROUP BY`, le groupe vaut NULL et tout filtre aval sur cette valeur le supprime → "
        "**résultat vide**. Génère **≥2 lignes VARIÉES par groupe** : les lignes "
        "supplémentaires doivent varier sur la **dimension corrélée** (ex. plusieurs "
        "ENTITÉS/échantillons distincts par valeur de GROUP BY), pas dupliquer la même "
        "entité ni les mêmes valeurs. Si un filtre de MAGNITUDE suit (ex. "
        "`ABS(corr) <= 0.5`), prévois **≥3 points dispersés** : 2 points donnent une "
        "corrélation parfaite (±1) qui serait exclue. La description doit décrire un "
        "scénario MULTI-entités, pas une entité unique.\n"
        "⚠️ **Fenêtre temporelle** : si la variance est calculée sur une fenêtre "
        "(`OVER (... ORDER BY <date> ROWS/RANGE ...)`), les « plusieurs points » sont "
        "**plusieurs PÉRIODES pour la MÊME entité** (varie dans le TEMPS), PAS plusieurs "
        "entités. N'ajoute alors AUCUNE dimension/entité superflue — elle ferait exploser "
        "un CUBE/GROUP BY multi-axes sans servir la variance.\n"
    )


# ── Pré-digestion structurelle du SQL (partagée entre prompts) ──────────────
# Un aperçu compact du pipeline de CTEs, dérivé de `query_decomposed` (déjà produit
# par le validator → aucun re-parse sqlglot). Détection des opérations par heuristique
# regex (volontairement légère). Objectif : donner au LLM la CARTE de la requête
# (ordre d'exécution, entrées de chaque étape, ce que chaque étape fait) au lieu de le
# laisser ré-inférer la structure depuis le SQL brut. Réutilisable par tout prompt qui
# dispose de `query_decomposed` (générateur, suggestions, suggestion unique…).

# (motif regex, libellé) — premier match l'emporte par catégorie, ordre = priorité d'affichage.
_DIGEST_OP_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\bUNION\s+ALL\b", re.IGNORECASE), "union de branches"),
    (re.compile(r"\bOVER\s*\(", re.IGNORECASE), "fenêtre"),
    (
        re.compile(
            r"\bGROUP\s+BY\b|\b(?:SUM|COUNT|AVG|MIN|MAX|ARRAY_AGG|STRING_AGG)\s*\(",
            re.IGNORECASE,
        ),
        "agrège",
    ),
    # JOIN relationnel : un JOIN dont le mot suivant n'est PAS UNNEST (aplatissement de tableau).
    (re.compile(r"\bJOIN\s+(?!UNNEST\b)", re.IGNORECASE), "jointure"),
    (re.compile(r"\bUNNEST\s*\(", re.IGNORECASE), "déplie un tableau"),
    (re.compile(r"\b(?:WHERE|QUALIFY|HAVING)\b", re.IGNORECASE), "filtre"),
    (re.compile(r"\bDISTINCT\b", re.IGNORECASE), "dédoublonne"),
    (re.compile(r"\bLIMIT\b", re.IGNORECASE), "limite le nombre de lignes"),
]


def _digest_ops(code: str) -> list[str]:
    """Opérations SQL d'une étape, détectées par heuristique regex (pas d'AST)."""
    return [
        label for pattern, label in _DIGEST_OP_PATTERNS if pattern.search(code or "")
    ]


def _digest_inputs(step: dict, cte_names: set[str]) -> list[str]:
    """Entrées lisibles d'une étape : CTEs dont elle dépend + tables sources de base.
    Les noms de CTE sont gardés tels quels ; les tables de base sont raccourcies à leur
    nom court. Déduplique en conservant l'ordre."""
    seen: set[str] = set()
    out: list[str] = []
    for dep in step.get("dependencies") or []:
        if dep and dep in cte_names and dep not in seen:
            seen.add(dep)
            out.append(f"`{dep}`")
    for src in step.get("sources") or []:
        table = (src or {}).get("table") if isinstance(src, dict) else None
        if table:
            short = table.split(".")[-1]
            key = f"table:{short}"
            if key not in seen:
                seen.add(key)
                out.append(f"table `{short}`")
    return out


def build_sql_digest(query_decomposed, max_steps: int = 20) -> str:
    """Aperçu compact et ordonné du pipeline de CTEs, à injecter À CÔTÉ du SQL brut.

    ``query_decomposed`` : la liste ``[{name, code, dependencies, sources}]`` produite par
    le validator (acceptée aussi sous forme de chaîne JSON, comme stockée dans le state).
    Retourne ``""`` quand la décomposition est absente, illisible, ou triviale (≤1 étape :
    le SQL brut se suffit alors à lui-même). Aucune dépendance réseau ni re-parse sqlglot —
    sûr à appeler dans n'importe quel prompt."""
    if not query_decomposed:
        return ""
    try:
        steps = (
            json.loads(query_decomposed)
            if isinstance(query_decomposed, str)
            else query_decomposed
        )
    except Exception:
        return ""
    if not isinstance(steps, list) or len(steps) <= 1:
        return ""

    cte_names = {s.get("name") for s in steps if isinstance(s, dict) and s.get("name")}
    lines: list[str] = []
    for step in steps[:max_steps]:
        if not isinstance(step, dict):
            continue
        name = step.get("name") or "?"
        inputs = _digest_inputs(step, cte_names)
        ops = _digest_ops(step.get("code", ""))
        parts = [f"- `{name}`"]
        if inputs:
            parts.append(" ← " + ", ".join(inputs))
        if ops:
            parts.append(" · " + ", ".join(ops))
        if name == "final_query":
            parts.append(" · **résultat final**")
        lines.append("".join(parts))

    extra = len(steps) - max_steps
    if extra > 0:
        lines.append(f"- … (+{extra} étape(s))")

    if not lines:
        return ""
    return (
        "**Structure de la requête** (pipeline, dans l'ordre d'exécution — `étape` ← entrées · opérations) :\n"
        + "\n".join(lines)
    )


def compact_passthrough_sql(
    sql: str,
    dialect: str = "bigquery",
    *,
    threshold: int = 6,
    keep_samples: int = 3,
) -> str:
    """Compacte les projections « passthrough » (``col`` nu ou ``col AS col``) avant injection
    d'un SQL dans un prompt de suggestions (W2).

    Les modèles de staging projettent souvent des dizaines de colonnes transmises telles quelles
    (``x AS x`` × 40 × 8 CTEs) : en SQL pretty-printé, une colonne par ligne → le prompt est noyé
    sous du bruit sans logique (la structure est déjà portée par ``build_sql_digest``). Pour chaque
    SELECT comptant plus de ``threshold`` projections passthrough, on n'en garde que ``keep_samples``
    (dé-aliasées) et on résume le reste par un commentaire ``… (+N autres colonnes transmises telles
    quelles)``. Les projections PORTEUSES DE LOGIQUE (calculs, casts, renommages ``a AS b``,
    littéraux, agrégats, fenêtres) sont toujours conservées intégralement.

    Pur, sans effet de bord. Parsing en échec ou aucun SELECT compacté → ``sql`` renvoyé inchangé.
    À réserver aux prompts d'ANALYSE (suggestions) : ne pas appliquer au générateur de données,
    qui a besoin de la liste exacte des colonnes."""
    if not sql:
        return sql
    try:
        import sqlglot
        from sqlglot import exp

        tree = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return sql
    if tree is None:
        return sql

    def _is_passthrough(proj) -> bool:
        inner = proj.this if isinstance(proj, exp.Alias) else proj
        if not isinstance(inner, exp.Column):
            return False
        if isinstance(proj, exp.Alias):
            return (proj.alias or "").lower() == (inner.name or "").lower()
        return True

    changed = False
    for sel in list(tree.find_all(exp.Select)):
        projections = sel.expressions
        pt_total = sum(1 for p in projections if _is_passthrough(p))
        if pt_total <= threshold:
            continue
        new_exprs: list = []
        seen_pt = 0
        last_sample = None
        for proj in projections:
            if not _is_passthrough(proj):
                new_exprs.append(proj)
                continue
            seen_pt += 1
            if seen_pt <= keep_samples:
                inner = proj.this if isinstance(proj, exp.Alias) else proj
                bare = inner.copy()  # dé-aliase (`col AS col` → `col`)
                bare.comments = None  # repart d'un nœud sans commentaire hérité
                new_exprs.append(bare)
                last_sample = bare
        remaining = pt_total - min(keep_samples, pt_total)
        if last_sample is not None and remaining > 0:
            last_sample.add_comments(
                [f"… (+{remaining} autres colonnes transmises telles quelles)"]
            )
        sel.set("expressions", new_exprs)
        changed = True

    if not changed:
        return sql
    try:
        return tree.sql(dialect=dialect, pretty=True)
    except Exception:
        return sql


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

# Champs en langage naturel de la réponse few-shot, par langue de sortie.
# Le LLM recopie la langue de la réponse AI d'exemple plus sûrement que la
# directive de langue — ces champs DOIVENT donc être dans la langue de sortie.
_FEW_SHOT_ANSWER_TEXTS = {
    "fr": {
        "reasoning": (
            "La clé de jointure est dérivée (CASE → CONCAT('BP', type_carte)) : "
            "type_carte='GOLD' produit 'BPGOLD' = produits.code, et C001 n'a "
            "aucune ligne au 2024-02-29 pour rester une ouverture."
        ),
        "test_name": "Ouverture carte Gold réseau BP",
        "description": (
            "Pour la carte C001 (réseau BP, type GOLD) présente sur la photo du "
            "2024-03-31 et absente de celle du 2024-02-29 → elle ressort comme "
            "ouverture avec le libellé Carte Gold BP."
        ),
    },
    "en": {
        "reasoning": (
            "The join key is derived (CASE → CONCAT('BP', type_carte)): "
            "type_carte='GOLD' produces 'BPGOLD' = produits.code, and C001 has "
            "no row on 2024-02-29 so it qualifies as an opening."
        ),
        "test_name": "Gold card opening on BP network",
        "description": (
            "For card C001 (network BP, type GOLD) present in the 2024-03-31 "
            "snapshot and absent from the 2024-02-29 one → it comes out as an "
            "opening with the label Carte Gold BP."
        ),
    },
}


def _few_shot_example_answer() -> str:
    texts = _FEW_SHOT_ANSWER_TEXTS.get(get_language(), _FEW_SHOT_ANSWER_TEXTS["en"])
    return json.dumps(
        {
            "unit_test_build_reasoning": texts["reasoning"],
            "test_name": texts["test_name"],
            "unit_test_description": texts["description"],
            "tags": [tag_labels()[0]],
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


def _few_shot_messages() -> list[tuple[str, str]]:
    """Paire few-shot (human, ai) — construite à l'appel pour suivre la langue de sortie."""
    return [
        ("human", _FEW_SHOT_EXAMPLE_HUMAN),
        ("ai", _few_shot_example_answer()),
    ]


# Squelette du format de description + exemples travaillés, par langue de sortie.
# Ce sont les ancres que le LLM recopie dans sa sortie : elles doivent être dans
# la langue de sortie même si le reste des instructions reste en français.
_DESC_FORMAT = {
    "fr": {
        "skeleton": "Pour [sujet avec valeurs concrètes : IDs, dates, montants, statuts] [condition/situation] → [résultat attendu]",
        "skeleton_short": "Pour [sujet avec valeurs concrètes] [condition] → [résultat attendu]",
        "example": "Pour le porteur COLLAB789 (banque 001) dont la carte démarre le 2026-01-15 → il est compté comme OUVERTURE sur le mois d'analyse",
        "name_example": "Ouverture nouveau client janvier",
    },
    "en": {
        "skeleton": "For [subject with concrete values: IDs, dates, amounts, statuses] [condition/situation] → [expected result]",
        "skeleton_short": "For [subject with concrete values] [condition] → [expected result]",
        "example": "For cardholder COLLAB789 (bank 001) whose card starts on 2026-01-15 → it is counted as an OPENING for the analysis month",
        "name_example": "New customer opening January",
    },
}


def description_format() -> dict:
    """Squelette/exemples de `unit_test_description` dans la langue de sortie configurée."""
    return _DESC_FORMAT.get(get_language(), _DESC_FORMAT["en"])


def _focus_note(focus_path: str) -> str:
    """Note injectée quand la génération d'un test cible une branche d'un UNION ALL.

    Le `focus_path` ne pilote QUE la génération des données ; le test s'EXÉCUTE sur le script
    complet. La description doit donc décrire la sortie complète — et une ASYMÉTRIE entre
    branches complémentaires (un même sujet présent dans une branche, absent dans l'autre) est
    une information métier VALIDE à expliciter, pas un défaut à masquer. ``""`` si pas de focus
    (``focus_path`` vide ou ``"all"``). Partagée par generate_data_prompt et update_data_prompt.
    """
    if not focus_path or focus_path == "all":
        return ""
    return f"""

**Focus de génération — branche « {focus_path} » d'un UNION ALL** : tu cibles les DONNÉES pour allumer cette branche (le schéma et les contraintes ci-dessus sont déjà réduits à elle). ⚠️ MAIS le test s'EXÉCUTE sur le SCRIPT COMPLET (toutes les branches du UNION ALL), pas sur cette seule branche. Conséquences :
- `unit_test_description` décrit la sortie du SCRIPT COMPLET pour ces données.
- Les branches complémentaires (ex. « activité » vs « parc ») peuvent produire un résultat ASYMÉTRIQUE : un même sujet peut apparaître dans une branche et PAS dans l'autre. C'est une INFORMATION MÉTIER valide (ex. « ce contrat a de l'activité mais n'apparaît pas dans le parc ») — décris-la explicitement quand elle se produit, ne la masque pas et n'invente pas d'office les indicateurs des autres branches.
- N'annonce que des valeurs de sortie réellement produites par le script complet sur ces données."""


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
    multi_branch: bool = False,
    focus_path: str = "",
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

    # Leçons apprises de corrections passées (par table / jointure) — réinjectées
    # pour ne pas répéter une erreur déjà corrigée sur un autre test.
    lessons_block_str = format_lessons_block(profile, used_columns)
    lessons_block = f"\n{lessons_block_str}\n" if lessons_block_str else ""

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
    min_points_hint_block = _build_min_points_agg_hint_block(sql, dialect)

    if user_instruction:
        consignes_1_2 = """\
1. **Respectez l'instruction utilisateur** : le test doit implémenter exactement le scénario décrit
   dans l'instruction ci-dessous, même s'il s'agit d'un cas limite, d'un résultat vide ou de valeurs NULL.
2. **Cas spécifiques permis** : contrairement au test standard, ce test peut couvrir des scénarios
   d'erreurs, de jointures défaillantes ou tout autre cas demandé par l'instruction."""
    else:
        consignes_1_2 = """\
1. **Cas nominal = le BUT de la requête réalisé une fois proprement, ET observable.** Construisez le **témoin minimal** qui (a) SURVIT jusqu'au SELECT final (résultat non vide, sans NULL ni valeurs vides gratuites) **et** (b) produit une sortie sur laquelle on peut **formuler une assertion** — un nombre de lignes maîtrisé, pas un pavé.
   **« Minimal » se mesure sur la SORTIE, pas sur l'entrée.** Avant de générer, prédisez combien de lignes la requête va émettre d'après ses **opérateurs structurels**, et gardez ce nombre aussi petit que la requête l'autorise :
   - `GROUP BY CUBE / ROLLUP / GROUPING SETS` → une ligne **par combinaison de regroupement** (2^N sous-totaux **même avec une seule entité**). Impossible à supprimer par les données — alors **n'ajoutez aucune valeur de dimension superflue** (une 2ᵉ banque, un 2ᵉ réseau multiplient les sous-totaux) et **ancrez l'assertion sur UNE ligne identifiable** (la ligne de détail, ou le total), nommée par ses colonnes discriminantes dans la description.
   - `UNNEST(array)` / `CROSS JOIN` → une ligne **par élément** : gardez les tableaux au strict nécessaire pour que la logique tienne (la fenêtre exige N éléments → exactement N, pas plus).
   - Les fonctions de fenêtre ne démultiplient pas (1 ligne → 1 ligne) ; un **détecteur** émet en revanche **toutes** les lignes qui franchissent le seuil → n'en fabriquez **qu'une**.
   **Forme selon la dernière clause filtrante :**
   - elle laisse passer des données ordinaires (jointures, agrégats, dérivations) → un cas d'usage métier **standard** suffit ;
   - elle ne retient qu'une condition **rare** (seuil sur valeur calculée — z-score, percentile, écart à une moyenne mobile —, `HAVING`, `QUALIFY`, sélection d'un extrême) → le témoin nominal **EST l'occurrence de cette condition**. Des données uniformément « normales » donnent un résultat **VIDE** : **fabriquez le contraste** qui la déclenche (une population de référence assez longue pour remplir la fenêtre — comptez les `PRECEDING` et les `ARRAY_LENGTH(...) >= N` — + un point déviant dont l'écart **brut** dépasse le seuil), surtout ne lissez pas. Méfiez-vous des transformations appliquées à la stat mais pas au numérateur (winsorisation / percentile) : gardez la base resserrée, faites porter le pic sur la valeur brute.
   **La description affirme un fait vérifiable sur la sortie réelle, jamais une valeur que vous ne pouvez pas connaître.** Précisez les **entrées** (IDs, dates, montants) et le **sens** du résultat (« franchit le seuil de détection », « ressort comme ouverture ») — **interdiction d'énoncer une valeur calculée** (z-score, moyenne, total agrégé) : vous l'inventeriez, et le juge la verra fausse.
2. **Exclusion des cas exceptionnels** : pas de scénarios d'erreurs ni de jointures défaillantes. La déviation exigée par une clause de détection (point 1) n'en est PAS une : c'est la cible métier que la requête existe pour capturer."""

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

    if multi_branch:
        # Requête à branches mutuellement exclusives (UNION ALL) : on demande un
        # branch_plan bidirectionnel (must_hold / must_not_hold) et on distingue
        # sélection-par-table vs sélection-par-valeur (cas des photos M/M-1).
        reasoning_bullet += (
            " Pour une branche inter-dépendante, citez aussi la condition qui doit rester "
            "FAUSSE (branche concurrente vide, anti-jointure non matchée — voir `branch_plan.must_not_hold`)."
        )
        consigne_3 = """\
3. **Une seule branche, sélection explicite** : quand le SQL contient plusieurs alternatives (`A OR B`, `CASE WHEN`, plusieurs chemins de jointure), choisir **une seule branche** et renseigner `branch_plan` AVANT de générer `data`. La STRATÉGIE dépend de la façon dont la branche est sélectionnée :
   - **Sélection par TABLE** (une table / un chemin de jointure dédié par alternative) → laisser les tables des autres branches VIDES (null).
   - **Sélection par VALEUR** (date, statut, type sur des tables PARTAGÉES — cas des photos M/M-1) → ne vider AUCUNE table : placer la valeur dans l'intervalle qui satisfait UNIQUEMENT la branche visée (cf. #5), et garantir qu'aucune ligne ne retombe dans une autre branche pour la même clé.
   À défaut d'instruction utilisateur, préférer la branche **sans dépendance inter-branches** (sans anti-jointure ni `NOT IN` référençant une autre branche). La description nomme la branche choisie. Les autres branches alimentent les suggestions.
3bis. **Branches inter-dépendantes (anti-jointure croisée)** : si la branche ciblée comporte une anti-jointure contre une CTE issue d'une AUTRE branche du `UNION ALL` (`NOT IN (SELECT … FROM autre_branche)`), les autres branches doivent rester VIDES pour la clé testée — liste-le dans `branch_plan.must_not_hold`. Ne cible JAMAIS une branche dont la survie exigerait qu'une autre branche soit simultanément peuplée ET vide."""
    else:
        consigne_3 = """\
3. **Une seule branche** : quand le SQL contient plusieurs alternatives (`condition_A OR condition_B`, `CASE WHEN … THEN … ELSE …`, plusieurs chemins de jointure), choisir **une seule branche** et construire des données qui satisfont uniquement celle-ci. Ne pas couvrir plusieurs alternatives à la fois. La description nomme explicitement la branche choisie (ex. "Pour un utilisateur premium …", pas "Pour un utilisateur premium ou avec cumulated_montant > 1000 …"). Les tables propres aux autres branches peuvent rester à null ; les tables PARTAGÉES doivent rester cohérentes avec la branche choisie. Les autres branches alimentent les suggestions."""

    # Focus de GÉNÉRATION sur une branche UNION ALL (cf. _focus_note) : données ciblées sur la
    # branche, mais exécution sur le script complet → la description couvre la sortie complète.
    focus_note = _focus_note(focus_path)

    system_message_content = (
        output_language_directive()
        + """

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
        + "\n"
        + consigne_3
        + focus_note
        + """
4. **Clés de jointure DÉRIVÉES (le point le plus important)** : quand une clé de JOIN est le produit d'un `CASE` / `CAST` / `SAFE_CAST` / `SUBSTR` / `SPLIT` / `REGEXP`, générez la valeur SOURCE qui, APRÈS transformation, égale la clé de l'autre côté — **pas** la valeur finale.
   - `JOIN ON a.k = CASE WHEN t.reseau='BP' THEN '1' END` et `a.k` vient de `t2` → mettez `t2.k = '1'`.
   - `SUBSTR(col, 2, LEN(col)-2)` puis `SPLIT ','` : la colonne SOURCE doit inclure les caractères de bord qui seront retirés — mais avec des bords **NON-QUOTES** (`xPROD1x`, `_PROD1_`, `[PROD1]`), pas `PROD1` brut. ⚠️ N'emballe JAMAIS toute la valeur dans des quotes (`'PROD1'`, `'"PROD1"'`) : l'insertion retire tout emballage de quotes imbriqué comme un artefact → la valeur arriverait NUE et le `SUBSTR` mangerait de vraies lettres (`PROD1` → `ROD`). Si le SQL retire spécifiquement une quote (`TRIM(col, '"')`), garde CETTE quote à l'intérieur, entourée d'un bord non-quote : `x"PROD1"x`.
   - `SAFE_CAST(x AS INT64)` utilisé en clé : `x` doit être numérique des deux côtés et égal.
5. **Conditions temporelles & formats de date** : décodez les bornes de chaque filtre de date AVANT de générer. Les bornes viennent des **littéraux du SQL, jamais de la date courante**. Si une CTE exige `dt_deb < D1 AND dt_fin >= D2` et qu'une autre caractérise la branche par l'inverse (présent en M, absent en M-1), placez les dates dans l'intervalle qui satisfait UNIQUEMENT la branche visée. Méfiez-vous des sentinelles (`'0001-01-01'`, `'9999-12-31'`). Quand une fonction de parsing (`PARSE_DATE`, `SAFE.PARSE_DATE`, …) lit la valeur, la chaîne d'entrée doit respecter **exactement** le format attendu. ⚠️ Ne confonds pas : un champ **typé** `date`/`timestamp` du schéma s'écrit TOUJOURS en **ISO** (`YYYY-MM-DD`, resp. `YYYY-MM-DDTHH:MM:SS`), quel que soit le format des littéraux du SQL. Le « format exact » ci-dessus ne concerne QUE les colonnes **TEXTE** que le SQL parse lui-même (`PARSE_DATE('%d-%m-%Y', col)` → la colonne source est une STRING, écris `01-01-2026`).
6. **Cohérence inter-tables** : les valeurs partagées entre tables — clés de jointure ET littéraux des filtres (année, dates, statuts, devises) — doivent être **cohérentes dans TOUTES les tables**. Si la requête filtre 2017, aucune donnée 2016. Une valeur cohérente sur une table mais incohérente avec les autres rend le test invalide.
7. **Agrégats** : si le SQL contient `GROUP BY` + `COUNT`/`SUM`/`AVG`/`STDDEV`/`CORR`, mettez plusieurs lignes **variées** partageant la **même clé de groupe** (sinon COUNT=1, STDDEV=0). Ex. : GROUP BY date → 3 lignes `date='2024-01-01'` et 2 lignes `date='2024-01-02'`, pas une date par ligne. Gardez la clé **unique côté table de dimension** pour éviter un fan-out many-to-many qui fausse l'agrégat. Si `ORDER BY` + `OFFSET` sur l'agrégat, des COUNT tous distincts (3, 2, 1 — pas 3, 1, 1, 1 qui créent un ex æquo) pour un OFFSET déterministe.
8. **LEFT JOIN** : le prédicat ON n'a PAS à matcher pour qu'une ligne survive — une table en LEFT JOIN peut rester vide si le scénario ne la requiert pas. Les INNER JOIN, eux, exigent des clés correspondantes et non nulles.
9. **Anti-jointures / `NOT IN` / `NOT EXISTS`** : générez des données qui NE matchent PAS la table anti-jointe (sinon la ligne est supprimée).
10. **NULL** : pas de valeur NULL/vide GRATUITE. Exception explicite : si la logique testée DÉPEND d'un NULL (ex. `LEFT JOIN … WHERE x IS NULL`, `segment IS NULL`), produisez ce NULL — c'est le test.
11. **Casse stricte** : les clés de `data` reprennent EXACTEMENT les clés de `<schema>` (format `{dataset}_{table}`).
12. **Un seul test**, et **ne pas inclure la requête SQL** dans le résultat.

**Ce que cet outil NE doit PAS tester** (laisser au moteur de warehouse) :
- Les expressions constantes dans le SELECT final (ex. `SELECT 'valeur_fixe' AS col`) : résultat trivial, aucune logique métier à valider.
- Les fonctions d'agrégation pures (SUM, AVG, COUNT, MIN, MAX) sans filtre ou condition métier : elles sont garanties correctes par le moteur SQL.
- Le comportement interne des fonctions SQL (ordre de STRING_AGG, précision de CAST, format de DATE_FORMAT…) : tester la DB, pas la logique métier.
- Les règles de calcul que toute implémentation SQL correcte produirait identiquement.
Privilégier des scénarios où **la logique métier** — les filtres, jointures, conditions temporelles, règles de déduplication — est réellement testée.

**Format de sortie obligatoire** : un objet JSON unique, champs dans l'ordre du schéma (reasoning d'abord).
"""
        + reasoning_bullet
        + f"""
- `test_name`: 3-6 mots, lecteur métier, sans jargon SQL ni noms techniques (ex. "{description_format()["name_example"]}"). Pas de noms de CTE/colonnes.
- `unit_test_description`: Description **métier contextualisée** au format *"{description_format()["skeleton"]}"*. Nommer la branche choisie quand le SQL a des alternatives. Exemple : "{description_format()["example"]}". Interdits : noms de colonnes/CTE, syntaxe SQL, formulations génériques type "Vérifie que le calcul est correct", et **toute valeur calculée** (z-score, moyenne, total agrégé) que vous ne pouvez pas connaître sans exécuter la requête.
"""
        + f"- `tags`: Labels décrivant les types de cas couverts. Choisir parmi : {', '.join(f'`{t}`' for t in tag_labels())}.\n"
        + """- `data`: Données cohérentes, correctes pour la requête.

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
        f"{lessons_block}"
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
        f"{min_points_hint_block}"
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
Génère **un seul** test unitaire en appliquant les consignes du message système.
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
    prompt_messages.extend(_few_shot_messages())
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
    focus_path: str = "",
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
        output_language_directive() + "\n\n"
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
        + _focus_note(focus_path)
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
    min_points_hint_block = _build_min_points_agg_hint_block(sql, dialect)

    final_human_message_content = f"""
Modifie les données JSON selon l'instruction ci-dessous :
{sql_block}{volume_hints_block}{fanout_hint_block}{min_points_hint_block}{existing_test_block}
<Instruction>
{user_input}
</Instruction>

Génère un unique test unitaire modifié selon l'instruction ci-dessus.
Respecte l'ordre des champs : unit_test_description, unit_test_build_reasoning, tags, data.
- `unit_test_description` : description métier au format "{description_format()["skeleton_short"]}" — mentionner des valeurs concrètes (IDs, dates, statuts), pas de formulation générique
- `tags` : labels pertinents parmi {", ".join(tag_labels())}
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
        f"- unit_test_description: business-contextualized description in the format '{description_format()['skeleton_short']}'. Mention concrete values (IDs, dates, statuses). Avoid generic formulations like 'Checks the computation is correct'.\n"
        f"- tags: relevant labels among {', '.join(tag_labels())}.\n"
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
    *,
    dialect: str = "",
    history: Optional[List[BaseMessage]] = None,
) -> ChatPromptTemplate:
    """Routeur d'intention MockSQL → JSON {"reasoning","route"} avec route ∈ {"generator","other"}.

    `generator` = l'utilisateur demande à MockSQL d'agir (créer/modifier/supprimer un test) ;
    `other` = l'utilisateur pose une question ou réfléchit à voix haute.
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

    lessons_block_str = format_lessons_block(profile, used_columns or [])
    if lessons_block_str:
        profile_section += f"\n{lessons_block_str}\n"

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
    lang = output_language_name()
    if script1 and script2:
        diff = difflib.unified_diff(
            script1.splitlines(),
            script2.splitlines(),
            lineterm="",
        )
        diff_text = "\n".join(diff)
        if diff_text.strip() != "":
            query_text = (
                f"Please explain in {lang} what exactly has changed in the initial query "
                f"without 100 words.\nDo not translate table names.\n{diff_text}"
            )
        else:
            query_text = f"Please explain in {lang} the query without 100 words.\nDo not translate table names.\n{script1}"
    else:
        script = script1 or script2
        query_text = f"Please explain in {lang} the query without 100 words.\nDo not translate table names.\n{script}"

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
