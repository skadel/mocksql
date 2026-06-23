import json
import logging
import re
import uuid

import sqlglot
from sqlglot import exp
from pydantic import BaseModel, Field

from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.examples_generator import retrieve_existing_tests
from build_query.path_slicer import ALL_PATH
from build_query.prompt_tools import (
    _format_profile_block,
    build_sql_digest,
    compact_passthrough_sql,
)
from build_query.state import QueryState
from storage.config import is_native_thinking_active
from storage.test_repository import get_test, update_test
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE
from utils.saver import get_message_type
from utils.sqlglot_ast import strip_with
from utils.test_utils import build_test_detail

logger = logging.getLogger(__name__)

# Plafond de suggestions actives affichées dans le panneau. En mode append (l'agent
# « rajoute des suggestions »), au-delà de ce nombre on garde les plus récentes ; quand
# le plafond est déjà atteint, on prévient l'utilisateur au lieu d'ajouter en silence.
SUGGESTIONS_CAP = 5


def _merge_suggestions(
    new: list[str], pending: list[str], cap: int = SUGGESTIONS_CAP
) -> list[str]:
    """Mode append : nouvelles suggestions en tête (les plus récentes), puis celles
    déjà en attente. Déduplique (1ʳᵉ occurrence conservée) et tronque au plafond —
    si l'ajout dépasse ``cap``, ce sont les plus anciennes qui sont écartées."""
    seen: set[str] = set()
    merged: list[str] = []
    for s in [*new, *pending]:
        s = (s or "").strip()
        if s and s not in seen:
            seen.add(s)
            merged.append(s)
    return merged[:cap]


# ---------------------------------------------------------------------------
# Catalogue de pièges — sélectionné selon les constructions réellement présentes
# dans le SQL (cf. _select_pitfalls). Évite de noyer le prompt sous des sections
# non pertinentes (ex. fonctions fenêtre absentes du SQL analysé).
# ---------------------------------------------------------------------------

_PITFALL_AGG = """Agrégats contre-intuitifs :
- COUNT DISTINCT non-additif : sum(count_distinct par sous-groupe) ≠ count_distinct global — un même élément peut apparaître dans plusieurs groupes
- Ratio d'agrégats : sum(ratio) ≠ sum(numérateur) / sum(dénominateur) — le ratio ne peut pas être ré-agrégé
- NULL exclus silencieusement : COUNT(col) ≠ COUNT(*) quand col contient des NULLs ; SUM/AVG ignorent aussi les NULLs
- Dénominateur nul : si le dénominateur d'un ratio peut être 0, la requête explose ou retourne NULL sans warning
- Agrégation multi-niveaux : une métrique calculée à granularité fine puis ré-agrégée peut différer du calcul direct au niveau grossier"""

_PITFALL_WINDOW = """Fonctions fenêtre (LAG, LEAD, RANK, etc.) :
- LAG/LEAD retournent NULL sur la première/dernière ligne de la partition — que fait la logique en aval avec ce NULL ?
- ROW_NUMBER sur ex æquo : non-déterministe sans colonne de départage unique
- RANK vs DENSE_RANK : RANK saute des numéros après un ex æquo (1,1,3), DENSE_RANK non (1,1,2) — lequel est attendu ?
- LAST_VALUE piège : la frame par défaut est ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, pas toute la partition — LAST_VALUE retourne souvent la valeur courante, pas la dernière de la partition
- Fenêtre glissante en début de série : les N premières lignes ont une fenêtre plus petite que N → moyenne/variance calculée sur moins de points, ce qui peut générer de faux positifs ou faux négatifs
- Cumul avec ORDER BY et doublons : si deux lignes ont la même valeur de tri, leur ordre relatif est aléatoire et le cumul est non-déterministe"""

_PITFALL_STATS = """Algorithmes statistiques (z-score, anomalies, seuils dynamiques) :
- Contamination du baseline : si une anomalie fait partie de la fenêtre de calcul de la moyenne/variance, elle tire le seuil vers le haut — exemple : stable pendant 11 mois, hausse en M+12, hausse similaire en M+13 → M+12 gonfle la variance et M+13 n'est plus détecté comme anomalie
- Dérive progressive masquée : une série d'anomalies successives peut décaler le baseline progressivement sans qu'aucune ne dépasse le seuil individuellement
- Fenêtre trop courte : en début de série, la variance est calculée sur peu de points, le z-score est instable et peut déclencher de faux positifs
- Percentile/médiane sur peu de points : un percentile (médiane, P90, etc.) calculé sur 1 ou 2 valeurs n'a pas de sens — l'interpolation renvoie une valeur plausible mais arbitraire, et un percentile « par fenêtre » (calculé sur toute la population au lieu d'un sous-groupe) mélange des populations qui ne devraient pas l'être"""

_PITFALL_GROUPING = """Agrégats multi-niveaux (ROLLUP, CUBE, GROUPING SETS) :
- Lignes de sous-total et de total global : ROLLUP/CUBE ajoutent des lignes où une ou plusieurs clés de regroupement valent NULL — ces NULL signifient « tous » (le total), pas une donnée absente. Sans GROUPING(), impossible de distinguer un vrai NULL métier d'une ligne de total → un filtre `WHERE col IS NOT NULL` ou un JOIN sur cette clé fait disparaître ou dédouble silencieusement les totaux
- Double comptage en aval : ré-agréger ou sommer un résultat qui contient déjà des lignes de sous-total compte chaque valeur deux fois (une fois dans le détail, une fois dans le total)
- Total global manquant ou en trop : selon que le métier attend une ligne « toutes catégories confondues », sa présence/absence change le nombre de lignes du rapport sans changer le détail"""

_PITFALL_JOINS = """JOINs :
- Fan-out silencieux : clé de jointure non-unique → multiplication des lignes avant agrégation, les SUM/COUNT sont gonflés sans erreur
- Comptage d'entités via JOIN sur table de faits : si le SQL compte des entités distinctes (clients, points de vente, commandes) en les joingnant à une table où elles apparaissent plusieurs fois (contrats, transactions, événements), chaque entité est comptée N fois sauf si un DISTINCT ou une dédoublication explicite est en place — c'est l'un des bugs les plus fréquents en BI, souvent invisible car le résultat reste plausible (ex. +5%)
- NULL dans la clé de jointure : un NULL ne matche jamais un autre NULL en SQL → lignes silencieusement perdues avec INNER JOIN"""

_ALL_PITFALLS = "\n\n".join(
    [_PITFALL_AGG, _PITFALL_WINDOW, _PITFALL_STATS, _PITFALL_GROUPING, _PITFALL_JOINS]
)

# Fonctions d'agrégation statistique → déclenchent la section z-score/anomalies.
_STAT_FUNC_RE = re.compile(
    r"\b(?:STDDEV\w*|VARIANCE|VAR_(?:POP|SAMP)|PERCENTILE_(?:CONT|DISC)"
    r"|APPROX_QUANTILES|CORR|COVAR_(?:POP|SAMP))\s*\(",
    re.IGNORECASE,
)


def _has_real_table_join(tree: exp.Expression) -> bool:
    """Vrai si le SQL contient un JOIN entre tables (relationnel), pas seulement un
    ``CROSS/LEFT JOIN UNNEST(...)``. sqlglot représente l'aplatissement de tableau par
    un ``exp.Join`` dont le ``this`` est un ``exp.Unnest`` — ce n'est PAS un join
    relationnel et ne doit pas déclencher la section « fan-out / clé non-unique »."""
    for join in tree.find_all(exp.Join):
        if not isinstance(join.this, exp.Unnest):
            return True
    return False


def _select_pitfalls(sql: str, dialect: str) -> str:
    """Ne garder que les sections du catalogue pertinentes pour ce SQL.

    Détection via l'AST sqlglot (fenêtres, agrégats, grouping sets, joins) + regex
    (fonctions statistiques). En cas d'échec de parsing, on retombe sur le catalogue
    complet pour ne jamais perdre de couverture.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except Exception:
        return _ALL_PITFALLS
    if tree is None:
        return _ALL_PITFALLS

    sections: list[str] = []
    if tree.find(exp.AggFunc) is not None or tree.find(exp.Group) is not None:
        sections.append(_PITFALL_AGG)
    if tree.find(exp.Window) is not None:
        sections.append(_PITFALL_WINDOW)
    if _STAT_FUNC_RE.search(sql):
        sections.append(_PITFALL_STATS)
    if tree.find(exp.Rollup, exp.Cube, exp.GroupingSets) is not None:
        sections.append(_PITFALL_GROUPING)
    if _has_real_table_join(tree):
        sections.append(_PITFALL_JOINS)

    return "\n\n".join(sections) if sections else _ALL_PITFALLS


# 1. Structure Pydantic (avec Chain of Thought)
class TestSuggestion(BaseModel):
    text: str = Field(
        description=(
            "La suggestion, formulée en langage métier. Commence par un verbe et décrit un "
            "comportement observable pour le domaine (ex : un total incohérent, des lignes "
            "manquantes, un classement incorrect). Évite le jargon d'implémentation — noms de "
            "fonctions SQL, d'opérateurs, de colonnes ou de CTE (pas de EXTRACT, COUNT DISTINCT, "
            "LAG, JOIN, CTE, etc.). Un terme d'analyse indispensable au sens "
            "(référence/baseline, écart-type, seuil, percentile) est toléré s'il est nécessaire "
            'pour décrire le symptôme. Préfixée par "[PROD] " si et seulement si elle est ancrée '
            "sur le profil statistique réel fourni."
        )
    )
    rationale: str = Field(
        default="",
        description=(
            "OBLIGATOIRE et NON VIDE pour les suggestions [PROD], vide sinon. "
            "Une phrase en langage métier qui cite la preuve chiffrée tirée du profil et qui "
            "explique pourquoi ce cas mérite un test — ex : "
            "\"Le profil indique que le champ 'code banque' est vide 3% du temps : ce cas n'est "
            'couvert par aucun test existant." Doit citer une valeur concrète du profil '
            "(taux de NULL, min/max, cardinalité, valeur observée), jamais une fonction SQL."
        ),
    )
    target_path: str = Field(
        default="",
        description=(
            "Nom machine EXACT d'une branche du catalogue <branches_union_all> sur laquelle "
            "FOCALISER ce test (repris tel quel). À ne renseigner QUE si isoler cette branche "
            "révèle un cas qu'un test sur l'assemblage complet masquerait. Vide (\"\") sinon — "
            "c'est le cas par défaut, ne propose pas un focus par branche « pour la forme »."
        ),
    )


class TestSuggestionsOutput(BaseModel):
    analyse_des_manques: str = Field(
        description=(
            "Justification brève : 3 ou 4 phrases maximum, jamais plus. "
            "Le raisonnement détaillé se fait dans le canal de réflexion (thinking natif), "
            "hors de ce champ — ici on ne garde que la conclusion. "
            "Identifie le pattern métier du SQL et les hypothèses implicites sur les données "
            "dont dépend son bon fonctionnement, pour justifier le choix des 3 suggestions."
        )
    )
    suggestions: list[TestSuggestion] = Field(
        description="Liste exacte de 3 suggestions de cas de tests non couverts.",
        min_length=1,
        max_length=3,
    )


def _extract_verdicts(state) -> dict:
    """Return {test_index: verdict_text} from EVALUATION messages in state."""
    verdicts: dict = {}
    for m in state.get("messages", []):
        if get_message_type(m) == MsgType.EVALUATION:
            idx = m.additional_kwargs.get("test_index")
            if idx is not None:
                verdicts[idx] = m.content
    return verdicts


def _format_test_block(tc: dict, verdict: str | None, max_rows: int = 3) -> str:
    """Format a single test case (input, output, verdict) for the suggestion prompt."""
    detail = build_test_detail(tc)
    parts = []

    tags = ", ".join(detail.get("tags") or []) or "—"
    status = detail.get("status") or "?"
    parts.append(f"  Description : {detail['description']}")
    parts.append(f"  Tags : {tags} | Statut d'exécution : {status}")

    if verdict:
        parts.append(f"  Verdict : {verdict}")

    input_data = detail.get("input_data") or {}
    if input_data:
        parts.append("  Données d'entrée :")
        for table_name, rows in input_data.items():
            if isinstance(rows, list):
                shown = rows[:max_rows]
                extra = (
                    f" (+{len(rows) - max_rows} autres)" if len(rows) > max_rows else ""
                )
                parts.append(
                    f"    {table_name}: {json.dumps(shown, ensure_ascii=False)}{extra}"
                )

    result_rows = detail.get("result_rows") or []
    row_count = detail.get("row_count", 0)
    if result_rows:
        shown = result_rows[:max_rows]
        extra = f" (+{row_count - max_rows} autres)" if row_count > max_rows else ""
        parts.append(f"  Résultat DuckDB ({row_count} ligne(s)) :")
        parts.append(f"    {json.dumps(shown, ensure_ascii=False)}{extra}")
    elif status == "empty_results":
        parts.append("  Résultat DuckDB : 0 ligne retournée")

    if detail.get("error"):
        parts.append(f"  Erreur : {detail['error']}")

    return "\n".join(parts)


def _parse_path_plans(state: QueryState) -> dict:
    """``path_plans`` (catalogue UNION ALL) en dict, ``{}`` si absent/illisible."""
    raw = state.get("path_plans")
    if not raw:
        return {}
    try:
        plans = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return {}
    return plans if isinstance(plans, dict) else {}


def _covered_paths(state: QueryState, test_cases: list[dict]) -> set:
    """Paths déjà couverts : un test porte ce ``target_path`` (inclut le test fraîchement
    généré via ``state['target_path']``, pas encore persisté)."""
    covered = {tc.get("target_path") for tc in test_cases if tc.get("target_path")}
    if state.get("target_path"):
        covered.add(state["target_path"])
    return covered


def _branch_discriminant_sql(sliced: str, dialect: str, limit: int = 2500) -> str:
    """Partie discriminante d'une branche slicée : son SELECT final, débarrassé du préfixe
    CTE commun.

    Les branches d'un UNION ALL partagent un long tronc ``WITH`` (souvent > ``limit``) et ne
    diffèrent que par leur SELECT (le filtre/littéral propre + les colonnes propres). On retire
    donc le ``WITH`` pour exposer ce SELECT discriminant — injecté tel quel dans le catalogue de
    branches du suggesteur (cf. ``_branch_catalog_block``). Parsing en échec → repli sur la tête
    du sliced brut."""
    try:
        parsed = sqlglot.parse_one(sliced, read=dialect)
        rendered = (
            strip_with(parsed).sql(dialect=dialect, pretty=True).strip()
            if parsed is not None
            else sliced
        )
    except Exception:
        rendered = sliced
    rendered = rendered or sliced
    if len(rendered) > limit:
        rendered = rendered[:limit] + "\n-- … (tronqué)"
    return rendered


def _branch_catalog_block(state: QueryState, covered: set, dialect: str) -> str:
    """Catalogue des branches d'un UNION ALL injecté dans le suggesteur CONTEXTUEL.

    Pour chaque branche : nom machine, couverture, et SELECT discriminant (sans le tronc CTE
    commun). Cadre le UNION comme « mêmes sources présentées différemment » et offre le focus
    par branche comme une suggestion OPTIONNELLE (champ ``target_path`` de ``TestSuggestion``),
    décidée par le LLM au vu du code et des résultats — jamais une suggestion forcée, une par
    branche, comme l'ancien track déterministe. ``""`` si pas de catalogue de paths."""
    plans = _parse_path_plans(state)
    names = [n for n in plans if n != ALL_PATH and isinstance(plans.get(n), dict)]
    if not names:
        return ""
    lines = []
    for name in names:
        plan = plans[name]
        cov = "déjà couverte" if name in covered else "non couverte"
        discriminant = _branch_discriminant_sql(
            (plan.get("sliced_sql") or "").strip(), dialect, limit=600
        )
        lines.append(f"- `{name}` ({cov}) :\n<sql>\n{discriminant}\n</sql>")
    example = names[0]
    return (
        "<branches_union_all>\n"
        "Ce SQL assemble plusieurs présentations des MÊMES sources via UNION ALL : le tronc "
        "de calcul est partagé (exécuter une branche traite déjà les autres). Le découpage en "
        "branches est un choix de PRÉSENTATION, pas des pipelines de données indépendants.\n"
        "Tu PEUX proposer une suggestion focalisée sur UNE branche — uniquement si isoler "
        "cette branche révèle un cas qu'un test sur l'assemblage complet masquerait — en "
        f"renseignant son `target_path` avec le nom machine exact (ex. `{example}`). Sinon "
        "laisse `target_path` vide. Ne propose JAMAIS un focus par branche « pour la forme ».\n"
        "Branches :\n" + "\n".join(lines) + "\n</branches_union_all>"
    )


# Prompt compact pour la génération d'UNE suggestion enchaînée immédiatement en test
# (boucle multi-tests). Réutilise les helpers de formatage (_format_test_block,
# _select_pitfalls, _format_profile_block) et le modèle structuré TestSuggestionsOutput.
_SINGLE_SUGGESTION_SYSTEM = (
    MOCKSQL_PRODUCT_PREAMBLE
    + """

Tu agis comme l'expert en tests unitaires SQL de MockSQL (dialecte: {dialect}). Tu identifies
LE cas de test le plus utile **non encore couvert** par les tests existants : celui où le
résultat est contre-intuitif, ambigu, ou où l'ingénieur pourrait se tromper sur ce que la
requête retourne réellement. Une seule suggestion, la plus pertinente."""
)


async def generate_single_suggestion(state: QueryState):
    """Boucle multi-tests : génère UNE suggestion (au lieu de 3 pour le panneau) et prépare
    immédiatement sa construction en test via le chemin clic-suggestion existant.

    Ne persiste rien dans le panneau et n'émet pas de message SUGGESTIONS : on pose
    ``input`` (le texte de la suggestion) + ``suggestion_intent`` pour que le
    ``conversational_agent`` produise un nouveau test, et on incrémente ``auto_tests_built``
    (le compteur que ``route_evaluator`` lit pour décider de continuer ou de clore via le
    ``suggestions_generator``). Fallback : si le LLM ne rend rien, on pose tout de même
    ``suggestion_intent`` avec une consigne générique — le garde-fou ``route_agent_output``
    garantit qu'un test sort quand même."""
    # Checkpoint incrémental : le test qui vient d'être réglé (dernier RESULTS) est
    # persisté AVANT de lancer la génération du suivant — une coupure pendant la suite
    # de la boucle ne fait alors plus perdre les tests déjà finis.
    from utils.saver import persist_completed_tests

    persisted = persist_completed_tests(state)
    if persisted:
        logger.diag(
            "[single_suggestion] checkpoint : %d test(s) persisté(s) avant le tour suivant",
            persisted,
        )

    built = (state.get("auto_tests_built") or 0) + 1
    base = {"auto_tests_built": built, "suggestion_intent": True}

    test_cases = await retrieve_existing_tests(state["session"], state)

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    dialect = state.get("dialect", "bigquery")
    profile = state.get("profile")
    used_columns = state.get("used_columns") or []
    profile_block = _format_profile_block(profile, used_columns) if profile else ""

    verdicts = _extract_verdicts(state)
    existing = "\n\n".join(
        f"Test {tc.get('test_index')} — {tc.get('test_name') or ''}\n"
        + _format_test_block(tc, verdicts.get(tc.get("test_index")))
        for tc in test_cases
    )
    existing_block = existing or "Aucun test existant pour le moment."
    pitfalls_block = _select_pitfalls(sql, dialect)
    # W2 — le SQL injecté est compacté (projections passthrough résumées) ; les détections
    # structurelles (pitfalls) tournent sur le SQL ORIGINAL pour ne rien perdre.
    sql_for_prompt = compact_passthrough_sql(sql, dialect)
    sql_digest = build_sql_digest(state.get("query_decomposed"))
    profile_section = (
        f"Profil statistique réel des données :\n{profile_block}"
        if profile_block
        else ""
    )

    prompt_template = ChatPromptTemplate.from_messages(
        [
            ("system", _SINGLE_SUGGESTION_SYSTEM),
            (
                "user",
                """Requête SQL à analyser :
<sql>
{sql}
</sql>

{sql_digest}

Tests déjà générés (à ne PAS reproduire — propose un cas distinct) :
<tests_existants>
{existing_block}
</tests_existants>

{profile_section}

Pièges classiques pertinents pour ce SQL :
{pitfalls_block}

Génère exactement 1 suggestion de cas de test non couvert, formulée en langage métier
(commence par un verbe, décris un symptôme métier observable — pas de fonction SQL ni de
détail d'implémentation). Renseigne aussi `analyse_des_manques` en une phrase.""",
            ),
        ]
    )

    suggestion_text = ""
    try:
        llm = make_llm()
        structured_llm = llm.with_structured_output(TestSuggestionsOutput)
        result = await (prompt_template | structured_llm).ainvoke(
            {
                "dialect": dialect,
                "sql": sql_for_prompt,
                "sql_digest": sql_digest,
                "existing_block": existing_block,
                "profile_section": profile_section,
                "pitfalls_block": pitfalls_block,
            }
        )
        items = result.suggestions or []
        if items:
            suggestion_text = (items[0].text or "").strip()
        logger.diag("[single_suggestion] tour %d → %r", built, suggestion_text[:120])
    except Exception as e:  # pragma: no cover — best-effort, fallback ci-dessous
        logger.warning("Erreur LLM lors de la génération d'une suggestion: %s", e)

    base["input"] = suggestion_text or (
        "Génère un nouveau test couvrant un cas limite non encore couvert par les tests "
        "existants (valeurs NULL, plage vide, ex æquo, ou format de sortie)."
    )
    return base


async def generate_suggestions(state: QueryState):
    """Génère des suggestions de cas de tests non encore couverts et les émet comme message SUGGESTIONS."""

    # Checkpoint incrémental : à la sortie de la boucle multi-tests, le dernier test réglé
    # n'est pas encore persisté (history_saver ne tourne qu'après). Une coupure pendant
    # cette génération de suggestions le perdrait — on le sauve d'abord (idempotent).
    from utils.saver import persist_completed_tests

    persist_completed_tests(state)

    # --- 1. Préparation des données ---
    test_cases = await retrieve_existing_tests(state["session"], state)
    if not test_cases:
        return {}

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    dialect = state.get("dialect", "bigquery")
    profile = state.get("profile")
    used_columns = state.get("used_columns") or []
    profile_block = _format_profile_block(profile, used_columns) if profile else ""

    raw_instructions = (state.get("agent_tool_args") or {}).get(
        "instructions", ""
    ) or ""
    if isinstance(raw_instructions, list):
        raw_instructions = " ".join(str(x) for x in raw_instructions if x)
    instructions = raw_instructions.strip()

    stored = get_test(state["session"]) or {}

    def _clean(items) -> list[str]:
        return [s.strip() for s in (items or []) if isinstance(s, str) and s.strip()]

    dismissed_suggestions = _clean(stored.get("dismissed_suggestions"))
    accepted_suggestions = _clean(stored.get("accepted_suggestions"))
    # « En attente » = encore dans le panneau, ni acceptée ni rejetée.
    _resolved = set(dismissed_suggestions) | set(accepted_suggestions)
    pending_suggestions = [
        s for s in _clean(stored.get("suggestions")) if s not in _resolved
    ]

    # Mode append (l'agent « rajoute des suggestions ») vs replace (bouton « Régénérer »
    # du panneau, ou agent avec replace=True). En append, les nouvelles s'ajoutent aux
    # existantes (plafond SUGGESTIONS_CAP) ; en replace, elles écrasent toute la liste.
    agent_args = state.get("agent_tool_args") or {}
    from_agent = state.get("agent_tool_call") == "generate_suggestions"
    replace_requested = bool(agent_args.get("replace")) or bool(
        state.get("regenerate_suggestions")
    )
    is_append = from_agent and not replace_requested

    # Plafond atteint : on n'ajoute pas en silence — on prévient l'utilisateur et on le
    # laisse décider (supprimer une suggestion du panneau, ou demander un remplacement).
    if is_append and len(pending_suggestions) >= SUGGESTIONS_CAP:
        logger.diag(
            "[suggestions] plafond atteint (%d ≥ %d) — pas d'ajout, message utilisateur",
            len(pending_suggestions),
            SUGGESTIONS_CAP,
        )
        cap_msg = (
            f"Tu as déjà {len(pending_suggestions)} suggestions, soit le maximum "
            f"({SUGGESTIONS_CAP}). Supprime-en une ou deux dans le panneau, ou demande-moi "
            "de les remplacer (« remplace les suggestions »), et je t'en proposerai de nouvelles."
        )
        return {
            "messages": [
                AIMessage(
                    content=cap_msg,
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.OTHER,
                        "parent": state.get("user_message_id")
                        or state.get("parent_message_id"),
                        "request_id": state.get("request_id"),
                    },
                )
            ]
        }

    verdicts = _extract_verdicts(state)
    test_blocks = []
    for tc in test_cases:
        idx = tc.get("test_index")
        name = tc.get("test_name") or f"test_{idx}"
        header = f"Test {idx} — {name}"
        body = _format_test_block(tc, verdicts.get(idx))
        test_blocks.append(f"{header}\n{body}")
    existing = "\n\n".join(test_blocks)

    # Formatage propre avec balises XML pour le prompt. NB : f-string — le contenu réel
    # des instructions doit être injecté ici. Un littéral `{}` resterait tel quel (la valeur
    # est passée comme kwarg à .format() qui ne ré-interpole pas les accolades du contenu) →
    # les consignes spécifiques de l'utilisateur seraient silencieusement perdues.
    instruction_block = (
        f"<instructions_specifiques>\n{instructions}\n</instructions_specifiques>"
        if instructions
        else ""
    )
    existing_tests_block = (
        existing if existing else "Aucun test existant pour le moment."
    )
    prior_sections = []
    if accepted_suggestions:
        prior_sections.append(
            "  Déjà transformées en test par l'ingénieur (couvertes — ne pas reproposer) :\n"
            + "\n".join(f"  - {s}" for s in accepted_suggestions)
        )
    if pending_suggestions:
        prior_sections.append(
            "  Actuellement en attente dans le panneau (déjà visibles — ne pas produire de doublon) :\n"
            + "\n".join(f"  - {s}" for s in pending_suggestions)
        )
    if dismissed_suggestions:
        prior_sections.append(
            "  Rejetées par l'ingénieur (ne pas reproposer, ni variante proche) :\n"
            + "\n".join(f"  - {s}" for s in dismissed_suggestions)
        )
    prior_suggestions_block = (
        "<suggestions_deja_proposees>\n"
        + "\n\n".join(prior_sections)
        + "\n</suggestions_deja_proposees>"
        if prior_sections
        else ""
    )

    # --- 2. Construction du Prompt ---
    # Quand le thinking natif est actif (flash/pro), le raisonnement se fait dans le
    # canal de réflexion → `analyse_des_manques` ne porte qu'une conclusion brève (cf.
    # schéma TestSuggestionsOutput). Demander un chain-of-thought *dans le JSON* le
    # contredirait. Sinon (flash-lite / thinking coupé), le CoT in-schema est le seul
    # raisonnement disponible : on le réclame explicitement.
    native_thinking = is_native_thinking_active()
    reasoning_directive = (
        "Mène ton analyse dans ton canal de réflexion : comprends d'abord ce que fait le "
        "SQL (quel algorithme, quel pattern métier), puis identifie les hypothèses implicites "
        "sur les données. Dans `analyse_des_manques`, ne reporte que la conclusion (3-4 phrases)."
        if native_thinking
        else "Raisonne en mode chain-of-thought directement dans `analyse_des_manques` (3-4 phrases "
        "max) : pars de ce que fait le SQL (quel algorithme, quel pattern métier), puis des "
        "hypothèses implicites sur les données, avant de justifier le choix des suggestions."
    )
    system_prompt = (
        MOCKSQL_PRODUCT_PREAMBLE
        + """

Ici, tu agis comme l'expert en assurance qualité et en tests unitaires SQL de MockSQL (dialecte: {dialect}). Tu proposes à l'utilisateur les cas de tests les plus utiles **non encore couverts**.
Ton objectif est d'identifier les cas de tests les plus utiles — ceux où le résultat est contre-intuitif, ambigu, ou où l'ingénieur pourrait se tromper sur ce que la requête retourne réellement.
"""
        + reasoning_directive
    )
    prompt_template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                system_prompt,
            ),
            (
                "user",
                """Voici la requête SQL à analyser :
<sql>
{sql}
</sql>

{sql_digest}

{branch_catalog_block}

{instruction_block}

Voici les tests déjà générés avec leurs données d'entrée, résultats d'exécution et verdicts (à ne pas reproduire) :
<tests_existants>
{existing_tests_block}
</tests_existants>

{prior_suggestions_block}

{profile_section}

Le bloc <suggestions_deja_proposees> liste les suggestions déjà traitées, regroupées par statut. Ne reproduis aucune d'elles, ni de variante proche : les **rejetées** ont été explicitement écartées par l'ingénieur, les **acceptées** sont déjà couvertes par un test existant, les **en attente** sont déjà affichées dans le panneau. Tes 3 nouvelles suggestions doivent être distinctes de toutes ces entrées.

{results_grounding}
Génère exactement 3 nouvelles suggestions de cas de tests non encore couverts.
Chaque suggestion doit être une assertion actionnable courte commençant par un verbe (ex : "Vérifie que...", "S'assure que...", "Teste le comportement...").

**Priorise les cas où le résultat attendu est incertain ou contre-intuitif.** Voici les pièges classiques pertinents pour les constructions détectées dans ce SQL — consulte-les et applique ceux qui s'appliquent réellement :

{pitfalls_block}

Pour ces patterns, formule la suggestion en décrivant le **symptôme métier observable** : qu'est-ce que l'utilisateur métier constaterait comme anomalie dans le rapport ou le résultat ? Évite le jargon **d'implémentation** — noms de fonctions SQL, d'opérateurs, de colonnes ou de CTE. En revanche, un terme **d'analyse indispensable** au sens (référence/baseline de calcul, écart-type, seuil, percentile, moyenne glissante) reste acceptable s'il est nécessaire pour décrire le symptôme : la prohibition vise le *comment technique*, pas le vocabulaire métier de l'analyse. L'ingénieur a besoin de comprendre *ce qui ne va pas dans les données*, pas *par quelle fonction SQL*.

Mauvais exemple (cite une fonction SQL, décrit la mécanique au lieu du symptôme métier) :
"Vérifie que les incidents de 2024 ne sont pas exclus silencieusement par l'EXTRACT lorsque la date est NULL."
Bon exemple (symptôme métier observable, chiffrable, sans jargon) :
"Vérifie que le total annuel d'incidents correspond bien à la somme des totaux mensuels — un écart signalerait des incidents rattachés à aucun mois, donc invisibles dans le rapport annuel."

Autre mauvais exemple (banal, vrai par construction, n'apprend rien) :
"Vérifie que la requête retourne des lignes quand il y a des données en entrée."
Autre bon exemple (cas limite où le résultat surprend l'ingénieur) :
"Vérifie qu'un client présent dans deux régions n'est compté qu'une fois dans le total national — sinon le total national dépasse la somme des effectifs réels."

Autre bon exemple (cas analytique — **une anomalie en masque une autre**, terme technique indispensable toléré) :
"Vérifie qu'une hausse brutale un mois n'empêche pas de détecter une hausse comparable le mois suivant : si la première gonfle la référence de calcul (moyenne et écart-type), la seconde repasse sous le seuil d'alerte et reste invisible dans le rapport d'anomalies."

{prod_instruction_block}""",
            ),
        ]
    )

    # --- 3. Exécution avec LangChain (Structured Output) ---
    llm = make_llm()
    structured_llm = llm.with_structured_output(TestSuggestionsOutput)
    chain = prompt_template | structured_llm

    profile_section = (
        f"Profil statistique réel des données (distributions mesurées en production) :\n{profile_block}"
        if profile_block
        else ""
    )
    pitfalls_block = _select_pitfalls(sql, dialect)
    # W2 — le SQL injecté est compacté (projections passthrough résumées) pour ne pas noyer
    # le prompt ; les détections structurelles (pitfalls) tournent sur le SQL ORIGINAL.
    sql_for_prompt = compact_passthrough_sql(sql, dialect)
    # Pré-digestion structurelle (pipeline de CTEs) injectée à côté du SQL brut — donne
    # au LLM la carte de la requête sans qu'il ait à la ré-inférer (cf. build_sql_digest).
    sql_digest = build_sql_digest(state.get("query_decomposed"))
    # Consigne [PROD] injectée seulement si un profil réel est disponible : sinon
    # le préfixe [PROD] serait un mensonge (aucune donnée de prod sous la main).
    prod_instruction_block = (
        "Un profil statistique réel des données est fourni ci-dessus. Au moins une de tes "
        "3 suggestions doit cibler un cas qui existe réellement dans ces données mesurées, "
        'et son champ `text` doit être préfixé par "[PROD] " (ex : "[PROD] Vérifie que..."). '
        "Pour CHAQUE suggestion [PROD], le champ `rationale` est obligatoire et non vide : une "
        "phrase en langage métier qui cite la preuve chiffrée tirée du profil (taux de valeurs "
        "vides, min/max, cardinalité, valeur observée) et explique pourquoi ce cas mérite un test — "
        "ex : \"Le profil indique que le champ 'code banque' est vide 3% du temps, un cas "
        "qu'aucun test existant ne couvre.\" Les suggestions non-[PROD] laissent `rationale` vide."
        if profile_block
        else "Aucun profil statistique réel n'est disponible : n'emploie jamais le préfixe \"[PROD]\" "
        "et laisse le champ `rationale` vide pour toutes les suggestions."
    )

    # #6 — Ne prétendre exploiter les résultats d'exécution que s'il existe au moins un
    # test ayant réellement retourné des lignes. Si tous les tests sont vides ou en
    # échec, il n'y a aucun résultat à analyser : on recentre la consigne sur la
    # structure du SQL pour éviter une suggestion « contextualisée » sur du vide.
    any_successful_result = any(
        (build_test_detail(tc).get("row_count") or 0) > 0 for tc in test_cases
    )
    results_grounding = (
        "En t'appuyant sur les données d'entrée ET les résultats d'exécution réels de chaque "
        "test, identifie les cas non couverts : combinaisons de valeurs absentes, comportements "
        "limites non testés, scénarios que les données actuelles ne permettent pas de valider."
        if any_successful_result
        else "Aucun test existant n'a encore produit de résultat exploitable (résultats vides ou "
        "en échec) : ne prétends pas t'appuyer sur des résultats d'exécution. Raisonne depuis la "
        "structure du SQL et les hypothèses implicites qu'il fait sur les données pour identifier "
        "les cas non couverts."
    )

    # Catalogue des branches UNION ALL : injecté dans le prompt pour que le suggesteur
    # contextuel puisse, S'IL le juge utile, proposer un test focalisé sur une branche
    # (via le champ `target_path`). Plus de track déterministe « une suggestion par
    # branche » : le focus est devenu une suggestion contextuelle parmi les autres.
    covered_paths = _covered_paths(state, test_cases)
    branch_catalog_block = _branch_catalog_block(state, covered_paths, dialect)

    # Échec LLM → panneau vide (pas de repli déterministe, cf. décision produit).
    suggestions: list[str] = []
    rationales: dict[str, str] = {}
    suggestion_paths: dict[str, str] = {}
    gap_analysis = ""
    try:
        try:
            _formatted = prompt_template.format_messages(
                dialect=dialect,
                sql=sql_for_prompt,
                sql_digest=sql_digest,
                branch_catalog_block=branch_catalog_block,
                instruction_block=instruction_block,
                existing_tests_block=existing_tests_block,
                prior_suggestions_block=prior_suggestions_block,
                profile_section=profile_section,
                pitfalls_block=pitfalls_block,
                prod_instruction_block=prod_instruction_block,
                results_grounding=results_grounding,
            )
            logger.diag(
                "[suggestions] PROMPT LLM — system (extrait):\n%s",
                _formatted[0].content[:500],
            )
            logger.diag(
                "[suggestions] PROMPT LLM — user (extrait):\n%s",
                _formatted[1].content[:2000],
            )
        except Exception:
            pass

        result = await chain.ainvoke(
            {
                "dialect": dialect,
                "sql": sql_for_prompt,
                "sql_digest": sql_digest,
                "branch_catalog_block": branch_catalog_block,
                "instruction_block": instruction_block,
                "existing_tests_block": existing_tests_block,
                "prior_suggestions_block": prior_suggestions_block,
                "profile_section": profile_section,
                "pitfalls_block": pitfalls_block,
                "prod_instruction_block": prod_instruction_block,
                "results_grounding": results_grounding,
            }
        )
        gap_analysis = (result.analyse_des_manques or "").strip()
        logger.diag(
            "[suggestions] analyse_des_manques:\n%s",
            gap_analysis[:1500],
        )
        items = result.suggestions[:3]
        # Aplatissement : on conserve `suggestions` en list[str] (dedup / consommation /
        # rejet restent indexés sur le texte) et on transporte les explications [PROD]
        # dans une side-map {texte → rationale} (cf. front : tag [PROD] cliquable).
        suggestions = [s.text.strip() for s in items if s.text and s.text.strip()]
        rationales = {
            s.text.strip(): s.rationale.strip()
            for s in items
            if s.text and s.text.strip() and s.rationale and s.rationale.strip()
        }
        # Focus par branche : le LLM peut marquer une suggestion comme focalisée sur une
        # branche du catalogue (champ `target_path`). On ne garde que les paths VALIDES
        # (présents dans path_plans) pour câbler ensuite le clic → focus (cf.
        # conversational_agent : remap déterministe texte-suggestion → nom machine).
        plan_names = set(_parse_path_plans(state).keys())
        suggestion_paths = {
            s.text.strip(): s.target_path.strip()
            for s in items
            if s.text
            and s.text.strip()
            and s.target_path
            and s.target_path.strip() in plan_names
        }
        logger.diag(
            "[suggestions] suggestions générées (%d):\n%s",
            len(suggestions),
            "\n".join(
                f"  [{i + 1}] {s}"
                + (f"  →focus[{suggestion_paths[s]}]" if s in suggestion_paths else "")
                + (f"  ⟪{rationales[s]}⟫" if s in rationales else "")
                for i, s in enumerate(suggestions)
            ),
        )

    except Exception as e:
        logger.warning("Erreur LLM lors de la génération des suggestions: %s", e)
        suggestions, rationales, suggestion_paths = [], {}, {}

    if not suggestions:
        return {}

    # Mode append : on fusionne les nouvelles (en tête = plus récentes) avec celles déjà
    # en attente, dédupliqué et plafonné. Les rationales [PROD] suivent les textes retenus.
    if is_append:
        merged = _merge_suggestions(suggestions, pending_suggestions)
        existing_rationales = stored.get("suggestion_rationales") or {}
        combined_rationales = {**existing_rationales, **rationales}
        rationales = {k: v for k, v in combined_rationales.items() if k in merged}
        # Idem pour les focus : on conserve les mappings des suggestions encore en attente.
        existing_paths = stored.get("suggestion_paths") or {}
        combined_paths = {**existing_paths, **suggestion_paths}
        suggestion_paths = {k: v for k, v in combined_paths.items() if k in merged}
        logger.diag(
            "[suggestions] append → %d nouvelle(s) + %d en attente = %d après fusion (plafond %d)",
            len(suggestions),
            len(pending_suggestions),
            len(merged),
            SUGGESTIONS_CAP,
        )
        suggestions = merged

    suggestion_paths = {k: v for k, v in suggestion_paths.items() if k in suggestions}

    # --- 3b. Persistance sur le modèle ---
    # Les suggestions sont un état du modèle (panneau dédié), pas un tour de chat :
    # on les stocke sur le fichier test. Le message SUGGESTIONS émis plus bas ne sert
    # qu'au rafraîchissement live du panneau via SSE et n'est PAS persisté dans
    # l'historique de conversation (cf. history_saver).
    update_test(
        state["session"],
        {
            "suggestions": suggestions,
            "suggestion_rationales": rationales,
            "suggestion_paths": suggestion_paths,
        },
    )

    # --- 4. Détermination du parent_id ---
    messages = state.get("messages", [])
    parent_id = state.get("user_message_id") or state.get("parent_message_id")

    # When triggered by an explicit user request via conversational_agent, keep
    # user_message_id as parent so the suggestion bubble attaches to the request.
    # In the normal post-evaluate flow (agent_tool_call is unset), attach to the
    # last EVALUATION or RESULTS message from the current run instead.
    if state.get("agent_tool_call") != "generate_suggestions":
        for m in reversed(messages):
            if get_message_type(m) == MsgType.EVALUATION:
                parent_id = m.id
                break
            if get_message_type(m) == MsgType.RESULTS:
                parent_id = m.id

    # --- 5. Retour au state LangGraph ---
    # `coverage_gap_analysis` est consommé par final_response (1ʳᵉ génération) pour tisser
    # l'analyse des manques dans le message de clôture et pointer vers le panneau.
    return {
        "coverage_gap_analysis": gap_analysis,
        "messages": [
            AIMessage(
                content=json.dumps(suggestions, ensure_ascii=False),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.SUGGESTIONS,
                    "parent": parent_id,
                    "request_id": state.get("request_id"),
                    "profile_available": bool(profile_block),
                    "rationales": rationales,
                },
            )
        ],
    }
