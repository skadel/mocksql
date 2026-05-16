import json
import uuid
from pydantic import BaseModel, Field

from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

from build_query.examples_generator import retrieve_existing_tests
from build_query.prompt_tools import _format_profile_block
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import build_test_detail


# 1. Structure Pydantic (avec Chain of Thought)
class TestSuggestionsOutput(BaseModel):
    analyse_des_manques: str = Field(
        description=(
            "Raisonnement en deux étapes. "
            "1) Identifie l'algorithme ou le pattern métier implémenté par le SQL "
            "(détection d'anomalies, classement, agrégation multi-niveaux, cohorte, etc.). "
            "2) Liste les hypothèses implicites que cet algorithme fait sur les données "
            "et les cas concrets où ces hypothèses peuvent être violées. "
            "Ce raisonnement guide le choix des 3 suggestions."
        )
    )
    suggestions: list[str] = Field(
        description="Liste exacte de 3 suggestions de cas de tests d'une phrase commençant par un verbe.",
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


async def generate_suggestions(state: QueryState):
    """Génère des suggestions de cas de tests non encore couverts et les émet comme message SUGGESTIONS."""

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

    verdicts = _extract_verdicts(state)
    test_blocks = []
    for tc in test_cases:
        idx = tc.get("test_index")
        name = tc.get("test_name") or f"test_{idx}"
        header = f"Test {idx} — {name}"
        body = _format_test_block(tc, verdicts.get(idx))
        test_blocks.append(f"{header}\n{body}")
    existing = "\n\n".join(test_blocks)

    # Formatage propre avec balises XML pour le prompt
    instruction_block = (
        "<instructions_specifiques>\n{}\n</instructions_specifiques>"
        if instructions
        else ""
    )
    existing_tests_block = (
        existing if existing else "Aucun test existant pour le moment."
    )

    # --- 2. Construction du Prompt ---
    prompt_template = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                """Tu es un expert en assurance qualité et en tests unitaires SQL (dialecte: {dialect}).
Ton objectif est d'identifier les cas de tests les plus utiles — ceux où le résultat est contre-intuitif, ambigu, ou où l'ingénieur pourrait se tromper sur ce que la requête retourne réellement.
Raisonne en mode chain-of-thought : commence par comprendre ce que fait le SQL (quel algorithme, quel pattern métier), puis identifie les hypothèses implicites sur les données, avant de proposer les suggestions.""",
            ),
            (
                "user",
                """Voici la requête SQL à analyser :
<sql>
{sql}
</sql>

{instruction_block}

Voici les tests déjà générés avec leurs données d'entrée, résultats d'exécution et verdicts (à ne pas reproduire) :
<tests_existants>
{existing_tests_block}
</tests_existants>

{profile_section}

En t'appuyant sur les données d'entrée et les résultats de chaque test, identifie les cas non couverts : combinaisons de valeurs absentes, comportements limites non testés, scénarios que les données actuelles ne permettent pas de valider.
Génère exactement 3 nouvelles suggestions de cas de tests non encore couverts.
Chaque suggestion doit être une assertion actionnable courte commençant par un verbe (ex : "Vérifie que...", "S'assure que...", "Teste le comportement...").

**Priorise les cas où le résultat attendu est incertain ou contre-intuitif.** Voici un catalogue de pièges classiques — consulte-le et applique ceux qui sont pertinents pour ce SQL :

Agrégats contre-intuitifs :
- COUNT DISTINCT non-additif : sum(count_distinct par sous-groupe) ≠ count_distinct global — un même élément peut apparaître dans plusieurs groupes
- Ratio d'agrégats : sum(ratio) ≠ sum(numérateur) / sum(dénominateur) — le ratio ne peut pas être ré-agrégé
- NULL exclus silencieusement : COUNT(col) ≠ COUNT(*) quand col contient des NULLs ; SUM/AVG ignorent aussi les NULLs
- Dénominateur nul : si le dénominateur d'un ratio peut être 0, la requête explose ou retourne NULL sans warning
- Agrégation multi-niveaux : une métrique calculée à granularité fine puis ré-agrégée peut différer du calcul direct au niveau grossier

Fonctions fenêtre (LAG, LEAD, RANK, etc.) :
- LAG/LEAD retournent NULL sur la première/dernière ligne de la partition — que fait la logique en aval avec ce NULL ?
- ROW_NUMBER sur ex æquo : non-déterministe sans colonne de départage unique
- RANK vs DENSE_RANK : RANK saute des numéros après un ex æquo (1,1,3), DENSE_RANK non (1,1,2) — lequel est attendu ?
- LAST_VALUE piège : la frame par défaut est ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, pas toute la partition — LAST_VALUE retourne souvent la valeur courante, pas la dernière de la partition
- Fenêtre glissante en début de série : les N premières lignes ont une fenêtre plus petite que N → moyenne/variance calculée sur moins de points, ce qui peut générer de faux positifs ou faux négatifs
- Cumul avec ORDER BY et doublons : si deux lignes ont la même valeur de tri, leur ordre relatif est aléatoire et le cumul est non-déterministe

Algorithmes statistiques (z-score, anomalies, seuils dynamiques) :
- Contamination du baseline : si une anomalie fait partie de la fenêtre de calcul de la moyenne/variance, elle tire le seuil vers le haut — exemple : stable pendant 11 mois, hausse en M+12, hausse similaire en M+13 → M+12 gonfle la variance et M+13 n'est plus détecté comme anomalie
- Dérive progressive masquée : une série d'anomalies successives peut décaler le baseline progressivement sans qu'aucune ne dépasse le seuil individuellement
- Fenêtre trop courte : en début de série, la variance est calculée sur peu de points, le z-score est instable et peut déclencher de faux positifs

JOINs :
- Fan-out silencieux : clé de jointure non-unique → multiplication des lignes avant agrégation, les SUM/COUNT sont gonflés sans erreur
- Comptage d'entités via JOIN sur table de faits : si le SQL compte des entités distinctes (clients, points de vente, commandes) en les joingnant à une table où elles apparaissent plusieurs fois (contrats, transactions, événements), chaque entité est comptée N fois sauf si un DISTINCT ou une dédoublication explicite est en place — c'est l'un des bugs les plus fréquents en BI, souvent invisible car le résultat reste plausible (ex. +5%)
- NULL dans la clé de jointure : un NULL ne matche jamais un autre NULL en SQL → lignes silencieusement perdues avec INNER JOIN

Pour ces patterns, formule la suggestion en rendant explicite ce que l'utilisateur pourrait croire à tort — par exemple : "Vérifie que le total global de [métrique] correspond à la somme des valeurs par [dimension] — ce qui n'est pas garanti avec COUNT DISTINCT."

Si un profil statistique est fourni, au moins une suggestion doit cibler un cas qui existe réellement dans les données — formule-la ainsi : "[PROD] Vérifie que..." pour la distinguer des suggestions génériques.""",
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

    try:
        result = await chain.ainvoke(
            {
                "dialect": dialect,
                "sql": sql,
                "instruction_block": instruction_block,
                "existing_tests_block": existing_tests_block,
                "profile_section": profile_section,
            }
        )
        suggestions = result.suggestions[:3]

    except Exception as e:
        print(f"Erreur LLM lors de la génération des suggestions: {e}")
        return {}

    if not suggestions:
        return {}

    # --- 4. Détermination du parent_id ---
    messages = state.get("messages", [])
    parent_id = state.get("parent_message_id") or state.get("user_message_id")
    for m in reversed(messages):
        if get_message_type(m) == MsgType.EVALUATION:
            parent_id = m.id
            break
        if get_message_type(m) == MsgType.RESULTS:
            parent_id = m.id

    # --- 5. Retour au state LangGraph ---
    return {
        "messages": [
            AIMessage(
                content=json.dumps(suggestions, ensure_ascii=False),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.SUGGESTIONS,
                    "parent": parent_id,
                    "request_id": state.get("request_id"),
                    "profile_available": bool(profile_block),
                },
            )
        ]
    }
