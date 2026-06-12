import json
import logging
import uuid
from pydantic import BaseModel, Field

from langchain_core.messages import AIMessage
from langchain_core.prompts import ChatPromptTemplate

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.examples_generator import retrieve_existing_tests
from build_query.prompt_tools import _format_profile_block
from build_query.state import QueryState
from storage.test_repository import get_test, update_test
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.saver import get_message_type
from utils.test_utils import build_test_detail

logger = logging.getLogger(__name__)


# 1. Structure Pydantic (avec Chain of Thought)
class TestSuggestionsOutput(BaseModel):
    analyse_des_manques: str = Field(
        description=(
            "Raisonnement en 3 à 4 phrases maximum. "
            "Identifie le pattern métier du SQL et les hypothèses implicites sur les données "
            "dont dépend son bon fonctionnement. "
            "Ce raisonnement guide le choix des 3 suggestions. "
            "Ne pas dépasser 4 phrases."
        )
    )
    suggestions: list[str] = Field(
        description=(
            "Liste exacte de 3 suggestions de cas de tests, chacune formulée en langage métier. "
            "Chaque suggestion commence par un verbe et décrit un comportement observable pour le domaine "
            "(ex : un total incohérent, des lignes manquantes, un classement incorrect). "
            "Ne jamais mentionner de fonctions SQL, d'opérateurs ou de détails d'implémentation "
            "(pas de EXTRACT, COUNT DISTINCT, LAG, NULL, JOIN, CTE, etc.) dans le texte final."
        ),
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

    stored = get_test(state["session"]) or {}
    dismissed_suggestions = [
        s.strip() for s in (stored.get("dismissed_suggestions") or []) if s.strip()
    ]

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
    dismissed_block = (
        "<suggestions_rejetees>\n"
        + "\n".join(f"- {s}" for s in dismissed_suggestions)
        + "\n</suggestions_rejetees>"
        if dismissed_suggestions
        else ""
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

{dismissed_block}

{profile_section}

Si des suggestions ont été jugées non pertinentes par l'ingénieur (bloc <suggestions_rejetees>), ne les régénère pas ni de variantes proches — elles ont été explicitement écartées.

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

Pour ces patterns, formule la suggestion en décrivant uniquement le **symptôme métier observable** : qu'est-ce que l'utilisateur métier constaterait comme anomalie dans le rapport ou le résultat ? Évite toute mention de fonctions SQL, d'opérateurs ou de détails d'implémentation — l'ingénieur a besoin de comprendre *ce qui ne va pas dans les données*, pas *pourquoi techniquement*.

Mauvais exemple : "Vérifie que les incidents de 2024 ne sont pas exclus silencieusement par l'EXTRACT lorsque la date est NULL."
Bon exemple : "Vérifie que le total annuel d'incidents correspond bien à la somme des totaux mensuels — un écart indiquerait des incidents invisibles dans le rapport annuel."

Si un profil statistique est fourni, au moins une suggestion doit cibler un cas qui existe réellement dans les données — formule-la ainsi : "[PROD] Vérifie que..." pour la distinguer des suggestions génériques. Cette suggestion doit aussi rester en langage métier.""",
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
        try:
            _formatted = prompt_template.format_messages(
                dialect=dialect,
                sql=sql,
                instruction_block=instruction_block,
                existing_tests_block=existing_tests_block,
                dismissed_block=dismissed_block,
                profile_section=profile_section,
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
                "sql": sql,
                "instruction_block": instruction_block,
                "existing_tests_block": existing_tests_block,
                "dismissed_block": dismissed_block,
                "profile_section": profile_section,
            }
        )
        logger.diag(
            "[suggestions] analyse_des_manques:\n%s",
            result.analyse_des_manques[:1500],
        )
        logger.diag(
            "[suggestions] suggestions générées (%d):\n%s",
            len(result.suggestions),
            "\n".join(f"  [{i + 1}] {s}" for i, s in enumerate(result.suggestions)),
        )
        suggestions = result.suggestions[:3]

    except Exception as e:
        print(f"Erreur LLM lors de la génération des suggestions: {e}")
        return {}

    if not suggestions:
        return {}

    # --- 3b. Persistance sur le modèle ---
    # Les suggestions sont un état du modèle (panneau dédié), pas un tour de chat :
    # on les stocke sur le fichier test. Le message SUGGESTIONS émis plus bas ne sert
    # qu'au rafraîchissement live du panneau via SSE et n'est PAS persisté dans
    # l'historique de conversation (cf. history_saver).
    update_test(state["session"], {"suggestions": suggestions})

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
