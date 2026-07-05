import hashlib
import json
import logging
import uuid
from typing import Literal

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE
from utils.saver import get_message_type
from utils.test_utils import find_current_test

logger = logging.getLogger(__name__)


def _format_input_for_judge(input_data) -> str:
    """Sérialise les données d'entrée pour le juge LLM **sans troncature**.

    Les données d'entrée (et la sortie) sont l'information la plus importante pour
    juger un test : les tronquer — a fortiori au milieu d'un objet JSON — faisait
    croire au juge que la donnée était malformée/incomplète. Il hallucinait alors un
    « jeu de données tronqué » comme cause d'un résultat vide, alors que DuckDB avait
    reçu les données complètes. On émet toujours le JSON complet et valide (les mocks
    produisent de petits volumes, aucun risque de budget de prompt).
    """
    try:
        return json.dumps(input_data, ensure_ascii=False, indent=2)
    except Exception:
        return str(input_data)


def _format_trace_for_intent_judge(failing_cte: str, cte_trace: dict) -> str:
    """Bloc de trace d'exécution pour le juge d'intention (« le vide est-il voulu ? »).

    Donne au juge OÙ les lignes disparaissent au lieu de lui faire simuler de tête une
    requête à 15 CTE (sinon il hallucine un « jeu de données tronqué/incomplet »). Trois
    informations : l'échelle de lignes par étape, la transition qui élimine les lignes,
    et la décomposition du prédicat bloquant.

    **Neutre par construction** : contrairement à `examples_generator._format_cte_trace_hint`
    (orienté générateur, qui se termine par « ajoute/patche les données »), ce bloc ne pousse
    vers aucune conclusion — le juge doit pouvoir conclure « vide voulu » comme « données mal
    construites » à partir du même signal.
    """
    if not cte_trace:
        return ""
    lines = [
        "Trace d'exécution (lignes produites par étape, dans l'ordre du flux de données) :"
    ]
    past_failing = False
    for name, info in cte_trace.items():
        if not isinstance(info, dict):
            continue
        rc = info.get("row_count", -1)
        if past_failing:
            # En aval de l'étape bloquante : 0 ligne propagé, replié en une ligne.
            lines.append(
                f"- `{name}` : {max(rc, 0)} ligne(s) (propagation du vide amont)"
            )
            continue
        if rc == -1:
            lines.append(f"- `{name}` : erreur d'exécution")
        else:
            marker = " ← première étape vide" if name == failing_cte else ""
            lines.append(f"- `{name}` : {rc} ligne(s){marker}")
        if name == failing_cte:
            past_failing = True

    # Transition bloquante (dernière étape > 0 → première à 0) + décomposition de prédicat.
    info = cte_trace.get(failing_cte) or {}
    detail: list[str] = []
    steps = info.get("steps") or []
    blocker_idx = next(
        (i for i, s in enumerate(steps) if s.get("count", -1) == 0), None
    )
    if blocker_idx is not None:
        prev = steps[blocker_idx - 1] if blocker_idx > 0 else None
        if prev:
            detail.append(f"- {prev.get('label', '?')} → {prev.get('count')} ligne(s)")
        blk = steps[blocker_idx]
        detail.append(
            f"- {blk.get('label', '?')} → 0 ligne(s) ← les lignes sont éliminées ici"
        )
    for bl in info.get("join_breakdown") or []:
        detail.append(f"- {bl}")
    if detail:
        lines.append("")
        lines.append(f"Là où les lignes disparaissent (`{failing_cte}`) :")
        lines.extend(detail)
    return "\n".join(lines)


def _empty_intent_fingerprint(sql, input_data, scenario, failing_cte) -> str:
    """Empreinte du verdict d'intention vide pour memoïsation.

    Le verdict ne dépend que de : le SQL, les données injectées, le texte du scénario et
    la CTE où le vide apparaît. Inchangés → on réutilise le verdict stocké sans rappeler
    le LLM. Le SQL est normalisé (whitespace) pour qu'un reformatage cosmétique ne casse
    pas le cache.
    """
    norm_sql = " ".join((sql or "").split())
    payload = json.dumps(
        {
            "sql": norm_sql,
            "data": input_data,
            "scenario": scenario or "",
            "failing_cte": failing_cte or "",
        },
        ensure_ascii=False,
        sort_keys=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class _ReevalResult(BaseModel):
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    explanation: str


async def _reevaluate_empty_result(
    state: QueryState, current_test: dict, last_results_msg: AIMessage
) -> dict:
    """LLM re-evaluation when the conversational agent suspects bad_data was a false positive."""
    test_desc = current_test.get("unit_test_description", "")
    input_data = current_test.get("data", {})
    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    reason = state.get("reevaluation_context", "")
    eval_test_index = current_test.get("test_index")

    input_summary = _format_input_for_judge(input_data)

    prompt = f"""SQL testé (dialecte {state.get("dialect", "bigquery")}) :
{sql}

Scénario du test : {test_desc}

Données d'entrée injectées dans DuckDB :
{input_summary}

Résultat DuckDB : 0 lignes retournées.

Justification de l'agent de diagnostic (pourquoi 0 lignes serait correct) :
{reason}

Évalue la qualité de ce test. Le fait que la requête retourne 0 lignes est-il cohérent avec le scénario décrit et les données fournies ?
- "Excellent" ou "Bon" si 0 lignes est bien le comportement attendu pour ce scénario.
- "Insuffisant" si les données d'entrée ne permettent pas de valider le scénario malgré la justification."""

    llm = make_llm().with_structured_output(_ReevalResult)
    try:
        logger.diag("[evaluator] PROMPT LLM (réévaluation):\n%s", prompt[:3000])
        result = await llm.ainvoke(
            [
                SystemMessage(
                    content=MOCKSQL_PRODUCT_PREAMBLE
                    + "\n\nTu réévalues ici la qualité d'un test (verdict argumenté) pour l'utilisateur."
                ),
                HumanMessage(content=prompt),
            ]
        )
        verdict = result.verdict
        explanation = result.explanation
    except Exception as exc:
        logger.warning("[evaluator] _reevaluate_empty_result failed: %s", exc)
        verdict = "Insuffisant"
        explanation = "Réévaluation impossible — erreur LLM."

    logger.diag(
        "[evaluator] réévaluation après request_reevaluation : verdict=%s — %s",
        verdict,
        explanation,
    )
    evaluation_feedback = "bad_data" if verdict == "Insuffisant" else None
    return {
        "messages": [
            AIMessage(
                content=f"**{verdict}** — {explanation}",
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.EVALUATION,
                    "parent": last_results_msg.id,
                    "request_id": state.get("request_id"),
                    "test_index": eval_test_index,
                },
            )
        ],
        "evaluation_feedback": evaluation_feedback,
        "status": "complete",
        "reevaluation_context": None,
    }


async def _classify_empty_intent(
    state: QueryState, current_test: dict, sql: str
) -> tuple[str, str]:
    """Verdict LLM : 0 ligne est-il le comportement *voulu* de ce test ?

    Appelé UNE SEULE FOIS, à la première occurrence du vide (cf. `empty_results_regen`
    falsy dans `evaluate_tests`). Décide si l'absence de résultat est intentionnelle
    (axe `empty`, branche d'UNION ALL volontairement vide, filtre qui exclut tout…)
    plutôt que de classer mécaniquement tout vide en `bad_data`.

    Retourne `(verdict, explanation)` :
    - verdict ∈ {"Excellent", "Bon"} → le vide est attendu (PASS).
    - verdict == "Insuffisant"       → le vide est inattendu ; `explanation` sert de
      message *utilisateur* clair (le diag structurel CTE reste interne au générateur).

    En cas d'échec LLM, retombe sur ("Insuffisant", "") → comportement déterministe
    historique (la boucle de régénération prend le relais).
    """
    test_desc = current_test.get("unit_test_description", "")
    input_data = current_test.get("data", {})
    input_summary = _format_input_for_judge(input_data)
    cte_trace = current_test.get("cte_trace") or {}
    failing_cte = current_test.get("failing_cte") or ""

    # Cache de verdict : SQL + données + scénario + CTE bloquante inchangés → on réutilise
    # le verdict déjà jugé sans rappeler le LLM (cf. _empty_intent_fingerprint).
    fingerprint = _empty_intent_fingerprint(sql, input_data, test_desc, failing_cte)
    cached = current_test.get("empty_intent_cache") or {}
    if cached.get("fingerprint") == fingerprint and cached.get("verdict"):
        logger.diag(
            "[evaluator] intent vide: verdict en cache réutilisé (%s)",
            cached["verdict"],
        )
        return cached["verdict"], cached.get("explanation", "")

    trace_block = _format_trace_for_intent_judge(failing_cte, cte_trace)
    trace_section = f"\n{trace_block}\n" if trace_block else ""

    prompt = f"""SQL testé (dialecte {state.get("dialect", "bigquery")}) :
{sql}

Scénario du test : {test_desc}

Données d'entrée injectées dans DuckDB :
{input_summary}

Résultat DuckDB : 0 ligne retournée.
{trace_section}
Le fait que la requête retourne 0 ligne est-il le comportement ATTENDU pour ce scénario ?
- "Excellent" ou "Bon" si 0 ligne est précisément ce que le scénario veut démontrer
  (filtre qui exclut tout, plage vide, branche d'UNION ALL non concernée, anti-jointure…).
- "Insuffisant" si le scénario suppose des lignes en sortie et que leur absence trahit
  des données d'entrée mal construites.

Appuie-toi sur la trace ci-dessus : l'étape où les lignes disparaissent indique si le vide
est voulu (un filtre/anti-join/plage vide précisément ciblé par le scénario) ou s'il trahit
des données mal construites en amont. Attention aux comparaisons sur NULL (`<>`, `=`, `IN`)
qui éliminent silencieusement une ligne — c'est un vide non voulu, pas un cas limite.

Rédige `explanation` pour un utilisateur (data engineer) en une phrase claire, SANS jargon
de CTE interne : dis pourquoi le vide est correct, ou ce qui manque dans les données pour
que le scénario produise des lignes."""

    llm = make_llm().with_structured_output(_ReevalResult)
    try:
        logger.diag("[evaluator] PROMPT LLM (intent vide):\n%s", prompt[:3000])
        result = await llm.ainvoke(
            [
                SystemMessage(
                    content=MOCKSQL_PRODUCT_PREAMBLE
                    + "\n\nTu évalues ici si l'absence de résultat est le comportement voulu du test."
                ),
                HumanMessage(content=prompt),
            ]
        )
        return result.verdict, result.explanation
    except Exception as exc:
        logger.warning("[evaluator] _classify_empty_intent failed: %s", exc)
        return "Insuffisant", ""


async def evaluate_tests(state: QueryState):
    """
    Lit le verdict pré-calculé par l'executor (embedded dans le résultat du test),
    émet le message EVALUATION et gère le routing (bad_data / bad_assertions / too_many_rows).

    Le verdict et les assertions sont produits en un seul appel LLM dans _generate_assertions_and_evaluate
    (examples_executor.py). Ce nœud ne fait plus d'appel LLM.
    """
    if state.get("error"):
        return {}

    results_msgs = [
        m for m in state.get("messages", []) if get_message_type(m) == MsgType.RESULTS
    ]
    if not results_msgs:
        return {}

    last_results = results_msgs[-1]
    try:
        all_tests = json.loads(last_results.content)
    except Exception:
        return {}

    if not isinstance(all_tests, list):
        all_tests = [all_tests]

    sql = (state.get("optimized_sql") or state.get("query", "")).strip()
    current_test = find_current_test(all_tests, state.get("test_index"))
    if current_test is None:
        return {}

    # Re-evaluation requested by conversational_agent: skip automatic bad_data classification.
    if current_test.get("status") == "empty_results" and state.get(
        "reevaluation_context"
    ):
        return await _reevaluate_empty_result(state, current_test, last_results)

    # DuckDB data error (Invalid Input Error, Conversion Error): wrong format/type in generated data.
    if current_test.get("status") == "bad_data_error":
        exec_error = current_test.get("exec_error", "")
        display_reason = (
            "Les données générées contiennent des valeurs au mauvais format."
        )
        diag = f"Erreur DuckDB lors de l'exécution :\n{exec_error}"
        gen_retries = (
            state.get("gen_retries") if state.get("gen_retries") is not None else 3
        )
        if gen_retries == 0:
            stub_test = dict(current_test)
            for table_name in stub_test.get("data", {}):
                stub_test["data"][table_name] = []
            stub_test["tags"] = list(
                set(
                    stub_test.get("tags", [])
                    + ["FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED"]
                )
            )
            return {
                "examples": [
                    AIMessage(
                        content=json.dumps(stub_test),
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EXAMPLES,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "evaluation_feedback": "bad_data",
                "status": "complete",
            }
        return {
            "messages": [
                AIMessage(
                    content=f"**Insuffisant** — {display_reason}",
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": current_test.get("test_index"),
                        "diag": diag,
                        "intermediate": True,
                    },
                )
            ],
            "evaluation_feedback": "bad_data",
            "status": "empty_results",
        }

    # Fast path: empty_results due to structural SQL constraint (no LLM needed).
    if current_test.get("status") == "empty_results":
        from build_query.constraint_simplifier import (
            check_correlated_aggregate_cardinality,
            check_having_cardinality,
        )

        dialect = state.get("dialect", "bigquery")
        cardinality_error: str | None = None
        for _check in (
            check_having_cardinality,
            check_correlated_aggregate_cardinality,
        ):
            try:
                _check(sql, dialect)
            except ValueError as exc:
                cardinality_error = str(exc)
                break

        if cardinality_error:
            logger.diag("[evaluator] too_many_rows détecté: %s", cardinality_error)
            return {
                "messages": [
                    AIMessage(
                        content=f"**Insuffisant** — {cardinality_error}",
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EVALUATION,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                            "test_index": current_test.get("test_index"),
                        },
                    )
                ],
                "evaluation_feedback": "too_many_rows",
                "status": "complete",
            }

        gen_retries = (
            state.get("gen_retries") if state.get("gen_retries") is not None else 3
        )

        cte_trace = current_test.get("cte_trace", {})
        failing_cte = current_test.get("failing_cte", "")
        if cte_trace:
            # Source unique de format de trace (compressée, centrée sur l'étape
            # bloquante) — partagée avec le feedback du générateur via _build_eval_messages.
            from build_query.examples_generator import _format_cte_trace_hint

            diag = _format_cte_trace_hint(failing_cte, cte_trace)
            structural_reason = (
                f"La CTE `{failing_cte}` est vide — les données ne satisfont pas ses contraintes."
                if failing_cte
                else "Les données d'entrée ne produisent aucun résultat."
            )
        elif failing_cte:
            diag = f"La requête retourne 0 ligne — la CTE `{failing_cte}` est vide. Les données d'entrée ne satisfont pas les contraintes de jointure ou de filtre."
            structural_reason = f"La CTE `{failing_cte}` est vide — les données ne satisfont pas ses contraintes."
        else:
            diag = "La requête retourne 0 ligne. Les données d'entrée ne produisent aucun résultat."
            structural_reason = "Les données d'entrée ne produisent aucun résultat."

        # Garde d'intention LLM — une seule fois, à la 1ʳᵉ occurrence du vide
        # (`empty_results_regen` falsy). Sur les retries de régénération on reste sur
        # la boucle déterministe SANS rappeler le LLM (compromis hybride : verdict LLM
        # une fois, correction déterministe ensuite). Le diag structurel `diag` reste
        # interne au générateur ; le message *utilisateur* porte l'explication LLM.
        display_reason = structural_reason
        if not state.get("empty_results_regen"):
            verdict, explanation = await _classify_empty_intent(
                state, current_test, sql
            )
            if verdict in ("Excellent", "Bon"):
                logger.diag(
                    "[evaluator] empty_results jugé INTENTIONNEL par le LLM (%s) → PASS",
                    verdict,
                )
                empty_assertion = {
                    "description": "La requête doit retourner 0 ligne (table vide intentionnelle)",
                    "sql": "SELECT * FROM __result__",
                    "passed": True,
                }
                updated_test = {
                    **current_test,
                    "assertion_results": [empty_assertion],
                    "verdict": verdict,
                    "evaluation_explanation": explanation,
                    # Memoïse le verdict : prochain run inchangé → pas de rappel LLM.
                    "empty_intent_cache": {
                        "fingerprint": _empty_intent_fingerprint(
                            sql,
                            current_test.get("data", {}),
                            current_test.get("unit_test_description", ""),
                            failing_cte,
                        ),
                        "verdict": verdict,
                        "explanation": explanation,
                    },
                }
                updated_all_tests = [
                    updated_test
                    if t.get("test_index") == current_test.get("test_index")
                    else t
                    for t in all_tests
                ]
                parent = last_results.additional_kwargs.get("parent") or state.get(
                    "parent_message_id"
                )
                sql_kw = state.get("query", "").strip()
                optimized_kw = state.get("optimized_sql", "").strip()
                new_results_id = str(uuid.uuid4())
                eval_msg_id = str(uuid.uuid4())
                return {
                    "messages": [
                        AIMessage(
                            content=json.dumps(
                                updated_all_tests,
                                ensure_ascii=False,
                                indent=2,
                                default=str,
                            ),
                            id=new_results_id,
                            additional_kwargs={
                                **last_results.additional_kwargs,
                                "type": MsgType.RESULTS,
                                "parent": parent,
                                "request_id": state.get("request_id"),
                                **({"sql": sql_kw} if sql_kw else {}),
                                **(
                                    {"optimized_sql": optimized_kw}
                                    if optimized_kw
                                    else {}
                                ),
                            },
                        ),
                        AIMessage(
                            content=f"**{verdict}** — {explanation}",
                            id=eval_msg_id,
                            additional_kwargs={
                                "type": MsgType.EVALUATION,
                                "parent": new_results_id,
                                "request_id": state.get("request_id"),
                                "test_index": current_test.get("test_index"),
                            },
                        ),
                    ],
                    "evaluation_feedback": None,
                    "status": "complete",
                }
            if explanation:
                display_reason = explanation

        logger.diag(
            "[evaluator] empty_results → bad_data, retries=%d",
            gen_retries,
        )

        if gen_retries == 0:
            logger.warning(
                "[evaluator] Circuit breaker déclenché pour le test %s",
                current_test.get("test_index"),
            )
            stub_test = dict(current_test)
            for table_name in stub_test.get("data", {}):
                stub_test["data"][table_name] = []
            stub_test["tags"] = list(
                set(
                    stub_test.get("tags", [])
                    + ["FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED"]
                )
            )

            return {
                "examples": [
                    AIMessage(
                        content=json.dumps(stub_test),
                        id=str(uuid.uuid4()),
                        additional_kwargs={
                            "type": MsgType.EXAMPLES,
                            "parent": last_results.id,
                            "request_id": state.get("request_id"),
                        },
                    )
                ],
                "evaluation_feedback": "bad_data",
                "status": "complete",
            }

        state_update: dict = {
            "messages": [
                AIMessage(
                    content=f"**Insuffisant** — {display_reason}",
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": current_test.get("test_index"),
                        "diag": diag,
                        "intermediate": True,
                    },
                )
            ],
            "evaluation_feedback": "bad_data",
            "status": "empty_results",
            # Route straight to the generator for a holistic regeneration targeting
            # the failing CTE (cte_trace travels in the RESULTS message). Bypasses the
            # conversational_agent, whose single-field patches cannot fix a 0-row query.
            # Decrement gen_retries here since we skip the agent (which normally does it).
            "empty_results_regen": True,
            "gen_retries": gen_retries - 1,
        }
        return state_update

    verdict = current_test.get("verdict")
    reason_type = current_test.get("reason_type")
    explanation = current_test.get("evaluation_explanation", "")

    if not verdict:
        return {}

    logger.diag(
        "[evaluator] verdict=%s reason_type=%s — %s", verdict, reason_type, explanation
    )

    eval_test_index = current_test.get("test_index")

    # Désync description↔données d'ENTRÉE AVEC prémisse utilisateur explicite : la cible de
    # correction est CONNUE (la prémisse), donc on tente d'abord une correction automatique —
    # aligner les données injectées SUR la prémisse — via la boucle bad_data → conversational_agent
    # (dont le premise_guard protège déjà la prémisse d'un écrasement muet). On ne réécrit JAMAIS
    # la prémisse : la boucle ramène les DONNÉES vers elle. On ne décrémente PAS gen_retries ici —
    # c'est bad_data_to_agent/l'agent qui le fait (cf. query_chain._bad_data_to_agent). Le
    # diagnostic synthétique (kind="premise_desync") porte les 6 clés lues par
    # _build_agent_eval_context et déclenche le trigger dédié côté agent. À retries épuisés (ou
    # sans prémisse) on retombe sur le VALIDATION_PROMPT ci-dessous — jamais pire qu'avant.
    _gen_retries_lookahead = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 1
    )
    if (
        reason_type == "bad_input_description"
        and current_test.get("user_premise")
        and _gen_retries_lookahead > 0
    ):
        premise = current_test["user_premise"]
        current_test["diagnostic"] = {
            "root_cause": (
                "Les données d'entrée injectées ne respectent pas la prémisse "
                "explicitement énoncée par l'utilisateur pour ce test."
            ),
            "sql_pattern": "premise_desync",
            "data_issue": f"{explanation} Prémisse à respecter : « {premise} ».",
            "fix_recipe": (
                f"Aligner les données d'entrée injectées sur la prémisse « {premise} » "
                f"— corriger les VALEURS des lignes, ni la prémisse ni la description."
            ),
            "affected_tables": list((current_test.get("data") or {}).keys()),
            "affected_ctes": [],
            "kind": "premise_desync",
        }
        # Bascule vers la queue générique bad_data (plus bas) : reason_type local devient
        # "bad_data" → l'if VALIDATION_PROMPT ci-dessous est court-circuité, et evaluation_feedback
        # sera calculé à "bad_data" (verdict reste "Insuffisant"). Le diagnostic posé ci-dessus
        # sera attaché à l'EVALUATION + émis en BAD_DATA_DIAGNOSTIC.
        logger.diag(
            "[evaluator] bad_input_description + user_premise (retries=%s) → boucle bad_data (premise_desync) test=%s",
            _gen_retries_lookahead,
            eval_test_index,
        )
        reason_type = "bad_data"

    # Désync description↔réel (données valides) : on NE boucle PAS. On sauve l'état, on émet
    # le verdict puis un VALIDATION_PROMPT actionnable (Valider / Corriger côté UI). Trois causes :
    #   needs_validation → écart de CARDINALITÉ (nb de lignes annoncé ≠ réel)
    #   bad_description  → écart de VALEUR concrète (la description ment sur une valeur de sortie)
    #   bad_input_description (sans prémisse, ou retries épuisés) → écart description↔entrées
    # Dans les trois cas, l'évaluateur a proposé une `corrected_description` qu'accept_validation
    # appliquera au clic. Cf. assertion_generator (détection) et accept_validation (application).
    if reason_type in ("needs_validation", "bad_description", "bad_input_description"):
        actual_rows = 0
        try:
            actual_rows = len(json.loads(current_test.get("results_json") or "[]"))
        except Exception:
            actual_rows = 0
        expected_rows = current_test.get("expected_row_count")
        if (
            reason_type == "needs_validation"
            and expected_rows is not None
            and actual_rows
        ):
            question = (
                f"Le résultat produit {actual_rows} ligne(s) alors que ce scénario en "
                f"suppose {expected_rows}. Valides-tu ce résultat tel quel (la description "
                f"sera réalignée), ou faut-il corriger le test ?"
            )
        elif reason_type == "needs_validation":
            question = (
                "Le résultat ne correspond pas à la cardinalité supposée par la description. "
                "Valides-tu ce résultat tel quel, ou faut-il corriger le test ?"
            )
        elif reason_type == "bad_input_description":
            # Desync description ↔ données d'ENTRÉE injectées (TICKET-2). On ne réécrit
            # JAMAIS le narratif en silence : si le test porte une prémisse utilisateur
            # (TICKET-1), la question pointe l'attente énoncée plutôt que de proposer un
            # simple réalignement cosmétique.
            if current_test.get("user_premise"):
                question = (
                    "Les données injectées ne correspondent pas à la prémisse que tu as "
                    "énoncée pour ce test. Valides-tu les données réelles (la description "
                    "sera réalignée), ou faut-il corriger les données pour respecter ta "
                    "prémisse ?"
                )
            else:
                question = (
                    "La description annonce des valeurs d'entrée qui ne correspondent pas "
                    "aux données réellement injectées. Valides-tu les données telles quelles "
                    "(la description sera réalignée), ou faut-il corriger le test ?"
                )
        else:  # bad_description
            question = (
                "La description annonce une valeur que le calcul ne produit pas. Valides-tu la "
                "sortie réelle tel quel (la description sera réalignée), ou faut-il corriger le test ?"
            )
        eval_msg_id = str(uuid.uuid4())
        logger.diag(
            "[evaluator] %s test=%s attendu=%s réel=%s",
            reason_type,
            eval_test_index,
            expected_rows,
            actual_rows,
        )
        return {
            "messages": [
                AIMessage(
                    content=f"**{verdict}** — {explanation}",
                    id=eval_msg_id,
                    additional_kwargs={
                        "type": MsgType.EVALUATION,
                        "parent": last_results.id,
                        "request_id": state.get("request_id"),
                        "test_index": eval_test_index,
                    },
                ),
                AIMessage(
                    content=question,
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.VALIDATION_PROMPT,
                        "parent": eval_msg_id,
                        "request_id": state.get("request_id"),
                        "test_index": eval_test_index,
                        "reason_type": reason_type,
                        "expected_row_count": expected_rows,
                        "actual_row_count": actual_rows,
                    },
                ),
            ],
            "evaluation_feedback": reason_type,
            "status": "complete",
            "empty_results_regen": False,
        }

    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 1
    )
    debug_retries = (
        state.get("debug_retries") if state.get("debug_retries") is not None else 2
    )

    evaluation_feedback = (
        reason_type if verdict == "Insuffisant" and reason_type else None
    )
    triggers_agent_retry = evaluation_feedback == "bad_data" and not state.get(
        "assertion_only"
    )
    triggers_assertion_retry = evaluation_feedback == "bad_assertions"

    new_status = "complete"
    if triggers_agent_retry and gen_retries > 0:
        new_status = "empty_results"
    elif triggers_assertion_retry and debug_retries > 0:
        new_status = "bad_assertions"

    diagnostic = current_test.get("diagnostic")
    eval_msg_id = str(uuid.uuid4())
    eval_msg_kwargs: dict = {
        "type": MsgType.EVALUATION,
        "parent": last_results.id,
        "request_id": state.get("request_id"),
        "test_index": eval_test_index,
    }
    if diagnostic and evaluation_feedback == "bad_data":
        eval_msg_kwargs["diagnostic"] = diagnostic

    messages: list = [
        AIMessage(
            content=f"**{verdict}** — {explanation}",
            id=eval_msg_id,
            additional_kwargs=eval_msg_kwargs,
        )
    ]

    if diagnostic and evaluation_feedback == "bad_data":
        messages.append(
            AIMessage(
                content=json.dumps(diagnostic),
                id=str(uuid.uuid4()),
                additional_kwargs={
                    "type": MsgType.BAD_DATA_DIAGNOSTIC,
                    "parent": eval_msg_id,
                    "test_index": eval_test_index,
                    "request_id": state.get("request_id"),
                },
            )
        )

    state_update: dict = {
        "messages": messages,
        "evaluation_feedback": evaluation_feedback,
        "status": new_status,
        "empty_results_regen": False,
    }

    if triggers_assertion_retry:
        state_update["debug_retries"] = debug_retries - 1

    return state_update
