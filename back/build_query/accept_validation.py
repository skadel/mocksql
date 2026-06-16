"""Nœud d'acceptation d'un test ``needs_validation``.

Déclenché quand l'utilisateur clique « Je valide l'état actuel » sur un test dont la
description supposait une cardinalité différente de la sortie réelle (cf.
``test_evaluator`` → ``VALIDATION_PROMPT``). L'utilisateur tranche l'ambiguïté en
faveur du réel : on réaligne la description sur la sortie observée (1 appel LLM) et on
flippe le verdict à « Bon ». Les données et les assertions (au niveau ligne) sont
conservées telles quelles — seul le narratif mentait sur le nombre de lignes.

Déterministe (pas l'agent conversationnel) : l'utilisateur a cliqué un bouton, il
attend une action, pas une réponse libre.
"""

import json
import logging
import uuid

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pydantic import BaseModel

import utils.logger  # noqa: F401 — registers DIAG level (15)
from build_query.state import QueryState
from storage.test_repository import get_test, update_test
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import MOCKSQL_PRODUCT_PREAMBLE

logger = logging.getLogger(__name__)


class _RealignedDescription(BaseModel):
    unit_test_description: str
    test_name: str


async def _realign_description(
    test_case: dict, actual_rows: int
) -> _RealignedDescription:
    """Réécrit la description pour qu'elle reflète fidèlement la sortie réelle."""
    old_desc = test_case.get("unit_test_description", "")
    old_name = test_case.get("test_name", "")
    try:
        sample = json.loads(test_case.get("results_json") or "[]")
    except Exception:
        sample = []

    prompt = f"""Description actuelle du test : {old_desc}
Titre actuel : {old_name}

Sortie réelle de la requête : {actual_rows} ligne(s).
Sortie réelle complète : {json.dumps(sample, ensure_ascii=False, default=str)}

L'utilisateur a VALIDÉ que cette sortie est le comportement attendu. La description
ci-dessus suppose un nombre de lignes différent — réécris-la pour qu'elle décrive
fidèlement la sortie réelle (notamment sa cardinalité), sans inventer d'autres faits.
Garde le même scénario métier, ajuste uniquement ce qui contredit le réel.
Réponds avec une description concise et un titre court (3–6 mots)."""

    llm = make_llm().with_structured_output(_RealignedDescription)
    try:
        result = await llm.ainvoke(
            [
                SystemMessage(
                    content=MOCKSQL_PRODUCT_PREAMBLE
                    + "\n\nTu réalignes la description d'un test sur sa sortie réelle, validée par l'utilisateur."
                ),
                HumanMessage(content=prompt),
            ]
        )
        return result
    except Exception as exc:
        logger.warning("[accept_validation] réalignement LLM échoué: %s", exc)
        return _RealignedDescription(unit_test_description=old_desc, test_name=old_name)


async def accept_validation(state: QueryState):
    """Réaligne la description sur le réel et marque le test comme validé (Bon)."""
    test_index = state.get("test_index")
    if test_index is None:
        logger.warning("[accept_validation] test_index absent — no-op")
        return {}

    test = get_test(state["session"])
    if not test:
        return {}

    target = next(
        (
            c
            for c in test.get("test_cases") or []
            if str(c.get("test_index")) == str(test_index)
        ),
        None,
    )
    if target is None:
        logger.warning("[accept_validation] test_index=%s introuvable", test_index)
        return {}

    try:
        actual_rows = len(json.loads(target.get("results_json") or "[]"))
    except Exception:
        actual_rows = 0

    # Chemin nominal : l'évaluateur a déjà proposé une description réalignée (corrected_description)
    # dans le VALIDATION_PROMPT — on l'applique tel quel, sans 2ᵉ appel LLM. Fallback réalignement
    # LLM uniquement pour les tests anciens (sauvés avant ce champ) ou si le champ est vide.
    corrected_desc = (target.get("corrected_description") or "").strip()
    if corrected_desc:
        realigned = _RealignedDescription(
            unit_test_description=corrected_desc,
            test_name=(target.get("corrected_name") or "").strip(),
        )
        logger.diag(
            "[accept_validation] corrected_description appliquée (pré-calculée)"
        )
    else:
        realigned = await _realign_description(target, actual_rows)
    explanation = "Validé par toi : la description reflète désormais la sortie réelle."

    updated_cases = []
    for c in test.get("test_cases") or []:
        if str(c.get("test_index")) == str(test_index):
            was_input_desync = c.get("reason_type") == "bad_input_description"
            c = dict(c)
            c["unit_test_description"] = realigned.unit_test_description
            if realigned.test_name:
                c["test_name"] = realigned.test_name
            c["verdict"] = "Bon"
            c["reason_type"] = None
            c["evaluation_explanation"] = explanation
            c.pop("expected_row_count", None)
            c.pop("corrected_description", None)
            c.pop("corrected_name", None)
            # T1↔T2 : valider une desync d'entrée = l'utilisateur accepte les données
            # réelles, donc sa prémisse d'entrée était fausse → on la retire pour que
            # le garde bad_data ne protège plus une prémisse abandonnée. (Une validation
            # de SORTIE — needs_validation / bad_description — ne touche pas la prémisse
            # d'entrée, qui reste pertinente.)
            if was_input_desync:
                c.pop("user_premise", None)
        updated_cases.append(c)

    update_test(state["session"], {"test_cases": updated_cases})
    logger.diag("[accept_validation] test=%s validé → Bon", test_index)

    parent = (
        state["messages"][-1].id
        if state.get("messages")
        else state.get("parent_message_id")
    )
    update_msg = AIMessage(
        content=json.dumps(
            {
                "test_index": test_index,
                "new_name": realigned.test_name,
                "new_description": realigned.unit_test_description,
            }
        ),
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.UPDATE_TEST,
            "parent": parent,
            "request_id": state.get("request_id"),
            "test_index": test_index,
        },
    )
    eval_msg = AIMessage(
        content=f"**Bon** — {explanation}",
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "parent": update_msg.id,
            "request_id": state.get("request_id"),
            "test_index": test_index,
        },
    )
    return {"messages": [update_msg, eval_msg]}
