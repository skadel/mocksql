import copy
import json
import logging
import uuid

from langchain_core.messages import AIMessage
from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate

from build_query.examples_generator import (
    _convert_datetime_fields,
    retrieve_existing_tests,
)
from build_query.state import QueryState
from models.schemas import get_schemas
from utils.examples import create_pydantic_models, filter_columns
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from utils.prompt_utils import create_output_fixing_parser

import utils.logger  # noqa: F401

logger = logging.getLogger(__name__)

# Champs d'args conservés dans le ledger des tentatives (le reste — test_index
# notamment — est un détail de résolution interne, pas une donnée de l'op).
_LEDGER_OP_FIELDS = (
    "table",
    "row_index",
    "field",
    "value_json",
    "tables",
    "instruction",
)


def append_correction_attempt(state: QueryState, test_uid, ops: list) -> list:
    """Retourne le ledger ``correction_attempts`` augmenté d'une entrée pour ce lot.

    ``ops`` : liste ``[{tool, args}]`` (format ``agent_tool_args["calls"]``) ou
    ``[{"tool": "regen"}]`` pour une régénération complète. L'``outcome`` est
    laissé à None — complété par ``bad_data_to_agent`` quand la boucle re-rentre.
    """
    attempts = list(state.get("correction_attempts") or [])
    flat_ops = []
    for op in ops:
        args = op.get("args") or {}
        flat_ops.append(
            {
                "tool": op.get("tool", "?"),
                **{k: args[k] for k in _LEDGER_OP_FIELDS if k in args},
            }
        )
    attempts.append(
        {
            "round": len(attempts) + 1,
            "test_uid": test_uid,
            "ops": flat_ops,
            "outcome": None,
        }
    )
    return attempts


def _coerce_value_json(raw):
    """Décode un ``value_json`` (littéral JSON encodé en chaîne) en valeur Python.

    Récupère l'erreur récurrente du LLM (surtout flash-lite) qui copie l'exemple
    Python-repr du docstring de ``patch_test_field`` (`'"texte"'`) et entoure le
    littéral JSON de guillemets SIMPLES parasites → ``json.loads`` échoue et, sans
    récupération, la valeur garderait ses quotes (ex. SIRET ``'"99999999999999"'``
    qui casse ``LENGTH(...)=14`` / ``REGEXP '^[0-9]+$'`` → ligne filtrée → vide).
    On retente après avoir retiré une seule couche de guillemets simples enveloppants ;
    sinon on retombe sur ``raw`` brut (comportement historique)."""
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        pass
    if isinstance(raw, str) and len(raw) >= 2 and raw[0] == "'" and raw[-1] == "'":
        try:
            return json.loads(raw[1:-1])
        except (json.JSONDecodeError, TypeError):
            pass
    return raw


async def apply_single_patch(
    state: QueryState, test_case: dict, data: dict, tool_name: str, args: dict
) -> dict:
    """Applique une opération chirurgicale sur data et retourne data modifié."""
    test_index = args.get("test_index", "?")
    if tool_name == "patch_test_field":
        table = args.get("table", "")
        row_idx = int(args.get("row_index", 0))
        field = args.get("field", "")
        value = _coerce_value_json(args.get("value_json", "null"))
        if table not in data or row_idx >= len(data[table]):
            logger.warning(
                "[data_patcher] patch_test_field test=%s: cible %s[%d] introuvable — patch ignoré",
                test_index,
                table,
                row_idx,
            )
        elif field not in data[table][row_idx]:
            # Ne jamais créer un champ fantôme absent de la ligne : il corromprait
            # les données du test (colonne inexistante dans le schéma réel). L'agent
            # est censé avoir été renvoyé en amont (_validate_data_patch_calls).
            logger.warning(
                "[data_patcher] patch_test_field test=%s: champ '%s' absent de %s[%d] — patch ignoré (champs: %s)",
                test_index,
                field,
                table,
                row_idx,
                sorted(data[table][row_idx]),
            )
        else:
            data[table][row_idx][field] = value
            logger.diag(
                "[data_patcher] patch_test_field test=%s %s[%d].%s = %r",
                test_index,
                table,
                row_idx,
                field,
                value,
            )
    elif tool_name == "remove_test_row":
        table = args.get("table", "")
        row_idx = int(args.get("row_index", 0))
        if table in data and row_idx < len(data[table]):
            data[table].pop(row_idx)
            logger.diag(
                "[data_patcher] remove_test_row test=%s %s[%d] supprimé",
                test_index,
                table,
                row_idx,
            )
        else:
            logger.warning(
                "[data_patcher] remove_test_row test=%s: cible %s[%d] introuvable — suppression ignorée",
                test_index,
                table,
                row_idx,
            )
    elif tool_name == "add_test_row":
        tables = args.get("tables") or []
        instruction = args.get("instruction", "")
        data = await _add_rows(state, test_case, data, tables, instruction)
    return data


async def data_patcher_node(state: QueryState):
    """Applique un patch chirurgical sur les données d'un test existant sans appel au générateur complet."""
    tool_call = state.get("agent_tool_call", "")
    args = state.get("agent_tool_args") or {}

    if tool_call == "data_batch":
        ops = args.get("calls", [])
        if not ops:
            logger.warning("[data_patcher] data_batch: liste d'opérations vide")
            return {}

        # Regroupement des opérations par test_index : un seul data_batch peut désormais
        # patcher PLUSIEURS tests (régénération partielle sur changement de source — chaque
        # test reçoit le même delta minimal). Le cas mono-test (boucle bad_data) reste
        # strictement identique : un seul groupe → un seul message EXAMPLES.
        groups: dict = {}
        for op in ops:
            ti = op.get("args", {}).get("test_index")
            if ti is None:
                continue
            groups.setdefault(str(ti), []).append(op)
        if not groups:
            logger.warning(
                "[data_patcher] data_batch: aucun test_index résolu dans les ops"
            )
            return {}

        existing_tests = await retrieve_existing_tests(state["session"], state)
        examples_msgs = []
        last_test_index = None
        parent = state.get("agent_message_id") or state.get("user_message_id")
        for test_index, ti_ops in groups.items():
            test_case = next(
                (
                    t
                    for t in existing_tests
                    if str(t.get("test_index")) == str(test_index)
                ),
                None,
            )
            if test_case is None:
                logger.warning(
                    "[data_patcher] data_batch: test_index=%s introuvable", test_index
                )
                continue

            data = copy.deepcopy(test_case.get("data") or {})

            failing_assertions = [
                a
                for a in (test_case.get("assertion_results") or [])
                if a.get("status") != "pass"
            ]
            if failing_assertions:
                logger.diag(
                    "[data_patcher] data_batch test=%s — %d assertion(s) en échec avant patch:\n%s",
                    test_index,
                    len(failing_assertions),
                    json.dumps(
                        [
                            {
                                "description": a.get("description", "?"),
                                "error": a.get("error", ""),
                            }
                            for a in failing_assertions
                        ],
                        ensure_ascii=False,
                        indent=2,
                    ),
                )

            logger.diag(
                "[data_patcher] data_batch test=%s — %d opération(s): %s",
                test_index,
                len(ti_ops),
                [op["tool"] for op in ti_ops],
            )
            for op in ti_ops:
                data = await apply_single_patch(
                    state, test_case, data, op["tool"], op["args"]
                )

            updated_test = {**test_case, "data": data}
            examples_msgs.append(
                AIMessage(
                    content=json.dumps(updated_test, default=str),
                    id=str(uuid.uuid4()),
                    additional_kwargs={
                        "type": MsgType.EXAMPLES,
                        "parent": parent,
                        "request_id": state.get("request_id"),
                    },
                )
            )
            last_test_index = test_index

        if not examples_msgs:
            return {}

        update = {
            "examples": examples_msgs,
            "test_index": int(last_test_index)
            if str(last_test_index).isdigit()
            else None,
        }
        # Ledger de la boucle bad_data : mémorise les ops du lot pour que le round
        # suivant les voie (l'outcome est complété par bad_data_to_agent si la
        # boucle re-rentre). Sans lui, le round 2 ne sait pas que le round 1 a
        # déjà patché telle colonne sans effet et peut répéter/défaire le patch.
        # Mono-test uniquement (la boucle bad_data ne traite qu'un test à la fois).
        if state.get("evaluation_feedback") == "bad_data" and len(groups) == 1:
            only_test = next(
                (
                    t
                    for t in existing_tests
                    if str(t.get("test_index")) == str(last_test_index)
                ),
                None,
            )
            update["correction_attempts"] = append_correction_attempt(
                state, (only_test or {}).get("test_uid"), ops
            )
        return update

    # Comportement existant — opération chirurgicale unique
    test_index = args.get("test_index")
    if test_index is None:
        logger.warning("[data_patcher] test_index absent des agent_tool_args")
        return {}

    existing_tests = await retrieve_existing_tests(state["session"], state)
    test_case = next(
        (t for t in existing_tests if str(t.get("test_index")) == str(test_index)),
        None,
    )
    if test_case is None:
        logger.warning("[data_patcher] test_index=%s introuvable", test_index)
        return {}

    data = copy.deepcopy(test_case.get("data") or {})

    failing_assertions = [
        a
        for a in (test_case.get("assertion_results") or [])
        if a.get("status") != "pass"
    ]
    if failing_assertions:
        logger.diag(
            "[data_patcher] %s test=%s — %d assertion(s) en échec avant patch:\n%s",
            tool_call,
            test_index,
            len(failing_assertions),
            json.dumps(
                [
                    {
                        "description": a.get("description", "?"),
                        "error": a.get("error", ""),
                    }
                    for a in failing_assertions
                ],
                ensure_ascii=False,
                indent=2,
            ),
        )

    data = await apply_single_patch(state, test_case, data, tool_call, args)

    updated_test = {**test_case, "data": data}
    parent = state.get("agent_message_id") or state.get("user_message_id")
    msg = AIMessage(
        content=json.dumps(updated_test, default=str),
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.EXAMPLES,
            "parent": parent,
            "request_id": state.get("request_id"),
        },
    )
    return {
        "examples": [msg],
        "test_index": int(test_index) if str(test_index).isdigit() else None,
        "rerun_only": True,
    }


def _flatten_table_name(name: str) -> str:
    """Forme aplatie canonique d'un nom de table : 2 dernières parties jointes par `_`,
    comme `filter_columns`. `a.b.c` → `b_c` ; `b.c` → `b_c` ; `b_c` → `b_c`.
    """
    parts = name.split(".")
    return "_".join(parts[-2:]) if len(parts) >= 2 else name


def _scope_schema_for_tables(filtered_schema: list, tables: list) -> list:
    """Restreint filtered_schema aux tables demandées.

    Le LLM réfère souvent une table par son nom BigQuery COMPLET
    (bigquery-public-data.chicago_taxi_trips.taxi_trips), alors que filter_columns
    produit un nom APLATI (chicago_taxi_trips_taxi_trips). On rapproche les deux via
    la forme aplatie (et on tolère un nom déjà aplati passé tel quel).
    """
    wanted = {_flatten_table_name(t) for t in tables} | set(tables)
    return [t for t in filtered_schema if t["table_name"] in wanted]


async def _add_rows(
    state: QueryState, test_case: dict, data: dict, tables: list, instruction: str
) -> dict:
    """Génère de nouvelles lignes pour les tables spécifiées via un appel LLM scopé."""
    schema = await get_schemas(project_id=state["project"])
    used_columns = [json.loads(c) for c in state.get("used_columns") or []]
    filtered_schema = filter_columns(schema, used_columns)

    scoped_schema = _scope_schema_for_tables(filtered_schema, tables)
    if not scoped_schema:
        logger.warning(
            "[data_patcher] add_test_row: tables=%s introuvables dans filtered_schema (%s)",
            tables,
            [t["table_name"] for t in filtered_schema],
        )
        return data

    # Noms aplatis résolus : alignent la recherche des lignes existantes (data est clé
    # par nom aplati) sur le schéma scopé, même si le LLM a passé des noms complets.
    tables = [t["table_name"] for t in scoped_schema]

    ScopedModel = create_pydantic_models(scoped_schema)
    raw_parser = PydanticOutputParser(pydantic_object=ScopedModel)
    parser = create_output_fixing_parser(raw_parser)

    existing_rows_block = _format_rows_with_indices(data, tables)
    test_desc = test_case.get("unit_test_description", "")
    instruction_line = (
        f"\nInstruction spécifique : {instruction}" if instruction else ""
    )

    prompt = ChatPromptTemplate.from_messages(
        [
            (
                "system",
                "Tu génères des données de test SQL pour MockSQL.",
            ),
            (
                "human",
                "SQL testé :\n{sql}\n\n"
                "Scénario du test : {test_desc}\n\n"
                "Lignes existantes dans ce test :\n{existing_rows}\n\n"
                "Génère exactement 1 nouvelle ligne par table listée dans le schéma de sortie, "
                "cohérente avec les données existantes et différente d'elles.{instruction_line}\n\n"
                "{format_instructions}",
            ),
        ]
    )

    chain = prompt | make_llm() | parser
    result = await chain.ainvoke(
        {
            "sql": state.get("optimized_sql") or state.get("query", ""),
            "test_desc": test_desc,
            "existing_rows": existing_rows_block,
            "instruction_line": instruction_line,
            "format_instructions": raw_parser.get_format_instructions(),
        }
    )

    # model_dump (≠ .dict()) honore serialize_by_alias → noms de colonnes réels
    # (colonnes dbt à underscore initial préservées).
    new_rows_dict = _convert_datetime_fields(result.model_dump())
    for table_name, new_rows in new_rows_dict.items():
        if new_rows:
            data.setdefault(table_name, []).extend(new_rows)
            logger.diag(
                "[data_patcher] add_test_row: %d ligne(s) ajoutée(s) à %s",
                len(new_rows),
                table_name,
            )

    return data


def _format_rows_with_indices(data: dict, tables: list) -> str:
    lines = []
    for table in tables:
        rows = data.get(table, [])
        lines.append(f"Table {table}:")
        if rows:
            for i, row in enumerate(rows):
                lines.append(f"  [{i}] {json.dumps(row, ensure_ascii=False)}")
        else:
            lines.append("  (aucune ligne)")
    return "\n".join(lines)
