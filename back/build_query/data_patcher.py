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


async def data_patcher_node(state: QueryState):
    """Applique un patch chirurgical sur les données d'un test existant sans appel au générateur complet."""
    tool_call = state.get("agent_tool_call", "")
    args = state.get("agent_tool_args") or {}
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

    if tool_call == "patch_test_field":
        table = args.get("table", "")
        row_idx = int(args.get("row_index", 0))
        field = args.get("field", "")
        raw = args.get("value_json", "null")
        try:
            value = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            value = raw
        if table in data and row_idx < len(data[table]):
            data[table][row_idx][field] = value
            logger.diag(
                "[data_patcher] patch_test_field test=%s %s[%d].%s = %r",
                test_index,
                table,
                row_idx,
                field,
                value,
            )

    elif tool_call == "remove_test_row":
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

    elif tool_call == "add_test_row":
        tables = args.get("tables") or []
        instruction = args.get("instruction", "")
        data = await _add_rows(state, test_case, data, tables, instruction)

    updated_test = {**test_case, "data": data}

    parent = state.get("agent_message_id") or state.get("user_message_id")
    msg = AIMessage(
        content=json.dumps(updated_test),
        id=str(uuid.uuid4()),
        additional_kwargs={
            "type": MsgType.EXAMPLES,
            "parent": parent,
            "request_id": state.get("request_id"),
        },
    )
    return {
        "examples": [msg],
        "test_index": test_index,
        "rerun_only": True,
    }


async def _add_rows(
    state: QueryState, test_case: dict, data: dict, tables: list, instruction: str
) -> dict:
    """Génère de nouvelles lignes pour les tables spécifiées via un appel LLM scopé."""
    schema = await get_schemas(project_id=state["project"])
    used_columns = [json.loads(c) for c in state.get("used_columns") or []]
    filtered_schema = filter_columns(schema, used_columns)

    scoped_schema = [t for t in filtered_schema if t["table_name"] in tables]
    if not scoped_schema:
        logger.warning(
            "[data_patcher] add_test_row: tables=%s introuvables dans filtered_schema (%s)",
            tables,
            [t["table_name"] for t in filtered_schema],
        )
        return data

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
                "Tu génères des données de test SQL pour MockSQL.\n\n"
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

    new_rows_dict = _convert_datetime_fields(result.dict())
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
