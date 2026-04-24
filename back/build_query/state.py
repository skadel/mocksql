from typing import TypedDict, Union, Optional, Dict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from typing_extensions import Annotated


class QueryState(TypedDict):
    input: str
    query: str  # raw SQL provided by user
    validated_sql: str
    optimized_sql: str
    title: str
    dialect: str
    schemas: list
    messages: Annotated[list[AnyMessage], add_messages]
    examples: Annotated[list[AnyMessage], add_messages]
    history: Annotated[list[AnyMessage], add_messages]
    status: Union[str, None]
    gen_retries: int
    changed_message_id: str
    parent_message_id: str
    session: str
    project: str
    reasoning: str
    user_message_id: str
    error: str
    route: str
    request_id: str
    current_query: str  # raw faulty SQL (used by fixer on line_error)
    query_decomposed: str  # JSON-encoded CTE steps set by validator
    user_tables: str
    used_columns: list
    used_columns_changed: bool
    optimize: bool
    save: Union[str, None]
    test_index: Optional[int]  # if set, only regenerate/modify this test (0-based)
    profile_result: Optional[str]  # JSON uploaded by user after running profile query
    profile_complete: Optional[
        bool
    ]  # set by profile_checker: True if all columns profiled
    profile: Optional[Dict]  # full profile dict passed to generator
    profile_billing_tb: Optional[
        float
    ]  # estimated BigQuery bytes processed for profile SQL (TB)
    rerun_all_tests: Optional[
        bool
    ]  # True when SQL changed: re-run all existing tests with new SQL
