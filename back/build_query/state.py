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
    debug_retries: int
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
    test_uid: Optional[
        str
    ]  # stable test identity to target (preferred over test_index)
    test_index: Optional[
        int
    ]  # legacy slot/order number; fallback target if no test_uid
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
    rerun_only: Optional[
        bool
    ]  # True when user reruns a single test to verify — skips suggestions
    assertion_only: Optional[
        bool
    ]  # True when user edits assertion metadata only (no input data regeneration)
    suggestion_intent: Optional[
        bool
    ]  # True when input comes from clicking a coverage suggestion: the agent must
    # produce a test action (add new, or extend an existing near-duplicate) and never
    # answer in free text. route_agent_output falls back to generator if it produces none.
    has_existing_tests: Optional[
        bool
    ]  # set by pre_routing: True if test_cases already exist
    regenerate_suggestions: Optional[
        bool
    ]  # True when the user explicitly asks for fresh coverage suggestions (panel button):
    # routing → suggestions_generator directly, and skip final_response (no "tests generated" msg)
    agent_tool_call: Optional[
        str
    ]  # tool called by conversational_agent: "generate_test" | "delete_test" | None
    agent_tool_args: Optional[Dict]  # args of the tool call
    agent_message_id: Optional[
        str
    ]  # ID of the GENERATE_TEST_SCENARIO msg emitted by conversational_agent
    model_context: Optional[
        str
    ]  # concatenated mocksql.md files (global → file-specific)
    evaluation_feedback: Optional[
        str
    ]  # reason set by test_evaluator when Insuffisant: "bad_data" | "bad_assertions" | "too_many_rows" | "bad_description" | "bad_input_description" | "needs_validation"
    # "bad_description" = la description annonce une sortie contredite par le résultat réel
    # (donnée valide, narratif faux) → flagué, aucune boucle de retry (route vers complétion).
    # "bad_input_description" = la description annonce des valeurs d'ENTRÉE contredites par les
    # données réellement injectées (donnée valide, narratif d'entrée faux) → même délégation que
    # bad_description (VALIDATION_PROMPT), aucune boucle de retry. Cf. TICKET-2.
    # "needs_validation" = la description suppose une cardinalité (nb de lignes) que le résultat
    # ne produit pas, MAIS les données d'entrée sont valides → on NE corrige pas en boucle :
    # on sauve l'état et on demande à l'utilisateur de valider (VALIDATION_PROMPT). Cf.
    # accept_validation (réaligne la description sur le réel + verdict Bon).
    validate_intent: Optional[
        bool
    ]  # set by the front when the user clicks « Je valide l'état actuel » on a
    # needs_validation test → routing routes to accept_validation (deterministic).
    empty_results_regen: Optional[
        bool
    ]  # set by test_evaluator: DuckDB returned 0 rows → route straight to generator
    # for a holistic regeneration (targeting the failing CTE), bypassing the
    # conversational_agent's single-field patching which cannot fix 0-row queries.
    auto_correct: Optional[
        bool
    ]  # set by bad_data_to_agent: signals the conversational_agent it was triggered
    # automatically (bad_data retry, no fresh user input) → take the auto-correction
    # branch regardless of a stale `input`. Reset by the agent after reading.
    reevaluation_context: Optional[
        str
    ]  # set by conversational_agent when it suspects the evaluation was wrong; triggers LLM re-eval in test_evaluator
    correction_attempts: Optional[
        list
    ]  # bad_data loop ledger (state-level — distinct from the per-test
    # `correction_attempts` used by assertion_corrector). One entry per round:
    # {round, test_uid, ops: [{tool, table, row_index, field, value_json}…],
    #  outcome: {blocking_cte, digest} | None}. Written by data_patcher/generator,
    # outcome completed by bad_data_to_agent from the fresh diagnostic, rendered as
    # an alternating AI/HUMAN conversation in conversational_agent, reset by
    # history_saver (loop exit). Also feeds the anti-no-op guard (a batch identical
    # to a past attempt is rejected without consuming a retry).
