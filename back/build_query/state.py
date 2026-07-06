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
    target_path: Optional[
        str
    ]  # branche UNION ALL ciblée : nom machine (cf. path_slicer.PathSpec) | "all" | None.
    # None/"all" = SQL complet (comportement actuel inchangé). Pose par le generator
    # (defaut 1re gen = focus[0]) ou par l'agent (set_target_path). Persiste par test.
    path_plans: Optional[
        str
    ]  # JSON {path_name: {sliced_sql, used_columns, branch_index, host_cte}} + "all".
    # Construit UNE fois a la validation (AST pur, cf. path_slicer) ; lu par le generator
    # et suggestions au lieu de re-slicer. Le constraints_hint (simplify) reste lazy+cache.
    focus_fallback: Optional[
        bool
    ]  # Trigger one-shot pose par le noeud focus_fallback : force _should_regenerate
    # a relancer le generator en target_path="all" apres un focus non convergent.
    focus_fallback_used: Optional[
        bool
    ]  # Garde anti-boucle : le fallback focus->all ne se declenche qu'UNE fois par run
    # (route_evaluator retombe ensuite sur les sorties historiques).
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
    user_rerun: Optional[
        bool
    ]  # True ONLY on the « Relancer » button (front sends it alongside rerun_all_tests,
    # which is shared with sql_update): read-only rerun — route_evaluator never enters
    # the correction loops (bad_data_to_agent / assertion_corrector)
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
    tests_target: Optional[
        int
    ]  # N tests requested by the user at first generation (1–3, default 1). N = total tests,
    # so N-1 extra tests are auto-built from single suggestions after the nominal one.
    auto_tests_built: Optional[
        int
    ]  # counter of auto-built tests in this run (excludes the nominal test); drives the
    # loop-continuation decision in route_evaluator (built < tests_target - 1).
    resume_batch: Optional[
        bool
    ]  # set by pre_routing when a first-gen multi-test batch was interrupted (tests exist on
    # disk but fewer than tests_target). Routes straight to generate_single_suggestion to
    # build the missing tests (skips rebuilding the nominal) and lets route_evaluator keep
    # looping even though has_existing_tests is True.
    regenerate_suggestions: Optional[
        bool
    ]  # True when the user explicitly asks for fresh coverage suggestions (panel button):
    # routing → suggestions_generator directly, and skip final_response (no "tests generated" msg)
    coverage_gap_analysis: Optional[
        str
    ]  # `analyse_des_manques` from suggestions_generator: short gap analysis woven into
    # final_response so the closing message tells the user what's uncovered and points to the panel
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
    apply_description_intent: Optional[
        bool
    ]  # set by the front when the user accepts a proposed description update (panel button) →
    # routing routes to apply_description (deterministic). The agent NEVER applies a
    # description change directly: update_test_description only ever proposes (propose_description
    # node), and the change is applied here once the user accepts. test_index targets the test.
    reject_description_intent: Optional[
        bool
    ]  # set by the front when the user declines a proposed description update (panel button) →
    # routing routes to reject_description (clears the pending proposal, keeps the actual desc).
    revalidated: Optional[
        bool
    ]  # set by accept_validation when it successfully validated a test → reprise post-éval :
    # route_after_accept → suggestions_generator (au lieu de history_saver direct), pour
    # regénérer les suggestions « comme si on reprenait après l'évaluation ». Pas de re-run
    # DuckDB : les résultats/assertions stockés sont déjà à jour (input + SQL inchangés).
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
    pending_lessons: Optional[
        list
    ]  # leçons formulées par l'agent (outil note_lesson) au fil du run, accumulées
    # ici. Chaque entrée : {scope: "table"|"join", key, rule, source: "correction"|"user"}.
    # Persistées dans le profil partagé par history_saver via lessons.persist_pending_lessons :
    # source="user" → toujours ; source="correction" → seulement à convergence.
    # Réinjectées dans les prompts de génération via lessons.format_lessons_block.
