import { createAsyncThunk } from '@reduxjs/toolkit';
import { v4 as uuidv4 } from 'uuid';
import {
  addTextMessage,
  appendQueryComponentMessage,
  appendComponentToLastMessage,
  appendStreamingReasoning,
  patchMessageContents,
  removeMessage,
  setError,
  setLoading,
  setLoadingMessage
} from "../features/buildModel/buildModelSlice";
import { formatMessage } from "../utils/messages";
import { ChatQueryParams } from '../utils/types';
import { updateModel } from "./models";
import { apiRequest, streamThunk } from "./utils";

const chatController = new AbortController();

export const stopStream = () => {
  chatController.abort();
};

export const chatQuery = createAsyncThunk(
  'gpt/generateQuery',
  async (
    params: ChatQueryParams,
    { dispatch }
  ) => {
    const {
      userInput, sessionId, project,
      query, ChangedMessageId, t, user,
      parentMessageId, userTables, profileResult, testIndex, context, assertionOnly, rerunOnly, forceRoute, silent
    } = params;

    if (!userInput && !query && !userTables && !profileResult) return;
    dispatch(setLoading(true));

    // For silent ops (sql_update, rerun test), reuse parentMessageId so bot responses
    // attach directly to the existing thread without a new user message bubble.
    const userMessageId = silent ? (parentMessageId || uuidv4()) : uuidv4();
    const request_id = uuidv4();

    if (!silent) {
      if (profileResult) {
        dispatch(addTextMessage({
          id: userMessageId,
          type: 'user',
          contents: { text: '📊 Résultats de profiling uploadés' },
          parent: ChangedMessageId,
          children: [],
        }));
      } else if (userTables) {
        dispatch(addTextMessage({
          id: userMessageId,
          type: 'user',
          contents: { text: 'Modification des exemples', tables: userTables },
          parent: ChangedMessageId,
          children: [],
        }));
      } else if (userInput) {
        dispatch(addTextMessage({
          id: userMessageId,
          type: 'user',
          contents: { text: userInput },
          parent: (ChangedMessageId && ChangedMessageId !== '') ? ChangedMessageId : (parentMessageId || undefined),
          children: [],
        }));
      }
    }

    let step = '';
    const capturedSteps = ['parser', 'generator', 'executor'];
    let convStreamId: string | null = null;
    // Accumulates the raw streaming text emitted by conversational_agent before the final
    // message arrives — stored as `contents.reasoning` on the scenario message.
    let convStreamText = '';
    // ID of the generate_test_scenario message; used as parent for evaluation in conv flow.
    let convGenerateParentId: string | null = null;
    // ID of the primary bot message that accumulates evaluation + suggestions into one bubble.
    // Set to the scenario message (conv flow) or first examples message (initial flow).
    let generationSummaryId: string | null = null;

    dispatch(setError(''));
    const token = localStorage.getItem('jwt') || '';

    await streamThunk(
      `${import.meta.env.VITE_BACKEND_URL}/api/query/build/stream_events`,
      {
        method: 'POST',
        openWhenHidden: true,
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({
          input: {
            input: userInput,
            query: query || '',
            validated_sql: '',
            optimized_sql: '',
            user_tables: userTables ? JSON.stringify(userTables) : '',
            profile_result: profileResult || '',
            dialect: 'bigquery',
            schemas: [],
            session: sessionId,
            project,
            user,
            user_message_id: userMessageId,
            parent_message_id: parentMessageId || '',
            changed_message_id: ChangedMessageId || '',
            request_id,
            gen_retries: 10,
            debug_retries: 3,
            used_columns: [],
            used_columns_changed: false,
            optimize: true,
            route: forceRoute ?? (testIndex != null ? 'generator' : ''),
            status: '',
            save: '',
            title: '',
            reasoning: '',
            error: '',
            current_query: '',
            query_decomposed: '',
            test_index: testIndex ?? null,
            rerun_all_tests: context === 'sql_update',
            assertion_only: assertionOnly ?? false,
            rerun_only: rerunOnly ?? false,
            profile_complete: null,
            profile: null,
            profile_billing_tb: null,
            messages: [],
            history: [],
            examples: [],
          },
          config: {},
          kwargs: { version: 'v2' },
          diff: false,
        }),
        onmessage(msg) {
          const pd = msg.data ? JSON.parse(msg.data) : null;
          if (!pd) return;
          if (pd.error) throw pd;

          if (pd.event === 'on_chain_start' && capturedSteps.includes(pd.name)) {
            step = pd.name;
            const loadingMap: Record<string, string> = {
              parser:    t('loading.validate_query'),
              generator: t('loading.generating_examples'),
              executor:  t('loading.executing_query'),
            };
            dispatch(setLoadingMessage(loadingMap[step] || ''));
          }
          else if (pd.event === 'on_chain_end' && capturedSteps.includes(pd.name)) {
            step = '';
          }
          else if (pd.event === 'on_chain_stream' && pd.name === 'routing') {
            const title = (pd.data?.chunk?.title || '').trim()
              .toLowerCase()
              .replace(/[^\p{L}\p{N}]+/gu, '_');
            if (title) {
              dispatch(updateModel({
                name: title,
                session_id: sessionId,
                user_sub: user,
                project_id: project,
              }));
            }
          }
          else if (pd.event === 'on_chat_model_stream') {
            const rawContent = pd.data?.chunk?.content;
            const text = Array.isArray(rawContent)
              ? rawContent.filter((c: any) => c.type === 'text').map((c: any) => c.text || '').join('')
              : typeof rawContent === 'string' ? rawContent : '';

            if (!text) return;

            // Stream conversational agent tokens directly into the chat thread
            if (pd.metadata?.langgraph_node === 'conversational_agent') {
              convStreamText += text;
              if (!convStreamId) {
                convStreamId = uuidv4();
                dispatch(appendQueryComponentMessage({
                  id: convStreamId,
                  type: 'bot',
                  contents: { text },
                  parent: userMessageId,
                  children: [],
                }));
              } else {
                dispatch(appendComponentToLastMessage({
                  id: convStreamId,
                  type: 'bot',
                  contents: { text },
                  parent: userMessageId,
                  children: [],
                }));
              }
            } else {
              dispatch(appendStreamingReasoning(text));
            }
          }
          else if (pd.event === 'on_chain_stream') {
            const messages = pd.data?.chunk?.messages || [];

            if (pd.name === 'conversational_agent') {
              const formattedMsgs = messages.map((m: any) => formatMessage(m));
              const scenarioMsg = formattedMsgs.find((m: any) => m.contentType === 'generate_test_scenario');

              if (scenarioMsg) {
                // Replace the streaming placeholder with the real persisted scenario message.
                // Embed the accumulated reasoning text so MessageBody can render it as "Réflexion".
                if (convStreamId) {
                  dispatch(removeMessage(convStreamId));
                  convStreamId = null;
                }
                const msgWithReasoning = convStreamText
                  ? { ...scenarioMsg, contents: { ...scenarioMsg.contents, reasoning: convStreamText } }
                  : scenarioMsg;
                convStreamText = '';
                console.log('[SSE] conv_agent: dispatching real scenario message', msgWithReasoning.id);
                dispatch(appendQueryComponentMessage(msgWithReasoning));
                convGenerateParentId = scenarioMsg.id;
                generationSummaryId = scenarioMsg.id;
                // Dispatch any accompanying text messages (rare but possible).
                formattedMsgs
                  .filter((m: any) => m.contentType !== 'generate_test_scenario')
                  .forEach((nm: any) => {
                    console.log('[SSE] formatted message:', nm.contentType, nm);
                    dispatch(appendQueryComponentMessage(nm));
                  });
              } else {
                // Regular text response — replace streaming placeholder with persisted message.
                if (convStreamId) {
                  console.log('[SSE] conv_agent chain_stream: replacing placeholder', convStreamId);
                  dispatch(removeMessage(convStreamId));
                  convStreamId = null;
                  convStreamText = '';
                }
                formattedMsgs.forEach((nm: any) => {
                  console.log('[SSE] formatted message:', nm.contentType, nm);
                  if (nm.contents.tables !== undefined) {
                    if (testIndex !== undefined) nm.testIndex = testIndex;
                    else if (context === 'sql_update') nm.context = 'sql_update';
                  }
                  dispatch(appendQueryComponentMessage(nm));
                });
              }
            } else if (convGenerateParentId && (pd.name === 'executor' || pd.name === 'test_evaluator')) {
              // Conversational generate_test flow:
              // - results/examples → silent (TestsPanel handles display)
              // - evaluation → folded into the scenario message (generationSummaryId)
              messages.forEach((m: any) => {
                const nm = formatMessage(m);
                if (nm.contents.tables !== undefined && testIndex !== undefined) nm.testIndex = testIndex;
                if (nm.contentType === 'evaluation' && pd.name === 'test_evaluator' && generationSummaryId) {
                  // Fold evaluation text into the scenario bubble instead of a standalone message
                  dispatch(patchMessageContents({ id: generationSummaryId, patch: { evaluationText: nm.contents.text } }));
                  dispatch(appendQueryComponentMessage({ ...nm, silent: true } as any)); // testResults.evaluation sync
                } else {
                  dispatch(appendQueryComponentMessage({ ...nm, silent: true } as any));
                }
              });

              if (pd.name === 'test_evaluator') {
                convGenerateParentId = null;
                // generationSummaryId is kept — suggestions_generator will use it
              }
            } else {
              messages.forEach((m: any) => {
                const nm = formatMessage(m);

                if (nm.contentType === 'examples') {
                  if (testIndex !== undefined) nm.testIndex = testIndex;
                  else if (context === 'sql_update') nm.context = 'sql_update';
                  // Track the first examples message as the consolidation target
                  if (!generationSummaryId) generationSummaryId = nm.id;
                  dispatch(appendQueryComponentMessage(nm));
                  return;
                }

                if (nm.contentType === 'results') {
                  if (testIndex !== undefined) nm.testIndex = testIndex;
                  // Silent — TestsPanel shows execution results; state sync still happens
                  dispatch(appendQueryComponentMessage({ ...nm, silent: true } as any));
                  return;
                }

                if (nm.contentType === 'evaluation' && generationSummaryId) {
                  // Fold evaluation into examples bubble (conv flow uses generationSummaryId too)
                  // Only embed text for conv flow where there's one test; for initial flow the
                  // verdict is already in testResults / TestsPanel so we skip the text embedding.
                  dispatch(appendQueryComponentMessage({ ...nm, silent: true } as any)); // testResults sync
                  return;
                }

                if (nm.contentType === 'suggestions' && generationSummaryId) {
                  // Fold suggestions into the primary bubble (examples or scenario)
                  dispatch(patchMessageContents({ id: generationSummaryId, patch: { suggestions: nm.contents.suggestions, profileAvailable: nm.contents.profileAvailable } }));
                  dispatch(appendQueryComponentMessage({ ...nm, silent: true } as any)); // state.suggestions sync
                  return;
                }

                const isIntermediate = m.additional_kwargs?.intermediate === true;
                dispatch(appendQueryComponentMessage(isIntermediate ? { ...nm, silent: true } as any : nm));
              });
            }
          }
        }
      },
      dispatch,
      chatController
    );
  }
);

export interface ValidateQueryParams {
  sql: string;
  project: string;
  user?: string;
  dialect: string;
  session: string;
  parent_message_id?: string;
}

export interface ValidateQueryResult {
  valid: boolean;
  error?: string;
  missing_tables?: string[];
  auto_import_available?: boolean;
  tables_to_import?: string[];
  used_columns?: any[];
  query_decomposed?: string;
  optimized_sql?: string;
  sql_message_id?: string;
}

export const validateQueryApi = async (params: ValidateQueryParams): Promise<ValidateQueryResult> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/validate-query`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data;
};

export interface CheckProfileParams {
  sql: string;
  project: string;
  user?: string;
  dialect: string;
  session: string;
  used_columns: any[];
  missing_columns?: any[];
  expected_joins?: any[];
  profile_result?: string;
  force?: boolean;
}

export interface CheckProfileResult {
  profile_complete: boolean;
  profile_error?: string;
  auto_profile_available?: boolean;
  missing_columns?: any[];
}

export const checkProfileApi = async (params: CheckProfileParams): Promise<CheckProfileResult> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/check-profile`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data;
};

export interface BuildProfileRequestParams {
  sql: string;
  project: string;
  dialect: string;
  session: string;
  missing_columns: any[];
}

export interface BuildProfileRequestResult {
  profile_request: import('../utils/types').ProfileRequest;
}

export const buildProfileRequestApi = async (params: BuildProfileRequestParams): Promise<BuildProfileRequestResult> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/build-profile-request`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data;
};

export interface SaveProfileParams {
  session: string;
  project: string;
  user?: string;
  profile_result: string;
}

export const saveProfileApi = async (params: SaveProfileParams): Promise<void> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/save-profile`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
};

export interface SkipProfilingParams {
  session: string;
  user?: string;
}

export const skipProfilingApi = async (params: SkipProfilingParams): Promise<void> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/skip-profile`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
};

export interface FetchPageArgs {
  page: number;
  project: string;
  sql: string;
  msgId: string;
  dialect: string;
  limit?: number;
}

export interface FetchPageResponse {
  rows: Record<string, unknown>[];
  total: number;
  limit: number;
  offset: number;
  msgId: string;
}

export interface AutoProfileParams {
  profile_sql: string;
  profile_queries?: string[];
  project: string;
  user?: string;
  session: string;
}

export interface AutoProfileResponse {
  saved: boolean;
  profile_status: 'complete' | 'partial' | 'failed';
  errors?: Array<{ query_index: number; error: string }>;
}

export const autoProfileApi = async (params: AutoProfileParams): Promise<AutoProfileResponse> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/auto-profile`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data as AutoProfileResponse;
};

export interface RefreshSchemasParams {
  tables?: string[];
}

export const refreshSchemasApi = async (params: RefreshSchemasParams = {}): Promise<{ refreshed: number; tables: string[] }> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/refresh-schemas`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ tables: params.tables ?? [] }),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data;
};

export interface ImportMissingTablesParams {
  tables_to_import: string[];
  project: string;
  dialect?: string;
  user?: string;
}

export const importMissingTablesApi = async (params: ImportMissingTablesParams): Promise<{ imported: number; tables: string[] }> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${import.meta.env.VITE_BACKEND_URL}/api/import-missing-tables`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(params),
    }
  );
  const data = await response.json();
  if (!response.ok) throw data;
  return data;
};

export const fetchPage = createAsyncThunk<FetchPageResponse, FetchPageArgs, { rejectValue: string }>(
  'query/fetchPage',
  async ({ project, page, sql, msgId, dialect, limit = 20 }, { dispatch, rejectWithValue }) => {
    const safePage = Number.isFinite(page) ? Math.max(0, page) : 0;
    const safeLimit = Number.isFinite(limit) ? Math.max(1, limit) : 20;
    const offset = safePage * safeLimit;

    return apiRequest<FetchPageResponse>({
      url: `${import.meta.env.VITE_BACKEND_URL}/api/fetch-page`,
      method: 'POST',
      body: { project, sql, dialect, msgId, offset, limit: safeLimit },
      defaultFailureMessage: 'Failed to fetch page',
      dispatch,
      rejectWithValue: (v: unknown) => rejectWithValue(v as string),
    });
  }
);
