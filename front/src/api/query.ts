import { createAsyncThunk } from '@reduxjs/toolkit';
import { v4 as uuidv4 } from 'uuid';
import {
  addTextMessage,
  appendQueryComponentMessage,
  appendStreamingReasoning,
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
      parentMessageId, userTables, profileResult, testIndex, context, assertionOnly, forceRoute
    } = params;

    if (!userInput && !query && !userTables && !profileResult) return;
    dispatch(setLoading(true));

    const userMessageId = uuidv4();
    const request_id = uuidv4();

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
        parent: parentMessageId || undefined,
        children: [],
      }));
    } else if (context === 'sql_update') {
      dispatch(addTextMessage({
        id: userMessageId,
        type: 'user',
        contents: { text: 'Mise à jour SQL' },
        contentType: 'sql_update',
        parent: parentMessageId || undefined,
        children: [],
      }));
    }

    let step = '';
    const capturedSteps = ['parser', 'generator', 'executor'];

    dispatch(setError(''));
    const token = localStorage.getItem('jwt') || '';

    await streamThunk(
      `${process.env.REACT_APP_BACKEND_URL}/api/query/build/stream_events`,
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
            gen_retries: 2,
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
            const text = (pd.data?.chunk?.content || [])
              .filter((c: any) => c.type === 'text')
              .map((c: any) => c.text || '')
              .join('');
            if (text) dispatch(appendStreamingReasoning(text));
          }
          else if (pd.event === 'on_chain_stream') {
            (pd.data?.chunk.messages || []).forEach((m: any) => {
              const nm = formatMessage(m);
              if (nm.contents.tables !== undefined) {
                if (testIndex !== undefined) {
                  nm.testIndex = testIndex;
                } else if (context === 'sql_update') {
                  nm.context = 'sql_update';
                }
              }
              dispatch(appendQueryComponentMessage(nm));
            });
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
    `${process.env.REACT_APP_BACKEND_URL}/api/validate-query`,
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
}

export interface CheckProfileResult {
  profile_complete: boolean;
  auto_profile_available?: boolean;
  profile_request?: import('../utils/types').ProfileRequest;
}

export const checkProfileApi = async (params: CheckProfileParams): Promise<CheckProfileResult> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${process.env.REACT_APP_BACKEND_URL}/api/check-profile`,
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
    `${process.env.REACT_APP_BACKEND_URL}/api/save-profile`,
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
    `${process.env.REACT_APP_BACKEND_URL}/api/skip-profile`,
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
  project: string;
  user?: string;
  session: string;
}

export const autoProfileApi = async (params: AutoProfileParams): Promise<void> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${process.env.REACT_APP_BACKEND_URL}/api/auto-profile`,
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

export interface ImportMissingTablesParams {
  tables_to_import: string[];
  project: string;
  user?: string;
}

export const importMissingTablesApi = async (params: ImportMissingTablesParams): Promise<{ imported: number; tables: string[] }> => {
  const token = localStorage.getItem('jwt') || '';
  const response = await fetch(
    `${process.env.REACT_APP_BACKEND_URL}/api/import-missing-tables`,
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
      url: `${process.env.REACT_APP_BACKEND_URL}/api/fetch-page`,
      method: 'POST',
      body: { project, sql, dialect, msgId, offset, limit: safeLimit },
      defaultFailureMessage: 'Failed to fetch page',
      dispatch,
      rejectWithValue: (v: unknown) => rejectWithValue(v as string),
    });
  }
);
