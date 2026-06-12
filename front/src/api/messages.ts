import { createAsyncThunk } from '@reduxjs/toolkit';
import { apiRequest } from "./utils";

export async function dismissSuggestionApi(sessionId: string, suggestion: string): Promise<void> {
  const token = localStorage.getItem('jwt') || '';
  await fetch(`${import.meta.env.VITE_BACKEND_URL}/api/suggestions/dismiss`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
    body: JSON.stringify({ sessionId, suggestion }),
  });
}


export async function clearHistoryApi(sessionId: string): Promise<void> {
  const token = localStorage.getItem('jwt') || '';
  await fetch(`${import.meta.env.VITE_BACKEND_URL}/api/clearHistory`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
    body: JSON.stringify({ sessionId }),
  });
}


export const patchModelSql = createAsyncThunk(
  'messages/patchModelSql',
  async (
    { sessionId, sql, optimizedSql, testResults, restoredMessageId }: {
      sessionId: string;
      sql: string;
      optimizedSql: string;
      testResults?: any[];
      restoredMessageId?: string;
    },
    { rejectWithValue, dispatch }
  ) => {
    await apiRequest({
      url: `${import.meta.env.VITE_BACKEND_URL}/api/models/sql`,
      method: 'PATCH',
      body: {
        sessionId,
        sql,
        optimized_sql: optimizedSql,
        ...(testResults !== undefined && { test_results: testResults }),
        ...(restoredMessageId !== undefined && { restored_message_id: restoredMessageId }),
      },
      defaultFailureMessage: 'Failed to update sql',
      dispatch,
      rejectWithValue,
    });
  }
);


export const patchModelTests = createAsyncThunk(
  'messages/patchModelTests',
  async (
    { sessionId, tests }: { sessionId: string; tests: any[] },
    { rejectWithValue, dispatch }
  ) => {
    await apiRequest({
      url: `${import.meta.env.VITE_BACKEND_URL}/api/models/tests`,
      method: 'PATCH',
      body: { sessionId, tests },
      defaultFailureMessage: 'Failed to update tests',
      dispatch,
      rejectWithValue,
    });
    return tests;
  }
);


export interface AssertionInput {
  description: string;
  expected_condition: string;
}

export interface ApplyAssertionsResult {
  test_index: any;
  assertion_results: any[];
  evaluation: string;
}

// Ré-exécute une liste d'assertions fournie sur les données inchangées du test
// (modif / suppression / ajout assertion par assertion) et renvoie les résultats
// recalculés + verdict. Le backend persiste déjà test_cases.
export const applyAssertions = createAsyncThunk(
  'messages/applyAssertions',
  async (
    { sessionId, testIndex, assertions }:
      { sessionId: string; testIndex: any; assertions: AssertionInput[] },
    { rejectWithValue, dispatch }
  ) => {
    const res = await apiRequest<ApplyAssertionsResult>({
      url: `${import.meta.env.VITE_BACKEND_URL}/api/tests/apply_assertions`,
      method: 'POST',
      body: { sessionId, testIndex, assertions },
      defaultFailureMessage: 'Failed to apply assertions',
      dispatch,
      rejectWithValue,
    });
    return res;
  }
);


export const getMessages = createAsyncThunk(
  'models/getMessages',
  async (
    { modelId, t }: { modelId: string; t: Function },
    { rejectWithValue, dispatch }
  ) => {
    const url = `${import.meta.env.VITE_BACKEND_URL}/api/getMessages`;
    const response = await apiRequest<{ messages: any[]; sql: string | null; optimized_sql: string | null; test_results: any[]; last_error?: string; sql_history?: any[] }>({
      url,
      method: 'POST',
      body: { modelId },
      defaultFailureMessage: t('errors.failed_to_fetch_messages'),
      dispatch,
      rejectWithValue,
    });

    if (!response.sql) {
      return rejectWithValue({ detail: 'Not found: No access or model does not exist.' });
    }
    return response;
  }
);
