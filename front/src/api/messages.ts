import { createAsyncThunk } from '@reduxjs/toolkit';
import { apiRequest } from "./utils";


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
      url: `${process.env.REACT_APP_BACKEND_URL}/api/models/sql`,
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
      url: `${process.env.REACT_APP_BACKEND_URL}/api/models/tests`,
      method: 'PATCH',
      body: { sessionId, tests },
      defaultFailureMessage: 'Failed to update tests',
      dispatch,
      rejectWithValue,
    });
    return tests;
  }
);


export const getMessages = createAsyncThunk(
  'models/getMessages',
  async (
    { modelId, t }: { modelId: string; t: Function },
    { rejectWithValue, dispatch }
  ) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/getMessages`;
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
