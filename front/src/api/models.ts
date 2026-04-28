import { createAsyncThunk } from '@reduxjs/toolkit';
import {
    Model
} from '../utils/types';
import { apiRequest } from "./utils";

export interface SqlFile {
  name: string;
  path: string;
  session_id?: string;
  updated_at?: string;
  test_name?: string;
}

export interface TestSession {
  test_id: string;
  model_name: string;
  sql: string;
  optimized_sql: string;
  test_cases: any[];
  created_at: string;
  updated_at: string;
}

export const fetchSqlFiles = async (): Promise<SqlFile[]> => {
  const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/models`);
  if (!response.ok) return [];
  return response.json();
};

export const fetchModelSql = async (modelName: string): Promise<string | null> => {
  const response = await fetch(
    `${process.env.REACT_APP_BACKEND_URL}/api/models/sql?name=${encodeURIComponent(modelName)}`,
  );
  if (!response.ok) return null;
  const data = await response.json();
  return data.sql ?? null;
};

export const createTestApi = async (modelName: string): Promise<TestSession> => {
  const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/tests`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ model_name: modelName }),
  });
  if (!response.ok) throw await response.json();
  return response.json();
};

export const getTestsByModelName = async (modelName: string): Promise<TestSession[]> => {
  const response = await fetch(`${process.env.REACT_APP_BACKEND_URL}/api/tests?model_name=${encodeURIComponent(modelName)}`);
  if (!response.ok) return [];
  return response.json();
};


// ------------------- Thunks -------------------

export const createModel = createAsyncThunk(
  'models/createModel',
  async (model: Model, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/models`;
    return apiRequest<Model>({
      url,
      method: 'POST',
      body: { model },
      defaultFailureMessage: 'Failed to add the model',
      dispatch,
      rejectWithValue,
    });
  }
);
export const updateModel = createAsyncThunk(
  'models/addModel',
  async (model: Model, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/models`;
    return apiRequest<Model>({
      url,
      method: 'POST',
      body: { model },
      defaultFailureMessage: 'Failed to add the model',
      dispatch,
      rejectWithValue,
    });
  }
);

export const deleteModel = createAsyncThunk(
  'models/deleteModel',
  async (modelId: string, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/models/${modelId}`;
    return apiRequest<string>({
      url,
      method: 'DELETE',
      defaultFailureMessage: `Failed to delete model: ${modelId}`,
      dispatch,
      rejectWithValue,
    });
  }
);


export const fetchModels = createAsyncThunk(
    'models/fetchModels',
    async (_: void, { rejectWithValue, dispatch }) => {
      const url = `${process.env.REACT_APP_BACKEND_URL}/api/models`;
      return apiRequest<SqlFile[]>({
        url,
        method: 'GET',
        defaultFailureMessage: 'Failed to fetch models',
        dispatch,
        rejectWithValue,
      });
    }
  );
  