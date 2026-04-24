import { createAsyncThunk } from '@reduxjs/toolkit';
import {
    Model
} from '../utils/types';
import { apiRequest } from "./utils";


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
    async (
      { currentProjectId }: { currentProjectId: string | undefined },
      { rejectWithValue, dispatch }
    ) => {
      const url = `${process.env.REACT_APP_BACKEND_URL}/api/models?project_id=${currentProjectId}`;
      return apiRequest<any[]>({
        url,
        method: 'GET',
        defaultFailureMessage: 'Failed to fetch models',
        dispatch,
        rejectWithValue,
      });
    }
  );
  