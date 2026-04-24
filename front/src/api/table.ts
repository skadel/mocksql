import { createAsyncThunk } from '@reduxjs/toolkit';
import {
    RejectValue
} from '../utils/types';
import { apiRequest } from "./utils";

export const getTableChanges = createAsyncThunk(
  'query/getTableChanges',
  async (
    { modelId, currentProjectId }: { modelId: string; currentProjectId: string },
    { dispatch, rejectWithValue }
  ) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/getTableChanges`;
    return apiRequest<any[]>({
      url,
      method: 'POST',
      body: { session: modelId, project: currentProjectId },
      defaultFailureMessage: 'Failed to retrieve table changes',
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchUniqueColumns = createAsyncThunk<
  any, // Ajustez le type de retour si besoin
  { modelId: string; currentProjectId: string; id?: string },
  { rejectValue: RejectValue }
>(
  'query/fetchUniqueColumns',
  async ({ modelId, currentProjectId, id }, { dispatch, rejectWithValue }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/uniqueColumns`;

    // On ne met id que s'il est défini
    const body: Record<string, any> = {
      session: modelId,
      project: currentProjectId,
      ...(id && { id }),
    };

    return apiRequest<any>({
      url,
      method: 'POST',
      body,
      defaultFailureMessage: 'Failed to fetch unique columns',
      dispatch,
      rejectWithValue,
    });
  }
);


export const saveTableDetails = createAsyncThunk(
  'table/saveTableDetails',
  async (tableData: any, { dispatch, rejectWithValue }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/saveTableDetails`;
    return apiRequest<any>({
      url,
      method: 'POST',
      body: { ...tableData },
      defaultFailureMessage: 'Failed to save table details',
      dispatch,
      rejectWithValue,
    });
  }
);


export const fetchListTablesAndDatasets = createAsyncThunk<
  any, // Adjust type as needed
  { inputValue?: string },
  { rejectValue: RejectValue }
>(
  'query/fetchListTablesAndDatasets',
  async ({ inputValue }, { dispatch, rejectWithValue }) => {
    let url = `${process.env.REACT_APP_BACKEND_URL}/api/list-datasets-and-tables`;
    if (inputValue) {
      const queryParams = new URLSearchParams({ input_value: inputValue }).toString();
      url = `${url}?${queryParams}`;
    }
    return apiRequest<any>({
      url,
      method: 'GET',
      defaultFailureMessage: 'Failed to fetch datasets and tables',
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchPostgresTables = createAsyncThunk<
  any, // Adjust type as needed
  { project?: string, inputValue?: string },
  { rejectValue: RejectValue }
>(
  'query/fetchPostgresTables',
  async ({ project, inputValue }, { dispatch, rejectWithValue }) => {
    let url = `${process.env.REACT_APP_BACKEND_URL}/api/list-postgres-tables`;
    if (inputValue || project) {
      const params = new URLSearchParams();
      if (inputValue) {
        params.append('input_value', inputValue);
      }
      if (project) {
        params.append('project', project);
      }
      url += `?${params.toString()}`;
    }

    return apiRequest<any>({
      url,
      method: 'GET',
      defaultFailureMessage: 'Failed to fetch datasets and tables',
      dispatch,
      rejectWithValue,
    });
  }
);
export const fetchMotherDuckTables = createAsyncThunk<
  any, // Adjust type as needed
  { project?: string, inputValue?: string },
  { rejectValue: RejectValue }
>(
  'query/fetchMotherDuckTables',
  async ({ project, inputValue }, { dispatch, rejectWithValue }) => {
    let url = `${process.env.REACT_APP_BACKEND_URL}/api/list-duckdb-tables`;
    if (inputValue || project) {
      const params = new URLSearchParams();
      if (inputValue) {
        params.append('input_value', inputValue);
      }
      if (project) {
        params.append('project', project);
      }
      url += `?${params.toString()}`;
    }

    return apiRequest<any>({
      url,
      method: 'GET',
      defaultFailureMessage: 'Failed to fetch datasets and tables',
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchSchema = createAsyncThunk<
  any, // Adjust type as needed
  { inputs: string[] },
  { rejectValue: RejectValue }
>(
  'query/fetchSchema',
  async ({ inputs }, { dispatch, rejectWithValue }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/schema`;
    return apiRequest<any>({
      url,
      method: 'POST',
      body: { inputs },
      defaultFailureMessage: 'Failed to fetch schema',
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchPGSchema = createAsyncThunk<
  any,
  { inputs: string[], projectId: string },
  { rejectValue: RejectValue }
>(
  'query/fetchPgSchema',
  async ({ inputs, projectId }, { dispatch, rejectWithValue }) => {
    let url = `${process.env.REACT_APP_BACKEND_URL}/api/pg-schema`;
    if (projectId) {
      const params = new URLSearchParams();
      params.append('project', projectId);
      url += `?${params.toString()}`;
    }
    return apiRequest<any>({
      url,
      method: 'POST',
      body: { inputs },
      defaultFailureMessage: 'Failed to fetch schema',
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchDuckDBSchema = createAsyncThunk<
  any,
  { inputs: string[], projectId: string },
  { rejectValue: RejectValue }
>(
  'query/fetchDuckSchema',
  async ({ inputs, projectId }, { dispatch, rejectWithValue }) => {
    let url = `${process.env.REACT_APP_BACKEND_URL}/api/duckdb-schema`;
    if (projectId) {
      const params = new URLSearchParams();
      params.append('project', projectId);
      url += `?${params.toString()}`;
    }
    return apiRequest<any>({
      url,
      method: 'POST',
      body: { inputs },
      defaultFailureMessage: 'Failed to fetch schema',
      dispatch,
      rejectWithValue,
    });
  }
);