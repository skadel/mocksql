import { createAsyncThunk } from '@reduxjs/toolkit';
import {
  Project
} from '../utils/types';
import { apiRequest } from "./utils";


export const fetchProjectById = createAsyncThunk(
  'project/fetchProjectById',
  async (projectId: string, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/project/${projectId}`;
    return apiRequest<string>({
      url,
      method: 'GET',
      defaultFailureMessage: `Failed to fetch project: ${projectId}`,
      dispatch,
      rejectWithValue,
    });
  }
);

export const addProject = createAsyncThunk(
  'projects/addProject',
  async (project: Project, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/projects`;
    return apiRequest<Project>({
      url,
      method: 'POST',
      body: project,
      defaultFailureMessage: 'Failed to add project',
      dispatch,
      rejectWithValue,
    });
  }
);

export const deleteProject = createAsyncThunk(
  'projects/deleteProject',
  async (projectId: string, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/projects/${projectId}`;
    return apiRequest<string>({
      url,
      method: 'DELETE',
      defaultFailureMessage: `Error deleting project: ${projectId}`,
      dispatch,
      rejectWithValue,
    });
  }
);

export const deleteProjectTable = createAsyncThunk(
  'projects/deleteProjectTable',
  async (
    { projectId, tableName }: { projectId: string; tableName: string },
    { rejectWithValue, dispatch }
  ) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/projects/${projectId}/table/${tableName}`;
    return apiRequest<string>({
      url,
      method: 'DELETE',
      defaultFailureMessage: `Erreur lors de la suppression de la table "${tableName}" du projet "${projectId}"`,
      dispatch,
      rejectWithValue,
    });
  }
);

export const fetchProjects = createAsyncThunk(
  'projects/fetchProjects',
  async (user_sub: string | undefined, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/projects`;
    return apiRequest<Project[]>({
      url,
      method: 'GET',
      defaultFailureMessage: 'Failed to fetch projects',
      dispatch,
      rejectWithValue,
    });
  }
);


export const shareProject = createAsyncThunk<
  string[],
  { project: string; target: string } // arg type
>(
  'projects/shareProject',
  async ({ project, target }, { rejectWithValue, dispatch }) => {
    const url = `${process.env.REACT_APP_BACKEND_URL}/api/projects/share`;
    return apiRequest<Project>({
      url,
      method: 'POST',
      body: { project, target },
      dispatch,
      rejectWithValue,
    });
  }
);

