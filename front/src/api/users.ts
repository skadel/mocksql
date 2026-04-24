import { createAsyncThunk } from '@reduxjs/toolkit';

const API_BASE = process.env.REACT_APP_BACKEND_URL;

/**
 * Fetch the current authenticated user. If unauthenticated, open login dialog.
 */
export const fetchCurrentUser = createAsyncThunk(
  'user/fetchCurrentUser',
  async (_, thunkAPI) => {
    try {
      const token = localStorage.getItem('jwt');
      const response = await fetch(`${API_BASE}/api/current-user`, {
        method: 'GET',
        headers: token
          ? { 'Authorization': `Bearer ${token}` }
          : undefined,
      });
      if (response.ok) {
        const user = await response.json();
        return user;
      } else {
        return thunkAPI.rejectWithValue('Unauthorized');
      }
    } catch (error) {
      console.error('Failed to fetch current user', error);
      return thunkAPI.rejectWithValue('Network error');
    }
  }
);

/**
 * Fetch a list of users matching the given query string.
 * Used for sharing models via autocomplete.
 */
export const fetchUsers = createAsyncThunk(
  'user/fetchUsers',
  async (
    params: { query: string },
    thunkAPI
  ) => {
    try {
      const token = localStorage.getItem('jwt');
      const { query } = params;
      const url = new URL(`${API_BASE}/api/users/search`);
      url.searchParams.set('q', query);
      const response = await fetch(url.toString(), {
        method: 'GET',
        headers: {
          'Authorization': token ? `Bearer ${token}` : '',
          'Content-Type': 'application/json'
        },
      });
      if (!response.ok) {
        console.error('Error fetching users:', response.statusText);
        return [];
      }
      const users = await response.json();
      // Expect users as array of { id: string, name: string }
      return users;
    } catch (error) {
      console.error('Failed to fetch users', error);
      return [];
    }
  }
);
