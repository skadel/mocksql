import { Dispatch } from '@reduxjs/toolkit';
import {
  setError,
  setLoading,
} from "../features/buildModel/buildModelSlice";
import {
  RejectValue
} from '../utils/types';
import { EventSourceMessage, fetchEventSource, FetchEventSourceInit } from '@microsoft/fetch-event-source';

/**
 * Utility to get the JWT token from localStorage
 */
function getToken(): string | null {
  return localStorage.getItem('jwt');
}

/**
 * Handle HTTP responses and throw appropriate errors.
 */
async function handleHttpResponse<T>(
  response: Response,
  defaultFailureMessage: string,
  dispatch: Dispatch,
  rejectWithValue: (value: RejectValue) => any
): Promise<T | ReturnType<typeof rejectWithValue>> {
  let responseData;
  try {
    // Attempt to parse the response body
    responseData = await response.json();
  } catch (error) {
    responseData = null; // If parsing fails, default to null
  }

  if (!response.ok) {
    if (response.status === 401) {
      return rejectWithValue({
        detail: "Votre session a expiré. Veuillez vous reconnecter pour continuer."
      });
    }
    if (response.status === 403) {
      return rejectWithValue({
        detail: "Vous n'êtes pas autorisé à effectuer cette action. Contactez un administrateur si nécessaire."
      });
    }
    return rejectWithValue(responseData || { detail: defaultFailureMessage });
  }

  // Return the parsed JSON for successful responses
  return responseData as T;
}

/**
 * Centralized API request helper
 */
export async function apiRequest<T>({
  url,
  method = 'GET',
  body,
  defaultFailureMessage = 'An error occurred',
  dispatch,
  rejectWithValue,
}: {
  url: string;
  method?: 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE';
  body?: Record<string, any>;
  defaultFailureMessage?: string;
  dispatch: Dispatch;
  rejectWithValue: (value: RejectValue) => any;
}): Promise<T | ReturnType<typeof rejectWithValue>> {
  const token = getToken();
  try {
    const headers: HeadersInit = {
      'Content-Type': 'application/json'
    };
    if (token) headers['Authorization'] = `Bearer ${token}`;

    const response = await fetch(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });

    return await handleHttpResponse<T>(
      response,
      defaultFailureMessage,
      dispatch,
      rejectWithValue
    );
  } catch (error: any) {
    if (error?.response?.status === 403) {
      return rejectWithValue({ detail: 'JWT expired' });
    }
    return rejectWithValue({ detail: error.message || defaultFailureMessage });
  }
}


export async function streamThunk(
  url: string,
  init: Omit<FetchEventSourceInit, 'onmessage'> & { onmessage: (msg: EventSourceMessage) => void },
  dispatch: any,
  controller: AbortController
) {
  try {
    await fetchEventSource(url, {
      ...init,
      signal: controller.signal,
      onmessage: init.onmessage,
      onclose() {},
      onerror(err) { throw err; }
    });
  } catch (error: any) {
      if (controller.signal.aborted) {
        dispatch(setError('Flux interrompu par l’utilisateur.'));
      } else {
        dispatch(setError("Une erreur est survenue lors de l'exécution"));
        console.error('Stream error:', error);
      }
  } finally {
    dispatch(setLoading(false));
  }
}
