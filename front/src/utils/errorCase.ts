import { PayloadAction, SerializedError } from '@reduxjs/toolkit';
import { AppBarState, BuildModelState, ProjectState } from './types';


export function handleRejectedCase(
    state: BuildModelState,
    action: PayloadAction<
        unknown,
        string,
        any,
        SerializedError
    >,
    defaultMessage: string
) {
    // Reset loading indicators
    state.loading = false;
    state.loading_message = undefined;

    // Handle unauthorized errors (401)
    if (
        action.payload &&
        typeof action.payload === "object" &&
        "status" in action.payload &&
        (action.payload as any).status === 401
    ) {
        state.error = "Please login to continue";
        return;
    }

    // Handle other errors
    const detail =
        action.payload && typeof action.payload === "object" && "detail" in action.payload
            ? (action.payload as any).detail
            : null;

    state.error = detail
        ? `${action.error?.message || defaultMessage} details: ${detail}`
        : `${action.error?.message || defaultMessage}`;
}

export function handleRejectedCaseAppBar(
    state: AppBarState,
    action: PayloadAction<
        unknown,
        string,
        any,
        SerializedError
    >,
    defaultMessage: string
) {
    // Reset loading indicators
    state.loadingAppBar = false;
    state.loadingSaveModel = false;
    state.loadingSaveModelMessage = undefined;

    // Handle unauthorized errors (401)
    if (
        action.payload &&
        typeof action.payload === "object" &&
        "status" in action.payload &&
        (action.payload as any).status === 401
    ) {
        state.error = "Please login to continue";
        return;
    }

    // Handle other errors
    const detail =
        action.payload && typeof action.payload === "object" && "detail" in action.payload
            ? (action.payload as any).detail
            : null;

    state.error = detail
        ? `${action.error?.message || defaultMessage} details: ${detail}`
        : `${action.error?.message || defaultMessage}`;
}

export function handleRejectedCaseProject(
    state: ProjectState,
    action: PayloadAction<
        unknown,
        string,
        any,
        SerializedError
    >,
    defaultMessage: string
) {
    // Reset loading indicators
    state.loading = false;

    // Handle unauthorized errors (401)
    if (
        action.payload &&
        typeof action.payload === "object" &&
        "status" in action.payload &&
        (action.payload as any).status === 401
    ) {
        state.error = "Please login to continue";
        return;
    }

    // Handle other errors
    const detail =
        action.payload && typeof action.payload === "object" && "detail" in action.payload
            ? (action.payload as any).detail
            : null;

    state.error = detail
        ? `${action.error?.message || defaultMessage} details: ${detail}`
        : `${action.error?.message || defaultMessage}`;
}


type SetErrorAction = (message: string) => void;
type UserLoginErroAction = (message: string) => void;
type SetDialogAction = (isOpen: boolean) => void;

const errorActions = (
    setProjectCreationError: SetErrorAction,
    userLoginErroAction: UserLoginErroAction,
    setOpenLoginDialog: SetDialogAction
) => ({
  'Not authenticated': () => {
    userLoginErroAction('Vous n’êtes pas authentifié.');
  },
  'Token has expired': () => {
    userLoginErroAction('Votre session a expiré. Veuillez vous reconnecter.');
  },
  'Token signature invalid': () => {
    userLoginErroAction('La signature du jeton est invalide.');
  },
  'Token decode error: Not enough segments': () => {
    userLoginErroAction('Votre session a expiré. Veuillez vous reconnecter.');
  },
  'Unknown error': () => {
    setProjectCreationError('Une erreur inconnue s’est produite.');
  }
});

export const handleError = (
  errorKey: string | undefined,
  setProjectCreationError: SetErrorAction,
  userLoginErroAction: UserLoginErroAction,
  setOpenLoginDialog: SetDialogAction
) => {
  const actions = errorActions(setProjectCreationError, userLoginErroAction, setOpenLoginDialog);

  const action = actions[errorKey as keyof typeof actions] || (() => {
    setProjectCreationError(errorKey || 'Une erreur inattendue s’est produite.');
  });

  action();
};
