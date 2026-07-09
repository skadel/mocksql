import React from 'react';
import { createRoot } from 'react-dom/client';
import { Provider } from 'react-redux';
import { store } from './app/store';
import { I18nextProvider } from 'react-i18next';
import App from './App';
import i18n, { applyBackendLanguage } from './i18n';
import { fetchConfig } from './api/models';
import './style/tokens.css';
import 'prismjs/themes/prism.css';

// Langue par défaut de l'UI = celle de mocksql.yml (défaut anglais), sauf choix
// explicite de l'utilisateur déjà persisté en localStorage (cf. applyBackendLanguage).
fetchConfig().then((config) => applyBackendLanguage(config?.language));

// Get the root container
const container = document.getElementById('root');

// Ensure container is non-null
if (!container) {
  throw new Error("Root container 'root' not found in the DOM");
}

// Create a root using ReactDOM's createRoot
const root = createRoot(container);

// Render the application
root.render(
  <React.StrictMode>
    <I18nextProvider i18n={i18n}>
      <Provider store={store}>
        <App />
      </Provider>
    </I18nextProvider>
  </React.StrictMode>
);
