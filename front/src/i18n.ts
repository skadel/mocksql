import i18n from 'i18next';
import { initReactI18next } from 'react-i18next';

import translationEN from './locales/en/translation.json';
import translationFR from './locales/fr/translation.json';

const resources = {
  en: {
    translation: translationEN,
  },
  fr: {
    translation: translationFR,
  },
};

// localStorage key portant le choix EXPLICITE de l'utilisateur (sélecteur de langue).
// Tant qu'il est absent, la langue par défaut vient de mocksql.yml via /api/config
// (voir applyBackendLanguage). Anglais = défaut produit.
export const LANGUAGE_STORAGE_KEY = 'mocksql_language';

const storedLanguage =
  typeof localStorage !== 'undefined'
    ? localStorage.getItem(LANGUAGE_STORAGE_KEY)
    : null;

i18n
  .use(initReactI18next)
  .init({
    resources,
    lng: storedLanguage || 'en', // défaut anglais ; surchargé par le choix utilisateur puis par mocksql.yml
    fallbackLng: 'en',
    interpolation: {
      escapeValue: false,
    },
  });

/**
 * Applique la langue par défaut renvoyée par le backend (mocksql.yml).
 * Ne s'applique QUE si l'utilisateur n'a pas déjà choisi une langue explicitement
 * (pas de clé localStorage) — son choix manuel prime toujours sur la config projet.
 */
export function applyBackendLanguage(language?: string | null): void {
  if (!language) return;
  if (localStorage.getItem(LANGUAGE_STORAGE_KEY)) return;
  if (!Object.keys(resources).includes(language)) return;
  if (i18n.language !== language) {
    i18n.changeLanguage(language);
  }
}

export default i18n;
