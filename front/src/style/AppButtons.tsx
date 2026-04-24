/**
 * AppButtons — composants boutons standardisés
 *
 * Palette :
 *   Primary  : #1ca8a4  hover #159e9a
 *   Danger   : #f44336  hover #d32f2f
 *   Warning  : #ed6c02  hover #e65100
 *   Neutral  : #6c757d  hover #e9ecef (bg)
 *
 * Usage :
 *   import { PrimaryButton, GhostButton, TealIconButton, ... } from '../../../style/AppButtons';
 */

import { Button, IconButton } from '@mui/material';
import { styled } from '@mui/material/styles';

// ─── Couleurs partagées ────────────────────────────────────────────────────────
const primary     = '#1ca8a4';
const primaryHover = '#159e9a';
const primaryLight = '#e8f7f6';

// ─── Boutons plein (contained) ─────────────────────────────────────────────────

/** CTA principal — fond teal */
export const PrimaryButton = styled(Button)({
  backgroundColor: primary,
  color: '#fff',
  borderRadius: 8,
  textTransform: 'none',
  fontWeight: 600,
  '&:hover': { backgroundColor: primaryHover },
  '&.Mui-disabled': { backgroundColor: '#ccc', color: '#fff' },
});
PrimaryButton.defaultProps = { variant: 'contained' };

/** Bouton destructeur — fond rouge */
export const DangerButton = styled(Button)({
  backgroundColor: '#f44336',
  color: '#fff',
  borderRadius: 8,
  textTransform: 'none',
  fontWeight: 600,
  '&:hover': { backgroundColor: '#d32f2f' },
});
DangerButton.defaultProps = { variant: 'contained' };

/** Bouton avertissement — fond orange */
export const WarningContainedButton = styled(Button)({
  backgroundColor: '#ed6c02',
  color: '#fff',
  borderRadius: 8,
  textTransform: 'none',
  fontWeight: 500,
  '&:hover': { backgroundColor: '#e65100' },
  '&.Mui-disabled': { backgroundColor: '#ccc', color: '#fff' },
});
WarningContainedButton.defaultProps = { variant: 'contained' };

// ─── Boutons contour (outlined) ────────────────────────────────────────────────

/** Action secondaire — contour teal */
export const OutlinedPrimaryButton = styled(Button)({
  borderColor: primary,
  color: primary,
  borderRadius: 8,
  textTransform: 'none',
  fontWeight: 600,
  '&:hover': { borderColor: primaryHover, backgroundColor: primaryLight },
});
OutlinedPrimaryButton.defaultProps = { variant: 'outlined' };

/** Bouton avertissement contour — orange */
export const WarningOutlinedButton = styled(Button)({
  borderColor: '#ed6c02',
  color: '#ed6c02',
  borderRadius: 8,
  textTransform: 'none',
  '&:hover': { borderColor: '#e65100', backgroundColor: '#fff3e0' },
});
WarningOutlinedButton.defaultProps = { variant: 'outlined' };

/** Bouton neutre/skip — contour gris */
export const NeutralButton = styled(Button)({
  borderColor: '#ddd',
  color: '#999',
  borderRadius: 8,
  textTransform: 'none',
  fontWeight: 600,
  '&:hover': { borderColor: '#aaa', color: '#555', backgroundColor: 'transparent' },
});
NeutralButton.defaultProps = { variant: 'outlined' };

// ─── Boutons texte (ghost) ─────────────────────────────────────────────────────

/** Annuler / Fermer — texte teal, sans bordure */
export const GhostButton = styled(Button)({
  color: primary,
  textTransform: 'none',
  fontWeight: 500,
  '&:hover': { backgroundColor: primaryLight },
});
GhostButton.defaultProps = { variant: 'text' };

/** Annuler neutre — texte gris */
export const NeutralGhostButton = styled(Button)({
  color: '#6c757d',
  textTransform: 'none',
  '&:hover': { backgroundColor: '#e9ecef' },
});
NeutralGhostButton.defaultProps = { variant: 'text' };

// ─── IconButtons ────────────────────────────────────────────────────────────────

/** Icône teal (historique, info, restauration…) */
export const TealIconButton = styled(IconButton)({
  color: primary,
  padding: '2px',
});

/** Icône grise atténuée (édition, actions secondaires) */
export const MutedIconButton = styled(IconButton)({
  color: '#aaa',
  padding: '2px',
});

/** Icône rouge (suppression) */
export const DangerIconButton = styled(IconButton)({
  color: '#e57373',
  padding: '2px',
});

/** Icône sur fond sombre (blocs de code) */
export const CodeBlockIconButton = styled(IconButton)({
  color: '#f8f8f2',
  backgroundColor: '#333',
  '&:hover': { backgroundColor: '#444' },
});
