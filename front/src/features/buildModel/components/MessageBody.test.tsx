import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import type { Message } from '../../../utils/types';

// DisplayTable importe le barrel `@mui/icons-material` (lourd, et cassé dans
// l'env de test) ; on le stubbe car le chemin d'erreur testé ne l'utilise pas.
vi.mock('./DisplayTable', () => ({ default: () => null }));

import MessageBody from './MessageBody';

const errorMessage = (error: string): Message => ({
  id: 'm1',
  type: 'bot',
  contentType: 'error',
  contents: { error },
});

const STALE = `La colonne \`amount\` est référencée dans le SQL mais introuvable dans le schéma en cache. C'est généralement le signe d'un schéma en cache **périmé ou incomplet**.\n\nRafraîchis le schéma puis relance la génération :\n  mocksql refresh-schemas`;

describe('MessageBody — bouton "Rafraîchir le schéma" sur erreur de schéma périmé', () => {
  it('affiche le bouton quand l\'erreur signale un schéma périmé et qu\'un handler est fourni', () => {
    render(<MessageBody msg={errorMessage(STALE)} onRefreshSchemas={vi.fn()} />);
    expect(screen.getByRole('button', { name: /Rafraîchir le schéma/i })).toBeInTheDocument();
  });

  it('appelle onRefreshSchemas au clic', () => {
    const onRefreshSchemas = vi.fn();
    render(<MessageBody msg={errorMessage(STALE)} onRefreshSchemas={onRefreshSchemas} />);
    fireEvent.click(screen.getByRole('button', { name: /Rafraîchir le schéma/i }));
    expect(onRefreshSchemas).toHaveBeenCalledTimes(1);
  });

  it('n\'affiche pas le bouton pour une erreur générique', () => {
    render(<MessageBody msg={errorMessage('Erreur de syntaxe SQL à la ligne 3')} onRefreshSchemas={vi.fn()} />);
    expect(screen.queryByRole('button', { name: /Rafraîchir le schéma/i })).not.toBeInTheDocument();
  });

  it('n\'affiche pas le bouton si aucun handler n\'est fourni', () => {
    render(<MessageBody msg={errorMessage(STALE)} />);
    expect(screen.queryByRole('button', { name: /Rafraîchir le schéma/i })).not.toBeInTheDocument();
  });
});
