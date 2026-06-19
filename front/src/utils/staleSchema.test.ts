import { describe, it, expect } from 'vitest';
import { isStaleSchemaError } from './staleSchema';

describe('isStaleSchemaError', () => {
  it('matche le banner court "Unknown column … schéma en cache probablement périmé"', () => {
    expect(
      isStaleSchemaError('Unknown column: no_contrat_commercant (schéma en cache probablement périmé)')
    ).toBe(true);
  });

  it('matche le message long poussant vers refresh-schemas', () => {
    expect(
      isStaleSchemaError("La colonne `x` est introuvable. Rafraîchis le schéma :\n  mocksql refresh-schemas")
    ).toBe(true);
  });

  it('matche la variante "périmé ou incomplet"', () => {
    expect(isStaleSchemaError('schéma en cache **périmé ou incomplet**')).toBe(true);
  });

  it('ne matche pas une erreur générique', () => {
    expect(isStaleSchemaError('Erreur de syntaxe SQL à la ligne 3')).toBe(false);
  });

  it('gère null / undefined / chaîne vide', () => {
    expect(isStaleSchemaError(null)).toBe(false);
    expect(isStaleSchemaError(undefined)).toBe(false);
    expect(isStaleSchemaError('')).toBe(false);
  });
});
