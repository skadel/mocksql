import {
  statusToVerdict,
  verdictText,
  testExecStatus,
  getVerdictInfo,
  isAwaitingEvaluation,
  VERDICT_META,
} from './verdict';

// ---------------------------------------------------------------------------
// statusToVerdict
// ---------------------------------------------------------------------------

describe('statusToVerdict', () => {
  it('returns good for complete', () => {
    expect(statusToVerdict('complete')).toBe('good');
  });

  it('returns bad for error', () => {
    expect(statusToVerdict('error')).toBe('bad');
  });

  it('returns pending when status is undefined', () => {
    expect(statusToVerdict(undefined)).toBe('pending');
  });

  it('returns pending for unknown status', () => {
    expect(statusToVerdict('running')).toBe('pending');
  });

  describe('empty_results', () => {
    it('returns warn when test does not expect empty output', () => {
      expect(statusToVerdict('empty_results', { unit_test_description: 'vérifie le chemin nominal' })).toBe('warn');
    });

    it('returns good when test expects 0 lignes', () => {
      expect(statusToVerdict('empty_results', { unit_test_description: 'retourne 0 ligne quand aucune commande' })).toBe('good');
    });

    it('returns good when test expects aucune ligne', () => {
      expect(statusToVerdict('empty_results', { unit_test_description: 'aucune ligne retournée' })).toBe('good');
    });

    it('returns good when test expects résultats vides', () => {
      expect(statusToVerdict('empty_results', { unit_test_description: 'le résultat est vide' })).toBe('good');
    });

    it('returns good when test describes retourne ... vide', () => {
      expect(statusToVerdict('empty_results', { unit_test_description: 'retourne un ensemble vide pour un utilisateur inconnu' })).toBe('good');
    });

    it('returns warn when test has no description', () => {
      expect(statusToVerdict('empty_results', {})).toBe('warn');
    });
  });

  describe('evaluation override', () => {
    it('returns good when evaluation contains Excellent', () => {
      expect(statusToVerdict('empty_results', { evaluation: '**Excellent** — Couvre le cas nominal.' })).toBe('good');
    });

    it('returns good when evaluation contains Bon', () => {
      expect(statusToVerdict('error', { evaluation: '**Bon** — Données valides.' })).toBe('good');
    });

    it('returns warn when evaluation contains Insuffisant', () => {
      expect(statusToVerdict('complete', { evaluation: '**Insuffisant** — Données trop simples.' })).toBe('warn');
    });

    it('falls through to status logic when evaluation is absent', () => {
      expect(statusToVerdict('complete', { evaluation: '' })).toBe('good');
    });
  });

  describe('validation state', () => {
    it('returns validation for needs_validation reason_type', () => {
      expect(statusToVerdict('complete', { reason_type: 'needs_validation', evaluation: '**Insuffisant** — ...' })).toBe('validation');
    });

    it('returns validation for bad_description reason_type', () => {
      expect(statusToVerdict('complete', { reason_type: 'bad_description', evaluation: '**Insuffisant** — ...' })).toBe('validation');
    });

    it('returns validation for bad_input_description reason_type', () => {
      expect(statusToVerdict('complete', { reason_type: 'bad_input_description', evaluation: '**Insuffisant** — ...' })).toBe('validation');
    });

    it('takes priority over evaluation field', () => {
      // reason_type doit primer sur l'évaluation LLM pour éviter d'afficher « Insuffisant »
      expect(statusToVerdict('complete', { reason_type: 'needs_validation', evaluation: '**Insuffisant** — écart cardinalité.' })).toBe('validation');
    });

    it('does not return validation when reason_type is absent', () => {
      expect(statusToVerdict('complete', { evaluation: '**Insuffisant** — Données trop simples.' })).toBe('warn');
    });
  });
});

// ---------------------------------------------------------------------------
// verdictText
// ---------------------------------------------------------------------------

describe('verdictText', () => {
  it('returns evaluation text when test has evaluation', () => {
    const evaluation = '**Bon** — Couvre la jointure.';
    expect(verdictText('error', { evaluation })).toBe(evaluation);
  });

  // Textes de repli localisés — langue par défaut du produit : anglais.
  it('returns complete message for complete status', () => {
    const text = verdictText('complete');
    expect(text).toContain('results');
    expect(text).toContain('valid');
  });

  it('returns error message for error status', () => {
    const text = verdictText('error');
    expect(text).toContain('failed');
  });

  it('returns "expected empty" message when test expects empty', () => {
    const text = verdictText('empty_results', { unit_test_description: '0 ligne attendue' });
    expect(text).toContain('no rows');
    expect(text).toContain('valid');
  });

  it('returns warning message for unexpected empty_results', () => {
    const text = verdictText('empty_results', { unit_test_description: 'cas nominal' });
    expect(text).toContain('Check');
  });

  it('returns pending message for undefined status', () => {
    expect(verdictText(undefined)).toContain('Running');
  });
});

// ---------------------------------------------------------------------------
// testExecStatus
// ---------------------------------------------------------------------------

describe('testExecStatus', () => {
  it('returns pass for complete', () => {
    expect(testExecStatus({ status: 'complete' })).toBe('pass');
  });

  it('returns fail for error', () => {
    expect(testExecStatus({ status: 'error' })).toBe('fail');
  });

  it('returns pending when status is absent', () => {
    expect(testExecStatus({})).toBe('pending');
  });

  it('returns pass for empty_results when test expects empty', () => {
    expect(testExecStatus({ status: 'empty_results', unit_test_description: '0 ligne attendue' })).toBe('pass');
  });

  it('returns fail for empty_results when test does not expect empty', () => {
    expect(testExecStatus({ status: 'empty_results', unit_test_description: 'chemin nominal' })).toBe('fail');
  });

  // Régression : le juge a validé le test (« Bon »/« Excellent ») mais la description
  // ne matche pas le regex testExpectsEmpty → exec doit suivre le verdict (pass),
  // sinon le header affiche « 1 en échec » alors que la carte est verte « Bon ».
  it('returns pass for empty_results when judge verdict is Bon', () => {
    expect(testExecStatus({ status: 'empty_results', evaluation: '**Bon** — le résultat vide est attendu.' })).toBe('pass');
  });

  it('returns pass for empty_results when judge verdict is Excellent', () => {
    expect(testExecStatus({ status: 'empty_results', evaluation: '**Excellent** — couvre le filtre sans correspondance.' })).toBe('pass');
  });

  it('still returns fail for empty_results when judge verdict is Insuffisant', () => {
    expect(testExecStatus({ status: 'empty_results', evaluation: '**Insuffisant** — aucune ligne mais cas nominal attendu.', unit_test_description: 'chemin nominal' })).toBe('fail');
  });
});

// ---------------------------------------------------------------------------
// getVerdictInfo
// ---------------------------------------------------------------------------

describe('getVerdictInfo', () => {
  it('returns consistent verdict, meta, text and execStatus for complete', () => {
    const info = getVerdictInfo({ status: 'complete' });
    expect(info.verdict).toBe('good');
    expect(info.execStatus).toBe('pass');
    expect(info.fg).toBe(VERDICT_META.good.fg);
    expect(info.label).toBe(VERDICT_META.good.label);
    expect(typeof info.text).toBe('string');
    expect(info.text.length).toBeGreaterThan(0);
  });

  it('returns bad verdict and fail exec for error', () => {
    const info = getVerdictInfo({ status: 'error' });
    expect(info.verdict).toBe('bad');
    expect(info.execStatus).toBe('fail');
    expect(info.bg).toBe(VERDICT_META.bad.bg);
  });

  it('returns warn verdict and fail exec for unexpected empty', () => {
    const info = getVerdictInfo({ status: 'empty_results', unit_test_description: 'chemin nominal' });
    expect(info.verdict).toBe('warn');
    expect(info.execStatus).toBe('fail');
  });

  it('returns good verdict and pass exec for expected empty', () => {
    const info = getVerdictInfo({ status: 'empty_results', unit_test_description: 'aucune ligne retournée' });
    expect(info.verdict).toBe('good');
    expect(info.execStatus).toBe('pass');
  });

  it('returns pending info when test has no status', () => {
    const info = getVerdictInfo({});
    expect(info.verdict).toBe('pending');
    expect(info.execStatus).toBe('pending');
    expect(info.fg).toBe(VERDICT_META.pending.fg);
  });
});

// ---------------------------------------------------------------------------
// isAwaitingEvaluation
// ---------------------------------------------------------------------------

// Régression : pendant un run, un test exécuté (complete) sans verdict LLM affichait
// le badge « Bon » optimiste dérivé du statut, à côté du spinner « Évaluation… ».
describe('isAwaitingEvaluation', () => {
  it('returns true when execution finished but no LLM verdict yet during a run', () => {
    expect(isAwaitingEvaluation({ status: 'complete' }, true)).toBe(true);
  });

  it('applies to error and empty_results too (verdict exec tout aussi provisoire)', () => {
    expect(isAwaitingEvaluation({ status: 'error' }, true)).toBe(true);
    expect(isAwaitingEvaluation({ status: 'empty_results' }, true)).toBe(true);
  });

  it('returns false once the LLM verdict arrived', () => {
    expect(isAwaitingEvaluation({ status: 'complete', evaluation: '**Bon** — ok.' }, true)).toBe(false);
  });

  it('returns false outside a run (modèle rechargé sans champ evaluation)', () => {
    expect(isAwaitingEvaluation({ status: 'complete' }, false)).toBe(false);
  });

  it('returns false while still pending or without status (autre spinner déjà affiché)', () => {
    expect(isAwaitingEvaluation({ status: 'pending' }, true)).toBe(false);
    expect(isAwaitingEvaluation({}, true)).toBe(false);
  });

  it('returns false for validation reason_type (l\'évaluateur a déjà tranché)', () => {
    expect(isAwaitingEvaluation({ status: 'complete', reason_type: 'needs_validation' }, true)).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// VERDICT_META completeness
// ---------------------------------------------------------------------------

describe('VERDICT_META', () => {
  const verdicts = ['good', 'warn', 'bad', 'pending', 'validation'] as const;
  it.each(verdicts)('has label, fg, bg, border for %s', (v) => {
    const meta = VERDICT_META[v];
    expect(meta.label).toBeTruthy();
    expect(meta.fg).toBeTruthy();
    expect(meta.bg).toBeTruthy();
    expect(meta.border).toBeTruthy();
  });
});
