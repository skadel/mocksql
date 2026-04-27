import {
  statusToVerdict,
  verdictText,
  testExecStatus,
  getVerdictInfo,
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
});

// ---------------------------------------------------------------------------
// verdictText
// ---------------------------------------------------------------------------

describe('verdictText', () => {
  it('returns evaluation text when test has evaluation', () => {
    const evaluation = '**Bon** — Couvre la jointure.';
    expect(verdictText('error', { evaluation })).toBe(evaluation);
  });

  it('returns complete message for complete status', () => {
    const text = verdictText('complete');
    expect(text).toContain('résultats');
    expect(text).toContain('valide');
  });

  it('returns error message for error status', () => {
    const text = verdictText('error');
    expect(text).toContain('échoué');
  });

  it('returns "expected empty" message when test expects empty', () => {
    const text = verdictText('empty_results', { unit_test_description: '0 ligne attendue' });
    expect(text).toContain('aucune ligne');
    expect(text).toContain('valide');
  });

  it('returns warning message for unexpected empty_results', () => {
    const text = verdictText('empty_results', { unit_test_description: 'cas nominal' });
    expect(text).toContain("Vérifiez");
  });

  it('returns pending message for undefined status', () => {
    expect(verdictText(undefined)).toContain("cours");
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
// VERDICT_META completeness
// ---------------------------------------------------------------------------

describe('VERDICT_META', () => {
  const verdicts = ['good', 'warn', 'bad', 'pending'] as const;
  it.each(verdicts)('has label, fg, bg, border for %s', (v) => {
    const meta = VERDICT_META[v];
    expect(meta.label).toBeTruthy();
    expect(meta.fg).toBeTruthy();
    expect(meta.bg).toBeTruthy();
    expect(meta.border).toBeTruthy();
  });
});
