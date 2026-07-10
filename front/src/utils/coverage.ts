// Single source of truth for the 6 coverage axes (v15 model).
// Consumed by both the TestsPanel CoverageGrid (UI) and HtmlExporter (static
// export) so the axis list, detection heuristics, and score stay in sync — a
// change here propagates to every surface instead of being re-replicated.

export interface CoverageAxis {
  key: string;
  label: string;
  hint: string;
}

// v15 coverage axes (cf. data.js:coverageAxes).
// Drops happy/equal/types ; adds bornes/doublons/volumetrie.
export const COVERAGE_AXES: CoverageAxis[] = [
  { key: 'null', label: 'Valeurs NULL', hint: 'colonnes manquantes / vides' },
  { key: 'vide', label: 'Fenêtre vide', hint: 'aucune ligne sur la période' },
  { key: 'ex_aequo', label: 'Ex æquo', hint: 'égalités de tri / départage' },
  { key: 'bornes', label: 'Bornes & négatifs', hint: '0, valeurs négatives, hors plage' },
  { key: 'doublons', label: 'Doublons', hint: 'lignes dupliquées en entrée' },
  { key: 'volumetrie', label: 'Volumétrie', hint: '1 ligne vs. N lignes' },
];

const AXIS_KEYS = new Set(COVERAGE_AXES.map((a) => a.key));

// Coverage prefers backend-declared axes (the v15 model: each test carries
// `axes: string[]`). Until the backend tags them, we fall back to regex
// heuristics over the test title + tags.
export function detectCoveredAxes(tests: any[]): Set<string> {
  const covered = new Set<string>();

  // 1. Trust explicit backend-declared axes when at least one test has them.
  const declared = tests.some((t) => Array.isArray(t.axes) && t.axes.length > 0);
  if (declared) {
    tests.forEach((t) =>
      (t.axes ?? []).forEach((a: string) => {
        if (AXIS_KEYS.has(a)) covered.add(a);
      }),
    );
    return covered;
  }

  // 2. Fallback heuristics on the test text.
  tests.forEach((t) => {
    const s = ((t.unit_test_description ?? '') + ' ' + (t.tags ?? []).join(' ')).toLowerCase();
    if (/null.checks|null|manquant|absent/.test(s)) covered.add('null');
    if (/vide|aucune|inexistant|0.ligne|z[ée]ro|sans.donn[ée]es|ensemble.vide|fen[êe]tre.vide|plage.vide/.test(s)) covered.add('vide');
    if (/ex.[æa]quo|\btie\b|classement|d[ée]partage|non.d[ée]terministe/.test(s)) covered.add('ex_aequo');
    if (/borne|n[ée]gatif|hors.plage|hors.borne|d[ée]bordement|overflow|valeur.limite/.test(s)) covered.add('bornes');
    if (/doublon|dupliqu|duplicate|m[êe]me.cl[ée]/.test(s)) covered.add('doublons');
    if (/volum|cardinalit|une.seule.ligne|1.ligne|plusieurs.lignes|n.lignes|grand.volume/.test(s)) covered.add('volumetrie');
  });
  return covered;
}

// Aggregate coverage numbers shared by the UI grid and the export header.
export function computeCoverage(tests: any[]): {
  covered: Set<string>;
  n: number;
  total: number;
  pct: number;
} {
  const covered = detectCoveredAxes(tests);
  const total = COVERAGE_AXES.length;
  const n = covered.size;
  const pct = total > 0 ? Math.round((n / total) * 100) : 0;
  return { covered, n, total, pct };
}
