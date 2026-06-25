export type Verdict = 'good' | 'warn' | 'bad' | 'pending' | 'validation';
export type ExecStatus = 'pass' | 'fail' | 'pending';

export interface VerdictMeta {
  label: string;
  fg: string;
  bg: string;
  border: string;
}

export const VERDICT_META: Record<Verdict, VerdictMeta> = {
  good:       { label: 'Bon',         fg: '#23a26d', bg: '#e9f7f0', border: '#23a26d' },
  warn:       { label: 'Insuffisant', fg: '#d89323', bg: '#fcf3e1', border: '#d89323' },
  bad:        { label: 'Incorrect',   fg: '#d0503f', bg: '#fbeceb', border: '#d0503f' },
  pending:    { label: 'En attente',  fg: '#888',    bg: '#f4f7f7', border: '#ccc'    },
  validation: { label: 'À valider',   fg: '#1565c0', bg: '#e8f0fd', border: '#1976d2' },
};

const VALIDATION_REASON_TYPES = new Set(['needs_validation', 'bad_description', 'bad_input_description']);

function testExpectsEmpty(test: any): boolean {
  const desc = (test.unit_test_description ?? '').toLowerCase();
  return /retourne\s+.{0,40}vide|résultat[s]?\s+(?:est\s+)?vide[s]?|0\s+ligne|aucune\s+ligne/.test(desc);
}

export function statusToVerdict(status: string | undefined, test?: any): Verdict {
  // Description désync → état neutre « À valider », ni Insuffisant ni Bon.
  if (test?.reason_type && VALIDATION_REASON_TYPES.has(test.reason_type)) return 'validation';
  if (test?.evaluation) {
    if (/Excellent|Bon/.test(test.evaluation))  return 'good';
    if (/Insuffisant/.test(test.evaluation))    return 'warn';
  }
  if (status === 'complete')      return 'good';
  if (status === 'empty_results') return (test && testExpectsEmpty(test)) ? 'good' : 'warn';
  if (status === 'error')         return 'bad';
  return 'pending';
}

export function verdictText(status: string | undefined, test?: any): string {
  if (test?.evaluation) return test.evaluation;
  if (status === 'complete')
    return "La requête a produit des résultats sur ces données d'entrée. Le test est valide.";
  if (status === 'empty_results') {
    if (test && testExpectsEmpty(test))
      return "La requête n'a retourné aucune ligne, conformément au comportement attendu. Le test est valide.";
    return "La requête n'a retourné aucune ligne. Vérifiez que les données d'entrée déclenchent bien le chemin de calcul attendu.";
  }
  if (status === 'error')
    return "La requête a échoué sur ces données. Inspectez les données d'entrée ou la requête SQL.";
  return "En cours d'exécution…";
}

export function testExecStatus(test: any): ExecStatus {
  if (test.status === 'complete') return 'pass';
  if (test.status === 'empty_results') {
    // Le juge prime : s'il a validé le test (« Bon »/« Excellent »), le résultat
    // vide est attendu → pass. Aligne l'exec sur statusToVerdict pour éviter un
    // header « en échec » sur une carte verte. Sinon, heuristique sur la description.
    if (/Excellent|Bon/.test(test.evaluation ?? '')) return 'pass';
    return testExpectsEmpty(test) ? 'pass' : 'fail';
  }
  if (test.status === 'error') return 'fail';
  return 'pending';
}

export interface VerdictInfo extends VerdictMeta {
  verdict: Verdict;
  text: string;
  execStatus: ExecStatus;
}

/** Consolidates all verdict computations for a single test object. */
export function getVerdictInfo(test: any): VerdictInfo {
  const verdict = statusToVerdict(test.status, test);
  const meta = VERDICT_META[verdict];
  return {
    verdict,
    ...meta,
    text: verdictText(test.status, test),
    execStatus: testExecStatus(test),
  };
}
