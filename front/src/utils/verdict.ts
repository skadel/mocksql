import i18n from '../i18n';

export type Verdict = 'good' | 'warn' | 'bad' | 'pending' | 'validation';
export type ExecStatus = 'pass' | 'fail' | 'pending';

export interface VerdictMeta {
  label: string;
  fg: string;
  bg: string;
  border: string;
}

// Couleurs par verdict ; le label affiché est localisé au moment du rendu
// (getVerdictInfo) — `label` ici n'est qu'un repli si la clé i18n manque.
export const VERDICT_META: Record<Verdict, VerdictMeta> = {
  good:       { label: 'Good',         fg: '#23a26d', bg: '#e9f7f0', border: '#23a26d' },
  warn:       { label: 'Insufficient', fg: '#d89323', bg: '#fcf3e1', border: '#d89323' },
  bad:        { label: 'Incorrect',    fg: '#d0503f', bg: '#fbeceb', border: '#d0503f' },
  pending:    { label: 'Pending',      fg: '#888',    bg: '#f4f7f7', border: '#ccc'    },
  validation: { label: 'To review',    fg: '#1565c0', bg: '#e8f0fd', border: '#1976d2' },
};

const VALIDATION_REASON_TYPES = new Set(['needs_validation', 'bad_description', 'bad_input_description']);

function testExpectsEmpty(test: any): boolean {
  const desc = (test.unit_test_description ?? '').toLowerCase();
  // Descriptions générées en FR ou EN selon la langue de sortie configurée.
  return (
    /retourne\s+.{0,40}vide|résultat[s]?\s+(?:est\s+)?vide[s]?|0\s+ligne|aucune\s+ligne/.test(desc) ||
    /returns?\s+.{0,40}empty|empty\s+result|no\s+rows?|zero\s+rows?|0\s+rows?/.test(desc)
  );
}

// Les tokens de verdict du juge sont un enum structuré historique (« Excellent » /
// « Bon » / « Insuffisant ») mais le texte libre autour est localisé — on tolère
// les deux langues pour ne pas dépendre de la discipline du LLM sur l'enum.
const GOOD_RE = /Excellent|Bon|Good/;
const WARN_RE = /Insuffisant|Insufficient/;

export function statusToVerdict(status: string | undefined, test?: any): Verdict {
  // Description désync → état neutre « À valider », ni Insuffisant ni Bon.
  if (test?.reason_type && VALIDATION_REASON_TYPES.has(test.reason_type)) return 'validation';
  if (test?.evaluation) {
    if (GOOD_RE.test(test.evaluation)) return 'good';
    if (WARN_RE.test(test.evaluation)) return 'warn';
  }
  if (status === 'complete')      return 'good';
  if (status === 'empty_results') return (test && testExpectsEmpty(test)) ? 'good' : 'warn';
  if (status === 'error')         return 'bad';
  return 'pending';
}

export function verdictText(status: string | undefined, test?: any): string {
  if (test?.evaluation) return test.evaluation;
  if (status === 'complete') return i18n.t('verdict.text_complete');
  if (status === 'empty_results') {
    if (test && testExpectsEmpty(test)) return i18n.t('verdict.text_empty_expected');
    return i18n.t('verdict.text_empty_unexpected');
  }
  if (status === 'error') return i18n.t('verdict.text_error');
  return i18n.t('verdict.text_running');
}

/** Vrai quand l'exécution du test est terminée mais que le verdict LLM n'est pas encore
 *  rendu (run en cours) : le verdict dérivé du seul statut d'exécution serait optimiste
 *  (complete → « Bon » avant que le juge ne parle) — l'UI doit montrer « Évaluation… »
 *  au lieu du badge. Hors run (isLoading false), le verdict basé sur le statut reste
 *  affiché tel quel (modèles rechargés sans champ evaluation). */
export function isAwaitingEvaluation(test: any, isLoading: boolean | undefined): boolean {
  if (!isLoading || !test) return false;
  if (!test.status || test.status === 'pending') return false;
  if (test.evaluation) return false;
  // reason_type de validation = l'évaluateur a déjà tranché (état « À valider »).
  if (test.reason_type && VALIDATION_REASON_TYPES.has(test.reason_type)) return false;
  return true;
}

export function testExecStatus(test: any): ExecStatus {
  if (test.status === 'complete') return 'pass';
  if (test.status === 'empty_results') {
    // Le juge prime : s'il a validé le test (« Bon »/« Excellent »), le résultat
    // vide est attendu → pass. Aligne l'exec sur statusToVerdict pour éviter un
    // header « en échec » sur une carte verte. Sinon, heuristique sur la description.
    if (GOOD_RE.test(test.evaluation ?? '')) return 'pass';
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

// Le texte du juge commence par son grade interne (« Excellent — … ») alors que le badge
// affiche déjà le label localisé → on retire ce préfixe à l'affichage pour ne pas montrer
// deux échelles (« Good — Excellent — … »). La classification (statusToVerdict) garde le
// texte brut.
const GRADE_PREFIX_RE = /^\s*(?:Excellent|Bon|Insuffisant|Good|Insufficient)\s*[—–-]\s*/;

/** Consolidates all verdict computations for a single test object. */
export function getVerdictInfo(test: any): VerdictInfo {
  const verdict = statusToVerdict(test.status, test);
  const meta = VERDICT_META[verdict];
  return {
    verdict,
    ...meta,
    label: i18n.t(`verdict.${verdict}`, meta.label),
    // Les ** du markdown du juge ne sont pas rendus par l'UI — on les retire à l'affichage.
    text: verdictText(test.status, test).replace(/\*\*/g, '').replace(GRADE_PREFIX_RE, ''),
    execStatus: testExecStatus(test),
  };
}
