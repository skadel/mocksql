import AddIcon from '@mui/icons-material/Add';
import CancelIcon from '@mui/icons-material/Cancel';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import HistoryIcon from '@mui/icons-material/History';
import LinkIcon from '@mui/icons-material/Link';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import ReplayIcon from '@mui/icons-material/Replay';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import CommentIcon from '@mui/icons-material/Comment';
import ViewListIcon from '@mui/icons-material/ViewList';
import ViewAgendaIcon from '@mui/icons-material/ViewAgenda';
import FilterListIcon from '@mui/icons-material/FilterList';
import SyncIcon from '@mui/icons-material/Sync';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  IconButton,
  List,
  ListItemButton,
  ListItemText,
  Popover,
  Skeleton,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import { DangerIconButton, MutedIconButton, OutlinedPrimaryButton, PrimaryButton, TealIconButton } from '../../../style/AppButtons';
import { SqlHistoryEntry } from '../../../utils/types';
import React, { useState, useMemo, useRef, useEffect } from 'react';
import SqlEditor from '../../../shared/SqlEditor';
import { patchModelTests } from '../../../api/messages';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import ExcelDownloader from '../../../shared/ExcelDownloader';
import ExcelUploader from '../../../shared/ExcelUploader';
import { setTestResults } from '../buildModelSlice';
import DisplayTable from './DisplayTable';
import { useLocalStorageState } from '../../../hooks/useLocalStorageState';
import { useTestPanelState, VerdictFilter } from '../hooks/useTestPanelState';
import {
  Verdict,
  VERDICT_META,
  statusToVerdict,
  verdictText,
  testExecStatus,
  getVerdictInfo,
} from '../../../utils/verdict';
import { TEAL, INK, BODY, MUTED, PLACEHOLDER, BORDER, SURFACE, TEAL_SUBTLE } from '../../../theme/tokens';

/* ─── tag colours ─────────────────────────────────────────────────── */
const TAG_COLORS: Record<string, { bg: string; fg: string }> = {
  'Logique métier':     { bg: '#e6f7f6', fg: TEAL },
  'Null checks':        { bg: '#fdecea', fg: '#d32f2f' },
  'Cas limites':        { bg: '#fff3e0', fg: '#e65100' },
  'Intégration':        { bg: '#eef1f7', fg: '#50609d' },
  'Valeurs dupliquées': { bg: '#f3e8e6', fg: '#6d4c41' },
  'Performance':        { bg: '#e0f2f1', fg: '#00695c' },
};
function tagStyle(tag: string) {
  return TAG_COLORS[tag] ?? { bg: '#f0f0f0', fg: '#555' };
}

/* ─── coverage ────────────────────────────────────────────────────── */
const COVERAGE_BUCKETS = [
  { key: 'happy', label: 'Cas nominal',     weight: 25 },
  { key: 'null',  label: 'Valeurs NULL',    weight: 20 },
  { key: 'empty', label: 'Données vides',   weight: 15 },
  { key: 'dup',   label: 'Doublons',        weight: 15 },
  { key: 'limit', label: 'Valeurs limites', weight: 15 },
  { key: 'tie',   label: 'Tri / Ex æquo',  weight: 10 },
];

function axisCompleteness(n: number): number {
  if (n === 0) return 0;
  if (n === 1) return 40;
  if (n === 2) return 65;
  if (n === 3) return 85;
  return 100;
}

function axisColor(comp: number): string {
  return comp === 0 ? '#c8d2d4' : comp >= 85 ? '#2BB0A8' : '#d89323';
}

function computeCoverage(tests: any[]) {
  const counts: Record<string, number> = {};
  COVERAGE_BUCKETS.forEach((b) => { counts[b.key] = 0; });

  tests.forEach((t) => {
    const s = ((t.unit_test_description ?? '') + ' ' + (t.tags ?? []).join(' ')).toLowerCase();
    if (/logique.m.tier|calcul|pourcentage|croissance|nominal|résultat|attendu|standard/.test(s)) counts.happy++;
    if (/null.checks|null|manquant|absent/.test(s))                                              counts.null++;
    if (/vide|aucune|inexistant|0.ligne|zéro|sans.données|ensemble.vide/.test(s))               counts.empty++;
    if (/valeurs.dupliqu|doublon|dupliqué|répété/.test(s))                                       counts.dup++;
    if (/cas.limites|limite|extrême|bord|boundary|borne|plage/.test(s))                          counts.limit++;
    if (/ex.æquo|ex.aequo|\btie\b|classement|rang\b/.test(s))                                   counts.tie++;
  });

  let score = 0;
  COVERAGE_BUCKETS.forEach((b) => {
    score += (b.weight * axisCompleteness(counts[b.key])) / 100;
  });

  return { score: Math.round(score), counts };
}

/* ─── CoverageBar ─────────────────────────────────────────────────── */
function CoverageRing({ score, fg }: { score: number; fg: string }) {
  const size = 52, stroke = 5, r = (size - stroke) / 2, c = 2 * Math.PI * r;
  const off = c - (score / 100) * c;
  return (
    <svg width={size} height={size} style={{ flexShrink: 0 }}>
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke="#edf1f2" strokeWidth={stroke} />
      <circle cx={size / 2} cy={size / 2} r={r} fill="none" stroke={fg} strokeWidth={stroke}
        strokeDasharray={c} strokeDashoffset={off} strokeLinecap="round"
        transform={`rotate(-90 ${size / 2} ${size / 2})`}
        style={{ transition: 'stroke-dashoffset .5s' }} />
      <text x={size / 2} y={size / 2 + 4} textAnchor="middle" fontSize="12" fontWeight="700" fill="#1a1a1a">{score}</text>
    </svg>
  );
}

function CoverageBar({ tests }: { tests: any[] }) {
  const { score, counts } = useMemo(() => computeCoverage(tests), [tests]);
  const toneFg = score >= 80 ? '#23a26d' : score >= 50 ? '#d89323' : '#d0503f';

  const bucketData = useMemo(
    () => COVERAGE_BUCKETS.map((b) => {
      const n = counts[b.key];
      const comp = axisCompleteness(n);
      return { ...b, n, comp, color: axisColor(comp) };
    }),
    [counts],
  );

  const uncovered = bucketData.filter((b) => b.n === 0);
  const partial   = bucketData.filter((b) => b.n > 0 && b.comp < 85);

  return (
    <Box sx={{ bgcolor: SURFACE, border: `1px solid ${BORDER}`, borderRadius: '12px', p: '14px 16px', mb: 1.5 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <CoverageRing score={score} fg={toneFg} />
          <Box>
            <Typography sx={{ fontSize: 10, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.7 }}>Couverture</Typography>
            <Typography sx={{ fontSize: 18, fontWeight: 700, color: INK, lineHeight: 1.1 }}>
              {score}%
              <Typography component="span" sx={{ fontSize: 11, color: MUTED, fontWeight: 500, ml: 1 }}>
                — {tests.length} test{tests.length > 1 ? 's' : ''}
              </Typography>
            </Typography>
          </Box>
        </Box>

        <Box sx={{ flex: 1, minWidth: 160 }}>
          <Box sx={{ display: 'flex', gap: '3px', height: 8, borderRadius: '4px', overflow: 'hidden', bgcolor: '#edf1f2' }}>
            {bucketData.map((b) => (
              <Box
                key={b.key}
                title={`${b.label} : ${b.comp}% (${b.n} test${b.n !== 1 ? 's' : ''})`}
                sx={{ flex: b.weight, position: 'relative', bgcolor: '#edf1f2', overflow: 'hidden' }}
              >
                <Box sx={{
                  position: 'absolute', left: 0, top: 0, bottom: 0,
                  width: `${b.comp}%`,
                  bgcolor: b.color,
                  transition: 'width .5s',
                }} />
              </Box>
            ))}
          </Box>

          <Box sx={{ display: 'flex', gap: '8px', mt: 0.75, flexWrap: 'wrap' }}>
            {bucketData.map((b) => (
              <Box key={b.key} sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', fontSize: 10.5 }}>
                <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: b.color, flexShrink: 0 }} />
                <Typography component="span" sx={{ fontSize: 10.5, color: b.n > 0 ? BODY : PLACEHOLDER }}>
                  {b.label}
                </Typography>
                {b.n > 0 && (
                  <Typography component="span" sx={{ fontSize: 9.5, color: b.color, fontWeight: 700 }}>
                    {b.comp}%·{b.n}
                  </Typography>
                )}
              </Box>
            ))}
          </Box>
        </Box>
      </Box>

      {(uncovered.length > 0 || partial.length > 0) && (
        <Box sx={{ mt: 1.25, pt: 1.25, borderTop: '1px dashed #e4eaec' }}>
          {uncovered.length > 0 && (
            <Box sx={{ fontSize: 12, color: BODY }}>
              <Typography component="span" sx={{ color: MUTED, fontSize: 12 }}>Non couvert : </Typography>
              {uncovered.map((b, i) => (
                <Typography key={b.key} component="span" sx={{ fontWeight: 500, fontSize: 12 }}>
                  {b.label}{i < uncovered.length - 1 ? ', ' : ''}
                </Typography>
              ))}
            </Box>
          )}
          {partial.length > 0 && (
            <Box sx={{ fontSize: 12, color: BODY, mt: uncovered.length > 0 ? 0.5 : 0 }}>
              <Typography component="span" sx={{ color: '#d89323', fontSize: 12 }}>Peu couvert : </Typography>
              {partial.map((b, i) => (
                <Typography key={b.key} component="span" sx={{ fontWeight: 500, fontSize: 12 }}>
                  {b.label} ({b.n} test{b.n > 1 ? 's' : ''}){i < partial.length - 1 ? ', ' : ''}
                </Typography>
              ))}
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
}

/* ─── Comments ─────────────────────────────────────────────────────── */
interface Comment { id: string; text: string; author: string; initials: string; ts: number; }

function relTime(ts: number): string {
  const s = Math.floor((Date.now() - ts) / 1000);
  if (s < 60) return "à l'instant";
  const m = Math.floor(s / 60);
  if (m < 60) return `il y a ${m} min`;
  const h = Math.floor(m / 60);
  if (h < 24) return `il y a ${h} h`;
  return `il y a ${Math.floor(h / 24)} j`;
}

function CommentsSection({ testKey, comments, onAdd, onDelete }: {
  testKey: string;
  comments: Comment[];
  onAdd: (text: string) => void;
  onDelete: (id: string) => void;
}) {
  const [draft, setDraft] = useState('');
  function submit() {
    const tx = draft.trim();
    if (!tx) return;
    onAdd(tx);
    setDraft('');
  }
  return (
    <Box sx={{ px: 2, py: 1.5, bgcolor: '#fbfcfc', borderTop: `1px solid #eff3f4` }}>
      <Typography sx={{ fontSize: 10, fontWeight: 700, color: MUTED, letterSpacing: 0.6, textTransform: 'uppercase', mb: 1 }}>
        Commentaires d'équipe{comments.length > 0 ? ` · ${comments.length}` : ''}
      </Typography>
      {comments.length === 0 && (
        <Typography sx={{ fontSize: 12, color: PLACEHOLDER, fontStyle: 'italic', mb: 1 }}>
          Aucun commentaire. Note ici un contexte métier, une décision d'équipe ou un point à vérifier.
        </Typography>
      )}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mb: comments.length ? 1.25 : 0 }}>
        {comments.map((c) => (
          <Box key={c.id} sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
            <Box sx={{ width: 24, height: 24, borderRadius: '50%', bgcolor: '#ecf7f6', color: TEAL, display: 'grid', placeItems: 'center', fontWeight: 700, fontSize: 10, flexShrink: 0 }}>
              {c.initials}
            </Box>
            <Box sx={{ flex: 1, bgcolor: '#fff', border: `1px solid ${BORDER}`, borderRadius: '10px', p: '7px 11px' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.25 }}>
                <Typography sx={{ fontWeight: 600, fontSize: 11, color: BODY }}>{c.author}</Typography>
                <Typography sx={{ fontSize: 11, color: PLACEHOLDER }}>· {relTime(c.ts)}</Typography>
                <IconButton size="small" onClick={() => onDelete(c.id)} sx={{ ml: 'auto', p: 0.25, color: PLACEHOLDER, '&:hover': { color: '#d0503f' } }}>
                  <DeleteIcon sx={{ fontSize: 12 }} />
                </IconButton>
              </Box>
              <Typography sx={{ fontSize: 12.5, color: INK, lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>{c.text}</Typography>
            </Box>
          </Box>
        ))}
      </Box>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-end', bgcolor: '#fff', border: `1px solid ${BORDER}`, borderRadius: '10px', p: '6px 6px 6px 10px' }}>
        <Box sx={{ width: 22, height: 22, borderRadius: '50%', bgcolor: '#ecf7f6', color: TEAL, display: 'grid', placeItems: 'center', fontWeight: 700, fontSize: 10, flexShrink: 0 }}>
          CB
        </Box>
        <TextField
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); submit(); } }}
          placeholder="Ajouter un commentaire (Cmd/Ctrl+Entrée)"
          multiline
          maxRows={4}
          variant="standard"
          InputProps={{ disableUnderline: true }}
          sx={{ flex: 1, fontSize: 12.5, '& textarea': { fontSize: 12.5, color: INK, py: 0.5 } }}
        />
        <Box
          component="button"
          onClick={submit}
          disabled={!draft.trim()}
          sx={{
            px: 1.5, py: 0.75, fontSize: 11.5, fontWeight: 600, border: 'none', borderRadius: '7px',
            bgcolor: draft.trim() ? '#2BB0A8' : '#c8d2d4', color: '#fff',
            cursor: draft.trim() ? 'pointer' : 'not-allowed', fontFamily: 'inherit',
          }}
        >
          Publier
        </Box>
      </Box>
    </Box>
  );
}

/* ─── StatusDot ───────────────────────────────────────────────────── */
function StatusDot({ status, test }: { status: string | undefined; test?: any }) {
  const { verdict } = getVerdictInfo(test ?? { status });
  if (verdict === 'good')    return <CheckCircleIcon sx={{ fontSize: 18, color: '#23a26d', flexShrink: 0 }} />;
  if (verdict === 'bad')     return <CancelIcon sx={{ fontSize: 18, color: '#d0503f', flexShrink: 0 }} />;
  if (verdict === 'warn')    return <WarningAmberIcon sx={{ fontSize: 18, color: '#d89323', flexShrink: 0 }} />;
  return <CircularProgress size={14} thickness={5} sx={{ color: TEAL, flexShrink: 0 }} />;
}

/* ─── CompactRow ──────────────────────────────────────────────────── */
function CompactRow({ test, idx, commentCount, onExpand, onAsk, onDelete }: {
  test: any; idx: number; commentCount: number;
  onExpand: () => void; onAsk: () => void; onDelete: () => void;
}) {
  const { verdict, label, fg, bg, border } = getVerdictInfo(test);
  const tags: string[] = test.tags ?? [];
  return (
    <Box
      id={`test-${idx + 1}`}
      onClick={onExpand}
      sx={{
        bgcolor: SURFACE, border: `1px solid ${BORDER}`, borderLeft: `3px solid ${border}`,
        borderRadius: '10px', display: 'grid',
        gridTemplateColumns: '22px 108px 1fr auto',
        alignItems: 'center', gap: 1, p: '9px 12px', cursor: 'pointer',
        '&:hover': { bgcolor: '#fafcfc' },
      }}
    >
      <StatusDot status={test.status} test={test} />
      <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: bg, color: fg, px: '8px', py: '2px', borderRadius: 999, fontSize: 11, fontWeight: 700, justifySelf: 'start' }}>
        {verdict === 'good' && <CheckCircleIcon sx={{ fontSize: 11 }} />}
        {verdict === 'warn' && <WarningAmberIcon sx={{ fontSize: 11 }} />}
        {verdict === 'bad'  && <CancelIcon sx={{ fontSize: 11 }} />}
        {label}
      </Box>
      <Box sx={{ minWidth: 0, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Typography sx={{ fontSize: 11, color: MUTED, fontVariantNumeric: 'tabular-nums' }}>#{idx + 1}</Typography>
        <Typography sx={{ fontSize: 12.5, color: INK, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', fontWeight: 500 }}>
          {test.unit_test_description ?? '—'}
        </Typography>
        {tags.slice(0, 1).map((tg) => {
          const tc = tagStyle(tg);
          return <Chip key={tg} label={tg} size="small" sx={{ fontSize: 10, height: 18, bgcolor: tc.bg, color: tc.fg, border: 'none', flexShrink: 0 }} />;
        })}
      </Box>
      <Box sx={{ display: 'flex', gap: 0.25 }} onClick={(e) => e.stopPropagation()}>
        {commentCount > 0 && (
          <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '3px', fontSize: 11, color: BODY, px: '7px', py: '2px', bgcolor: SURFACE, borderRadius: 999, fontWeight: 600, mr: 0.5 }}>
            <CommentIcon sx={{ fontSize: 10 }} /> {commentCount}
          </Box>
        )}
        <Tooltip title="Modifier avec MockSQL">
          <MutedIconButton size="small" onClick={onAsk}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></MutedIconButton>
        </Tooltip>
        <Tooltip title="Supprimer">
          <DangerIconButton size="small" onClick={onDelete}><DeleteIcon sx={{ fontSize: 14 }} /></DangerIconButton>
        </Tooltip>
      </Box>
    </Box>
  );
}

/* ─── ResultWithAssertions ───────────────────────────────────────── */
interface AssertionItem {
  description: string;
  sql?: string;
  passed: boolean;
  failing_rows?: any[];
  error?: string;
}

function AssertionRow({ a, expanded, onToggle, onDelete }: {
  a: AssertionItem;
  expanded: boolean;
  onToggle: () => void;
  onDelete: () => void;
}) {
  const statusColor = a.passed ? '#23a26d' : '#d0503f';
  const statusBg    = a.passed ? '#eaf5f0' : '#fbeceb';
  const failCount   = a.failing_rows?.length ?? 0;
  return (
    <Box sx={{ borderTop: '1px solid #eff3f4', '&:first-of-type': { borderTop: 'none' } }}>
      <Box onClick={onToggle} sx={{ p: '8px 12px', display: 'flex', alignItems: 'center', gap: 1.25, cursor: 'pointer', '&:hover': { bgcolor: '#fafcfc' } }}>
        <Box sx={{ width: 18, height: 18, borderRadius: '50%', bgcolor: statusBg, color: statusColor, display: 'grid', placeItems: 'center', flexShrink: 0 }}>
          {a.passed ? <CheckCircleIcon sx={{ fontSize: 11 }} /> : <CancelIcon sx={{ fontSize: 11 }} />}
        </Box>
        <Typography sx={{ flex: 1, fontSize: 12.5, color: INK, lineHeight: 1.45, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: expanded ? 'normal' : 'nowrap' }}>
          {a.description}
        </Typography>
        {!a.passed && failCount > 0 && !expanded && (
          <Typography sx={{ fontSize: 11, color: statusColor, fontWeight: 600, flexShrink: 0 }}>
            {failCount} ligne{failCount > 1 ? 's' : ''} en échec
          </Typography>
        )}
        <Box sx={{ transform: expanded ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.15s', display: 'inline-flex', color: MUTED, flexShrink: 0 }}>
          <ExpandMoreIcon sx={{ fontSize: 14 }} />
        </Box>
      </Box>
      {expanded && (
        <Box sx={{ px: '12px', pb: '10px', pl: '42px', bgcolor: '#fafbfc' }}>
          {a.error && (
            <Typography sx={{ fontSize: 11.5, color: '#d0503f', fontFamily: 'monospace', mb: 0.75 }}>{a.error}</Typography>
          )}
          {!a.passed && failCount > 0 && (
            <Typography sx={{ fontSize: 11.5, color: '#d0503f', mb: 0.75 }}>
              {failCount} ligne{failCount > 1 ? 's' : ''} en échec
            </Typography>
          )}
          {a.sql && (
            <Box component="pre" sx={{ m: 0, mb: 1, p: '8px 10px', fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace", bgcolor: '#eef2f3', borderRadius: '7px', overflowX: 'auto', color: '#2b3b3e', lineHeight: 1.5, border: '1px solid #dce4e6' }}>
              {a.sql}
            </Box>
          )}
          <Box component="button" onClick={onDelete}
            sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '9px', py: '4px', fontSize: 11, fontWeight: 500, border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', color: '#d0503f', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#fbeceb', borderColor: '#d0503f' } }}>
            <DeleteIcon sx={{ fontSize: 12 }} /> Supprimer
          </Box>
        </Box>
      )}
    </Box>
  );
}

function ResultWithAssertions({ inputData, outputData, assertionResults }: {
  inputData: Record<string, any[]>;
  outputData: any[];
  assertionResults: AssertionItem[];
}) {
  const [expandedSet, setExpandedSet] = useState<Set<number>>(() => {
    const s = new Set<number>();
    assertionResults.forEach((a, i) => { if (!a.passed) s.add(i); });
    return s;
  });
  const [localAssertions, setLocalAssertions] = useState<AssertionItem[]>(assertionResults);

  useEffect(() => {
    setLocalAssertions(assertionResults);
    setExpandedSet(() => {
      const s = new Set<number>();
      assertionResults.forEach((a, i) => { if (!a.passed) s.add(i); });
      return s;
    });
  }, [assertionResults]);

  function toggle(i: number) {
    setExpandedSet(prev => { const n = new Set(prev); if (n.has(i)) n.delete(i); else n.add(i); return n; });
  }

  function deleteAssertion(i: number) {
    setLocalAssertions(prev => prev.filter((_, j) => j !== i));
  }

  const passCount = localAssertions.filter(a => a.passed).length;
  const failCount = localAssertions.filter(a => !a.passed).length;
  const hasInput = Object.keys(inputData).length > 0;
  const hasOutput = outputData.length > 0;
  const hasAssertions = localAssertions.length > 0;

  if (!hasInput && !hasOutput && !hasAssertions) return null;

  return (
    <Box sx={{ borderTop: '1px solid #eff3f4' }}>
      {/* Header */}
      {hasAssertions && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25, px: 2, py: '8px', bgcolor: '#f0f3f4', borderBottom: '1px solid #eff3f4' }}>
          <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, letterSpacing: 0.6, textTransform: 'uppercase' }}>
            Scénario du test
          </Typography>
          <Typography sx={{ fontSize: 11.5, color: failCount > 0 ? '#d0503f' : '#23a26d', fontWeight: 600 }}>
            {passCount}/{localAssertions.length} assertion{localAssertions.length > 1 ? 's' : ''} passe{passCount !== 1 ? 'nt' : ''}
            {failCount > 0 && <Box component="span" sx={{ ml: 1 }}>· {failCount} échoue{failCount > 1 ? 'nt' : ''}</Box>}
          </Typography>
        </Box>
      )}

      {/* 1. Input data */}
      {hasInput && (
        <Box sx={{ px: 2, pt: 1.5, pb: 1 }}>
          <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6, mb: 0.75 }}>
            {hasAssertions ? '1 · ' : ''}Données d'entrée
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, overflowX: 'auto' }}>
            {Object.entries(inputData).map(([key, val]) => (
              <DisplayTable key={key} jsonData={val as any[]} tableName={key} />
            ))}
          </Box>
        </Box>
      )}

      {/* Arrow */}
      {hasInput && (hasOutput || hasAssertions) && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: '2px', color: MUTED }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M12 5v14M6 13l6 6 6-6" /></svg>
        </Box>
      )}

      {/* 2. Query result */}
      {hasOutput && (
        <Box sx={{ px: 2, pb: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.75 }}>
            <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6 }}>
              {hasAssertions ? '2 · ' : ''}Résultat de la requête
            </Typography>
            <Typography sx={{ fontSize: 11, color: MUTED, ml: 'auto' }}>
              {outputData.length} ligne{outputData.length > 1 ? 's' : ''}
            </Typography>
          </Box>
          <Box sx={{ overflowX: 'auto' }}>
            <DisplayTable jsonData={outputData} tableName="Résultat" />
          </Box>
        </Box>
      )}

      {/* Arrow */}
      {hasAssertions && (hasInput || hasOutput) && (
        <Box sx={{ display: 'flex', justifyContent: 'center', py: '2px', color: MUTED }}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M12 5v14M6 13l6 6 6-6" /></svg>
        </Box>
      )}

      {/* 3. Assertions */}
      {hasAssertions && (
        <Box sx={{ px: 2, pb: 1.5 }}>
          <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6, mb: 0.75 }}>
            3 · Assertions sur ce résultat
          </Typography>
          <Box sx={{ border: `1px solid ${BORDER}`, borderRadius: '8px', overflow: 'hidden' }}>
            {localAssertions.map((a, i) => (
              <AssertionRow key={i} a={a} expanded={expandedSet.has(i)} onToggle={() => toggle(i)} onDelete={() => deleteAssertion(i)} />
            ))}
          </Box>
        </Box>
      )}
    </Box>
  );
}

/* ─── SuggestionRow ──────────────────────────────────────────────── */
function SuggestionRow({ text, tag, onAdd, onFill }: { text: string; tag?: string; onAdd?: () => void; onFill?: () => void }) {
  const tc = tag ? tagStyle(tag) : { bg: SURFACE, fg: MUTED };
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25, p: '9px 11px', border: `1px solid ${BORDER}`, borderRadius: '10px', bgcolor: '#fafcfc' }}>
      <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: tc.fg, flexShrink: 0 }} />
      <Typography sx={{ flex: 1, fontSize: 12.5, color: BODY, lineHeight: 1.45 }}>{text}</Typography>
      {tag && <Chip label={tag} size="small" sx={{ fontSize: 10, height: 18, bgcolor: tc.bg, color: tc.fg, border: 'none', flexShrink: 0 }} />}
      <Box
        component="button"
        onClick={onFill ?? onAdd}
        sx={{
          display: 'inline-flex', alignItems: 'center', gap: '5px',
          px: '10px', py: '5px', fontSize: 12, bgcolor: '#2BB0A8', color: '#fff',
          border: 'none', borderRadius: '8px', cursor: 'pointer', fontWeight: 600, fontFamily: 'inherit',
          '&:hover': { bgcolor: '#1f948d' },
        }}
      >
        <AddIcon sx={{ fontSize: 12 }} /> Ajouter
      </Box>
    </Box>
  );
}

/* ─── FilterChip ─────────────────────────────────────────────────── */
function FilterChip({ label, count, active, color, onClick }: { label: string; count: number; active: boolean; color: string; onClick: () => void }) {
  return (
    <Box
      component="button"
      onClick={onClick}
      sx={{
        display: 'inline-flex', alignItems: 'center', gap: '6px',
        px: '10px', py: '5px', fontSize: 12, cursor: 'pointer', fontFamily: 'inherit',
        border: `1.2px solid ${active ? color : BORDER}`,
        bgcolor: SURFACE, color: INK, borderRadius: 999, fontWeight: active ? 700 : 500,
        '&:hover': { borderColor: color },
      }}
    >
      <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: color }} />
      {label}
      <Box sx={{ fontSize: 10.5, color: active ? color : PLACEHOLDER, bgcolor: active ? 'transparent' : SURFACE, px: '6px', borderRadius: 999, fontWeight: 700 }}>
        {count}
      </Box>
    </Box>
  );
}

/* ─── SqlStrip ───────────────────────────────────────────────────── */
export interface SqlStripProps {
  sql: string;
  onUpdate?: (newSql: string) => void;
  disabled?: boolean;
  loading?: boolean;
  hasError?: boolean;
  optimizedSql?: string;
  sqlHistory?: SqlHistoryEntry[];
  onHistorySelect?: (entry: SqlHistoryEntry) => void;
  historyRestoreTrigger?: number;
  /** File name shown in the reload banner (e.g. "peak_growth.sql") */
  sqlFileName?: string;
  /** Fetches the latest SQL from the source file; returns null on failure */
  onReloadFile?: () => Promise<string | null>;
}

type ReloadStatus = 'idle' | 'loading' | 'changed' | 'same';

function SqlStrip({ sql, onUpdate, disabled, loading, hasError, optimizedSql, sqlHistory, onHistorySelect, historyRestoreTrigger, sqlFileName, onReloadFile }: SqlStripProps) {
  const [open, setOpen] = useState(true);
  const [editedSql, setEditedSql] = useState(sql);
  const [viewMode, setViewMode] = useState<'raw' | 'optimized'>('raw');
  const [historyAnchor, setHistoryAnchor] = useState<HTMLElement | null>(null);
  const [reloadStatus, setReloadStatus] = useState<ReloadStatus>('idle');
  const prevDisabled = useRef(disabled);
  const prevTrigger = useRef(historyRestoreTrigger);

  async function handleReloadFile() {
    if (!onReloadFile || reloadStatus === 'loading') return;
    setReloadStatus('loading');
    const newSql = await onReloadFile();
    if (newSql === null) { setReloadStatus('idle'); return; }
    if (newSql.trim() !== sql.trim()) {
      setReloadStatus('changed');
      setEditedSql(newSql);
      onUpdate?.(newSql);
      setTimeout(() => setReloadStatus('idle'), 3000);
    } else {
      setReloadStatus('same');
      setTimeout(() => setReloadStatus('idle'), 2000);
    }
  }

  useEffect(() => {
    if (prevDisabled.current && !disabled && !hasError) setOpen(false);
    prevDisabled.current = disabled;
  }, [disabled, hasError]);

  useEffect(() => {
    if (optimizedSql) setViewMode('raw');
  }, [optimizedSql]);

  useEffect(() => {
    if (historyRestoreTrigger !== undefined && historyRestoreTrigger !== prevTrigger.current) {
      prevTrigger.current = historyRestoreTrigger;
      setEditedSql(sql);
      setViewMode('raw');
      setOpen(true);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [historyRestoreTrigger]);

  const lines = sql.split('\n');
  const first = lines.find((l) => l.trim()) ?? '';
  const second = lines.slice(lines.indexOf(first) + 1).find((l) => l.trim())?.trim() ?? '';
  const preview = [first, second].filter(Boolean).join(' ');
  const truncated = preview.length > 80 ? preview.slice(0, 80) + '…' : preview;

  const handleToggle = () => {
    if (!open) { setEditedSql(sql); setViewMode('raw'); }
    setOpen((v) => !v);
  };

  const handleUpdate = () => {
    if (!editedSql.trim()) return;
    onUpdate?.(editedSql);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); handleUpdate(); }
  };

  const showToggle = !!optimizedSql && optimizedSql.trim() !== sql.trim();
  const isOptimizedView = viewMode === 'optimized';
  const editorValue = isOptimizedView ? (optimizedSql ?? '') : editedSql;
  const hasHistory = sqlHistory && sqlHistory.length > 0;

  return (
    <>
      {/* Reload banner — shown when file changed */}
      {reloadStatus === 'changed' && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: '6px', bgcolor: '#e9f7f0', borderBottom: '1px solid #b2e0ce' }}>
          <CheckCircleIcon sx={{ fontSize: 14, color: '#23a26d' }} />
          <Typography sx={{ fontSize: 11.5, color: '#23a26d', fontWeight: 600 }}>
            Fichier rechargé — tests relancés
          </Typography>
        </Box>
      )}
      {reloadStatus === 'same' && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 2, py: '6px', bgcolor: '#e9f7f0', borderBottom: '1px solid #b2e0ce' }}>
          <CheckCircleIcon sx={{ fontSize: 14, color: '#23a26d' }} />
          <Typography sx={{ fontSize: 11.5, color: '#23a26d', fontWeight: 600 }}>
            Fichier synchronisé — aucun changement détecté
          </Typography>
        </Box>
      )}

      <Box sx={{ bgcolor: SURFACE, borderBottom: `1px solid ${BORDER}`, flexShrink: 0 }}>
        <Box
          onClick={handleToggle}
          sx={{ display: 'flex', alignItems: 'center', gap: 1.5, px: 2, py: '9px', cursor: 'pointer', '&:hover': { bgcolor: '#fafcfc' } }}
        >
          <Typography sx={{ fontSize: 11.5, fontWeight: 700, color: '#1f948d', letterSpacing: 0.5, fontFamily: 'monospace', flexShrink: 0 }}>
            SQL
          </Typography>
          {sqlFileName && (
            <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', flexShrink: 0 }}>
              <FolderOpenIcon sx={{ fontSize: 12, color: MUTED }} />
              <Typography sx={{ fontSize: 11, color: MUTED, fontFamily: 'monospace' }}>{sqlFileName}</Typography>
            </Box>
          )}
          {!open && (
            <Typography sx={{ fontSize: 12, color: '#3b4f52', fontFamily: 'monospace', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', flex: 1, minWidth: 0 }}>
              {truncated}
            </Typography>
          )}
          {open && <Box sx={{ flex: 1 }} />}
          {onReloadFile && (
            <Tooltip title="Recharger le fichier SQL et relancer les tests">
              <Box
                component="button"
                onClick={(e: React.MouseEvent) => { e.stopPropagation(); handleReloadFile(); }}
                disabled={reloadStatus === 'loading' || disabled}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '4px',
                  px: '9px', py: '4px', fontSize: 11.5, fontWeight: 600,
                  border: `1px solid ${BORDER}`, borderRadius: '7px',
                  bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit', flexShrink: 0,
                  '&:hover': { borderColor: '#2BB0A8', color: '#2BB0A8', bgcolor: '#f0fafa' },
                  '&:disabled': { opacity: 0.5, cursor: 'not-allowed' },
                }}
              >
                <SyncIcon sx={{
                  fontSize: 13,
                  '@keyframes spin': { from: { transform: 'rotate(0deg)' }, to: { transform: 'rotate(360deg)' } },
                  animation: reloadStatus === 'loading' ? 'spin 0.8s linear infinite' : 'none',
                }} />
                Recharger
              </Box>
            </Tooltip>
          )}
          {hasHistory && (
            <Tooltip title={`Historique (${sqlHistory!.length})`}>
              <TealIconButton
                size="small"
                onClick={(e) => { e.stopPropagation(); setHistoryAnchor(e.currentTarget); }}
                sx={{ flexShrink: 0, mr: 0.25 }}
              >
                <HistoryIcon sx={{ fontSize: 14 }} />
              </TealIconButton>
            </Tooltip>
          )}
          <Box sx={{ flexShrink: 0, display: 'flex', alignItems: 'center' }}>
            {open ? <ExpandLessIcon sx={{ fontSize: 16, color: PLACEHOLDER }} /> : <ExpandMoreIcon sx={{ fontSize: 16, color: PLACEHOLDER }} />}
          </Box>
        </Box>

        {open && (
          <>
            {showToggle && (
              <Box sx={{ display: 'flex', px: 2, py: 0.75, gap: 0, bgcolor: '#f5fafa', borderBottom: `1px solid ${BORDER}` }}>
                <Button
                  size="small"
                  onClick={() => setViewMode('raw')}
                  sx={{
                    fontSize: 11, py: 0.25, px: 1.5, minWidth: 0, textTransform: 'none', fontWeight: 600,
                    borderRadius: '6px 0 0 6px',
                    backgroundColor: !isOptimizedView ? TEAL : 'transparent',
                    color: !isOptimizedView ? '#fff' : TEAL,
                    border: `1px solid ${TEAL}`, borderRight: 'none',
                    '&:hover': { backgroundColor: !isOptimizedView ? '#159e9a' : '#e8f7f6' },
                  }}
                >
                  Original
                </Button>
                <Button
                  size="small"
                  onClick={() => setViewMode('optimized')}
                  sx={{
                    fontSize: 11, py: 0.25, px: 1.5, minWidth: 0, textTransform: 'none', fontWeight: 600,
                    borderRadius: '0 6px 6px 0',
                    backgroundColor: isOptimizedView ? TEAL : 'transparent',
                    color: isOptimizedView ? '#fff' : TEAL,
                    border: `1px solid ${TEAL}`,
                    '&:hover': { backgroundColor: isOptimizedView ? '#159e9a' : '#e8f7f6' },
                  }}
                >
                  Optimisé
                </Button>
              </Box>
            )}

            <SqlEditor
              value={editorValue}
              onChange={(v) => { if (!isOptimizedView) setEditedSql(v); }}
              disabled={disabled || isOptimizedView}
              maxHeight={240}
              fontSize={12.5}
              minHeight={80}
              onKeyDown={handleKeyDown}
            />

            {!isOptimizedView && (
              <Box sx={{ display: 'flex', justifyContent: 'flex-end', px: 2, py: 1, borderTop: '1px solid #e8f5f4' }}>
                <PrimaryButton
                  size="small"
                  startIcon={loading ? <CircularProgress size={14} color="inherit" /> : <PlayArrowIcon />}
                  onClick={handleUpdate}
                  disabled={disabled || !editedSql.trim()}
                >
                  {loading ? 'Validation…' : 'Mettre à jour'}
                </PrimaryButton>
              </Box>
            )}
          </>
        )}
      </Box>

      <Popover
        open={Boolean(historyAnchor)}
        anchorEl={historyAnchor}
        onClose={() => setHistoryAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
      >
        <Box sx={{ width: 360, maxHeight: 380, overflow: 'auto' }}>
          <Box sx={{ px: 2, py: 1, bgcolor: '#f0fafa', borderBottom: '1px solid #d0eeec' }}>
            <Typography variant="caption" sx={{ fontWeight: 700, color: TEAL }}>Historique SQL</Typography>
          </Box>
          <List dense disablePadding>
            {[...(sqlHistory ?? [])].reverse().map((entry, i, arr) => {
              const num = arr.length - i;
              const previewLine = entry.sql.split('\n').find(l => l.trim())?.slice(0, 60) ?? '';
              const hasOpt = entry.optimizedSql && entry.optimizedSql.trim() !== entry.sql.trim();
              return (
                <React.Fragment key={entry.id}>
                  {i > 0 && <Divider />}
                  <ListItemButton
                    onClick={() => { onHistorySelect?.(entry); setHistoryAnchor(null); }}
                    sx={{ px: 2, py: 0.75, '&:hover': { bgcolor: '#e8f7f6' } }}
                  >
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="caption" sx={{ fontWeight: 700, color: TEAL, flexShrink: 0 }}>#{num}</Typography>
                          {hasOpt && (
                            <Typography variant="caption" sx={{ fontSize: 10, color: '#888', bgcolor: '#f0f0f0', px: 0.5, borderRadius: 0.5 }}>optimisé</Typography>
                          )}
                        </Box>
                      }
                      secondary={
                        <Typography variant="caption" sx={{ fontFamily: 'monospace', fontSize: 11, color: '#555', display: 'block', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                          {previewLine}…
                        </Typography>
                      }
                    />
                  </ListItemButton>
                </React.Fragment>
              );
            })}
          </List>
        </Box>
      </Popover>
    </>
  );
}

/* ─── TestCard ───────────────────────────────────────────────────── */
interface TestCardProps {
  test: any;
  idx: number;
  selectedTestIndex: number | null;
  isEditing: boolean;
  editedDescription: string | undefined;
  isCollapsed: boolean;
  areCommentsOpen: boolean;
  comments: Comment[];
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onEditDescription: (val: string) => void;
  onDelete: () => void;
  onToggleCollapse: () => void;
  onToggleComments: () => void;
  onAddComment: (text: string) => void;
  onDeleteComment: (id: string) => void;
  onSelectForModification: () => void;
  onRerunTest?: () => void;
  onUpload?: (data: Record<string, any[]>) => void;
}

function TestCard({
  test, idx, selectedTestIndex,
  isEditing, editedDescription, isCollapsed,
  areCommentsOpen, comments,
  onStartEdit, onSaveEdit, onEditDescription,
  onDelete, onToggleCollapse, onToggleComments,
  onAddComment, onDeleteComment,
  onSelectForModification, onRerunTest, onUpload,
}: TestCardProps) {
  const { verdict, label, fg, bg, border, text: vText } = getVerdictInfo(test);
  const tags: string[] = test.tags ?? [];
  const description = editedDescription ?? test.unit_test_description ?? '';
  const testKey = `${idx}`;
  const [copied, setCopied] = useState(false);

  const handleCopyLink = (e: React.MouseEvent) => {
    e.stopPropagation();
    const url = new URL(window.location.href);
    url.hash = `test-${idx + 1}`;
    navigator.clipboard.writeText(url.toString());
    window.location.hash = `test-${idx + 1}`;
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const inputData: Record<string, any[]> = test.data ?? test.test_data ?? {};
  const outputData: any[] = test.results_json
    ? (() => { try { return JSON.parse(test.results_json); } catch { return []; } })()
    : [];

  return (
    <Box
      id={`test-${idx + 1}`}
      sx={{
        bgcolor: selectedTestIndex === idx ? '#f0fafa' : SURFACE,
        border: `1px solid ${BORDER}`,
        borderLeft: `3px solid ${border}`,
        borderRadius: '12px',
        overflow: 'hidden',
      }}
    >
      {/* Card header */}
      <Box sx={{ p: '14px 16px 10px' }}>
        {/* Verdict badge + tags */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, flexWrap: 'wrap', mb: 0.75 }}>
          {test.status !== 'pending' && (
            <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: bg, color: fg, px: '8px', py: '3px', borderRadius: 999, fontSize: 11.5, fontWeight: 700 }}>
              {verdict === 'good' && <CheckCircleIcon sx={{ fontSize: 11 }} />}
              {verdict === 'warn' && <WarningAmberIcon sx={{ fontSize: 11 }} />}
              {verdict === 'bad'  && <CancelIcon sx={{ fontSize: 11 }} />}
              {label}
            </Box>
          )}
          {test.status === 'pending' && (
            <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', color: MUTED, fontSize: 11.5 }}>
              <CircularProgress size={11} thickness={5} sx={{ color: TEAL }} /> En cours…
            </Box>
          )}
          {tags.map((tg) => {
            const tc = tagStyle(tg);
            return <Chip key={tg} label={tg} size="small" sx={{ fontSize: 10.5, height: 20, bgcolor: tc.bg, color: tc.fg, border: 'none' }} />;
          })}
          <Typography sx={{ fontSize: 11, color: PLACEHOLDER, ml: 'auto' }}>#{idx + 1}</Typography>
        </Box>

        {/* Description — inline editable */}
        {isEditing ? (
          <TextField
            value={editedDescription ?? test.unit_test_description}
            onChange={(e) => onEditDescription(e.target.value)}
            onBlur={onSaveEdit}
            onKeyDown={(e) => { e.stopPropagation(); if (e.key === 'Enter' && !e.shiftKey) onSaveEdit(); }}
            onClick={(e) => e.stopPropagation()}
            autoFocus fullWidth size="small" variant="standard" multiline minRows={2}
          />
        ) : (
          <Typography sx={{ fontWeight: 600, color: INK, fontSize: 13.5, lineHeight: 1.5 }}>
            {description}
          </Typography>
        )}

        {/* Verdict text */}
        {test.status && test.status !== 'pending' && (
          <Box sx={{
            mt: 1, p: '9px 12px', bgcolor: bg, borderRadius: '8px',
            borderLeft: `2px solid ${fg}`, fontSize: 12.5, color: BODY, lineHeight: 1.55,
          }}>
            <Typography component="span" sx={{ fontWeight: 700, color: fg, fontSize: 12.5 }}>
              Verdict · {label}
            </Typography>
            {' — '}{vText}
          </Box>
        )}
      </Box>

      {/* Action bar */}
      <Box sx={{ display: 'flex', gap: 0.5, px: 1.5, pb: 1, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
        {test.status !== 'pending' && (
          <>
            <Tooltip title="Éditer la description">
              <MutedIconButton size="small" onClick={onStartEdit}><EditIcon sx={{ fontSize: 14 }} /></MutedIconButton>
            </Tooltip>
            <Tooltip title={selectedTestIndex === idx ? 'Sélectionné — écris ton instruction dans le chat' : 'Modifier avec MockSQL'}>
              {selectedTestIndex === idx
                ? <TealIconButton size="small" onClick={onSelectForModification}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></TealIconButton>
                : <MutedIconButton size="small" onClick={onSelectForModification}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></MutedIconButton>
              }
            </Tooltip>
            {onRerunTest && (
              <Tooltip title="Relancer ce test">
                <MutedIconButton size="small" onClick={onRerunTest}><ReplayIcon sx={{ fontSize: 14 }} /></MutedIconButton>
              </Tooltip>
            )}
          </>
        )}
        <Tooltip title={copied ? 'Copié !' : 'Copier le lien vers ce test'}>
          <MutedIconButton size="small" onClick={handleCopyLink}>
            <LinkIcon sx={{ fontSize: 14, color: copied ? TEAL : undefined }} />
          </MutedIconButton>
        </Tooltip>

        <Tooltip title="Commentaires d'équipe">
          <Box
            component="button"
            onClick={onToggleComments}
            sx={{
              display: 'inline-flex', alignItems: 'center', gap: '4px',
              px: '9px', py: '4px', fontSize: 11.5, fontWeight: 600, cursor: 'pointer',
              border: '1px solid', borderColor: comments.length ? TEAL : BORDER,
              borderRadius: '7px', bgcolor: areCommentsOpen ? TEAL_SUBTLE : '#fff',
              color: comments.length ? TEAL : PLACEHOLDER, fontFamily: 'inherit',
              '&:hover': { borderColor: TEAL, color: TEAL },
            }}
          >
            <CommentIcon sx={{ fontSize: 12 }} />
            Commentaires
            {comments.length > 0 && (
              <Box sx={{ bgcolor: INK, color: '#fff', fontSize: 10, fontWeight: 700, px: '5px', borderRadius: 999, ml: 0.25 }}>
                {comments.length}
              </Box>
            )}
          </Box>
        </Tooltip>

        <Box sx={{ width: 1, bgcolor: BORDER, mx: 0.25 }} />

        <Tooltip title="Supprimer">
          <DangerIconButton size="small" onClick={onDelete}><DeleteIcon sx={{ fontSize: 14 }} /></DangerIconButton>
        </Tooltip>

        <Tooltip title={isCollapsed ? 'Voir les données' : 'Replier les données'}>
          <MutedIconButton size="small" onClick={onToggleCollapse}>
            {isCollapsed ? <ExpandMoreIcon sx={{ fontSize: 16 }} /> : <ExpandLessIcon sx={{ fontSize: 16 }} />}
          </MutedIconButton>
        </Tooltip>
      </Box>

      {/* Comments */}
      {areCommentsOpen && (
        <CommentsSection
          testKey={testKey}
          comments={comments}
          onAdd={onAddComment}
          onDelete={onDeleteComment}
        />
      )}

      {/* Data section */}
      {!isCollapsed && (
        test.status === 'pending' ? (
          <Box sx={{ borderTop: '1px solid #eff3f4', px: 2, py: 2 }}>
            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1 }} />
          </Box>
        ) : (
          <>
            <ResultWithAssertions
              inputData={inputData}
              outputData={outputData}
              assertionResults={test.assertion_results ?? []}
            />
            {test.status !== 'pending' && Object.keys(inputData).length > 0 && (
              <Box sx={{ px: 2, pb: 1.5, display: 'flex', gap: 1 }}>
                <ExcelDownloader data={inputData} fileName={`test_${idx + 1}.xlsx`} />
                {onUpload && <ExcelUploader onUpload={onUpload} />}
              </Box>
            )}
          </>
        )
      )}
    </Box>
  );
}

/* ─── Props ──────────────────────────────────────────────────────── */
interface TestsPanelProps {
  onAddTest: () => void;
  onSelectForModification: (idx: number) => void;
  selectedTestIndex: number | null;
  onUpload?: (uploadedData: Record<string, any[]>) => void;
  onSuggestionFill?: (text: string) => void;
  onRerunTest?: (idx: number) => void;
  onOpenChat?: () => void;
  modelId?: string;
  sqlProps?: SqlStripProps;
}

/* ═══════════════════════════════════════════════════════════════════ */
const TestsPanel: React.FC<TestsPanelProps> = ({
  onAddTest, onSelectForModification, selectedTestIndex,
  onUpload, onSuggestionFill, onRerunTest, onOpenChat, modelId,
  sqlProps,
}) => {
  const dispatch = useAppDispatch();
  const currentModelId = useAppSelector((state) => state.appBarModel.currentModelId);
  const testResults: any[] = useAppSelector((state) => state.buildModel.testResults ?? []);

  const {
    editingIndex, setEditingIndex,
    editedDescriptions, setEditedDescriptions,
    collapsed, setCollapsed,
    filter, setFilter,
    compact, setCompact,
    openComments, setOpenComments,
  } = useTestPanelState();

  const commentsKey = `pt_comments_${currentModelId ?? 'new'}`;
  const [allComments, setAllComments] = useLocalStorageState<Record<string, Comment[]>>(commentsKey, {});

  function addComment(testKey: string, text: string) {
    const c: Comment = { id: 'c' + Date.now(), text, author: 'Vous', initials: 'ME', ts: Date.now() };
    setAllComments({ ...allComments, [testKey]: [...(allComments[testKey] ?? []), c] });
    setOpenComments((o) => ({ ...o, [testKey]: true }));
  }
  function deleteComment(testKey: string, id: string) {
    setAllComments({ ...allComments, [testKey]: (allComments[testKey] ?? []).filter((c) => c.id !== id) });
  }

  const persist = (updated: any[]) => {
    dispatch(setTestResults(updated));
    if (currentModelId) dispatch(patchModelTests({ sessionId: currentModelId, tests: updated }));
  };

  const handleDelete = (idx: number) => {
    persist(testResults.filter((_, i) => i !== idx));
    setCollapsed(prev => { const next = new Set(prev); next.delete(idx); return next; });
  };

  const handleSaveEdit = (idx: number) => {
    const newDesc = editedDescriptions[idx];
    setEditingIndex(null);
    setEditedDescriptions((prev) => { const next = { ...prev }; delete next[idx]; return next; });
    if (newDesc === undefined) return;
    persist(testResults.map((t, i) => i === idx ? { ...t, unit_test_description: newDesc } : t));
  };

  const prevTestCountRef = useRef<number | null>(null);

  useEffect(() => {
    if (testResults.length === 0) {
      prevTestCountRef.current = 0;
      return;
    }

    // First time tests appear: navigate to hash if present
    if (prevTestCountRef.current === null) {
      prevTestCountRef.current = testResults.length;
      const hash = window.location.hash.slice(1);
      if (hash) {
        setTimeout(() => {
          document.getElementById(hash)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        }, 0);
      }
      return;
    }

    // Auto-scroll to newly added test
    const prev = prevTestCountRef.current;
    const curr = testResults.length;
    if (curr > prev) {
      const targetId = `test-${curr}`;
      setTimeout(() => {
        const el = document.getElementById(targetId);
        if (el) {
          el.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          window.location.hash = targetId;
        }
      }, 0);
    }
    prevTestCountRef.current = curr;
  }, [testResults.length]);

  const execSummary = useMemo(() => ({
    pass:    testResults.filter((t) => testExecStatus(t) === 'pass').length,
    fail:    testResults.filter((t) => testExecStatus(t) === 'fail').length,
    pending: testResults.filter((t) => testExecStatus(t) === 'pending').length,
  }), [testResults]);

  const counts = useMemo(() => ({
    all:  testResults.length,
    good: testResults.filter((t) => statusToVerdict(t.status, t) === 'good').length,
    warn: testResults.filter((t) => statusToVerdict(t.status, t) === 'warn').length,
    bad:  testResults.filter((t) => statusToVerdict(t.status, t) === 'bad').length,
  }), [testResults]);

  const filteredTests = useMemo(() => testResults
    .map((t, i) => ({ t, i }))
    .filter(({ t }) => filter === 'all' || statusToVerdict(t.status, t) === filter),
    [testResults, filter]);

  const allSuggestions = useMemo(() => {
    const seen = new Set<string>();
    const out: { text: string; tag?: string }[] = [];
    for (const test of testResults) {
      for (const s of (test.suggestions ?? [])) {
        if (!seen.has(s)) { seen.add(s); out.push({ text: s }); }
      }
    }
    return out.slice(-3);
  }, [testResults]);

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      {/* Header */}
      {testResults.length > 0 && (
        <Box sx={{ flexShrink: 0, px: 2, py: 1.25, borderBottom: '1px solid #eee', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
            <Typography variant="body2" sx={{ fontWeight: 700, color: TEAL }}>
              🧪 {testResults.length} test{testResults.length > 1 ? 's' : ''}
            </Typography>
            {execSummary.fail > 0 && (
              <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: '#fbeceb', color: '#d0503f', px: '8px', py: '2px', borderRadius: 999, fontSize: 11, fontWeight: 700 }}>
                <CancelIcon sx={{ fontSize: 11 }} />
                {execSummary.fail} en échec
              </Box>
            )}
            {execSummary.fail === 0 && execSummary.pending === 0 && (
              <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: '#e9f7f0', color: '#23a26d', px: '8px', py: '2px', borderRadius: 999, fontSize: 11, fontWeight: 700 }}>
                <CheckCircleIcon sx={{ fontSize: 11 }} />
                Tous passent
              </Box>
            )}
          </Box>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 'auto' }}>
            {onOpenChat && (
              <Box
                component="button"
                onClick={onOpenChat}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '5px',
                  px: '11px', py: '5px', fontSize: 12, fontWeight: 600,
                  border: `1.2px solid ${BORDER}`, borderRadius: 999,
                  bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit',
                  '&:hover': { borderColor: TEAL, color: TEAL, bgcolor: '#f0fafa' },
                }}
              >
                <AutoAwesomeIcon sx={{ fontSize: 13 }} />
                Demander à MockSQL
              </Box>
            )}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, bgcolor: SURFACE, border: `1px solid ${BORDER}`, borderRadius: 999, p: '2px' }}>
              <Tooltip title="Vue détaillée">
                <Box
                  component="button"
                  onClick={() => setCompact(false)}
                  sx={{
                    display: 'inline-flex', alignItems: 'center', gap: '4px', px: '10px', py: '4px',
                    fontSize: 11.5, borderRadius: 999, border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                    bgcolor: !compact ? TEAL_SUBTLE : 'transparent',
                    color: !compact ? TEAL : MUTED, fontWeight: !compact ? 700 : 500,
                  }}
                >
                  <ViewAgendaIcon sx={{ fontSize: 13 }} /> Détaillé
                </Box>
              </Tooltip>
              <Tooltip title="Vue compacte">
                <Box
                  component="button"
                  onClick={() => setCompact(true)}
                  sx={{
                    display: 'inline-flex', alignItems: 'center', gap: '4px', px: '10px', py: '4px',
                    fontSize: 11.5, borderRadius: 999, border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                    bgcolor: compact ? TEAL_SUBTLE : 'transparent',
                    color: compact ? TEAL : MUTED, fontWeight: compact ? 700 : 500,
                  }}
                >
                  <ViewListIcon sx={{ fontSize: 13 }} /> Compact
                </Box>
              </Tooltip>
            </Box>
          </Box>
        </Box>
      )}

      {/* SQL strip */}
      {sqlProps?.sql && <SqlStrip {...sqlProps} />}

      {/* Empty state */}
      {testResults.length === 0 && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 2, p: 2 }}>
          <Typography variant="body2" sx={{ color: '#999', textAlign: 'center' }}>
            Aucun test généré pour l'instant.
          </Typography>
          <OutlinedPrimaryButton startIcon={<AddIcon />} onClick={onAddTest} size="small">
            Ajouter un test
          </OutlinedPrimaryButton>
        </Box>
      )}

      {/* Scrollable content */}
      {testResults.length > 0 && (
        <Box sx={{ flex: 1, overflowY: 'auto', px: 1.5, pt: 1.5, pb: 1 }}>
          <CoverageBar tests={testResults} />

          {/* Filter chips */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1.25, flexWrap: 'wrap' }}>
            <FilterListIcon sx={{ fontSize: 14, color: PLACEHOLDER }} />
            <FilterChip label="Tous"        count={counts.all}  active={filter === 'all'}  color={MUTED}      onClick={() => setFilter('all')} />
            <FilterChip label="Bon"         count={counts.good} active={filter === 'good'} color="#23a26d"    onClick={() => setFilter('good')} />
            <FilterChip label="Insuffisant" count={counts.warn} active={filter === 'warn'} color="#d89323"    onClick={() => setFilter('warn')} />
            <FilterChip label="Incorrect"   count={counts.bad}  active={filter === 'bad'}  color="#d0503f"    onClick={() => setFilter('bad')} />
          </Box>

          {/* Test list */}
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
            {filteredTests.map(({ t: test, i: idx }) => {
              const testKey = `${idx}`;
              const testComments = allComments[testKey] ?? [];

              if (compact) {
                return (
                  <CompactRow
                    key={idx}
                    test={test}
                    idx={idx}
                    commentCount={testComments.length}
                    onExpand={() => {
                      setCompact(false);
                      setCollapsed(prev => { const next = new Set(prev); next.delete(idx); return next; });
                    }}
                    onAsk={() => onSelectForModification(idx)}
                    onDelete={() => handleDelete(idx)}
                  />
                );
              }

              return (
                <TestCard
                  key={idx}
                  test={test}
                  idx={idx}
                  selectedTestIndex={selectedTestIndex}
                  isEditing={editingIndex === idx}
                  editedDescription={editedDescriptions[idx]}
                  isCollapsed={collapsed.has(idx)}
                  areCommentsOpen={!!openComments[testKey]}
                  comments={testComments}
                  onStartEdit={() => setEditingIndex(idx)}
                  onSaveEdit={() => handleSaveEdit(idx)}
                  onEditDescription={(val) => setEditedDescriptions((prev) => ({ ...prev, [idx]: val }))}
                  onDelete={() => handleDelete(idx)}
                  onToggleCollapse={() => setCollapsed(prev => {
                    const next = new Set(prev);
                    if (next.has(idx)) next.delete(idx); else next.add(idx);
                    return next;
                  })}
                  onToggleComments={() => setOpenComments((o) => ({ ...o, [testKey]: !o[testKey] }))}
                  onAddComment={(text) => addComment(testKey, text)}
                  onDeleteComment={(id) => deleteComment(testKey, id)}
                  onSelectForModification={() => onSelectForModification(idx)}
                  onRerunTest={onRerunTest ? () => onRerunTest(idx) : undefined}
                  onUpload={onUpload}
                />
              );
            })}

            {filteredTests.length === 0 && (
              <Box sx={{ textAlign: 'center', p: '36px 12px', color: PLACEHOLDER, fontSize: 13, bgcolor: SURFACE, border: `1px dashed ${BORDER}`, borderRadius: '12px' }}>
                Aucun test ne correspond à ce filtre.
              </Box>
            )}
          </Box>

          {/* Suggestions */}
          {allSuggestions.length > 0 && (
            <Box sx={{ mt: 2, bgcolor: SURFACE, border: `1px solid ${BORDER}`, borderRadius: '14px', p: '14px 16px' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.25 }}>
                <AutoAwesomeIcon sx={{ fontSize: 14, color: '#2BB0A8' }} />
                <Typography sx={{ fontSize: 13, fontWeight: 700, color: INK }}>Cas suggérés par MockSQL</Typography>
                <Typography sx={{ fontSize: 11, color: PLACEHOLDER, ml: 'auto' }}>Basé sur la couverture</Typography>
              </Box>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
                {allSuggestions.map((s, i) => (
                  <SuggestionRow
                    key={i}
                    text={s.text}
                    tag={s.tag}
                    onFill={() => onSuggestionFill ? onSuggestionFill(s.text) : onAddTest()}
                  />
                ))}
              </Box>
            </Box>
          )}

          {allSuggestions.length === 0 && (
            <Box sx={{ mt: 1.5, px: 0.5 }}>
              <Chip
                label="Ajouter un test"
                size="small"
                clickable
                icon={<AddIcon style={{ fontSize: 12 }} />}
                onClick={onAddTest}
                sx={{ fontSize: 11, height: 24, bgcolor: '#f0fafa', color: TEAL, border: '1px solid #d0eeec', '&:hover': { bgcolor: '#d0eeec' } }}
              />
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
};

export default React.memo(TestsPanel);
