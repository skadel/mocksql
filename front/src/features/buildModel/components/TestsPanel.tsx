import AddIcon from '@mui/icons-material/Add';
import CancelIcon from '@mui/icons-material/Cancel';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DeleteIcon from '@mui/icons-material/Delete';
import EditIcon from '@mui/icons-material/Edit';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import HistoryIcon from '@mui/icons-material/History';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import ReplayIcon from '@mui/icons-material/Replay';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import CommentIcon from '@mui/icons-material/Comment';
import ViewListIcon from '@mui/icons-material/ViewList';
import ViewAgendaIcon from '@mui/icons-material/ViewAgenda';
import FilterListIcon from '@mui/icons-material/FilterList';
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
import { highlight, languages } from 'prismjs';
import 'prismjs/components/prism-sql';
import Editor from 'react-simple-code-editor';
import { patchModelTests } from '../../../api/messages';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import ExcelDownloader from '../../../shared/ExcelDownloader';
import ExcelUploader from '../../../shared/ExcelUploader';
import { setTestResults } from '../buildModelSlice';
import DisplayTable from './DisplayTable';
/* ─── tag colours ─────────────────────────────────────────────────── */
const TAG_COLORS: Record<string, { bg: string; fg: string }> = {
  'Logique métier':     { bg: '#e6f7f6', fg: '#1ca8a4' },
  'Null checks':        { bg: '#fdecea', fg: '#d32f2f' },
  'Cas limites':        { bg: '#fff3e0', fg: '#e65100' },
  'Intégration':        { bg: '#eef1f7', fg: '#50609d' },
  'Valeurs dupliquées': { bg: '#f3e8e6', fg: '#6d4c41' },
  'Performance':        { bg: '#e0f2f1', fg: '#00695c' },
};
function tagStyle(tag: string) {
  return TAG_COLORS[tag] ?? { bg: '#f0f0f0', fg: '#555' };
}

/* ─── verdict helpers ─────────────────────────────────────────────── */
type Verdict = 'good' | 'warn' | 'bad' | 'pending';

function testExpectsEmpty(test: any): boolean {
  const desc = (test.unit_test_description ?? '').toLowerCase();
  return /retourne\s+.{0,40}vide|résultat[s]?\s+(?:est\s+)?vide[s]?|0\s+ligne|aucune\s+ligne/.test(desc);
}

function statusToVerdict(status: string | undefined, test?: any): Verdict {
  if (test?.evaluation) {
    if (/Excellent|Bon/.test(test.evaluation))   return 'good';
    if (/Insuffisant/.test(test.evaluation))     return 'warn';
  }
  if (status === 'complete')      return 'good';
  if (status === 'empty_results') return (test && testExpectsEmpty(test)) ? 'good' : 'warn';
  if (status === 'error')         return 'bad';
  return 'pending';
}

const VERDICT_META: Record<Verdict, { label: string; fg: string; bg: string; border: string }> = {
  good:    { label: 'Bon',         fg: '#23a26d', bg: '#e9f7f0', border: '#23a26d' },
  warn:    { label: 'Insuffisant', fg: '#d89323', bg: '#fcf3e1', border: '#d89323' },
  bad:     { label: 'Incorrect',   fg: '#d0503f', bg: '#fbeceb', border: '#d0503f' },
  pending: { label: 'En attente',  fg: '#888',    bg: '#f4f7f7', border: '#ccc'    },
};

function verdictText(status: string | undefined, test?: any): string {
  if (test?.evaluation) return test.evaluation;
  if (status === 'complete')      return "La requête a produit des résultats sur ces données d'entrée. Le test est valide.";
  if (status === 'empty_results') {
    if (test && testExpectsEmpty(test)) return "La requête n'a retourné aucune ligne, conformément au comportement attendu. Le test est valide.";
    return "La requête n'a retourné aucune ligne. Vérifiez que les données d'entrée déclenchent bien le chemin de calcul attendu.";
  }
  if (status === 'error')         return "La requête a échoué sur ces données. Inspectez les données d'entrée ou la requête SQL.";
  return "En cours d'exécution…";
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

/** Saturation curve: 1 test → 40 %, 2 → 65 %, 3 → 85 %, 4+ → 100 % */
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
    <Box sx={{ bgcolor: '#fff', border: '1px solid #e4eaec', borderRadius: '12px', p: '14px 16px', mb: 1.5 }}>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5 }}>
          <CoverageRing score={score} fg={toneFg} />
          <Box>
            <Typography sx={{ fontSize: 10, fontWeight: 700, color: '#6b8287', textTransform: 'uppercase', letterSpacing: 0.7 }}>Couverture</Typography>
            <Typography sx={{ fontSize: 18, fontWeight: 700, color: '#0f272a', lineHeight: 1.1 }}>
              {score}%
              <Typography component="span" sx={{ fontSize: 11, color: '#6b8287', fontWeight: 500, ml: 1 }}>
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
                <Typography component="span" sx={{ fontSize: 10.5, color: b.n > 0 ? '#3b5357' : '#9aabb0' }}>
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
            <Box sx={{ fontSize: 12, color: '#3b5357' }}>
              <Typography component="span" sx={{ color: '#6b8287', fontSize: 12 }}>Non couvert : </Typography>
              {uncovered.map((b, i) => (
                <Typography key={b.key} component="span" sx={{ fontWeight: 500, fontSize: 12 }}>
                  {b.label}{i < uncovered.length - 1 ? ', ' : ''}
                </Typography>
              ))}
            </Box>
          )}
          {partial.length > 0 && (
            <Box sx={{ fontSize: 12, color: '#3b5357', mt: uncovered.length > 0 ? 0.5 : 0 }}>
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
    <Box sx={{ px: 2, py: 1.5, bgcolor: '#fbfcfc', borderTop: '1px solid #eff3f4' }}>
      <Typography sx={{ fontSize: 10, fontWeight: 700, color: '#6b8287', letterSpacing: 0.6, textTransform: 'uppercase', mb: 1 }}>
        Commentaires d'équipe{comments.length > 0 ? ` · ${comments.length}` : ''}
      </Typography>
      {comments.length === 0 && (
        <Typography sx={{ fontSize: 12, color: '#9aabb0', fontStyle: 'italic', mb: 1 }}>
          Aucun commentaire. Note ici un contexte métier, une décision d'équipe ou un point à vérifier.
        </Typography>
      )}
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, mb: comments.length ? 1.25 : 0 }}>
        {comments.map((c) => (
          <Box key={c.id} sx={{ display: 'flex', gap: 1, alignItems: 'flex-start' }}>
            <Box sx={{ width: 24, height: 24, borderRadius: '50%', bgcolor: '#ecf7f6', color: '#1ca8a4', display: 'grid', placeItems: 'center', fontWeight: 700, fontSize: 10, flexShrink: 0 }}>
              {c.initials}
            </Box>
            <Box sx={{ flex: 1, bgcolor: '#fff', border: '1px solid #e4eaec', borderRadius: '10px', p: '7px 11px' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 0.25 }}>
                <Typography sx={{ fontWeight: 600, fontSize: 11, color: '#3b5357' }}>{c.author}</Typography>
                <Typography sx={{ fontSize: 11, color: '#9aabb0' }}>· {relTime(c.ts)}</Typography>
                <IconButton size="small" onClick={() => onDelete(c.id)} sx={{ ml: 'auto', p: 0.25, color: '#9aabb0', '&:hover': { color: '#d0503f' } }}>
                  <DeleteIcon sx={{ fontSize: 12 }} />
                </IconButton>
              </Box>
              <Typography sx={{ fontSize: 12.5, color: '#0f272a', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>{c.text}</Typography>
            </Box>
          </Box>
        ))}
      </Box>
      <Box sx={{ display: 'flex', gap: 1, alignItems: 'flex-end', bgcolor: '#fff', border: '1px solid #e4eaec', borderRadius: '10px', p: '6px 6px 6px 10px' }}>
        <Box sx={{ width: 22, height: 22, borderRadius: '50%', bgcolor: '#ecf7f6', color: '#1ca8a4', display: 'grid', placeItems: 'center', fontWeight: 700, fontSize: 10, flexShrink: 0 }}>
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
          sx={{ flex: 1, fontSize: 12.5, '& textarea': { fontSize: 12.5, color: '#0f272a', py: 0.5 } }}
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

/* ─── CompactRow ──────────────────────────────────────────────────── */
function CompactRow({ test, idx, commentCount, onExpand, onAsk, onDelete }: {
  test: any; idx: number; commentCount: number;
  onExpand: () => void; onAsk: () => void; onDelete: () => void;
}) {
  const verdict = statusToVerdict(test.status, test);
  const vm = VERDICT_META[verdict];
  const tags: string[] = test.tags ?? [];
  return (
    <Box
      onClick={onExpand}
      sx={{
        bgcolor: '#fff', border: '1px solid #e4eaec', borderLeft: `3px solid ${vm.border}`,
        borderRadius: '10px', display: 'grid',
        gridTemplateColumns: '22px 108px 1fr auto',
        alignItems: 'center', gap: 1, p: '9px 12px', cursor: 'pointer',
        '&:hover': { bgcolor: '#fafcfc' },
      }}
    >
      {/* status dot */}
      <StatusDot status={test.status} test={test} />
      {/* verdict badge */}
      <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: vm.bg, color: vm.fg, px: '8px', py: '2px', borderRadius: 999, fontSize: 11, fontWeight: 700, justifySelf: 'start' }}>
        {verdict === 'good' && <CheckCircleIcon sx={{ fontSize: 11 }} />}
        {verdict === 'warn' && <WarningAmberIcon sx={{ fontSize: 11 }} />}
        {verdict === 'bad'  && <CancelIcon sx={{ fontSize: 11 }} />}
        {vm.label}
      </Box>
      {/* title + first tag */}
      <Box sx={{ minWidth: 0, display: 'flex', alignItems: 'center', gap: 1 }}>
        <Typography sx={{ fontSize: 11, color: '#6b8287', fontVariantNumeric: 'tabular-nums' }}>#{idx + 1}</Typography>
        <Typography sx={{ fontSize: 12.5, color: '#0f272a', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', fontWeight: 500 }}>
          {test.unit_test_description ?? '—'}
        </Typography>
        {tags.slice(0, 1).map((tg) => {
          const tc = tagStyle(tg);
          return <Chip key={tg} label={tg} size="small" sx={{ fontSize: 10, height: 18, bgcolor: tc.bg, color: tc.fg, border: 'none', flexShrink: 0 }} />;
        })}
      </Box>
      {/* actions */}
      <Box sx={{ display: 'flex', gap: 0.25 }} onClick={(e) => e.stopPropagation()}>
        {commentCount > 0 && (
          <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '3px', fontSize: 11, color: '#3b5357', px: '7px', py: '2px', bgcolor: '#f4f7f7', borderRadius: 999, fontWeight: 600, mr: 0.5 }}>
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

function StatusDot({ status, test }: { status: string | undefined; test?: any }) {
  if (status === 'complete')      return <CheckCircleIcon sx={{ fontSize: 18, color: '#23a26d', flexShrink: 0 }} />;
  if (status === 'empty_results') {
    if (test && testExpectsEmpty(test)) return <CheckCircleIcon sx={{ fontSize: 18, color: '#23a26d', flexShrink: 0 }} />;
    return <WarningAmberIcon sx={{ fontSize: 18, color: '#d89323', flexShrink: 0 }} />;
  }
  if (status === 'error')         return <CancelIcon sx={{ fontSize: 18, color: '#d0503f', flexShrink: 0 }} />;
  return <CircularProgress size={14} thickness={5} sx={{ color: '#1ca8a4', flexShrink: 0 }} />;
}

/* ─── AssertionsPanel ────────────────────────────────────────────── */
function AssertionsPanel({ assertions }: { assertions: any[] }) {
  if (!assertions || assertions.length === 0) return null;
  const passCount = assertions.filter((a) => a.passed).length;
  const allPass = passCount === assertions.length;
  return (
    <Box sx={{ px: 2, pb: 1.5, pt: 1 }}>
      <Typography sx={{ fontSize: 11, fontWeight: 700, color: '#3b5357', letterSpacing: 0.5, textTransform: 'uppercase', mb: 0.75 }}>
        Assertions · {passCount}/{assertions.length} {allPass ? '✓' : 'passées'}
      </Typography>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: '5px' }}>
        {assertions.map((a: any, i: number) => (
          <Box
            key={i}
            sx={{
              display: 'flex', alignItems: 'flex-start', gap: 1,
              p: '7px 10px', borderRadius: '8px',
              bgcolor: a.passed ? '#e9f7f0' : '#fbeceb',
              border: `1px solid ${a.passed ? '#b2e0ce' : '#f3c4be'}`,
            }}
          >
            {a.passed
              ? <CheckCircleIcon sx={{ fontSize: 14, color: '#23a26d', mt: '1px', flexShrink: 0 }} />
              : <CancelIcon sx={{ fontSize: 14, color: '#d0503f', mt: '1px', flexShrink: 0 }} />
            }
            <Box sx={{ flex: 1, minWidth: 0 }}>
              <Typography sx={{ fontSize: 12, color: '#0f272a', fontWeight: 500 }}>
                {a.description}
              </Typography>
              {!a.passed && a.failing_rows && a.failing_rows.length > 0 && (
                <Typography sx={{ fontSize: 11, color: '#d0503f', mt: 0.25 }}>
                  {a.failing_rows.length} ligne{a.failing_rows.length > 1 ? 's' : ''} en échec
                </Typography>
              )}
              {!a.passed && a.error && (
                <Typography sx={{ fontSize: 11, color: '#d0503f', mt: 0.25, fontFamily: 'monospace' }}>
                  {a.error}
                </Typography>
              )}
            </Box>
          </Box>
        ))}
      </Box>
    </Box>
  );
}

/* ─── SuggestionRow ──────────────────────────────────────────────── */
function SuggestionRow({ text, tag, onAdd, onFill }: { text: string; tag?: string; onAdd?: () => void; onFill?: () => void }) {
  const tc = tag ? tagStyle(tag) : { bg: '#f4f7f7', fg: '#6b8287' };
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25, p: '9px 11px', border: '1px solid #e4eaec', borderRadius: '10px', bgcolor: '#fafcfc' }}>
      <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: tc.fg, flexShrink: 0 }} />
      <Typography sx={{ flex: 1, fontSize: 12.5, color: '#3b5357', lineHeight: 1.45 }}>{text}</Typography>
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

/* ─── Filter chip ────────────────────────────────────────────────── */
function FilterChip({ label, count, active, color, onClick }: { label: string; count: number; active: boolean; color: string; onClick: () => void }) {
  return (
    <Box
      component="button"
      onClick={onClick}
      sx={{
        display: 'inline-flex', alignItems: 'center', gap: '6px',
        px: '10px', py: '5px', fontSize: 12, cursor: 'pointer', fontFamily: 'inherit',
        border: `1.2px solid ${active ? color : '#e4eaec'}`,
        bgcolor: '#fff', color: '#0f272a', borderRadius: 999, fontWeight: active ? 700 : 500,
        '&:hover': { borderColor: color },
      }}
    >
      <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: color }} />
      {label}
      <Box sx={{ fontSize: 10.5, color: active ? color : '#9aabb0', bgcolor: active ? 'transparent' : '#f4f7f7', px: '6px', borderRadius: 999, fontWeight: 700 }}>
        {count}
      </Box>
    </Box>
  );
}

/* ─── SqlStrip ───────────────────────────────────────────────────── */
interface SqlStripProps {
  sql: string;
  onUpdate?: (newSql: string) => void;
  disabled?: boolean;
  loading?: boolean;
  hasError?: boolean;
  optimizedSql?: string;
  sqlHistory?: SqlHistoryEntry[];
  onHistorySelect?: (entry: SqlHistoryEntry) => void;
  historyRestoreTrigger?: number;
}

function SqlStrip({ sql, onUpdate, disabled, loading, hasError, optimizedSql, sqlHistory, onHistorySelect, historyRestoreTrigger }: SqlStripProps) {
  const [open, setOpen] = useState(true);
  const [editedSql, setEditedSql] = useState(sql);
  const [viewMode, setViewMode] = useState<'raw' | 'optimized'>('raw');
  const [historyAnchor, setHistoryAnchor] = useState<HTMLElement | null>(null);
  const prevDisabled = useRef(disabled);
  const prevTrigger = useRef(historyRestoreTrigger);

  // Collapse when request completes without error
  useEffect(() => {
    if (prevDisabled.current && !disabled && !hasError) setOpen(false);
    prevDisabled.current = disabled;
  }, [disabled, hasError]);

  // Reset to raw view when new optimized SQL arrives
  useEffect(() => {
    if (optimizedSql) setViewMode('raw');
  }, [optimizedSql]);

  // When history is restored externally, open strip with new SQL
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
      <Box sx={{ bgcolor: '#fff', borderBottom: '1px solid #e4eaec', flexShrink: 0 }}>
        {/* Header — always visible */}
        <Box
          onClick={handleToggle}
          sx={{ display: 'flex', alignItems: 'center', gap: 1.5, px: 2, py: '9px', cursor: 'pointer', '&:hover': { bgcolor: '#fafcfc' } }}
        >
          <Typography sx={{ fontSize: 11.5, fontWeight: 700, color: '#1f948d', letterSpacing: 0.5, fontFamily: 'monospace', flexShrink: 0 }}>
            SQL
          </Typography>
          {!open && (
            <Typography sx={{ fontSize: 12, color: '#3b4f52', fontFamily: 'monospace', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', flex: 1, minWidth: 0 }}>
              {truncated}
            </Typography>
          )}
          {open && <Box sx={{ flex: 1 }} />}
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
            {open ? <ExpandLessIcon sx={{ fontSize: 16, color: '#9aabb0' }} /> : <ExpandMoreIcon sx={{ fontSize: 16, color: '#9aabb0' }} />}
          </Box>
        </Box>

        {/* Expanded section */}
        {open && (
          <>
            {/* Original / Optimisé toggle */}
            {showToggle && (
              <Box sx={{ display: 'flex', px: 2, py: 0.75, gap: 0, bgcolor: '#f5fafa', borderBottom: '1px solid #e4eaec' }}>
                <Button
                  size="small"
                  onClick={() => setViewMode('raw')}
                  sx={{
                    fontSize: 11, py: 0.25, px: 1.5, minWidth: 0, textTransform: 'none', fontWeight: 600,
                    borderRadius: '6px 0 0 6px',
                    backgroundColor: !isOptimizedView ? '#1ca8a4' : 'transparent',
                    color: !isOptimizedView ? '#fff' : '#1ca8a4',
                    border: '1px solid #1ca8a4', borderRight: 'none',
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
                    backgroundColor: isOptimizedView ? '#1ca8a4' : 'transparent',
                    color: isOptimizedView ? '#fff' : '#1ca8a4',
                    border: '1px solid #1ca8a4',
                    '&:hover': { backgroundColor: isOptimizedView ? '#159e9a' : '#e8f7f6' },
                  }}
                >
                  Optimisé
                </Button>
              </Box>
            )}

            {/* Editor */}
            <Box
              onKeyDown={handleKeyDown}
              sx={{ maxHeight: 240, overflowY: 'auto', '& .npm__react-simple-code-editor__textarea': { outline: 'none !important' } }}
            >
              <Editor
                value={editorValue}
                onValueChange={(v) => { if (!isOptimizedView) setEditedSql(v); }}
                highlight={(code) => highlight(code, languages.sql, 'sql')}
                padding={14}
                style={{ fontFamily: '"Fira Mono", "Fira Code", monospace', fontSize: 12.5, minHeight: 80 }}
                disabled={disabled || isOptimizedView}
              />
            </Box>

            {/* Footer */}
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

      {/* History popover */}
      <Popover
        open={Boolean(historyAnchor)}
        anchorEl={historyAnchor}
        onClose={() => setHistoryAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
      >
        <Box sx={{ width: 360, maxHeight: 380, overflow: 'auto' }}>
          <Box sx={{ px: 2, py: 1, bgcolor: '#f0fafa', borderBottom: '1px solid #d0eeec' }}>
            <Typography variant="caption" sx={{ fontWeight: 700, color: '#1ca8a4' }}>Historique SQL</Typography>
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
                          <Typography variant="caption" sx={{ fontWeight: 700, color: '#1ca8a4', flexShrink: 0 }}>#{num}</Typography>
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
  sql?: string;
  onSqlUpdate?: (sql: string) => void;
  optimizedSql?: string;
  sqlHistory?: SqlHistoryEntry[];
  onHistorySelect?: (entry: SqlHistoryEntry) => void;
  historyRestoreTrigger?: number;
  sqlDisabled?: boolean;
  sqlLoading?: boolean;
  sqlHasError?: boolean;
}

/* ═══════════════════════════════════════════════════════════════════ */
const TestsPanel: React.FC<TestsPanelProps> = ({
  onAddTest, onSelectForModification, selectedTestIndex, onUpload, onSuggestionFill, onRerunTest, onOpenChat, modelId,
  sql, onSqlUpdate, optimizedSql, sqlHistory, onHistorySelect, historyRestoreTrigger, sqlDisabled, sqlLoading, sqlHasError,
}) => {
  const dispatch = useAppDispatch();
  const currentModelId = useAppSelector((state) => state.appBarModel.currentModelId);
  const testResults: any[] = useAppSelector((state) => state.buildModel.testResults ?? []);
  const storedQuery: string = useAppSelector((state) => state.buildModel.query ?? '');

  const [editingIndex, setEditingIndex] = useState<number | null>(null);
  const [editedDescriptions, setEditedDescriptions] = useState<Record<number, string>>({});
  const [collapsed, setCollapsed] = useState<Set<number>>(new Set());
  const [filter, setFilter] = useState<'all' | 'good' | 'warn' | 'bad'>('all');
  const [compact, setCompact] = useState(false);
  const [openComments, setOpenComments] = useState<Record<number, boolean>>({});

  /* ── comments (localStorage) ── */
  const commentsKey = `pt_comments_${currentModelId ?? 'new'}`;
  const [allComments, setAllComments] = useState<Record<string, Comment[]>>(() => {
    try { return JSON.parse(localStorage.getItem(commentsKey) ?? '{}'); } catch { return {}; }
  });
  function saveComments(next: Record<string, Comment[]>) {
    setAllComments(next);
    try { localStorage.setItem(commentsKey, JSON.stringify(next)); } catch { /* ignore */ }
  }
  function addComment(testKey: string, text: string) {
    const c: Comment = { id: 'c' + Date.now(), text, author: 'Vous', initials: 'ME', ts: Date.now() };
    saveComments({ ...allComments, [testKey]: [...(allComments[testKey] ?? []), c] });
    setOpenComments((o) => ({ ...o, [testKey]: true }));
  }
  function deleteComment(testKey: string, id: string) {
    saveComments({ ...allComments, [testKey]: (allComments[testKey] ?? []).filter((c) => c.id !== id) });
  }

  /* ── persist ── */
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

  /* ── filter counts ── */
  const counts = useMemo(() => ({
    all:  testResults.length,
    good: testResults.filter((t) => statusToVerdict(t.status, t) === 'good').length,
    warn: testResults.filter((t) => statusToVerdict(t.status, t) === 'warn').length,
    bad:  testResults.filter((t) => statusToVerdict(t.status, t) === 'bad').length,
  }), [testResults]);

  const filteredTests = useMemo(() => testResults
    .map((t, i) => ({ t, i }))
    .filter(({ t }) => {
      if (filter === 'all') return true;
      return statusToVerdict(t.status, t) === filter;
    }), [testResults, filter]);

  /* ── suggestions ── */
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
      {/* Header — only when tests exist */}
      {testResults.length > 0 && (
        <Box sx={{ flexShrink: 0, px: 2, py: 1.25, borderBottom: '1px solid #eee', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 1 }}>
          <Typography variant="body2" sx={{ fontWeight: 700, color: '#1ca8a4' }}>
            🧪 {testResults.length} test{testResults.length > 1 ? 's' : ''}
          </Typography>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, ml: 'auto' }}>
            {/* Ask MockSQL button */}
            {onOpenChat && (
              <Box
                component="button"
                onClick={onOpenChat}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '5px',
                  px: '11px', py: '5px', fontSize: 12, fontWeight: 600,
                  border: '1.2px solid #e4eaec', borderRadius: 999,
                  bgcolor: '#fff', color: '#3b5357', cursor: 'pointer', fontFamily: 'inherit',
                  '&:hover': { borderColor: '#1ca8a4', color: '#1ca8a4', bgcolor: '#f0fafa' },
                }}
              >
                <AutoAwesomeIcon sx={{ fontSize: 13 }} />
                Demander à MockSQL
              </Box>
            )}
          {/* Compact / detailed toggle */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, bgcolor: '#f4f7f7', border: '1px solid #e4eaec', borderRadius: 999, p: '2px' }}>
            <Tooltip title="Vue détaillée">
              <Box
                component="button"
                onClick={() => setCompact(false)}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '4px', px: '10px', py: '4px',
                  fontSize: 11.5, borderRadius: 999, border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                  bgcolor: !compact ? '#ecf7f6' : 'transparent',
                  color: !compact ? '#1ca8a4' : '#6b8287', fontWeight: !compact ? 700 : 500,
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
                  bgcolor: compact ? '#ecf7f6' : 'transparent',
                  color: compact ? '#1ca8a4' : '#6b8287', fontWeight: compact ? 700 : 500,
                }}
              >
                <ViewListIcon sx={{ fontSize: 13 }} /> Compact
              </Box>
            </Tooltip>
          </Box>
          </Box>
        </Box>
      )}

      {/* SQL strip — always visible, pinned above scroll area */}
      {sql && (
        <SqlStrip
          sql={sql}
          onUpdate={onSqlUpdate}
          disabled={sqlDisabled}
          loading={sqlLoading}
          hasError={sqlHasError}
          optimizedSql={optimizedSql}
          sqlHistory={sqlHistory}
          onHistorySelect={onHistorySelect}
          historyRestoreTrigger={historyRestoreTrigger}
        />
      )}

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

      {/* Scrollable content — only when tests exist */}
      {testResults.length > 0 && (
      <Box sx={{ flex: 1, overflowY: 'auto', px: 1.5, pt: 1.5, pb: 1 }}>

        {/* Coverage bar */}
        {testResults.length > 0 && <CoverageBar tests={testResults} />}

        {/* Filter chips */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, mb: 1.25, flexWrap: 'wrap' }}>
          <FilterListIcon sx={{ fontSize: 14, color: '#9aabb0' }} />
          <FilterChip label="Tous"        count={counts.all}  active={filter === 'all'}  color="#6b8287" onClick={() => setFilter('all')} />
          <FilterChip label="Bon"         count={counts.good} active={filter === 'good'} color="#23a26d" onClick={() => setFilter('good')} />
          <FilterChip label="Insuffisant" count={counts.warn} active={filter === 'warn'} color="#d89323" onClick={() => setFilter('warn')} />
          <FilterChip label="Incorrect"   count={counts.bad}  active={filter === 'bad'}  color="#d0503f" onClick={() => setFilter('bad')} />
        </Box>

        {/* Test list */}
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
          {filteredTests.map(({ t: test, i: idx }) => {
            const testKey = `${idx}`;
            const testComments = allComments[testKey] ?? [];
            const verdict = statusToVerdict(test.status, test);
            const vm = VERDICT_META[verdict];
            const description = editedDescriptions[idx] ?? test.unit_test_description ?? '';
            const tags: string[] = test.tags ?? [];

            const inputData: Record<string, any[]> = test.data ?? test.test_data ?? {};
            const outputData: any[] = test.results_json
              ? (() => { try { return JSON.parse(test.results_json); } catch { return []; } })()
              : [];

            if (compact) {
              return (
                <CompactRow
                  key={idx}
                  test={test}
                  idx={idx}
                  commentCount={testComments.length}
                  onExpand={() => { setCompact(false); setCollapsed(prev => { const next = new Set(prev); next.delete(idx); return next; }); }}
                  onAsk={() => onSelectForModification(idx)}
                  onDelete={() => handleDelete(idx)}
                />
              );
            }

            return (
              <Box
                key={idx}
                sx={{
                  bgcolor: '#fff',
                  border: '1px solid #e4eaec',
                  borderLeft: `3px solid ${vm.border}`,
                  borderRadius: '12px',
                  overflow: 'hidden',
                  ...(selectedTestIndex === idx && { bgcolor: '#f0fafa' }),
                }}
              >
                {/* Card header */}
                <Box sx={{ p: '14px 16px 10px' }}>
                  {/* Verdict badge + tags row */}
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75, flexWrap: 'wrap', mb: 0.75 }}>
                    {test.status !== 'pending' && (
                      <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: vm.bg, color: vm.fg, px: '8px', py: '3px', borderRadius: 999, fontSize: 11.5, fontWeight: 700 }}>
                        {verdict === 'good' && <CheckCircleIcon sx={{ fontSize: 11 }} />}
                        {verdict === 'warn' && <WarningAmberIcon sx={{ fontSize: 11 }} />}
                        {verdict === 'bad'  && <CancelIcon sx={{ fontSize: 11 }} />}
                        {vm.label}
                      </Box>
                    )}
                    {test.status === 'pending' && (
                      <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', color: '#6b8287', fontSize: 11.5 }}>
                        <CircularProgress size={11} thickness={5} sx={{ color: '#1ca8a4' }} /> En cours…
                      </Box>
                    )}
                    {tags.map((tg) => {
                      const tc = tagStyle(tg);
                      return <Chip key={tg} label={tg} size="small" sx={{ fontSize: 10.5, height: 20, bgcolor: tc.bg, color: tc.fg, border: 'none' }} />;
                    })}
                    <Typography sx={{ fontSize: 11, color: '#9aabb0', ml: 'auto' }}>#{idx + 1}</Typography>
                  </Box>

                  {/* Description — inline editable */}
                  {editingIndex === idx ? (
                    <TextField
                      value={editedDescriptions[idx] ?? test.unit_test_description}
                      onChange={(e) => setEditedDescriptions((prev) => ({ ...prev, [idx]: e.target.value }))}
                      onBlur={() => handleSaveEdit(idx)}
                      onKeyDown={(e) => { e.stopPropagation(); if (e.key === 'Enter' && !e.shiftKey) handleSaveEdit(idx); }}
                      onClick={(e) => e.stopPropagation()}
                      autoFocus fullWidth size="small" variant="standard" multiline minRows={2}
                    />
                  ) : (
                    <Typography sx={{ fontWeight: 600, color: '#0f272a', fontSize: 13.5, lineHeight: 1.5 }}>
                      {description}
                    </Typography>
                  )}

                  {/* Verdict text — always visible */}
                  {test.status && test.status !== 'pending' && (
                    <Box sx={{
                      mt: 1, p: '9px 12px', bgcolor: vm.bg, borderRadius: '8px',
                      borderLeft: `2px solid ${vm.fg}`, fontSize: 12.5, color: '#3b5357', lineHeight: 1.55,
                    }}>
                      <Typography component="span" sx={{ fontWeight: 700, color: vm.fg, fontSize: 12.5 }}>
                        Verdict · {vm.label}
                      </Typography>
                      {' — '}{verdictText(test.status, test)}
                    </Box>
                  )}
                </Box>

                {/* Action bar */}
                <Box sx={{ display: 'flex', gap: 0.5, px: 1.5, pb: 1, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
                  {test.status !== 'pending' && (
                    <>
                      <Tooltip title="Éditer la description">
                        <MutedIconButton size="small" onClick={() => setEditingIndex(idx)}><EditIcon sx={{ fontSize: 14 }} /></MutedIconButton>
                      </Tooltip>
                      <Tooltip title={selectedTestIndex === idx ? 'Sélectionné — écris ton instruction dans le chat' : 'Modifier avec MockSQL'}>
                        {selectedTestIndex === idx
                          ? <TealIconButton size="small" onClick={() => onSelectForModification(idx)}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></TealIconButton>
                          : <MutedIconButton size="small" onClick={() => onSelectForModification(idx)}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></MutedIconButton>
                        }
                      </Tooltip>
                      {onRerunTest && (
                        <Tooltip title="Relancer ce test">
                          <MutedIconButton size="small" onClick={() => onRerunTest(idx)}><ReplayIcon sx={{ fontSize: 14 }} /></MutedIconButton>
                        </Tooltip>
                      )}
                    </>
                  )}
                  {/* Comments toggle */}
                  <Tooltip title="Commentaires d'équipe">
                    <Box
                      component="button"
                      onClick={() => setOpenComments((o) => ({ ...o, [testKey]: !o[testKey] }))}
                      sx={{
                        display: 'inline-flex', alignItems: 'center', gap: '4px',
                        px: '9px', py: '4px', fontSize: 11.5, fontWeight: 600, cursor: 'pointer',
                        border: '1px solid', borderColor: testComments.length ? '#1ca8a4' : '#e4eaec',
                        borderRadius: '7px', bgcolor: openComments[testKey] ? '#ecf7f6' : '#fff',
                        color: testComments.length ? '#1ca8a4' : '#9aabb0', fontFamily: 'inherit',
                        '&:hover': { borderColor: '#1ca8a4', color: '#1ca8a4' },
                      }}
                    >
                      <CommentIcon sx={{ fontSize: 12 }} />
                      Commentaires
                      {testComments.length > 0 && (
                        <Box sx={{ bgcolor: '#0f272a', color: '#fff', fontSize: 10, fontWeight: 700, px: '5px', borderRadius: 999, ml: 0.25 }}>
                          {testComments.length}
                        </Box>
                      )}
                    </Box>
                  </Tooltip>

                  <Box sx={{ width: 1, bgcolor: '#e4eaec', mx: 0.25 }} />

                  <Tooltip title="Supprimer">
                    <DangerIconButton size="small" onClick={() => handleDelete(idx)}><DeleteIcon sx={{ fontSize: 14 }} /></DangerIconButton>
                  </Tooltip>

                  {/* Expand/collapse data */}
                  <Tooltip title={collapsed.has(idx) ? 'Voir les données' : 'Replier les données'}>
                    <MutedIconButton size="small" onClick={() => setCollapsed(prev => { const next = new Set(prev); if (next.has(idx)) next.delete(idx); else next.add(idx); return next; })}>
                      {collapsed.has(idx) ? <ExpandMoreIcon sx={{ fontSize: 16 }} /> : <ExpandLessIcon sx={{ fontSize: 16 }} />}
                    </MutedIconButton>
                  </Tooltip>
                </Box>

                {/* Comments section */}
                {openComments[testKey] && (
                  <CommentsSection
                    testKey={testKey}
                    comments={testComments}
                    onAdd={(text) => addComment(testKey, text)}
                    onDelete={(id) => deleteComment(testKey, id)}
                  />
                )}

                {/* Expanded data section */}
                {!collapsed.has(idx) && (
                  <Box sx={{ borderTop: '1px solid #eff3f4' }}>
                    <AssertionsPanel assertions={test.assertion_results ?? []} />
                    <Box sx={{ px: 2, pb: 2, pt: test.assertion_results?.length ? 0 : 1.5, display: 'flex', flexDirection: 'column', gap: 2 }}>
                      {/* Input data */}
                      <Box>
                        <Typography variant="caption" sx={{ fontWeight: 700, color: '#3b5357', display: 'block', mb: 0.75 }}>
                          Données d'entrée
                        </Typography>
                        {Object.keys(inputData).length > 0 ? (
                          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, overflowX: 'auto' }}>
                            {Object.entries(inputData).map(([key, val]) => (
                              <DisplayTable key={key} jsonData={val as any[]} tableName={key} />
                            ))}
                          </Box>
                        ) : (
                          <Alert severity="info" sx={{ py: 0 }}>Pas de données d'entrée</Alert>
                        )}
                        {test.status !== 'pending' && (
                          <Box sx={{ display: 'flex', gap: 1, mt: 1 }}>
                            <ExcelDownloader data={inputData} fileName={`test_${idx + 1}.xlsx`} />
                            {onUpload && <ExcelUploader onUpload={onUpload} />}
                          </Box>
                        )}
                      </Box>

                      {/* Output */}
                      <Box>
                        <Typography variant="caption" sx={{ fontWeight: 700, color: '#3b5357', display: 'block', mb: 0.75 }}>
                          Résultats
                        </Typography>
                        {test.status === 'pending' ? (
                          <Box>
                            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
                            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
                            <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1 }} />
                          </Box>
                        ) : Array.isArray(outputData) && outputData.length > 0 ? (
                          <Box sx={{ overflowX: 'auto' }}>
                            <DisplayTable jsonData={outputData} tableName={`Résultats test #${idx + 1}`} />
                          </Box>
                        ) : (
                          <Alert severity="info" sx={{ py: 0 }}>Pas de résultats</Alert>
                        )}
                      </Box>
                    </Box>
                  </Box>
                )}
              </Box>
            );
          })}

          {filteredTests.length === 0 && (
            <Box sx={{ textAlign: 'center', p: '36px 12px', color: '#9aabb0', fontSize: 13, bgcolor: '#fff', border: '1px dashed #e4eaec', borderRadius: '12px' }}>
              Aucun test ne correspond à ce filtre.
            </Box>
          )}
        </Box>

        {/* Suggestions */}
        {allSuggestions.length > 0 && (
          <Box sx={{ mt: 2, bgcolor: '#fff', border: '1px solid #e4eaec', borderRadius: '14px', p: '14px 16px' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1.25 }}>
              <AutoAwesomeIcon sx={{ fontSize: 14, color: '#2BB0A8' }} />
              <Typography sx={{ fontSize: 13, fontWeight: 700, color: '#0f272a' }}>Cas suggérés par MockSQL</Typography>
              <Typography sx={{ fontSize: 11, color: '#9aabb0', ml: 'auto' }}>Basé sur la couverture</Typography>
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

        {/* Fallback add button when no suggestions */}
        {allSuggestions.length === 0 && (
          <Box sx={{ mt: 1.5, px: 0.5 }}>
            <Chip
              label="Ajouter un test"
              size="small"
              clickable
              icon={<AddIcon style={{ fontSize: 12 }} />}
              onClick={onAddTest}
              sx={{ fontSize: 11, height: 24, bgcolor: '#f0fafa', color: '#1ca8a4', border: '1px solid #d0eeec', '&:hover': { bgcolor: '#d0eeec' } }}
            />
          </Box>
        )}
      </Box>
      )}
    </Box>
  );
};

export default React.memo(TestsPanel);
