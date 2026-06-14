import AccessTimeIcon from '@mui/icons-material/AccessTime';
import AddIcon from '@mui/icons-material/Add';
import CancelIcon from '@mui/icons-material/Cancel';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import DeleteIcon from '@mui/icons-material/Delete';
import ThumbDownOutlinedIcon from '@mui/icons-material/ThumbDownOutlined';
import EditIcon from '@mui/icons-material/Edit';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ExpandLessIcon from '@mui/icons-material/ExpandLess';
import HistoryIcon from '@mui/icons-material/History';
import LinkIcon from '@mui/icons-material/Link';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import ReplayIcon from '@mui/icons-material/Replay';
import WarningAmberIcon from '@mui/icons-material/WarningAmber';
import CommentIcon from '@mui/icons-material/Comment';
import FilterListIcon from '@mui/icons-material/FilterList';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import DifferenceIcon from '@mui/icons-material/Difference';
import {
  Box,
  Button,
  Chip,
  CircularProgress,
  Dialog,
  DialogActions,
  DialogContent,
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
import { DangerIconButton, MutedIconButton, TealIconButton } from '../../../style/AppButtons';
import { SqlHistoryEntry } from '../../../utils/types';
import React, { useState, useMemo, useRef, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import SqlEditor from '../../../shared/SqlEditor';
import { patchModelTests, applyAssertions } from '../../../api/messages';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { relativeDate } from '../../../utils/dates';
import ExcelDownloader from '../../../shared/ExcelDownloader';
import ExcelUploader from '../../../shared/ExcelUploader';
import HtmlExporter from '../../../shared/HtmlExporter';
import { setTestResults } from '../buildModelSlice';
import DisplayTable from './DisplayTable';
import { useLocalStorageState } from '../../../hooks/useLocalStorageState';
import { useTestPanelState } from '../hooks/useTestPanelState';
import {
  statusToVerdict,
  testExecStatus,
  getVerdictInfo,
} from '../../../utils/verdict';
import { TEAL, TEAL_ALT, INK, BODY, MUTED, PLACEHOLDER, BORDER, SURFACE, TEAL_SUBTLE, AMBER, AMBER_BG, GREEN, GREEN_BG } from '../../../theme/tokens';

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
// v15 coverage axes (cf. design-v15-spec §4 + data.js:coverageAxes).
// Drops happy/equal/types ; adds bornes/doublons/volumetrie.
const COVERAGE_AXES = [
  { key: 'null',       label: 'Valeurs NULL',      hint: 'colonnes manquantes / vides' },
  { key: 'vide',       label: 'Fenêtre vide',      hint: 'aucune ligne sur la période' },
  { key: 'ex_aequo',   label: 'Ex æquo',           hint: 'égalités de tri / départage' },
  { key: 'bornes',     label: 'Bornes & négatifs', hint: '0, valeurs négatives, hors plage' },
  { key: 'doublons',   label: 'Doublons',          hint: 'lignes dupliquées en entrée' },
  { key: 'volumetrie', label: 'Volumétrie',        hint: '1 ligne vs. N lignes' },
];

const AXIS_KEYS = new Set(COVERAGE_AXES.map((a) => a.key));

// Coverage prefers backend-declared axes (the v15 model: each test carries
// `axes: string[]`). Until the backend tags them, we fall back to regex
// heuristics over the test title + tags. See design-v15-spec §4.
function detectCoveredAxes(tests: any[]): Set<string> {
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
    if (/null.checks|null|manquant|absent/.test(s))                                          covered.add('null');
    if (/vide|aucune|inexistant|0.ligne|z[ée]ro|sans.donn[ée]es|ensemble.vide|fen[êe]tre.vide|plage.vide/.test(s)) covered.add('vide');
    if (/ex.[æa]quo|\btie\b|classement|d[ée]partage|non.d[ée]terministe/.test(s))            covered.add('ex_aequo');
    if (/borne|n[ée]gatif|hors.plage|hors.borne|d[ée]bordement|overflow|valeur.limite/.test(s)) covered.add('bornes');
    if (/doublon|dupliqu|duplicate|m[êe]me.cl[ée]/.test(s))                                   covered.add('doublons');
    if (/volum|cardinalit|une.seule.ligne|1.ligne|plusieurs.lignes|n.lignes|grand.volume/.test(s)) covered.add('volumetrie');
  });
  return covered;
}

/* ─── CoverageGrid ────────────────────────────────────────────────── */
// Conservé volontairement : couverture 6 axes dépriorisée (cf. CLAUDE.md), non montée mais gardée.
// eslint-disable-next-line @typescript-eslint/no-unused-vars
function CoverageGrid({ tests, onSuggestionClick }: { tests: any[]; onSuggestionClick?: (text: string) => void }) {
  const covered = useMemo(() => detectCoveredAxes(tests), [tests]);
  const n = covered.size;
  const total = COVERAGE_AXES.length;
  const pct = total > 0 ? Math.round((n / total) * 100) : 0;
  const gaps = total - n;

  return (
    <Box data-testid="coverage-bar" sx={{ border: `1px solid ${BORDER}`, borderRadius: '12px', bgcolor: SURFACE, p: '14px 15px', mb: 1.5 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: '11px' }}>
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={TEAL} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
          <circle cx="12" cy="12" r="10"/><circle cx="12" cy="12" r="6"/><circle cx="12" cy="12" r="2"/>
        </svg>
        <Typography sx={{ fontSize: 13.5, fontWeight: 600, color: INK }}>Couverture des cas limites</Typography>
        <Box sx={{ ml: 'auto', fontSize: 12, fontWeight: 600, color: BODY, display: 'flex', gap: '3px' }}>
          {n}/{total} axes
          {gaps > 0 && <Box component="span" sx={{ color: AMBER, fontWeight: 500 }}> · {gaps} à couvrir</Box>}
        </Box>
      </Box>

      {/* Progress bar */}
      <Box sx={{ height: 5, borderRadius: 999, bgcolor: BORDER, overflow: 'hidden', mb: '13px' }}>
        <Box sx={{ height: '100%', width: `${pct}%`, bgcolor: TEAL_ALT, borderRadius: 999, transition: 'width .4s ease' }} />
      </Box>

      {/* 2-column grid of axes */}
      <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '7px' }}>
        {COVERAGE_AXES.map((ax) => {
          const ok = covered.has(ax.key);
          return (
            <Box key={ax.key} sx={{
              display: 'flex', alignItems: 'flex-start', gap: '8px', p: '8px 10px',
              borderRadius: '9px',
              border: `1px solid ${ok ? BORDER : '#f0d890'}`,
              bgcolor: ok ? '#fff' : AMBER_BG,
            }}>
              <Box sx={{
                width: 19, height: 19, borderRadius: '50%', display: 'grid', placeItems: 'center',
                flexShrink: 0, mt: '1px',
                bgcolor: ok ? GREEN_BG : '#f6e3bd',
                color: ok ? GREEN : AMBER,
              }}>
                {ok
                  ? <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M20 6 9 17l-5-5"/></svg>
                  : <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><circle cx="12" cy="12" r="10"/></svg>
                }
              </Box>
              <Box sx={{ flex: 1, minWidth: 0 }}>
                <Typography sx={{ display: 'block', fontSize: 12.5, fontWeight: 600, color: INK, lineHeight: 1.3 }}>{ax.label}</Typography>
                <Typography sx={{ display: 'block', fontSize: 11, color: MUTED, mt: '1px', lineHeight: 1.4 }}>{ax.hint}</Typography>
              </Box>
              {!ok && onSuggestionClick && (
                <Box
                  component="button"
                  onClick={() => onSuggestionClick(`Ajoute un test pour le cas « ${ax.label} » — ${ax.hint}`)}
                  sx={{
                    alignSelf: 'center', display: 'inline-flex', alignItems: 'center', gap: '3px',
                    fontSize: 11, fontWeight: 600, color: AMBER, bgcolor: 'transparent', border: 'none',
                    cursor: 'pointer', fontFamily: 'inherit', whiteSpace: 'nowrap',
                    p: '3px 5px', borderRadius: '6px',
                    '&:hover': { bgcolor: '#fef3e0' },
                  }}
                >
                  <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <circle cx="12" cy="12" r="10"/><path d="M8 12h8"/><path d="M12 8v8"/>
                  </svg>
                  Tester
                </Box>
              )}
            </Box>
          );
        })}
      </Box>
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

function CommentsSection({ comments, onAdd, onDelete }: {
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
      data-testid={`test-card-${idx + 1}`}
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
  expected_condition?: string;
  sql?: string;
  passed: boolean;
  failing_rows?: any[];
  error?: string;
}

function AssertionRow({ a, expanded, onToggle, onDelete, onEdit, editable, disabled }: {
  a: AssertionItem;
  expanded: boolean;
  onToggle: () => void;
  onDelete: () => void;
  onEdit?: (patch: { description: string; expected_condition: string }) => void;
  editable?: boolean;
  disabled?: boolean;
}) {
  const statusColor = a.passed ? '#23a26d' : '#d0503f';
  const statusBg    = a.passed ? '#eaf5f0' : '#fbeceb';
  const failCount   = a.failing_rows?.length ?? 0;
  const [showSql, setShowSql] = useState(false);
  const [editing, setEditing] = useState(false);
  const [draftDesc, setDraftDesc] = useState(a.description ?? '');
  const [draftCond, setDraftCond] = useState(a.expected_condition ?? '');

  function startEdit(e: React.MouseEvent) {
    e.stopPropagation();
    setDraftDesc(a.description ?? '');
    setDraftCond(a.expected_condition ?? '');
    setEditing(true);
  }
  function saveEdit() {
    const cond = draftCond.trim();
    if (!cond) return;
    onEdit?.({ description: draftDesc.trim(), expected_condition: cond });
    setEditing(false);
  }
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
          {editing ? (
            <Box sx={{ mb: 1 }}>
              <Typography sx={{ fontSize: 10.5, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.4 }}>
                Description (lisible)
              </Typography>
              <TextField
                value={draftDesc}
                onChange={(e) => setDraftDesc(e.target.value)}
                onClick={(e) => e.stopPropagation()}
                fullWidth size="small" multiline minRows={1}
                sx={{ mb: 1, '& .MuiInputBase-input': { fontSize: 12.5 } }}
              />
              <Typography sx={{ fontSize: 10.5, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.4 }}>
                Condition attendue (vraie pour chaque ligne)
              </Typography>
              <TextField
                value={draftCond}
                onChange={(e) => setDraftCond(e.target.value)}
                onClick={(e) => e.stopPropagation()}
                onKeyDown={(e) => { e.stopPropagation(); if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); saveEdit(); } }}
                placeholder="ex : amount > 0"
                fullWidth size="small" multiline minRows={1}
                sx={{ mb: 1, '& .MuiInputBase-input': { fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace" } }}
              />
              <Box sx={{ display: 'flex', gap: 1 }}>
                <Box component="button" onClick={(e) => { e.stopPropagation(); saveEdit(); }} disabled={disabled || !draftCond.trim()}
                  sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '10px', py: '4px', fontSize: 11, fontWeight: 600, border: 'none', borderRadius: '7px', bgcolor: draftCond.trim() ? '#2BB0A8' : '#c8d2d4', color: '#fff', cursor: draftCond.trim() ? 'pointer' : 'not-allowed', fontFamily: 'inherit' }}>
                  Enregistrer
                </Box>
                <Box component="button" onClick={(e) => { e.stopPropagation(); setEditing(false); }}
                  sx={{ px: '10px', py: '4px', fontSize: 11, fontWeight: 500, border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit' }}>
                  Annuler
                </Box>
              </Box>
            </Box>
          ) : a.expected_condition && (
            <Box sx={{ mb: 1 }}>
              <Typography sx={{ fontSize: 10.5, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.4 }}>
                Condition attendue (vraie pour chaque ligne)
              </Typography>
              <Box component="pre" sx={{ m: 0, p: '8px 10px', fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace", bgcolor: '#eef2f3', borderRadius: '7px', overflowX: 'auto', color: '#2b3b3e', lineHeight: 1.5, border: '1px solid #dce4e6' }}>
                {a.expected_condition}
              </Box>
            </Box>
          )}
          {a.sql && (
            <Box sx={{ mb: 1 }}>
              <Box component="button" onClick={(e) => { e.stopPropagation(); setShowSql((v) => !v); }}
                sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', p: 0, mb: showSql ? 0.5 : 0, border: 'none', bgcolor: 'transparent', color: MUTED, cursor: 'pointer', fontFamily: 'inherit', fontSize: 10.5, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.04em', '&:hover': { color: INK } }}>
                <Box sx={{ transform: showSql ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.15s', display: 'inline-flex' }}>
                  <ExpandMoreIcon sx={{ fontSize: 13 }} />
                </Box>
                Requête de validation
              </Box>
              {showSql && (
                <Box component="pre" sx={{ m: 0, p: '8px 10px', fontSize: 11, fontFamily: "'JetBrains Mono', monospace", bgcolor: '#f4f6f7', borderRadius: '7px', overflowX: 'auto', color: MUTED, lineHeight: 1.5, border: '1px solid #e6ebec' }}>
                  {a.sql}
                </Box>
              )}
            </Box>
          )}
          {!editing && (
            <Box sx={{ display: 'flex', gap: 1 }}>
              {editable && onEdit && (
                <Box component="button" onClick={startEdit} disabled={disabled}
                  sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '9px', py: '4px', fontSize: 11, fontWeight: 500, border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', color: '#6941c6', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#f4f0fb', borderColor: '#6941c6' } }}>
                  <EditIcon sx={{ fontSize: 12 }} /> Modifier
                </Box>
              )}
              <Box component="button" onClick={(e) => { e.stopPropagation(); onDelete(); }} disabled={disabled}
                sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '9px', py: '4px', fontSize: 11, fontWeight: 500, border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', color: '#d0503f', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#fbeceb', borderColor: '#d0503f' } }}>
                <DeleteIcon sx={{ fontSize: 12 }} /> Supprimer
              </Box>
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
}

function ResultWithAssertions({ inputData, outputData, assertionResults, onEditAssertions, onApplyAssertions }: {
  inputData: Record<string, any[]>;
  outputData: any[];
  assertionResults: AssertionItem[];
  onEditAssertions?: () => void;
  onApplyAssertions?: (assertions: { description: string; expected_condition: string }[]) => Promise<void> | void;
}) {
  const [expandedSet, setExpandedSet] = useState<Set<number>>(() => {
    const s = new Set<number>();
    assertionResults.forEach((a, i) => { if (!a.passed) s.add(i); });
    return s;
  });
  const [localAssertions, setLocalAssertions] = useState<AssertionItem[]>(assertionResults);
  const [applying, setApplying] = useState(false);
  const [adding, setAdding] = useState(false);
  const [newDesc, setNewDesc] = useState('');
  const [newCond, setNewCond] = useState('');

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

  // Toute mutation (edit/delete/add) recompose la liste complète puis ré-exécute côté backend.
  async function applyList(next: AssertionItem[]) {
    if (!onApplyAssertions) {
      setLocalAssertions(next); // fallback local-only (pas de persistance)
      return;
    }
    setLocalAssertions(next); // optimiste ; réconcilié par le useEffect au retour serveur
    setApplying(true);
    try {
      await onApplyAssertions(
        next.map(a => ({ description: a.description ?? '', expected_condition: a.expected_condition ?? '' })),
      );
    } finally {
      setApplying(false);
    }
  }

  function deleteAssertion(i: number) {
    applyList(localAssertions.filter((_, j) => j !== i));
  }

  function editAssertion(i: number, patch: { description: string; expected_condition: string }) {
    applyList(localAssertions.map((a, j) => (j === i ? { ...a, ...patch } : a)));
  }

  function addAssertion() {
    const cond = newCond.trim();
    if (!cond) return;
    applyList([...localAssertions, { description: newDesc.trim(), expected_condition: cond, passed: true }]);
    setNewDesc(''); setNewCond(''); setAdding(false);
  }

  const passCount = localAssertions.filter(a => a.passed).length;
  const failCount = localAssertions.filter(a => !a.passed).length;
  const hasInput = Object.keys(inputData).length > 0;
  const hasOutput = outputData.length > 0;
  const hasAssertions = localAssertions.length > 0;

  if (!hasInput && !hasOutput && !hasAssertions && !onApplyAssertions) return null;

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
      {(hasAssertions || !!onApplyAssertions) && (
        <Box sx={{ px: 2, pb: 1.5 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mb: 0.75 }}>
            <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6 }}>
              3 · Assertions sur ce résultat
            </Typography>
            {applying && <CircularProgress size={12} thickness={5} sx={{ color: TEAL, ml: 0.25 }} />}
            {onEditAssertions && (
              <Tooltip title="Régénérer toutes les assertions via MockSQL">
                <MutedIconButton size="small" onClick={onEditAssertions} sx={{ color: '#6941c6', ml: 0.25 }}>
                  <AutoAwesomeIcon sx={{ fontSize: 13 }} />
                </MutedIconButton>
              </Tooltip>
            )}
          </Box>
          {hasAssertions && (
            <Box sx={{ border: `1px solid ${BORDER}`, borderRadius: '8px', overflow: 'hidden' }}>
              {localAssertions.map((a, i) => (
                <AssertionRow
                  key={i}
                  a={a}
                  expanded={expandedSet.has(i)}
                  onToggle={() => toggle(i)}
                  onDelete={() => deleteAssertion(i)}
                  onEdit={(patch) => editAssertion(i, patch)}
                  editable={!!onApplyAssertions}
                  disabled={applying}
                />
              ))}
            </Box>
          )}

          {/* Ajouter une assertion */}
          {onApplyAssertions && (
            adding ? (
              <Box sx={{ mt: 1, p: '10px 12px', border: `1px solid ${BORDER}`, borderRadius: '8px', bgcolor: '#fafbfc' }}>
                <Typography sx={{ fontSize: 10.5, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.4 }}>
                  Description (lisible)
                </Typography>
                <TextField
                  value={newDesc} onChange={(e) => setNewDesc(e.target.value)}
                  placeholder="ex : Le montant total est toujours positif"
                  fullWidth size="small" multiline minRows={1}
                  sx={{ mb: 1, '& .MuiInputBase-input': { fontSize: 12.5 } }}
                />
                <Typography sx={{ fontSize: 10.5, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.04em', mb: 0.4 }}>
                  Condition attendue (vraie pour chaque ligne)
                </Typography>
                <TextField
                  value={newCond} onChange={(e) => setNewCond(e.target.value)}
                  onKeyDown={(e) => { if (e.key === 'Enter' && (e.metaKey || e.ctrlKey)) { e.preventDefault(); addAssertion(); } }}
                  placeholder="ex : amount > 0"
                  fullWidth size="small" multiline minRows={1}
                  sx={{ mb: 1, '& .MuiInputBase-input': { fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace" } }}
                />
                <Box sx={{ display: 'flex', gap: 1 }}>
                  <Box component="button" onClick={addAssertion} disabled={applying || !newCond.trim()}
                    sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '10px', py: '4px', fontSize: 11, fontWeight: 600, border: 'none', borderRadius: '7px', bgcolor: newCond.trim() ? '#2BB0A8' : '#c8d2d4', color: '#fff', cursor: newCond.trim() ? 'pointer' : 'not-allowed', fontFamily: 'inherit' }}>
                    Ajouter
                  </Box>
                  <Box component="button" onClick={() => { setAdding(false); setNewDesc(''); setNewCond(''); }}
                    sx={{ px: '10px', py: '4px', fontSize: 11, fontWeight: 500, border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit' }}>
                    Annuler
                  </Box>
                </Box>
              </Box>
            ) : (
              <Box component="button" onClick={() => setAdding(true)} disabled={applying}
                sx={{ mt: 1, display: 'inline-flex', alignItems: 'center', gap: '5px', px: '10px', py: '5px', fontSize: 11.5, fontWeight: 600, border: `1px dashed ${BORDER}`, borderRadius: '8px', bgcolor: '#fff', color: TEAL, cursor: 'pointer', fontFamily: 'inherit', '&:hover': { borderColor: TEAL, bgcolor: '#f0fafa' } }}>
                <AddIcon sx={{ fontSize: 13 }} /> Ajouter une assertion
              </Box>
            )
          )}
        </Box>
      )}
    </Box>
  );
}

/* ─── SuggestionRow ──────────────────────────────────────────────── */
function SuggestionRow({ text, tag, rationale, onAdd, onFill, onDismiss }: { text: string; tag?: string; rationale?: string; onAdd?: () => void; onFill?: () => void; onDismiss?: () => void }) {
  const tc = tag ? tagStyle(tag) : { bg: SURFACE, fg: MUTED };
  const isProd = /^\[PROD\]\s*/i.test(text);
  const displayText = isProd ? text.replace(/^\[PROD\]\s*/i, '') : text;
  const [prodAnchor, setProdAnchor] = useState<HTMLElement | null>(null);
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25, p: '9px 11px', border: `1px solid ${BORDER}`, borderRadius: '10px', bgcolor: '#fafcfc' }}>
      <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: isProd ? TEAL : tc.fg, flexShrink: 0 }} />
      {isProd && (
        <>
          <Tooltip title={rationale ? 'Pourquoi ce cas vient des données réelles — cliquer' : 'Cas ancré sur les données de production'}>
            <Chip
              label="PROD"
              size="small"
              onClick={rationale ? (e) => setProdAnchor(e.currentTarget) : undefined}
              sx={{
                fontSize: 9.5, height: 18, fontWeight: 700, letterSpacing: 0.3,
                bgcolor: TEAL, color: '#fff', border: 'none', flexShrink: 0,
                cursor: rationale ? 'pointer' : 'default',
                '& .MuiChip-label': { px: '7px' },
              }}
            />
          </Tooltip>
          <Popover
            open={Boolean(prodAnchor)}
            anchorEl={prodAnchor}
            onClose={() => setProdAnchor(null)}
            anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
            transformOrigin={{ vertical: 'top', horizontal: 'left' }}
          >
            <Box sx={{ p: '10px 12px', maxWidth: 320 }}>
              <Typography sx={{ fontSize: 10, fontWeight: 700, color: TEAL, mb: 0.5, letterSpacing: 0.4 }}>
                ANCRÉ SUR LES DONNÉES RÉELLES
              </Typography>
              <Typography sx={{ fontSize: 12, color: BODY, lineHeight: 1.5 }}>{rationale}</Typography>
            </Box>
          </Popover>
        </>
      )}
      <Typography sx={{ flex: 1, fontSize: 12.5, color: BODY, lineHeight: 1.45 }}>{displayText}</Typography>
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
      {onDismiss && (
        <Tooltip title="Non pertinent — ne plus suggérer">
          <Box
            component="button"
            onClick={onDismiss}
            sx={{
              display: 'inline-flex', alignItems: 'center',
              p: '5px', bgcolor: 'transparent', color: MUTED, border: 'none',
              borderRadius: '6px', cursor: 'pointer', flexShrink: 0,
              '&:hover': { bgcolor: '#fef2f2', color: '#e57373' },
            }}
          >
            <ThumbDownOutlinedIcon sx={{ fontSize: 13 }} />
          </Box>
        </Tooltip>
      )}
    </Box>
  );
}

/* ─── SuggestionsSection ─────────────────────────────────────────── */
/* Panneau dédié des suggestions de couverture (hors fil de conversation).
 * Source : state.buildModel.suggestions (champ modèle, chargé via getMessages).
 * « Ajouter » consomme la suggestion (→ test) ; « Régénérer » en demande de nouvelles. */
function SuggestionsSection({ suggestions, rationales, onAdd, onDismiss, onRegenerate, regenerating, highlighted, boxRef }: {
  suggestions: string[];
  rationales?: Record<string, string>;
  onAdd?: (text: string) => void;
  onDismiss?: (text: string) => void;
  onRegenerate?: () => void;
  regenerating?: boolean;
  highlighted?: boolean;
  boxRef?: React.RefObject<HTMLDivElement>;
}) {
  const isEmpty = suggestions.length === 0;
  return (
    <Box
      ref={boxRef}
      sx={{
        mt: 1.5,
        border: `1px solid ${highlighted ? TEAL : BORDER}`,
        borderRadius: '12px',
        bgcolor: SURFACE,
        p: '12px 13px',
        transition: 'border-color 0.5s ease, box-shadow 0.5s ease',
        boxShadow: highlighted ? `0 0 0 3px ${TEAL}22` : 'none',
      }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
        <AutoAwesomeIcon sx={{ fontSize: 15, color: TEAL }} />
        <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: INK }}>
          Cas suggérés
        </Typography>
        <Box sx={{ ml: 'auto' }}>
          <Tooltip title="Régénérer des suggestions">
            <span>
              <Box
                component="button"
                onClick={onRegenerate}
                disabled={regenerating || !onRegenerate}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '5px',
                  px: '9px', py: '4px', fontSize: 11.5, fontWeight: 600,
                  border: `1.2px solid ${BORDER}`, borderRadius: 999,
                  bgcolor: '#fff', color: BODY, fontFamily: 'inherit',
                  cursor: regenerating ? 'default' : 'pointer',
                  opacity: regenerating ? 0.6 : 1,
                  '&:hover': { borderColor: TEAL, color: TEAL, bgcolor: '#f0fafa' },
                }}
              >
                {regenerating
                  ? <CircularProgress size={11} thickness={5} sx={{ color: TEAL }} />
                  : <ReplayIcon sx={{ fontSize: 13 }} />}
                Régénérer
              </Box>
            </span>
          </Tooltip>
        </Box>
      </Box>
      {isEmpty ? (
        <Box sx={{ textAlign: 'center', py: '14px', px: '8px' }}>
          <Typography sx={{ fontSize: 12, color: BODY, lineHeight: 1.5 }}>
            Plus aucune suggestion valide pour l'instant.
          </Typography>
          <Typography sx={{ fontSize: 11.5, color: PLACEHOLDER, mt: 0.5 }}>
            Voulez-vous régénérer d'autres suggestions ?
          </Typography>
        </Box>
      ) : (
        <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.75 }}>
          {suggestions.map((s, i) => (
            <SuggestionRow key={i} text={s} rationale={rationales?.[s]} onAdd={() => onAdd?.(s)} onDismiss={onDismiss ? () => onDismiss(s) : undefined} />
          ))}
        </Box>
      )}
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
  collapseSignal?: number;
  sqlFileName?: string;
  hasTests?: boolean;
}

function SqlStrip({ sql, disabled, hasError, optimizedSql, sqlHistory, onHistorySelect, historyRestoreTrigger, collapseSignal, sqlFileName, hasTests }: SqlStripProps) {
  // Démarre fermé dès qu'il y a des tests : on ne déroule le SQL que sur action explicite.
  const [open, setOpen] = useState(() => !hasTests);
  const [viewMode, setViewMode] = useState<'raw' | 'optimized'>('raw');
  const [historyAnchor, setHistoryAnchor] = useState<HTMLElement | null>(null);
  const prevDisabled = useRef(disabled);
  const prevTrigger = useRef(historyRestoreTrigger);
  const prevCollapseSignal = useRef(collapseSignal);
  // Vrai uniquement quand l'utilisateur a explicitement déroulé le SQL (clic).
  const userExpandedRef = useRef(false);

  useEffect(() => {
    if (prevDisabled.current && !disabled && !hasError) setOpen(false);
    prevDisabled.current = disabled;
  }, [disabled, hasError]);

  // Quand des tests apparaissent et que l'utilisateur n'a pas déroulé le SQL : refermer.
  useEffect(() => {
    if (hasTests && !userExpandedRef.current) setOpen(false);
  }, [hasTests]);

  useEffect(() => {
    if (optimizedSql) setViewMode('raw');
  }, [optimizedSql]);

  useEffect(() => {
    if (historyRestoreTrigger !== undefined && historyRestoreTrigger !== prevTrigger.current) {
      prevTrigger.current = historyRestoreTrigger;
      setViewMode('raw');
      setOpen(true);
    }
   
  }, [historyRestoreTrigger]);

  useEffect(() => {
    if (collapseSignal !== undefined && collapseSignal !== prevCollapseSignal.current) {
      prevCollapseSignal.current = collapseSignal;
      setOpen(false);
    }
   
  }, [collapseSignal]);

  const lines = sql.split('\n');
  const first = lines.find((l) => l.trim()) ?? '';
  const second = lines.slice(lines.indexOf(first) + 1).find((l) => l.trim())?.trim() ?? '';
  const preview = [first, second].filter(Boolean).join(' ');
  const truncated = preview.length > 80 ? preview.slice(0, 80) + '…' : preview;

  const handleToggle = () => {
    if (!open) setViewMode('raw');
    userExpandedRef.current = !open;
    setOpen((v) => !v);
  };

  const showToggle = !!optimizedSql && optimizedSql.trim() !== sql.trim();
  const isOptimizedView = viewMode === 'optimized';
  const editorValue = isOptimizedView ? (optimizedSql ?? '') : sql;
  const hasHistory = sqlHistory && sqlHistory.length > 0;

  return (
    <>
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
              onChange={() => {}}
              disabled={true}
              maxHeight={240}
              fontSize={12.5}
              minHeight={80}
            />
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
/* ─── DecisionBlock ────────────────────────────────────────────────────
 * v15 « décision métier figée » (design-v15-spec §5). Optional: only renders
 * when the backend attaches a `decision` to the test. Masked otherwise.
 * Fields: question, decision, decidedBy[{initials,color}], decidedAt, inProdShare.
 */
function DecisionBlock({ test }: { test: any }) {
  const decidedBy: { initials: string; color?: string }[] = test.decidedBy ?? [];
  return (
    <Box sx={{ mt: 1, bgcolor: '#fff', border: `1px solid ${BORDER}`, borderRadius: '12px', p: '13px 15px' }}>
      {test.question && (
        <Typography sx={{ fontSize: 12.5, color: BODY, fontStyle: 'italic', mb: '9px', lineHeight: 1.5 }}>
          {test.question}
        </Typography>
      )}
      <Typography sx={{ fontSize: 10.5, fontWeight: 700, letterSpacing: '0.04em', textTransform: 'uppercase', color: MUTED, mb: '6px' }}>
        Décision métier
      </Typography>
      <Typography sx={{ fontSize: 13, color: INK, lineHeight: 1.5 }}>{test.decision}</Typography>

      {(decidedBy.length > 0 || test.decidedAt || test.inProdShare) && (
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: '11px', flexWrap: 'wrap' }}>
          {decidedBy.length > 0 && (
            <Box sx={{ display: 'flex' }}>
              {decidedBy.map((p, i) => (
                <Box
                  key={i}
                  sx={{
                    width: 22, height: 22, borderRadius: 999, display: 'grid', placeItems: 'center',
                    color: '#fff', fontSize: 9.5, fontWeight: 700, border: `2px solid ${SURFACE}`,
                    bgcolor: p.color ?? TEAL, ml: i === 0 ? 0 : '-7px',
                  }}
                >
                  {p.initials}
                </Box>
              ))}
            </Box>
          )}
          {test.decidedAt && (
            <Typography sx={{ fontSize: 11.5, color: MUTED }}>Validé · {test.decidedAt}</Typography>
          )}
          {test.inProdShare && (
            <Box sx={{ ml: 'auto', fontSize: 11, fontWeight: 600, color: '#16746e', bgcolor: '#ecf7f6', borderRadius: 999, px: '10px', py: '3px' }}>
              {test.inProdShare}
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
}

interface TestCardProps {
  test: any;
  idx: number;
  selectedTestIndex: number | null;
  isEditing: boolean;
  editedDescription: string | undefined;
  isCollapsed: boolean;
  areCommentsOpen: boolean;
  comments: Comment[];
  isLoading?: boolean;
  showRetryPrompt?: boolean;
  onStartEdit: () => void;
  onSaveEdit: () => void;
  onEditDescription: (val: string) => void;
  onDelete: () => void;
  onToggleCollapse: () => void;
  onToggleComments: () => void;
  onAddComment: (text: string) => void;
  onDeleteComment: (id: string) => void;
  onSelectForModification: () => void;
  onEditAssertions?: () => void;
  onApplyAssertions?: (assertions: { description: string; expected_condition: string }[]) => Promise<void> | void;
  onRerunTest?: () => void;
  onValidateTest?: () => void;
  onCorrectTest?: () => void;
  onUpload?: (data: Record<string, any[]>) => void;
}

function TestCard({
  test, idx, selectedTestIndex,
  isEditing, editedDescription, isCollapsed,
  areCommentsOpen, comments, isLoading,
  showRetryPrompt,
  onStartEdit, onSaveEdit, onEditDescription,
  onDelete, onToggleCollapse, onToggleComments,
  onAddComment, onDeleteComment,
  onSelectForModification, onEditAssertions, onApplyAssertions, onRerunTest, onValidateTest, onCorrectTest, onUpload,
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
      data-testid={`test-card-${idx + 1}`}
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
              <CircularProgress size={11} thickness={5} sx={{ color: TEAL }} />
              {Object.keys(inputData).length > 0 ? 'Exécution DuckDB…' : 'Génération…'}
            </Box>
          )}
          {test.status !== 'pending' && isLoading && !test.evaluation && (
            <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', color: MUTED, fontSize: 11.5 }}>
              <CircularProgress size={11} thickness={5} sx={{ color: TEAL }} /> Évaluation…
            </Box>
          )}
          {tags.map((tg) => {
            const tc = tagStyle(tg);
            return <Chip key={tg} label={tg} size="small" sx={{ fontSize: 10.5, height: 20, bgcolor: tc.bg, color: tc.fg, border: 'none' }} />;
          })}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, ml: 'auto' }}>
            <Tooltip title="Replier ce test">
              <MutedIconButton size="small" data-testid={`collapse-test-${idx + 1}`} onClick={onToggleCollapse}>
                <ExpandLessIcon sx={{ fontSize: 15 }} />
              </MutedIconButton>
            </Tooltip>
            <Typography sx={{ fontSize: 11, color: PLACEHOLDER }}>#{idx + 1}</Typography>
          </Box>
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
          <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 0.5 }}>
            <Typography sx={{ fontWeight: 600, color: INK, fontSize: 13.5, lineHeight: 1.5, flex: 1 }}>
              {description}
            </Typography>
            {test.status !== 'pending' && (
              <Tooltip title="Éditer la description">
                <MutedIconButton size="small" onClick={onStartEdit} sx={{ mt: '1px', flexShrink: 0 }}>
                  <EditIcon sx={{ fontSize: 13 }} />
                </MutedIconButton>
              </Tooltip>
            )}
          </Box>
        )}

        {/* Décision métier figée (v15 §5) — optional, masked when absent */}
        {test.decision && <DecisionBlock test={test} />}

        {/* Verdict text */}
        {test.status && test.status !== 'pending' && isLoading && !test.evaluation && (
          <Box sx={{ mt: 1, p: '9px 12px', bgcolor: '#f5f7f8', borderRadius: '8px', display: 'flex', alignItems: 'center', gap: '6px' }}>
            <CircularProgress size={10} thickness={5} sx={{ color: TEAL }} />
            <Typography sx={{ fontSize: 12, color: MUTED }}>Évaluation en cours…</Typography>
          </Box>
        )}
        {test.status && test.status !== 'pending' && (!isLoading || test.evaluation) && (
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

      {/* Validation prompt — désync description↔cardinalité (données valides) : l'utilisateur tranche */}
      {test.reason_type === 'needs_validation' && (onValidateTest || onCorrectTest) && (() => {
        let actual = 0;
        try { actual = (JSON.parse(test.results_json || '[]') || []).length; } catch { /* noop */ }
        const expected = test.expected_row_count;
        return (
          <Box sx={{ px: 2, pb: 1.5, display: 'flex', flexDirection: 'column', gap: 1 }}>
            <Typography sx={{ fontSize: 12.5, color: '#8a5c00', lineHeight: 1.4 }}>
              {expected != null
                ? `Le résultat produit ${actual} ligne(s) alors que ce scénario en suppose ${expected}. `
                : 'Le résultat ne correspond pas à la cardinalité supposée par la description. '}
              Valides-tu ce résultat (la description sera réalignée) ou faut-il corriger le test ?
            </Typography>
            <Box sx={{ display: 'flex', gap: 0.75, flexWrap: 'wrap' }}>
              {onValidateTest && (
                <Button
                  variant="contained"
                  size="small"
                  startIcon={<CheckCircleIcon sx={{ fontSize: 14 }} />}
                  onClick={onValidateTest}
                  sx={{ fontSize: 12, boxShadow: 'none', bgcolor: '#23a26d', '&:hover': { boxShadow: 'none', bgcolor: '#1c8459' } }}
                >
                  Je valide l'état actuel
                </Button>
              )}
              {onCorrectTest && (
                <Button
                  variant="outlined"
                  size="small"
                  startIcon={<AutoAwesomeIcon sx={{ fontSize: 14 }} />}
                  onClick={onCorrectTest}
                  sx={{ fontSize: 12, borderColor: '#d89323', color: '#8a5c00', '&:hover': { borderColor: '#b37820', bgcolor: '#fffbf0' } }}
                >
                  Corriger le test
                </Button>
              )}
            </Box>
          </Box>
        );
      })()}

      {/* Retry prompt — affiché quand bad_data retries épuisés */}
      {showRetryPrompt && onRerunTest && (
        <Box sx={{ px: 2, pb: 1.5 }}>
          <Button
            variant="outlined"
            size="small"
            startIcon={<ReplayIcon sx={{ fontSize: 14 }} />}
            onClick={onRerunTest}
            sx={{ fontSize: 12, borderColor: '#d89323', color: '#8a5c00', '&:hover': { borderColor: '#b37820', bgcolor: '#fffbf0' } }}
          >
            Continuer à essayer de corriger ce test ?
          </Button>
        </Box>
      )}

      {/* Action bar */}
      <Box sx={{ display: 'flex', gap: 0.5, px: 1.5, pb: 1, justifyContent: 'flex-end', flexWrap: 'wrap' }}>
        {test.status !== 'pending' && (
          <>
            <Tooltip title={selectedTestIndex === idx ? 'Sélectionné — écris ton instruction dans le chat' : 'Modifier avec MockSQL'}>
              {selectedTestIndex === idx
                ? <TealIconButton size="small" onClick={onSelectForModification}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></TealIconButton>
                : <MutedIconButton size="small" onClick={onSelectForModification}><AutoAwesomeIcon sx={{ fontSize: 14 }} /></MutedIconButton>
              }
            </Tooltip>
            {onRerunTest && (
              <Tooltip title="Regénérer ce test">
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
          <Box sx={{ borderTop: '1px solid #eff3f4' }}>
            {Object.keys(inputData).length > 0 ? (
              <>
                <Box sx={{ px: 2, pt: 1.5, pb: 1 }}>
                  <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6, mb: 0.75 }}>
                    Données d'entrée
                  </Typography>
                  <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, overflowX: 'auto' }}>
                    {Object.entries(inputData).map(([key, val]) => (
                      <DisplayTable key={key} jsonData={val as any[]} tableName={key} />
                    ))}
                  </Box>
                </Box>
                <Box sx={{ display: 'flex', justifyContent: 'center', py: '2px', color: MUTED }}>
                  <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"><path d="M12 5v14M6 13l6 6 6-6" /></svg>
                </Box>
                <Box sx={{ px: 2, pb: 2 }}>
                  <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
                  <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1 }} />
                </Box>
              </>
            ) : (
              <Box sx={{ px: 2, py: 2 }}>
                <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
                <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1, mb: 0.5 }} />
                <Skeleton variant="rectangular" height={28} sx={{ borderRadius: 1 }} />
              </Box>
            )}
          </Box>
        ) : (
          <>
            <ResultWithAssertions
              inputData={inputData}
              outputData={outputData}
              assertionResults={test.assertion_results ?? []}
              onEditAssertions={onEditAssertions}
              onApplyAssertions={onApplyAssertions}
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

/* ─── Diff utils ──────────────────────────────────────────────────── */
type DiffLineType = 'same' | 'add' | 'remove';
interface DiffLine { type: DiffLineType; text: string; oldNo: number | null; newNo: number | null; }

function computeLineDiff(oldText: string, newText: string): DiffLine[] {
  const a = oldText.split('\n'), b = newText.split('\n');
  const m = a.length, n = b.length;
  const dp = Array.from({ length: m + 1 }, () => new Array(n + 1).fill(0));
  for (let i = 1; i <= m; i++)
    for (let j = 1; j <= n; j++)
      dp[i][j] = a[i-1] === b[j-1] ? dp[i-1][j-1] + 1 : Math.max(dp[i-1][j], dp[i][j-1]);
  const raw: Pick<DiffLine, 'type' | 'text'>[] = [];
  let i = m, j = n;
  while (i > 0 || j > 0) {
    if (i > 0 && j > 0 && a[i-1] === b[j-1]) { raw.unshift({ type: 'same',   text: a[i-1] }); i--; j--; }
    else if (j > 0 && (i === 0 || dp[i][j-1] >= dp[i-1][j])) { raw.unshift({ type: 'add',    text: b[j-1] }); j--; }
    else { raw.unshift({ type: 'remove', text: a[i-1] }); i--; }
  }
  let oldNo = 1, newNo = 1;
  return raw.map(l => {
    const line: DiffLine = { ...l, oldNo: l.type === 'add' ? null : oldNo, newNo: l.type === 'remove' ? null : newNo };
    if (l.type !== 'add') oldNo++;
    if (l.type !== 'remove') newNo++;
    return line;
  });
}

/* collapse unchanged runs longer than 2×ctx, keep ctx lines around each hunk */
function contextualLines(lines: DiffLine[], ctx = 3): (DiffLine | { collapsed: number })[] {
  const changed = new Set(lines.map((l, i) => l.type !== 'same' ? i : -1).filter(i => i >= 0));
  if (changed.size === 0) return [];
  const visible = new Set<number>();
  changed.forEach(ci => { for (let k = Math.max(0, ci - ctx); k <= Math.min(lines.length - 1, ci + ctx); k++) visible.add(k); });
  const out: (DiffLine | { collapsed: number })[] = [];
  let i = 0;
  while (i < lines.length) {
    if (visible.has(i)) { out.push(lines[i++]); }
    else {
      let cnt = 0;
      while (i < lines.length && !visible.has(i)) { cnt++; i++; }
      out.push({ collapsed: cnt });
    }
  }
  return out;
}

/* ─── StaleInfo ──────────────────────────────────────────────────── */
export interface StaleInfo {
  isStale: boolean;
  commitsSince: number;
  lastTestedAt?: string;
  onReevaluate: () => void;
  currentSql?: string;
  onFetchNewSql?: () => Promise<string | null>;
}

function StaleBanner({ info, tests, sqlFileName }: { info: StaleInfo; tests: any[]; sqlFileName?: string }) {
  const { t } = useTranslation();
  const [modalOpen, setModalOpen] = useState(false);
  const [newSql, setNewSql] = useState<string | null>(null);
  const [fetching, setFetching] = useState(false);

  const changesLabel = info.commitsSince > 0
    ? `${info.commitsSince} changement${info.commitsSince > 1 ? 's' : ''} depuis le dernier test`
    : 'le fichier source a été modifié';

  async function handleOpenDiff() {
    setModalOpen(true);
    if (newSql !== null || !info.onFetchNewSql) return;
    setFetching(true);
    const result = await info.onFetchNewSql();
    setFetching(false);
    setNewSql(result);
  }

  const diffLines = useMemo(() => {
    if (!info.currentSql || !newSql) return [];
    return computeLineDiff(info.currentSql, newSql);
  }, [info.currentSql, newSql]);

  const chunks = useMemo(() => contextualLines(diffLines), [diffLines]);
  const addCount    = diffLines.filter(l => l.type === 'add').length;
  const removeCount = diffLines.filter(l => l.type === 'remove').length;
  const noDiff      = newSql !== null && addCount === 0 && removeCount === 0;

  const showDiffBtn = !!info.onFetchNewSql && !!info.currentSql;

  return (
    <>
      <Box sx={{
        display: 'flex', alignItems: 'center', gap: 1.5,
        px: 2, py: '9px', flexShrink: 0,
        bgcolor: '#fffbeb', borderBottom: '1px solid #f5d878',
      }}>
        <WarningAmberIcon sx={{ fontSize: 15, color: '#c78f00', flexShrink: 0 }} />
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography sx={{ fontSize: 12, fontWeight: 700, color: '#7a5500', lineHeight: 1.3 }}>
            Fichier modifié — {changesLabel}
          </Typography>
          {info.lastTestedAt && (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: '4px', mt: '2px' }}>
              <AccessTimeIcon sx={{ fontSize: 11, color: '#a07820' }} />
              <Typography sx={{ fontSize: 11, color: '#a07820' }}>
                Testé {relativeDate(info.lastTestedAt, t)}
              </Typography>
            </Box>
          )}
        </Box>
        {showDiffBtn && (
          <Box
            component="button"
            onClick={handleOpenDiff}
            sx={{
              display: 'inline-flex', alignItems: 'center', gap: '5px',
              px: '11px', py: '5px', fontSize: 11.5, fontWeight: 600,
              border: '1.5px solid #c0cdd0', borderRadius: '8px',
              bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit', flexShrink: 0,
              '&:hover': { borderColor: '#2BB0A8', color: '#2BB0A8', bgcolor: '#f0fafa' },
            }}
          >
            <DifferenceIcon sx={{ fontSize: 13 }} />
            Voir la diff
          </Box>
        )}
        <Box
          component="button"
          onClick={info.onReevaluate}
          sx={{
            display: 'inline-flex', alignItems: 'center', gap: '5px',
            px: '11px', py: '5px', fontSize: 11.5, fontWeight: 600,
            border: '1.5px solid #c78f00', borderRadius: '8px',
            bgcolor: '#fff', color: '#7a5500', cursor: 'pointer', fontFamily: 'inherit', flexShrink: 0,
            '&:hover': { bgcolor: '#fff8e1', borderColor: '#a07820' },
          }}
        >
          <ReplayIcon sx={{ fontSize: 13 }} />
          Ré-évaluer
        </Box>
      </Box>

      {/* ── Diff modal ─────────────────────────────────────────── */}
      <Dialog open={modalOpen} onClose={() => setModalOpen(false)} maxWidth="md" fullWidth
        PaperProps={{ sx: { borderRadius: '14px', maxHeight: '85vh' } }}>

        {/* Header */}
        <Box sx={{ px: 2.5, pt: 2, pb: 1.5, borderBottom: `1px solid ${BORDER}`, display: 'flex', alignItems: 'center', gap: 1.5, flexShrink: 0 }}>
          <DifferenceIcon sx={{ fontSize: 18, color: '#c78f00' }} />
          <Box sx={{ flex: 1 }}>
            <Typography sx={{ fontWeight: 700, fontSize: 14, color: INK }}>
              Diff SQL{sqlFileName ? ` — ${sqlFileName}` : ''}
            </Typography>
            {!fetching && newSql !== null && (
              <Box sx={{ display: 'flex', gap: 1, mt: '3px' }}>
                {addCount > 0 && (
                  <Typography sx={{ fontSize: 11, fontWeight: 700, color: '#23a26d', bgcolor: '#e6ffec', px: '7px', borderRadius: 999 }}>
                    +{addCount} ligne{addCount > 1 ? 's' : ''}
                  </Typography>
                )}
                {removeCount > 0 && (
                  <Typography sx={{ fontSize: 11, fontWeight: 700, color: '#d0503f', bgcolor: '#ffebe9', px: '7px', borderRadius: 999 }}>
                    −{removeCount} ligne{removeCount > 1 ? 's' : ''}
                  </Typography>
                )}
                {noDiff && (
                  <Typography sx={{ fontSize: 11, color: MUTED }}>Aucune différence détectée</Typography>
                )}
              </Box>
            )}
          </Box>
          <IconButton size="small" onClick={() => setModalOpen(false)} sx={{ color: MUTED }}>
            <CancelIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </Box>

        <DialogContent sx={{ p: 0, display: 'flex', flexDirection: 'column', gap: 0, overflow: 'hidden' }}>

          {/* Diff view */}
          <Box sx={{ flex: chunks.length > 0 ? '0 1 auto' : 1, overflowY: 'auto', maxHeight: tests.length > 0 ? '45vh' : '65vh' }}>
            {fetching && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, p: 3, color: MUTED }}>
                <CircularProgress size={16} thickness={4} sx={{ color: TEAL }} />
                <Typography sx={{ fontSize: 13 }}>Chargement du fichier…</Typography>
              </Box>
            )}
            {!fetching && newSql === null && !info.onFetchNewSql && (
              <Typography sx={{ p: 3, fontSize: 13, color: MUTED }}>Aperçu de la diff non disponible.</Typography>
            )}
            {!fetching && chunks.length > 0 && (
              <Box component="table" sx={{ width: '100%', borderCollapse: 'collapse', fontFamily: "'JetBrains Mono', 'Fira Code', monospace", fontSize: 12 }}>
                <tbody>
                  {chunks.map((entry, ei) => {
                    if ('collapsed' in entry) {
                      return (
                        <Box component="tr" key={`c-${ei}`} sx={{ bgcolor: '#f5f8fa' }}>
                          <Box component="td" colSpan={3} sx={{ px: 2, py: '4px', color: MUTED, fontSize: 11.5, fontStyle: 'italic', borderBottom: `1px solid ${BORDER}` }}>
                            ··· {entry.collapsed} ligne{entry.collapsed > 1 ? 's' : ''} identique{entry.collapsed > 1 ? 's' : ''} masquée{entry.collapsed > 1 ? 's' : ''}
                          </Box>
                        </Box>
                      );
                    }
                    const l = entry as DiffLine;
                    const bg    = l.type === 'add' ? '#e6ffec' : l.type === 'remove' ? '#ffebe9' : '#fff';
                    const fg    = l.type === 'add' ? '#23a26d' : l.type === 'remove' ? '#d0503f' : MUTED;
                    const prefix = l.type === 'add' ? '+' : l.type === 'remove' ? '−' : ' ';
                    return (
                      <Box component="tr" key={ei} sx={{ bgcolor: bg, '&:hover': { filter: 'brightness(0.97)' } }}>
                        <Box component="td" sx={{ px: '10px', py: '2px', color: MUTED, fontSize: 10.5, userSelect: 'none', minWidth: 36, textAlign: 'right', borderRight: `1px solid ${BORDER}`, opacity: 0.6, whiteSpace: 'nowrap' }}>
                          {l.oldNo ?? ''}
                        </Box>
                        <Box component="td" sx={{ px: '10px', py: '2px', color: MUTED, fontSize: 10.5, userSelect: 'none', minWidth: 36, textAlign: 'right', borderRight: `1px solid ${BORDER}`, opacity: 0.6, whiteSpace: 'nowrap' }}>
                          {l.newNo ?? ''}
                        </Box>
                        <Box component="td" sx={{ px: '14px', py: '2px', whiteSpace: 'pre', color: l.type === 'same' ? INK : fg }}>
                          <Box component="span" sx={{ color: fg, userSelect: 'none', mr: '6px', fontWeight: 700 }}>{prefix}</Box>
                          {l.text}
                        </Box>
                      </Box>
                    );
                  })}
                </tbody>
              </Box>
            )}
            {!fetching && noDiff && (
              <Box sx={{ p: 3, textAlign: 'center' }}>
                <CheckCircleIcon sx={{ fontSize: 28, color: '#23a26d', mb: 1 }} />
                <Typography sx={{ fontSize: 13, color: '#23a26d', fontWeight: 600 }}>Fichier synchronisé — aucune modification détectée</Typography>
              </Box>
            )}
          </Box>

          {/* Test summary */}
          {tests.length > 0 && (
            <>
              <Box sx={{ px: 2.5, py: '8px', bgcolor: '#f5f8fa', borderTop: `1px solid ${BORDER}`, borderBottom: `1px solid ${BORDER}`, flexShrink: 0 }}>
                <Typography sx={{ fontSize: 11, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6 }}>
                  Tests actuels · {tests.length}
                </Typography>
              </Box>
              <Box sx={{ overflowY: 'auto', maxHeight: '28vh' }}>
                {tests.map((test, idx) => {
                  const { verdict, label, fg, bg } = getVerdictInfo(test);
                  const execSt = testExecStatus(test);
                  return (
                    <Box key={idx} sx={{ display: 'flex', alignItems: 'center', gap: 1.25, px: 2.5, py: '7px', borderBottom: `1px solid #f0f3f4`, '&:last-of-type': { borderBottom: 'none' } }}>
                      <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', bgcolor: bg, color: fg, px: '7px', py: '2px', borderRadius: 999, fontSize: 10.5, fontWeight: 700, flexShrink: 0 }}>
                        {verdict === 'good' && <CheckCircleIcon sx={{ fontSize: 10 }} />}
                        {verdict === 'warn' && <WarningAmberIcon sx={{ fontSize: 10 }} />}
                        {verdict === 'bad'  && <CancelIcon sx={{ fontSize: 10 }} />}
                        {label}
                      </Box>
                      {execSt === 'fail' && (
                        <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '3px', bgcolor: '#ffebe9', color: '#d0503f', px: '6px', py: '1px', borderRadius: 999, fontSize: 10, fontWeight: 700, flexShrink: 0 }}>
                          <CancelIcon sx={{ fontSize: 9 }} /> Échec
                        </Box>
                      )}
                      {execSt === 'pass' && (
                        <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '3px', bgcolor: '#e6ffec', color: '#23a26d', px: '6px', py: '1px', borderRadius: 999, fontSize: 10, fontWeight: 700, flexShrink: 0 }}>
                          <CheckCircleIcon sx={{ fontSize: 9 }} /> Pass
                        </Box>
                      )}
                      <Typography sx={{ fontSize: 12.5, color: INK, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        #{idx + 1} {test.unit_test_description ?? '—'}
                      </Typography>
                    </Box>
                  );
                })}
              </Box>
            </>
          )}
        </DialogContent>

        <DialogActions sx={{ px: 2.5, py: 1.5, borderTop: `1px solid ${BORDER}`, gap: 1 }}>
          <Box component="button" onClick={() => setModalOpen(false)} sx={{ px: '14px', py: '6px', fontSize: 12, fontWeight: 600, border: `1.5px solid ${BORDER}`, borderRadius: '8px', bgcolor: '#fff', color: BODY, cursor: 'pointer', fontFamily: 'inherit', '&:hover': { borderColor: MUTED } }}>
            Fermer
          </Box>
          <Box
            component="button"
            onClick={() => { setModalOpen(false); info.onReevaluate(); }}
            sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', px: '14px', py: '6px', fontSize: 12, fontWeight: 600, border: '1.5px solid #c78f00', borderRadius: '8px', bgcolor: '#fff', color: '#7a5500', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#fff8e1' } }}
          >
            <ReplayIcon sx={{ fontSize: 13 }} /> Ré-évaluer
          </Box>
        </DialogActions>
      </Dialog>
    </>
  );
}

/* ─── Props ──────────────────────────────────────────────────────── */
interface TestsPanelProps {
  onAddTest: () => void;
  onSelectForModification: (idx: number) => void;
  onEditAssertions?: (idx: number) => void;
  selectedTestIndex: number | null;
  onUpload?: (uploadedData: Record<string, any[]>) => void;
  onRerunTest?: (idx: number) => void;
  onValidateTest?: (idx: number) => void;
  onCorrectTest?: (idx: number) => void;
  onOpenChat?: () => void;
  onSuggestionClick?: (text: string) => void;
  onDismissSuggestion?: (text: string) => void;
  onRegenerateSuggestions?: () => void;
  modelId?: string;
  retryBadDataTestIndex?: number | null;
  sqlProps?: SqlStripProps;
  staleInfo?: StaleInfo;
}

/* ═══════════════════════════════════════════════════════════════════ */
const TestsPanel: React.FC<TestsPanelProps> = ({
  onSelectForModification, onEditAssertions, selectedTestIndex,
  onUpload, onRerunTest, onValidateTest, onCorrectTest, onOpenChat, onSuggestionClick, onDismissSuggestion, onRegenerateSuggestions,
  retryBadDataTestIndex,
  sqlProps, staleInfo,
}) => {
  const dispatch = useAppDispatch();
  const currentModelId = useAppSelector((state) => state.appBarModel.currentModelId);
  const testResults: any[] = useAppSelector((state) => state.buildModel.testResults ?? []);
  const suggestions: string[] = useAppSelector((state) => state.buildModel.suggestions ?? []);
  const suggestionRationales: Record<string, string> = useAppSelector((state) => state.buildModel.suggestionRationales ?? {});
  const isLoading = useAppSelector((state) => !!state.buildModel.loading);

  const [suggestionsHighlighted, setSuggestionsHighlighted] = useState(false);
  const suggestionsRef = useRef<HTMLDivElement>(null);
  const prevSuggestionsLengthRef = useRef(suggestions.length);

  useEffect(() => {
    const prev = prevSuggestionsLengthRef.current;
    prevSuggestionsLengthRef.current = suggestions.length;
    if (prev === 0 && suggestions.length > 0) {
      setSuggestionsHighlighted(true);
      setTimeout(() => suggestionsRef.current?.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 80);
      setTimeout(() => setSuggestionsHighlighted(false), 2500);
    }
  }, [suggestions.length]);
  const loadingTestIndex = useAppSelector((state) => state.buildModel.loadingTestIndex);

  const {
    editingIndex, setEditingIndex,
    editedDescriptions, setEditedDescriptions,
    expanded, setExpanded,
    filter, setFilter,
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
    setExpanded(prev => { const next = new Set(prev); next.delete(idx); return next; });
  };

  const handleApplyAssertions = async (
    testIndex: any,
    assertions: { description: string; expected_condition: string }[],
  ) => {
    if (!currentModelId) return;
    const res = await dispatch(
      applyAssertions({ sessionId: currentModelId, testIndex, assertions }),
    ).unwrap();
    dispatch(
      setTestResults(
        testResults.map((t: any) =>
          String(t.test_index) === String(res.test_index)
            ? { ...t, assertion_results: res.assertion_results, evaluation: res.evaluation }
            : t,
        ),
      ),
    );
  };

  const handleSaveEdit = (idx: number) => {
    const newDesc = editedDescriptions[idx];
    setEditingIndex(null);
    setEditedDescriptions((prev) => { const next = { ...prev }; delete next[idx]; return next; });
    if (newDesc === undefined) return;
    persist(testResults.map((t, i) => i === idx ? { ...t, unit_test_description: newDesc } : t));
  };

  const prevStaleRef = useRef<boolean>(false);
  const [syncedAt, setSyncedAt] = useState<number | null>(null);

  useEffect(() => {
    const isNowStale = staleInfo?.isStale ?? false;
    if (prevStaleRef.current && !isNowStale) setSyncedAt(Date.now());
    prevStaleRef.current = isNowStale;
   
  }, [staleInfo?.isStale]);

  useEffect(() => {
    if (!syncedAt) return;
    const t = setTimeout(() => setSyncedAt(null), 6000);
    return () => clearTimeout(t);
  }, [syncedAt]);

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
      const newSet = new Set<number>();
      for (let i = prev; i < curr; i++) newSet.add(i);
      setExpanded(newSet);
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

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', height: '100%', overflow: 'hidden' }}>
      {/* Stale banner */}
      {staleInfo?.isStale && <StaleBanner info={staleInfo} tests={testResults} sqlFileName={sqlProps?.sqlFileName} />}
      {!staleInfo?.isStale && syncedAt && (
        <Box sx={{
          display: 'flex', alignItems: 'center', gap: 1,
          px: 2, py: '7px', flexShrink: 0,
          bgcolor: '#eaf5f0', borderBottom: '1px solid #b2e0ce',
        }}>
          <CheckCircleIcon sx={{ fontSize: 14, color: '#23a26d', flexShrink: 0 }} />
          <Typography sx={{ fontSize: 12, fontWeight: 600, color: '#1b7a55' }}>
            Synchronisé — {relTime(syncedAt)}
          </Typography>
        </Box>
      )}

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
            <HtmlExporter tests={testResults} sqlFileName={sqlProps?.sqlFileName} />
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
          </Box>
        </Box>
      )}

      {/* SQL strip */}
      {sqlProps?.sql && <SqlStrip {...sqlProps} hasTests={testResults.length > 0} />}

      {/* Loading skeleton */}
      {testResults.length === 0 && (isLoading || !!sqlProps?.loading) && (
        <Box sx={{ flex: 1, overflowY: 'auto', px: 1.5, pt: 1.5, pb: 1 }}>
          <Skeleton variant="rounded" height={16} width="55%" sx={{ mb: 2, borderRadius: 999 }} />
          {[0, 1, 2].map((i) => (
            <Box key={i} sx={{ border: `1px solid ${BORDER}`, borderRadius: 2, p: 1.5, mb: 1 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                <Skeleton variant="circular" width={14} height={14} />
                <Skeleton variant="text" width="60%" height={16} />
                <Skeleton variant="rounded" width={48} height={18} sx={{ ml: 'auto', borderRadius: 999 }} />
              </Box>
              <Skeleton variant="text" width="80%" height={13} />
              <Skeleton variant="text" width="50%" height={13} />
            </Box>
          ))}
        </Box>
      )}

      {/* Empty state */}
      {testResults.length === 0 && !isLoading && !sqlProps?.loading && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: 2, p: 2 }}>
          <Typography variant="body2" sx={{ color: '#999', textAlign: 'center' }}>
            Aucun test généré pour l'instant.
          </Typography>
        </Box>
      )}

      {/* Scrollable content */}
      {testResults.length > 0 && (
        <Box data-testid="demo-zoom-tests" sx={{ flex: 1, overflowY: 'auto', px: 1.5, pt: 1.5, pb: 1 }}>
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

              if (!expanded.has(idx)) {
                return (
                  <CompactRow
                    key={idx}
                    test={test}
                    idx={idx}
                    commentCount={testComments.length}
                    onExpand={() => setExpanded(prev => { const next = new Set(prev); next.add(idx); return next; })}
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
                  isCollapsed={false}
                  areCommentsOpen={!!openComments[testKey]}
                  comments={testComments}
                  isLoading={isLoading && (loadingTestIndex === undefined || test.test_index === loadingTestIndex)}
                  onStartEdit={() => setEditingIndex(idx)}
                  onSaveEdit={() => handleSaveEdit(idx)}
                  onEditDescription={(val) => setEditedDescriptions((prev) => ({ ...prev, [idx]: val }))}
                  onDelete={() => handleDelete(idx)}
                  onToggleCollapse={() => setExpanded(prev => { const next = new Set(prev); next.delete(idx); return next; })}
                  onToggleComments={() => setOpenComments((o) => ({ ...o, [testKey]: !o[testKey] }))}
                  onAddComment={(text) => addComment(testKey, text)}
                  onDeleteComment={(id) => deleteComment(testKey, id)}
                  onSelectForModification={() => onSelectForModification(idx)}
                  onEditAssertions={onEditAssertions ? () => onEditAssertions(idx) : undefined}
                  onApplyAssertions={(a) => handleApplyAssertions(test.test_index, a)}
                  onRerunTest={onRerunTest ? () => onRerunTest(idx) : undefined}
                  onValidateTest={onValidateTest ? () => onValidateTest(idx) : undefined}
                  onCorrectTest={onCorrectTest ? () => onCorrectTest(idx) : undefined}
                  onUpload={onUpload}
                  showRetryPrompt={retryBadDataTestIndex != null && retryBadDataTestIndex === test.test_index}
                />
              );
            })}

            {filteredTests.length === 0 && (
              <Box sx={{ textAlign: 'center', p: '36px 12px', color: PLACEHOLDER, fontSize: 13, bgcolor: SURFACE, border: `1px dashed ${BORDER}`, borderRadius: '12px' }}>
                Aucun test ne correspond à ce filtre.
              </Box>
            )}
          </Box>

          {/* Panneau dédié des suggestions (hors fil de conversation) */}
          <SuggestionsSection
            suggestions={suggestions}
            rationales={suggestionRationales}
            onAdd={onSuggestionClick}
            onDismiss={onDismissSuggestion}
            onRegenerate={onRegenerateSuggestions}
            regenerating={isLoading}
            highlighted={suggestionsHighlighted}
            boxRef={suggestionsRef}
          />

        </Box>
      )}
    </Box>
  );
};

export default React.memo(TestsPanel);
