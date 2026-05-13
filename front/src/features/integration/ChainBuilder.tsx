import React, { useState, useCallback } from 'react';
import { Box, CircularProgress, Typography } from '@mui/material';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import AddIcon from '@mui/icons-material/Add';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import CodeIcon from '@mui/icons-material/Code';
import DeleteIcon from '@mui/icons-material/Delete';
import LinkIcon from '@mui/icons-material/Link';
import { SqlFile, fetchModelSql } from '../../api/models';
import { IntegrationStep } from '../../utils/types';
import SqlEditor from '../../shared/SqlEditor';
import { SqlFilePickerModal } from '../../shared/SqlFilePickerModal';
import {
  BORDER,
  INK,
  MUTED,
  SURFACE,
  TEAL,
  TEAL_SUBTLE,
} from '../../theme/tokens';

// ─── internal step type ───────────────────────────────────────────────────────
interface UIChainStep {
  id: string;
  filePath: string;
  fileName: string;
  produces: string;
  sqlContent: string | null;
  loadingSql: boolean;
  showSql: boolean;
}

// ─── helpers ──────────────────────────────────────────────────────────────────
function stepId(): string {
  return 'cs' + Date.now() + Math.random().toString(36).slice(2, 6);
}

function toUIStep(s: IntegrationStep, sqlFiles: SqlFile[]): UIChainStep {
  const nameKey = s.sql.replace(/\.sql$/, '');
  const match = sqlFiles.find((f) => f.name === nameKey);
  return {
    id: stepId(),
    filePath: nameKey,
    fileName: match?.name ?? nameKey.split('/').pop() ?? nameKey,
    produces: s.produces,
    sqlContent: null,
    loadingSql: false,
    showSql: false,
  };
}

function toIntegrationSteps(uiSteps: UIChainStep[]): IntegrationStep[] {
  return uiSteps.map((s) => ({ sql: s.filePath + '.sql', produces: s.produces }));
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function iconBtnSx(extra?: object) {
  return {
    background: 'transparent',
    border: 'none',
    cursor: 'pointer',
    width: 28,
    height: 28,
    borderRadius: '7px',
    display: 'grid',
    placeItems: 'center',
    color: MUTED,
    fontFamily: 'inherit',
    p: 0,
    '&:hover': { bgcolor: SURFACE, color: INK },
    ...extra,
  };
}

// ─── CHAIN STEP CARD ──────────────────────────────────────────────────────────
interface ChainStepCardProps {
  step: UIChainStep;
  idx: number;
  isLast: boolean;
  upstream: string[];
  onRemove: () => void;
  onMoveUp: (() => void) | null;
  onMoveDown: (() => void) | null;
  onProducesChange: (v: string) => void;
  onToggleSql: () => void;
}

function ChainStepCard({
  step,
  idx,
  isLast,
  upstream,
  onRemove,
  onMoveUp,
  onMoveDown,
  onProducesChange,
  onToggleSql,
}: ChainStepCardProps) {
  const refs = step.sqlContent
    ? upstream.filter(
        (a) => a && new RegExp('\\b' + escapeRegex(a) + '\\b', 'i').test(step.sqlContent!),
      )
    : [];

  return (
    <Box sx={{ border: isLast ? `1.5px solid ${TEAL}` : `1px solid ${BORDER}`, borderRadius: '12px', bgcolor: '#fff', overflow: 'hidden' }}>
      {/* Header row */}
      <Box sx={{ p: '10px 12px', display: 'flex', alignItems: 'center', gap: 1.25 }}>
        {/* Index badge */}
        <Box sx={{ width: 26, height: 26, borderRadius: '7px', bgcolor: isLast ? TEAL : SURFACE, color: isLast ? '#fff' : MUTED, display: 'grid', placeItems: 'center', fontWeight: 700, fontSize: 12, flexShrink: 0 }}>
          {idx + 1}
        </Box>

        {/* File info */}
        <Box sx={{ minWidth: 0, flex: 1 }}>
          <Box sx={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <CodeIcon sx={{ fontSize: 12, color: MUTED, flexShrink: 0 }} />
            <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: INK, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
              {step.fileName}
            </Typography>
            {isLast && (
              <Box sx={{ ml: '4px', fontSize: 9.5, fontWeight: 700, color: '#fff', bgcolor: TEAL, px: '6px', py: '1px', borderRadius: 999, letterSpacing: 0.3, flexShrink: 0 }}>
                SORTIE
              </Box>
            )}
          </Box>
          <Typography sx={{ fontSize: 10.5, color: MUTED, mt: '1px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {step.filePath}
          </Typography>
        </Box>

        {/* Action buttons */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: '2px', flexShrink: 0 }}>
          <Box component="button" onClick={onToggleSql} title={step.showSql ? 'Replier SQL' : 'Voir SQL'} sx={iconBtnSx({ color: step.showSql ? TEAL : MUTED })}>
            <CodeIcon sx={{ fontSize: 13 }} />
          </Box>
          {onMoveUp && (
            <Box component="button" onClick={onMoveUp} title="Monter" sx={iconBtnSx()}>
              <ArrowUpwardIcon sx={{ fontSize: 13 }} />
            </Box>
          )}
          {onMoveDown && (
            <Box component="button" onClick={onMoveDown} title="Descendre" sx={iconBtnSx()}>
              <ArrowDownwardIcon sx={{ fontSize: 13 }} />
            </Box>
          )}
          <Box component="button" onClick={onRemove} title="Retirer" sx={iconBtnSx({ color: '#d0503f', '&:hover': { bgcolor: '#fbeceb', color: '#d0503f' } })}>
            <DeleteIcon sx={{ fontSize: 13 }} />
          </Box>
        </Box>
      </Box>

      {/* Produces row */}
      <Box sx={{ p: '8px 12px 10px', display: 'flex', alignItems: 'center', gap: 1.25, flexWrap: 'wrap', bgcolor: '#fafbfb', borderTop: `1px solid ${BORDER}` }}>
        <Typography sx={{ fontSize: 10.5, fontWeight: 700, color: MUTED, letterSpacing: 0.6, textTransform: 'uppercase' }}>Produit</Typography>
        <Typography sx={{ color: MUTED, fontSize: 13 }}>→</Typography>
        <Box
          component="input"
          value={step.produces}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => onProducesChange(e.target.value)}
          placeholder="nom_de_la_table_ou_vue"
          sx={{ p: '5px 10px', fontSize: 12, fontFamily: '"JetBrains Mono", monospace', border: `1px solid ${BORDER}`, borderRadius: '7px', bgcolor: '#fff', minWidth: 200, outline: 'none', '&:focus': { borderColor: TEAL, boxShadow: `0 0 0 2px ${TEAL_SUBTLE}` } }}
        />
        {refs.length > 0 && (
          <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', fontSize: 11, color: '#1f948d' }}>
            <LinkIcon sx={{ fontSize: 12 }} />
            <Typography sx={{ fontSize: 11, color: '#1f948d' }}>référence{refs.length > 1 ? 'nt' : ''}</Typography>
            {refs.map((r) => (
              <Box key={r} component="code" sx={{ bgcolor: TEAL_SUBTLE, color: '#1f948d', px: '6px', py: '1px', borderRadius: '5px', fontSize: 10.5 }}>
                {r}
              </Box>
            ))}
          </Box>
        )}
      </Box>

      {/* SQL viewer */}
      {step.showSql && (
        <Box sx={{ borderTop: `1px solid ${BORDER}` }}>
          {step.loadingSql ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, p: 2 }}>
              <CircularProgress size={14} sx={{ color: TEAL }} />
              <Typography sx={{ fontSize: 12, color: MUTED }}>Chargement du SQL…</Typography>
            </Box>
          ) : (
            <SqlEditor value={step.sqlContent ?? ''} onChange={() => {}} readOnly maxHeight={220} />
          )}
        </Box>
      )}
    </Box>
  );
}

// ─── CHAIN BUILDER ────────────────────────────────────────────────────────────
interface ChainBuilderProps {
  steps: IntegrationStep[];
  sqlFiles: SqlFile[];
  onChange: (steps: IntegrationStep[]) => void;
}

export function ChainBuilder({ steps: initialSteps, sqlFiles, onChange }: ChainBuilderProps) {
  const [uiSteps, setUiSteps] = useState<UIChainStep[]>(() =>
    initialSteps.filter((s) => s.sql).map((s) => toUIStep(s, sqlFiles)),
  );
  const [pickerOpen, setPickerOpen] = useState(false);

  const updateSteps = useCallback(
    (next: UIChainStep[]) => {
      setUiSteps(next);
      onChange(toIntegrationSteps(next));
    },
    [onChange],
  );

  const handlePick = useCallback(
    (file: SqlFile) => {
      const newStep: UIChainStep = {
        id: stepId(),
        filePath: file.name,
        fileName: file.name.split('/').pop() ?? file.name,
        produces: file.name.split('/').pop() ?? file.name,
        sqlContent: null,
        loadingSql: false,
        showSql: false,
      };
      setPickerOpen(false);
      setUiSteps((prev) => {
        const next = [...prev, newStep];
        onChange(toIntegrationSteps(next));
        return next;
      });
    },
    [onChange],
  );

  const remove = (id: string) => updateSteps(uiSteps.filter((s) => s.id !== id));

  const move = (id: string, dir: -1 | 1) => {
    const i = uiSteps.findIndex((s) => s.id === id);
    const j = i + dir;
    if (i < 0 || j < 0 || j >= uiSteps.length) return;
    const next = [...uiSteps];
    [next[i], next[j]] = [next[j], next[i]];
    updateSteps(next);
  };

  const updateProduces = (id: string, produces: string) =>
    updateSteps(uiSteps.map((s) => (s.id === id ? { ...s, produces } : s)));

  const toggleSql = async (id: string) => {
    const step = uiSteps.find((s) => s.id === id);
    if (!step) return;
    const willOpen = !step.showSql;
    setUiSteps((prev) => prev.map((s) => (s.id === id ? { ...s, showSql: !s.showSql } : s)));
    if (willOpen && step.sqlContent === null) {
      setUiSteps((prev) => prev.map((s) => (s.id === id ? { ...s, loadingSql: true } : s)));
      const sql = await fetchModelSql(step.filePath);
      setUiSteps((prev) =>
        prev.map((s) => (s.id === id ? { ...s, sqlContent: sql ?? '', loadingSql: false } : s)),
      );
    }
  };

  const excludePaths = uiSteps.map((s) => s.filePath);

  if (uiSteps.length === 0) {
    return (
      <>
        <Box sx={{ border: `1px dashed ${BORDER}`, borderRadius: '12px', p: '28px 18px', textAlign: 'center', bgcolor: '#fff' }}>
          <Box sx={{ display: 'inline-flex', width: 42, height: 42, borderRadius: '11px', bgcolor: TEAL_SUBTLE, color: TEAL, alignItems: 'center', justifyContent: 'center', mb: 1.25 }}>
            <AccountTreeIcon sx={{ fontSize: 20 }} />
          </Box>
          <Typography sx={{ fontSize: 13.5, color: INK, fontWeight: 600, mb: '4px' }}>
            Aucun script dans la chaîne
          </Typography>
          <Typography sx={{ fontSize: 12, color: MUTED, mb: 2 }}>
            Ajoute au moins 2 scripts pour construire une chaîne d'intégration.
          </Typography>
          <Box
            component="button"
            onClick={() => setPickerOpen(true)}
            sx={{ display: 'inline-flex', alignItems: 'center', gap: '7px', p: '8px 16px', fontSize: 13, fontWeight: 600, border: 'none', borderRadius: '9px', bgcolor: TEAL, color: '#fff', cursor: 'pointer', fontFamily: 'inherit' }}
          >
            <AddIcon sx={{ fontSize: 13 }} /> Ajouter le premier script
          </Box>
        </Box>
        {pickerOpen && (
          <SqlFilePickerModal
            sqlFiles={sqlFiles}
            excludePaths={[]}
            title="Ajouter un script à la chaîne"
            cta="Ajouter à la chaîne"
            onClose={() => setPickerOpen(false)}
            onPick={handlePick}
          />
        )}
      </>
    );
  }

  return (
    <>
      <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0 }}>
        {uiSteps.map((step, i) => (
          <React.Fragment key={step.id}>
            <ChainStepCard
              step={step}
              idx={i}
              isLast={i === uiSteps.length - 1}
              upstream={uiSteps.slice(0, i).map((s) => s.produces).filter(Boolean)}
              onRemove={() => remove(step.id)}
              onMoveUp={i > 0 ? () => move(step.id, -1) : null}
              onMoveDown={i < uiSteps.length - 1 ? () => move(step.id, 1) : null}
              onProducesChange={(v) => updateProduces(step.id, v)}
              onToggleSql={() => toggleSql(step.id)}
            />
            {i < uiSteps.length - 1 && (
              <Box sx={{ display: 'flex', justifyContent: 'center', color: MUTED, py: '2px' }}>
                <ArrowDownwardIcon sx={{ fontSize: 16 }} />
              </Box>
            )}
          </React.Fragment>
        ))}
        <Box
          component="button"
          onClick={() => setPickerOpen(true)}
          sx={{ mt: 1.25, display: 'inline-flex', alignItems: 'center', justifyContent: 'center', gap: '7px', p: '10px 14px', fontSize: 13, fontWeight: 600, border: `1px dashed ${TEAL}`, borderRadius: '10px', bgcolor: TEAL_SUBTLE, color: '#1f948d', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#d2efec' } }}
        >
          <AddIcon sx={{ fontSize: 13 }} /> Ajouter un script à la chaîne
        </Box>
      </Box>

      {pickerOpen && (
        <SqlFilePickerModal
          sqlFiles={sqlFiles}
          excludePaths={excludePaths}
          title="Ajouter un script à la chaîne"
          cta="Ajouter à la chaîne"
          onClose={() => setPickerOpen(false)}
          onPick={handlePick}
        />
      )}
    </>
  );
}
