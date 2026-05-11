import React, { useState } from 'react';
import {
  Box,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Tooltip,
  Typography,
} from '@mui/material';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import CodeIcon from '@mui/icons-material/Code';
import CloseIcon from '@mui/icons-material/Close';
import TableChartIcon from '@mui/icons-material/TableChart';
import SqlEditor from '../../shared/SqlEditor';
import { fetchModelSql } from '../../api/models';
import { IntegrationStep } from '../../utils/types';
import { BORDER, INK, MUTED, TEAL, TEAL_SUBTLE } from '../../theme/tokens';

type StepStatus = 'pass' | 'fail' | 'error' | undefined;

interface IntegrationPipelineProps {
  chain: IntegrationStep[];
  stepStatuses?: Record<string, StepStatus>;
}

function stepBorderColor(status: StepStatus): string {
  if (status === 'pass') return '#23a26d';
  if (status === 'fail') return '#d0503f';
  if (status === 'error') return '#d89323';
  return BORDER;
}

function StepCard({
  step,
  index,
  status,
  onClick,
}: {
  step: IntegrationStep;
  index: number;
  status: StepStatus;
  onClick: () => void;
}) {
  const filename = step.sql.split('/').pop() ?? step.sql;
  const borderColor = stepBorderColor(status);

  return (
    <Tooltip title="Cliquer pour voir le SQL" placement="top">
      <Box
        onClick={onClick}
        sx={{
          display: 'flex',
          flexDirection: 'column',
          gap: 0.75,
          p: '10px 14px',
          border: `1.5px solid ${borderColor}`,
          borderRadius: '10px',
          bgcolor: '#fff',
          cursor: 'pointer',
          minWidth: 148,
          maxWidth: 220,
          flexShrink: 0,
          '&:hover': { bgcolor: TEAL_SUBTLE, borderColor: TEAL },
          transition: 'all .12s',
        }}
      >
        {/* Header row: index + status icon */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
          <Box
            sx={{
              width: 20,
              height: 20,
              borderRadius: '5px',
              bgcolor: TEAL_SUBTLE,
              color: TEAL,
              display: 'grid',
              placeItems: 'center',
              fontSize: 10,
              fontWeight: 700,
              flexShrink: 0,
            }}
          >
            {index + 1}
          </Box>
          {status === 'pass' && <CheckCircleIcon sx={{ fontSize: 14, color: '#23a26d', ml: 'auto' }} />}
          {status === 'fail' && <CancelIcon sx={{ fontSize: 14, color: '#d0503f', ml: 'auto' }} />}
          {status === 'error' && <ErrorOutlineIcon sx={{ fontSize: 14, color: '#d89323', ml: 'auto' }} />}
        </Box>

        {/* SQL file */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <CodeIcon sx={{ fontSize: 12, color: MUTED, flexShrink: 0 }} />
          <Typography
            sx={{
              fontSize: 11.5,
              color: INK,
              fontWeight: 600,
              fontFamily: 'monospace',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {filename}
          </Typography>
        </Box>

        {/* Produces table */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5 }}>
          <TableChartIcon sx={{ fontSize: 11, color: MUTED, flexShrink: 0 }} />
          <Typography
            sx={{
              fontSize: 10.5,
              color: MUTED,
              fontFamily: 'monospace',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            {step.produces}
          </Typography>
        </Box>
      </Box>
    </Tooltip>
  );
}

export function IntegrationPipeline({ chain, stepStatuses }: IntegrationPipelineProps) {
  const [openStep, setOpenStep] = useState<IntegrationStep | null>(null);
  const [sqlContent, setSqlContent] = useState<string>('');
  const [loadingSql, setLoadingSql] = useState(false);

  const handleStepClick = async (step: IntegrationStep) => {
    setOpenStep(step);
    setSqlContent('');
    setLoadingSql(true);
    const sql = await fetchModelSql(step.sql);
    setSqlContent(sql ?? '-- SQL non disponible');
    setLoadingSql(false);
  };

  if (chain.length === 0) return null;

  return (
    <>
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, overflowX: 'auto', py: 1 }}>
        {chain.map((step, i) => (
          <React.Fragment key={i}>
            <StepCard
              step={step}
              index={i}
              status={stepStatuses?.[step.sql]}
              onClick={() => handleStepClick(step)}
            />
            {i < chain.length - 1 && (
              <ArrowForwardIcon sx={{ fontSize: 20, color: MUTED, flexShrink: 0 }} />
            )}
          </React.Fragment>
        ))}
      </Box>

      {/* SQL viewer dialog */}
      <Dialog open={!!openStep} onClose={() => setOpenStep(null)} maxWidth="md" fullWidth>
        <DialogTitle
          sx={{ display: 'flex', alignItems: 'center', gap: 1, pb: 1, borderBottom: `1px solid ${BORDER}` }}
        >
          <CodeIcon sx={{ fontSize: 16, color: TEAL }} />
          <Typography sx={{ fontFamily: 'monospace', fontSize: 13.5, fontWeight: 600, flex: 1, color: INK }}>
            {openStep?.sql}
          </Typography>
          <Typography sx={{ fontSize: 11, color: MUTED, fontFamily: 'monospace', mr: 1 }}>
            → {openStep?.produces}
          </Typography>
          <IconButton size="small" onClick={() => setOpenStep(null)}>
            <CloseIcon sx={{ fontSize: 16 }} />
          </IconButton>
        </DialogTitle>
        <DialogContent sx={{ pt: 1.5, pb: 2 }}>
          {loadingSql ? (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, p: 3 }}>
              <CircularProgress size={16} sx={{ color: TEAL }} />
              <Typography sx={{ fontSize: 12.5, color: MUTED }}>Chargement du SQL…</Typography>
            </Box>
          ) : (
            <SqlEditor value={sqlContent} onChange={() => {}} readOnly />
          )}
        </DialogContent>
      </Dialog>
    </>
  );
}
