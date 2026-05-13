import React, { useState } from 'react';
import {
  Box,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Typography,
} from '@mui/material';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import CodeIcon from '@mui/icons-material/Code';
import CloseIcon from '@mui/icons-material/Close';
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

function stepBgColor(status: StepStatus): string {
  if (status === 'pass') return '#e9f7f0';
  if (status === 'fail') return '#fbeceb';
  if (status === 'error') return '#fcf3e1';
  return '#fff';
}

function StepStatusIcon({ status }: { status: StepStatus }) {
  if (status === 'pass') return <CheckCircleIcon sx={{ fontSize: 13, color: '#23a26d' }} />;
  if (status === 'fail') return <CancelIcon sx={{ fontSize: 13, color: '#d0503f' }} />;
  if (status === 'error') return <ErrorOutlineIcon sx={{ fontSize: 13, color: '#d89323' }} />;
  return null;
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
      <Box sx={{ display: 'flex', alignItems: 'stretch', gap: 0, overflowX: 'auto', pb: '2px' }}>
        {chain.map((step, i) => {
          const status = stepStatuses?.[step.sql];
          const isLast = i === chain.length - 1;
          const filename = step.sql.split('/').pop() ?? step.sql;
          const borderColor = stepBorderColor(status);
          const bgColor = stepBgColor(status);

          return (
            <React.Fragment key={i}>
              <Box
                onClick={() => handleStepClick(step)}
                sx={{
                  flex: '0 0 auto',
                  minWidth: 130,
                  maxWidth: 200,
                  p: '9px 12px',
                  border: isLast ? `1.5px solid ${TEAL}` : `1px solid ${borderColor}`,
                  bgcolor: status ? bgColor : isLast ? TEAL_SUBTLE : '#fff',
                  borderRadius: '10px',
                  cursor: 'pointer',
                  textAlign: 'left',
                  display: 'flex',
                  flexDirection: 'column',
                  gap: '3px',
                  '&:hover': { borderColor: TEAL, bgcolor: TEAL_SUBTLE },
                  transition: 'all .12s',
                }}
              >
                {/* Index + status */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                  <Box sx={{ width: 18, height: 18, borderRadius: '50%', bgcolor: isLast ? TEAL : '#eef2f3', color: isLast ? '#fff' : MUTED, display: 'grid', placeItems: 'center', fontSize: 10.5, fontWeight: 700, flexShrink: 0 }}>
                    {i + 1}
                  </Box>
                  <Box component="code" sx={{ fontSize: 11.5, color: INK, fontWeight: 600, flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {step.produces || filename}
                  </Box>
                  {isLast && (
                    <Box sx={{ fontSize: 9, fontWeight: 700, color: '#fff', bgcolor: TEAL, px: '5px', py: '1px', borderRadius: 999, letterSpacing: 0.3, flexShrink: 0 }}>
                      SORTIE
                    </Box>
                  )}
                  {status && (
                    <Box sx={{ ml: 'auto', flexShrink: 0 }}>
                      <StepStatusIcon status={status} />
                    </Box>
                  )}
                </Box>

                {/* Filename */}
                <Box sx={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                  <CodeIcon sx={{ fontSize: 11, color: MUTED, flexShrink: 0 }} />
                  <Typography sx={{ fontSize: 10.5, color: MUTED, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {filename}
                  </Typography>
                </Box>
              </Box>

              {i < chain.length - 1 && (
                <Box sx={{ display: 'flex', alignItems: 'center', px: '6px', color: MUTED }}>
                  <ArrowForwardIcon sx={{ fontSize: 14 }} />
                </Box>
              )}
            </React.Fragment>
          );
        })}
      </Box>

      {/* SQL viewer dialog */}
      <Dialog open={!!openStep} onClose={() => setOpenStep(null)} maxWidth="md" fullWidth>
        <DialogTitle sx={{ display: 'flex', alignItems: 'center', gap: 1, pb: 1, borderBottom: `1px solid ${BORDER}` }}>
          <CodeIcon sx={{ fontSize: 16, color: TEAL }} />
          <Typography sx={{ fontFamily: 'monospace', fontSize: 13.5, fontWeight: 600, flex: 1, color: INK }}>
            {openStep?.sql.split('/').pop()}
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
