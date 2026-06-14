import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Box, LinearProgress, Typography } from '@mui/material';

/**
 * Feedback affiché pendant la phase pré-génération (validate → profile → generate)
 * sur la GenerateView. Le validate est un appel REST bloquant qui peut être long
 * (dry-run + sqlglot optimize/split + extraction). Plutôt qu'un label figé, on
 * nomme les sous-étapes en cours et on affiche le temps écoulé pour montrer que
 * « plein de choses » se passent et que ça progresse.
 */
const SubmissionProgress: React.FC<{ label: string }> = ({ label }) => {
  const { t } = useTranslation();
  const [elapsed, setElapsed] = useState(0);
  const [detailIdx, setDetailIdx] = useState(0);

  const phase =
    label === t('loading.validating_sql')
      ? 'validating'
      : label === t('loading.checking_profiling')
        ? 'profiling'
        : label === t('loading.generating_tests')
          ? 'generating'
          : 'other';

  const details =
    phase === 'validating'
      ? (t('submission.validating_detail', { returnObjects: true }) as string[])
      : [];

  // Reset + tick the elapsed counter whenever the phase label changes.
  useEffect(() => {
    setElapsed(0);
    setDetailIdx(0);
    const start = performance.now();
    const id = setInterval(() => setElapsed(Math.floor((performance.now() - start) / 1000)), 1000);
    return () => clearInterval(id);
  }, [label]);

  // Unroll the validator sub-steps one by one (~2s each), then hold on the last
  // one until validation actually completes.
  useEffect(() => {
    if (details.length <= 1) return;
    const id = setInterval(
      () => setDetailIdx((i) => Math.min(i + 1, details.length - 1)),
      5000
    );
    return () => clearInterval(id);
     
  }, [label, details.length]);

  // During validation the rolling pipeline is the primary line; elsewhere the
  // phase label is.
  const primary = details.length > 0 ? details[detailIdx] : label;

  return (
    <Box sx={{ mt: 3 }}>
      <LinearProgress
        variant="indeterminate"
        sx={{ height: 6, borderRadius: 3, backgroundColor: '#e0f7f5', '& .MuiLinearProgress-bar': { backgroundColor: '#1ca8a4' } }}
      />
      <Typography variant="body2" sx={{ mt: 0.75, color: '#555', textAlign: 'center' }}>
        {primary}
        {elapsed > 0 ? ` · ${elapsed} s` : ''}
      </Typography>
    </Box>
  );
};

export default SubmissionProgress;
