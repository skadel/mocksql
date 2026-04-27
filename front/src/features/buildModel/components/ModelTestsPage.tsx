import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { Alert, Box, Button, CircularProgress, Typography } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import { getTestsByModelName, TestSession } from '../../../api/models';
import { drawerWidth } from '../../appBar/components/DrawerComponent';
import { useAppSelector } from '../../../app/hooks';
import { relativeDate } from '../../../utils/dates';
import { ROUTES } from '../../../routes';
import { TEAL, INK, BORDER, MUTED, TEAL_BG, BORDER_MUTED } from '../../../theme/tokens';

/* ── TestSessionCard ─────────────────────────────────────────────── */
function TestSessionCard({ test, index, onClick }: { test: TestSession; index: number; onClick: () => void }) {
  const { t } = useTranslation();
  return (
    <Box
      onClick={onClick}
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        px: 2.5,
        py: 2,
        border: `1px solid ${BORDER}`,
        borderRadius: '12px',
        bgcolor: '#fff',
        cursor: 'pointer',
        '&:hover': { borderColor: TEAL, bgcolor: '#f5fdfc' },
        transition: 'border-color .12s, background .12s',
      }}
    >
      <Box
        sx={{
          width: 36,
          height: 36,
          borderRadius: '9px',
          bgcolor: TEAL_BG,
          color: TEAL,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          fontSize: 13,
          fontWeight: 700,
          flexShrink: 0,
        }}
      >
        {index + 1}
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography sx={{ fontSize: 13.5, fontWeight: 600, color: INK }}>
          {test.test_cases?.[0]?.test_name || `Session ${test.test_id.slice(0, 8)}…`}
        </Typography>
        <Typography sx={{ fontSize: 11.5, color: MUTED, mt: 0.25 }}>
          {relativeDate(test.updated_at || test.created_at, t)} · {t('model.tests_count', { count: test.test_cases?.length ?? 0 })}
        </Typography>
      </Box>
      <ArrowForwardIcon sx={{ fontSize: 16, color: BORDER_MUTED, flexShrink: 0 }} />
    </Box>
  );
}

/* ── ModelTestsPage ──────────────────────────────────────────────── */
const ModelTestsPage: React.FC = () => {
  const { modelName } = useParams<{ modelName: string }>();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const drawerOpen = useAppSelector(s => s.appBarModel.drawerOpen);
  const [tests, setTests] = useState<TestSession[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const decodedName = modelName ? decodeURIComponent(modelName) : '';

  useEffect(() => {
    if (!decodedName) return;
    setLoading(true);
    setError(null);
    getTestsByModelName(decodedName)
      .then(results =>
        setTests(results.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || '')))
      )
      .catch(() => setError(t('model.load_error')))
      .finally(() => setLoading(false));
  }, [decodedName, t]);

  if (!decodedName) return null;

  return (
    <Box
      sx={{
        height: '100vh',
        width: '100%',
        maxWidth: `calc(100vw - ${drawerOpen ? drawerWidth : 0}px)`,
        transition: 'max-width 0.2s ease',
        overflow: 'auto',
      }}
    >
      <Box sx={{ maxWidth: 680, mx: 'auto', mt: 6, px: 3 }}>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'flex-start', justifyContent: 'space-between', mb: 4 }}>
          <Box>
            <Typography sx={{ fontSize: 11.5, color: MUTED, fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.6px', mb: 0.5 }}>
              {t('model.sql_model')}
            </Typography>
            <Typography sx={{ fontSize: 22, fontWeight: 700, color: INK, fontFamily: 'monospace' }}>
              {decodedName}.sql
            </Typography>
            <Typography sx={{ fontSize: 13, color: MUTED, mt: 0.5 }}>
              {loading ? '…' : t('model.test_sessions', { count: tests.length })}
            </Typography>
          </Box>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => navigate(ROUTES.newTest(decodedName))}
            sx={{
              bgcolor: TEAL,
              '&:hover': { bgcolor: '#159e9a' },
              textTransform: 'none',
              borderRadius: 2,
              px: 2.5,
              mt: 0.5,
              flexShrink: 0,
            }}
          >
            {t('model.new_test')}
          </Button>
        </Box>

        {/* Loading */}
        {loading && (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
            <CircularProgress size={28} sx={{ color: TEAL }} />
          </Box>
        )}

        {/* Error */}
        {!loading && error && (
          <Alert severity="error" sx={{ mt: 4 }}>{error}</Alert>
        )}

        {/* Empty */}
        {!loading && !error && tests.length === 0 && (
          <Box sx={{ textAlign: 'center', mt: 8 }}>
            <Typography sx={{ fontSize: 14, color: MUTED }}>
              {t('model.no_sessions')}
            </Typography>
          </Box>
        )}

        {/* Sessions list */}
        {!loading && !error && tests.length > 0 && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {tests.map((test, i) => (
              <TestSessionCard
                key={test.test_id}
                test={test}
                index={i}
                onClick={() => navigate(ROUTES.testSession(test.test_id))}
              />
            ))}
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default ModelTestsPage;
