import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Box, Button, CircularProgress, Typography } from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import { getTestsByModelName, TestSession } from '../../../api/models';
import { drawerWidth } from '../../appBar/components/DrawerComponent';
import { useAppSelector } from '../../../app/hooks';

const TEAL = '#1ca8a4';
const INK = '#0f272a';

function relativeDate(iso?: string): string {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return "à l'instant";
  if (m < 60) return `il y a ${m} min`;
  const h = Math.floor(m / 60);
  if (h < 24) return `il y a ${h} h`;
  return `il y a ${Math.floor(h / 24)} j`;
}

const ModelTestsPage: React.FC = () => {
  const { modelName } = useParams<{ modelName: string }>();
  const navigate = useNavigate();
  const drawerOpen = useAppSelector(s => s.appBarModel.drawerOpen);
  const [tests, setTests] = useState<TestSession[]>([]);
  const [loading, setLoading] = useState(true);

  const decodedName = modelName ? decodeURIComponent(modelName) : '';

  useEffect(() => {
    if (!decodedName) return;
    setLoading(true);
    getTestsByModelName(decodedName)
      .then(results =>
        setTests(results.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || '')))
      )
      .finally(() => setLoading(false));
  }, [decodedName]);

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
            <Typography sx={{ fontSize: 11.5, color: '#6b8287', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.6px', mb: 0.5 }}>
              Modèle SQL
            </Typography>
            <Typography sx={{ fontSize: 22, fontWeight: 700, color: INK, fontFamily: 'monospace' }}>
              {decodedName}.sql
            </Typography>
            <Typography sx={{ fontSize: 13, color: '#6b8287', mt: 0.5 }}>
              {loading ? '…' : `${tests.length} session${tests.length !== 1 ? 's' : ''} de test`}
            </Typography>
          </Box>
          <Button
            variant="contained"
            startIcon={<AddIcon />}
            onClick={() => navigate(`/?model=${encodeURIComponent(decodedName)}&forceNew=1`)}
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
            Nouveau test
          </Button>
        </Box>

        {/* Sessions list */}
        {loading ? (
          <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
            <CircularProgress size={28} sx={{ color: TEAL }} />
          </Box>
        ) : tests.length === 0 ? (
          <Box sx={{ textAlign: 'center', mt: 8 }}>
            <Typography sx={{ fontSize: 14, color: '#6b8287' }}>
              Aucune session de test pour ce modèle.
            </Typography>
          </Box>
        ) : (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
            {tests.map((test, i) => (
              <Box
                key={test.test_id}
                onClick={() => navigate(`/models/${test.test_id}`)}
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 2,
                  px: 2.5,
                  py: 2,
                  border: '1px solid #e4eaec',
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
                    bgcolor: '#f0faf9',
                    color: TEAL,
                    display: 'flex',
                    alignItems: 'center',
                    justifyContent: 'center',
                    fontSize: 13,
                    fontWeight: 700,
                    flexShrink: 0,
                  }}
                >
                  {i + 1}
                </Box>
                <Box sx={{ flex: 1, minWidth: 0 }}>
                  <Typography sx={{ fontSize: 13.5, fontWeight: 600, color: INK }}>
                    {test.test_cases?.[0]?.test_name || `Session ${test.test_id.slice(0, 8)}…`}
                  </Typography>
                  <Typography sx={{ fontSize: 11.5, color: '#6b8287', mt: 0.25 }}>
                    {relativeDate(test.updated_at || test.created_at)} · {test.test_cases?.length ?? 0} test{(test.test_cases?.length ?? 0) !== 1 ? 's' : ''}
                  </Typography>
                </Box>
                <ArrowForwardIcon sx={{ fontSize: 16, color: '#c0c8ca', flexShrink: 0 }} />
              </Box>
            ))}
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default ModelTestsPage;
