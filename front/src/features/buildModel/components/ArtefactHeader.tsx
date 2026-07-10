import React from 'react';
import { Box, Button, Chip, CircularProgress, Tooltip, Typography } from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import RefreshIcon from '@mui/icons-material/Refresh';
import ScienceIcon from '@mui/icons-material/Science';
import { useTranslation } from 'react-i18next';
import type { TFunction } from 'i18next';

interface ArtefactHeaderProps {
  testCount: number;
  onRerun: () => void;
  rerunning: boolean;
  sqlDirty?: boolean;
  onRefreshProfile?: () => void;
  refreshing?: boolean;
  profiledAt?: string | null;
}

// Fraîcheur du profil en langage naturel : « à l'instant », « il y a 3 h », « il y a 5 j ».
const formatProfiledAt = (t: TFunction, iso?: string | null): string | null => {
  if (!iso) return null;
  const then = new Date(iso).getTime();
  if (Number.isNaN(then)) return null;
  const diffMin = Math.floor((Date.now() - then) / 60000);
  if (diffMin < 1) return t('panel.profiled_now');
  if (diffMin < 60) return t('panel.profiled_min', { count: diffMin });
  const diffH = Math.floor(diffMin / 60);
  if (diffH < 24) return t('panel.profiled_h', { count: diffH });
  const diffD = Math.floor(diffH / 24);
  return t('panel.profiled_d', { count: diffD });
};

const ArtefactHeader: React.FC<ArtefactHeaderProps> = ({ testCount, onRerun, rerunning, sqlDirty, onRefreshProfile, refreshing, profiledAt }) => {
  const { t } = useTranslation();
  const freshness = formatProfiledAt(t, profiledAt);
  return (
    <Box
      sx={{
        px: 2.5,
        py: 1.25,
        borderBottom: '1px solid #e4eaec',
        bgcolor: '#f3f6f7',
        display: 'flex',
        alignItems: 'center',
        gap: 1.5,
        flexShrink: 0,
      }}
    >
      <ScienceIcon sx={{ fontSize: 15, color: '#2BB0A8' }} />
      <Typography sx={{ fontWeight: 600, fontSize: 13.5, color: '#0f272a' }}>
        {t('panel.suite_title')}
      </Typography>
      <Typography sx={{ fontSize: 12, color: '#6b8287' }}>
        · {t('model.tests_count', { count: testCount })}
      </Typography>

      {sqlDirty && (
        <Chip
          label={t('panel.sql_modified')}
          size="small"
          variant="outlined"
          sx={{
            bgcolor: '#fff7e6',
            color: '#a86a00',
            borderColor: '#f3d28a',
            fontSize: 11,
            height: 22,
          }}
        />
      )}

      <Box sx={{ marginLeft: 'auto', display: 'flex', alignItems: 'center', gap: 1 }}>
        {onRefreshProfile && !refreshing && (
          <Typography data-testid="profile-freshness" sx={{ fontSize: 11, color: freshness ? '#9aacb0' : '#a86a00' }}>
            {freshness ?? t('panel.never_profiled')}
          </Typography>
        )}
        {onRefreshProfile && (
          <Tooltip title={t('panel.refresh_tooltip')} arrow placement="top">
            <Button
              size="small"
              variant="text"
              onClick={onRefreshProfile}
              disabled={rerunning || refreshing}
              startIcon={
                <RefreshIcon sx={{
                  fontSize: 14,
                  animation: refreshing ? 'spin 0.8s linear infinite' : 'none',
                  '@keyframes spin': { from: { transform: 'rotate(0deg)' }, to: { transform: 'rotate(360deg)' } },
                }} />
              }
              sx={{
                fontSize: 12,
                color: refreshing ? '#1ca8a4' : '#6b8287',
                textTransform: 'none',
                fontWeight: 500,
                py: 0.5,
                px: 1.25,
                '&:hover': { color: '#1ca8a4', bgcolor: '#ecf7f6' },
              }}
            >
              {refreshing ? t('panel.refreshing') : t('panel.refresh_schema')}
            </Button>
          </Tooltip>
        )}
        <Button
          size="small"
          variant="outlined"
          onClick={onRerun}
          disabled={rerunning}
          startIcon={
            rerunning
              ? <CircularProgress size={11} sx={{ color: '#6b8287' }} />
              : <PlayArrowIcon sx={{ fontSize: 13 }} />
          }
          sx={{
            fontSize: 12,
            borderColor: '#e4eaec',
            color: '#3b5357',
            textTransform: 'none',
            fontWeight: 500,
            py: 0.5,
            px: 1.25,
            '&:hover': { borderColor: '#2BB0A8', color: '#1ca8a4', bgcolor: '#ecf7f6' },
          }}
        >
          {t('panel.rerun')}
        </Button>
      </Box>
    </Box>
  );
};

export default ArtefactHeader;
