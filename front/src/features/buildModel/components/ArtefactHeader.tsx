import React from 'react';
import { Box, Button, Chip, CircularProgress, Typography } from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import ScienceIcon from '@mui/icons-material/Science';

interface ArtefactHeaderProps {
  testCount: number;
  onRerun: () => void;
  rerunning: boolean;
  sqlDirty?: boolean;
}

const ArtefactHeader: React.FC<ArtefactHeaderProps> = ({ testCount, onRerun, rerunning, sqlDirty }) => {
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
        Suite de tests
      </Typography>
      <Typography sx={{ fontSize: 12, color: '#6b8287' }}>
        · {testCount} test{testCount !== 1 ? 's' : ''}
      </Typography>

      {sqlDirty && (
        <Chip
          label="SQL modifié"
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

      <Box sx={{ marginLeft: 'auto' }}>
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
          Relancer
        </Button>
      </Box>
    </Box>
  );
};

export default ArtefactHeader;
