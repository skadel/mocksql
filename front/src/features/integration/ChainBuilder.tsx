import React from 'react';
import {
  Box,
  IconButton,
  MenuItem,
  Select,
  TextField,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import DeleteIcon from '@mui/icons-material/Delete';
import ArrowUpwardIcon from '@mui/icons-material/ArrowUpward';
import ArrowDownwardIcon from '@mui/icons-material/ArrowDownward';
import ArrowForwardIcon from '@mui/icons-material/ArrowForward';
import { SqlFile } from '../../api/models';
import { IntegrationStep } from '../../utils/types';
import { BORDER, INK, MUTED, SURFACE, TEAL, TEAL_SUBTLE } from '../../theme/tokens';

interface ChainBuilderProps {
  steps: IntegrationStep[];
  sqlFiles: SqlFile[];
  onChange: (steps: IntegrationStep[]) => void;
}

export function ChainBuilder({ steps, sqlFiles, onChange }: ChainBuilderProps) {
  const add = () => onChange([...steps, { sql: '', produces: '' }]);

  const remove = (i: number) => onChange(steps.filter((_, j) => j !== i));

  const update = (i: number, field: keyof IntegrationStep, value: string) =>
    onChange(steps.map((s, j) => (j === i ? { ...s, [field]: value } : s)));

  const move = (i: number, dir: -1 | 1) => {
    const next = [...steps];
    [next[i], next[i + dir]] = [next[i + dir], next[i]];
    onChange(next);
  };

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1 }}>
      {steps.map((step, i) => (
        <React.Fragment key={i}>
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              p: '10px 12px',
              bgcolor: SURFACE,
              border: `1px solid ${BORDER}`,
              borderRadius: '10px',
            }}
          >
            {/* Step index badge */}
            <Box
              sx={{
                width: 24,
                height: 24,
                borderRadius: '6px',
                bgcolor: TEAL_SUBTLE,
                color: TEAL,
                display: 'grid',
                placeItems: 'center',
                fontSize: 11,
                fontWeight: 700,
                flexShrink: 0,
              }}
            >
              {i + 1}
            </Box>

            {/* SQL file selector */}
            <Select
              size="small"
              value={step.sql}
              onChange={(e) => update(i, 'sql', e.target.value)}
              displayEmpty
              sx={{
                flex: 1,
                fontSize: 12.5,
                fontFamily: 'monospace',
                '& .MuiSelect-select': { py: '6px' },
              }}
              renderValue={(v) =>
                v ? (
                  <Typography sx={{ fontSize: 12.5, fontFamily: 'monospace', color: INK }}>
                    {v}
                  </Typography>
                ) : (
                  <Typography sx={{ fontSize: 12.5, color: '#9aabb0' }}>
                    Sélectionner un script…
                  </Typography>
                )
              }
            >
              {sqlFiles.map((f) => (
                <MenuItem key={f.path} value={f.path} sx={{ fontSize: 12.5, fontFamily: 'monospace' }}>
                  {f.path}
                </MenuItem>
              ))}
            </Select>

            {/* "produit" label */}
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 0.5,
                px: 1,
                color: MUTED,
                flexShrink: 0,
              }}
            >
              <ArrowForwardIcon sx={{ fontSize: 14 }} />
              <Typography sx={{ fontSize: 11, color: MUTED }}>produit</Typography>
            </Box>

            {/* Produces table input */}
            <TextField
              size="small"
              placeholder="dataset.table"
              value={step.produces}
              onChange={(e) => update(i, 'produces', e.target.value)}
              sx={{
                width: 200,
                '& input': { fontSize: 12.5, py: '5px', fontFamily: 'monospace' },
              }}
            />

            {/* Reorder */}
            <Box sx={{ display: 'flex', flexDirection: 'column' }}>
              <IconButton
                size="small"
                disabled={i === 0}
                onClick={() => move(i, -1)}
                sx={{ p: 0.25 }}
              >
                <ArrowUpwardIcon sx={{ fontSize: 12 }} />
              </IconButton>
              <IconButton
                size="small"
                disabled={i === steps.length - 1}
                onClick={() => move(i, 1)}
                sx={{ p: 0.25 }}
              >
                <ArrowDownwardIcon sx={{ fontSize: 12 }} />
              </IconButton>
            </Box>

            {/* Delete */}
            <Tooltip title="Supprimer cette étape">
              <IconButton
                size="small"
                onClick={() => remove(i)}
                disabled={steps.length === 1}
                sx={{ color: '#d0503f', '&:hover': { bgcolor: '#fbeceb' } }}
              >
                <DeleteIcon sx={{ fontSize: 14 }} />
              </IconButton>
            </Tooltip>
          </Box>

          {/* Connector arrow between steps */}
          {i < steps.length - 1 && (
            <Box sx={{ display: 'flex', justifyContent: 'center', color: MUTED, my: -0.5 }}>
              <ArrowForwardIcon sx={{ fontSize: 16, transform: 'rotate(90deg)' }} />
            </Box>
          )}
        </React.Fragment>
      ))}

      <Box
        component="button"
        onClick={add}
        sx={{
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          gap: 0.5,
          px: 2,
          py: 1.25,
          border: `1.5px dashed ${BORDER}`,
          borderRadius: '10px',
          bgcolor: 'transparent',
          cursor: 'pointer',
          color: TEAL,
          fontSize: 13,
          fontWeight: 500,
          fontFamily: 'inherit',
          '&:hover': { bgcolor: '#f0faf9', borderColor: TEAL },
          mt: 0.5,
        }}
      >
        <AddIcon sx={{ fontSize: 16 }} />
        Ajouter une étape
      </Box>
    </Box>
  );
}
