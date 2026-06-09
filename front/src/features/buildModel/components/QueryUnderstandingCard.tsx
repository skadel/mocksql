import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Box, Chip, Collapse, Stack, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import PsychologyOutlinedIcon from '@mui/icons-material/PsychologyOutlined';
import type { QueryUnderstanding } from '../../../utils/types';

const TEAL = '#1ca8a4';

const tableChipSx = {
  bgcolor: '#e8f5f5',
  color: TEAL,
  fontFamily: 'monospace',
  fontSize: 11,
  height: 20,
  border: '1px solid #b2e0de',
} as const;

// "customers.id = orders.customer_id" → ["customers", "orders"] → "customers ↔ orders"
const toRelation = (join: string): string => {
  const tables = Array.from(
    new Set(
      join
        .split(/=|AND/)
        .map((s) => s.trim().split('.')[0].trim())
        .filter(Boolean),
    ),
  );
  return tables.join(' ↔ ');
};

// "orders.amount > 0" → "amount > 0" (drop the table prefix for readability)
const stripTablePrefix = (s: string): string => s.replace(/^[A-Za-z0-9_]+\./, '');

const Row: React.FC<{ label: string; children: React.ReactNode }> = ({ label, children }) => (
  <Box sx={{ display: 'flex', gap: 1, alignItems: 'baseline' }}>
    <Typography
      variant="caption"
      sx={{ fontWeight: 700, color: '#6b8287', minWidth: 64, flexShrink: 0 }}
    >
      {label}
    </Typography>
    <Box sx={{ flex: 1, minWidth: 0 }}>{children}</Box>
  </Box>
);

const QueryUnderstandingCard: React.FC<{ understanding: QueryUnderstanding }> = ({ understanding }) => {
  const { t } = useTranslation();
  const [open, setOpen] = useState(true);
  const [showCols, setShowCols] = useState(false);

  const { tables = [], constraints = {}, derived_expressions = [] } = understanding;
  const totalColumns = tables.reduce((acc, tb) => acc + (tb.columns?.length ?? 0), 0);

  const relations = Array.from(new Set((constraints.joins ?? []).map(toRelation).filter(Boolean)));
  const filters = (constraints.filters ?? []).map(stripTablePrefix);
  const shownFilters = filters.slice(0, 4);
  const extraFilters = filters.length - shownFilters.length;

  return (
    <Box sx={{ border: '1px solid #b2e0de', borderRadius: 2, bgcolor: '#f5fbfb', mb: 1.25, overflow: 'hidden' }}>
      {/* Header */}
      <Box
        onClick={() => setOpen((o) => !o)}
        sx={{ display: 'flex', alignItems: 'center', gap: 1, px: 1.5, py: 0.85, cursor: 'pointer', userSelect: 'none' }}
      >
        <PsychologyOutlinedIcon sx={{ fontSize: 17, color: TEAL }} />
        <Typography variant="body2" sx={{ fontWeight: 700, color: '#155e5b', flex: 1 }}>
          {t('understanding.title')}
        </Typography>
        <Typography variant="caption" sx={{ color: '#6b8287' }}>
          {t('understanding.summary', { tables: tables.length, columns: totalColumns })}
        </Typography>
        <ExpandMoreIcon
          sx={{ fontSize: 20, color: '#6b8287', transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }}
        />
      </Box>

      <Collapse in={open}>
        <Stack gap={0.85} sx={{ px: 1.5, pb: 1.25 }}>
          {/* Tables */}
          <Row label={t('understanding.tables')}>
            <Stack direction="row" flexWrap="wrap" gap={0.5}>
              {tables.map((tb) => (
                <Chip key={`${tb.database ?? ''}.${tb.table}`} label={tb.table} size="small" sx={tableChipSx} />
              ))}
            </Stack>
          </Row>

          {/* Relations (plain language, table-level) */}
          {relations.length > 0 && (
            <Row label={t('understanding.relations')}>
              <Typography variant="caption" sx={{ color: '#37474f' }}>
                {relations.join(' · ')}
              </Typography>
            </Row>
          )}

          {/* Filters (column-level, table prefix stripped) */}
          {shownFilters.length > 0 && (
            <Row label={t('understanding.filters')}>
              <Typography variant="caption" sx={{ color: '#37474f', fontFamily: 'monospace' }}>
                {shownFilters.join(' · ')}
                {extraFilters > 0 ? `  +${extraFilters}` : ''}
              </Typography>
            </Row>
          )}

          {/* Computed columns — count only, never raw SQL */}
          {derived_expressions.length > 0 && (
            <Row label={t('understanding.computed')}>
              <Typography variant="caption" sx={{ color: '#37474f' }}>
                {derived_expressions.length}
              </Typography>
            </Row>
          )}

          {/* Columns — hidden by default to keep the card light */}
          {totalColumns > 0 && (
            <Box>
              <Typography
                onClick={() => setShowCols((s) => !s)}
                variant="caption"
                sx={{ color: TEAL, cursor: 'pointer', fontWeight: 600, '&:hover': { textDecoration: 'underline' } }}
              >
                {showCols ? t('understanding.hide_columns') : t('understanding.show_columns')}
              </Typography>
              <Collapse in={showCols}>
                <Stack gap={0.5} sx={{ mt: 0.5 }}>
                  {tables.map((tb) => (
                    <Box key={`cols-${tb.database ?? ''}.${tb.table}`}>
                      <Typography variant="caption" sx={{ fontWeight: 700, color: '#6b8287', fontFamily: 'monospace' }}>
                        {tb.table}
                      </Typography>
                      <Stack direction="row" flexWrap="wrap" gap={0.5} sx={{ mt: 0.25 }}>
                        {(tb.columns ?? []).map((col) => (
                          <Chip key={col} label={col} size="small" sx={tableChipSx} />
                        ))}
                      </Stack>
                    </Box>
                  ))}
                </Stack>
              </Collapse>
            </Box>
          )}
        </Stack>
      </Collapse>
    </Box>
  );
};

export default QueryUnderstandingCard;
