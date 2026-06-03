import React from 'react';
import { Box, Drawer, Typography, IconButton } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import { SqlHistoryEntry } from '../../../utils/types';
import { TEAL, INK, BODY, MUTED, PLACEHOLDER, BORDER } from '../../../theme/tokens';

interface HistoryDrawerProps {
  open: boolean;
  onClose: () => void;
  fileName: string;
  entries: SqlHistoryEntry[];
  onRestore: (entry: SqlHistoryEntry) => void;
}

const HistoryDrawer: React.FC<HistoryDrawerProps> = ({ open, onClose, fileName, entries, onRestore }) => {
  const reversed = [...entries].reverse();

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: 360,
          maxWidth: '86vw',
          display: 'flex',
          flexDirection: 'column',
          bgcolor: '#f4f7f7',
          borderLeft: `1px solid ${BORDER}`,
        },
      }}
    >
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.25, p: '16px 18px', borderBottom: `1px solid ${BORDER}`, flexShrink: 0 }}>
        <svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke={TEAL} strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
          <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
          <path d="M3 3v5h5"/>
          <path d="M12 7v5l4 2"/>
        </svg>
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography sx={{ fontSize: 15, fontWeight: 600, color: INK }}>Historique SQL</Typography>
          <Typography sx={{ fontSize: 12, color: MUTED, fontFamily: 'monospace', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
            {fileName}
          </Typography>
        </Box>
        <IconButton size="small" onClick={onClose} sx={{ color: MUTED, '&:hover': { color: INK } }}>
          <CloseIcon sx={{ fontSize: 17 }} />
        </IconButton>
      </Box>

      {/* Timeline */}
      <Box sx={{ flex: 1, overflow: 'auto', p: '18px 18px 22px' }}>
        {reversed.length === 0 ? (
          <Typography sx={{ fontSize: 13, color: PLACEHOLDER, textAlign: 'center', pt: 4 }}>
            Aucune version SQL enregistrée.
          </Typography>
        ) : (
          <Box sx={{ position: 'relative', pl: '26px' }}>
            {/* Vertical line */}
            <Box sx={{ position: 'absolute', left: '8px', top: '6px', bottom: '6px', width: '2px', bgcolor: BORDER }} />

            {reversed.map((entry, i) => {
              const num = reversed.length - i;
              const isCurrent = i === 0;
              const preview = entry.sql.split('\n').find(l => l.trim())?.slice(0, 55) ?? '';
              const hasOpt = entry.optimizedSql && entry.optimizedSql.trim() !== entry.sql.trim();

              return (
                <Box key={entry.id} sx={{ position: 'relative', pb: i < reversed.length - 1 ? '18px' : 0 }}>
                  {/* Timeline dot */}
                  <Box sx={{
                    position: 'absolute', left: '-26px', top: '2px',
                    width: 18, height: 18, borderRadius: '50%',
                    bgcolor: '#f4f7f7',
                    border: `2px solid ${isCurrent ? TEAL : BORDER}`,
                    display: 'grid', placeItems: 'center',
                  }}>
                    <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: isCurrent ? TEAL : MUTED }} />
                  </Box>

                  {/* Content */}
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: '7px', mb: '4px' }}>
                    <Typography sx={{ fontSize: 13, fontWeight: 600, color: INK }}>
                      Version #{num}
                    </Typography>
                    {isCurrent && (
                      <Box sx={{ fontSize: 9.5, fontWeight: 700, letterSpacing: '0.03em', textTransform: 'uppercase', bgcolor: '#ecf7f6', color: TEAL, borderRadius: 999, px: '7px', py: '1px' }}>
                        actuelle
                      </Box>
                    )}
                    {hasOpt && (
                      <Box sx={{ fontSize: 9.5, fontWeight: 700, letterSpacing: '0.03em', textTransform: 'uppercase', bgcolor: '#eef1f7', color: '#50609d', borderRadius: 999, px: '7px', py: '1px' }}>
                        optimisée
                      </Box>
                    )}
                  </Box>

                  <Typography sx={{ fontSize: 12, color: BODY, fontFamily: 'monospace', mb: '6px', lineHeight: 1.5, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {preview}…
                  </Typography>

                  {!isCurrent && (
                    <Box
                      component="button"
                      onClick={() => { onRestore(entry); onClose(); }}
                      sx={{
                        display: 'inline-flex', alignItems: 'center', gap: '4px',
                        fontSize: 11.5, fontWeight: 600, color: TEAL,
                        bgcolor: 'transparent', border: 'none', cursor: 'pointer', fontFamily: 'inherit',
                        p: '2px 0',
                        '&:hover': { textDecoration: 'underline' },
                      }}
                    >
                      <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                        <path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/>
                        <path d="M3 3v5h5"/>
                      </svg>
                      Restaurer cette version
                    </Box>
                  )}
                </Box>
              );
            })}
          </Box>
        )}
      </Box>

      {/* Footer */}
      <Box sx={{ borderTop: `1px solid ${BORDER}`, p: '11px 18px', display: 'flex', alignItems: 'center', gap: '8px', fontSize: 11.5, color: PLACEHOLDER, flexShrink: 0 }}>
        <Box sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#f7c948', boxShadow: '0 0 0 3px #fff7e6', flexShrink: 0 }} />
        Versions SQL locales · ré-exécutables sur DuckDB · 0 € facturé
      </Box>
    </Drawer>
  );
};

export default HistoryDrawer;
