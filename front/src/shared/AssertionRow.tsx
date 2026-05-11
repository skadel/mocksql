import React, { useState } from 'react';
import { Box, Typography } from '@mui/material';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DeleteIcon from '@mui/icons-material/Delete';
import { AssertionItem } from '../utils/types';
import { INK, MUTED } from '../theme/tokens';

interface AssertionRowProps {
  a: AssertionItem;
  expanded: boolean;
  onToggle: () => void;
  onDelete?: () => void;
}

export function AssertionRow({ a, expanded, onToggle, onDelete }: AssertionRowProps) {
  const statusColor = a.passed ? '#23a26d' : '#d0503f';
  const statusBg    = a.passed ? '#eaf5f0' : '#fbeceb';
  const failCount   = a.failing_rows?.length ?? 0;
  return (
    <Box sx={{ borderTop: '1px solid #eff3f4', '&:first-of-type': { borderTop: 'none' } }}>
      <Box onClick={onToggle} sx={{ p: '8px 12px', display: 'flex', alignItems: 'center', gap: 1.25, cursor: 'pointer', '&:hover': { bgcolor: '#fafcfc' } }}>
        <Box sx={{ width: 18, height: 18, borderRadius: '50%', bgcolor: statusBg, color: statusColor, display: 'grid', placeItems: 'center', flexShrink: 0 }}>
          {a.passed ? <CheckCircleIcon sx={{ fontSize: 11 }} /> : <CancelIcon sx={{ fontSize: 11 }} />}
        </Box>
        <Typography sx={{ flex: 1, fontSize: 12.5, color: INK, lineHeight: 1.45, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: expanded ? 'normal' : 'nowrap' }}>
          {a.description}
        </Typography>
        {!a.passed && failCount > 0 && !expanded && (
          <Typography sx={{ fontSize: 11, color: statusColor, fontWeight: 600, flexShrink: 0 }}>
            {failCount} ligne{failCount > 1 ? 's' : ''} en échec
          </Typography>
        )}
        <Box sx={{ transform: expanded ? 'rotate(0deg)' : 'rotate(-90deg)', transition: 'transform 0.15s', display: 'inline-flex', color: MUTED, flexShrink: 0 }}>
          <ExpandMoreIcon sx={{ fontSize: 14 }} />
        </Box>
      </Box>
      {expanded && (
        <Box sx={{ px: '12px', pb: '10px', pl: '42px', bgcolor: '#fafbfc' }}>
          {a.error && (
            <Typography sx={{ fontSize: 11.5, color: '#d0503f', fontFamily: 'monospace', mb: 0.75 }}>{a.error}</Typography>
          )}
          {!a.passed && failCount > 0 && (
            <Typography sx={{ fontSize: 11.5, color: '#d0503f', mb: 0.75 }}>
              {failCount} ligne{failCount > 1 ? 's' : ''} en échec
            </Typography>
          )}
          {a.sql && (
            <Box component="pre" sx={{ m: 0, mb: 1, p: '8px 10px', fontSize: 11.5, fontFamily: "'JetBrains Mono', monospace", bgcolor: '#eef2f3', borderRadius: '7px', overflowX: 'auto', color: '#2b3b3e', lineHeight: 1.5, border: '1px solid #dce4e6' }}>
              {a.sql}
            </Box>
          )}
          {onDelete && (
            <Box component="button" onClick={onDelete}
              sx={{ display: 'inline-flex', alignItems: 'center', gap: '4px', px: '9px', py: '4px', fontSize: 11, fontWeight: 500, border: '1px solid #e4eaec', borderRadius: '7px', bgcolor: '#fff', color: '#d0503f', cursor: 'pointer', fontFamily: 'inherit', '&:hover': { bgcolor: '#fbeceb', borderColor: '#d0503f' } }}>
              <DeleteIcon sx={{ fontSize: 12 }} /> Supprimer
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
}

export function AssertionList({ assertions, readOnly = false, onDelete }: {
  assertions: AssertionItem[];
  readOnly?: boolean;
  onDelete?: (i: number) => void;
}) {
  const [expandedSet, setExpandedSet] = useState<Set<number>>(() => {
    const s = new Set<number>();
    assertions.forEach((a, i) => { if (!a.passed) s.add(i); });
    return s;
  });

  function toggle(i: number) {
    setExpandedSet(prev => {
      const n = new Set(prev);
      if (n.has(i)) n.delete(i); else n.add(i);
      return n;
    });
  }

  return (
    <>
      {assertions.map((a, i) => (
        <AssertionRow
          key={i}
          a={a}
          expanded={expandedSet.has(i)}
          onToggle={() => toggle(i)}
          onDelete={readOnly ? undefined : () => onDelete?.(i)}
        />
      ))}
    </>
  );
}
