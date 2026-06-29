import React, { useMemo } from 'react';
import { Dialog, DialogTitle, DialogContent, DialogActions, Button, Box, Typography } from '@mui/material';

// Vue « diff des données de test » affichée après une régénération PARTIELLE sur changement
// de source. Chaque test est sérialisé en CSV par table (préfixée d'un en-tête `#nom_table`),
// et on affiche un diff ligne-à-ligne (LCS) entre l'AVANT (snapshot) et l'APRÈS (patché) :
// lignes ajoutées en vert, retirées en rouge, inchangées en gris. Aucune dépendance externe.

type TestLike = {
  test_uid?: string;
  test_index?: number | string;
  test_name?: string;
  title?: string;
  data?: Record<string, any[]>;
  test_data?: Record<string, any[]>;
};

const fmtVal = (v: any): string =>
  v === null || v === undefined ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v);

function tableCsvLines(name: string, rows: any[]): string[] {
  const cols: string[] = [];
  const seen = new Set<string>();
  (rows || []).forEach((r) =>
    Object.keys(r || {}).forEach((k) => {
      if (!seen.has(k)) { seen.add(k); cols.push(k); }
    })
  );
  const lines = [`#${name}`];
  if (cols.length) lines.push(cols.join(','));
  (rows || []).forEach((r) => lines.push(cols.map((c) => fmtVal(r?.[c])).join(',')));
  return lines;
}

function testCsvLines(test: TestLike | undefined): string[] {
  const data = test?.data ?? test?.test_data ?? {};
  const out: string[] = [];
  Object.keys(data).forEach((table, i) => {
    if (i > 0) out.push('');
    out.push(...tableCsvLines(table, data[table] || []));
  });
  return out;
}

type DiffLine = { type: 'same' | 'add' | 'del'; text: string };

function diffLines(a: string[], b: string[]): DiffLine[] {
  const n = a.length;
  const m = b.length;
  const dp: number[][] = Array.from({ length: n + 1 }, () => new Array(m + 1).fill(0));
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const out: DiffLine[] = [];
  let i = 0;
  let j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j]) { out.push({ type: 'same', text: a[i] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { out.push({ type: 'del', text: a[i] }); i++; }
    else { out.push({ type: 'add', text: b[j] }); j++; }
  }
  while (i < n) { out.push({ type: 'del', text: a[i] }); i++; }
  while (j < m) { out.push({ type: 'add', text: b[j] }); j++; }
  return out;
}

const keyOf = (t: TestLike): string => String(t.test_uid ?? t.test_index ?? '');

interface Props {
  open: boolean;
  before: TestLike[];
  after: TestLike[];
  onClose: () => void;
}

const TestDataDiff: React.FC<Props> = ({ open, before, after, onClose }) => {
  const sections = useMemo(() => {
    const beforeByKey = new Map(before.map((t) => [keyOf(t), t]));
    return after.map((t) => {
      const prev = beforeByKey.get(keyOf(t));
      const diff = diffLines(testCsvLines(prev), testCsvLines(t));
      const added = diff.filter((d) => d.type === 'add').length;
      const removed = diff.filter((d) => d.type === 'del').length;
      return {
        key: keyOf(t),
        title: t.test_name || t.title || `Test ${t.test_index ?? ''}`,
        diff,
        added,
        removed,
        changed: added > 0 || removed > 0,
      };
    });
  }, [before, after]);

  const lineColor = (type: DiffLine['type']) =>
    type === 'add' ? { bg: '#e6f4ea', fg: '#137333' } : type === 'del' ? { bg: '#fce8e6', fg: '#c5221f' } : { bg: 'transparent', fg: '#5f6368' };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Changements de données (avant → après)</DialogTitle>
      <DialogContent dividers>
        {sections.every((s) => !s.changed) && (
          <Typography variant="body2" sx={{ color: '#5f6368', mb: 2 }}>
            Aucune donnée d'entrée n'a changé — seules les colonnes/tables du diff de schéma ont été ajustées.
          </Typography>
        )}
        {sections.map((s) => (
          <Box key={s.key} sx={{ mb: 2.5 }}>
            <Typography variant="subtitle2" sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 0.5 }}>
              {s.title}
              {s.changed ? (
                <Box component="span" sx={{ fontSize: 12, color: '#5f6368' }}>
                  (<Box component="span" sx={{ color: '#137333' }}>+{s.added}</Box>{' / '}
                  <Box component="span" sx={{ color: '#c5221f' }}>−{s.removed}</Box> lignes)
                </Box>
              ) : (
                <Box component="span" sx={{ fontSize: 12, color: '#9aa0a6' }}>(inchangé)</Box>
              )}
            </Typography>
            <Box
              component="pre"
              sx={{
                m: 0, p: 1, borderRadius: 1, border: '1px solid #e0e0e0', bgcolor: '#fafafa',
                fontFamily: 'monospace', fontSize: 12, lineHeight: 1.5, overflowX: 'auto', whiteSpace: 'pre',
              }}
            >
              {s.diff.map((d, idx) => {
                const c = lineColor(d.type);
                const prefix = d.type === 'add' ? '+ ' : d.type === 'del' ? '− ' : '  ';
                return (
                  <Box key={idx} component="div" sx={{ bgcolor: c.bg, color: c.fg }}>
                    {prefix}{d.text}
                  </Box>
                );
              })}
            </Box>
          </Box>
        ))}
      </DialogContent>
      <DialogActions>
        <Button onClick={onClose}>Fermer</Button>
      </DialogActions>
    </Dialog>
  );
};

export default TestDataDiff;
