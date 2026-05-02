import FolderIcon from '@mui/icons-material/Folder';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import { Box, CircularProgress, Tooltip, Typography } from '@mui/material';
import React, { useEffect, useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useAppDispatch } from '../../../app/hooks';
import { resetContext } from '../../buildModel/buildModelSlice';
import { setCurrentId } from '../appBarSlice';
import { fetchModelPriority } from '../../../api/models';
import { ExploreModel } from '../../../utils/types';

const MUTED = '#6b8287';
const INK   = '#0f272a';

/* ── Tree structures ────────────────────────────────────────── */

interface ExploreFolderNode {
  type: 'folder';
  name: string;
  path: string;
  children: ExploreTreeNode[];
}

interface ExploreFileNode {
  type: 'file';
  model: ExploreModel;
}

type ExploreTreeNode = ExploreFolderNode | ExploreFileNode;

function buildExploreTree(models: ExploreModel[]): ExploreTreeNode[] {
  const root: ExploreTreeNode[] = [];
  const folderMap = new Map<string, ExploreFolderNode>();

  const getOrCreate = (folderPath: string): ExploreFolderNode => {
    if (folderMap.has(folderPath)) return folderMap.get(folderPath)!;
    const parts      = folderPath.split('/');
    const name       = parts[parts.length - 1];
    const parentPath = parts.slice(0, -1).join('/');
    const node: ExploreFolderNode = { type: 'folder', name, path: folderPath, children: [] };
    folderMap.set(folderPath, node);
    if (parentPath) {
      getOrCreate(parentPath).children.push(node);
    } else {
      root.push(node);
    }
    return node;
  };

  for (const model of models) {
    const lastSlash = model.name.lastIndexOf('/');
    const folder    = lastSlash >= 0 ? model.name.slice(0, lastSlash) : undefined;
    const file: ExploreFileNode = { type: 'file', model };
    if (folder) {
      getOrCreate(folder).children.push(file);
    } else {
      root.push(file);
    }
  }

  return root;
}

function countExploreFiles(node: ExploreFolderNode): number {
  return node.children.reduce<number>((acc, child) => {
    if (child.type === 'file') return acc + 1;
    return acc + countExploreFiles(child);
  }, 0);
}

/* ── Priority score badge ───────────────────────────────────── */

function scoreColor(score: number): string {
  if (score >= 70) return '#d0503f';
  if (score >= 40) return '#d89323';
  return '#23a26d';
}

function ScoreBadge({ score }: { score: number }) {
  const color = scoreColor(score);
  return (
    <Tooltip title={`Score de priorité : ${Math.round(score)}/100`} placement="left">
      <Box sx={{
        minWidth: 28,
        height: 17,
        borderRadius: '9px',
        bgcolor: `${color}18`,
        border: `1px solid ${color}40`,
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        px: '5px',
        flexShrink: 0,
      }}>
        <Typography sx={{ fontSize: 10, fontWeight: 700, color, lineHeight: 1 }}>
          {Math.round(score)}
        </Typography>
      </Box>
    </Tooltip>
  );
}

/* ── Complexity chips ───────────────────────────────────────── */

function ComplexityChips({ breakdown }: { breakdown: ExploreModel['complexity_breakdown'] }) {
  const chips: { label: string; title: string }[] = [];
  if (breakdown.window_functions) chips.push({ label: `${breakdown.window_functions}W`, title: `${breakdown.window_functions} window function(s)` });
  if (breakdown.case_when)        chips.push({ label: `${breakdown.case_when}C`,         title: `${breakdown.case_when} CASE WHEN` });
  if (breakdown.regex)            chips.push({ label: `${breakdown.regex}R`,              title: `${breakdown.regex} regex` });
  if (!chips.length) return null;

  return (
    <Box sx={{ display: 'flex', gap: '2px', flexShrink: 0 }}>
      {chips.map(c => (
        <Tooltip key={c.label} title={c.title} placement="top">
          <Box sx={{
            fontSize: 9, fontWeight: 600, color: MUTED,
            bgcolor: '#edf0f1', borderRadius: '4px',
            px: '4px', py: '1px', lineHeight: 1.4, flexShrink: 0,
          }}>
            {c.label}
          </Box>
        </Tooltip>
      ))}
    </Box>
  );
}

/* ── File row ───────────────────────────────────────────────── */

function ExploreFileRow({ model, depth }: { model: ExploreModel; depth: number }) {
  const navigate = useNavigate();
  const dispatch = useAppDispatch();

  const lastSlash  = model.name.lastIndexOf('/');
  const displayName = lastSlash >= 0 ? model.name.slice(lastSlash + 1) : model.name;

  const handleClick = () => {
    if (model.is_tested && model.session_id) {
      dispatch(setCurrentId(model.session_id));
      navigate(`/models/${model.session_id}`);
    } else {
      dispatch(resetContext());
      dispatch(setCurrentId(''));
      navigate(`/?model=${encodeURIComponent(model.model_name)}&forceNew=1`);
    }
  };

  return (
    <Box
      onClick={handleClick}
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: '5px',
        pl: `${depth * 16 + 12}px`,
        pr: '8px',
        py: '5px',
        cursor: 'pointer',
        borderRadius: '6px',
        mx: '4px',
        '&:hover': { bgcolor: '#e8ecee' },
      }}
    >
      {/* Tested indicator */}
      <Box sx={{ width: 12, height: 12, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0 }}>
        {model.is_tested
          ? <CheckCircleIcon sx={{ fontSize: 11, color: '#23a26d' }} />
          : <Box sx={{ width: 7, height: 7, borderRadius: '50%', border: `1.5px solid ${MUTED}` }} />}
      </Box>

      <Typography noWrap sx={{ fontSize: 12.5, color: INK, flex: 1, minWidth: 0 }} title={model.name}>
        {displayName}
      </Typography>

      <ComplexityChips breakdown={model.complexity_breakdown} />

      {model.recent_commits > 0 && (
        <Tooltip title={`${model.recent_commits} commit(s) ces 90 derniers jours`} placement="top">
          <Typography sx={{ fontSize: 10, color: MUTED, flexShrink: 0, letterSpacing: '-0.3px' }}>
            {model.recent_commits}↑
          </Typography>
        </Tooltip>
      )}

      <ScoreBadge score={model.priority_score} />
    </Box>
  );
}

/* ── Folder node ────────────────────────────────────────────── */

function ExploreFolderNodeComponent({ node, depth, q }: { node: ExploreFolderNode; depth: number; q: string }) {
  const [open, setOpen] = useState(depth === 0);
  const fileCount = countExploreFiles(node);

  return (
    <Box>
      <Box
        onClick={() => setOpen(o => !o)}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
          pl: `${depth * 16 + 8}px`,
          pr: '8px',
          py: '5px',
          cursor: 'pointer',
          borderRadius: '6px',
          mx: '4px',
          '&:hover': { bgcolor: '#e8ecee' },
        }}
      >
        <KeyboardArrowRightIcon sx={{
          fontSize: 13, color: MUTED, flexShrink: 0,
          transition: 'transform .15s', transform: open ? 'rotate(90deg)' : 'none',
        }} />
        {open
          ? <FolderOpenIcon sx={{ fontSize: 13, color: '#d89323', flexShrink: 0 }} />
          : <FolderIcon     sx={{ fontSize: 13, color: MUTED,     flexShrink: 0 }} />}
        <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: INK, flex: 1 }}>
          {node.name}
        </Typography>
        <Typography sx={{ fontSize: 10.5, color: MUTED }}>{fileCount}</Typography>
      </Box>

      {open && node.children.map(child => (
        child.type === 'file'
          ? <ExploreFileRow key={child.model.name} model={child.model} depth={depth + 1} />
          : <ExploreFolderNodeComponent key={child.path} node={child} depth={depth + 1} q={q} />
      ))}
    </Box>
  );
}

/* ── Root component ─────────────────────────────────────────── */

interface Props {
  search: string;
}

const ModelExplorer: React.FC<Props> = ({ search }) => {
  const [models, setModels]   = useState<ExploreModel[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError]     = useState<string | null>(null);

  useEffect(() => {
    setLoading(true);
    fetchModelPriority()
      .then(setModels)
      .catch(e => setError(String(e)))
      .finally(() => setLoading(false));
  }, []);

  const q = search.toLowerCase().trim();

  const filtered = useMemo(() => {
    if (!q) return models;
    return models.filter(m => m.name.toLowerCase().includes(q));
  }, [models, q]);

  const tree = useMemo(() => buildExploreTree(filtered), [filtered]);

  if (loading) {
    return (
      <Box sx={{ display: 'flex', justifyContent: 'center', pt: '32px' }}>
        <CircularProgress size={20} sx={{ color: '#2BB0A8' }} />
      </Box>
    );
  }

  if (error) {
    return (
      <Box sx={{ p: '16px', textAlign: 'center' }}>
        <Typography sx={{ fontSize: 12, color: '#d0503f' }}>{error}</Typography>
      </Box>
    );
  }

  if (!filtered.length) {
    return (
      <Box sx={{ p: '24px 16px', textAlign: 'center' }}>
        <Typography sx={{ fontSize: 12.5, color: MUTED }}>
          {q ? 'Aucun résultat' : 'Aucun modèle trouvé'}
        </Typography>
      </Box>
    );
  }

  const untestedCount = filtered.filter(m => !m.is_tested).length;

  return (
    <>
      {untestedCount > 0 && !q && (
        <Box sx={{
          display: 'flex', alignItems: 'center', gap: '6px',
          mx: '6px', mb: '4px', px: '10px', py: '6px',
          bgcolor: '#f0faf8', border: '1px solid #a8ddd8', borderRadius: '8px',
        }}>
          <Typography sx={{ fontSize: 11.5, fontWeight: 600, color: '#1a7a74', flex: 1 }}>
            {untestedCount} modèle{untestedCount > 1 ? 's' : ''} sans tests
          </Typography>
          <Typography sx={{ fontSize: 10.5, color: '#2BB0A8' }}>à couvrir</Typography>
        </Box>
      )}

      {tree.map(node => (
        node.type === 'file'
          ? <ExploreFileRow key={node.model.name} model={node.model} depth={0} />
          : <ExploreFolderNodeComponent key={node.path} node={node} depth={0} q={q} />
      ))}
    </>
  );
};

export default ModelExplorer;
