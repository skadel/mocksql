import FolderIcon from '@mui/icons-material/Folder';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import KeyboardArrowRightIcon from '@mui/icons-material/KeyboardArrowRight';
import { Box, Chip, Typography } from '@mui/material';
import React, { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useTranslation } from 'react-i18next';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { resetContext } from '../../buildModel/buildModelSlice';
import { setCurrentId } from '../appBarSlice';
import { Model } from '../../../utils/types';
import { relativeDate } from '../../../utils/dates';

const TEAL  = '#2BB0A8';
const MUTED = '#6b8287';
const INK   = '#0f272a';
const LINE  = '#c9d3d6';

/* ── Tree data structures ─────────────────────────────────────── */

interface FolderNode {
  type: 'folder';
  name: string;
  path: string;
  children: TreeNode[];
}

interface FileNode {
  type: 'file';
  model: Model;
}

type TreeNode = FolderNode | FileNode;

function buildTree(models: Model[]): TreeNode[] {
  const root: TreeNode[] = [];
  const folderMap = new Map<string, FolderNode>();

  const getOrCreate = (folderPath: string): FolderNode => {
    if (folderMap.has(folderPath)) return folderMap.get(folderPath)!;
    const parts      = folderPath.split('/');
    const name       = parts[parts.length - 1];
    const parentPath = parts.slice(0, -1).join('/');
    const node: FolderNode = { type: 'folder', name, path: folderPath, children: [] };
    folderMap.set(folderPath, node);
    if (parentPath) {
      getOrCreate(parentPath).children.push(node);
    } else {
      root.push(node);
    }
    return node;
  };

  for (const model of models) {
    const file: FileNode = { type: 'file', model };
    if (model.folder) {
      getOrCreate(model.folder).children.push(file);
    } else {
      root.push(file);
    }
  }

  return root;
}

function folderHasMatch(node: FolderNode, q: string): boolean {
  return node.children.some(child => {
    if (child.type === 'file') {
      return (child.model.name ?? child.model.session_id ?? '').toLowerCase().includes(q);
    }
    return folderHasMatch(child, q);
  });
}

function countFiles(node: FolderNode): number {
  return node.children.reduce<number>((acc, child) => {
    if (child.type === 'file') return acc + 1;
    return acc + countFiles(child);
  }, 0);
}

/* ── SQL file icon ────────────────────────────────────────────── */

function SqlIcon({ color }: { color: string }) {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none" style={{ flexShrink: 0 }}>
      <rect x="3" y="3" width="18" height="18" rx="3" stroke={color} strokeWidth="2" />
      <path d="M8 9h3M8 12h8M8 15h5" stroke={color} strokeWidth="1.8" strokeLinecap="round" />
    </svg>
  );
}

/* ── File row ─────────────────────────────────────────────────── */

interface FileRowProps {
  model: Model;
  depth: number;
  currentModelId?: string;
}

const FileRow: React.FC<FileRowProps> = ({ model, depth, currentModelId }) => {
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const { t } = useTranslation();
  const isTested = model.isTested ?? true;
  const isActive = isTested && model.session_id === currentModelId;

  const handleClick = () => {
    dispatch(resetContext());
    dispatch(setCurrentId(''));
    if (isTested) {
      navigate(`/models/${model.session_id}`);
    } else {
      navigate(`/?model=${encodeURIComponent(model.session_id)}`);
    }
  };

  return (
    <Box
      onClick={handleClick}
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: '7px',
        pl: `${16 + depth * 16}px`,
        pr: '12px',
        py: '6px',
        mx: '6px',
        borderRadius: '7px',
        cursor: 'pointer',
        bgcolor: isActive ? '#e6f4f3' : 'transparent',
        '&:hover': { bgcolor: isActive ? '#e6f4f3' : '#edf0f1' },
        transition: 'background .12s',
      }}
    >
      <SqlIcon color={isActive ? TEAL : isTested ? '#23a26d' : LINE} />
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography
          noWrap
          sx={{
            fontSize: 12.5,
            fontWeight: isActive ? 600 : 400,
            color: isActive ? TEAL : INK,
            lineHeight: 1.3,
          }}
        >
          {model.name || model.session_id}
        </Typography>
        <Typography sx={{ fontSize: 10.5, color: '#a0adb0', whiteSpace: 'nowrap', mt: '1px' }}>
          {isTested ? relativeDate(model.updateDate, t) : t('model.not_tested')}
        </Typography>
      </Box>
    </Box>
  );
};

/* ── Tree node renderer (recursive) ──────────────────────────── */

interface NodeProps {
  node: TreeNode;
  depth: number;
  q: string;
  currentModelId?: string;
}

const TreeNodeComponent: React.FC<NodeProps> = ({ node, depth, q, currentModelId }) => {
  const [open, setOpen] = useState(true);

  if (node.type === 'file') {
    if (q && !(node.model.name ?? node.model.session_id ?? '').toLowerCase().includes(q)) {
      return null;
    }
    return <FileRow model={node.model} depth={depth} currentModelId={currentModelId} />;
  }

  const visible = q ? folderHasMatch(node, q) : true;
  if (!visible) return null;

  const isOpen  = q ? true : open;
  const nFiles  = countFiles(node);

  return (
    <>
      <Box
        onClick={() => !q && setOpen(o => !o)}
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: '4px',
          pl: `${8 + depth * 16}px`,
          pr: '10px',
          py: '5px',
          mx: '6px',
          borderRadius: '6px',
          cursor: q ? 'default' : 'pointer',
          userSelect: 'none',
          '&:hover': { bgcolor: q ? 'transparent' : '#edf0f1' },
        }}
      >
        {/* Animated chevron */}
        <KeyboardArrowRightIcon
          sx={{
            fontSize: 14,
            color: MUTED,
            flexShrink: 0,
            transform: isOpen ? 'rotate(90deg)' : 'rotate(0deg)',
            transition: 'transform 0.15s ease',
          }}
        />

        {/* Folder icon */}
        {isOpen
          ? <FolderOpenIcon sx={{ fontSize: 15, color: TEAL,  flexShrink: 0 }} />
          : <FolderIcon     sx={{ fontSize: 15, color: MUTED, flexShrink: 0 }} />}

        <Typography
          sx={{
            fontSize: 12,
            fontWeight: 600,
            color: isOpen ? INK : MUTED,
            flex: 1,
            letterSpacing: '0.1px',
          }}
        >
          {node.name}
        </Typography>

        {/* File count badge */}
        {!q && (
          <Chip
            label={nFiles}
            size="small"
            sx={{
              height: 16,
              fontSize: 10,
              fontWeight: 600,
              color: MUTED,
              bgcolor: '#e4eaec',
              border: 'none',
              '& .MuiChip-label': { px: '5px' },
            }}
          />
        )}
      </Box>

      {/* Animated children */}
      <Box
        sx={{
          overflow: 'hidden',
          maxHeight: isOpen ? '9999px' : 0,
          transition: 'max-height 0.18s ease',
        }}
      >
        {/* Left indent guide line */}
        <Box sx={{ position: 'relative' }}>
          <Box
            sx={{
              position: 'absolute',
              left: `${16 + depth * 16}px`,
              top: 0,
              bottom: 0,
              width: '1px',
              bgcolor: LINE,
            }}
          />
          {node.children.map(child => (
            <TreeNodeComponent
              key={child.type === 'file' ? child.model.session_id : child.path}
              node={child}
              depth={depth + 1}
              q={q}
              currentModelId={currentModelId}
            />
          ))}
        </Box>
      </Box>
    </>
  );
};

/* ── Root list ────────────────────────────────────────────────── */

interface Props {
  search: string;
}

const SqlFileList: React.FC<Props> = ({ search }) => {
  const { t } = useTranslation();
  const allModels      = useAppSelector(s => s.appBarModel.models);
  const currentModelId = useAppSelector(s => s.appBarModel.currentModelId);

  const models = useMemo(() => allModels.filter(m => m.isTested), [allModels]);
  const q      = search.toLowerCase().trim();
  const tree   = useMemo(() => buildTree(models), [models]);

  const hasResults = q
    ? models.some(m => (m.name ?? m.session_id ?? '').toLowerCase().includes(q))
    : models.length > 0;

  if (!hasResults) {
    return (
      <Box sx={{ p: '24px 16px', textAlign: 'center' }}>
        <Typography sx={{ fontSize: 12.5, color: MUTED }}>
          {q ? t('search.no_results') : t('search.no_models')}
        </Typography>
      </Box>
    );
  }

  return (
    <>
      {tree.map(node => (
        <TreeNodeComponent
          key={node.type === 'file' ? node.model.session_id : node.path}
          node={node}
          depth={0}
          q={q}
          currentModelId={currentModelId}
        />
      ))}
    </>
  );
};

export default SqlFileList;
