import React, { useCallback, useState } from 'react';
import {
  Box,
  CircularProgress,
  Dialog,
  DialogContent,
  DialogTitle,
  IconButton,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import CloseIcon from '@mui/icons-material/Close';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import SearchIcon from '@mui/icons-material/Search';
import StorageIcon from '@mui/icons-material/Storage';
import { SqlFile, fetchModelSql } from '../api/models';
import SqlEditor from './SqlEditor';
import { BORDER, INK, MUTED, SURFACE, TEAL, TEAL_SUBTLE } from '../theme/tokens';

function SqlFileIcon({ active }: { active: boolean }) {
  return (
    <svg width="13" height="13" viewBox="0 0 24 24" fill="none">
      <path d="M8 9l-3 3 3 3" stroke={active ? TEAL : MUTED} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
      <path d="M16 9l3 3-3 3" stroke={active ? TEAL : MUTED} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function relTime(iso: string | undefined): string {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return "à l'instant";
  if (m < 60) return `il y a ${m} min`;
  const h = Math.floor(m / 60);
  if (h < 24) return `il y a ${h} h`;
  return `il y a ${Math.floor(h / 24)} j`;
}

function modelsBasePath(files: SqlFile[]): string {
  const p = files[0]?.path;
  if (!p) return '';
  const idx = p.lastIndexOf('/');
  if (idx <= 0) return p;
  return p.slice(0, idx);
}

export interface SqlFilePickerModalProps {
  sqlFiles: SqlFile[];
  excludePaths?: string[];
  title?: string;
  cta?: string;
  onClose: () => void;
  onPick: (file: SqlFile) => void;
}

export function SqlFilePickerModal({
  sqlFiles,
  excludePaths = [],
  title,
  cta,
  onClose,
  onPick,
}: SqlFilePickerModalProps) {
  const available = sqlFiles.filter((f) => !excludePaths.includes(f.name));
  const [q, setQ] = useState('');
  const [sel, setSel] = useState<SqlFile | null>(available[0] ?? null);
  const [previewSql, setPreviewSql] = useState('');
  const [loadingPreview, setLoadingPreview] = useState(false);

  const filtered = available.filter((f) =>
    (f.name + ' ' + (f.path ?? '')).toLowerCase().includes(q.toLowerCase()),
  );

  const selectFile = useCallback(async (f: SqlFile) => {
    setSel(f);
    setPreviewSql('');
    setLoadingPreview(true);
    const sql = await fetchModelSql(f.name);
    setPreviewSql(sql ?? '-- SQL non disponible');
    setLoadingPreview(false);
  }, []);

  const basePath = modelsBasePath(sqlFiles);

  return (
    <Dialog
      open
      onClose={onClose}
      maxWidth={false}
      PaperProps={{
        sx: {
          width: 780,
          maxWidth: '90vw',
          maxHeight: '80vh',
          borderRadius: '14px',
          overflow: 'hidden',
          display: 'flex',
          flexDirection: 'column',
        },
      }}
    >
      {/* Header */}
      <DialogTitle
        sx={{
          p: '14px 18px',
          borderBottom: `1px solid ${BORDER}`,
          display: 'flex',
          alignItems: 'center',
          gap: 1.5,
          flexShrink: 0,
        }}
      >
        <Box sx={{ color: TEAL, display: 'inline-flex' }}>
          <FolderOpenIcon sx={{ fontSize: 18 }} />
        </Box>
        <Box sx={{ flex: 1 }}>
          <Typography sx={{ fontSize: 15, fontWeight: 700, color: INK }}>
            {title ?? 'Choisir un fichier SQL local'}
          </Typography>
          <Typography sx={{ fontSize: 12, color: MUTED, mt: '2px' }}>
            Les tests seront générés à partir du SQL sélectionné.
          </Typography>
        </Box>
        <IconButton size="small" onClick={onClose}>
          <CloseIcon sx={{ fontSize: 16 }} />
        </IconButton>
      </DialogTitle>

      {/* Search */}
      <Box sx={{ p: '10px 16px', borderBottom: `1px solid ${BORDER}`, bgcolor: SURFACE, flexShrink: 0 }}>
        <Box sx={{ position: 'relative' }}>
          <Box
            sx={{
              position: 'absolute',
              left: 11,
              top: '50%',
              transform: 'translateY(-50%)',
              color: MUTED,
              display: 'inline-flex',
            }}
          >
            <SearchIcon sx={{ fontSize: 14 }} />
          </Box>
          <Box
            component="input"
            autoFocus
            value={q}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setQ(e.target.value)}
            placeholder="Rechercher un fichier ou un chemin…"
            sx={{
              width: '100%',
              p: '9px 12px 9px 34px',
              border: `1px solid ${BORDER}`,
              borderRadius: '9px',
              bgcolor: '#fff',
              fontSize: 13,
              outline: 'none',
              color: INK,
              fontFamily: 'inherit',
              boxSizing: 'border-box',
              '&:focus': { borderColor: TEAL },
            }}
          />
        </Box>
        {basePath && (
          <Box
            sx={{
              display: 'flex',
              alignItems: 'center',
              gap: '6px',
              mt: '6px',
              fontSize: 11.5,
              color: MUTED,
            }}
          >
            <StorageIcon sx={{ fontSize: 11 }} />
            <span>Dossier surveillé :</span>
            <Box component="code" sx={{ fontSize: 11, color: INK, fontFamily: 'monospace' }}>
              {basePath}
            </Box>
          </Box>
        )}
      </Box>

      {/* Body: list + preview */}
      <DialogContent
        sx={{
          p: 0,
          display: 'grid',
          gridTemplateColumns: '300px 1fr',
          minHeight: 0,
          overflow: 'hidden',
          flex: 1,
        }}
      >
        {/* File list */}
        <Box sx={{ borderRight: `1px solid ${BORDER}`, overflow: 'auto', p: '6px' }}>
          {filtered.length === 0 ? (
            <Box sx={{ p: '24px 12px', textAlign: 'center', fontSize: 12, color: MUTED }}>
              Aucun fichier ne correspond.
            </Box>
          ) : (
            filtered.map((f) => {
              const active = sel?.path === f.path;
              return (
                <Box
                  key={f.path}
                  onClick={() => selectFile(f)}
                  onDoubleClick={() => onPick(f)}
                  sx={{
                    p: '9px 10px',
                    borderRadius: '8px',
                    cursor: 'pointer',
                    mb: '2px',
                    bgcolor: active ? TEAL_SUBTLE : 'transparent',
                    border: `1px solid ${active ? '#d2efec' : 'transparent'}`,
                    '&:hover': { bgcolor: active ? TEAL_SUBTLE : SURFACE },
                  }}
                >
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: '7px' }}>
                    <SqlFileIcon active={active} />
                    <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: INK, flex: 1 }}>
                      {f.name}
                    </Typography>
                    {f.updated_at && (
                      <Typography sx={{ fontSize: 10.5, color: MUTED, flexShrink: 0 }}>
                        {relTime(f.updated_at)}
                      </Typography>
                    )}
                  </Box>
                  <Typography
                    sx={{
                      fontSize: 10.5,
                      color: MUTED,
                      mt: '2px',
                      pl: '20px',
                      overflow: 'hidden',
                      textOverflow: 'ellipsis',
                      whiteSpace: 'nowrap',
                    }}
                  >
                    {f.path ?? f.name}
                  </Typography>
                </Box>
              );
            })
          )}
        </Box>

        {/* SQL preview */}
        <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
          <Box
            sx={{
              p: '10px 14px',
              borderBottom: `1px solid ${BORDER}`,
              fontSize: 11,
              fontWeight: 700,
              color: MUTED,
              letterSpacing: 0.6,
              textTransform: 'uppercase',
              display: 'flex',
              alignItems: 'center',
              gap: 1,
              flexShrink: 0,
            }}
          >
            Aperçu
            {sel && (
              <Box
                component="code"
                sx={{ textTransform: 'none', letterSpacing: 0, color: INK, fontSize: 11, fontWeight: 500 }}
              >
                {sel.name}
              </Box>
            )}
          </Box>
          <Box sx={{ flex: 1, overflow: 'auto', bgcolor: '#eef2f3' }}>
            {loadingPreview ? (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, p: 3 }}>
                <CircularProgress size={14} sx={{ color: TEAL }} />
                <Typography sx={{ fontSize: 12, color: MUTED }}>Chargement…</Typography>
              </Box>
            ) : sel && previewSql ? (
              <SqlEditor value={previewSql} onChange={() => {}} readOnly maxHeight={340} />
            ) : (
              <Box sx={{ p: 3, fontSize: 12.5, color: MUTED, fontStyle: 'italic' }}>
                {sel ? 'Chargement…' : 'Sélectionner un fichier pour voir le SQL.'}
              </Box>
            )}
          </Box>
        </Box>
      </DialogContent>

      {/* Footer */}
      <Box
        sx={{
          p: '12px 16px',
          borderTop: `1px solid ${BORDER}`,
          display: 'flex',
          alignItems: 'center',
          gap: 1.5,
          bgcolor: SURFACE,
          flexShrink: 0,
        }}
      >
        <Typography sx={{ fontSize: 11.5, color: MUTED }}>
          Double-clic pour sélectionner rapidement.
        </Typography>
        <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
          <Box
            component="button"
            onClick={onClose}
            sx={{
              p: '7px 14px',
              fontSize: 12.5,
              border: `1px solid ${BORDER}`,
              borderRadius: '8px',
              bgcolor: '#fff',
              color: INK,
              cursor: 'pointer',
              fontFamily: 'inherit',
              fontWeight: 500,
            }}
          >
            Annuler
          </Box>
          <Box
            component="button"
            onClick={() => sel && onPick(sel)}
            disabled={!sel}
            sx={{
              p: '8px 16px',
              border: 'none',
              borderRadius: '8px',
              bgcolor: sel ? TEAL : '#cbd9da',
              color: '#fff',
              fontWeight: 600,
              fontSize: 13,
              cursor: sel ? 'pointer' : 'not-allowed',
              display: 'inline-flex',
              alignItems: 'center',
              gap: '6px',
              fontFamily: 'inherit',
            }}
          >
            <AddIcon sx={{ fontSize: 14 }} />
            {cta ?? 'Générer les tests'}
          </Box>
        </Box>
      </Box>
    </Dialog>
  );
}
