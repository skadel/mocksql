import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import InfoIcon from '@mui/icons-material/Info';
import UploadIcon from '@mui/icons-material/Upload';
import {
  Box,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Tooltip,
  Typography,
} from '@mui/material';
import { CodeBlockIconButton, GhostButton, TealIconButton } from '../style/AppButtons';
import { highlight, languages } from 'prismjs';
import React, { useId, useImperativeHandle, useRef, useState } from 'react';
import { StyledUploadButton } from '../style/StyledComponents';

export interface BigQueryUploaderHandle {
  triggerUpload: () => void;
}

interface BigQueryUploaderProps {
  sqlQuery: string;
  onFileContent: (content: string, fileName: string) => void;
  accept?: string;
  disabled?: boolean;
  uploadLabel?: string;
  instructionsTitle?: string;
  downloadFormat?: string;
  inline?: boolean;
}

const SqlBlock: React.FC<{ sqlQuery: string }> = ({ sqlQuery }) => (
  <Box sx={{ bgcolor: '#f5f5f5', borderRadius: 2 }}>
    <Box sx={{ display: 'flex', justifyContent: 'flex-end', px: 1, pt: 1 }}>
      <CodeBlockIconButton
        onClick={() => navigator.clipboard.writeText(sqlQuery)}
        size="small"
      >
        <ContentCopyIcon fontSize="small" />
      </CodeBlockIconButton>
    </Box>
    <Box sx={{ overflow: 'auto', p: 2, pt: 0 }}>
      <Box
        component="pre"
        sx={{ fontFamily: '"Fira code", "Fira Mono", monospace', fontSize: 12, m: 0, background: '#f5f5f5', borderRadius: '4px' }}
        dangerouslySetInnerHTML={{ __html: highlight(sqlQuery || '', languages.sql, 'sql') }}
      />
    </Box>
  </Box>
);

const BigQueryUploader = React.forwardRef<BigQueryUploaderHandle, BigQueryUploaderProps>(({
  sqlQuery,
  onFileContent,
  accept = '.json',
  disabled,
  uploadLabel = 'Uploader les résultats',
  instructionsTitle = 'Instructions',
  downloadFormat,
  inline = false,
}, ref) => {
  const [open, setOpen] = useState(false);
  const inputId = useId();
  const inputRef = useRef<HTMLInputElement>(null);

  useImperativeHandle(ref, () => ({
    triggerUpload: () => inputRef.current?.click(),
  }));

  const fmt = downloadFormat ?? (accept === '.json' ? 'JSON' : 'CSV ou JSON');

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      onFileContent(ev.target?.result as string, file.name);
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  const uploadButton = (
    <Box display="flex" alignItems="center" gap={1}>
      <label htmlFor={inputId} style={{ pointerEvents: disabled ? 'none' : 'auto' }}>
        <StyledUploadButton as="span" sx={disabled ? { opacity: 0.5 } : {}}>
          <UploadIcon /> {uploadLabel}
        </StyledUploadButton>
      </label>
      <input
        ref={inputRef}
        id={inputId}
        type="file"
        accept={accept}
        disabled={disabled}
        style={{ display: 'none' }}
        onChange={handleFileChange}
      />
    </Box>
  );

  if (inline) {
    return (
      <Box>
        <Typography variant="body2" sx={{ mb: 1 }}>
          Ouvrez <strong>BigQuery</strong>, exécutez la requête ci-dessous, téléchargez les résultats au format <strong>{fmt}</strong> et importez-les.
        </Typography>
        <SqlBlock sqlQuery={sqlQuery} />
        <Box sx={{ mt: 2 }}>{uploadButton}</Box>
      </Box>
    );
  }

  return (
    <>
      <Box display="flex" alignItems="center" gap={1}>
        {uploadButton}
        <Tooltip title="Instructions" arrow>
          <TealIconButton onClick={() => setOpen(true)} sx={{ padding: '8px' }}>
            <InfoIcon />
          </TealIconButton>
        </Tooltip>
      </Box>

      <Dialog open={open} onClose={() => setOpen(false)} maxWidth="md" fullWidth>
        <DialogTitle sx={{ color: '#1ca8a4' }}>{instructionsTitle}</DialogTitle>
        <DialogContent>
          <ol style={{ paddingLeft: 20, margin: 0 }}>
            <li>
              <Typography variant="body2" paragraph>
                Ouvrez la console <strong>BigQuery</strong> de Google Cloud Platform.
              </Typography>
            </li>
            <li>
              <Typography variant="body2" sx={{ mb: 1 }}>
                Exécutez la requête SQL suivante dans l'éditeur de requêtes.
              </Typography>
              <Box sx={{ mb: 2 }}>
                <SqlBlock sqlQuery={sqlQuery} />
              </Box>
            </li>
            <li>
              <Typography variant="body2" paragraph>
                Téléchargez les résultats au format <strong>{fmt}</strong>.
              </Typography>
            </li>
            <li>
              <Typography variant="body2">
                Importez le fichier via le bouton <strong>"{uploadLabel}"</strong> ci-dessus.
              </Typography>
            </li>
          </ol>
        </DialogContent>
        <DialogActions>
          <GhostButton onClick={() => setOpen(false)}>Fermer</GhostButton>
        </DialogActions>
      </Dialog>
    </>
  );
});

export default BigQueryUploader;
