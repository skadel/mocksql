import { ContentCopy } from '@mui/icons-material';
import { Box, Dialog, DialogContent, DialogTitle, IconButton } from '@mui/material';
import { highlight, languages } from 'prismjs';
import React from 'react';

interface SQLCodeDialogProps {
  open: boolean;
  sqlCode: string | null;
  onClose: () => void;
}

const SQLCodeDialog: React.FC<SQLCodeDialogProps> = ({ open, sqlCode, onClose }) => {
  const handleCopyToClipboard = () => {
    if (sqlCode) {
      navigator.clipboard.writeText(sqlCode);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
      <DialogTitle>Requête SQL</DialogTitle>
      <DialogContent>
        <Box
          sx={{
            position: 'relative',
            backgroundColor: '#f5f5f5',
            borderRadius: 2,
            padding: 2,
            overflow: 'auto',
          }}
        >
          <IconButton
            onClick={handleCopyToClipboard}
            size="small"
            sx={{
              position: 'absolute',
              top: 8,
              right: 8,
              color: '#f8f8f2',
              backgroundColor: '#333',
              '&:hover': {
                backgroundColor: '#444',
              },
            }}
          >
            <ContentCopy />
          </IconButton>
          <Box
            component="pre"
            sx={{
              fontFamily: '"Fira code", "Fira Mono", monospace',
              fontSize: 12,
              minHeight: '200px',
              width: '100%',
              background: '#f5f5f5',
              borderRadius: '4px',
            }}
            dangerouslySetInnerHTML={{
              __html: highlight(sqlCode || '', languages.sql, 'sql'),
            }}
          />
        </Box>
      </DialogContent>
    </Dialog>
  );
};

export default SQLCodeDialog;
