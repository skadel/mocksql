import React, { useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import { Alert, Box, Chip, Stack, Typography } from '@mui/material';
import DownloadIcon from '@mui/icons-material/Download';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import DisplayTable from './DisplayTable';
import { StyledButton } from '../../../style/StyledComponents';
import type { Message } from '../../../utils/types';

type MessageBodyProps = {
  msg: Message;
  currentModelId?: string;
  currentProjectId?: string;
  currentProjectDialect?: string;
  currentModelName?: string;
  onUpload?: (
    messageId: string,
    parent: string | undefined,
    type: Message['type'] | undefined,
    uploadedData: Record<string, any[]>
  ) => void;
  onProfileUpload?: (messageId: string, parent: string | undefined, jsonContent: string) => void;
  onPageChange?: (page: number, project: string, sql: string, msgId: string, limit?: number) => void;
  onExecute?: (id: string) => void;
  onCreateClick?: (id: string) => void;
  onSuggestionClick?: (text: string) => void;
};

const MessageBody: React.FC<MessageBodyProps> = ({
  msg,
  currentModelId,
  currentProjectId,
  currentModelName = 'data',
  onUpload,
  onProfileUpload,
  onPageChange,
  onExecute,
  onCreateClick,
  onSuggestionClick,
}) => {
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleDownloadSQL = (sql: string) => {
    const blob = new Blob([sql], { type: 'text/plain' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'profile_query.sql';
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleProfileFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (ev) => {
      const content = ev.target?.result as string;
      onProfileUpload?.((msg as any).id, (msg as any).parent, content);
    };
    reader.readAsText(file);
    e.target.value = '';
  };

  return (
    <>
      {/* Label de contexte pour les demandes de modification de tests */}
      {msg.contentType === 'examples_update' && (
        <Typography
          variant="caption"
          sx={{ display: 'block', color: '#888', fontStyle: 'italic', mb: 0.5 }}
        >
          Demande de modification des tests
        </Typography>
      )}

      {/* Profile request */}
      {msg.contents.profileRequest && (() => {
        const pr = msg.contents.profileRequest!;
        return (
          <Box sx={{ mt: 0.5 }}>
            <Typography variant="body2" sx={{ mb: 1.5, color: '#444', whiteSpace: 'pre-wrap' }}>
              {pr.message}
            </Typography>

            {/* Missing columns grouped by table */}
            <Stack gap={1} sx={{ mb: 1.5 }}>
              {pr.missing_columns.map((entry) => (
                <Box key={entry.table}>
                  <Typography variant="caption" sx={{ fontWeight: 700, color: '#555', display: 'block', mb: 0.5 }}>
                    {entry.table}
                  </Typography>
                  <Stack direction="row" flexWrap="wrap" gap={0.75}>
                    {entry.used_columns.map((col) => (
                      <Chip
                        key={col}
                        label={col}
                        size="small"
                        sx={{ bgcolor: '#fff8e1', border: '1px solid #ffe082', fontSize: 11 }}
                      />
                    ))}
                  </Stack>
                </Box>
              ))}
            </Stack>

            {/* Actions */}
            <Stack direction="row" gap={1.5} flexWrap="wrap">
              <StyledButton onClick={() => handleDownloadSQL(pr.profile_query)} sx={{ fontSize: 12, px: 1, py: 0.5, '& .MuiSvgIcon-root': { fontSize: 16 } }}>
                <DownloadIcon sx={{ mr: 0.5, fontSize: 16 }} />
                Télécharger la requête SQL
              </StyledButton>

              <StyledButton onClick={() => fileInputRef.current?.click()} sx={{ fontSize: 12, px: 1, py: 0.5, '& .MuiSvgIcon-root': { fontSize: 16 } }}>
                <UploadFileIcon sx={{ mr: 0.5, fontSize: 16 }} />
                Uploader les résultats JSON
              </StyledButton>
              <input
                ref={fileInputRef}
                type="file"
                accept=".json,application/json"
                style={{ display: 'none' }}
                onChange={handleProfileFileChange}
              />
            </Stack>
          </Box>
        );
      })()}

      {/* Evaluation card */}
      {msg.contentType === 'evaluation' && msg.contents.text && (
        <Box
          sx={{
            mt: 1,
            p: 1.5,
            bgcolor: '#f8fffe',
            borderRadius: '8px',
            border: '1px solid #c8e6e4',
            borderLeft: '3px solid #1ca8a4',
            fontSize: 13,
            color: '#333',
            '& p': { margin: '0 0 6px 0' },
            '& p:last-child': { marginBottom: 0 },
            '& strong': { color: '#1ca8a4' },
          }}
        >
          <ReactMarkdown>{msg.contents.text}</ReactMarkdown>
        </Box>
      )}

      {/* Texte */}
      {msg.contents.text && msg.contentType !== 'evaluation' && (
        msg.type === 'user' ? (
          <Typography
            variant="body2"
            sx={{ mt: 0.5, textAlign: 'right', whiteSpace: 'pre-wrap', color: '#333' }}
          >
            {msg.contents.text}
          </Typography>
        ) : (
          <div style={{ marginTop: '4px', overflowX: 'auto' }}>
            <ReactMarkdown>{msg.contents.text}</ReactMarkdown>
          </div>
        )
      )}

      {/* Unit Tests — compact summary, full panel is on the left */}
      {Array.isArray(msg.contents.tables) && (msg.contents.tables as any[]).length > 0 && (
        <Box
          sx={{
            display: 'inline-flex',
            alignItems: 'center',
            mt: 1.5,
            px: 1.5,
            py: 0.75,
            bgcolor: '#f0fafa',
            borderRadius: '10px',
            border: '1px solid #d0eeec',
          }}
        >
          <Typography variant="body2" sx={{ fontWeight: 700, color: '#1ca8a4' }}>
            {msg.testIndex !== undefined
              ? `✅ Données du test n°${msg.testIndex + 1} mises à jour`
              : msg.context === 'sql_update'
                ? (() => {
                    const n = (msg.contents.tables as any[]).length;
                    return `✅ Requête mise à jour · ${n} test${n > 1 ? 's' : ''} régénéré${n > 1 ? 's' : ''}`;
                  })()
                : (() => {
                    const n = (msg.contents.tables as any[]).length;
                    return `✅ ${n} test${n > 1 ? 's' : ''} généré${n > 1 ? 's' : ''} avec succès`;
                  })()
            }
          </Typography>
        </Box>
      )}

      {/* Résultats réels (pagination) */}
      {Array.isArray(msg.contents.real_res) ? (
        <Box sx={{ mt: 1, display: 'flex', gap: 2, overflowX: 'auto', flexWrap: 'wrap' }}>
          <DisplayTable
            jsonData={msg.contents.real_res}
            meta={msg.contents.meta}
            msgId={msg.id}
            onPageChange={onPageChange}
            tableName=""
            project={currentProjectId}
          />
        </Box>
      ) : null}

      {/* Résultats d'exécution des tests unitaires */}
      {Array.isArray(msg.contents.res) && msg.contents.res.length > 0 &&
        (msg.contents.res as any[]).some((r) => 'results_json' in r) && (
          <Box sx={{ mt: 1 }}>
            {(msg.contents.res as any[]).map((testResult, i) => {
              const status = testResult.status as string | undefined;
              const isComplete = status === 'complete';
              const isEmpty = status === 'empty_results';
              const expectsEmpty = isEmpty && /retourne\s+.{0,40}vide|résultat[s]?\s+(?:est\s+)?vide[s]?|0\s+ligne|aucune\s+ligne/.test(
                (testResult.unit_test_description ?? '').toLowerCase()
              );
              const isSuccess = isComplete || expectsEmpty;

              let rowCount = 0;
              if (isComplete) {
                try { rowCount = JSON.parse(testResult.results_json ?? '[]').length; } catch { /* keep 0 */ }
              }

              const emoji = isSuccess ? '✅' : isEmpty ? '⚠️' : '❌';
              const chipLabel = isComplete
                ? `${rowCount} ligne${rowCount > 1 ? 's' : ''}`
                : isEmpty ? 'Vide' : 'Erreur';
              const chipBg = isSuccess ? '#e8f7f6' : isEmpty ? '#fff8e1' : '#fde8e8';
              const chipColor = isSuccess ? '#1ca8a4' : isEmpty ? '#f57c00' : '#d32f2f';
              const chipBorder = isSuccess ? '#b2e4e2' : isEmpty ? '#ffe082' : '#f5c6c6';

              return (
                <Box
                  key={testResult.test_index ?? i}
                  sx={{ display: 'flex', alignItems: 'flex-start', gap: 1, mb: 0.75 }}
                >
                  <Typography sx={{ fontSize: 13, lineHeight: 1.4, flexShrink: 0 }}>
                    {emoji}
                  </Typography>
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography variant="caption" sx={{ fontWeight: 700, color: '#333' }}>
                      Test {(testResult.test_index ?? i) + 1}
                    </Typography>
                    {testResult.unit_test_description && (
                      <Typography
                        variant="caption"
                        sx={{ display: 'block', color: '#666', fontSize: 11, whiteSpace: 'normal' }}
                      >
                        {testResult.unit_test_description}
                      </Typography>
                    )}
                  </Box>
                  <Chip
                    label={chipLabel}
                    size="small"
                    sx={{
                      fontSize: 10,
                      height: 18,
                      flexShrink: 0,
                      bgcolor: chipBg,
                      color: chipColor,
                      border: `1px solid ${chipBorder}`,
                      fontWeight: 700,
                    }}
                  />
                </Box>
              );
            })}
          </Box>
      )}
      {/* Erreur */}
      {msg.contents.error && (
        <Alert severity="error" sx={{ mt: 2 }}>
          {msg.contents.error}
        </Alert>
      )}
    </>
  );
};

export default MessageBody;
