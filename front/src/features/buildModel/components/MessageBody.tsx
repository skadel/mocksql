import React, { useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { Alert, Box, Chip, Collapse, Stack, Table, TableBody, TableCell, TableHead, TableRow, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import DownloadIcon from '@mui/icons-material/Download';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import DisplayTable from './DisplayTable';
import { StyledButton } from '../../../style/StyledComponents';
import type { DebugCountStep, DebugCountStepsResult, DebugRunCteResult, Message } from '../../../utils/types';

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
  onRequestProfile?: () => void;
  debugMessages?: Message[];
};

const DebugRunCteContent: React.FC<{ d: DebugRunCteResult }> = ({ d }) => {
  if (d.error) return <Alert severity="error" sx={{ mt: 1 }}>{d.error}</Alert>;
  const cols = d.rows.length > 0 ? Object.keys(d.rows[0]) : [];
  return (
    <Box sx={{ mt: 1 }}>
      <Typography sx={{ fontSize: 11, color: '#6b8287', textTransform: 'uppercase', letterSpacing: '0.04em', fontWeight: 600, mb: 0.75 }}>
        {d.cte_name}{d.column ? ` · ${d.column}` : ''} — {d.row_count} ligne{d.row_count !== 1 ? 's' : ''}
      </Typography>
      {d.lineage && (
        <Typography variant="caption" sx={{ display: 'block', color: '#888', mb: 0.75, fontStyle: 'italic' }}>
          Lineage : {d.lineage}
        </Typography>
      )}
      {cols.length > 0 ? (
        <Box sx={{ overflowX: 'auto', border: '1px solid #e0e0e0', borderRadius: '6px' }}>
          <Table size="small">
            <TableHead>
              <TableRow sx={{ bgcolor: '#f5f5f5' }}>
                {cols.map((c) => (
                  <TableCell key={c} sx={{ fontSize: 11, fontWeight: 700, color: '#555', py: 0.5, px: 1 }}>{c}</TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {d.rows.map((row, i) => (
                <TableRow key={i} sx={{ '&:last-child td': { border: 0 } }}>
                  {cols.map((c) => (
                    <TableCell key={c} sx={{ fontSize: 11, color: '#333', py: 0.5, px: 1 }}>
                      {row[c] === null || row[c] === undefined ? <em style={{ color: '#aaa' }}>null</em> : String(row[c])}
                    </TableCell>
                  ))}
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </Box>
      ) : (
        <Typography variant="caption" sx={{ color: '#999' }}>Aucune ligne retournée.</Typography>
      )}
    </Box>
  );
};

const DebugCountStepsContent: React.FC<{ d: DebugCountStepsResult }> = ({ d }) => {
  if (d.error) return <Alert severity="error" sx={{ mt: 1 }}>{d.error}</Alert>;
  const maxCount = Math.max(...d.steps.map((s) => s.count), 1);
  return (
    <Box sx={{ mt: 1 }}>
      <Typography sx={{ fontSize: 11, color: '#6b8287', textTransform: 'uppercase', letterSpacing: '0.04em', fontWeight: 600, mb: 0.75 }}>
        {d.cte_name} — analyse étape par étape
      </Typography>
      <Stack gap={0.5}>
        {d.steps.map((step: DebugCountStep, i: number) => {
          const isZero = step.count === 0;
          const pct = maxCount > 0 ? Math.round((step.count / maxCount) * 100) : 0;
          const barColor = isZero ? '#ef5350' : i === 0 ? '#1ca8a4' : '#42a5f5';
          return (
            <Box key={i} sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
              <Typography sx={{ fontSize: 11, color: '#555', width: 260, flexShrink: 0, lineHeight: 1.3 }}>
                {step.label}
              </Typography>
              <Box sx={{ flex: 1, height: 14, bgcolor: '#f0f0f0', borderRadius: 1, overflow: 'hidden' }}>
                <Box sx={{ width: `${pct}%`, height: '100%', bgcolor: barColor, borderRadius: 1, transition: 'width 0.3s' }} />
              </Box>
              <Typography sx={{ fontSize: 11, fontWeight: 700, color: isZero ? '#ef5350' : '#333', width: 32, textAlign: 'right', flexShrink: 0 }}>
                {step.count}
              </Typography>
            </Box>
          );
        })}
      </Stack>
    </Box>
  );
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
  onRequestProfile,
  debugMessages,
}) => {
  const fileInputRef = useRef<HTMLInputElement>(null);
  const [reasoningOpen, setReasoningOpen] = useState(false);
  const [debugOpen, setDebugOpen] = useState(false);

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

      {/* Notification de génération de test sur scénario */}
      {msg.contentType === 'generate_test_scenario' && msg.contents.text && (
        <Box>
          {msg.contents.reasoning && (
            <Box sx={{ mb: 0.75 }}>
              <Box
                onClick={() => setReasoningOpen(o => !o)}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: 0.5, cursor: 'pointer',
                  color: '#888', fontSize: 12,
                  '&:hover': { color: '#555' },
                }}
              >
                <ExpandMoreIcon sx={{ fontSize: 14, transform: reasoningOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
                Réflexion
              </Box>
              <Collapse in={reasoningOpen}>
                <Typography variant="caption" sx={{ display: 'block', mt: 0.5, color: '#888', fontStyle: 'italic', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>
                  {msg.contents.reasoning}
                </Typography>
              </Collapse>
            </Box>
          )}
          <Box
            sx={{
              mt: 0.5,
              p: 1.25,
              bgcolor: '#f0fafa',
              borderRadius: '8px',
              border: '1px solid #c8e6e4',
              borderLeft: '3px solid #1ca8a4',
            }}
          >
            <Typography sx={{ fontSize: 11, color: '#1ca8a4', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', mb: 0.5 }}>
              Génération de test pour le scénario suivant
            </Typography>
            <Typography variant="body2" sx={{ color: '#333', lineHeight: 1.5 }}>
              {msg.contents.text}
            </Typography>
          </Box>
        </Box>
      )}

      {/* Texte */}
      {msg.contents.text && msg.contentType !== 'evaluation' && msg.contentType !== 'generate_test_scenario' && (
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
      {/* Suggestions */}
      {msg.contentType === 'suggestions' && Array.isArray(msg.contents.suggestions) && msg.contents.suggestions.length > 0 && (
        <Box sx={{ mt: 0.5 }}>
          <Typography sx={{ fontSize: 11, color: '#6b8287', textTransform: 'uppercase', letterSpacing: '0.04em', fontWeight: 600, mb: 0.75 }}>
            Cas suggérés
          </Typography>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.75 }}>
            {(msg.contents.suggestions as string[]).map((s, i) => (
              <Box
                key={i}
                component="button"
                onClick={() => onSuggestionClick?.(s)}
                sx={{
                  display: 'flex',
                  alignItems: 'flex-start',
                  gap: 1,
                  textAlign: 'left',
                  p: '9px 11px',
                  borderRadius: '9px',
                  border: '1px solid #d2efec',
                  bgcolor: '#f8fffe',
                  cursor: 'pointer',
                  fontFamily: 'inherit',
                  width: '100%',
                  '&:hover': { bgcolor: '#ecf7f6', borderColor: '#2BB0A8' },
                }}
              >
                <Typography sx={{ color: '#2BB0A8', fontWeight: 700, fontSize: 14, flexShrink: 0, lineHeight: 1.4 }}>
                  +
                </Typography>
                <Typography sx={{ flex: 1, fontSize: 13, color: '#333', lineHeight: 1.4 }}>
                  {s}
                </Typography>
              </Box>
            ))}
          </Box>
          {msg.contents.profileAvailable === false && (
            <Box sx={{ mt: 1, display: 'flex', alignItems: 'center', gap: 1, p: '7px 10px', borderRadius: '8px', bgcolor: '#fdf6e3', border: '1px solid #f0d080' }}>
              <Typography sx={{ fontSize: 12, color: '#7a5f00', flex: 1 }}>
                Profil non disponible — suggestions génériques uniquement.
              </Typography>
              {onRequestProfile && (
                <Box
                  component="button"
                  onClick={onRequestProfile}
                  sx={{ fontSize: 12, color: '#2BB0A8', fontWeight: 600, background: 'none', border: 'none', cursor: 'pointer', p: 0, fontFamily: 'inherit', '&:hover': { textDecoration: 'underline' } }}
                >
                  Lancer le profil
                </Box>
              )}
            </Box>
          )}
          </Box>
      )}

      {/* Debug — standalone fallback (edge case: message rendered directly) */}
      {msg.contents.debugRunCte && <DebugRunCteContent d={msg.contents.debugRunCte} />}
      {msg.contents.debugCountSteps && <DebugCountStepsContent d={msg.contents.debugCountSteps} />}

      {/* Debug — résultats collapsés (rattachés au message parent) */}
      {debugMessages && debugMessages.length > 0 && (
        <Box sx={{ mt: 0.75 }}>
          <Box
            onClick={() => setDebugOpen(o => !o)}
            sx={{
              display: 'inline-flex', alignItems: 'center', gap: 0.5, cursor: 'pointer',
              color: '#888', fontSize: 12,
              '&:hover': { color: '#555' },
            }}
          >
            <ExpandMoreIcon sx={{ fontSize: 14, transform: debugOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
            Réflexion
          </Box>
          <Collapse in={debugOpen}>
            <Box sx={{ mt: 0.5 }}>
              {debugMessages.map(dm => (
                <Box key={dm.id}>
                  {dm.contents.debugRunCte && <DebugRunCteContent d={dm.contents.debugRunCte} />}
                  {dm.contents.debugCountSteps && <DebugCountStepsContent d={dm.contents.debugCountSteps} />}
                </Box>
              ))}
            </Box>
          </Collapse>
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
