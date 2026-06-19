import React, { useRef, useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Alert, Box, Button, Chip, Collapse, IconButton, LinearProgress, Stack, Tooltip, Typography } from '@mui/material';
import RefreshRoundedIcon from '@mui/icons-material/RefreshRounded';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import CloseIcon from '@mui/icons-material/Close';
import ChatBubbleOutlineIcon from '@mui/icons-material/ChatBubbleOutline';
import HistoryIcon from '@mui/icons-material/History';
import DeleteSweepOutlinedIcon from '@mui/icons-material/DeleteSweepOutlined';
import DroppableTextField from '../../../shared/DroppableTextField';

import MessageDisplay from './MessageDisplay';
import HistoryDrawer from './HistoryDrawer';
import { AnyRenderable, Message, MessageGroup, RequestGroup, SqlHistoryEntry } from '../../../utils/types';
import { isStaleSchemaError } from '../../../utils/staleSchema';

function DbIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
      <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3" />
    </svg>
  );
}

const HISTORY_RESET_THRESHOLD = 100;

function estimateTokens(messages: AnyRenderable[]): number {
  let chars = 0;
  const visit = (items: AnyRenderable[]) => {
    for (const item of items) {
      if ((item as any).type === 'request_group') {
        visit((item as RequestGroup).items);
      } else if ((item as any).type === 'group') {
        (item as MessageGroup).branches.forEach(visit);
      } else {
        const c = (item as Message).contents;
        if (!c) continue;
        chars += (c.text || '').length;
        chars += (c.sql || '').length;
        chars += (c.optimizedSql || '').length;
        chars += (c.error || '').length;
      }
    }
  };
  visit(messages);
  return Math.round(chars / 4);
}

function formatTokenCount(n: number): string {
  return n.toLocaleString('fr-FR');
}

interface ChatColumnProps {
  fileName: string;
  onChangeFile: () => void;
  selectedTestIndex: number | null;
  assertionOnly: boolean;
  onClearAnchor: () => void;
  renderMessages: AnyRenderable[];
  userInput: string;
  setUserInput: React.Dispatch<React.SetStateAction<string>>;
  onSend: () => void;
  isSending: boolean;
  loading: boolean | null;
  loading_message?: string | null;
  queuedCount?: number;
  understandingDraft?: Array<{ database?: string; table: string; columns: string[] }> | null;
  validationMs?: number | null;
  error: string | null;
  alwaysFix: boolean;
  onAlwaysFixChange: (v: boolean) => void;
  sqlHistory: SqlHistoryEntry[];
  onSqlRestore: (e: SqlHistoryEntry) => void;
  onRestoreState: (sql?: string, optimizedSql?: string, messageId?: string, restoredTestResults?: any[]) => void;
  restoredMessageId?: string;
  streamingReasoning?: string;
  lastReasoning?: string;
  onStopStream: () => void;
  sendMessage: (...args: any[]) => void;
  sqlQuery: string;
  onClearHistory: () => void;
  onRequestProfile?: () => void;
  onRefreshSchemas?: () => void;
  focusTrigger?: number;
}

const ChatColumn: React.FC<ChatColumnProps> = ({
  fileName,
  onChangeFile,
  selectedTestIndex,
  assertionOnly,
  onClearAnchor,
  renderMessages,
  userInput,
  setUserInput,
  onSend,
  isSending,
  loading,
  loading_message,
  queuedCount = 0,
  understandingDraft,
  validationMs,
  error,
  alwaysFix,
  onAlwaysFixChange,
  sqlHistory,
  onSqlRestore,
  onRestoreState,
  restoredMessageId,
  lastReasoning,
  onStopStream,
  sendMessage,
  sqlQuery,
  onClearHistory,
  onRequestProfile,
  onRefreshSchemas,
  focusTrigger,
}) => {
  const { t } = useTranslation();
  const showHistoryBanner = renderMessages.length > HISTORY_RESET_THRESHOLD;
  const estimatedTokens = showHistoryBanner ? estimateTokens(renderMessages) : 0;
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const [reasoningOpen, setReasoningOpen] = useState(false);
  const [historyDrawerOpen, setHistoryDrawerOpen] = useState(false);
  const [confirmClear, setConfirmClear] = useState(false);
  const confirmClearTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (lastReasoning) setReasoningOpen(false);
  }, [lastReasoning]);

  useEffect(() => {
    if (focusTrigger) inputRef.current?.focus();
  }, [focusTrigger]);

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [renderMessages, loading]);

  const isLoading = loading || isSending;

  return (
    <Box
      component="aside"
      sx={{
        width: 440,
        minWidth: 440,
        bgcolor: '#f3f6f7',
        display: 'flex',
        flexDirection: 'column',
        height: '100%',
        borderRight: '1px solid #e4eaec',
        overflow: 'hidden',
      }}
    >
      {/* Header — file selector */}
      <Box
        sx={{
          px: 1.75,
          py: 1.5,
          borderBottom: '1px solid #e4eaec',
          display: 'flex',
          alignItems: 'center',
          gap: 1.25,
          flexShrink: 0,
          bgcolor: '#f3f6f7',
        }}
      >
        <Box
          onClick={onChangeFile}
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: '7px',
            textDecoration: 'none',
            flexShrink: 0,
            cursor: 'pointer',
            '&:hover .msql-icon': { bgcolor: '#1ca8a4' },
            '&:hover .msql-label': { color: '#1ca8a4' },
          }}
        >
          <Box
            className="msql-icon"
            sx={{
              width: 26,
              height: 26,
              borderRadius: '7px',
              bgcolor: '#2BB0A8',
              color: '#fff',
              display: 'grid',
              placeItems: 'center',
              flexShrink: 0,
              transition: 'background-color 0.15s',
            }}
          >
            <DbIcon />
          </Box>
          <Typography
            className="msql-label"
            sx={{
              fontSize: 13.5,
              fontWeight: 700,
              color: '#0f272a',
              letterSpacing: '-0.2px',
              transition: 'color 0.15s',
            }}
          >
            MockSQL
          </Typography>
        </Box>
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography
            sx={{
              fontSize: 11,
              color: '#6b8287',
              textTransform: 'uppercase',
              letterSpacing: '0.04em',
              fontWeight: 600,
              lineHeight: 1.2,
            }}
          >
            Fichier testé
          </Typography>
          <Typography
            sx={{
              fontWeight: 600,
              color: '#0f272a',
              fontSize: 13.5,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              lineHeight: 1.3,
            }}
          >
            {fileName}
          </Typography>
        </Box>
        <Box
          component="button"
          onClick={onChangeFile}
          sx={{
            flexShrink: 0,
            padding: '6px 11px',
            fontSize: 12,
            borderRadius: '7px',
            border: '1px solid #e4eaec',
            bgcolor: '#fff',
            color: '#3b5357',
            cursor: 'pointer',
            fontWeight: 500,
            fontFamily: 'inherit',
            '&:hover': { bgcolor: '#ecf7f6', borderColor: '#2BB0A8', color: '#1ca8a4' },
          }}
        >
          Changer
        </Box>
        {sqlHistory.length > 0 && (
          <Box
            component="button"
            onClick={() => setHistoryDrawerOpen(true)}
            title={`Historique SQL (${sqlHistory.length} version${sqlHistory.length > 1 ? 's' : ''})`}
            sx={{
              flexShrink: 0, width: 30, height: 30, borderRadius: '8px', border: 'none',
              bgcolor: 'transparent', color: '#9aabb0', cursor: 'pointer',
              display: 'flex', alignItems: 'center', justifyContent: 'center',
              '&:hover': { bgcolor: 'rgba(19,35,41,.06)', color: '#3b5357' },
            }}
          >
            <HistoryIcon sx={{ fontSize: 16 }} />
          </Box>
        )}
        {renderMessages.length > 0 && !isLoading && (
          <Tooltip
            title={confirmClear ? 'Cliquer à nouveau pour vider' : 'Vider la conversation (les tests sont conservés)'}
            arrow
          >
            <Box
              component="button"
              onClick={() => {
                if (confirmClear) {
                  if (confirmClearTimer.current) clearTimeout(confirmClearTimer.current);
                  setConfirmClear(false);
                  onClearHistory();
                } else {
                  setConfirmClear(true);
                  confirmClearTimer.current = setTimeout(() => setConfirmClear(false), 3000);
                }
              }}
              sx={{
                flexShrink: 0, width: 30, height: 30, borderRadius: '8px', border: 'none',
                bgcolor: confirmClear ? 'rgba(220,38,38,.1)' : 'transparent',
                color: confirmClear ? '#dc2626' : '#9aabb0', cursor: 'pointer',
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                transition: 'color .15s, background-color .15s',
                '&:hover': {
                  bgcolor: confirmClear ? 'rgba(220,38,38,.16)' : 'rgba(19,35,41,.06)',
                  color: confirmClear ? '#dc2626' : '#3b5357',
                },
              }}
            >
              <DeleteSweepOutlinedIcon sx={{ fontSize: 16 }} />
            </Box>
          </Tooltip>
        )}
      </Box>

      <HistoryDrawer
        open={historyDrawerOpen}
        onClose={() => setHistoryDrawerOpen(false)}
        fileName={fileName}
        entries={sqlHistory}
        onRestore={onSqlRestore}
      />

      {/* Anchor banner — when a test is selected */}
      {selectedTestIndex !== null && (
        <Box
          sx={{
            px: 1.75,
            py: 1,
            bgcolor: '#ecf7f6',
            borderBottom: '1px solid #d2efec',
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            flexShrink: 0,
          }}
        >
          <ChatBubbleOutlineIcon sx={{ fontSize: 14, color: '#1ca8a4' }} />
          <Typography
            sx={{
              fontSize: 12,
              color: '#1f948d',
              fontWeight: 500,
              flex: 1,
              minWidth: 0,
              whiteSpace: 'nowrap',
              overflow: 'hidden',
              textOverflow: 'ellipsis',
            }}
          >
            {assertionOnly
              ? `Modifier l'assertion du test #${selectedTestIndex + 1}`
              : `Conversation sur le test #${selectedTestIndex + 1}`}
          </Typography>
          <IconButton
            size="small"
            onClick={onClearAnchor}
            sx={{ color: '#1f948d', p: 0.25 }}
          >
            <CloseIcon sx={{ fontSize: 14 }} />
          </IconButton>
        </Box>
      )}

      {/* History reset banner */}
      {showHistoryBanner && (
        <Box
          sx={{
            px: 1.75,
            py: 0.75,
            bgcolor: '#fffbeb',
            borderBottom: '1px solid #fde68a',
            display: 'flex',
            alignItems: 'center',
            gap: 1,
            flexShrink: 0,
          }}
        >
          <HistoryIcon sx={{ fontSize: 14, color: '#b45309', flexShrink: 0 }} />
          <Typography sx={{ fontSize: 11.5, color: '#92400e', flex: 1, minWidth: 0 }}>
            {renderMessages.length} messages · ~{formatTokenCount(estimatedTokens)} tokens
          </Typography>
          <Tooltip title={`Vider l'historique du chat pour économiser ~${formatTokenCount(estimatedTokens)} tokens sur les prochains appels LLM`} arrow>
            <Box
              component="button"
              onClick={onClearHistory}
              sx={{
                flexShrink: 0,
                padding: '3px 9px',
                fontSize: 11,
                borderRadius: '6px',
                border: '1px solid #fcd34d',
                bgcolor: '#fef3c7',
                color: '#92400e',
                cursor: 'pointer',
                fontWeight: 600,
                fontFamily: 'inherit',
                whiteSpace: 'nowrap',
                '&:hover': { bgcolor: '#fde68a', borderColor: '#f59e0b' },
              }}
            >
              Réinitialiser
            </Box>
          </Tooltip>
        </Box>
      )}

      {/* Loading bar */}
      {isLoading && (
        <LinearProgress
          variant="indeterminate"
          sx={{
            height: 3,
            bgcolor: '#d2efec',
            '& .MuiLinearProgress-bar': { bgcolor: '#2BB0A8' },
            flexShrink: 0,
          }}
        />
      )}

      {/* Messages area */}
      <Box
        ref={scrollRef}
        sx={{
          flex: 1,
          overflow: 'auto',
          px: 1.75,
          pt: 1.5,
          minHeight: 0,
        }}
      >
        <MessageDisplay
          sendMessage={(input, msgId, parentMsgId, userTables, profileResult) =>
            sendMessage(input, sqlQuery, msgId, parentMsgId, userTables, false, undefined, profileResult)
          }
          renderMessages={renderMessages}
          onRestoreState={onRestoreState}
          restoredMessageId={restoredMessageId}
          alwaysFix={alwaysFix}
          onAlwaysFixChange={onAlwaysFixChange}
          sqlHistory={sqlHistory}
          onSqlRestore={onSqlRestore}
          onRequestProfile={onRequestProfile}
          onRefreshSchemas={onRefreshSchemas}
        />

        {isLoading && (() => {
          const dots = (
            <Box sx={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
              {[0, 1, 2].map((i) => (
                <Box
                  key={i}
                  sx={{
                    width: 6,
                    height: 6,
                    borderRadius: '50%',
                    bgcolor: '#2BB0A8',
                    animation: 'msql-bounce 1.2s ease-in-out infinite',
                    animationDelay: `${i * 0.2}s`,
                    '@keyframes msql-bounce': {
                      '0%, 80%, 100%': { transform: 'translateY(0)', opacity: 0.4 },
                      '40%': { transform: 'translateY(-5px)', opacity: 1 },
                    },
                  }}
                />
              ))}
            </Box>
          );

          // Checklist vivante : montre les tables/colonnes déjà extraites par `validate`,
          // puis l'étape backend en cours. Fallback aux dots si rien n'est encore extrait.
          if (understandingDraft && understandingDraft.length > 0) {
            const totalColumns = understandingDraft.reduce((acc, tb) => acc + (tb.columns?.length ?? 0), 0);
            const seconds = validationMs != null ? Math.max(0.1, validationMs / 1000).toFixed(1) : null;
            return (
              <Box sx={{ py: 1 }}>
                <Stack gap={0.75}>
                  {seconds && (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                      <CheckCircleOutlineIcon sx={{ fontSize: 16, color: '#2BB0A8' }} />
                      <Typography variant="caption" sx={{ color: '#37474f', fontWeight: 600 }}>
                        {t('loading.query_validated_in', { seconds })}
                      </Typography>
                    </Box>
                  )}
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                    <CheckCircleOutlineIcon sx={{ fontSize: 16, color: '#2BB0A8' }} />
                    <Typography variant="caption" sx={{ color: '#37474f', fontWeight: 600 }}>
                      {understandingDraft.length} {t('loading.step_tables')} · {totalColumns} {t('loading.step_columns')}
                    </Typography>
                    <Stack direction="row" flexWrap="wrap" gap={0.5} sx={{ ml: 0.5 }}>
                      {understandingDraft.map((tb) => (
                        <Chip
                          key={`${tb.database ?? ''}.${tb.table}`}
                          label={tb.table}
                          size="small"
                          sx={{ bgcolor: '#e8f5f5', color: '#1ca8a4', fontFamily: 'monospace', fontSize: 10, height: 18, border: '1px solid #b2e0de' }}
                        />
                      ))}
                    </Stack>
                  </Box>
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                    {dots}
                    <Typography variant="caption" sx={{ color: '#6b8287' }}>
                      {loading_message || t('loading.step_preparing')}
                    </Typography>
                  </Box>
                </Stack>
              </Box>
            );
          }

          return (
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, py: 1 }}>
              {dots}
              {loading_message && (
                <Typography variant="caption" sx={{ color: '#6b8287' }}>
                  {loading_message}
                </Typography>
              )}
            </Box>
          );
        })()}

        {!isLoading && lastReasoning && (
          <Box sx={{ mt: 0.75, mb: 0.5 }}>
            {/* Claude-Code-style collapsible "Réflexion" block */}
            <Box
              onClick={() => setReasoningOpen(o => !o)}
              sx={{
                display: 'flex', alignItems: 'center', gap: '6px',
                cursor: 'pointer', width: '100%', textAlign: 'left',
                p: '4px 2px', borderRadius: '6px', color: '#6b8287',
                '&:hover': { color: '#3b5357' },
              }}
            >
              {/* Sparkles icon */}
              <Box component="span" sx={{ color: '#1ca8a4', display: 'inline-flex', flexShrink: 0 }}>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M9.937 15.5A2 2 0 0 0 8.5 14.063l-6.135-1.582a.5.5 0 0 1 0-.962L8.5 9.936A2 2 0 0 0 9.937 8.5l1.582-6.135a.5.5 0 0 1 .962 0L14.063 8.5A2 2 0 0 0 15.5 9.937l6.135 1.581a.5.5 0 0 1 0 .964L15.5 14.063a2 2 0 0 0-1.437 1.437l-1.582 6.135a.5.5 0 0 1-.962 0z"/>
                </svg>
              </Box>
              <Typography component="span" sx={{ fontSize: 12, fontWeight: 600, color: 'inherit' }}>Réflexion</Typography>
              <Typography component="span" sx={{ fontSize: 11.5, color: '#9aabb0' }}>· terminée</Typography>
              <Box component="span" sx={{ ml: 'auto', display: 'inline-flex', color: '#9aabb0', transition: 'transform .2s', transform: reasoningOpen ? 'rotate(180deg)' : 'none' }}>
                <svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="m6 9 6 6 6-6"/></svg>
              </Box>
            </Box>
            <Collapse in={reasoningOpen}>
              <Box sx={{
                mt: '4px', ml: '2px', pl: '8px',
                borderLeft: '2px solid #dce4e6',
                display: 'flex', flexDirection: 'column', gap: '6px', pb: '2px',
              }}>
                {lastReasoning.split('\n').filter(l => l.trim()).map((line, i) => (
                  <Box key={i} sx={{ display: 'flex', gap: '7px', alignItems: 'flex-start', pl: '6px' }}>
                    <Box component="span" sx={{ color: '#1ca8a4', flexShrink: 0, mt: '2px', display: 'inline-flex' }}>
                      <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><path d="m9 18 6-6-6-6"/></svg>
                    </Box>
                    <Typography sx={{ fontSize: 11.5, color: '#6b8287', lineHeight: 1.5 }}>{line}</Typography>
                  </Box>
                ))}
              </Box>
            </Collapse>
          </Box>
        )}

        {error && (
          <Alert
            severity="error"
            sx={{ borderRadius: '12px', my: 1, fontSize: 12 }}
            action={
              onRefreshSchemas && isStaleSchemaError(error) ? (
                <Button
                  size="small"
                  color="error"
                  variant="outlined"
                  startIcon={<RefreshRoundedIcon sx={{ fontSize: 16 }} />}
                  onClick={onRefreshSchemas}
                  sx={{ whiteSpace: 'nowrap', textTransform: 'none' }}
                >
                  Rafraîchir le schéma
                </Button>
              ) : undefined
            }
          >
            {error}
          </Alert>
        )}
      </Box>

      {/* Input area */}
      <Box
        data-testid="demo-zoom-chat"
        sx={{
          flexShrink: 0,
          px: 1.75,
          pt: 1,
          pb: 1.75,
          borderTop: '1px solid #e4eaec',
          bgcolor: '#f3f6f7',
        }}
      >
        {queuedCount > 0 && (
          <Box
            sx={{
              display: 'inline-flex', alignItems: 'center', gap: 0.5,
              alignSelf: 'flex-start', mb: 0.75, px: 1, py: 0.25,
              borderRadius: '12px', bgcolor: 'rgba(28,168,164,0.12)',
              color: '#1ca8a4', fontSize: 11, fontWeight: 600,
            }}
          >
            {queuedCount} instruction{queuedCount > 1 ? 's' : ''} en file · prise{queuedCount > 1 ? 's' : ''} en compte après la génération
          </Box>
        )}
        <DroppableTextField
          userInput={userInput}
          setUserInput={setUserInput}
          sendMessage={onSend}
          stopStream={onStopStream}
          inputRef={inputRef}
          placeholder={
            isLoading
              ? 'Ajoute une instruction, ou pose une question sur le résultat…'
              : selectedTestIndex !== null
              ? 'Comment améliorer ce test ?'
              : 'Décris un cas à couvrir, ou demande un ajustement…'
          }
        />
        <Typography
          variant="caption"
          sx={{ color: '#9aabb0', mt: 0.75, display: 'block', textAlign: 'center', fontSize: 10.5 }}
        >
          {isLoading
            ? 'Une instruction infléchit la génération ; une question y répond sans la toucher'
            : 'Entrée pour envoyer · Maj+Entrée pour saut de ligne'}
        </Typography>
      </Box>
    </Box>
  );
};

export default ChatColumn;
