import React, { useRef, useEffect, useState } from 'react';
import { Alert, Box, Collapse, IconButton, LinearProgress, Tooltip, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import ChatBubbleOutlineIcon from '@mui/icons-material/ChatBubbleOutline';
import HistoryIcon from '@mui/icons-material/History';
import DroppableTextField from '../../../shared/DroppableTextField';

import MessageDisplay from './MessageDisplay';
import { Message, SqlHistoryEntry } from '../../../utils/types';

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

function estimateTokens(messages: Message[]): number {
  let chars = 0;
  for (const msg of messages) {
    chars += (msg.contents.text || '').length;
    chars += (msg.contents.sql || '').length;
    chars += (msg.contents.optimizedSql || '').length;
    chars += (msg.contents.error || '').length;
  }
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
  renderMessages: Message[];
  userInput: string;
  setUserInput: React.Dispatch<React.SetStateAction<string>>;
  onSend: () => void;
  isSending: boolean;
  loading: boolean | null;
  loading_message?: string | null;
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
  error,
  alwaysFix,
  onAlwaysFixChange,
  sqlHistory,
  onSqlRestore,
  onRestoreState,
  restoredMessageId,
  streamingReasoning,
  lastReasoning,
  onStopStream,
  sendMessage,
  sqlQuery,
  onClearHistory,
  onRequestProfile,
}) => {
  const showHistoryBanner = renderMessages.length > HISTORY_RESET_THRESHOLD;
  const estimatedTokens = showHistoryBanner ? estimateTokens(renderMessages) : 0;
  const scrollRef = useRef<HTMLDivElement>(null);
  const [reasoningOpen, setReasoningOpen] = useState(false);

  useEffect(() => {
    if (lastReasoning) setReasoningOpen(false);
  }, [lastReasoning]);

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
      </Box>

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
        />

        {isLoading && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, py: 1 }}>
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
            {loading_message && (
              <Typography variant="caption" sx={{ color: '#6b8287' }}>
                {loading_message}
              </Typography>
            )}
          </Box>
        )}

        {!isLoading && lastReasoning && (
          <Box sx={{ mt: 0.5, mb: 0.5 }}>
            <Box
              onClick={() => setReasoningOpen(o => !o)}
              sx={{
                display: 'inline-flex',
                alignItems: 'center',
                gap: 0.5,
                cursor: 'pointer',
                color: '#6b8287',
                '&:hover': { color: '#3b5357' },
              }}
            >
              <ExpandMoreIcon
                sx={{
                  fontSize: 14,
                  transition: 'transform 0.2s',
                  transform: reasoningOpen ? 'rotate(0deg)' : 'rotate(-90deg)',
                }}
              />
              <Typography variant="caption" sx={{ fontSize: 11, fontWeight: 500 }}>
                Réflexion
              </Typography>
            </Box>
            <Collapse in={reasoningOpen}>
              <Typography
                variant="caption"
                sx={{
                  display: 'block',
                  mt: 0.5,
                  px: 1.25,
                  py: 0.75,
                  bgcolor: '#eef3f4',
                  borderRadius: '8px',
                  fontSize: 11,
                  color: '#6b8287',
                  fontStyle: 'italic',
                  whiteSpace: 'pre-wrap',
                  lineHeight: 1.6,
                  maxHeight: 260,
                  overflow: 'auto',
                }}
              >
                {lastReasoning}
              </Typography>
            </Collapse>
          </Box>
        )}

        {error && (
          <Alert severity="error" sx={{ borderRadius: '12px', my: 1, fontSize: 12 }}>
            {error}
          </Alert>
        )}
      </Box>

      {/* Input area */}
      <Box
        sx={{
          flexShrink: 0,
          px: 1.75,
          pt: 1,
          pb: 1.75,
          borderTop: '1px solid #e4eaec',
          bgcolor: '#f3f6f7',
        }}
      >
        <DroppableTextField
          userInput={userInput}
          setUserInput={setUserInput}
          sendMessage={onSend}
          stopStream={onStopStream}
          disabled={isSending}
          placeholder={
            isSending
              ? 'En cours…'
              : selectedTestIndex !== null
              ? 'Comment améliorer ce test ?'
              : 'Décris un cas à couvrir, ou demande un ajustement…'
          }
        />
        <Typography
          variant="caption"
          sx={{ color: '#9aabb0', mt: 0.75, display: 'block', textAlign: 'center', fontSize: 10.5 }}
        >
          Entrée pour envoyer · Maj+Entrée pour saut de ligne
        </Typography>
      </Box>
    </Box>
  );
};

export default ChatColumn;
