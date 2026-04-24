// src/features/buildModel/components/MessageDisplay.tsx
import CodeIcon from '@mui/icons-material/Code';
import EditIcon from '@mui/icons-material/Edit';
import HistoryIcon from '@mui/icons-material/History';
import {
  Alert,
  Avatar,
  Box,
  Card,
  CardContent,
  Checkbox,
  Chip,
  Divider,
  FormControlLabel,
  Grid,
  TextField,
  Tooltip,
  Typography
} from '@mui/material';
import { MutedIconButton, TealIconButton } from '../../../style/AppButtons';
import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { fetchPage } from '../../../api/query';
import { fetchUniqueColumns } from '../../../api/table';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { StyledButton } from '../../../style/StyledComponents';
import { AnyRenderable, Message, MessageGroup, SqlHistoryEntry } from '../../../utils/types';
import { setSelectedChildIndex } from '../buildModelSlice';
import MessageBody from './MessageBody';
import MessageGroupComponent from './MessageGroupComponent';

interface MessageDisplayProps {
  sendMessage: (
    userInput: string,
    messageId: string | undefined,
    parentMessageId: string | undefined,
    userTables?: Record<string, Record<string, any>[]> | undefined,
    profileResult?: string
  ) => void;
  renderMessages: AnyRenderable[];
  onRestoreState?: (sql?: string, optimizedSql?: string, messageId?: string, testResults?: any[]) => void;
  restoredMessageId?: string;
  alwaysFix: boolean;
  onAlwaysFixChange: (value: boolean) => void;
  sqlHistory?: SqlHistoryEntry[];
  onSqlRestore?: (entry: SqlHistoryEntry) => void;
}

const SqlChangeDivider: React.FC<{ entry: SqlHistoryEntry; onRestore?: (e: SqlHistoryEntry) => void }> = ({ entry, onRestore }) => {
  const preview = entry.sql.split('\n').find(l => l.trim())?.slice(0, 50) ?? '';
  return (
    <Box sx={{ display: 'flex', alignItems: 'center', my: 1, gap: 1 }}>
      <Divider sx={{ flex: 1, borderColor: '#d0eeec' }} />
      <Tooltip title={`SQL: ${preview}… — Cliquer pour restaurer`}>
        <Chip
          icon={<CodeIcon sx={{ fontSize: '14px !important', color: '#1ca8a4 !important' }} />}
          label="SQL mis à jour"
          size="small"
          onClick={() => onRestore?.(entry)}
          sx={{
            fontSize: 10,
            height: 22,
            bgcolor: '#e8f7f6',
            color: '#1ca8a4',
            border: '1px solid #b2e4e2',
            cursor: 'pointer',
            fontWeight: 600,
            '&:hover': { bgcolor: '#d0eeec' },
          }}
        />
      </Tooltip>
      <Divider sx={{ flex: 1, borderColor: '#d0eeec' }} />
    </Box>
  );
};

const MessageDisplay: React.FC<MessageDisplayProps> = ({ sendMessage, renderMessages, onRestoreState, restoredMessageId, alwaysFix, onAlwaysFixChange, sqlHistory, onSqlRestore }) => {
  const { t } = useTranslation();
  const dispatch = useAppDispatch();
  const { queryComponentGraph } = useAppSelector((state) => state.buildModel);
  const messageRefs = useRef<Record<string, HTMLDivElement | null>>({});

  useEffect(() => {
    if (!restoredMessageId) return;
    const el = messageRefs.current[restoredMessageId];
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'center' });
  }, [restoredMessageId]);
  const { currentModelId, currentModel, currentProject, currentProjectId } = useAppSelector(
    (state) => state.appBarModel
  );

  const [editMessageId, setEditMessageId] = useState<string | undefined>(undefined);
  const [editText, setEditText] = useState<string>('');

  const handleEditClick = (msgId: string, text: string) => {
    setEditMessageId(msgId);
    setEditText(text);
  };

  const handleUpdateMessage = async () => {
    setEditMessageId(undefined);
    if (currentModelId && editMessageId) {
      const currentMessage = queryComponentGraph[editMessageId];
      if (currentMessage) {
        const parentMessageId = currentMessage.parent;
        dispatch(setSelectedChildIndex({ parentId: parentMessageId, index: null }));
        await sendMessage(editText, editMessageId, parentMessageId);
      }
    }
  };

  const cancelUpdateMessage = () => {
    setEditMessageId(undefined);
  };

  const handleUpload = async (
    messageId: string,
    parent: string | undefined,
    type: string | undefined,
    uploadedData: Record<string, any[]>
  ) => {
    parent = type === 'user' ? parent : messageId;
    await sendMessage('', messageId, parent, uploadedData);
  };

  const handleProfileUpload = async (
    messageId: string,
    parent: string | undefined,
    jsonContent: string
  ) => {
    await sendMessage('', messageId, parent, undefined, jsonContent);
  };

  const handleCreateClick = async (id: string) => {
    if (currentModelId && currentProjectId) {
      const resultAction = await dispatch(fetchUniqueColumns({ modelId: currentModelId, currentProjectId, id }));
      if (!fetchUniqueColumns.fulfilled.match(resultAction)) {
        console.error('Failed to fetch unique columns:', resultAction.payload || resultAction.error);
      }
    } else {
      console.error('No selected model');
    }
  };

  const handlePageChange = useCallback(
    (page: number, project: string, sql: string, msgId: string, limit: number = 20) => {
      const dialect = currentProject?.dialect;
      if (!dialect) return;
      dispatch(fetchPage({ project, sql, msgId, dialect, page, limit }));
    },
    [currentProject?.dialect, dispatch]
  );

  /** --- Rendu d'un message simple --- */
  const renderSingleMessage = (msg: Message, index: number) => {
    // === Affichage dédié si une erreur est présente dans le contenu ===
    const hasError = !!(msg as any)?.contents?.error;

    if (hasError) {
      const onFix = () => {
        sendMessage('__fix_error__', undefined, (msg as any).parent ?? (msg as any).id);
      };

      const errorText =
        (msg as any)?.contents?.error ??
        t('common.unknownError');

      return (
        <Grid container alignItems="center" justifyContent="flex-start" key={(msg as any).id} sx={{ my: 2 }}>
          <Grid item xs={12}>
            <Alert
              severity="error"
              variant="outlined"
              sx={{
                borderRadius: '16px',
                boxShadow: 2,
                '& .MuiAlert-message': { width: '100%' }
              }}
            >
              <Box sx={{ whiteSpace: 'pre-wrap', wordBreak: 'break-word', mb: 1 }}>
                {typeof errorText === 'string' ? errorText : JSON.stringify(errorText)}
              </Box>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
                <StyledButton onClick={onFix} size="small">
                  <EditIcon style={{ marginRight: 6, fontSize: 16 }} />
                  {t('action.fix') || 'Corriger cette erreur'}
                </StyledButton>
                <FormControlLabel
                  control={
                    <Checkbox
                      checked={alwaysFix}
                      onChange={(e) => onAlwaysFixChange(e.target.checked)}
                      size="small"
                      sx={{ color: '#d32f2f', '&.Mui-checked': { color: '#d32f2f' } }}
                    />
                  }
                  label={
                    <Typography variant="caption" sx={{ color: '#d32f2f', fontSize: 11 }}>
                      Toujours corriger automatiquement
                    </Typography>
                  }
                />
              </Box>
            </Alert>
          </Grid>
        </Grid>
      );
    }

    // --- Rendu normal user/bot quand il n'y a pas d'erreur ---
    const isUser = msg.type === 'user';
    const hasSql = !!(msg as any)?.contents?.sql;
    const hasTables = Array.isArray((msg as any)?.contents?.tables) && (msg as any).contents.tables.length > 0;
    const canRestore = !isUser && (hasSql || hasTables) && !!onRestoreState;
    const isRestored = (msg as any).id === restoredMessageId;

    return (
      <Grid
        container
        justifyContent={isUser ? 'flex-end' : 'flex-start'}
        key={(msg as any).id}
        ref={(el) => { messageRefs.current[(msg as any).id] = el as HTMLDivElement | null; }}
        sx={{ my: 0.5 }}
      >
        <Grid item xs={12} md={isUser ? 9 : 12}>
          <Card
            variant="outlined"
            sx={{
              backgroundColor: isUser ? '#f0f0f0' : 'white',
              borderRadius: '12px',
              boxShadow: isRestored ? '0 0 0 2px #1ca8a4' : 1,
              maxWidth: '100%',
              overflow: 'visible',
              borderColor: isRestored ? '#1ca8a4' : undefined,
            }}
          >
            {/* Compact header */}
            <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', px: 1.5, pt: 0.75, pb: 0 }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
                <Avatar
                  src={isUser ? 'user-avatar.png' : '/static/logo192.png'}
                  sx={{ width: 22, height: 22 }}
                />
                <Typography variant="caption" sx={{ fontWeight: 700, color: '#555' }}>
                  {isUser ? 'Vous' : 'MockSQL'}
                </Typography>
                {isRestored && (
                  <Chip
                    label="État restauré"
                    size="small"
                    sx={{ fontSize: 9, height: 18, bgcolor: '#e8f7f6', color: '#1ca8a4', border: '1px solid #b2e4e2', fontWeight: 700 }}
                  />
                )}
              </Box>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 0 }}>
                {canRestore && (
                  <Tooltip title="Restaurer cet état (query / tests)">
                    <TealIconButton
                      size="small"
                      onClick={() => {
                        const isResultsMsg = (msg as any).contentType === 'results';
                        const testResults = isResultsMsg
                          ? (msg as any).contents.res
                          : (msg as any).children
                              ?.map((id: string) => queryComponentGraph[id])
                              .find((m: any) => m?.contentType === 'results')
                              ?.contents?.res;
                        onRestoreState?.(
                          hasSql ? (msg as any).contents.sql : undefined,
                          (msg as any).contents.optimizedSql,
                          (msg as any).id,
                          testResults,
                        );
                      }}
                    >
                      <HistoryIcon sx={{ fontSize: 16 }} />
                    </TealIconButton>
                  </Tooltip>
                )}
                {isUser && editMessageId !== (msg as any).id && (
                  <MutedIconButton
                    size="small"
                    onClick={() => handleEditClick((msg as any).id, (msg as any)?.contents?.text || '')}
                  >
                    <EditIcon sx={{ fontSize: 16 }} />
                  </MutedIconButton>
                )}
              </Box>
            </Box>

            <CardContent sx={{ px: 1.5, py: 0.75, '&:last-child': { pb: 0.75 } }}>
              {editMessageId === (msg as any).id ? (
                <>
                  <TextField
                    fullWidth
                    variant="outlined"
                    value={editText}
                    onChange={(e) => setEditText(e.target.value)}
                    multiline
                    rows={3}
                    size="small"
                    sx={{ mt: 1, borderRadius: '8px' }}
                  />
                  <Box sx={{ display: 'flex', justifyContent: 'flex-end', gap: 1, mt: 0.5 }}>
                    <StyledButton onClick={handleUpdateMessage}>{t('action.send')}</StyledButton>
                    <StyledButton onClick={cancelUpdateMessage}>{t('action.cancel')}</StyledButton>
                  </Box>
                </>
              ) : (
                <MessageBody
                  msg={msg}
                  currentModelId={currentModelId}
                  currentProjectId={currentProjectId}
                  currentModelName={currentModel?.name || 'data'}
                  onUpload={handleUpload}
                  onProfileUpload={handleProfileUpload}
                  onPageChange={handlePageChange}
                  onExecute={undefined}
                  onCreateClick={handleCreateClick}
                  onSuggestionClick={(text) => sendMessage(text, undefined, (msg as any).id)}
                />
              )}
            </CardContent>
          </Card>
        </Grid>
      </Grid>
    );
  };

  return (
    <Box sx={{ maxWidth: '100%', overflowX: 'hidden', p: 0, mx: 0 }}>
      {renderMessages.map((item, index) => {
        // Groupes (branches)
        if ('type' in item && (item as any).type === 'group') {
          const group = item as MessageGroup;
          // Check for SQL change after the group's parent message
          const sqlAfterGroup = sqlHistory?.find(e => e.parentMessageId === group.parentId && e.parentMessageId !== '');
          return (
            <React.Fragment key={`group-${group.parentId}`}>
              <MessageGroupComponent
                group={group}
                renderSingleMessage={renderSingleMessage}
              />
              {sqlAfterGroup && (
                <SqlChangeDivider entry={sqlAfterGroup} onRestore={onSqlRestore} />
              )}
            </React.Fragment>
          );
        }

        // Message simple
        const msg = item as Message;
        const sqlAfterMsg = sqlHistory?.find(e => e.parentMessageId === msg.id && e.parentMessageId !== '');
        return (
          <React.Fragment key={msg.id}>
            {renderSingleMessage(msg, index)}
            {sqlAfterMsg && (
              <SqlChangeDivider entry={sqlAfterMsg} onRestore={onSqlRestore} />
            )}
          </React.Fragment>
        );
      })}
    </Box>
  );
};

export default React.memo(MessageDisplay);
