import React, { useCallback, useMemo, useRef, useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { v4 as uuidv4 } from 'uuid';
import { throttle } from 'lodash';
import { Alert, Box, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, Grid, IconButton, LinearProgress, Slide, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import DroppableTextField from '../../../shared/DroppableTextField';
import { Container } from '../../../style/StyledComponents';
import { getLastMessage } from '../../../utils/messages';
import MessageDisplay from './MessageDisplay';
import MissingTablesAlert from './MissingTablesAlert';
import SQLQueryBar from './SQLQueryBar';
import TestsPanel from './TestsPanel';
import DuckDBFooter from './DuckDBFooter';
import { drawerWidth } from '../../appBar/components/DrawerComponent';
import { createModel } from '../../../api/models';
import { chatQuery, stopStream, validateQueryApi, checkProfileApi, skipProfilingApi, importMissingTablesApi, autoProfileApi } from '../../../api/query';
import { getOrCreateUserId, getUserPreferences } from '../../../api/preferences';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { setCurrentId, setCurrentProjectId, setOpenProjectDialog, updateModelName } from '../../appBar/appBarSlice';
import { setError, setQueryComponentGraph, setQuery, setOptimizedQuery, setTestResults, pushSqlHistory, setRestoredMessageId as setRestoredMessageIdAction } from '../buildModelSlice';
import { getMessages, patchModelSql } from '../../../api/messages';
import { getRenderMessages } from '../../../selectors/getRenderMessages';
import { ProfileRequest, SqlHistoryEntry } from '../../../utils/types';

const ChatComponent: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams();

  const [userInput, setUserInput] = useState('');
  const [sqlQuery, setSqlQuery] = useState('');
  const [modelName, setModelName] = useState('');
  const [optimizedSql, setOptimizedSql] = useState('');
  const [restoredMessageId, setRestoredMessageId] = useState<string | undefined>(undefined);
  const [isSending, setIsSending] = useState(false);
  const [chatOverlayOpen, setChatOverlayOpen] = useState(false);
  const [selectedTestIndex, setSelectedTestIndex] = useState<number | null>(null);
  const [pendingFirstLoad, setPendingFirstLoad] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [lastErrorDismissed, setLastErrorDismissed] = useState(false);
  const [missingTables, setMissingTables] = useState<string[] | null>(null);
  const [tablesToImport, setTablesToImport] = useState<string[] | null>(null);
  const [isImporting, setIsImporting] = useState(false);
  const [pendingAutoProfile, setPendingAutoProfile] = useState<{
    profileRequest: ProfileRequest;
    onConfirm: () => Promise<void>;
    onSkip: () => Promise<void>;
  } | null>(null);
  const [isAutoProfileRunning, setIsAutoProfileRunning] = useState(false);
  const [validationStatus, setValidationStatus] = useState<'idle' | 'validating' | 'valid' | 'error'>('idle');
  const [submissionStep, setSubmissionStep] = useState<string | null>(null);
  const [alwaysFix, setAlwaysFix] = useState(() => localStorage.getItem('alwaysFix') === 'true');

  // -------- Sync des préférences d'import depuis le backend au montage
  useEffect(() => {
    const userId = getOrCreateUserId();
    getUserPreferences(userId).then((prefs) => {
      if (prefs.auto_import_always) {
        localStorage.setItem('autoImport_always', 'true');
      }
    });
  }, []);
  const [historyRestoreTrigger, setHistoryRestoreTrigger] = useState(0);
  const skipValidationRef = useRef(false);
  const autoFixedIds = useRef<Set<string>>(new Set());
  const awaitingGetMessagesRef = useRef(false);
  const pendingSessionRef = useRef<string | null>(null);
  const prevLoadingRef = useRef<boolean | null>(null);
  const wasHistoryLoadRef = useRef(false);

  const {
    queryComponentGraph: messages,
    loading,
    loading_message,
    error,
    selectedChildIndices,
    query: storedQuery,
    optimizedQuery: storedOptimizedQuery,
    sqlHistory,
    restoredMessageId: storedRestoredMessageId,
    lastError,
  } = useAppSelector((state) => state.buildModel);

  const messagesRef = useRef(messages);
  const {
    currentModelId,
    currentProjectId,
    currentProject,
    projects,
    drawerOpen,
  } = useAppSelector((state) => state.appBarModel);

  messagesRef.current = messages;

  const renderMessages = useAppSelector(getRenderMessages);
  const lastMsgHasError = useMemo(() => {
    if (!renderMessages.length) return false;
    const last = renderMessages[renderMessages.length - 1];
    return !!(last && 'contents' in last && (last as any).contents?.error);
  }, [renderMessages]);

  // ui phase: 'entry' | 'loading' | 'workspace'
  const uiPhase = !pendingFirstLoad && !currentModelId
    ? 'entry' : 'workspace';

  const containerRef = useRef<HTMLDivElement>(null);

  // -------- Auto-scroll
  const [shouldAutoScroll, setShouldAutoScroll] = useState(true);
  const handleScroll = useCallback(() => {
    if (!containerRef.current) return;
    const { scrollTop, scrollHeight, clientHeight } = containerRef.current;
    setShouldAutoScroll(scrollHeight - scrollTop - clientHeight <= 100);
  }, []);
  const throttledScrollHandler = useMemo(() => throttle(handleScroll, 200), [handleScroll]);

  useEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    container.addEventListener('scroll', throttledScrollHandler);
    return () => {
      container.removeEventListener('scroll', throttledScrollHandler);
      throttledScrollHandler.cancel();
    };
  }, [throttledScrollHandler]);

  useEffect(() => {
    if (containerRef.current && shouldAutoScroll) {
      containerRef.current.scrollTop = containerRef.current.scrollHeight;
    }
  }, [messages, loading, loading_message, shouldAutoScroll]);

  // -------- Routing / sélection projet & modèle
  useEffect(() => {
    if (currentProjectId && params.projectID && currentProjectId !== params.projectID) {
      const targetPath = `/models/${params.projectID || currentProjectId}`;
      if (location.pathname !== targetPath) {
        navigate(targetPath);
        dispatch(setCurrentProjectId(params.projectID));
      }
    } else if (params.projectID && !currentProjectId) {
      dispatch(setCurrentProjectId(params.projectID));
    }
  }, [location.pathname, navigate, params.projectID, currentProjectId, dispatch]);

  useEffect(() => {
    if (params.projectID) {
      dispatch(setCurrentProjectId(params.projectID));
    } else if (projects.length > 0) {
      const currentProjectExists = projects.some((p) => p.project_id === currentProjectId);
      if (!currentProjectExists) {
        dispatch(setCurrentProjectId(projects[0].project_id));
      }
    }
  }, [projects, currentProjectId, dispatch, params.projectID]);

  useEffect(() => {
    if (!currentProjectId) {
      dispatch(setQueryComponentGraph({}));
      dispatch(setOpenProjectDialog(true));
      return;
    }
    dispatch(setOpenProjectDialog(false));
    dispatch(setError(null));
  }, [currentProjectId, dispatch, t]);

  useEffect(() => {
    const modelID = params.modelID as string;
    const projectID = params.projectID as string;
    if (!projectID && !currentProjectId) return;
    if (!modelID && !currentModelId) return;
    else if (!modelID && currentModelId) {
      navigate(`/models/${projectID || currentProjectId}/${currentModelId}`);
    } else if (modelID && !currentModelId) {
      dispatch(setCurrentId(modelID));
      awaitingGetMessagesRef.current = true;
      dispatch(getMessages({ modelId: modelID, t }));
    } else if (modelID && currentModelId && modelID !== currentModelId) {
      dispatch(setCurrentId(modelID));
      awaitingGetMessagesRef.current = true;
      dispatch(getMessages({ modelId: modelID, t }));
    }
  }, [params.modelID, params.projectID, currentModelId, currentProjectId, dispatch, navigate, t]);

  const canType = !!currentProjectId && !!currentProject;

  // -------- Reset local SQL when switching models or clearing to a new model
  const prevModelIdRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    const prev = prevModelIdRef.current;
    prevModelIdRef.current = currentModelId ?? undefined;
    // Clear when navigating from one saved model to another, or back to new-model state
    if (prev && prev !== (currentModelId ?? undefined)) {
      setSqlQuery('');
      setOptimizedSql('');
      setModelName('');
      setPendingFirstLoad(false);
      setValidationStatus('idle');
      setSubmissionStep(null);
      setSubmitError(null);
      setMissingTables(null);
      setPendingAutoProfile(null);
      setIsAutoProfileRunning(false);
      setLastErrorDismissed(false);
      awaitingGetMessagesRef.current = false;
      autoFixedIds.current.clear();
    }
  }, [currentModelId]);

  // -------- Clear pendingFirstLoad once backend is done (no profiling needed)
  useEffect(() => {
    if (pendingFirstLoad && loading === false) {
      // Detect compilation/streaming errors during initial submission.
      // Read messages via ref to avoid re-triggering this effect when
      // dispatch(setQueryComponentGraph({})) mutates the messages state.
      const errorFromMsg = Object.values(messagesRef.current).find(m => m.contents?.error)?.contents?.error;
      const errorFromStream = error || null;
      const detectedError = errorFromMsg || errorFromStream;
      if (detectedError) {
        setSubmitError(detectedError);
        dispatch(setQueryComponentGraph({}));
      }
      setPendingFirstLoad(false);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loading, pendingFirstLoad, error, dispatch]);

  // -------- Sync stored SQL + optimized SQL → local state once loaded
  useEffect(() => {
    if (storedQuery) setSqlQuery(storedQuery);
    if (storedOptimizedQuery) setOptimizedSql(storedOptimizedQuery);
  }, [storedQuery, storedOptimizedQuery]);

  // -------- Sync stored restored message id → local state once loaded
  useEffect(() => {
    if (storedRestoredMessageId) setRestoredMessageId(storedRestoredMessageId);
  }, [storedRestoredMessageId]);

  // -------- Post-load: réinitialise le flag après chargement des messages
  useEffect(() => {
    if (!awaitingGetMessagesRef.current) return;
    if (loading !== false) return;
    awaitingGetMessagesRef.current = false;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loading]);

  // -------- Reset validation state when user edits SQL
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { setValidationStatus('idle'); setSubmitError(null); setMissingTables(null); setTablesToImport(null); }, [sqlQuery]);

  // -------- Brouillon localStorage (follow-up messages only)
  const draftKeyRef = useRef<string>('');
  useEffect(() => {
    draftKeyRef.current = `draft:${currentProjectId || 'no-project'}:${currentModelId || 'new'}`;
    const saved = localStorage.getItem(draftKeyRef.current);
    if (saved && !userInput) setUserInput(saved);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentProjectId, currentModelId]);

  useEffect(() => {
    if (draftKeyRef.current) localStorage.setItem(draftKeyRef.current, userInput);
  }, [userInput]);

  // -------- Core send function
  const sendMessage = useCallback(
    async (
      input: string,
      currentSqlQuery: string,
      messageId: string | undefined = '',
      parentMessageId: string | undefined = '',
      userTables?: Record<string, Record<string, any>[]>,
      create: boolean = false,
      testIndex?: number,
      profileResult?: string
    ): Promise<boolean> => {
      const text = (input ?? '').trim();

      if (!currentProject) { dispatch(setOpenProjectDialog(true)); return false; }
      if (!currentProjectId || currentProjectId === 'undefined' || currentProjectId === '') {
        dispatch(setOpenProjectDialog(true)); return false;
      }
      if (!text && !userTables && !currentSqlQuery && !profileResult) return false;

      let session = currentModelId;

      if (create && !session) {
        const newSession = uuidv4();
        try {
          await dispatch(createModel({
            name: modelName.trim() || 'nouveau_script',
            session_id: newSession,
            project_id: currentProjectId,
          })).unwrap?.();
          session = newSession;
        } catch {
          dispatch(setError('Création du modèle échouée.'));
          return false;
        }
      }

      if (!session) { dispatch(setError('Model introuvable')); return false; }

      try {
        await dispatch(chatQuery({
          userInput: text,
          sessionId: session,
          project: currentProjectId,
          dialect: currentProject.dialect,
          query: currentSqlQuery || undefined,
          ChangedMessageId: messageId,
          t,
          parentMessageId,
          userTables,
          profileResult,
          testIndex,
        })).unwrap?.();
        return true;
      } catch {
        return false;
      }
    },
    [currentProject, currentProjectId, currentModelId, modelName, dispatch, t]
  );

  // -------- First message from NewChatForm (SQL required)
  const handleNewChatSubmit = useCallback(
    async () => {
      if (isSending || !sqlQuery.trim()) return;
      if (!currentProject || !currentProjectId) return;

      setIsSending(true);
      setSubmitError(null);
      setValidationStatus('validating');

      // 1. Create model (reuse existing session if retrying after auto-import)
      const newSession: string = pendingSessionRef.current ?? uuidv4();
      if (!pendingSessionRef.current) {
        setSubmissionStep(t('Création du modèle…'));
        try {
          await dispatch(createModel({
            name: modelName.trim() || 'nouveau_script',
            session_id: newSession,
            project_id: currentProjectId,
          })).unwrap?.();
          pendingSessionRef.current = newSession;
        } catch {
          dispatch(setError('Création du modèle échouée.'));
          setValidationStatus('idle');
          setSubmissionStep(null);
          setIsSending(false);
          return;
        }
      }

      // 2. Validate + save SQL to model
      setSubmissionStep(t('Validation de la requête SQL…'));
      let validateResult: { valid: boolean; error?: string; missing_tables?: string[]; used_columns?: string[]; optimized_sql?: string; auto_import_available?: boolean; tables_to_import?: string[]; sql_message_id?: string } | null = null;
      try {
        validateResult = await validateQueryApi({
          sql: sqlQuery,
          project: currentProjectId,
          dialect: currentProject.dialect,
          session: newSession,
          parent_message_id: '',
        });
      } catch {
        setValidationStatus('error');
        setSubmissionStep(null);
        setSubmitError(t('Erreur lors de la validation.'));
        setIsSending(false);
        return;
      }

      if (!validateResult?.valid) {
        setValidationStatus('error');
        setSubmissionStep(null);
        if (validateResult?.missing_tables?.length) {
          setMissingTables(validateResult.missing_tables);
          if (validateResult.auto_import_available && validateResult.tables_to_import?.length) {
            setTablesToImport(validateResult.tables_to_import);
          }
          // Keep pendingSessionRef so the retry reuses this session
        } else {
          pendingSessionRef.current = null;
          setSubmitError(validateResult?.error || t('Requête invalide.'));
        }
        setIsSending(false);
        return;
      }

      setOptimizedSql(validateResult.optimized_sql ?? '');
      dispatch(pushSqlHistory({ id: uuidv4(), sql: sqlQuery, optimizedSql: validateResult.optimized_sql ?? '', parentMessageId: '' }));


      // 3. Check profile before starting stream
      setSubmissionStep(t('Vérification du profiling…'));
      const usedColumns = validateResult.used_columns ?? [];
      let profileComplete = true;
      try {
        const profileResult = await checkProfileApi({
          sql: sqlQuery,
          project: currentProjectId,
          dialect: currentProject.dialect,
          session: newSession,
          used_columns: usedColumns,
        });
        profileComplete = profileResult.profile_complete;
        if (!profileComplete && profileResult.profile_request) {
          if (profileResult.auto_profile_available && profileResult.profile_request.profile_query) {
            setValidationStatus('valid');
            navigate(`/models/${currentProjectId}/${newSession}`);
            dispatch(setCurrentId(newSession));
            const req = profileResult.profile_request;
            const capturedProjectId = currentProjectId;
            const capturedDialect = currentProject!.dialect;
            const capturedSql = sqlQuery;
            const doStreamNew = () => {
              setPendingFirstLoad(true);
              dispatch(chatQuery({
                userInput: '',
                sessionId: newSession,
                project: capturedProjectId,
                dialect: capturedDialect,
                query: capturedSql,
                ChangedMessageId: '',
                t,
                parentMessageId: '',
              }));
            };
            setPendingAutoProfile({
              profileRequest: req,
              onConfirm: async () => {
                setIsAutoProfileRunning(true);
                try { await autoProfileApi({ profile_sql: req.profile_query, project: capturedProjectId, session: newSession }); } catch { /* proceed anyway */ }
                setIsAutoProfileRunning(false);
                setPendingAutoProfile(null);
                doStreamNew();
              },
              onSkip: async () => {
                setPendingAutoProfile(null);
                try { await skipProfilingApi({ session: newSession }); } catch { /* non bloquant */ }
                doStreamNew();
              },
            });
            pendingSessionRef.current = null;
            setIsSending(false);
            return;
          } else {
            try { await skipProfilingApi({ session: newSession }); } catch { /* non bloquant */ }
            // fall through to stream
          }
        }
      } catch {
        // If check-profile fails, proceed to stream anyway
      }

      // 4. Navigate + start stream (pre_routing skips evaluate since SQL already saved)
      setSubmissionStep(t('Génération des tests…'));
      setValidationStatus('valid');
      navigate(`/models/${currentProjectId}/${newSession}`);
      dispatch(setCurrentId(newSession));
      setPendingFirstLoad(true);

      try {
        await dispatch(chatQuery({
          userInput: '',
          sessionId: newSession,
          project: currentProjectId,
          dialect: currentProject.dialect,
          query: sqlQuery,
          ChangedMessageId: '',
          t,
          parentMessageId: validateResult?.sql_message_id ?? '',
        })).unwrap?.();
      } catch { /* non bloquant */ }

      pendingSessionRef.current = null;
      setSubmissionStep(null);
      setIsSending(false);
    },
    [sqlQuery, modelName, currentProject, currentProjectId, dispatch, navigate, t, isSending]
  );

  // -------- Auto-import missing tables then retry submit
  const handleAutoImport = useCallback(async () => {
    if (!tablesToImport || !currentProjectId) return;
    setIsImporting(true);
    try {
      await importMissingTablesApi({
        tables_to_import: tablesToImport,
        project: currentProjectId,
      });
      setMissingTables(null);
      setTablesToImport(null);
      setValidationStatus('idle');
      // Retry the submit now that the schema has been updated
      await handleNewChatSubmit();
    } catch (err: any) {
      const detail = err?.detail;
      if (detail && typeof detail === 'object' && detail.needs_manual_config) {
        setSubmitError(detail.message || 'Tables non qualifiées : configurez-les manuellement dans les paramètres du projet.');
      } else {
        setSubmitError(typeof detail === 'string' ? detail : 'Erreur lors de l\'import des tables.');
      }
    } finally {
      setIsImporting(false);
    }
  }, [tablesToImport, currentProjectId, handleNewChatSubmit]);

  // -------- Silent auto-import when user preference is set
  useEffect(() => {
    if (!tablesToImport || !currentProjectId) return;
    const projectKey = `autoImport_project_${currentProjectId}`;
    const shouldAutoImport =
      currentProject?.auto_import === true ||
      localStorage.getItem('autoImport_always') === 'true' ||
      localStorage.getItem(projectKey) === 'true';
    if (shouldAutoImport) {
      handleAutoImport();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tablesToImport, currentProjectId]);

  // -------- Follow-up messages (DroppableTextField)
  const handleSendMessage = useCallback(
    async (textParam?: string, testIdx?: number) => {
      if (isSending) return;
      const text = (textParam ?? userInput).trim();
      const effectiveTestIndex = testIdx ?? (selectedTestIndex !== null ? selectedTestIndex : undefined);
      if (!text && effectiveTestIndex === undefined) return;

      setIsSending(true);
      const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
      const lastMessageId = lastMessage ? lastMessage.id : '';

      const ok = await sendMessage(text, sqlQuery, '', lastMessageId, undefined, false, effectiveTestIndex);
      setIsSending(false);

      if (ok) {
        setUserInput('');
        setSelectedTestIndex(null);
        if (draftKeyRef.current) localStorage.removeItem(draftKeyRef.current);
      }
    },
    [userInput, sqlQuery, renderMessages, selectedChildIndices, sendMessage, isSending, selectedTestIndex]
  );

  const onSendClick = useCallback(() => {
    handleSendMessage(userInput);
  }, [userInput, handleSendMessage]);

  // -------- SQL bar update (re-run with new SQL)
  const handleSQLUpdate = useCallback(
    async (newSql: string) => {
      if (isSending || !newSql.trim()) return;
      if (!currentProject || !currentProjectId || !currentModelId) return;
      setIsSending(true);
      setSqlQuery(newSql);

      const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
      const lastMessageId = lastMessage ? lastMessage.id : '';
      const effectiveParentId = restoredMessageId || lastMessageId;
      setRestoredMessageId(undefined);

      let resolvedOptimizedSql = optimizedSql;

      if (skipValidationRef.current) {
        // Restore from history — skip validation
        skipValidationRef.current = false;
      } else {
        // Validate + save SQL to model before streaming
        try {
          const validateResult = await validateQueryApi({
            sql: newSql,
            project: currentProjectId,
            dialect: currentProject.dialect,
            session: currentModelId,
            parent_message_id: effectiveParentId,
          });
          if (!validateResult?.valid) {
            if (validateResult?.missing_tables?.length) {
              setMissingTables(validateResult.missing_tables);
              if (validateResult.auto_import_available && validateResult.tables_to_import?.length) {
                setTablesToImport(validateResult.tables_to_import);
              }
            } else {
              setSubmitError(validateResult?.error || t('Requête invalide.'));
            }
            setIsSending(false);
            return;
          }
          resolvedOptimizedSql = validateResult.optimized_sql ?? '';
          setOptimizedSql(resolvedOptimizedSql);
        } catch {
          setSubmitError(t('Erreur lors de la validation.'));
          setIsSending(false);
          return;
        }
      }
      dispatch(pushSqlHistory({ id: uuidv4(), sql: newSql, optimizedSql: resolvedOptimizedSql, parentMessageId: effectiveParentId }));
      try {
        await dispatch(chatQuery({
          userInput: '',
          sessionId: currentModelId,
          project: currentProjectId,
          dialect: currentProject.dialect,
          query: newSql,
          ChangedMessageId: '',
          t,
          parentMessageId: effectiveParentId,
          context: 'sql_update',
        })).unwrap?.();
      } catch { /* non bloquant */ }
      setIsSending(false);
    },
    [isSending, currentProject, currentProjectId, currentModelId, optimizedSql, renderMessages, selectedChildIndices, dispatch, t, restoredMessageId]
  );

  // -------- Restore SQL from history (no revalidation needed)
  const handleHistorySelect = useCallback((entry: SqlHistoryEntry) => {
    setSqlQuery(entry.sql);
    setOptimizedSql(entry.optimizedSql);
    skipValidationRef.current = true;
    setHistoryRestoreTrigger((n) => n + 1);
  }, []);

  const handleStopStream = () => stopStream();

  const handleRestoreState = useCallback((sql?: string, optimizedSql?: string, messageId?: string, restoredTestResults?: any[]) => {
    if (sql) { setSqlQuery(sql); dispatch(setQuery(sql)); setHistoryRestoreTrigger((n) => n + 1); }
    if (optimizedSql !== undefined) { setOptimizedSql(optimizedSql); dispatch(setOptimizedQuery(optimizedSql)); }
    if (restoredTestResults) dispatch(setTestResults(restoredTestResults));
    if (messageId) { setRestoredMessageId(messageId); dispatch(setRestoredMessageIdAction(messageId)); }
    if (currentModelId && sql) {
      dispatch(patchModelSql({
        sessionId: currentModelId,
        sql,
        optimizedSql: optimizedSql ?? '',
        testResults: restoredTestResults,
        restoredMessageId: messageId,
      }));
    }
  }, [dispatch, currentModelId]);

  const handleAlwaysFixChange = useCallback((value: boolean) => {
    setAlwaysFix(value);
    localStorage.setItem('alwaysFix', String(value));
  }, []);

  // Auto-fix : déclenche la correction automatiquement si alwaysFix est activé
  useEffect(() => {
    if (!alwaysFix || loading !== false || isSending) return;
    const lastMsg = renderMessages[renderMessages.length - 1];
    if (!lastMsg || !('id' in lastMsg) || (lastMsg as any).type === 'group') return;
    const msg = lastMsg as any;
    if (!msg.contents?.error) return;
    if (autoFixedIds.current.has(msg.id)) return;
    autoFixedIds.current.add(msg.id);
    const lastMessage = msg;
    sendMessage('__fix_error__', sqlQuery, '', lastMessage.parent ?? lastMessage.id, undefined, false);
  }, [renderMessages, alwaysFix, loading, isSending, sqlQuery, sendMessage]);

  // -------- Browser notification on generation complete
  useEffect(() => {
    if (loading === true) {
      wasHistoryLoadRef.current = awaitingGetMessagesRef.current;
      if (Notification.permission === 'default') {
        Notification.requestPermission();
      }
    }
    if (prevLoadingRef.current === true && loading === false && !wasHistoryLoadRef.current) {
      if (Notification.permission === 'granted') {
        const body = error ? 'La génération a échoué.' : 'Tests générés avec succès !';
        new Notification('MockSQL', { body, icon: '/favicon.ico' });
      }
    }
    prevLoadingRef.current = loading;
  }, [loading, error]);

  const handleAddTest = useCallback(() => {
    setSelectedTestIndex(null);
    setChatOverlayOpen(true);
  }, []);

  const handleSelectTestForModification = useCallback((idx: number) => {
    setSelectedTestIndex(idx);
    setChatOverlayOpen(true);
  }, []);

  const handleRerunTest = useCallback((idx: number) => {
    handleSendMessage('Régénère ce test', idx);
  }, [handleSendMessage]);


  return (
    <Container
      sx={{
        height: '100vh',
        width: '100%',
        maxHeight: '100%',
        maxWidth: `calc(100vw - ${drawerOpen ? drawerWidth : 0}px)`,
        transition: 'max-width 0.2s ease',
        p: 0,
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      {/* Workspace-mode alerts (errors, missing tables) */}
      {uiPhase === 'workspace' && (submitError || (lastError && !lastErrorDismissed) || missingTables) && (
        <Box sx={{ flexShrink: 0, px: 2, pt: 1 }}>
          {submitError && (
            <Alert severity="error" sx={{ borderRadius: '12px', mb: 1 }} onClose={() => setSubmitError(null)}>
              {submitError}
            </Alert>
          )}
          {lastError && !lastErrorDismissed && !submitError && (
            <Alert severity="warning" sx={{ borderRadius: '12px', mb: 1 }} onClose={() => setLastErrorDismissed(true)}>
              {lastError}
            </Alert>
          )}
          {missingTables && currentProjectId && (
            <Box sx={{ mb: 1 }}>
              <MissingTablesAlert
                missingTables={missingTables}
                projectId={currentProjectId}
                onImport={tablesToImport ? handleAutoImport : undefined}
                importing={isImporting}
              />
            </Box>
          )}
        </Box>
      )}

      {/* ── STEP 1: SQL entry — TODO: replace with SQL file autocomplete ── */}
      {uiPhase === 'entry' && (
        <Box sx={{ flex: 1, overflow: 'auto', minHeight: 0 }} ref={containerRef}>
          <Grid container alignItems="center">
            <Grid item xs={false} md={1} />
            <Grid item xs={12} md={10}>
              {isSending && submissionStep && (
                <Box sx={{ mt: 2, mb: 1 }}>
                  <LinearProgress variant="indeterminate" sx={{ height: 6, borderRadius: 3, backgroundColor: '#e0f7f5', '& .MuiLinearProgress-bar': { backgroundColor: '#1ca8a4' } }} />
                  <Typography variant="body2" sx={{ mt: 0.75, color: '#555', textAlign: 'center' }}>{submissionStep}</Typography>
                </Box>
              )}
              {validationStatus === 'valid' && (
                <Alert severity="success" icon={false} sx={{ borderRadius: '12px', mt: 1 }}>
                  ✓ Requête valide
                </Alert>
              )}
              {submitError && (
                <Alert severity="error" sx={{ borderRadius: '12px', mt: 2 }}>
                  {submitError}
                </Alert>
              )}
              {missingTables && currentProjectId && (
                <MissingTablesAlert
                  missingTables={missingTables}
                  projectId={currentProjectId}
                  onImport={tablesToImport ? handleAutoImport : undefined}
                  importing={isImporting}
                />
              )}
              {!canType && (
                <Alert icon={false} severity="info" sx={{ borderRadius: '16px', margin: '20px', padding: 2, textAlign: 'center' }}>
                  {t('Sélectionnez ou créez un projet pour écrire.')}
                </Alert>
              )}
            </Grid>
          </Grid>
        </Box>
      )}

      {/* ── STEP 2: Workspace ── */}
      {uiPhase === 'workspace' && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
          {/* Loading bar — visible when chatQuery is running */}
          {(loading || isSending) && (
            <Box sx={{ flexShrink: 0, px: 2, pt: 0.75, pb: 0.5 }}>
              <LinearProgress variant="indeterminate" sx={{ height: 5, borderRadius: 3, backgroundColor: '#e0f7f5', '& .MuiLinearProgress-bar': { backgroundColor: '#1ca8a4' } }} />
              <Typography variant="caption" sx={{ color: '#6b8287', mt: 0.4, display: 'block', textAlign: 'center' }}>
                {loading ? (loading_message || 'Raisonnement') : 'Validation de la requête'}…
              </Typography>
            </Box>
          )}
          {/* Full-width tests panel — position:relative so overlay can anchor to it */}
          <Box sx={{ flex: 1, position: 'relative', display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
            <TestsPanel
              onAddTest={handleAddTest}
              onSelectForModification={handleSelectTestForModification}
              onRerunTest={handleRerunTest}
              selectedTestIndex={selectedTestIndex}
              sql={sqlQuery}
              onSqlUpdate={handleSQLUpdate}
              optimizedSql={optimizedSql}
              sqlHistory={sqlHistory}
              onHistorySelect={handleHistorySelect}
              historyRestoreTrigger={historyRestoreTrigger}
              sqlDisabled={!canType || isSending}
              sqlLoading={isSending}
              sqlHasError={lastMsgHasError}
              onSuggestionFill={(text) => { setSelectedTestIndex(null); setUserInput(text); setChatOverlayOpen(true); }}
              onUpload={(uploadedData) => {
                const lastMsg = renderMessages[renderMessages.length - 1] as any;
                sendMessage('', sqlQuery, lastMsg?.id, lastMsg?.id, uploadedData, false);
              }}
              onOpenChat={() => { setSelectedTestIndex(null); setChatOverlayOpen(true); }}
            />

            {/* Backdrop */}
            {chatOverlayOpen && (
              <Box
                onClick={() => { setChatOverlayOpen(false); setSelectedTestIndex(null); }}
                sx={{ position: 'absolute', inset: 0, bgcolor: 'rgba(15,39,42,.18)', zIndex: 20 }}
              />
            )}

            {/* Sliding chat panel */}
            <Slide direction="left" in={chatOverlayOpen} timeout={220} unmountOnExit mountOnEnter>
              <Box sx={{
                position: 'absolute', top: 0, right: 0, bottom: 0,
                width: { xs: '100%', sm: 480 },
                bgcolor: '#fff',
                borderLeft: '1px solid #e4eaec',
                boxShadow: '-10px 0 40px rgba(15,39,42,.09)',
                display: 'flex', flexDirection: 'column', zIndex: 21,
              }}>
                {/* Header */}
                <Box sx={{ px: 2, py: 1.5, borderBottom: '1px solid #e4eaec', display: 'flex', alignItems: 'center', gap: 1.5, flexShrink: 0 }}>
                  <Box sx={{ width: 30, height: 30, borderRadius: '9px', bgcolor: '#ecf7f6', color: '#1ca8a4', display: 'grid', placeItems: 'center' }}>
                    <AutoAwesomeIcon sx={{ fontSize: 15 }} />
                  </Box>
                  <Box sx={{ flex: 1, minWidth: 0 }}>
                    <Typography sx={{ fontWeight: 700, color: '#0f272a', fontSize: 14, lineHeight: 1.2 }}>MockSQL</Typography>
                    <Typography sx={{ fontSize: 11.5, color: '#6b8287', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                      {selectedTestIndex !== null
                        ? `Test #${selectedTestIndex + 1} — Modifier avec MockSQL`
                        : 'Instruction globale'}
                    </Typography>
                  </Box>
                  <IconButton size="small" onClick={() => { setChatOverlayOpen(false); setSelectedTestIndex(null); }}>
                    <CloseIcon sx={{ fontSize: 16 }} />
                  </IconButton>
                </Box>

                {/* Scrollable messages */}
                <Box sx={{ flex: 1, overflow: 'auto', minHeight: 0, px: 2, pt: 1 }} ref={containerRef}>
                  <MessageDisplay
                    sendMessage={(input, msgId, parentMsgId, userTables, profileResult) =>
                      sendMessage(input, sqlQuery, msgId, parentMsgId, userTables, false, undefined, profileResult)
                    }
                    renderMessages={renderMessages}
                    onRestoreState={handleRestoreState}
                    restoredMessageId={restoredMessageId}
                    alwaysFix={alwaysFix}
                    onAlwaysFixChange={handleAlwaysFixChange}
                    sqlHistory={sqlHistory}
                    onSqlRestore={handleHistorySelect}
                  />

                  {(loading || isSending) && (
                    <Alert icon={false} severity="info" sx={{ borderRadius: '16px', margin: '12px 0', padding: 2, textAlign: 'center', backgroundColor: '#f5f5f5', color: '#333' }}>
                      <Box sx={{ width: '100%', mb: 1 }}>
                        <LinearProgress variant="indeterminate" sx={{ width: '100%', height: 8, borderRadius: 4, backgroundColor: '#e0f7f5', '& .MuiLinearProgress-bar': { backgroundColor: '#1ca8a4' } }} />
                      </Box>
                      <Typography variant="body2">{loading ? (loading_message || 'Raisonnement') : 'Validation de la requête'}...</Typography>
                    </Alert>
                  )}

                  {error && <Alert severity="error" sx={{ borderRadius: '16px', my: 1 }}>{error}</Alert>}

                  {!canType && (
                    <Alert icon={false} severity="info" sx={{ borderRadius: '16px', my: 1, padding: 2, textAlign: 'center' }}>
                      {t('Sélectionnez ou créez un projet pour écrire.')}
                    </Alert>
                  )}
                </Box>

                {/* Pinned input */}
                <Box sx={{ flexShrink: 0, px: 2, py: 1, borderTop: '1px solid #e4eaec' }}>
                  {selectedTestIndex !== null && (
                    <Box sx={{ mb: 0.75 }}>
                      <Chip
                        label={`Modifier le test #${selectedTestIndex + 1}`}
                        onDelete={() => setSelectedTestIndex(null)}
                        size="small"
                        sx={{ bgcolor: '#e8f5f5', color: '#1ca8a4', border: '1px solid #1ca8a444', fontWeight: 600, fontSize: 11 }}
                      />
                    </Box>
                  )}
                  <DroppableTextField
                    userInput={userInput}
                    setUserInput={setUserInput}
                    sendMessage={onSendClick}
                    stopStream={handleStopStream}
                    disabled={!canType || isSending}
                    placeholder={
                      canType
                        ? isSending
                          ? t('Envoi en cours…')
                          : selectedTestIndex !== null
                            ? t('Décrire la modification souhaitée…')
                            : t('Demander une modification, ajouter des contraintes…')
                        : t('Choisissez un projet pour écrire…')
                    }
                  />
                </Box>
              </Box>
            </Slide>
          </Box>
          <DuckDBFooter />
        </Box>
      )}
      {/* ── Auto-profiling confirmation dialog ── */}
      {pendingAutoProfile && (
        <Dialog open maxWidth="sm" fullWidth>
          <DialogTitle sx={{ fontWeight: 700 }}>Profiling optionnel</DialogTitle>
          <DialogContent>
            <Box
              sx={{
                display: 'flex',
                alignItems: 'center',
                gap: 1.5,
                bgcolor: '#f0faf5',
                border: '1px solid #a5d6b7',
                borderRadius: 2,
                px: 2,
                py: 1.5,
                mb: 2,
              }}
            >
              <CheckCircleOutlineIcon sx={{ color: '#2e7d52', fontSize: 22, flexShrink: 0 }} />
              <Typography variant="body2" sx={{ color: '#1e5c38', fontWeight: 600 }}>
                Requête validée avec succès
              </Typography>
            </Box>
            <Typography variant="body2" sx={{ mb: 2 }}>
              MockSQL peut lancer automatiquement le profiling de vos colonnes.
              Cela <strong>améliore la précision des tests générés</strong> en permettant
              de mieux comprendre la distribution réelle des données.
            </Typography>
            {pendingAutoProfile.profileRequest.billing_tb !== undefined && (
              <Box
                sx={{
                  display: 'flex',
                  alignItems: 'center',
                  gap: 1.5,
                  bgcolor: '#fffbe6',
                  border: '1px solid #ffe082',
                  borderRadius: 2,
                  px: 2,
                  py: 1.5,
                  mb: 2,
                }}
              >
                <Typography variant="body2" sx={{ color: '#7a5f00', flex: 1 }}>
                  Cette opération va scanner environ{' '}
                  <strong>
                    {pendingAutoProfile.profileRequest.billing_tb < 0.001
                      ? '< 0,001'
                      : pendingAutoProfile.profileRequest.billing_tb.toFixed(3)}{' '}
                    To
                  </strong>{' '}
                  de données
                </Typography>
                <Chip
                  label={`~${pendingAutoProfile.profileRequest.billing_tb < 0.001
                    ? '< 0.001'
                    : pendingAutoProfile.profileRequest.billing_tb.toFixed(3)} To`}
                  size="small"
                  sx={{ bgcolor: '#fff3cd', color: '#7a5f00', border: '1px solid #ffe082', fontWeight: 700 }}
                />
              </Box>
            )}
          {isAutoProfileRunning && (
            <Box sx={{ px: 3, pb: 1 }}>
              <LinearProgress
                variant="indeterminate"
                sx={{ height: 6, borderRadius: 3, bgcolor: '#e0f7f5', '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' } }}
              />
              <Typography variant="caption" sx={{ color: '#555', mt: 0.5, display: 'block' }}>
                Profiling en cours…
              </Typography>
            </Box>
          )}
          </DialogContent>
          <DialogActions sx={{ px: 3, pb: 2, gap: 1 }}>
            <Button
              variant="outlined"
              onClick={() => pendingAutoProfile.onSkip()}
              disabled={isAutoProfileRunning}
              sx={{ textTransform: 'none', color: '#999', borderColor: '#ddd', '&:hover': { borderColor: '#aaa', bgcolor: 'transparent' } }}
            >
              Passer
            </Button>
            <Button
              variant="contained"
              onClick={pendingAutoProfile.onConfirm}
              disabled={isAutoProfileRunning}
              sx={{ textTransform: 'none', bgcolor: '#1ca8a4', '&:hover': { bgcolor: '#159e9a' } }}
            >
              {isAutoProfileRunning ? 'Profiling…' : 'Lancer le profiling'}
            </Button>
          </DialogActions>
        </Dialog>
      )}
    </Container>
  );
};

export default React.memo(ChatComponent);
