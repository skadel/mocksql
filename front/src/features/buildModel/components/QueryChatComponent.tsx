import React, { useCallback, useMemo, useRef, useState, useEffect } from 'react';
import { useTranslation, Trans } from 'react-i18next';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { v4 as uuidv4 } from 'uuid';
import { throttle } from 'lodash';
import { Alert, Box, Button, Chip, Dialog, DialogActions, DialogContent, DialogTitle, InputAdornment, LinearProgress, TextField, Typography } from '@mui/material';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import SearchIcon from '@mui/icons-material/Search';
import ScienceIcon from '@mui/icons-material/Science';
import { Container } from '../../../style/StyledComponents';
import { getLastMessage } from '../../../utils/messages';
import MissingTablesAlert from './MissingTablesAlert';
import TestsPanel from './TestsPanel';
import DuckDBFooter from './DuckDBFooter';
import ChatColumn from './ChatColumn';
import ArtefactHeader from './ArtefactHeader';
import { drawerWidth } from '../../appBar/components/DrawerComponent';
import { createModel, createTestApi, fetchModelSql, fetchModels } from '../../../api/models';
import SqlEditor from '../../../shared/SqlEditor';
import { chatQuery, stopStream, validateQueryApi, checkProfileApi, skipProfilingApi, importMissingTablesApi, autoProfileApi } from '../../../api/query';
import { useLocalStorageState } from '../../../hooks/useLocalStorageState';
import { useSqlFileLoader } from '../hooks/useSqlFileLoader';
import { FIX_ERROR_COMMAND } from '../constants';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { setCurrentId } from '../../appBar/appBarSlice';
import { setError, setQueryComponentGraph, setQuery, setOptimizedQuery, setTestResults, pushSqlHistory, setRestoredMessageId as setRestoredMessageIdAction, setWorkspaceMode, resetContext, resetMessages } from '../buildModelSlice';
import { getMessages, patchModelSql, clearHistoryApi } from '../../../api/messages';
import { getRenderMessages } from '../../../selectors/getRenderMessages';
import { ProfileRequest, SqlHistoryEntry } from '../../../utils/types';
import { relativeDate } from '../../../utils/dates';

// Dialect is read from the current project — fallback to bigquery for backward compat.



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
  const [selectedTestIndex, setSelectedTestIndex] = useState<number | null>(null);
  const [sqlDirty, setSqlDirty] = useState(false);
  const [pendingFirstLoad, setPendingFirstLoad] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [lastErrorDismissed, setLastErrorDismissed] = useState(false);
  const [missingTables, setMissingTables] = useState<string[] | null>(null);
  const [tablesToImport, setTablesToImport] = useState<string[] | null>(null);
  const [isImporting, setIsImporting] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  const [pendingAutoProfile, setPendingAutoProfile] = useState<{
    profileRequest: ProfileRequest;
    onConfirm: () => Promise<void>;
    onSkip: () => Promise<void>;
  } | null>(null);
  const [isAutoProfileRunning, setIsAutoProfileRunning] = useState(false);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const [_validationStatus, setValidationStatus] = useState<'idle' | 'validating' | 'valid' | 'error'>('idle');
  const [submissionStep, setSubmissionStep] = useState<string | null>(null);
  const [alwaysFix, setAlwaysFix] = useLocalStorageState('alwaysFix', false);
  const [selectedModelName, setSelectedModelName] = useState<string | null>(null);
  const [previewSql, setPreviewSql] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const sqlFiles = useSqlFileLoader();
  const [fileSearch, setFileSearch] = useState('');

  const [historyRestoreTrigger, setHistoryRestoreTrigger] = useState(0);
  const [assertionOnly, setAssertionOnly] = useState(false);
  const skipValidationRef = useRef(false);
  const forceNewRef = useRef(false);

  // Fetch SQL preview when a model is selected in the entry phase
  useEffect(() => {
    if (!selectedModelName) { setPreviewSql(null); return; }
    let cancelled = false;
    setPreviewLoading(true);
    fetchModelSql(selectedModelName).then((sql) => {
      if (!cancelled) { setPreviewSql(sql); setPreviewLoading(false); }
    });
    return () => { cancelled = true; };
  }, [selectedModelName]);

  // Pre-fill model selector from URL params (?model=xxx&forceNew=1)
  useEffect(() => {
    const searchParams = new URLSearchParams(location.search);
    const modelParam = searchParams.get('model');
    const isForceNew = searchParams.get('forceNew') === '1';
    if (modelParam) setSelectedModelName(modelParam);
    forceNewRef.current = isForceNew;
  }, [location.search]);
  const autoFixedIds = useRef<Set<string>>(new Set());
  const forcedRouteRef = useRef('');
  const awaitingGetMessagesRef = useRef(false);
  const pendingSessionRef = useRef<string | null>(null);
  const prevLoadingRef = useRef<boolean | null>(null);
  const isGeneratingRef = useRef(false);

  const {
    queryComponentGraph: messages,
    loading,
    loading_message,
    streamingReasoning,
    error,
    selectedChildIndices,
    query: storedQuery,
    optimizedQuery: storedOptimizedQuery,
    sqlHistory,
    restoredMessageId: storedRestoredMessageId,
    lastError,
    testResults,
  } = useAppSelector((state) => state.buildModel);

  const messagesRef = useRef(messages);
  const { currentModelId, drawerOpen, models: allModels } = useAppSelector((state) => state.appBarModel);
  const currentProjectId = useAppSelector((state) => state.appBarModel.currentProjectId);
  const currentProject = useAppSelector((state) => state.appBarModel.currentProject);
  const DIALECT = currentProject?.dialect ?? 'bigquery';
  const currentModel = useMemo(
    () => allModels.find(m => m.session_id === currentModelId),
    [allModels, currentModelId],
  );
  const currentModelName = currentModel?.name ?? '';
  const currentModelPath = currentModel
    ? (currentModel.folder ? `${currentModel.folder}/${currentModel.name}` : currentModel.name)
    : '';

  messagesRef.current = messages;

  const renderMessages = useAppSelector(getRenderMessages);
  const lastMsgHasError = useMemo(() => {
    if (!renderMessages.length) return false;
    const last = renderMessages[renderMessages.length - 1];
    return !!(last && 'contents' in last && (last as any).contents?.error);
  }, [renderMessages]);

  // ui phase: 'entry' | 'workspace'
  const uiPhase = !pendingFirstLoad && !currentModelId ? 'entry' : 'workspace';

  // Sync workspace mode to Redux so App.tsx can hide the sidebar
  useEffect(() => {
    dispatch(setWorkspaceMode(uiPhase === 'workspace'));
  }, [uiPhase, dispatch]);

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

  // -------- Model loading from URL param
  useEffect(() => {
    const modelID = params.modelID as string;
    if (!modelID && !currentModelId) return;
    if (!modelID && currentModelId) {
      navigate(`/models/${currentModelId}`);
    } else if (modelID && !currentModelId) {
      dispatch(setCurrentId(modelID));
      awaitingGetMessagesRef.current = true;
      dispatch(getMessages({ modelId: modelID, t }));
    } else if (modelID && currentModelId && modelID !== currentModelId) {
      dispatch(setCurrentId(modelID));
      awaitingGetMessagesRef.current = true;
      dispatch(getMessages({ modelId: modelID, t }));
    }
  }, [params.modelID, currentModelId, dispatch, navigate, t]);

  // -------- Reset local SQL when switching models
  const prevModelIdRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    const prev = prevModelIdRef.current;
    prevModelIdRef.current = currentModelId ?? undefined;
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

  // -------- Clear pendingFirstLoad once backend is done
  useEffect(() => {
    if (pendingFirstLoad && loading === false) {
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

  // -------- Sync stored SQL + optimized SQL → local state
  useEffect(() => {
    if (storedQuery) setSqlQuery(storedQuery);
    if (storedOptimizedQuery) setOptimizedSql(storedOptimizedQuery);
  }, [storedQuery, storedOptimizedQuery]);

  // -------- Sync restored message id → local state
  useEffect(() => {
    if (storedRestoredMessageId) setRestoredMessageId(storedRestoredMessageId);
  }, [storedRestoredMessageId]);

  // -------- Reset awaiting flag after messages load
  useEffect(() => {
    if (!awaitingGetMessagesRef.current) return;
    if (loading !== false) return;
    awaitingGetMessagesRef.current = false;
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loading]);

  // -------- Reset validation state when user edits SQL
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => { setValidationStatus('idle'); setSubmitError(null); setMissingTables(null); setTablesToImport(null); }, [sqlQuery]);

  // -------- Draft localStorage (follow-up messages only)
  const draftKeyRef = useRef<string>('');
  useEffect(() => {
    draftKeyRef.current = `draft:${currentModelId || 'new'}`;
    const saved = localStorage.getItem(draftKeyRef.current);
    if (saved && !userInput) setUserInput(saved);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [currentModelId]);

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
      profileResult?: string,
      isAssertionOnly?: boolean,
      forceRoute?: string
    ): Promise<boolean> => {
      const text = (input ?? '').trim();
      if (!text && !userTables && !currentSqlQuery && !profileResult) return false;

      let session = currentModelId;

      if (create && !session) {
        const newSession = uuidv4();
        try {
          await dispatch(createModel({
            name: modelName.trim() || 'nouveau_script',
            session_id: newSession,
          })).unwrap?.();
          session = newSession;
        } catch {
          dispatch(setError(t('errors.model_creation_failed')));
          return false;
        }
      }

      if (!session) { dispatch(setError(t('errors.model_not_found'))); return false; }

      try {
        isGeneratingRef.current = true;
        await dispatch(chatQuery({
          userInput: text,
          sessionId: session,
          project: '',
          dialect: DIALECT,
          query: currentSqlQuery || undefined,
          ChangedMessageId: messageId,
          t,
          parentMessageId,
          userTables,
          profileResult,
          testIndex,
          assertionOnly: isAssertionOnly,
          forceRoute,
        })).unwrap?.();
        return true;
      } catch {
        return false;
      }
    },
    [currentModelId, modelName, dispatch, t]
  );

  // -------- Shared validate → profile → generate flow
  const runSqlSubmissionFlow = useCallback(async (sql: string, sessionId: string) => {
    setSubmissionStep(t('loading.validating_sql'));
    let validateResult: { valid: boolean; error?: string; missing_tables?: string[]; used_columns?: string[]; optimized_sql?: string; auto_import_available?: boolean; tables_to_import?: string[]; sql_message_id?: string } | null = null;
    try {
      validateResult = await validateQueryApi({ sql, project: '', dialect: DIALECT, session: sessionId, parent_message_id: '' });
    } catch {
      setValidationStatus('error');
      setSubmissionStep(null);
      setSubmitError(t('errors.validation_error'));
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
      } else {
        pendingSessionRef.current = null;
        setSubmitError(validateResult?.error || t('errors.invalid_query'));
      }
      setIsSending(false);
      return;
    }

    setOptimizedSql(validateResult.optimized_sql ?? '');
    dispatch(pushSqlHistory({ id: uuidv4(), sql, optimizedSql: validateResult.optimized_sql ?? '', parentMessageId: '' }));

    setSubmissionStep(t('loading.checking_profiling'));
    const usedColumns = validateResult.used_columns ?? [];
    try {
      const profileResult = await checkProfileApi({ sql, project: '', dialect: DIALECT, session: sessionId, used_columns: usedColumns });
      if (!profileResult.profile_complete && profileResult.profile_request) {
        if (profileResult.auto_profile_available && profileResult.profile_request.profile_query) {
          setValidationStatus('valid');
          navigate(`/models/${sessionId}`);
          dispatch(setCurrentId(sessionId));
          const req = profileResult.profile_request;
          const doStream = () => {
            setPendingFirstLoad(true);
            isGeneratingRef.current = true;
            dispatch(chatQuery({ userInput: '', sessionId, project: '', dialect: DIALECT, query: sql, ChangedMessageId: '', t, parentMessageId: '' }));
          };
          setPendingAutoProfile({
            profileRequest: req,
            onConfirm: async () => {
              setIsAutoProfileRunning(true);
              try { await autoProfileApi({ profile_sql: req.profile_query, project: '', session: sessionId }); } catch {}
              setIsAutoProfileRunning(false);
              setPendingAutoProfile(null);
              doStream();
            },
            onSkip: async () => {
              setPendingAutoProfile(null);
              try { await skipProfilingApi({ session: sessionId }); } catch {}
              doStream();
            },
          });
          pendingSessionRef.current = null;
          setIsSending(false);
          return;
        } else {
          try { await skipProfilingApi({ session: sessionId }); } catch {}
        }
      }
    } catch {}

    setSubmissionStep(t('loading.generating_tests'));
    setValidationStatus('valid');
    navigate(`/models/${sessionId}`);
    dispatch(setCurrentId(sessionId));
    setPendingFirstLoad(true);

    try {
      isGeneratingRef.current = true;
      await dispatch(chatQuery({
        userInput: '',
        sessionId,
        project: '',
        dialect: DIALECT,
        query: sql,
        ChangedMessageId: '',
        t,
        parentMessageId: validateResult?.sql_message_id ?? '',
      })).unwrap?.();
    } catch {}

    pendingSessionRef.current = null;
    setSubmissionStep(null);
    setIsSending(false);
  }, [dispatch, navigate, t]);

  // -------- Submission from SQL file selector
  const handleFileSubmit = useCallback(async () => {
    if (!selectedModelName || isSending) return;

    setIsSending(true);
    setSubmitError(null);
    setValidationStatus('validating');
    setSubmissionStep(t('loading.loading_sql'));

    let testId: string;
    let fileSql: string;
    try {
      const test = await createTestApi(selectedModelName);
      testId = test.test_id;
      fileSql = test.sql;

      // Si des tests existent déjà et qu'on ne force pas une régénération → redirection directe
      if (test.test_cases && test.test_cases.length > 0 && !forceNewRef.current) {
        navigate(`/models/${testId}`);
        setSubmissionStep(null);
        setValidationStatus('idle');
        setIsSending(false);
        return;
      }
    } catch {
      setSubmitError(t('errors.test_creation_failed'));
      setValidationStatus('idle');
      setSubmissionStep(null);
      setIsSending(false);
      return;
    }

    forceNewRef.current = false;
    setSqlQuery(fileSql);
    setModelName(selectedModelName);
    pendingSessionRef.current = testId;

    await runSqlSubmissionFlow(fileSql, testId);
    dispatch(fetchModels());
  }, [selectedModelName, isSending, navigate, t, runSqlSubmissionFlow, dispatch]);

  // -------- First message (SQL required)
  const handleNewChatSubmit = useCallback(async () => {
    if (isSending || !sqlQuery.trim()) return;

    setIsSending(true);
    setSubmitError(null);
    setValidationStatus('validating');

    const newSession: string = pendingSessionRef.current ?? uuidv4();
    if (!pendingSessionRef.current) {
      setSubmissionStep(t('loading.creating_model'));
      try {
        await dispatch(createModel({ name: modelName.trim() || 'nouveau_script', session_id: newSession })).unwrap?.();
        pendingSessionRef.current = newSession;
      } catch {
        dispatch(setError(t('errors.model_creation_failed')));
        setValidationStatus('idle');
        setSubmissionStep(null);
        setIsSending(false);
        return;
      }
    }

    await runSqlSubmissionFlow(sqlQuery, newSession);
  }, [sqlQuery, modelName, dispatch, t, isSending, runSqlSubmissionFlow]);

  // -------- Auto-import missing tables then retry submit
  const handleAutoImport = useCallback(async () => {
    if (!tablesToImport) return;
    setIsImporting(true);
    setImportError(null);
    try {
      await importMissingTablesApi({
        tables_to_import: tablesToImport,
        project: '',
        dialect: DIALECT,
      });
      setMissingTables(null);
      setTablesToImport(null);
      setValidationStatus('idle');
      await handleNewChatSubmit();
    } catch (err: any) {
      const detail = err?.detail;
      if (detail && typeof detail === 'object' && detail.needs_manual_config) {
        setImportError(detail.message || t('errors.unqualified_tables'));
      } else {
        setImportError(typeof detail === 'string' ? detail : t('errors.import_error'));
      }
    } finally {
      setIsImporting(false);
    }
  }, [tablesToImport, handleNewChatSubmit, t]);

  // -------- Silent auto-import when user preference is set
  useEffect(() => {
    if (!tablesToImport) return;
    const globalAuto = localStorage.getItem('autoImport_always') === 'true';
    const projectAuto = currentProjectId
      ? localStorage.getItem(`autoImport_project_${currentProjectId}`) === 'true'
      : false;
    if (globalAuto || projectAuto) {
      handleAutoImport();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tablesToImport]);

  // -------- Follow-up messages
  const handleSendMessage = useCallback(
    async (textParam?: string, testIdx?: number) => {
      if (isSending) return;
      const text = (textParam ?? userInput).trim();
      const effectiveTestIndex = testIdx ?? (selectedTestIndex !== null ? selectedTestIndex : undefined);
      if (!text && effectiveTestIndex === undefined) return;

      setIsSending(true);
      const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
      const lastMessageId = lastMessage ? lastMessage.id : '';

      const routeHint = forcedRouteRef.current;
      forcedRouteRef.current = '';
      const ok = await sendMessage(text, sqlQuery, '', lastMessageId, undefined, false, effectiveTestIndex, undefined, assertionOnly, routeHint || undefined);
      setIsSending(false);

      if (ok) {
        setUserInput('');
        setSelectedTestIndex(null);
        setAssertionOnly(false);
        if (draftKeyRef.current) localStorage.removeItem(draftKeyRef.current);
      }
    },
    [userInput, sqlQuery, renderMessages, selectedChildIndices, sendMessage, isSending, selectedTestIndex, assertionOnly]
  );

  const onSendClick = useCallback(() => {
    handleSendMessage(userInput);
  }, [userInput, handleSendMessage]);

  // -------- SQL bar update (re-run with new SQL)
  const handleSQLUpdate = useCallback(
    async (newSql: string) => {
      if (isSending || !newSql.trim() || !currentModelId) return;
      setIsSending(true);
      setSqlDirty(true);
      setSqlQuery(newSql);

      const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
      const lastMessageId = lastMessage ? lastMessage.id : '';
      const effectiveParentId = restoredMessageId || lastMessageId;
      setRestoredMessageId(undefined);

      let resolvedOptimizedSql = optimizedSql;

      if (skipValidationRef.current) {
        skipValidationRef.current = false;
      } else {
        try {
          const validateResult = await validateQueryApi({
            sql: newSql,
            project: '',
            dialect: DIALECT,
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
              setSubmitError(validateResult?.error || t('errors.invalid_query'));
            }
            setIsSending(false);
            return;
          }
          resolvedOptimizedSql = validateResult.optimized_sql ?? '';
          setOptimizedSql(resolvedOptimizedSql);
        } catch {
          setSubmitError(t('errors.validation_error'));
          setIsSending(false);
          return;
        }
      }
      dispatch(pushSqlHistory({ id: uuidv4(), sql: newSql, optimizedSql: resolvedOptimizedSql, parentMessageId: effectiveParentId }));
      try {
        await dispatch(chatQuery({
          userInput: '',
          sessionId: currentModelId,
          project: '',
          dialect: DIALECT,
          query: newSql,
          ChangedMessageId: '',
          t,
          parentMessageId: effectiveParentId,
          context: 'sql_update',
          silent: true,
        })).unwrap?.();
      } catch {}
      setSqlDirty(false);
      setIsSending(false);
    },
    [isSending, currentModelId, optimizedSql, renderMessages, selectedChildIndices, dispatch, t, restoredMessageId]
  );

  // -------- Ré-évaluer (reload file + rerun)
  const handleReevaluate = useCallback(async () => {
    if (!currentModelPath || isSending) return;
    try {
      const newSql = await fetchModelSql(currentModelPath);
      if (newSql && newSql.trim()) await handleSQLUpdate(newSql);
      else await handleSQLUpdate(sqlQuery);
    } catch {
      await handleSQLUpdate(sqlQuery);
    }
  }, [currentModelPath, isSending, handleSQLUpdate, sqlQuery]);

  // -------- Restore SQL from history
  const handleHistorySelect = useCallback((entry: SqlHistoryEntry) => {
    setSqlQuery(entry.sql);
    setOptimizedSql(entry.optimizedSql);
    skipValidationRef.current = true;
    setHistoryRestoreTrigger((n) => n + 1);
  }, []);

  const handleStopStream = () => stopStream();

  const handleRequestProfile = useCallback(async () => {
    if (!currentModelId || !sqlQuery.trim()) return;
    try {
      const result = await checkProfileApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, used_columns: [] });
      const doStream = () => dispatch(chatQuery({ userInput: '', sessionId: currentModelId, project: '', dialect: DIALECT, query: sqlQuery, ChangedMessageId: '', t, parentMessageId: '' }));
      if (result.profile_error) {
        dispatch(setError(result.profile_error));
        return;
      }
      if (result.profile_complete) {
        doStream();
      } else if (result.profile_request?.profile_query && result.auto_profile_available) {
        const req = result.profile_request;
        setPendingAutoProfile({
          profileRequest: req,
          onConfirm: async () => {
            setIsAutoProfileRunning(true);
            try { await autoProfileApi({ profile_sql: req.profile_query, project: '', session: currentModelId }); } catch {}
            setIsAutoProfileRunning(false);
            setPendingAutoProfile(null);
            doStream();
          },
          onSkip: async () => {
            setPendingAutoProfile(null);
            try { await skipProfilingApi({ session: currentModelId }); } catch {}
            doStream();
          },
        });
      } else {
        doStream();
      }
    } catch (e) {
      console.error('[handleRequestProfile]', e);
      dispatch(setError('Erreur lors de la vérification du profil.'));
    }
  }, [currentModelId, sqlQuery, dispatch, t]);

  const handleRefreshProfile = useCallback(async () => {
    if (!currentModelId || !sqlQuery.trim()) return;
    try {
      const result = await checkProfileApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, used_columns: [], force: true });
      if (result.profile_error) {
        dispatch(setError(result.profile_error));
        return;
      }
      if (result.profile_complete) return; // nothing to profile (no used_columns)
      if (result.profile_request?.profile_query && result.auto_profile_available) {
        const req = result.profile_request;
        setPendingAutoProfile({
          profileRequest: req,
          onConfirm: async () => {
            setIsAutoProfileRunning(true);
            try { await autoProfileApi({ profile_sql: req.profile_query, project: '', session: currentModelId }); } catch {}
            setIsAutoProfileRunning(false);
            setPendingAutoProfile(null);
          },
          onSkip: async () => {
            setPendingAutoProfile(null);
          },
        });
      }
    } catch (e) {
      console.error('[handleRefreshProfile]', e);
      dispatch(setError('Erreur lors du rafraîchissement du profil.'));
    }
  }, [currentModelId, sqlQuery, dispatch]);

  const handleClearHistory = async () => {
    if (!currentModelId) return;
    await clearHistoryApi(currentModelId);
    dispatch(resetMessages());
  };

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
  }, [setAlwaysFix]);

  // Auto-fix
  useEffect(() => {
    if (!alwaysFix || loading !== false || isSending) return;
    const lastMsg = renderMessages[renderMessages.length - 1];
    if (!lastMsg || !('id' in lastMsg) || (lastMsg as any).type === 'group') return;
    const msg = lastMsg as any;
    if (!msg.contents?.error) return;
    if (autoFixedIds.current.has(msg.id)) return;
    autoFixedIds.current.add(msg.id);
    sendMessage(FIX_ERROR_COMMAND, sqlQuery, '', msg.parent ?? msg.id, undefined, false);
  }, [renderMessages, alwaysFix, loading, isSending, sqlQuery, sendMessage]);

  // -------- Browser notification on generation complete
  useEffect(() => {
    if (loading === true && Notification.permission === 'default') {
      Notification.requestPermission();
    }
    if (prevLoadingRef.current === true && loading === false && isGeneratingRef.current) {
      isGeneratingRef.current = false;
      if (Notification.permission === 'granted') {
        const body = error ? t('notifications.generation_failed') : t('notifications.generation_success');
        new Notification('MockSQL', { body, icon: '/favicon.ico' });
      }
    }
    prevLoadingRef.current = loading;
  }, [loading, error, t]);

  const handleAddTest = useCallback(() => {
    setSelectedTestIndex(null);
  }, []);

  const handleSelectTestForModification = useCallback((idx: number) => {
    setAssertionOnly(false);
    setSelectedTestIndex(idx);
  }, []);

  const handleEditAssertions = useCallback((idx: number) => {
    setAssertionOnly(true);
    setSelectedTestIndex(idx);
  }, []);

  const handleRerunTest = useCallback((idx: number) => {
    if (isSending) return;
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const lastMessageId = lastMessage ? lastMessage.id : '';
    dispatch(chatQuery({
      userInput: '',
      sessionId: currentModelId || '',
      project: '',
      dialect: DIALECT,
      query: sqlQuery,
      ChangedMessageId: '',
      t,
      parentMessageId: lastMessageId,
      testIndex: idx,
      forceRoute: 'generator',
      silent: true,
    }));
  }, [isSending, currentModelId, sqlQuery, renderMessages, selectedChildIndices, dispatch, t]);


  return (
    <Container
      sx={{
        height: '100vh',
        width: '100%',
        maxHeight: '100%',
        maxWidth: uiPhase === 'workspace' ? '100vw' : `calc(100vw - ${drawerOpen ? drawerWidth : 0}px)`,
        transition: 'max-width 0.2s ease',
        p: 0,
        display: 'flex',
        flexDirection: 'column',
      }}
    >
      {/* Workspace-mode alerts */}
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
          {missingTables && (
            <Box sx={{ mb: 1 }}>
              <MissingTablesAlert
                missingTables={missingTables}
                projectId={currentProjectId}
                dialect={DIALECT}
                onImport={tablesToImport ? handleAutoImport : undefined}
                importing={isImporting}
                importError={importError}
              />
            </Box>
          )}
        </Box>
      )}

      {/* ── STEP 1: GenerateView (file selector) ── */}
      {uiPhase === 'entry' && (
        <Box sx={{ flex: 1, overflow: 'auto', minHeight: 0, bgcolor: '#dde3e6' }} ref={containerRef}>
          <Box sx={{ maxWidth: 920, mx: 'auto', px: '28px', pt: '32px', pb: '60px' }}>

            {/* Page header */}
            <Box sx={{ display: 'flex', alignItems: 'center', gap: '14px', mb: '24px' }}>
              <Box sx={{ width: 44, height: 44, borderRadius: '12px', bgcolor: '#ecf7f6', color: '#2BB0A8', display: 'grid', placeItems: 'center', flexShrink: 0 }}>
                <ScienceIcon sx={{ fontSize: 24 }} />
              </Box>
              <Box>
                <Typography sx={{ fontSize: 22, fontWeight: 700, letterSpacing: -0.3, color: '#0f272a', lineHeight: 1.2 }}>
                  {t('generate.page_title')}
                </Typography>
                <Typography sx={{ fontSize: 13.5, color: '#6b8287', mt: '3px' }}>
                  {t('generate.page_subtitle')}
                </Typography>
              </Box>
            </Box>

            {/* Step 1 — File list */}
            <Box sx={{ mb: '24px' }}>
              {/* Step label row */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: '10px', mb: '10px' }}>
                <Box sx={{ width: 22, height: 22, borderRadius: '50%', bgcolor: '#2BB0A8', color: '#fff', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700, flexShrink: 0 }}>1</Box>
                <Typography sx={{ fontSize: 14.5, fontWeight: 600, color: '#0f272a' }}>{t('generate.choose_sql_file')}</Typography>
                <Box sx={{ ml: 'auto', display: 'flex', alignItems: 'center', gap: '5px', fontSize: 11.5, color: '#6b8287' }}>
                  <Box component="span" sx={{ fontSize: 11.5 }}>📁</Box>
                  <Typography component="code" sx={{ fontSize: 11, color: '#6b8287', fontFamily: 'monospace' }}>
                    {(() => {
                      const f = sqlFiles[0];
                      if (!f) return t('generate.models_path');
                      const depth = f.name.split('/').length;
                      let p = f.path.replace(/\\/g, '/');
                      for (let i = 0; i < depth; i++) p = p.replace(/\/[^/]+$/, '');
                      return p || t('generate.models_path');
                    })()}
                  </Typography>
                </Box>
              </Box>

              {/* File list panel */}
              <Box sx={{ border: '1px solid #c9d3d6', borderRadius: '12px', bgcolor: '#f3f6f7', overflow: 'hidden' }}>
                {/* Search bar */}
                <Box sx={{ px: '12px', py: '9px', borderBottom: '1px solid #dae2e4', bgcolor: '#dde3e6', display: 'flex', alignItems: 'center', gap: '8px' }}>
                  <TextField
                    size="small"
                    fullWidth
                    placeholder={t('generate.search_model')}
                    value={fileSearch}
                    onChange={e => setFileSearch(e.target.value)}
                    InputProps={{
                      startAdornment: (
                        <InputAdornment position="start">
                          <SearchIcon sx={{ fontSize: 16, color: '#6b8287' }} />
                        </InputAdornment>
                      ),
                      sx: {
                        fontSize: 12.5,
                        bgcolor: '#f3f6f7',
                        borderRadius: '8px',
                        '& fieldset': { borderColor: '#c9d3d6' },
                        '&:hover fieldset': { borderColor: '#2BB0A8' },
                        '&.Mui-focused fieldset': { borderColor: '#2BB0A8' },
                      },
                    }}
                    inputProps={{ sx: { py: '7px', px: '4px' } }}
                  />
                  <Typography sx={{ fontSize: 11, color: '#6b8287', whiteSpace: 'nowrap', flexShrink: 0 }}>
                    {(() => {
                      const count = fileSearch
                        ? sqlFiles.filter(f => (f.name + ' ' + (f.path ?? '')).toLowerCase().includes(fileSearch.toLowerCase())).length
                        : sqlFiles.length;
                      return `${count} fichier${count !== 1 ? 's' : ''}`;
                    })()}
                  </Typography>
                </Box>

                {/* File rows */}
                <Box sx={{ maxHeight: 280, overflow: 'auto' }}>
                  {sqlFiles
                    .filter(f => !fileSearch || (f.name + ' ' + (f.path ?? '')).toLowerCase().includes(fileSearch.toLowerCase()))
                    .map(f => {
                      const isActive = selectedModelName === f.name;
                      return (
                        <Box
                          key={f.name}
                          onClick={() => setSelectedModelName(f.name)}
                          sx={{
                            display: 'grid',
                            gridTemplateColumns: '18px 1fr auto auto',
                            gap: '10px',
                            alignItems: 'center',
                            px: '14px',
                            py: '9px',
                            cursor: 'pointer',
                            borderBottom: '1px solid #dae2e4',
                            bgcolor: isActive ? '#ecf7f6' : 'transparent',
                            transition: 'background .1s',
                            '&:hover': { bgcolor: isActive ? '#ecf7f6' : '#eef2f3' },
                            '&:last-child': { borderBottom: 'none' },
                          }}
                        >
                          {/* SQL icon */}
                          <Box sx={{ color: isActive ? '#2BB0A8' : '#6b8287', display: 'flex', alignItems: 'center' }}>
                            <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
                              <path d="M8 9l-3 3 3 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
                              <path d="M16 9l3 3-3 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
                            </svg>
                          </Box>
                          {/* Name + path */}
                          <Box sx={{ minWidth: 0 }}>
                            <Typography sx={{ fontSize: 13, fontWeight: isActive ? 600 : 500, color: '#0f272a', lineHeight: 1.3 }} noWrap>
                              {f.name}
                            </Typography>
                            <Typography sx={{ fontSize: 10.5, color: '#6b8287', mt: '1px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                              {f.path ?? f.name}
                            </Typography>
                          </Box>
                          {/* Last modified */}
                          <Typography sx={{ fontSize: 11, color: '#6b8287', textAlign: 'right', minWidth: 90, flexShrink: 0 }}>
                            {f.updated_at ? `modifié ${relativeDate(f.updated_at, t)}` : ''}
                          </Typography>
                        </Box>
                      );
                    })}
                  {sqlFiles.filter(f => !fileSearch || (f.name + ' ' + (f.path ?? '')).toLowerCase().includes(fileSearch.toLowerCase())).length === 0 && (
                    <Box sx={{ p: '24px 14px', textAlign: 'center' }}>
                      <Typography sx={{ fontSize: 12.5, color: '#6b8287' }}>{t('search.no_results')}</Typography>
                    </Box>
                  )}
                </Box>
              </Box>
            </Box>

            {/* Step 2 — SQL preview */}
            {selectedModelName && (
              <Box sx={{ mb: '24px' }}>
                <Box sx={{ display: 'flex', alignItems: 'center', gap: '10px', mb: '10px' }}>
                  <Box sx={{ width: 22, height: 22, borderRadius: '50%', bgcolor: '#2BB0A8', color: '#fff', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700, flexShrink: 0 }}>2</Box>
                  <Typography sx={{ fontSize: 14.5, fontWeight: 600, color: '#0f272a' }}>Aperçu du SQL</Typography>
                  <Typography sx={{ fontSize: 11.5, color: '#6b8287' }}>
                    · <Box component="code" sx={{ fontSize: 11.5, color: '#3b5357', fontFamily: 'monospace' }}>{selectedModelName}</Box>
                  </Typography>
                  <Box sx={{ ml: 'auto', fontSize: 10.5, px: '8px', py: '2px', borderRadius: 999, bgcolor: '#dde3e6', border: '1px solid #c9d3d6', color: '#6b8287', fontWeight: 500 }}>
                    Lecture seule
                  </Box>
                </Box>
                <Box sx={{ border: '1px solid #c9d3d6', borderRadius: '12px', overflow: 'hidden', bgcolor: '#f3f6f7' }}>
                  {/* SQL header bar */}
                  <Box sx={{ display: 'flex', alignItems: 'center', gap: '8px', px: '13px', py: '9px', bgcolor: '#eef5f4', borderBottom: '1px solid #c9d3d6' }}>
                    <Box sx={{ color: '#16746e', display: 'flex' }}>
                      <svg width="14" height="14" viewBox="0 0 24 24" fill="none">
                        <path d="M8 9l-3 3 3 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
                        <path d="M16 9l3 3-3 3" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round"/>
                      </svg>
                    </Box>
                    <Typography sx={{ fontSize: 11.5, fontWeight: 700, color: '#16746e', letterSpacing: 0.6, fontFamily: 'monospace' }}>SQL</Typography>
                    <Typography sx={{ ml: 'auto', fontSize: 11.5, color: '#6b8287' }}>
                      {DIALECT.charAt(0).toUpperCase() + DIALECT.slice(1)} · {previewSql ? `${previewSql.split('\n').length} lignes` : '…'}
                    </Typography>
                  </Box>
                  <Box sx={{ opacity: previewLoading ? 0.5 : 1, transition: 'opacity .15s' }}>
                    {previewSql !== null ? (
                      <SqlEditor value={previewSql} readOnly maxHeight={300} fontSize={12.5} minHeight={60} background="transparent" />
                    ) : (
                      <Box sx={{ p: '14px', display: 'flex', alignItems: 'center', gap: 1 }}>
                        <LinearProgress sx={{ flex: 1, height: 4, borderRadius: 2, bgcolor: '#e0f7f5', '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' } }} />
                      </Box>
                    )}
                  </Box>
                </Box>
              </Box>
            )}

            {/* Action bar */}
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '12px', mt: selectedModelName ? 0 : '18px' }}>
              <Box sx={{ display: 'flex', alignItems: 'center', gap: '5px', fontSize: 11.5, color: '#6b8287' }}>
                <Box component="span" sx={{ width: 8, height: 8, borderRadius: '50%', bgcolor: '#f7c948', display: 'inline-block', mr: '2px' }} />
                <Typography sx={{ fontSize: 11.5, color: '#6b8287' }}>Exécuté sur DuckDB en local — zéro coût {DIALECT.charAt(0).toUpperCase() + DIALECT.slice(1)}</Typography>
              </Box>
              <Button
                variant="contained"
                disabled={!selectedModelName || isSending}
                onClick={handleFileSubmit}
                startIcon={isSending ? undefined : <AutoAwesomeIcon sx={{ fontSize: 15 }} />}
                sx={{
                  bgcolor: !selectedModelName || isSending ? '#cbd9da' : '#2BB0A8',
                  '&:hover': { bgcolor: '#1f948d' },
                  '&:disabled': { bgcolor: '#cbd9da', color: '#fff' },
                  textTransform: 'none',
                  borderRadius: '10px',
                  px: '22px',
                  py: '11px',
                  fontSize: 14,
                  fontWeight: 600,
                  minWidth: 180,
                  boxShadow: '0 2px 0 rgba(22,116,110,.2)',
                }}
              >
                {isSending ? t('generate.generating') : t('generate.generate_tests')}
              </Button>
            </Box>

            {/* Feedback zone — loading bar, errors, import (below the button) */}
            {(isSending && submissionStep) && (
              <Box sx={{ mt: 3 }}>
                <LinearProgress variant="indeterminate" sx={{ height: 6, borderRadius: 3, backgroundColor: '#e0f7f5', '& .MuiLinearProgress-bar': { backgroundColor: '#1ca8a4' } }} />
                <Typography variant="body2" sx={{ mt: 0.75, color: '#555', textAlign: 'center' }}>{submissionStep}</Typography>
              </Box>
            )}
            {submitError && (
              <Alert severity="error" sx={{ borderRadius: '12px', mt: 2 }} onClose={() => setSubmitError(null)}>
                {submitError}
              </Alert>
            )}
            {missingTables && (
              <Box sx={{ mt: 2 }}>
                <MissingTablesAlert
                  missingTables={missingTables}
                  projectId={currentProjectId}
                  dialect={DIALECT}
                  onImport={tablesToImport ? handleAutoImport : undefined}
                  importing={isImporting}
                  importError={importError}
                />
              </Box>
            )}

          </Box>
        </Box>
      )}

      {/* ── STEP 2: Workspace ── */}
      {uiPhase === 'workspace' && (
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'row', minHeight: 0, overflow: 'hidden' }}>

          {/* Chat column — permanent left panel */}
          <ChatColumn
            fileName={currentModelName ? `${currentModelName}.sql` : 'requête.sql'}
            onChangeFile={() => {
              dispatch(resetContext());
              dispatch(setCurrentId(''));
              navigate('/');
            }}
            selectedTestIndex={selectedTestIndex}
            assertionOnly={assertionOnly}
            onClearAnchor={() => { setSelectedTestIndex(null); setAssertionOnly(false); }}
            renderMessages={renderMessages as any}
            userInput={userInput}
            setUserInput={setUserInput}
            onSend={onSendClick}
            isSending={isSending}
            loading={loading}
            loading_message={loading_message}
            error={error}
            alwaysFix={alwaysFix}
            onAlwaysFixChange={handleAlwaysFixChange}
            sqlHistory={sqlHistory}
            onSqlRestore={handleHistorySelect}
            onRestoreState={handleRestoreState}
            restoredMessageId={restoredMessageId}
            streamingReasoning={streamingReasoning}
            onStopStream={handleStopStream}
            sendMessage={sendMessage}
            sqlQuery={sqlQuery}
            onClearHistory={handleClearHistory}
            onRequestProfile={handleRequestProfile}
          />

          {/* Main area — tests + footer */}
          <Box sx={{ flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0 }}>
            <ArtefactHeader
              testCount={testResults?.length ?? 0}
              onRerun={handleReevaluate}
              rerunning={!!(loading || isSending)}
              sqlDirty={sqlDirty}
              onRefreshProfile={handleRefreshProfile}
            />
            <Box sx={{ flex: 1, position: 'relative', display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
              <TestsPanel
                onAddTest={handleAddTest}
                onSelectForModification={handleSelectTestForModification}
                onEditAssertions={handleEditAssertions}
                onRerunTest={handleRerunTest}
                onSuggestionClick={(text) => setUserInput(text)}
                selectedTestIndex={selectedTestIndex}
                sqlProps={{
                  sql: sqlQuery,
                  onUpdate: handleSQLUpdate,
                  optimizedSql,
                  sqlHistory,
                  onHistorySelect: handleHistorySelect,
                  historyRestoreTrigger,
                  disabled: isSending,
                  loading: isSending,
                  hasError: lastMsgHasError,
                  sqlFileName: currentModelName || undefined,
                  onReloadFile: currentModelPath
                    ? async () => {
                        try { return await fetchModelSql(currentModelPath); }
                        catch { return null; }
                      }
                    : undefined,
                }}
                staleInfo={currentModel?.isStale ? {
                  isStale: true,
                  commitsSince: currentModel.commitsSince ?? 0,
                  lastTestedAt: currentModel.updateDate,
                  onReevaluate: handleReevaluate,
                  currentSql: sqlQuery,
                  onFetchNewSql: currentModelPath
                    ? async () => {
                        try { return await fetchModelSql(currentModelPath); }
                        catch { return null; }
                      }
                    : undefined,
                } : undefined}
                onUpload={(uploadedData) => {
                  const lastMsg = renderMessages[renderMessages.length - 1] as any;
                  sendMessage('', sqlQuery, lastMsg?.id, lastMsg?.id, uploadedData, false);
                }}
                onOpenChat={() => { setSelectedTestIndex(null); }}
              />
            </Box>
            <DuckDBFooter />
          </Box>
        </Box>
      )}

      {/* ── Auto-profiling confirmation dialog ── */}
      {pendingAutoProfile && (
        <Dialog open maxWidth="sm" fullWidth>
          <DialogTitle sx={{ fontWeight: 700 }}>{t('profiling.title')}</DialogTitle>
          <DialogContent>
            {/* Validated badge */}
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
                {t('profiling.query_validated')}
              </Typography>
            </Box>

            {/* Sans / Avec comparison */}
            <Box sx={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 1.5, mb: 2 }}>
              <Box sx={{ bgcolor: '#fafafa', border: '1px solid #e8e8e8', borderRadius: 2, p: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 700, color: '#bbb', display: 'block', mb: 1.5, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                  {t('profiling.without_label')}
                </Typography>
                {(['without_item_1', 'without_item_2', 'without_item_3'] as const).map((key) => (
                  <Typography key={key} variant="body2" sx={{ color: '#ccc', fontSize: 12.5, mb: 0.75 }}>
                    — {t(`profiling.${key}`)}
                  </Typography>
                ))}
              </Box>
              <Box sx={{ bgcolor: '#f0faf5', border: '1px solid #a5d6b7', borderRadius: 2, p: 2 }}>
                <Typography variant="caption" sx={{ fontWeight: 700, color: '#1ca8a4', display: 'block', mb: 1.5, textTransform: 'uppercase', letterSpacing: 0.5 }}>
                  {t('profiling.with_label')}
                </Typography>
                {(['with_item_1', 'with_item_2', 'with_item_3'] as const).map((key) => (
                  <Typography key={key} variant="body2" sx={{ color: '#1e5c38', fontSize: 12.5, mb: 0.75, fontWeight: 500 }}>
                    ✓ {t(`profiling.${key}`)}
                  </Typography>
                ))}
              </Box>
            </Box>

            {/* Billing — inline, non-obtrusive */}
            {pendingAutoProfile.profileRequest.billing_tb !== undefined && (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                <Typography variant="caption" sx={{ color: '#aaa' }}>Scan estimé :</Typography>
                <Chip
                  label={`~${pendingAutoProfile.profileRequest.billing_tb < 0.001
                    ? '< 0,001'
                    : pendingAutoProfile.profileRequest.billing_tb.toFixed(3)} To · ${DIALECT.charAt(0).toUpperCase() + DIALECT.slice(1)}`}
                  size="small"
                  sx={{ bgcolor: '#f5f5f5', color: '#888', border: '1px solid #e0e0e0', fontWeight: 600, fontSize: 11 }}
                />
              </Box>
            )}

            {isAutoProfileRunning && (
              <Box sx={{ mt: 1, pb: 1 }}>
                <LinearProgress
                  variant="indeterminate"
                  sx={{ height: 6, borderRadius: 3, bgcolor: '#e0f7f5', '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' } }}
                />
                <Typography variant="caption" sx={{ color: '#555', mt: 0.5, display: 'block' }}>
                  {t('loading.profiling')}
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
              {t('profiling.skip_label')}
            </Button>
            <Button
              variant="contained"
              onClick={pendingAutoProfile.onConfirm}
              disabled={isAutoProfileRunning}
              sx={{ textTransform: 'none', bgcolor: '#1ca8a4', '&:hover': { bgcolor: '#159e9a' } }}
            >
              {isAutoProfileRunning ? t('loading.profiling_short') : t('action.run_profiling')}
            </Button>
          </DialogActions>
        </Dialog>
      )}
    </Container>
  );
};

export default React.memo(ChatComponent);
