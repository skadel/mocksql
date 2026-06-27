import React, { useCallback, useMemo, useRef, useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { useTranslation } from 'react-i18next';
import { useLocation, useNavigate, useParams } from 'react-router-dom';
import { v4 as uuidv4 } from 'uuid';
import { throttle } from 'lodash';
import { Alert, Box, Button, Chip, CircularProgress, Dialog, DialogActions, DialogContent, DialogTitle, IconButton, InputAdornment, LinearProgress, TextField, ToggleButton, ToggleButtonGroup, Tooltip, Typography } from '@mui/material';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import RefreshRoundedIcon from '@mui/icons-material/RefreshRounded';
import { isStaleSchemaError } from '../../../utils/staleSchema';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import DownloadIcon from '@mui/icons-material/Download';
import SearchIcon from '@mui/icons-material/Search';
import ScienceIcon from '@mui/icons-material/Science';
import AccessTimeIcon from '@mui/icons-material/AccessTime';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import { Container } from '../../../style/StyledComponents';
import { getLastMessage } from '../../../utils/messages';
import MissingTablesAlert from './MissingTablesAlert';
import TestsPanel from './TestsPanel';
import DuckDBFooter from './DuckDBFooter';
import ChatColumn from './ChatColumn';
import SubmissionProgress from './SubmissionProgress';
import ArtefactHeader from './ArtefactHeader';
import { drawerWidth } from '../../appBar/components/DrawerComponent';
import { createModel, createTestApi, fetchModelSql, fetchModels } from '../../../api/models';
import SqlEditor from '../../../shared/SqlEditor';
import { chatQuery, stopStream, validateQueryApi, checkProfileApi, buildProfileRequestApi, skipProfilingApi, importMissingTablesApi, autoProfileApi, refreshSchemasApi, getProfileMetaApi } from '../../../api/query';
import { useLocalStorageState } from '../../../hooks/useLocalStorageState';
import { useSqlFileLoader } from '../hooks/useSqlFileLoader';
import { FIX_ERROR_COMMAND } from '../constants';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { setCurrentId } from '../../appBar/appBarSlice';
import { setError, setQueryComponentGraph, setQuery, setOptimizedQuery, setTestResults, dismissSuggestion, dismissDescriptionProposal, pushSqlHistory, setRestoredMessageId as setRestoredMessageIdAction, setWorkspaceMode, resetContext, resetMessages, setLoadingMessage } from '../buildModelSlice';
import { getMessages, patchModelSql, clearHistoryApi, dismissSuggestionApi } from '../../../api/messages';
import { getRenderMessages } from '../../../selectors/getRenderMessages';
import { ChatQueryParams, ProfileRequest, SqlHistoryEntry } from '../../../utils/types';
import { relativeDate } from '../../../utils/dates';

// Dialect is read from the current project — fallback to bigquery for backward compat.

// ETA grosse-maille de la durée de génération. La génération fabrique des données
// synthétiques colonne par colonne : plus il y a de colonnes (× le nombre de tests
// demandés), plus c'est long. Formule : arrondi( (nb_colonnes / COLS_PER_MINUTE) × nb_tests ),
// plancher 1 min. COLS_PER_MINUTE est le seul knob à ajuster si l'estimation dérive.
const COLS_PER_MINUTE = 10;

// Compte les colonnes utilisées toutes tables confondues. `used_columns` est une liste
// d'entrées { ..., used_columns: [] } (string JSON ou objet selon la source).
const countUsedColumns = (usedColumns: unknown[] | undefined): number =>
  (usedColumns ?? []).reduce<number>((sum, uc) => {
    const obj = typeof uc === 'string' ? (() => { try { return JSON.parse(uc); } catch { return null; } })() : uc;
    return sum + (Array.isArray((obj as any)?.used_columns) ? (obj as any).used_columns.length : 0);
  }, 0);

const estimateGenerationMinutes = (nUsedCols: number, testsTarget: number): number =>
  Math.max(1, Math.round((nUsedCols / COLS_PER_MINUTE) * Math.max(1, testsTarget)));

const ChatComponent: React.FC = () => {
  const { t } = useTranslation();
  const dispatch = useAppDispatch();
  const navigate = useNavigate();
  const location = useLocation();
  const params = useParams();

  const [sqlQuery, setSqlQuery] = useState('');
  const [modelName, setModelName] = useState('');
  const [optimizedSql, setOptimizedSql] = useState('');
  const [restoredMessageId, setRestoredMessageId] = useState<string | undefined>(undefined);
  const [isSending, setIsSending] = useState(false);
  const [selectedTestIndex, setSelectedTestIndex] = useState<number | null>(null);
  const [addTestTrigger, setAddTestTrigger] = useState(0);
  const [sqlDirty, setSqlDirty] = useState(false);
  const [pendingFirstLoad, setPendingFirstLoad] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);
  const [lastErrorDismissed, setLastErrorDismissed] = useState(false);
  const [missingTables, setMissingTables] = useState<string[] | null>(null);
  const [tablesToImport, setTablesToImport] = useState<string[] | null>(null);
  const [isImporting, setIsImporting] = useState(false);
  const [importError, setImportError] = useState<string | null>(null);
  // Wizard du popup : étape 'count' (choix du nombre de tests, toujours en premier) puis,
  // si le profil manque (needsProfiling), étape 'profiling'. onConfirm/onSkip reçoivent le
  // nombre de tests choisi (lu au moment du clic, pas figé à la création du closure).
  const [pendingAutoProfile, setPendingAutoProfile] = useState<{
    profileRequest: ProfileRequest | null;
    needsProfiling: boolean;
    step: 'count' | 'profiling';
    // Nombre de colonnes utilisées (toutes tables confondues) — sert à estimer
    // grosse-maille la durée de génération dans le popup : plus il y a de colonnes
    // à fabriquer, plus c'est long. Cf. estimateGenerationMinutes().
    nUsedCols?: number;
    // Budget de scan configuré côté projet (mocksql.yml / env). undefined/null =>
    // non configuré : on demande la valeur à l'utilisateur (champ "Budget de scan").
    configBudget?: number | null;
    onConfirm: (testsTarget: number) => Promise<void>;
    onSkip: (testsTarget: number) => Promise<void>;
    onCancel: () => void;
  } | null>(null);
  // Nombre de tests à générer d'emblée (1–3, total). Réinitialisé à 1 à chaque ouverture.
  const [testsTarget, setTestsTarget] = useState(1);
  // Budget de scan (To) choisi par l'utilisateur quand aucun n'est configuré.
  // Mémorisé en localStorage (défaut 0.3 To) — au-delà, les tables sont profilées
  // à la demande ("profil partiel"). Cf. flux budget-aware du profiling.
  const [budgetTarget, setBudgetTarget] = useLocalStorageState('profileBudgetTb', 0.3);
  const budgetTargetRef = useRef(budgetTarget);
  useEffect(() => { budgetTargetRef.current = budgetTarget; }, [budgetTarget]);
  // Texte brut du champ budget : découplé de la valeur numérique committée pour
  // autoriser les états intermédiaires ("", "0.") sans que React ne les écrase.
  const [budgetInput, setBudgetInput] = useState(String(budgetTarget));
  // Profil partiel : tables différées (au-dessus du budget) que l'utilisateur peut
  // compléter à la demande via "Compléter le profil". null = profil complet.
  const [partialProfile, setPartialProfile] = useState<{
    deferred: Array<{ scope: string; billing_tb: number }>;
    budget: number | null;
    sql: string;
    usedColumns: any[];
    sessionId: string;
  } | null>(null);
  const [isAutoProfileRunning, setIsAutoProfileRunning] = useState(false);
  // Timestamp ISO du dernier profilage (profil global, partagé entre modèles) — affiché
  // sous forme « profilé il y a N j » à côté du bouton Rafraîchir pour donner une raison
  // concrète de cliquer.
  const [profiledAt, setProfiledAt] = useState<string | null>(null);
  const [autoProfileWarning, setAutoProfileWarning] = useState<{
    status: 'partial' | 'failed';
    errors: Array<{ query_index: number; error: string }>;
  } | null>(null);
   
  const [_validationStatus, setValidationStatus] = useState<'idle' | 'validating' | 'valid' | 'error'>('idle');
  // Tables/colonnes extraites par `validate`, affichées dans la checklist vivante pendant l'attente.
  const [understandingDraft, setUnderstandingDraft] = useState<
    Array<{ database?: string; table: string; columns: string[] }> | null
  >(null);
  // Durée du dry-run de validation (ms), affichée « Requête validée en X s » dans la checklist.
  const [validationMs, setValidationMs] = useState<number | null>(null);
  const [submissionStep, setSubmissionStep] = useState<string | null>(null);
  const [alwaysFix, setAlwaysFix] = useLocalStorageState('alwaysFix', false);
  const [selectedModelName, setSelectedModelName] = useState<string | null>(null);
  const [previewSql, setPreviewSql] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const sqlFiles = useSqlFileLoader();
  const [fileSearch, setFileSearch] = useState('');
  // v15 mode toggle. Integration mode is a stub: the tab is visible with a
  // "Bientôt" badge but does not change the flow.
  const [genMode, setGenMode] = useState<'unit' | 'integration'>('unit');

  const [historyRestoreTrigger, setHistoryRestoreTrigger] = useState(0);
  const [sqlCollapseSignal, setSqlCollapseSignal] = useState(0);
  const [assertionOnly, setAssertionOnly] = useState(false);
  const [pendingFileSql, setPendingFileSql] = useState<string | null>(null);
  // Messages tapés pendant qu'une génération est en cours : mis en file côté front,
  // affichés en « en attente » dans le fil, puis traités automatiquement et dans l'ordre
  // une fois le run terminé. Jamais injectés dans le run en cours. queuedRef = source de
  // vérité pour le drainage (pas de closure périmée dans l'effet) ; queuedMessages = miroir
  // pour le rendu.
  const queuedRef = useRef<{ id: string; text: string }[]>([]);
  const [queuedMessages, setQueuedMessages] = useState<{ id: string; text: string }[]>([]);
  const skipValidationRef = useRef(false);
  const forceNewRef = useRef(false);
  const [demoZoom, setDemoZoom] = useState<'chat' | 'tests' | null>(null);
  const [demoTransform, setDemoTransform] = useState<{ chat: string; tests: string }>({ chat: 'scale(1)', tests: 'scale(1)' });

  useEffect(() => {
    if (!demoZoom) { setDemoTransform({ chat: 'scale(1)', tests: 'scale(1)' }); return; }
    const testId = demoZoom === 'chat' ? 'demo-zoom-chat' : 'demo-zoom-tests';
    const el = document.querySelector(`[data-testid="${testId}"]`) as HTMLElement | null;
    if (!el) return;
    const rect = el.getBoundingClientRect();
    const S = demoZoom === 'chat' ? 1.5 : 1.3;
    const tx = Math.round(window.innerWidth / 2 - (rect.left + rect.width / 2));
    const ty = Math.round(window.innerHeight / 2 - (rect.top + rect.height / 2));
    setDemoTransform(prev => ({ ...prev, [demoZoom]: `translate(${tx}px, ${ty}px) scale(${S})` }));
  }, [demoZoom]);

  useEffect(() => {
    if (import.meta.env.VITE_DEMO_MODE !== 'true') return;
    (window as any).__demoZoom = (target: 'chat' | 'tests' | null) => setDemoZoom(target);
    const handleDemoKey = (e: KeyboardEvent) => {
      const el = e.target as HTMLElement;
      if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.isContentEditable) return;
      if (e.key === 'c' || e.key === 'C') setDemoZoom(z => z === 'chat' ? null : 'chat');
      else if (e.key === 't' || e.key === 'T') setDemoZoom(z => z === 'tests' ? null : 'tests');
      else if (e.key === 'Escape') setDemoZoom(null);
    };
    window.addEventListener('keydown', handleDemoKey);
    return () => {
      delete (window as any).__demoZoom;
      window.removeEventListener('keydown', handleDemoKey);
    };
  }, []);

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
  const lastChatQueryArgsRef = useRef<ChatQueryParams | null>(null);

  const {
    queryComponentGraph: messages,
    loading,
    loading_message,
    streamingReasoning,
    lastReasoning,
    error,
    selectedChildIndices,
    query: storedQuery,
    optimizedQuery: storedOptimizedQuery,
    sqlHistory,
    restoredMessageId: storedRestoredMessageId,
    lastError,
    testResults,
    retryBadDataTestIndex,
    testsTarget: storedTestsTarget,
  } = useAppSelector((state) => state.buildModel);

  const messagesRef = useRef(messages);
  const { currentModelId, drawerOpen, models: allModels } = useAppSelector((state) => state.appBarModel);
  const currentProjectId = useAppSelector((state) => state.appBarModel.currentProjectId);
  const currentProject = useAppSelector((state) => state.appBarModel.currentProject);
  const DIALECT = currentProject?.dialect ?? 'bigquery';

  const isRetryableError = (msg: string) =>
    msg.includes('Connexion perdue') || msg.includes('connexion réseau');

  const dispatchChatQuery = useCallback((args: ChatQueryParams) => {
    lastChatQueryArgsRef.current = args;
    return dispatch(chatQuery(args));
  }, [dispatch]);

  const currentModel = useMemo(
    () => allModels.find(m => m.session_id === currentModelId),
    [allModels, currentModelId],
  );
  const currentModelName = currentModel?.name ?? '';
  const currentModelPath = currentModel
    ? (currentModel.folder ? `${currentModel.folder}/${currentModel.name}` : currentModel.name)
    : '';

  messagesRef.current = messages;

  // -------- Filesystem change detection (focus + 60s polling)
  useEffect(() => {
    if (!currentModelPath || !sqlQuery) return;
    const path = currentModelPath;
    const sql = sqlQuery;

    async function check() {
      if (isSending) return;
      try {
        const fileSql = await fetchModelSql(path);
        if (fileSql && fileSql.trim() !== sql.trim()) {
          setPendingFileSql(fileSql);
        }
      } catch { /* ignore */ }
    }

    const interval = setInterval(check, 60_000);
    const onFocus = () => check();
    const onVisibility = () => { if (document.visibilityState === 'visible') check(); };

    window.addEventListener('focus', onFocus);
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      clearInterval(interval);
      window.removeEventListener('focus', onFocus);
      document.removeEventListener('visibilitychange', onVisibility);
    };
   
  }, [currentModelPath, sqlQuery, isSending]);

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
      // Bannières liées au profil : appartiennent au modèle quitté, sinon elles
      // restent affichées (et actionnables) sur le modèle suivant.
      setPartialProfile(null);
      setAutoProfileWarning(null);
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
    if (testResults && testResults.length > 0) {
      setSqlCollapseSignal(n => n + 1);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [loading]);

  // -------- Reset validation state when user edits SQL
   
  useEffect(() => { setValidationStatus('idle'); setSubmitError(null); setMissingTables(null); setTablesToImport(null); setUnderstandingDraft(null); setValidationMs(null); }, [sqlQuery]);

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
      isAssertionOnly?: boolean,
      forceRoute?: string,
      testUid?: string
    ): Promise<boolean> => {
      const text = (input ?? '').trim();
      if (!text && !userTables && !currentSqlQuery) return false;

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
        await dispatchChatQuery({
          userInput: text,
          sessionId: session,
          project: '',
          dialect: DIALECT,
          query: currentSqlQuery || undefined,
          ChangedMessageId: messageId,
          t,
          parentMessageId,
          userTables,
          testUid,
          testIndex,
          assertionOnly: isAssertionOnly,
          forceRoute,
        }).unwrap?.();
        return true;
      } catch {
        return false;
      }
    },
    [currentModelId, modelName, dispatch, t]
  );

  // -------- Profilage budgété (best-effort) : construit la requête de profiling
  // sous le budget donné, exécute les requêtes qui tiennent sous le budget, et
  // mémorise les tables différées pour proposer "Compléter le profil".
  // budget=null => aucun budget (on profile tout). Ne lève jamais.
  const runBudgetedProfiling = useCallback(async (args: {
    sessionId: string; sql: string; usedColumns: any[]; missingColumns: any[]; budget: number | null;
  }) => {
    const { sessionId, sql, usedColumns, missingColumns, budget } = args;
    setIsAutoProfileRunning(true);
    try {
      const { profile_request } = await buildProfileRequestApi({
        sql, project: '', dialect: DIALECT, session: sessionId,
        missing_columns: missingColumns,
        budget_tb: budget ?? undefined,
      });
      if (profile_request.profile_queries && profile_request.profile_queries.length) {
        const result = await autoProfileApi({
          profile_sql: profile_request.profile_query,
          profile_queries: profile_request.profile_queries,
          project: '', session: sessionId,
          partition_limit: profile_request.partition_limit,
        });
        if (result.profile_status !== 'complete') {
          setAutoProfileWarning({ status: result.profile_status, errors: result.errors ?? [] });
        }
      }
      if (profile_request.deferred && profile_request.deferred.length) {
        setPartialProfile({
          deferred: profile_request.deferred,
          budget: profile_request.budget_tb ?? budget,
          sql, usedColumns, sessionId,
        });
      } else {
        setPartialProfile(null);
      }
    } catch { /* profilage best-effort */ }
    setIsAutoProfileRunning(false);
  }, []);

  // -------- "Compléter le profil" : reprofile les tables différées SANS budget
  // (l'utilisateur accepte explicitement le scan complet). Les colonnes différées
  // sont toujours "manquantes" dans le profil → check-profile les redonne.
  const handleCompleteProfile = useCallback(async () => {
    const pp = partialProfile;
    if (!pp) return;
    setIsAutoProfileRunning(true);
    try {
      const r = await checkProfileApi({ sql: pp.sql, project: '', dialect: DIALECT, session: pp.sessionId, used_columns: pp.usedColumns });
      if (!r.profile_complete && r.missing_columns?.length) {
        const { profile_request } = await buildProfileRequestApi({
          sql: pp.sql, project: '', dialect: DIALECT, session: pp.sessionId,
          missing_columns: r.missing_columns, // pas de budget_tb => profile tout
        });
        if (profile_request.profile_queries?.length) {
          const result = await autoProfileApi({
            profile_sql: profile_request.profile_query,
            profile_queries: profile_request.profile_queries,
            project: '', session: pp.sessionId,
            partition_limit: profile_request.partition_limit,
          });
          if (result.profile_status !== 'complete') {
            setAutoProfileWarning({ status: result.profile_status, errors: result.errors ?? [] });
          }
        }
      }
      setPartialProfile(null);
      getProfileMetaApi().then((m) => setProfiledAt(m.profiled_at)).catch(() => {});
    } catch { /* best-effort */ }
    setIsAutoProfileRunning(false);
  }, [partialProfile]);

  // -------- Shared validate → profile → generate flow
  const runSqlSubmissionFlow = useCallback(async (sql: string, sessionId: string) => {
    setSubmissionStep(t('loading.validating_sql'));
    const validateStart = performance.now();
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

    setValidationMs(performance.now() - validateStart);
    setSubmissionStep(t('loading.checking_profiling'));
    const usedColumns = validateResult.used_columns ?? [];
    setUnderstandingDraft(
      usedColumns
        .map((c: any) => {
          const obj = typeof c === 'string' ? (() => { try { return JSON.parse(c); } catch { return null; } })() : c;
          if (!obj || !obj.table) return null;
          return { database: obj.database, table: obj.table, columns: obj.used_columns ?? [] };
        })
        .filter(Boolean) as Array<{ database?: string; table: string; columns: string[] }>
    );
    try {
      const profileResult = await checkProfileApi({ sql, project: '', dialect: DIALECT, session: sessionId, used_columns: usedColumns });
      if (!profileResult.profile_complete && profileResult.auto_profile_available && profileResult.missing_columns?.length) {
        setValidationStatus('valid');
        navigate(`/models/${sessionId}`);
        dispatch(setCurrentId(sessionId));
        const doStream = (testsTargetN: number) => {
          setPendingFirstLoad(true);
          isGeneratingRef.current = true;
          dispatchChatQuery({ userInput: '', sessionId, project: '', dialect: DIALECT, query: sql, ChangedMessageId: '', t, parentMessageId: '', testsTarget: testsTargetN });
        };
        // Budget de scan : si configuré côté projet on l'applique d'office (zéro
        // friction) ; sinon on demande la valeur dans le popup (défaut 0.3 To).
        const configBudget = profileResult.profile_budget_tb ?? null;
        const missingCols = profileResult.missing_columns;
        setTestsTarget(1);
        setPendingAutoProfile({
          profileRequest: null,
          needsProfiling: true,
          step: 'count',
          nUsedCols: countUsedColumns(usedColumns),
          configBudget,
          // One-click : profilage budgété (best-effort) PUIS génération, sans
          // étape intermédiaire ni clic manuel sur « Lancer le profiling ».
          onConfirm: async (n) => {
            const budget = configBudget ?? budgetTargetRef.current ?? null;
            await runBudgetedProfiling({ sessionId, sql, usedColumns, missingColumns: missingCols, budget });
            setPendingAutoProfile(null);
            doStream(n);
          },
          onSkip: async (n) => {
            setPendingAutoProfile(null);
            try { await skipProfilingApi({ session: sessionId }); } catch { /* skip best-effort */ }
            doStream(n);
          },
          onCancel: () => {
            setPendingAutoProfile(null);
            dispatch(resetContext());
            dispatch(setCurrentId(''));
            navigate('/');
          },
        });
        pendingSessionRef.current = null;
        setIsSending(false);
        return;
      } else if (!profileResult.profile_complete) {
        try { await skipProfilingApi({ session: sessionId }); } catch { /* skip best-effort */ }
      }
    } catch { /* profilage best-effort */ }

    // Génération effective, paramétrée par le nombre de tests choisi dans le popup.
    const runGeneration = async (testsTargetN: number) => {
      setSubmissionStep(t('loading.generating_tests'));
      navigate(`/models/${sessionId}`);
      dispatch(setCurrentId(sessionId));
      setPendingFirstLoad(true);
      try {
        isGeneratingRef.current = true;
        await dispatchChatQuery({
          userInput: '',
          sessionId,
          project: '',
          dialect: DIALECT,
          query: sql,
          ChangedMessageId: '',
          t,
          parentMessageId: validateResult?.sql_message_id ?? '',
          testsTarget: testsTargetN,
        }).unwrap?.();
      } catch { /* génération best-effort */ }
      pendingSessionRef.current = null;
      setSubmissionStep(null);
      setIsSending(false);
    };

    // Profil complet ou non auto-profilable : pas d'étape profiling, mais on demande tout de
    // même le nombre de tests via un popup à une seule étape avant de générer.
    setValidationStatus('valid');
    setTestsTarget(1);
    setPendingAutoProfile({
      profileRequest: null,
      needsProfiling: false,
      step: 'count',
      nUsedCols: countUsedColumns(usedColumns),
      onConfirm: async (n) => { setPendingAutoProfile(null); await runGeneration(n); },
      onSkip: async (n) => { setPendingAutoProfile(null); await runGeneration(n); },
      onCancel: () => {
        setPendingAutoProfile(null);
        dispatch(resetContext());
        dispatch(setCurrentId(''));
        navigate('/');
      },
    });
    pendingSessionRef.current = null;
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
    const projectAuto = localStorage.getItem(`autoImport_project_${currentProjectId}`) === 'true';
    if (globalAuto || projectAuto) {
      handleAutoImport();
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tablesToImport]);

  // -------- Follow-up messages
  const handleSendMessage = useCallback(
    async (textParam?: string, testIdx?: number): Promise<boolean> => {
      if (isSending) return false;
      const text = (textParam ?? '').trim();
      const effectiveTestIndex = testIdx ?? (selectedTestIndex !== null ? selectedTestIndex : undefined);
      if (!text && effectiveTestIndex === undefined) return false;

      setIsSending(true);
      const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
      const lastMessageId = lastMessage ? lastMessage.id : '';

      // `effectiveTestIndex` (= selectedTestIndex) est une position 0-based dans
      // testResults — surtout PAS le slot 1-based `test_index`. On résout le test par
      // position, puis on cible par son identité stable (test_uid) + son vrai test_index.
      const anchoredTest = effectiveTestIndex !== undefined
        ? (testResults || [])[effectiveTestIndex]
        : undefined;
      const effectiveTestUid = anchoredTest?.test_uid;

      const routeHint = forcedRouteRef.current;
      forcedRouteRef.current = '';
      const ok = await sendMessage(text, sqlQuery, '', lastMessageId, undefined, false, anchoredTest?.test_index ?? effectiveTestIndex, assertionOnly, routeHint || undefined, effectiveTestUid);
      setIsSending(false);

      if (ok) {
        setSelectedTestIndex(null);
        setAssertionOnly(false);
      }
      return !!ok;
    },
    [sqlQuery, renderMessages, selectedChildIndices, sendMessage, isSending, selectedTestIndex, assertionOnly, testResults]
  );

  // Met en file un message tapé pendant qu'un run est déjà en cours. On NE touche PAS au
  // run en vol : le message s'affiche en « en attente » dans le fil et sera rejoué comme un
  // message normal une fois le run terminé (cf. effet de complétion → drainage séquentiel).
  const enqueueMessage = useCallback((text: string): Promise<boolean> => {
    const trimmed = (text ?? '').trim();
    if (!trimmed) return Promise.resolve(false);
    queuedRef.current = [...queuedRef.current, { id: uuidv4(), text: trimmed }];
    setQueuedMessages(queuedRef.current);
    return Promise.resolve(true);
  }, []);

  const onSendClick = useCallback((text: string): Promise<boolean> => {
    // Pendant une génération en cours : le message est mis en file (affiché « en attente »)
    // et traité automatiquement après la fin du run. Sinon : envoi immédiat.
    if (loading || isSending) {
      return enqueueMessage(text);
    }
    return handleSendMessage(text);
  }, [loading, isSending, enqueueMessage, handleSendMessage]);

  // -------- SQL bar update (re-run with new SQL)
  const handleSQLUpdate = useCallback(
    async (newSql: string, reevaluate = false) => {
      if (isSending || !newSql.trim() || !currentModelId) return;
      setPendingFileSql(null);
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
        await dispatchChatQuery({
          userInput: '',
          sessionId: currentModelId,
          project: '',
          dialect: DIALECT,
          query: newSql,
          ChangedMessageId: '',
          t,
          parentMessageId: effectiveParentId,
          context: 'sql_update',
          reevaluate,
          silent: true,
        }).unwrap?.();
      } catch { /* mise à jour SQL best-effort */ }
      setSqlDirty(false);
      setIsSending(false);
    },
    [isSending, currentModelId, optimizedSql, renderMessages, selectedChildIndices, dispatch, t, restoredMessageId]
  );

  // -------- Re-run pur : ré-exécute tous les tests existants SANS présenter ça comme
  // une mise à jour SQL (pas de divider « SQL mis à jour », pas d'entrée d'historique,
  // loader « Relance des tests » plutôt que « Génération des exemples »). Le backend
  // réutilise les données existantes (rerun_all → generator skip).
  const handleRerunAll = useCallback(async () => {
    if (!currentModelId || isSending) return;
    setIsSending(true);
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const parentMessageId = lastMessage ? lastMessage.id : '';
    try {
      await dispatchChatQuery({
        userInput: '',
        sessionId: currentModelId,
        project: '',
        dialect: DIALECT,
        query: sqlQuery,
        ChangedMessageId: '',
        t,
        parentMessageId,
        rerunAll: true,
        silent: true,
      }).unwrap?.();
    } catch { /* re-run best-effort */ }
    setIsSending(false);
  }, [currentModelId, isSending, renderMessages, selectedChildIndices, sqlQuery, t, dispatchChatQuery]);

  // -------- Ré-évaluer (reload file + rerun)
  // « Relancer » recharge le fichier sur disque (détection de dérive) puis :
  //  - SQL identique  → re-run pur (pas de divider trompeur)
  //  - SQL différent  → vraie mise à jour SQL (divider légitime)
  const handleReevaluate = useCallback(async () => {
    if (!currentModelPath || isSending) return;
    let diskSql = '';
    try {
      diskSql = (await fetchModelSql(currentModelPath)) || '';
    } catch { /* lecture best-effort → on retombe sur le SQL courant */ }
    const effectiveSql = diskSql.trim() ? diskSql : sqlQuery;
    if (effectiveSql.trim() === (sqlQuery || '').trim()) {
      await handleRerunAll();
    } else {
      await handleSQLUpdate(effectiveSql, true);
    }
  }, [currentModelPath, isSending, handleSQLUpdate, handleRerunAll, sqlQuery]);

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
      const doStream = () => dispatchChatQuery({ userInput: '', sessionId: currentModelId, project: '', dialect: DIALECT, query: sqlQuery, ChangedMessageId: '', t, parentMessageId: '' });
      if (result.profile_error) {
        dispatch(setError(result.profile_error));
        return;
      }
      if (result.profile_complete) {
        doStream();
      } else if (result.auto_profile_available && result.missing_columns?.length) {
        let resolvedReq: import('../../../api/query').BuildProfileRequestResult['profile_request'] | null = null;
        // Re-profilage d'un modèle existant : pas de boucle multi-tests (réservée à la 1ʳᵉ
        // génération), donc on saute l'étape « nombre de tests » et on va direct au profiling.
        setPendingAutoProfile({
          profileRequest: null,
          needsProfiling: true,
          step: 'profiling',
          onConfirm: async () => {
            const req = resolvedReq;
            if (!req) return;
            setIsAutoProfileRunning(true);
            try {
              const result = await autoProfileApi({ profile_sql: req.profile_query, profile_queries: req.profile_queries, project: '', session: currentModelId, partition_limit: req.partition_limit });
              if (result.profile_status !== 'complete') {
                setAutoProfileWarning({ status: result.profile_status, errors: result.errors ?? [] });
              }
            } catch { /* profilage best-effort */ }
            setIsAutoProfileRunning(false);
            setPendingAutoProfile(null);
            doStream();
          },
          onSkip: async () => {
            setPendingAutoProfile(null);
            try { await skipProfilingApi({ session: currentModelId }); } catch { /* skip best-effort */ }
            doStream();
          },
          onCancel: () => setPendingAutoProfile(null),
        });
        buildProfileRequestApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, missing_columns: result.missing_columns })
          .then(({ profile_request }) => {
            resolvedReq = profile_request;
            setPendingAutoProfile((prev) => prev ? { ...prev, profileRequest: profile_request } : prev);
          })
          .catch(() => {});
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
    setIsAutoProfileRunning(true);
    try {
      await refreshSchemasApi({ tables: [] });
      // Re-derive used_columns from the current SQL (schemas just refreshed) so the
      // reprofiling targets the columns the query actually touches. Relying on the
      // persisted test's used_columns silently no-ops when they're absent/empty:
      // the force branch then computes _find_missing_columns({}, []) → [] → profile_complete.
      const validateResult = await validateQueryApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, parent_message_id: '' });
      const usedColumns = validateResult?.used_columns ?? [];
      const result = await checkProfileApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, used_columns: usedColumns, force: true });
      if (result.profile_error) {
        dispatch(setError(result.profile_error));
        return;
      }
      if (!result.missing_columns?.length) return;
      const { profile_request } = await buildProfileRequestApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, missing_columns: result.missing_columns });
      const refreshResult = await autoProfileApi({ profile_sql: profile_request.profile_query, profile_queries: profile_request.profile_queries, project: '', session: currentModelId, partition_limit: profile_request.partition_limit });
      if (refreshResult.profile_status !== 'complete') {
        setAutoProfileWarning({ status: refreshResult.profile_status, errors: refreshResult.errors ?? [] });
      }
    } catch (e) {
      console.error('[handleRefreshProfile]', e);
      dispatch(setError('Erreur lors du rafraîchissement du profil.'));
    } finally {
      setIsAutoProfileRunning(false);
      // Re-lit la fraîcheur après le profilage pour rafraîchir « profilé il y a … ».
      getProfileMetaApi().then((m) => setProfiledAt(m.profiled_at)).catch(() => {});
    }
  }, [currentModelId, sqlQuery, dispatch]);

  // Charge la fraîcheur du profil à l'ouverture d'un modèle (profil global).
  useEffect(() => {
    let cancelled = false;
    getProfileMetaApi()
      .then((m) => { if (!cancelled) setProfiledAt(m.profiled_at); })
      .catch(() => { if (!cancelled) setProfiledAt(null); });
    return () => { cancelled = true; };
  }, [currentModelId]);

  // Schéma en cache périmé (erreur "Unknown column … schéma probablement périmé") :
  // ré-importe le schéma depuis BigQuery — ciblé sur les seules tables de la
  // requête courante — puis reprend le flux normal (profilage différentiel des
  // colonnes nouvellement découvertes, puis régénération).
  const handleRefreshSchemasAndRetry = useCallback(async () => {
    if (!currentModelId || !sqlQuery.trim()) return;
    setIsAutoProfileRunning(true);
    try {
      // Dérive les tables de la requête (refs BQ 3-parties) pour ne rafraîchir
      // que celles-ci. Si la dérivation échoue, tables=[] → refresh global (fallback).
      const validateResult = await validateQueryApi({ sql: sqlQuery, project: '', dialect: DIALECT, session: currentModelId, parent_message_id: '' });
      const tables = Array.from(new Set(
        (validateResult?.used_columns ?? [])
          .map((c: any) => [c.project, c.database, c.table].filter(Boolean).join('.'))
          .filter((t: string) => t.split('.').length === 3)
      ));
      await refreshSchemasApi({ tables });
    } catch (e) {
      console.error('[handleRefreshSchemasAndRetry]', e);
      dispatch(setError('Erreur lors du rafraîchissement du schéma.'));
      setIsAutoProfileRunning(false);
      return;
    }
    setIsAutoProfileRunning(false);
    // Reprend le flux standard : check de profil différentiel + auto-profilage des
    // colonnes manquantes (les nouvelles colonnes du schéma rafraîchi) puis stream.
    await handleRequestProfile();
  }, [currentModelId, sqlQuery, dispatch, handleRequestProfile]);

  // Variante pour la phase « entry » (GenerateView) : ici currentModelId est vide
  // (uiPhase === 'entry'), donc on ne peut pas passer par handleRequestProfile.
  // On rafraîchit le schéma (ciblé sur les tables du SQL prévisualisé si possible)
  // puis on relance la génération comme le bouton « Générer les tests ».
  const handleRefreshSchemasFromEntry = useCallback(async () => {
    setSubmitError(null);
    try {
      let tables: string[] = [];
      const sql = previewSql || '';
      if (sql.trim()) {
        try {
          const validateResult = await validateQueryApi({ sql, project: '', dialect: DIALECT, session: '', parent_message_id: '' });
          tables = Array.from(new Set(
            (validateResult?.used_columns ?? [])
              .map((c: any) => [c.project, c.database, c.table].filter(Boolean).join('.'))
              .filter((t: string) => t.split('.').length === 3)
          ));
        } catch { /* dérivation best-effort → fallback refresh global */ }
      }
      await refreshSchemasApi({ tables });
    } catch (e) {
      console.error('[handleRefreshSchemasFromEntry]', e);
      setSubmitError('Erreur lors du rafraîchissement du schéma.');
      return;
    }
    forceNewRef.current = true;
    handleFileSubmit();
  }, [previewSql, handleFileSubmit]);

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
      if (error && !pendingFirstLoad) {
        setSubmitError(error);
      }
      if (Notification.permission === 'granted') {
        const body = error ? t('notifications.generation_failed') : t('notifications.generation_success');
        const notif = new Notification('MockSQL', { body, icon: '/favicon.ico' });
        notif.onclick = () => {
          window.focus();
          notif.close();
        };
      }
      // Drainage de la file : un message tapé pendant le run vient d'attendre la fin du
      // thread. On en prend UN (le plus ancien) et on le rejoue comme un message normal —
      // il devient un vrai tour de conversation. Sa complétion re-déclenchera cet effet →
      // le suivant est drainé → traitement séquentiel, dans l'ordre. On draine même après
      // une erreur pour ne jamais laisser un message bloqué « en attente ».
      if (currentModelId && queuedRef.current.length > 0) {
        const [next, ...rest] = queuedRef.current;
        queuedRef.current = rest;
        setQueuedMessages(rest);
        const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
        sendMessage(next.text, sqlQuery, '', lastMessage ? lastMessage.id : '');
      }
    }
    prevLoadingRef.current = loading;
  }, [loading, error, t, pendingFirstLoad, currentModelId, renderMessages, selectedChildIndices, sendMessage, sqlQuery]);

  const handleRetry = useCallback(() => {
    if (!lastChatQueryArgsRef.current) return;
    setSubmitError(null);
    dispatch(setError(''));
    isGeneratingRef.current = true;
    dispatchChatQuery(lastChatQueryArgsRef.current);
  }, [dispatch, dispatchChatQuery]);

  // Reprise d'une boucle multi-tests interrompue : on a demandé N tests mais seuls K<N sont
  // sur disque (coupure réseau/crash). Re-dispatcher la MÊME requête fait reprendre le batch
  // côté backend (pre_routing détecte tests sur disque < tests_target → construit les manquants,
  // sans reconstruire le nominal). Visible même après un reload (tests_target chargé via getMessages).
  const builtTestsCount = testResults?.length ?? 0;
  const canResumeBatch =
    !loading &&
    !!storedQuery &&
    !!storedTestsTarget &&
    builtTestsCount > 0 &&
    builtTestsCount < storedTestsTarget;

  const handleResume = useCallback(() => {
    if (!storedQuery || !currentModelId) return;
    setSubmitError(null);
    dispatch(setError(''));
    isGeneratingRef.current = true;
    setPendingFirstLoad(true);
    dispatchChatQuery({
      userInput: '',
      sessionId: currentModelId,
      project: '',
      dialect: DIALECT,
      query: storedQuery,
      ChangedMessageId: '',
      t,
      parentMessageId: '',
      testsTarget: storedTestsTarget,
    });
  }, [storedQuery, currentModelId, DIALECT, storedTestsTarget, t, dispatch, dispatchChatQuery]);

  const handleAddTest = useCallback(() => {
    setSelectedTestIndex(null);
    setAddTestTrigger(n => n + 1);
  }, []);

  // Clic sur une suggestion : on passe par l'agent (suggestionIntent) pour qu'il puisse
  // détecter qu'elle recoupe un test existant et l'étendre plutôt que créer un doublon.
  // Le backend impose une action de test (jamais de "c'est déjà vérifié") et retombe sur
  // le generator si l'agent ne produit rien → un test sort toujours.
  const handleSuggestionClick = useCallback(async (text: string) => {
    if (isSending) return;
    if (!currentModelId) return;
    setIsSending(true);
    setSelectedTestIndex(null);
    // Consommation optimiste : la suggestion disparaît du panneau dès le clic
    // (le backend la retire aussi du modèle via suggestion_intent).
    dispatch(dismissSuggestion(text));
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const lastMessageId = lastMessage ? lastMessage.id : '';
    try {
      isGeneratingRef.current = true;
      await dispatchChatQuery({
        userInput: text,
        sessionId: currentModelId,
        project: '',
        dialect: DIALECT,
        query: sqlQuery,
        ChangedMessageId: '',
        t,
        parentMessageId: lastMessageId,
        suggestionIntent: true,
      }).unwrap?.();
    } catch {
      /* erreur déjà gérée par le thunk */
    } finally {
      setIsSending(false);
    }
  }, [isSending, currentModelId, renderMessages, selectedChildIndices, sqlQuery, dispatch, t]);

  const handleDismissSuggestion = useCallback(async (text: string) => {
    if (!currentModelId) return;
    dispatch(dismissSuggestion(text));
    try {
      await dismissSuggestionApi(currentModelId, text);
    } catch {
      /* silent — l'optimistic update suffit pour l'UX */
    }
  }, [currentModelId, dispatch]);

  // Régénération à la demande : court-circuite l'agent (regenerate_suggestions) →
  // suggestions_generator direct, pas de message de clôture dans le fil.
  const handleRegenerateSuggestions = useCallback(async () => {
    if (isSending) return;
    if (!currentModelId) return;
    setIsSending(true);
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const lastMessageId = lastMessage ? lastMessage.id : '';
    try {
      isGeneratingRef.current = true;
      // Affiche d'emblée le bon libellé (sinon le fallback « Extraction des
      // colonnes… » s'affiche pendant pre_routing/routing avant suggestions_generator).
      dispatch(setLoadingMessage(t('loading.generating_suggestions')));
      await dispatchChatQuery({
        userInput: '',
        sessionId: currentModelId,
        project: '',
        dialect: DIALECT,
        query: sqlQuery,
        ChangedMessageId: '',
        t,
        parentMessageId: lastMessageId,
        regenerateSuggestions: true,
        silent: true,
      }).unwrap?.();
    } catch {
      /* erreur déjà gérée par le thunk */
    } finally {
      setIsSending(false);
    }
  }, [isSending, currentModelId, renderMessages, selectedChildIndices, sqlQuery, dispatch, t]);

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
    // `idx` = position 0-based dans testResults ; `test_index` = slot 1-based backend.
    const test = (testResults || [])[idx];
    if (!test) return;
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    // threadParentId ensures the rerun lands as a sibling of the original, not a child
    const parentMessageId = test?.threadParentId || (lastMessage ? lastMessage.id : '');
    dispatchChatQuery({
      userInput: '',
      sessionId: currentModelId || '',
      project: '',
      dialect: DIALECT,
      query: sqlQuery,
      ChangedMessageId: '',
      t,
      parentMessageId,
      testUid: test?.test_uid,
      testIndex: test?.test_index,
      forceRoute: 'generator',
      silent: true,
    });
  }, [isSending, currentModelId, sqlQuery, renderMessages, selectedChildIndices, dispatch, t, testResults]);

  // « Je valide l'état actuel » sur un test needs_validation : l'utilisateur tranche
  // l'ambiguïté en faveur du réel → accept_validation réaligne la description + verdict Bon.
  const handleValidateTest = useCallback((idx: number) => {
    if (isSending) return;
    // `idx` est la position 0-based dans testResults (cf. TestsPanel) ; `test_index`
    // est un slot 1-based côté backend → on résout par position, jamais par valeur.
    const test = (testResults || [])[idx];
    if (!test) return;
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const parentMessageId = test?.threadParentId || (lastMessage ? lastMessage.id : '');
    dispatchChatQuery({
      userInput: '',
      sessionId: currentModelId || '',
      project: '',
      dialect: DIALECT,
      query: '',
      ChangedMessageId: '',
      t,
      parentMessageId,
      testUid: test?.test_uid,
      testIndex: test?.test_index,
      validateIntent: true,
      silent: true,
    });
  }, [isSending, currentModelId, DIALECT, renderMessages, selectedChildIndices, t, testResults, dispatchChatQuery]);

  // « Corriger le test » : on ancre le chat sur ce test pour que l'utilisateur décrive le fix.
  const handleCorrectTest = useCallback((idx: number) => {
    handleSelectTestForModification(idx);
  }, [handleSelectTestForModification]);

  // « Appliquer » une description proposée par l'agent (jamais appliquée d'office) :
  // action déterministe → apply_description écrit la description proposée sur le test.
  const handleApplyDescription = useCallback((idx: number) => {
    if (isSending) return;
    const test = (testResults || [])[idx];
    if (!test) return;
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const parentMessageId = test?.threadParentId || (lastMessage ? lastMessage.id : '');
    dispatchChatQuery({
      userInput: '',
      sessionId: currentModelId || '',
      project: '',
      dialect: DIALECT,
      query: '',
      ChangedMessageId: '',
      t,
      parentMessageId,
      testUid: test?.test_uid,
      testIndex: test?.test_index,
      applyDescriptionIntent: true,
      silent: true,
    });
  }, [isSending, currentModelId, DIALECT, renderMessages, selectedChildIndices, t, testResults, dispatchChatQuery]);

  // « Garder l'actuelle » : retrait optimiste de la proposition + nettoyage backend.
  const handleRejectDescription = useCallback((idx: number) => {
    if (isSending) return;
    const test = (testResults || [])[idx];
    if (!test) return;
    dispatch(dismissDescriptionProposal(test.test_index));
    const lastMessage = getLastMessage(renderMessages, selectedChildIndices);
    const parentMessageId = test?.threadParentId || (lastMessage ? lastMessage.id : '');
    dispatchChatQuery({
      userInput: '',
      sessionId: currentModelId || '',
      project: '',
      dialect: DIALECT,
      query: '',
      ChangedMessageId: '',
      t,
      parentMessageId,
      testUid: test?.test_uid,
      testIndex: test?.test_index,
      rejectDescriptionIntent: true,
      silent: true,
    });
  }, [isSending, currentModelId, DIALECT, renderMessages, selectedChildIndices, t, testResults, dispatch, dispatchChatQuery]);


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
      {uiPhase === 'workspace' && (submitError || (lastError && !lastErrorDismissed) || missingTables || canResumeBatch) && (
        <Box sx={{ flexShrink: 0, px: 2, pt: 1 }}>
          {canResumeBatch && !missingTables && (
            <Alert
              severity="info"
              sx={{ borderRadius: '12px', mb: 1 }}
              action={
                <Button
                  size="small"
                  color="inherit"
                  variant="outlined"
                  startIcon={<RefreshRoundedIcon sx={{ fontSize: 16 }} />}
                  onClick={handleResume}
                  sx={{ whiteSpace: 'nowrap', textTransform: 'none', ml: 1 }}
                >
                  Reprendre la génération
                </Button>
              }
            >
              {`Génération interrompue : ${builtTestsCount}/${storedTestsTarget} tests construits. Reprends pour générer les ${storedTestsTarget! - builtTestsCount} manquants.`}
            </Alert>
          )}
          {submitError && (
            <Alert
              severity="error"
              sx={{ borderRadius: '12px', mb: 1 }}
              onClose={() => setSubmitError(null)}
              action={isStaleSchemaError(submitError) ? (
                <Button
                  size="small"
                  color="inherit"
                  variant="outlined"
                  startIcon={<RefreshRoundedIcon sx={{ fontSize: 16 }} />}
                  onClick={() => { setSubmitError(null); handleRefreshSchemasAndRetry(); }}
                  sx={{ whiteSpace: 'nowrap', textTransform: 'none', ml: 1 }}
                >
                  Rafraîchir le schéma
                </Button>
              ) : isRetryableError(submitError) && lastChatQueryArgsRef.current ? (
                <Button size="small" color="inherit" onClick={handleRetry} sx={{ whiteSpace: 'nowrap', ml: 1 }}>
                  Réessayer
                </Button>
              ) : undefined}
            >
              {submitError}
            </Alert>
          )}
          {lastError && !lastErrorDismissed && !submitError && (
            <Alert
              severity="warning"
              sx={{ borderRadius: '12px', mb: 1 }}
              onClose={() => setLastErrorDismissed(true)}
              action={isStaleSchemaError(lastError) ? (
                <Button
                  size="small"
                  color="inherit"
                  variant="outlined"
                  startIcon={<RefreshRoundedIcon sx={{ fontSize: 16 }} />}
                  onClick={handleRefreshSchemasAndRetry}
                  sx={{ whiteSpace: 'nowrap', textTransform: 'none', ml: 1 }}
                >
                  Rafraîchir le schéma
                </Button>
              ) : undefined}
            >
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

            {/* Mode toggle (v15 §8) — integration is a visible stub, no flow */}
            <Box sx={{ display: 'flex', gap: 0, bgcolor: '#f3f6f7', border: '1px solid #c9d3d6', borderRadius: '12px', p: '4px', mb: '20px' }}>
              {([
                { id: 'unit' as const, title: t('generate.mode_unit_title'), sub: t('generate.mode_unit_sub'), icon: <ScienceIcon sx={{ fontSize: 18 }} />, badge: null },
                { id: 'integration' as const, title: t('generate.mode_integration_title'), sub: t('generate.mode_integration_sub'), icon: (
                  <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round"><line x1="6" x2="6" y1="3" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>
                ), badge: t('generate.mode_soon') },
              ]).map((m) => {
                const on = genMode === m.id;
                return (
                  <Tooltip key={m.id} title={m.id === 'integration' ? t('generate.mode_integration_tooltip') : ''} placement="top" arrow disableHoverListener={m.id !== 'integration'}>
                    <Box
                      onClick={() => { if (m.id === 'unit') setGenMode('unit'); }}
                      role="button"
                      sx={{
                        flex: 1, display: 'flex', alignItems: 'center', gap: '10px', p: '11px 14px',
                        borderRadius: '9px', cursor: 'pointer', textAlign: 'left',
                        bgcolor: on ? '#fff' : 'transparent',
                        boxShadow: on ? '0 1px 2px rgba(15,39,42,0.04)' : 'none',
                        transition: 'all .15s',
                        opacity: m.id === 'integration' ? 0.85 : 1,
                      }}
                    >
                      <Box sx={{
                        width: 32, height: 32, borderRadius: '8px', display: 'grid', placeItems: 'center', flexShrink: 0,
                        bgcolor: on ? '#2BB0A8' : '#ecf7f6', color: on ? '#fff' : '#1f948d',
                      }}>
                        {m.icon}
                      </Box>
                      <Box sx={{ minWidth: 0 }}>
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: '7px' }}>
                          <Typography sx={{ fontSize: 13.5, fontWeight: 600, color: '#0f272a' }}>{m.title}</Typography>
                          {m.badge && (
                            <Box component="span" sx={{ fontSize: 9.5, fontWeight: 700, letterSpacing: '0.04em', textTransform: 'uppercase', color: '#16746e', bgcolor: '#ecf7f6', borderRadius: 999, px: '7px', py: '1px' }}>
                              {m.badge}
                            </Box>
                          )}
                        </Box>
                        <Typography sx={{ fontSize: 11.5, color: '#6b8287', mt: '1px' }}>{m.sub}</Typography>
                      </Box>
                    </Box>
                  </Tooltip>
                );
              })}
            </Box>

            {/* Step 1 — File list */}
            <Box sx={{ mb: '24px' }}>
              {/* Step label row */}
              <Box sx={{ display: 'flex', alignItems: 'center', gap: '10px', mb: '10px' }}>
                <Box sx={{ width: 22, height: 22, borderRadius: '50%', bgcolor: '#2BB0A8', color: '#fff', display: 'inline-flex', alignItems: 'center', justifyContent: 'center', fontSize: 11, fontWeight: 700, flexShrink: 0 }}>1</Box>
                <Typography sx={{ fontSize: 14.5, fontWeight: 600, color: '#0f272a' }}>{t('generate.choose_sql_file')}</Typography>
                {import.meta.env.VITE_DEMO_MODE !== 'true' && (
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
                )}
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
                    inputProps={{ sx: { py: '7px', px: '4px' }, 'data-testid': 'file-search-input' }}
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
                          data-testid={`generate-file-row-${f.name}`}
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
                            {import.meta.env.VITE_DEMO_MODE !== 'true' && (
                              <Typography sx={{ fontSize: 10.5, color: '#6b8287', mt: '1px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}>
                                {f.path ?? f.name}
                              </Typography>
                            )}
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
                data-testid="generate-button"
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
              <SubmissionProgress label={submissionStep} />
            )}
            {submitError && (
              <Alert
                severity="error"
                sx={{ borderRadius: '12px', mt: 2 }}
                onClose={() => setSubmitError(null)}
                action={isStaleSchemaError(submitError) ? (
                  <Button
                    size="small"
                    color="inherit"
                    variant="outlined"
                    startIcon={<RefreshRoundedIcon sx={{ fontSize: 16 }} />}
                    onClick={handleRefreshSchemasFromEntry}
                    sx={{ whiteSpace: 'nowrap', textTransform: 'none', ml: 1 }}
                  >
                    Rafraîchir le schéma
                  </Button>
                ) : undefined}
              >
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
        <Box sx={{ flex: 1, display: 'flex', flexDirection: 'row', minHeight: 0, overflow: demoZoom ? 'visible' : 'hidden', position: 'relative' }}>

          {/* Demo zoom backdrop */}
          {demoZoom && createPortal(
            <Box
              onClick={() => setDemoZoom(null)}
              sx={{
                position: 'fixed', inset: 0, zIndex: 1299,
                bgcolor: 'rgba(0,0,0,0.65)',
                transition: 'opacity 0.3s',
              }}
            />,
            document.body
          )}

          {/* Chat column — permanent left panel */}
          <Box sx={{
            position: 'relative',
            zIndex: demoZoom === 'chat' ? 1300 : 'auto',
            transform: demoZoom === 'chat' ? demoTransform.chat : 'scale(1)',
            transformOrigin: 'center center',
            transition: 'transform 0.45s cubic-bezier(0.4,0,0.2,1)',
            flexShrink: 0,
          }}>
          <ChatColumn
            fileName={(() => {
              const name = currentModelName || (selectedModelName ? selectedModelName.split('/').pop() : '');
              return name ? `${name}.sql` : 'requête.sql';
            })()}
            filePath={(() => {
              const path = currentModelPath || selectedModelName;
              return path ? `${path}.sql` : undefined;
            })()}
            onChangeFile={() => {
              dispatch(resetContext());
              dispatch(setCurrentId(''));
              navigate('/');
            }}
            selectedTestIndex={selectedTestIndex}
            assertionOnly={assertionOnly}
            onClearAnchor={() => { setSelectedTestIndex(null); setAssertionOnly(false); }}
            renderMessages={renderMessages as any}
            modelId={currentModelId}
            onSend={onSendClick}
            isSending={isSending}
            loading={loading}
            loading_message={loading_message}
            queuedMessages={queuedMessages}
            understandingDraft={understandingDraft}
            validationMs={validationMs}
            error={error}
            alwaysFix={alwaysFix}
            onAlwaysFixChange={handleAlwaysFixChange}
            sqlHistory={sqlHistory}
            onSqlRestore={handleHistorySelect}
            onRestoreState={handleRestoreState}
            restoredMessageId={restoredMessageId}
            streamingReasoning={streamingReasoning}
            lastReasoning={lastReasoning}
            onStopStream={handleStopStream}
            sendMessage={sendMessage}
            sqlQuery={sqlQuery}
            onClearHistory={handleClearHistory}
            onRequestProfile={handleRequestProfile}
            onRefreshSchemas={handleRefreshSchemasAndRetry}
            focusTrigger={addTestTrigger}
          />
          </Box>

          {/* Main area — tests + footer */}
          <Box sx={{
            flex: 1, display: 'flex', flexDirection: 'column', minWidth: 0,
            position: 'relative',
            zIndex: demoZoom === 'tests' ? 1300 : 'auto',
            transform: demoZoom === 'tests' ? demoTransform.tests : 'scale(1)',
            transformOrigin: 'center center',
            transition: 'transform 0.45s cubic-bezier(0.4,0,0.2,1)',
          }}>
            <ArtefactHeader
              testCount={testResults?.length ?? 0}
              onRerun={handleReevaluate}
              rerunning={!!(loading || isSending)}
              sqlDirty={sqlDirty}
              onRefreshProfile={handleRefreshProfile}
              refreshing={isAutoProfileRunning}
              profiledAt={profiledAt}
            />
            {autoProfileWarning && (
              <AutoProfileWarningBanner
                status={autoProfileWarning.status}
                errors={autoProfileWarning.errors}
                onClose={() => setAutoProfileWarning(null)}
              />
            )}
            {partialProfile && (
              <Alert
                severity="info"
                icon={<InfoOutlinedIcon fontSize="inherit" />}
                onClose={() => setPartialProfile(null)}
                sx={{ mb: 1, borderRadius: 2, alignItems: 'center' }}
                action={
                  <Button
                    color="inherit"
                    size="small"
                    disabled={isAutoProfileRunning}
                    onClick={handleCompleteProfile}
                    startIcon={isAutoProfileRunning ? <CircularProgress size={13} /> : undefined}
                    sx={{ textTransform: 'none', fontWeight: 700, whiteSpace: 'nowrap' }}
                  >
                    {isAutoProfileRunning ? 'Profilage…' : 'Compléter le profil'}
                  </Button>
                }
              >
                Profil partiel — {partialProfile.deferred.length} {partialProfile.deferred.length > 1 ? 'tables ont été différées' : 'table a été différée'}
                {partialProfile.budget != null ? ` (scan estimé > ${partialProfile.budget} To)` : ''} :{' '}
                {partialProfile.deferred.map((d) => d.scope).join(', ')}. La génération utilise le profil disponible ; complète-le si besoin.
              </Alert>
            )}
            <Box sx={{ flex: 1, position: 'relative', display: 'flex', flexDirection: 'column', minHeight: 0, overflow: 'hidden' }}>
              <TestsPanel
                onAddTest={handleAddTest}
                onSelectForModification={handleSelectTestForModification}
                onEditAssertions={handleEditAssertions}
                onRerunTest={handleRerunTest}
                onValidateTest={handleValidateTest}
                onCorrectTest={handleCorrectTest}
                onApplyDescription={handleApplyDescription}
                onRejectDescription={handleRejectDescription}
                onSuggestionClick={handleSuggestionClick}
                onDismissSuggestion={handleDismissSuggestion}
                onRegenerateSuggestions={handleRegenerateSuggestions}
                selectedTestIndex={selectedTestIndex}
                retryBadDataTestIndex={retryBadDataTestIndex}
                sqlProps={{
                  sql: sqlQuery,
                  onUpdate: handleSQLUpdate,
                  optimizedSql,
                  sqlHistory,
                  onHistorySelect: handleHistorySelect,
                  historyRestoreTrigger,
                  collapseSignal: sqlCollapseSignal,
                  disabled: isSending,
                  loading: isSending,
                  hasError: lastMsgHasError,
                  sqlFileName: currentModelName || undefined,
                }}
                staleInfo={
                  pendingFileSql
                    ? {
                        isStale: true,
                        commitsSince: 0,
                        lastTestedAt: currentModel?.updateDate,
                        onReevaluate: () => handleSQLUpdate(pendingFileSql, true),
                        currentSql: sqlQuery,
                        onFetchNewSql: async () => pendingFileSql,
                      }
                    : currentModel?.isStale
                    ? {
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
                      }
                    : undefined
                }
                onUpload={(uploadedData) => {
                  const lastMsg = getLastMessage(renderMessages, selectedChildIndices) as any;
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

            {/* Étape 1 du wizard : nombre de tests à générer d'emblée (1–3, total). */}
            {pendingAutoProfile.step === 'count' && (
              <Box sx={{ mb: 1 }}>
                <Typography variant="body2" sx={{ color: '#555', mb: 1.5 }}>
                  Combien de tests veux-tu générer d'emblée ? MockSQL construit le test du chemin
                  nominal, puis enchaîne les suivants à partir de cas non encore couverts.
                </Typography>
                <ToggleButtonGroup
                  exclusive
                  value={testsTarget}
                  onChange={(_, v) => { if (v) setTestsTarget(v); }}
                  sx={{ display: 'flex', gap: 1 }}
                >
                  {[1, 2, 3].map((n) => (
                    <ToggleButton
                      key={n}
                      value={n}
                      sx={{
                        flex: 1,
                        border: '1px solid #d8e6e5 !important',
                        borderRadius: '8px !important',
                        textTransform: 'none',
                        fontWeight: 700,
                        color: '#1ca8a4',
                        '&.Mui-selected': { bgcolor: '#1ca8a4', color: 'white', '&:hover': { bgcolor: '#159e9a' } },
                      }}
                    >
                      {n} test{n > 1 ? 's' : ''}
                    </ToggleButton>
                  ))}
                </ToggleButtonGroup>
                {pendingAutoProfile.nUsedCols != null && (
                  <EtaNotice
                    eta={estimateGenerationMinutes(pendingAutoProfile.nUsedCols, testsTarget)}
                    launchesNow
                  />
                )}

                {/* Budget de scan : profilage automatique sous ce seuil. Au-delà,
                    les tables sont différées (profil partiel) et profilables à la
                    demande. BigQuery uniquement (DuckDB/Postgres = scan gratuit). */}
                {pendingAutoProfile.needsProfiling && DIALECT === 'bigquery' && (
                  pendingAutoProfile.configBudget != null ? (
                    <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 2 }}>
                      <Typography variant="caption" sx={{ color: '#888' }}>Budget de scan :</Typography>
                      <Chip
                        size="small"
                        label={`${pendingAutoProfile.configBudget} To · auto`}
                        sx={{ bgcolor: '#f0faf5', color: '#1e5c38', border: '1px solid #a5d6b7', fontWeight: 600, fontSize: 11 }}
                      />
                    </Box>
                  ) : (
                    <Box sx={{ mt: 2 }}>
                      <Typography variant="body2" sx={{ color: '#555', fontWeight: 600, mb: 0.5 }}>
                        Budget de scan BigQuery
                      </Typography>
                      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                        <TextField
                          type="number"
                          size="small"
                          value={budgetInput}
                          onChange={(e) => {
                            const raw = e.target.value;
                            setBudgetInput(raw);
                            // Ne committe que les valeurs > 0 ; 0 / vide / invalide
                            // ne deviennent jamais le budget (0 différerait tout).
                            const v = parseFloat(raw);
                            if (!Number.isNaN(v) && v > 0) setBudgetTarget(v);
                          }}
                          onBlur={() => {
                            // Remet l'affichage sur la dernière valeur valide si le
                            // champ a été laissé vide / à 0 / invalide.
                            const v = parseFloat(budgetInput);
                            if (Number.isNaN(v) || v <= 0) setBudgetInput(String(budgetTarget));
                          }}
                          inputProps={{ min: 0.01, step: 0.1, style: { width: 70 } }}
                          InputProps={{ endAdornment: <InputAdornment position="end">To</InputAdornment> }}
                          disabled={isAutoProfileRunning}
                        />
                        <Typography variant="caption" sx={{ color: '#999' }}>
                          Les tables dont le scan dépasse ce seuil sont profilées à la demande.
                        </Typography>
                      </Box>
                    </Box>
                  )
                )}

                {pendingAutoProfile.needsProfiling && isAutoProfileRunning && (
                  <Box sx={{ mt: 2 }}>
                    <LinearProgress
                      variant="indeterminate"
                      sx={{ height: 6, borderRadius: 3, bgcolor: '#e0f7f5', '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' } }}
                    />
                    <Typography variant="caption" sx={{ color: '#555', mt: 0.5, display: 'block' }}>
                      {t('loading.profiling')}
                    </Typography>
                  </Box>
                )}
              </Box>
            )}

            {pendingAutoProfile.step === 'profiling' && (<>
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

            {/* Billing + download SQL — affichés uniquement quand le build-profile-request est résolu */}
            {pendingAutoProfile.profileRequest === null ? (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1, color: '#bbb' }}>
                <LinearProgress
                  variant="indeterminate"
                  sx={{ flex: 1, height: 4, borderRadius: 2, bgcolor: '#f0f0f0', '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' } }}
                />
                <Typography variant="caption" sx={{ color: '#aaa', whiteSpace: 'nowrap' }}>
                  Estimation en cours…
                </Typography>
              </Box>
            ) : (
              <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mb: 1 }}>
                {pendingAutoProfile.profileRequest.billing_tb !== undefined && (
                  <>
                    <Typography variant="caption" sx={{ color: '#aaa' }}>Scan estimé :</Typography>
                    <Chip
                      label={`~${pendingAutoProfile.profileRequest.billing_tb < 0.001
                        ? '< 0,001'
                        : pendingAutoProfile.profileRequest.billing_tb.toFixed(3)} To · ${DIALECT.charAt(0).toUpperCase() + DIALECT.slice(1)}`}
                      size="small"
                      sx={{ bgcolor: '#f5f5f5', color: '#888', border: '1px solid #e0e0e0', fontWeight: 600, fontSize: 11 }}
                    />
                  </>
                )}
                <Tooltip title="Télécharger la requête SQL de profiling (.sql)">
                  <IconButton
                    size="small"
                    onClick={() => {
                      const req = pendingAutoProfile.profileRequest!;
                      const queries = req.profile_queries;
                      const sql = queries && queries.length > 1
                        ? queries.map((q, i) => `-- Requête ${i + 1}\n${q}`).join('\n\n')
                        : req.profile_query;
                      const blob = new Blob([sql], { type: 'text/plain' });
                      const url = URL.createObjectURL(blob);
                      const a = document.createElement('a');
                      a.href = url;
                      a.download = 'profiling_query.sql';
                      a.click();
                      URL.revokeObjectURL(url);
                    }}
                    sx={{ color: '#bbb', '&:hover': { color: '#1ca8a4' } }}
                  >
                    <DownloadIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
              </Box>
            )}

            {pendingAutoProfile.nUsedCols != null && !isAutoProfileRunning && (
              <EtaNotice
                eta={estimateGenerationMinutes(pendingAutoProfile.nUsedCols, testsTarget)}
                launchesNow
              />
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
            </>)}
          </DialogContent>
          <DialogActions sx={{ px: 3, pb: 2, gap: 1 }}>
            <Button
              variant="text"
              onClick={() => pendingAutoProfile.onCancel()}
              disabled={isAutoProfileRunning}
              sx={{ textTransform: 'none', color: '#999', mr: 'auto', '&:hover': { bgcolor: 'transparent', color: '#666' } }}
            >
              Annuler
            </Button>
            {pendingAutoProfile.step === 'profiling' && (
              <Button
                variant="text"
                onClick={() => pendingAutoProfile.onSkip(testsTarget)}
                disabled={isAutoProfileRunning}
                sx={{ textTransform: 'none', color: '#bbb', fontSize: 12, '&:hover': { bgcolor: 'transparent', color: '#888' } }}
              >
                {t('profiling.skip_label')}
              </Button>
            )}
            {pendingAutoProfile.step === 'count' ? (
              <Button
                variant="contained"
                disabled={isAutoProfileRunning}
                onClick={() => pendingAutoProfile.onConfirm(testsTarget)}
                startIcon={isAutoProfileRunning ? <CircularProgress size={14} sx={{ color: 'white' }} /> : undefined}
                sx={{
                  textTransform: 'none', bgcolor: '#1ca8a4', '&:hover': { bgcolor: '#159e9a' },
                  '&.Mui-disabled': { bgcolor: '#1ca8a4', color: 'white', opacity: 0.85 },
                }}
              >
                {isAutoProfileRunning
                  ? t('loading.profiling_short')
                  : `Générer ${testsTarget} test${testsTarget > 1 ? 's' : ''}`}
              </Button>
            ) : (
              <Button
                variant="contained"
                onClick={() => pendingAutoProfile.onConfirm(testsTarget)}
                disabled={isAutoProfileRunning || pendingAutoProfile.profileRequest === null}
                startIcon={
                  !isAutoProfileRunning && pendingAutoProfile.profileRequest === null
                    ? <CircularProgress size={14} sx={{ color: 'white' }} />
                    : undefined
                }
                sx={{
                  textTransform: 'none',
                  bgcolor: '#1ca8a4',
                  '&:hover': { bgcolor: '#159e9a' },
                  '&.Mui-disabled': {
                    bgcolor: pendingAutoProfile.profileRequest === null ? '#1ca8a4' : undefined,
                    color: pendingAutoProfile.profileRequest === null ? 'white' : undefined,
                    opacity: pendingAutoProfile.profileRequest === null ? 0.85 : undefined,
                  },
                }}
              >
                {isAutoProfileRunning
                  ? t('loading.profiling_short')
                  : pendingAutoProfile.profileRequest === null
                    ? 'Estimation…'
                    : t('action.run_profiling')}
              </Button>
            )}
          </DialogActions>
        </Dialog>
      )}
    </Container>
  );
};

// Bandeau de durée estimée. `launchesNow` distingue l'écran dont le bouton lance
// réellement la génération (→ promesse « tu peux passer à autre chose ») de l'écran
// intermédiaire encore en attente d'une confirmation (→ on ne promet rien, sinon
// l'utilisateur s'absente alors qu'une étape requiert toujours son attention).
const EtaNotice: React.FC<{ eta: number; launchesNow: boolean }> = ({ eta, launchesNow }) => (
  <Box
    sx={{
      display: 'flex',
      alignItems: 'flex-start',
      gap: 1,
      mt: 2,
      bgcolor: '#f5fbfb',
      border: '1px solid #cfeceb',
      borderRadius: 2,
      px: 1.5,
      py: 1.25,
    }}
  >
    <AccessTimeIcon sx={{ color: '#1ca8a4', fontSize: 18, mt: '1px', flexShrink: 0 }} />
    <Typography variant="body2" sx={{ color: '#3a6b69', fontSize: 13 }}>
      {launchesNow ? (
        <>
          Le travail peut prendre ~{eta} minute{eta > 1 ? 's' : ''} — garde cet onglet
          ouvert (tu peux passer à autre chose), je te préviens par notification dès que
          c'est terminé.
        </>
      ) : (
        <>
          Le travail prendra ~{eta} minute{eta > 1 ? 's' : ''}. Une dernière étape de
          confirmation reste avant le lancement.
        </>
      )}
    </Typography>
  </Box>
);

const AutoProfileWarningBanner: React.FC<{
  status: 'partial' | 'failed';
  errors: Array<{ query_index: number; error: string }>;
  onClose: () => void;
}> = ({ status, errors, onClose }) => {
  const [expanded, setExpanded] = React.useState(false);
  const isPartial = status === 'partial';

  return (
    <Alert
      severity={isPartial ? 'warning' : 'error'}
      onClose={onClose}
      sx={{ mx: 2, mt: 1, borderRadius: 2 }}
    >
      <Typography variant="body2" sx={{ fontWeight: 600 }}>
        {isPartial
          ? `Profil partiellement importé — ${errors.length} requête(s) ont échoué.`
          : 'Le profiling a échoué (toutes les requêtes en erreur).'}
      </Typography>
      <Typography variant="body2" sx={{ mt: 0.25 }}>
        {isPartial
          ? 'Le générateur utilisera les données disponibles.'
          : 'La qualité des données générées sera moindre — génération en cours sans profil.'}
      </Typography>
      {errors.length > 0 && (
        <Box sx={{ mt: 0.5 }}>
          <Button
            size="small"
            variant="text"
            sx={{ p: 0, minWidth: 0, textTransform: 'none', fontSize: 12 }}
            onClick={() => setExpanded((v) => !v)}
          >
            {expanded ? 'Masquer les erreurs ↑' : 'Voir les erreurs ↓'}
          </Button>
          {expanded && (
            <Box sx={{ mt: 0.75, pl: 1, borderLeft: '2px solid', borderColor: isPartial ? 'warning.main' : 'error.main' }}>
              {errors.map((e) => (
                <Typography key={e.query_index} variant="caption" sx={{ display: 'block', fontFamily: 'monospace', color: 'text.secondary' }}>
                  Requête {e.query_index + 1} : {e.error}
                </Typography>
              ))}
            </Box>
          )}
        </Box>
      )}
    </Alert>
  );
};

export default React.memo(ChatComponent);
