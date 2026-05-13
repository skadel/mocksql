import React, { useCallback, useEffect, useState } from 'react';
import {
  Alert,
  Box,
  Button,
  Chip,
  CircularProgress,
  Divider,
  Tooltip,
  Typography,
} from '@mui/material';
import AddIcon from '@mui/icons-material/Add';
import ArrowBackIcon from '@mui/icons-material/ArrowBack';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import CancelIcon from '@mui/icons-material/Cancel';
import ErrorOutlineIcon from '@mui/icons-material/ErrorOutline';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import AccountTreeIcon from '@mui/icons-material/AccountTree';
import FolderOpenIcon from '@mui/icons-material/FolderOpen';
import { useAppDispatch, useAppSelector } from '../../app/hooks';
import { setWorkspaceMode } from '../buildModel/buildModelSlice';
import { drawerWidth } from '../appBar/components/DrawerComponent';
import { useSqlFileLoader } from '../buildModel/hooks/useSqlFileLoader';
import {
  fetchIntegrationFiles,
  fetchIntegrationSourceTables,
  runIntegrationApi,
  saveIntegrationChain,
} from '../../api/integration';
import { IntegrationRunResult, IntegrationStep, IntegrationTestItem } from '../../utils/types';
import { ChainBuilder } from './ChainBuilder';
import { IntegrationPipeline } from './IntegrationPipeline';
import { AssertionList } from '../../shared/AssertionRow';
import DisplayTable from '../buildModel/components/DisplayTable';
import {
  BORDER,
  INK,
  MUTED,
  SURFACE,
  TEAL,
  TEAL_SUBTLE,
} from '../../theme/tokens';

/* ─── ModeTabBtn ──────────────────────────────────────────────────── */
function ModeTabBtn({
  active,
  onClick,
  icon,
  title,
  sub,
  badge,
}: {
  active: boolean;
  onClick: () => void;
  icon: React.ReactNode;
  title: string;
  sub: string;
  badge?: string;
}) {
  return (
    <Box
      component="button"
      onClick={onClick}
      sx={{
        flex: 1,
        p: '11px 14px',
        bgcolor: active ? TEAL_SUBTLE : 'transparent',
        border: `1px solid ${active ? '#d2efec' : 'transparent'}`,
        borderRadius: '9px',
        cursor: 'pointer',
        display: 'flex',
        alignItems: 'center',
        gap: '11px',
        textAlign: 'left',
        fontFamily: 'inherit',
        '&:hover': { bgcolor: active ? TEAL_SUBTLE : '#f4f7f7' },
      }}
    >
      <Box sx={{ width: 32, height: 32, borderRadius: '8px', bgcolor: active ? TEAL : '#eef2f3', color: active ? '#fff' : MUTED, display: 'grid', placeItems: 'center', flexShrink: 0 }}>
        {icon}
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Box sx={{ display: 'flex', alignItems: 'center', gap: '7px' }}>
          <Typography sx={{ fontSize: 13.5, fontWeight: 600, color: active ? '#1f948d' : INK }}>
            {title}
          </Typography>
          {badge && (
            <Box sx={{ fontSize: 9.5, fontWeight: 700, color: '#1f948d', bgcolor: '#d2efec', px: '6px', py: '1px', borderRadius: 999, letterSpacing: 0.3 }}>
              {badge}
            </Box>
          )}
        </Box>
        <Typography sx={{ fontSize: 11.5, color: MUTED, mt: '1px' }}>{sub}</Typography>
      </Box>
    </Box>
  );
}

/* ─── IntegrationTestCard ─────────────────────────────────────────── */
function statusColor(status: string) {
  if (status === 'pass') return '#23a26d';
  if (status === 'fail') return '#d0503f';
  return '#d89323';
}
function statusBg(status: string) {
  if (status === 'pass') return '#e9f7f0';
  if (status === 'fail') return '#fbeceb';
  return '#fcf3e1';
}
function statusLabel(status: string) {
  if (status === 'pass') return 'Réussi';
  if (status === 'fail') return 'Échoué';
  return 'Erreur';
}
function StatusIcon({ status }: { status: string }) {
  if (status === 'pass') return <CheckCircleIcon sx={{ fontSize: 15, color: statusColor(status) }} />;
  if (status === 'fail') return <CancelIcon sx={{ fontSize: 15, color: statusColor(status) }} />;
  return <ErrorOutlineIcon sx={{ fontSize: 15, color: statusColor(status) }} />;
}

function IntegrationTestCard({ test, index }: { test: IntegrationTestItem; index: number }) {
  const [open, setOpen] = useState(false);
  const fg = statusColor(test.status);
  const bg = statusBg(test.status);
  const label = statusLabel(test.status);
  const inputData = test.data ?? {};

  return (
    <Box
      sx={{
        border: `1px solid ${BORDER}`,
        borderLeft: `3px solid ${fg}`,
        borderRadius: '10px',
        overflow: 'hidden',
        bgcolor: '#fff',
      }}
    >
      {/* Header row */}
      <Box
        onClick={() => setOpen((p) => !p)}
        sx={{
          display: 'grid',
          gridTemplateColumns: '22px 100px 1fr auto',
          alignItems: 'center',
          gap: 1,
          p: '9px 12px',
          cursor: 'pointer',
          '&:hover': { bgcolor: '#fafcfc' },
        }}
      >
        <StatusIcon status={test.status} />
        <Box
          sx={{
            display: 'inline-flex',
            alignItems: 'center',
            gap: '4px',
            bgcolor: bg,
            color: fg,
            px: '8px',
            py: '2px',
            borderRadius: 999,
            fontSize: 11,
            fontWeight: 700,
            justifySelf: 'start',
          }}
        >
          <StatusIcon status={test.status} />
          {label}
        </Box>
        <Box sx={{ minWidth: 0 }}>
          <Typography
            sx={{
              fontSize: 12.5,
              color: INK,
              fontWeight: 500,
              overflow: 'hidden',
              textOverflow: 'ellipsis',
              whiteSpace: 'nowrap',
            }}
          >
            <Typography component="span" sx={{ fontSize: 11, color: MUTED, mr: 0.75 }}>
              #{index + 1}
            </Typography>
            {test.title}
          </Typography>
        </Box>
        {test.rows_produced !== undefined && (
          <Typography sx={{ fontSize: 11, color: MUTED, flexShrink: 0 }}>
            {test.rows_produced} ligne{test.rows_produced !== 1 ? 's' : ''}
          </Typography>
        )}
      </Box>

      {/* Expanded details */}
      {open && (
        <Box sx={{ borderTop: `1px solid #eff3f4` }}>
          {/* Error message */}
          {test.error && (
            <Box sx={{ px: 2, py: 1.5 }}>
              <Typography sx={{ fontSize: 11.5, color: '#d0503f', fontFamily: 'monospace' }}>
                {test.error}
              </Typography>
            </Box>
          )}

          {/* Input data */}
          {Object.keys(inputData).length > 0 && (
            <Box sx={{ px: 2, pt: 1.5, pb: 1 }}>
              <Typography
                sx={{
                  fontSize: 10.5,
                  fontWeight: 700,
                  color: MUTED,
                  textTransform: 'uppercase',
                  letterSpacing: 0.6,
                  mb: 0.75,
                }}
              >
                Données d'entrée
              </Typography>
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1, overflowX: 'auto' }}>
                {Object.entries(inputData).map(([key, val]) => (
                  <DisplayTable key={key} jsonData={val} tableName={key} />
                ))}
              </Box>
            </Box>
          )}

          {/* Assertions */}
          {test.assertion_results.length > 0 && (
            <Box sx={{ px: 2, pb: 1.5 }}>
              <Typography
                sx={{
                  fontSize: 10.5,
                  fontWeight: 700,
                  color: MUTED,
                  textTransform: 'uppercase',
                  letterSpacing: 0.6,
                  mb: 0.75,
                }}
              >
                Assertions
              </Typography>
              <Box
                sx={{
                  border: `1px solid ${BORDER}`,
                  borderRadius: '8px',
                  overflow: 'hidden',
                }}
              >
                <AssertionList assertions={test.assertion_results} readOnly />
              </Box>
            </Box>
          )}

          {/* No assertions / pass without assertions */}
          {test.assertion_results.length === 0 && !test.error && (
            <Box sx={{ px: 2, py: 1.5 }}>
              <Typography sx={{ fontSize: 12, color: MUTED, fontStyle: 'italic' }}>
                Le pipeline s'est exécuté sans erreur — aucune assertion définie.
              </Typography>
            </Box>
          )}
        </Box>
      )}
    </Box>
  );
}

/* ─── IntegrationResultsPanel ────────────────────────────────────── */
function IntegrationResultsPanel({ result }: { result: IntegrationRunResult }) {
  const passed = result.tests.filter((t) => t.status === 'pass').length;
  const failed = result.tests.filter((t) => t.status !== 'pass').length;

  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
      {/* Summary bar */}
      <Box
        sx={{
          display: 'flex',
          alignItems: 'center',
          gap: 2,
          p: '12px 16px',
          bgcolor: SURFACE,
          border: `1px solid ${BORDER}`,
          borderRadius: '12px',
        }}
      >
        <Typography sx={{ fontSize: 13, fontWeight: 700, color: INK, flex: 1 }}>
          {result.name}
        </Typography>
        <Chip
          icon={<CheckCircleIcon sx={{ fontSize: 13, color: '#23a26d !important' }} />}
          label={`${passed} réussi${passed !== 1 ? 's' : ''}`}
          size="small"
          sx={{ bgcolor: '#e9f7f0', color: '#23a26d', fontWeight: 600, fontSize: 11 }}
        />
        {failed > 0 && (
          <Chip
            icon={<CancelIcon sx={{ fontSize: 13, color: '#d0503f !important' }} />}
            label={`${failed} échoué${failed !== 1 ? 's' : ''}`}
            size="small"
            sx={{ bgcolor: '#fbeceb', color: '#d0503f', fontWeight: 600, fontSize: 11 }}
          />
        )}
        <Typography sx={{ fontSize: 11.5, color: MUTED }}>
          {result.total} test{result.total !== 1 ? 's' : ''}
        </Typography>
      </Box>

      {/* Test cards */}
      {result.tests.length === 0 ? (
        <Box sx={{ p: 3, textAlign: 'center' }}>
          <Typography sx={{ fontSize: 13, color: MUTED, fontStyle: 'italic' }}>
            Aucun test défini dans le fichier d'intégration.
            <br />
            Ajoutez des entrées <code>tests:</code> dans le YAML pour définir des scénarios.
          </Typography>
        </Box>
      ) : (
        result.tests.map((test, i) => (
          <IntegrationTestCard key={i} test={test} index={i} />
        ))
      )}
    </Box>
  );
}

/* ─── ExistingFileCard ────────────────────────────────────────────── */
function ExistingFileCard({
  filename,
  onSelect,
}: {
  filename: string;
  onSelect: () => void;
}) {
  return (
    <Box
      onClick={onSelect}
      sx={{
        display: 'flex',
        alignItems: 'center',
        gap: 2,
        px: 2.5,
        py: 2,
        border: `1px solid ${BORDER}`,
        borderRadius: '12px',
        bgcolor: '#fff',
        cursor: 'pointer',
        '&:hover': { borderColor: TEAL, bgcolor: TEAL_SUBTLE },
        transition: 'all .12s',
      }}
    >
      <Box
        sx={{
          width: 34,
          height: 34,
          borderRadius: '9px',
          bgcolor: TEAL_SUBTLE,
          color: TEAL,
          display: 'grid',
          placeItems: 'center',
          flexShrink: 0,
        }}
      >
        <AccountTreeIcon sx={{ fontSize: 17 }} />
      </Box>
      <Box sx={{ flex: 1, minWidth: 0 }}>
        <Typography
          sx={{ fontSize: 13, fontWeight: 600, color: INK, fontFamily: 'monospace' }}
        >
          {filename}
        </Typography>
      </Box>
      <PlayArrowIcon sx={{ fontSize: 16, color: MUTED, flexShrink: 0 }} />
    </Box>
  );
}

/* ─── IntegrationPage ─────────────────────────────────────────────── */
const IntegrationPage: React.FC = () => {
  const dispatch = useAppDispatch();
  const drawerOpen = useAppSelector((s) => s.appBarModel.drawerOpen);
  const currentProjectId = useAppSelector((s) => s.appBarModel.currentProjectId);

  useEffect(() => {
    dispatch(setWorkspaceMode(false));
  }, [dispatch]);
  const sqlFiles = useSqlFileLoader();

  // Tab: existing files or build new chain
  const [tab, setTab] = useState<'existing' | 'build'>('existing');

  // Existing integration files
  const [integrationFiles, setIntegrationFiles] = useState<string[]>([]);
  const [filesLoading, setFilesLoading] = useState(true);

  // New chain builder state
  const [newName, setNewName] = useState('');
  const [newChain, setNewChain] = useState<IntegrationStep[]>([]);

  // Run results
  const [runResult, setRunResult] = useState<IntegrationRunResult | null>(null);
  const [runChain, setRunChain] = useState<IntegrationStep[]>([]);
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    setFilesLoading(true);
    fetchIntegrationFiles()
      .then(setIntegrationFiles)
      .finally(() => setFilesLoading(false));
  }, []);

  const reloadFiles = useCallback(async () => {
    const files = await fetchIntegrationFiles();
    setIntegrationFiles(files);
  }, []);

  const handleRunExisting = useCallback(
    async (filename: string) => {
      setRunError(null);
      setRunResult(null);
      setRunning(true);
      try {
        const sourceData = await fetchIntegrationSourceTables(filename);
        setRunChain(sourceData.chain ?? []);
        const result = await runIntegrationApi(filename, currentProjectId);
        result.chain = sourceData.chain;
        setRunResult(result);
      } catch (e: any) {
        setRunError(e?.detail ?? 'Erreur lors de l\'exécution');
      } finally {
        setRunning(false);
      }
    },
    [currentProjectId],
  );

  const handleSaveAndRun = useCallback(async () => {
    const validSteps = newChain.filter((s) => s.sql && s.produces);
    if (validSteps.length === 0) {
      setRunError('Définissez au moins une étape complète (script + table produite).');
      return;
    }
    const filename = (newName.trim().replace(/\s+/g, '_').toLowerCase() || 'integration') + '.yml';
    setSaving(true);
    setRunError(null);
    try {
      await saveIntegrationChain({ filename, name: newName.trim() || filename, chain: validSteps });
      await reloadFiles();
      setRunChain(validSteps);
      setRunning(true);
      const result = await runIntegrationApi(filename, currentProjectId);
      result.chain = validSteps;
      setRunResult(result);
    } catch (e: any) {
      setRunError(e?.detail ?? 'Erreur lors de l\'enregistrement');
    } finally {
      setSaving(false);
      setRunning(false);
    }
  }, [newChain, newName, currentProjectId, reloadFiles]);

  const handleBack = () => {
    setRunResult(null);
    setRunChain([]);
    setRunError(null);
  };

  const isLoading = running || saving;

  /* ── Result view ──────────────────────────────────────────────── */
  if (runResult) {
    return (
      <Box
        sx={{
          height: '100vh',
          maxWidth: `calc(100vw - ${drawerOpen ? drawerWidth : 0}px)`,
          transition: 'max-width 0.2s ease',
          display: 'flex',
          flexDirection: 'column',
          overflow: 'hidden',
        }}
      >
        {/* Topbar */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: 1.5,
            px: 3,
            py: 1.5,
            borderBottom: `1px solid ${BORDER}`,
            bgcolor: '#fff',
            flexShrink: 0,
          }}
        >
          <Button
            size="small"
            startIcon={<ArrowBackIcon sx={{ fontSize: 14 }} />}
            onClick={handleBack}
            sx={{ textTransform: 'none', fontSize: 12.5, color: MUTED }}
          >
            Retour
          </Button>
          <Divider orientation="vertical" flexItem />
          <AccountTreeIcon sx={{ fontSize: 16, color: TEAL }} />
          <Typography sx={{ fontSize: 13.5, fontWeight: 700, color: INK, flex: 1 }}>
            Tests d'intégration
          </Typography>
          <Chip
            label="DuckDB local · 0 € facturé"
            size="small"
            sx={{ fontSize: 11, bgcolor: TEAL_SUBTLE, color: TEAL, fontWeight: 600 }}
          />
        </Box>

        <Box sx={{ flex: 1, overflow: 'auto', px: 3, py: 2.5, display: 'flex', flexDirection: 'column', gap: 3 }}>
          {/* Pipeline visualization */}
          <Box>
            <Typography
              sx={{
                fontSize: 10,
                fontWeight: 700,
                color: MUTED,
                textTransform: 'uppercase',
                letterSpacing: 0.7,
                mb: 1,
              }}
            >
              Chaîne de traitement — cliquer pour voir le SQL
            </Typography>
            <IntegrationPipeline chain={runChain} />
          </Box>

          <Divider />

          {/* Test results */}
          <IntegrationResultsPanel result={runResult} />
        </Box>
      </Box>
    );
  }

  /* ── Entry view ──────────────────────────────────────────────── */
  return (
    <Box
      sx={{
        height: '100vh',
        maxWidth: `calc(100vw - ${drawerOpen ? drawerWidth : 0}px)`,
        transition: 'max-width 0.2s ease',
        overflow: 'auto',
      }}
    >
      <Box sx={{ maxWidth: 720, mx: 'auto', mt: 6, px: 3, pb: 6 }}>
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 2, mb: 4 }}>
          <Box
            sx={{
              width: 40,
              height: 40,
              borderRadius: '11px',
              bgcolor: TEAL_SUBTLE,
              color: TEAL,
              display: 'grid',
              placeItems: 'center',
              flexShrink: 0,
            }}
          >
            <AccountTreeIcon sx={{ fontSize: 20 }} />
          </Box>
          <Box>
            <Typography sx={{ fontSize: 22, fontWeight: 700, color: INK, lineHeight: 1.2 }}>
              Tests d'intégration
            </Typography>
            <Typography sx={{ fontSize: 13, color: MUTED, mt: 0.5 }}>
              Enchaînez plusieurs scripts SQL et vérifiez le résultat final — exécution DuckDB locale, 0 € facturé.
            </Typography>
          </Box>
        </Box>

        {/* Error */}
        {runError && (
          <Alert severity="error" sx={{ mb: 2, borderRadius: '10px' }} onClose={() => setRunError(null)}>
            {runError}
          </Alert>
        )}

        {/* Loading overlay */}
        {isLoading && (
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 3, p: '12px 16px', bgcolor: TEAL_SUBTLE, borderRadius: '10px' }}>
            <CircularProgress size={16} sx={{ color: TEAL }} />
            <Typography sx={{ fontSize: 12.5, color: TEAL, fontWeight: 500 }}>
              {saving ? 'Enregistrement de la chaîne…' : 'Exécution des tests…'}
            </Typography>
          </Box>
        )}

        {/* Mode switcher */}
        <Box sx={{ mb: 2.5, display: 'flex', gap: 0, bgcolor: '#fff', border: `1px solid ${BORDER}`, borderRadius: '12px', p: '4px' }}>
          <ModeTabBtn
            active={tab === 'existing'}
            onClick={() => setTab('existing')}
            icon={<FolderOpenIcon sx={{ fontSize: 16 }} />}
            title="Fichiers existants"
            sub={integrationFiles.length > 0 ? `${integrationFiles.length} fichier${integrationFiles.length > 1 ? 's' : ''}` : 'Aucun fichier'}
          />
          <ModeTabBtn
            active={tab === 'build'}
            onClick={() => setTab('build')}
            icon={<AddIcon sx={{ fontSize: 16 }} />}
            title="Créer une chaîne"
            sub="Enchaîner des scripts SQL"
            badge="Nouveau"
          />
        </Box>

        {/* ── Existing files tab ──────────────────────────────── */}
        {tab === 'existing' && (
          <>
            {filesLoading ? (
              <Box sx={{ display: 'flex', justifyContent: 'center', mt: 8 }}>
                <CircularProgress size={24} sx={{ color: TEAL }} />
              </Box>
            ) : integrationFiles.length === 0 ? (
              <Box
                sx={{
                  textAlign: 'center',
                  py: 8,
                  border: `1.5px dashed ${BORDER}`,
                  borderRadius: '14px',
                }}
              >
                <AccountTreeIcon sx={{ fontSize: 36, color: MUTED, mb: 1 }} />
                <Typography sx={{ fontSize: 14, color: MUTED, mb: 0.5 }}>
                  Aucun fichier d'intégration trouvé
                </Typography>
                <Typography sx={{ fontSize: 12.5, color: MUTED }}>
                  Créez votre première chaîne dans l'onglet "Créer une chaîne", ou déposez un fichier
                  <code style={{ fontSize: 11 }}>.yml</code> dans <code style={{ fontSize: 11 }}>.mocksql/integration/</code>.
                </Typography>
              </Box>
            ) : (
              <Box sx={{ display: 'flex', flexDirection: 'column', gap: 1.5 }}>
                {integrationFiles.map((filename) => (
                  <ExistingFileCard
                    key={filename}
                    filename={filename}
                    onSelect={() => handleRunExisting(filename)}
                  />
                ))}
              </Box>
            )}
          </>
        )}

        {/* ── Build new chain tab ─────────────────────────────── */}
        {tab === 'build' && (
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2.5 }}>
            {/* Name field */}
            <Box
              component="input"
              placeholder="Nom de la chaîne (ex: pipeline_revenue)"
              value={newName}
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewName(e.target.value)}
              sx={{ p: '9px 13px', fontSize: 13, border: `1px solid ${BORDER}`, borderRadius: '9px', bgcolor: '#fff', color: INK, outline: 'none', fontFamily: 'inherit', '&:focus': { borderColor: TEAL, boxShadow: `0 0 0 2px ${TEAL_SUBTLE}` } }}
            />

            {/* Explanation info card */}
            <Box sx={{ fontSize: 12.5, color: '#3b4f52', lineHeight: 1.5, p: '10px 12px', bgcolor: '#eef1f7', border: '1px solid #d6def0', borderRadius: '10px', display: 'flex', alignItems: 'flex-start', gap: '9px' }}>
              <AccountTreeIcon sx={{ fontSize: 14, color: '#5160a0', mt: '1px', flexShrink: 0 }} />
              <Box>
                Pour chaque script ajouté, indique la <strong>table</strong> ou la <strong>vue</strong> qu'il produit — les étapes suivantes pourront la référencer.
                Le dernier script est considéré comme la sortie finale ; la chaîne entière est testée comme une seule requête.
              </Box>
            </Box>

            {/* Chain builder */}
            <Box>
              <Typography sx={{ fontSize: 11, fontWeight: 700, color: MUTED, textTransform: 'uppercase', letterSpacing: 0.6, mb: 1 }}>
                Étapes de la chaîne
              </Typography>
              <ChainBuilder steps={newChain} sqlFiles={sqlFiles} onChange={setNewChain} />
            </Box>

            {/* Launch button */}
            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mt: 0.5 }}>
              <Typography sx={{ fontSize: 11.5, color: MUTED, display: 'inline-flex', alignItems: 'center', gap: '5px' }}>
                Exécuté sur DuckDB en local — zéro coût BigQuery
              </Typography>
              <Tooltip title={newChain.filter(s => s.sql && s.produces).length >= 2 ? '' : 'Ajoutez au moins 2 étapes complètes avant de lancer'}>
                <span>
                  <Button
                    variant="contained"
                    startIcon={isLoading ? <CircularProgress size={14} sx={{ color: '#fff' }} /> : <PlayArrowIcon />}
                    onClick={handleSaveAndRun}
                    disabled={isLoading || newChain.filter(s => s.sql && s.produces).length < 2}
                    sx={{ bgcolor: TEAL, '&:hover': { bgcolor: '#159e9a' }, textTransform: 'none', borderRadius: 2, px: 3 }}
                  >
                    {saving ? 'Enregistrement…' : running ? 'Exécution…' : 'Enregistrer & Lancer'}
                  </Button>
                </span>
              </Tooltip>
            </Box>
          </Box>
        )}
      </Box>
    </Box>
  );
};

export default IntegrationPage;
