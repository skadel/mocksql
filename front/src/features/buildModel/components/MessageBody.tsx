import React, { useRef, useState } from 'react';
import ReactMarkdown from 'react-markdown';
import { Accordion, AccordionDetails, AccordionSummary, Alert, Box, Chip, Collapse, Stack, Table, TableBody, TableCell, TableHead, TableRow, Typography } from '@mui/material';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import SearchIcon from '@mui/icons-material/Search';
import DownloadIcon from '@mui/icons-material/Download';
import UploadFileIcon from '@mui/icons-material/UploadFile';
import CheckRoundedIcon from '@mui/icons-material/CheckRounded';
import WarningAmberRoundedIcon from '@mui/icons-material/WarningAmberRounded';
import CloseRoundedIcon from '@mui/icons-material/CloseRounded';
import AutoAwesomeIcon from '@mui/icons-material/AutoAwesome';
import DisplayTable from './DisplayTable';
import QueryUnderstandingCard from './QueryUnderstandingCard';
import { StyledButton } from '../../../style/StyledComponents';
import type { DebugCountStep, DebugCountStepsResult, DebugRunCteResult, DiagnosticBlock, Message } from '../../../utils/types';

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

const markdownBodySx = {
  fontSize: 13,
  lineHeight: 1.5,
  color: '#333',
  '& p': { margin: '0 0 6px 0' },
  '& p:last-child': { marginBottom: 0 },
  '& strong': { fontWeight: 700 },
  '& ul, & ol': { paddingLeft: '1.5em', marginTop: '4px', marginBottom: '4px' },
  '& li': { marginBottom: '2px' },
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

const DIAG_ROWS: Array<{ key: keyof DiagnosticBlock; label: string }> = [
  { key: 'root_cause', label: 'Cause' },
  { key: 'sql_pattern', label: 'Pattern' },
  { key: 'data_issue', label: 'Données' },
  { key: 'fix_summary', label: 'Fix' },
  { key: 'affected_tables', label: 'Tables' },
  { key: 'affected_ctes', label: 'CTEs' },
];

const BadDataDiagnosticAccordion: React.FC<{ diagnostic: DiagnosticBlock }> = ({ diagnostic }) => (
  <Accordion
    disableGutters
    defaultExpanded={false}
    data-testid="bad-data-diagnostic"
    sx={{
      boxShadow: 'none',
      border: 'none',
      bgcolor: 'grey.50',
      borderRadius: '8px !important',
      mt: 1,
      '&:before': { display: 'none' },
    }}
  >
    <AccordionSummary
      expandIcon={<ExpandMoreIcon sx={{ fontSize: 16, color: '#888' }} />}
      sx={{ minHeight: 32, py: 0, px: 1.5, '& .MuiAccordionSummary-content': { my: 0.5 } }}
    >
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.75 }}>
        <SearchIcon sx={{ fontSize: 14, color: '#888' }} />
        <Typography variant="caption" sx={{ color: '#888', fontWeight: 600, letterSpacing: '0.03em' }}>
          Analyse diagnostique
        </Typography>
      </Box>
    </AccordionSummary>
    <AccordionDetails sx={{ px: 1.5, pb: 1.5, pt: 0, maxHeight: 300, overflowY: 'auto' }}>
      <Table size="small">
        <TableBody>
          {DIAG_ROWS.map(({ key, label }) => {
            const val = diagnostic[key];
            const display = Array.isArray(val) ? (val.length > 0 ? val.join(', ') : '—') : (val || '—');
            return (
              <TableRow key={key} sx={{ '&:last-child td': { border: 0 }, verticalAlign: 'top' }}>
                <TableCell sx={{ fontSize: 11, fontWeight: 700, color: '#666', py: 0.5, px: 0.75, width: 72, whiteSpace: 'nowrap', border: 0 }}>
                  {label}
                </TableCell>
                <TableCell sx={{ fontSize: 12, color: '#333', py: 0.5, px: 0.75, lineHeight: 1.5, border: 0 }}>
                  {display}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
    </AccordionDetails>
  </Accordion>
);

/* ----------------------------------------------------------------------------
 * Rendu des résultats de test — aligné sur le redesign « Chat » (design system).
 * Chaque test = une carte `.trow` : pastille de statut (icône, pas emoji),
 * identifiant mono, description, pastille de résultat, et une « Réflexion »
 * imbriquée (aperçu des lignes retournées).
 * -------------------------------------------------------------------------- */
const MONO = "'JetBrains Mono', ui-monospace, 'SF Mono', Menlo, Consolas, monospace";

const STATUS_CFG = {
  pass: { bg: '#e9f7f0', color: '#1c855a' },
  warn: { bg: '#fcf3e1', color: '#a86a00' },
  fail: { bg: '#fbeceb', color: '#b23a32' },
} as const;

type StatusKind = keyof typeof STATUS_CFG;

const StatusMarker: React.FC<{ kind: StatusKind }> = ({ kind }) => {
  const cfg = STATUS_CFG[kind];
  const Icon = kind === 'pass' ? CheckRoundedIcon : kind === 'warn' ? WarningAmberRoundedIcon : CloseRoundedIcon;
  return (
    <Box
      sx={{
        flexShrink: 0, mt: '1px', width: 19, height: 19, borderRadius: '999px',
        bgcolor: cfg.bg, color: cfg.color, display: 'grid', placeItems: 'center',
      }}
    >
      <Icon sx={{ fontSize: 12 }} />
    </Box>
  );
};

/** « Réflexion » imbriquée : aperçu des lignes (en mono) sous une carte de test. */
const TestDataPeek: React.FC<{ caption: string; rows: Record<string, any>[] }> = ({ caption, rows }) => {
  const [open, setOpen] = useState(false);
  const cols = rows.length > 0 ? Object.keys(rows[0]) : [];
  if (cols.length === 0) return null;
  return (
    <Box sx={{ mt: 1 }}>
      <Box
        onClick={() => setOpen((o) => !o)}
        sx={{
          display: 'inline-flex', alignItems: 'center', gap: 0.75, cursor: 'pointer',
          color: '#6b8287', '&:hover': { color: '#3b5357' },
        }}
      >
        <AutoAwesomeIcon sx={{ fontSize: 13, color: '#2bb0a8' }} />
        <Typography component="span" sx={{ fontSize: 11.5, fontWeight: 600, color: 'inherit' }}>
          Réflexion
        </Typography>
        <ExpandMoreIcon sx={{ fontSize: 13, color: '#8da0a4', transform: open ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
      </Box>
      <Collapse in={open}>
        <Box sx={{ pt: 0.75 }}>
          <Typography sx={{ fontFamily: MONO, fontSize: 11, fontWeight: 600, color: '#4f676b', mb: 0.5 }}>
            {caption}
          </Typography>
          <Box sx={{ overflowX: 'auto', border: '1px solid #dae2e4', borderRadius: '8px' }}>
            <Table
              size="small"
              sx={{ '& td, & th': { fontFamily: MONO, fontSize: 11, whiteSpace: 'nowrap', py: '5px', px: '9px', borderColor: '#dae2e4' } }}
            >
              <TableHead>
                <TableRow sx={{ bgcolor: '#f3f6f7' }}>
                  {cols.map((c) => (
                    <TableCell key={c} sx={{ color: '#6b8287 !important', fontWeight: '600 !important' }}>{c}</TableCell>
                  ))}
                </TableRow>
              </TableHead>
              <TableBody>
                {rows.map((r, i) => (
                  <TableRow key={i} sx={{ '&:last-child td': { border: 0 } }}>
                    {cols.map((c) => (
                      <TableCell key={c} sx={{ color: '#1f3a3e !important' }}>
                        {r[c] === null || r[c] === undefined ? <em style={{ color: '#aab8bb' }}>null</em> : String(r[c])}
                      </TableCell>
                    ))}
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Box>
        </Box>
      </Collapse>
    </Box>
  );
};

const TestResultRow: React.FC<{ testResult: any; index: number }> = ({ testResult, index }) => {
  const status = testResult.status as string | undefined;
  const isComplete = status === 'complete';
  const isEmpty = status === 'empty_results';
  const expectsEmpty = isEmpty && /retourne\s+.{0,40}vide|résultat[s]?\s+(?:est\s+)?vide[s]?|0\s+ligne|aucune\s+ligne/.test(
    (testResult.unit_test_description ?? '').toLowerCase()
  );
  const isSuccess = isComplete || expectsEmpty;

  let rows: Record<string, any>[] = [];
  if (isComplete) {
    try { rows = JSON.parse(testResult.results_json ?? '[]'); } catch { rows = []; }
  }
  const rowCount = rows.length;

  const kind: StatusKind = isSuccess ? 'pass' : isEmpty ? 'warn' : 'fail';
  const chipLabel = isComplete ? `${rowCount} ligne${rowCount > 1 ? 's' : ''}` : isEmpty ? 'Vide' : 'Erreur';
  const cfg = STATUS_CFG[kind];

  return (
    <Box sx={{ borderRadius: '10px', p: '10px 11px', bgcolor: '#fff', border: '1px solid #dae2e4' }}>
      <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.1 }}>
        <StatusMarker kind={kind} />
        <Box sx={{ flex: 1, minWidth: 0 }}>
          <Typography sx={{ fontFamily: MONO, fontSize: 11, fontWeight: 600, color: '#6b8287', letterSpacing: '0.02em' }}>
            Test {(testResult.test_index ?? index) + 1}
          </Typography>
          {testResult.unit_test_description && (
            <Typography sx={{ fontSize: 12, lineHeight: 1.5, color: '#3b5357', mt: '2px' }}>
              {testResult.unit_test_description}
            </Typography>
          )}
        </Box>
        <Box
          sx={{
            flexShrink: 0, fontSize: 11, fontWeight: 600, borderRadius: '999px',
            px: '9px', py: '3px', bgcolor: cfg.bg, color: cfg.color, whiteSpace: 'nowrap',
          }}
        >
          {chipLabel}
        </Box>
      </Box>
      {isComplete && rowCount > 0 && (
        <TestDataPeek caption={`résultat — ${rowCount} ligne${rowCount > 1 ? 's' : ''}`} rows={rows} />
      )}
    </Box>
  );
};

const MessageBody: React.FC<MessageBodyProps> = ({
  msg,
  currentProjectId,
  onProfileUpload,
  onPageChange,
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
      {/* Carte "Compréhension de la requête" */}
      {msg.contents.understanding && (
        <QueryUnderstandingCard understanding={msg.contents.understanding} />
      )}

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
            ...markdownBodySx,
            mt: 1,
            p: 1.5,
            bgcolor: '#f8fffe',
            borderRadius: '8px',
            border: '1px solid #c8e6e4',
            borderLeft: '3px solid #1ca8a4',
            '& strong': { color: '#1ca8a4' },
          }}
        >
          <ReactMarkdown>{msg.contents.text}</ReactMarkdown>
        </Box>
      )}

      {/* Correction de test — séparateur "Test modifié" + scénario */}
      {msg.contentType === 'generate_test_scenario' && msg.contents.text && (
        <Box>
          {/* Séparateur visuel — accent teal (redesign Chat) */}
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, my: 1 }}>
            <Box sx={{ flex: 1, height: '1px', bgcolor: '#d2efec' }} />
            <Chip
              label={msg.contents.action === 'add' ? 'Nouveau test' : 'Test modifié'}
              size="small"
              sx={{
                fontSize: 10,
                height: 20,
                bgcolor: '#ecf7f6',
                color: '#16746e',
                border: '1px solid #d2efec',
                fontWeight: 700,
                letterSpacing: '0.05em',
                textTransform: 'uppercase',
              }}
            />
            <Box sx={{ flex: 1, height: '1px', bgcolor: '#d2efec' }} />
          </Box>

          {/* Réflexion collapsible — sparkles + chevron (redesign Chat) */}
          {msg.contents.reasoning && (
            <Box sx={{ mb: 0.75 }}>
              <Box
                onClick={() => setReasoningOpen(o => !o)}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: 0.75, cursor: 'pointer',
                  color: '#6b8287', '&:hover': { color: '#3b5357' },
                }}
              >
                <AutoAwesomeIcon sx={{ fontSize: 13, color: '#2bb0a8' }} />
                <Typography component="span" sx={{ fontSize: 11.5, fontWeight: 600, color: 'inherit' }}>
                  Réflexion
                </Typography>
                <ExpandMoreIcon sx={{ fontSize: 13, color: '#8da0a4', transform: reasoningOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
              </Box>
              <Collapse in={reasoningOpen}>
                <Typography variant="caption" sx={{ display: 'block', mt: 0.5, color: '#6b8287', fontStyle: 'italic', lineHeight: 1.5, whiteSpace: 'pre-wrap' }}>
                  {msg.contents.reasoning}
                </Typography>
              </Collapse>
            </Box>
          )}

          {/* Description du scénario de correction — carte teal (redesign Chat) */}
          <Box
            sx={{
              p: 1.25,
              bgcolor: '#ecf7f6',
              borderRadius: '10px',
              border: '1px solid #d2efec',
            }}
          >
            <Typography sx={{ fontSize: 12.5, color: '#16746e', fontWeight: 700, mb: 0.5 }}>
              {msg.contents.action === 'add' ? 'Nouveau scénario ajouté' : 'Modification pour le scénario suivant'}
            </Typography>
            <Typography variant="body2" sx={{ color: '#3b5357', lineHeight: 1.55 }}>
              {msg.contents.text}
            </Typography>
          </Box>

          {/* Évaluation LLM consolidée dans la même bulle */}
          {msg.contents.evaluationText && (
            <Box
              sx={{
                ...markdownBodySx,
                mt: 1,
                p: 1.5,
                bgcolor: '#f8fffe',
                borderRadius: '8px',
                border: '1px solid #c8e6e4',
                borderLeft: '3px solid #1ca8a4',
                '& strong': { color: '#1ca8a4' },
              }}
            >
              <ReactMarkdown>{msg.contents.evaluationText}</ReactMarkdown>
            </Box>
          )}
          {/* Les suggestions ne sont plus rendues dans le fil : panneau dédié (TestsPanel). */}
        </Box>
      )}

      {/* Confirmation de mise à jour de la description d'un test */}
      {msg.contentType === 'update_test' && (
        <Box
          sx={{
            display: 'inline-flex',
            alignItems: 'center',
            mt: 1,
            px: 1.5,
            py: 0.75,
            bgcolor: '#f0fafa',
            borderRadius: '10px',
            border: '1px solid #d0eeec',
          }}
        >
          <Typography variant="body2" sx={{ fontWeight: 700, color: '#1ca8a4' }}>
            ✅ Description du test n°{((msg.contents as any).testIndex ?? 0) + 1} mise à jour
          </Typography>
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
          <Box sx={{ mt: 0.5, overflowX: 'auto', ...markdownBodySx }}>
            <ReactMarkdown>{msg.contents.text}</ReactMarkdown>
          </Box>
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

      {/* Suggestions : déplacées hors du fil → panneau dédié (TestsPanel). */}

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

      {/* Résultats d'exécution des tests unitaires — cartes `.trow` (redesign Chat) */}
      {Array.isArray(msg.contents.res) && msg.contents.res.length > 0 &&
        (msg.contents.res as any[]).some((r) => 'results_json' in r) && (
          <Box sx={{ mt: 1, display: 'flex', flexDirection: 'column', gap: 0.5 }}>
            {(msg.contents.res as any[]).map((testResult, i) => (
              <TestResultRow key={testResult.test_index ?? i} testResult={testResult} index={i} />
            ))}
          </Box>
      )}
      {/* Suggestions : rendues hors du fil → panneau dédié (TestsPanel). */}

      {/* Debug — standalone fallback (edge case: message rendered directly) */}
      {msg.contents.debugRunCte && <DebugRunCteContent d={msg.contents.debugRunCte} />}
      {msg.contents.debugCountSteps && <DebugCountStepsContent d={msg.contents.debugCountSteps} />}

      {/* Debug — résultats collapsés (rattachés au message parent) */}
      {debugMessages && debugMessages.length > 0 && (
        <Box sx={{ mt: 0.75 }}>
          <Box
            onClick={() => setDebugOpen(o => !o)}
            sx={{
              display: 'inline-flex', alignItems: 'center', gap: 0.75, cursor: 'pointer',
              color: '#6b8287', '&:hover': { color: '#3b5357' },
            }}
          >
            <AutoAwesomeIcon sx={{ fontSize: 13, color: '#2bb0a8' }} />
            <Typography component="span" sx={{ fontSize: 11.5, fontWeight: 600, color: 'inherit' }}>
              Réflexion
            </Typography>
            <ExpandMoreIcon sx={{ fontSize: 13, color: '#8da0a4', transform: debugOpen ? 'rotate(180deg)' : 'none', transition: 'transform 0.2s' }} />
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

      {/* Diagnostic bad_data — accordéon collapsé */}
      {msg.contentType === 'bad_data_diagnostic' && msg.contents.diagnostic && (
        <BadDataDiagnosticAccordion diagnostic={msg.contents.diagnostic} />
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
