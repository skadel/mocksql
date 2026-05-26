import SkipNextIcon from '@mui/icons-material/SkipNext';
import InfoOutlinedIcon from '@mui/icons-material/InfoOutlined';
import CheckCircleOutlineIcon from '@mui/icons-material/CheckCircleOutline';
import {
  Box,
  Chip,
  LinearProgress,
  Paper,
  Step,
  StepLabel,
  Stepper,
  Stack,
  Tooltip,
  Typography,
} from '@mui/material';
import { NeutralButton, PrimaryButton } from '../../../style/AppButtons';
import React, { useState } from 'react';
import BigQueryUploader from '../../../shared/BigQueryUploader';
import { ProfileRequest } from '../../../utils/types';

interface ProfilingStepProps {
  profileRequest: ProfileRequest;
  messageId: string;
  parentId: string | undefined;
  onUpload: (messageId: string, parentId: string | undefined, jsonContent: string) => void;
  onSkip: () => void;
  loading?: boolean;
  loading_message?: string;
}

const STEPS = ['Requête SQL', 'Profiling', 'Tests & Chat'];

// Single-query layout (existing behaviour).
const SingleQueryLayout: React.FC<{
  profileRequest: ProfileRequest;
  messageId: string;
  parentId: string | undefined;
  onUpload: (messageId: string, parentId: string | undefined, jsonContent: string) => void;
  loading?: boolean;
}> = ({ profileRequest, messageId, parentId, onUpload, loading }) => (
  <BigQueryUploader
    sqlQuery={profileRequest.profile_query}
    onFileContent={(content) => onUpload(messageId, parentId, content)}
    accept=".json"
    disabled={loading}
    uploadLabel="Uploader les résultats JSON"
    instructionsTitle="Instructions pour le profiling"
    downloadFormat="JSON"
    inline
  />
);

// Multi-query layout: one upload slot per table, merge on submit.
const MultiQueryLayout: React.FC<{
  profileRequest: ProfileRequest;
  queries: string[];
  messageId: string;
  parentId: string | undefined;
  onUpload: (messageId: string, parentId: string | undefined, jsonContent: string) => void;
  loading?: boolean;
}> = ({ profileRequest, queries, messageId, parentId, onUpload, loading }) => {
  const [slots, setSlots] = useState<(any[] | null)[]>(() => queries.map(() => null));

  const handleSlotUpload = (idx: number, content: string) => {
    try {
      const rows = JSON.parse(content);
      setSlots((prev) => {
        const next = [...prev];
        next[idx] = Array.isArray(rows) ? rows : [rows];
        return next;
      });
    } catch {
      // ignore malformed JSON — slot stays null
    }
  };

  const handleSubmit = () => {
    const merged = slots.flatMap((s) => s ?? []);
    onUpload(messageId, parentId, JSON.stringify(merged));
  };

  const uploadedCount = slots.filter(Boolean).length;
  const canSubmit = uploadedCount > 0 && !loading;

  return (
    <Stack gap={2}>
      <Typography variant="body2" sx={{ color: '#555' }}>
        Exécutez chaque requête dans <strong>BigQuery</strong>, uploadez les résultats JSON, puis cliquez sur <strong>Soumettre</strong>.
        Vous pouvez soumettre même si certaines requêtes échouent — le profiling sera partiel.
      </Typography>

      {queries.map((sql, i) => {
        const tableEntry = profileRequest.missing_columns[i];
        const label = tableEntry ? tableEntry.table : `Requête ${i + 1}`;
        const done = !!slots[i];

        return (
          <Paper
            key={i}
            variant="outlined"
            sx={{ p: 2, borderRadius: 2, borderColor: done ? '#1ca8a4' : undefined }}
          >
            <Stack direction="row" alignItems="center" gap={1} sx={{ mb: 1.5 }}>
              {done && <CheckCircleOutlineIcon sx={{ color: '#1ca8a4', fontSize: 18 }} />}
              <Typography variant="caption" sx={{ fontWeight: 700, color: '#333', fontFamily: 'monospace' }}>
                {label}
              </Typography>
              {done && (
                <Chip
                  label="Uploadé"
                  size="small"
                  sx={{ bgcolor: '#e8f5f5', color: '#1ca8a4', border: '1px solid #b2e0de', fontSize: 11 }}
                />
              )}
            </Stack>
            <BigQueryUploader
              sqlQuery={sql}
              onFileContent={(content) => handleSlotUpload(i, content)}
              accept=".json"
              disabled={loading}
              uploadLabel={done ? 'Remplacer les résultats' : 'Uploader les résultats JSON'}
              instructionsTitle={`Instructions — ${label}`}
              downloadFormat="JSON"
              inline
            />
          </Paper>
        );
      })}

      <PrimaryButton onClick={handleSubmit} disabled={!canSubmit}>
        Soumettre ({uploadedCount}/{queries.length} requêtes)
      </PrimaryButton>
    </Stack>
  );
};

const ProfilingStep: React.FC<ProfilingStepProps> = ({
  profileRequest,
  messageId,
  parentId,
  onUpload,
  onSkip,
  loading,
  loading_message,
}) => {
  const queries = profileRequest.profile_queries;
  const isMulti = queries && queries.length > 1;

  return (
    <Box
      sx={{
        flex: 1,
        overflow: 'auto',
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        pt: 5,
        px: 2,
      }}
    >
      {/* Stepper */}
      <Box sx={{ width: '100%', maxWidth: 560, mb: 4 }}>
        <Stepper activeStep={1}>
          {STEPS.map((label, i) => (
            <Step key={label} completed={i < 1}>
              <StepLabel
                StepIconProps={{
                  sx: {
                    '&.Mui-active': { color: '#1ca8a4' },
                    '&.Mui-completed': { color: '#1ca8a4' },
                  },
                }}
              >
                {label}
              </StepLabel>
            </Step>
          ))}
        </Stepper>
      </Box>

      {/* Main card */}
      <Paper
        elevation={2}
        sx={{ width: '100%', maxWidth: 640, borderRadius: 3, p: 3, mb: 2 }}
      >
        <Stack direction="row" alignItems="center" gap={1} sx={{ mb: 0.5 }}>
          <Typography variant="h6" sx={{ fontWeight: 700 }}>
            Profiling des colonnes
          </Typography>
          {profileRequest.billing_tb !== undefined && (
            <Tooltip title="Estimation BigQuery du coût total des requêtes de profiling (dry run)">
              <Chip
                icon={<InfoOutlinedIcon sx={{ fontSize: 14, color: '#1ca8a4 !important' }} />}
                label={`~${profileRequest.billing_tb < 0.001
                  ? '< 0.001'
                  : profileRequest.billing_tb.toFixed(3)} To`}
                size="small"
                sx={{
                  bgcolor: '#e8f5f5',
                  color: '#1ca8a4',
                  border: '1px solid #b2e0de',
                  fontWeight: 600,
                  fontSize: 12,
                  cursor: 'default',
                }}
              />
            </Tooltip>
          )}
        </Stack>

        {!isMulti && (
          <Typography variant="body2" sx={{ color: '#555', mb: 2, whiteSpace: 'pre-line' }}>
            Pour générer des données de test fiables, j'ai besoin du profiling de certaines colonnes.

            Merci d'exécuter la requête SQL ci-dessous et de fournir le résultat en JSON.
          </Typography>
        )}

        {/* Missing columns grouped by table */}
        <Stack gap={1} sx={{ mb: 2.5 }}>
          {profileRequest.missing_columns.map((entry) => (
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
                    sx={{
                      bgcolor: '#e8f5f5',
                      color: '#1ca8a4',
                      fontFamily: 'monospace',
                      fontSize: 11,
                      border: '1px solid #b2e0de',
                    }}
                  />
                ))}
              </Stack>
            </Box>
          ))}
        </Stack>

        {isMulti ? (
          <MultiQueryLayout
            profileRequest={profileRequest}
            queries={queries}
            messageId={messageId}
            parentId={parentId}
            onUpload={onUpload}
            loading={loading}
          />
        ) : (
          <SingleQueryLayout
            profileRequest={profileRequest}
            messageId={messageId}
            parentId={parentId}
            onUpload={onUpload}
            loading={loading}
          />
        )}

        {/* Loading feedback */}
        {loading && (
          <Box sx={{ mt: 2.5 }}>
            <LinearProgress
              variant="indeterminate"
              sx={{
                height: 6,
                borderRadius: 3,
                bgcolor: '#e0f7f5',
                '& .MuiLinearProgress-bar': { bgcolor: '#1ca8a4' },
              }}
            />
            <Typography variant="caption" sx={{ color: '#555', mt: 0.5, display: 'block' }}>
              {loading_message || 'Traitement…'}
            </Typography>
          </Box>
        )}
      </Paper>

      {/* Skip */}
      <NeutralButton
        startIcon={<SkipNextIcon />}
        onClick={onSkip}
        disabled={loading}
      >
        Passer cette étape
      </NeutralButton>
    </Box>
  );
};

export default ProfilingStep;
