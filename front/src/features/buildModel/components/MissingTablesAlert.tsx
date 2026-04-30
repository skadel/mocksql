import React, { useState } from 'react';
import { Box, Checkbox, FormControlLabel, LinearProgress, Tooltip, Typography } from '@mui/material'; // Tooltip kept for onDismiss button
import TableChartIcon from '@mui/icons-material/TableChart';
import DownloadIcon from '@mui/icons-material/Download';
import StorageIcon from '@mui/icons-material/Storage';
import SecurityIcon from '@mui/icons-material/Security';
import ScienceIcon from '@mui/icons-material/Science';
import CloseIcon from '@mui/icons-material/Close';
import { WarningContainedButton } from '../../../style/AppButtons';
import { updateProjectAutoImport } from '../../../api/preferences';

interface MissingTablesAlertProps {
  missingTables: string[];
  projectId: string;
  onImport?: () => void;
  importing?: boolean;
  onDismiss?: () => void;
}

const MissingTablesAlert: React.FC<MissingTablesAlertProps> = ({
  missingTables,
  projectId,
  onImport,
  importing,
  onDismiss,
}) => {
  const projectKey = `autoImport_project_${projectId}`;

  const [alwaysForProject, setAlwaysForProject] = useState(
    () => localStorage.getItem(projectKey) === 'true',
  );
  const handleAlwaysForProject = (checked: boolean) => {
    setAlwaysForProject(checked);
    if (checked) localStorage.setItem(projectKey, 'true');
    else localStorage.removeItem(projectKey);
    updateProjectAutoImport(projectId, checked);
    if (checked && onImport) onImport();
  };

  return (
    <Box sx={{ mt: 2 }}>
      {/* Main import card — amber/duck theme */}
      <Box
        sx={{
          bgcolor: '#fff8e4',
          border: '1px solid #f2d98b',
          borderRadius: '14px',
          p: '18px 20px',
        }}
      >
        {/* Header */}
        <Box sx={{ display: 'flex', alignItems: 'flex-start', gap: 1.5 }}>
          <Box
            sx={{
              width: 36, height: 36, borderRadius: '10px', bgcolor: '#f7e3a0',
              color: '#8a5a00', display: 'grid', placeItems: 'center', flexShrink: 0,
            }}
          >
            <TableChartIcon sx={{ fontSize: 18 }} />
          </Box>
          <Box sx={{ flex: 1 }}>
            <Typography sx={{ fontSize: 15, fontWeight: 700, color: '#8a5a00' }}>
              Tables introuvables dans l'index local
            </Typography>
            <Typography sx={{ fontSize: 12.5, color: '#8a6914', mt: 0.5, lineHeight: 1.55 }}>
              MockSQL transpile ta requête BigQuery et l'exécute en local via{' '}
              <Box component="strong" sx={{ color: '#8a5a00' }}>DuckDB</Box>{' '}
              — aucune requête facturée sur BigQuery. C'est uniquement le schéma qui est importé.
            </Typography>
          </Box>
          {onDismiss && (
            <Tooltip title="Annuler">
              <Box
                component="button"
                onClick={onDismiss}
                sx={{
                  display: 'grid', placeItems: 'center', width: 28, height: 28,
                  border: 'none', borderRadius: '7px', bgcolor: 'transparent',
                  cursor: 'pointer', color: '#8a5a00', '&:hover': { bgcolor: '#f7e3a0' },
                }}
              >
                <CloseIcon sx={{ fontSize: 16 }} />
              </Box>
            </Tooltip>
          )}
        </Box>

        {/* Table rows */}
        <Box sx={{ mt: 1.75, display: 'flex', flexDirection: 'column', gap: 1 }}>
          {missingTables.map((table, i) => (
            <Box
              key={table}
              sx={{
                bgcolor: '#fff',
                border: '1px solid #f0dea2',
                borderRadius: '10px',
                p: '10px 12px',
                display: 'flex',
                alignItems: 'center',
                gap: 1.5,
              }}
            >
              {importing ? (
                <Box sx={{ display: 'inline-flex', animation: 'spin .8s linear infinite', color: '#8a5a00' }}>
                  <DownloadIcon sx={{ fontSize: 16 }} />
                </Box>
              ) : (
                <TableChartIcon sx={{ fontSize: 16, color: '#b89a4a' }} />
              )}
              <Typography sx={{ fontFamily: 'monospace', fontSize: 12.5, fontWeight: 500, color: '#0f272a', flex: '0 0 auto' }}>
                {table}
              </Typography>
              {importing && (
                <Box sx={{ flex: 1, minWidth: 60 }}>
                  <LinearProgress
                    variant="indeterminate"
                    sx={{
                      height: 5, borderRadius: 3, bgcolor: '#fbecc5',
                      '& .MuiLinearProgress-bar': { bgcolor: '#f7c948' },
                    }}
                  />
                </Box>
              )}
              <Typography sx={{ fontSize: 11, fontWeight: 600, color: importing ? '#8a5a00' : '#b89a4a', flexShrink: 0, minWidth: 60, textAlign: 'right' }}>
                {importing ? 'Import…' : 'En attente'}
              </Typography>
            </Box>
          ))}
        </Box>

        {/* Actions row */}
        <Box sx={{ mt: 2, display: 'flex', alignItems: 'center', gap: 2, flexWrap: 'wrap' }}>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 0.25 }}>
            <FormControlLabel
              control={
                <Checkbox
                  size="small"
                  checked={alwaysForProject}
                  onChange={(e) => handleAlwaysForProject(e.target.checked)}
                  sx={{ py: 0.25, color: '#b89a4a', '&.Mui-checked': { color: '#1ca8a4' } }}
                />
              }
              label={<Typography sx={{ fontSize: 12.5, color: '#8a5a00' }}>Toujours importer pour ce projet</Typography>}
              sx={{ m: 0 }}
            />
          </Box>
          <Box sx={{ ml: 'auto', display: 'flex', gap: 1 }}>
            {onDismiss && (
              <Box
                component="button"
                onClick={onDismiss}
                sx={{
                  display: 'inline-flex', alignItems: 'center', gap: '6px',
                  px: '14px', py: '8px', fontSize: 13, fontWeight: 500,
                  border: '1px solid #e7ca7f', borderRadius: '9px', bgcolor: 'transparent',
                  color: '#8a5a00', cursor: 'pointer', fontFamily: 'inherit',
                  '&:hover': { bgcolor: '#f7e3a0' },
                }}
              >
                Annuler
              </Box>
            )}
            {onImport && (
              <WarningContainedButton
                size="small"
                startIcon={
                  importing
                    ? <Box component="span" sx={{ display: 'inline-block', width: 14, height: 14, border: '2px solid rgba(255,255,255,.45)', borderTop: '2px solid #fff', borderRadius: '50%', animation: 'spin .7s linear infinite' }} />
                    : <DownloadIcon fontSize="small" />
                }
                onClick={onImport}
                disabled={importing}
              >
                {importing
                  ? `Import en cours… (${missingTables.length} table${missingTables.length > 1 ? 's' : ''})`
                  : `Importer les tables (${missingTables.length})`
                }
              </WarningContainedButton>
            )}
          </Box>
        </Box>

        {importing && (
          <Typography sx={{ mt: 1, fontSize: 11.5, color: '#8a6914' }}>
            Import en cours — les tests se lanceront automatiquement à la fin.
          </Typography>
        )}
      </Box>

      {/* Info cards row */}
      <Box sx={{ mt: 1.5, display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: 1.25 }}>
        {[
          { icon: <StorageIcon sx={{ fontSize: 16 }} />, title: 'Exécution locale', desc: 'DuckDB exécute la requête sur ton poste. Aucune donnée réelle ne quitte ton infra.' },
          { icon: <SecurityIcon sx={{ fontSize: 16 }} />, title: '0 € facturé', desc: 'Aucune requête n\'atteint BigQuery — itère librement sans coup de facture.' },
          { icon: <ScienceIcon sx={{ fontSize: 16 }} />, title: 'Données synthétiques', desc: 'Seul le schéma est importé. Les valeurs utilisées pour les tests sont générées — zéro fuite de données prod.' },
        ].map(({ icon, title, desc }) => (
          <Box key={title} sx={{ bgcolor: '#f4f7f7', border: '1px solid #e4eaec', borderRadius: '12px', p: '12px 14px' }}>
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, color: '#1ca8a4', mb: 0.75 }}>
              {icon}
              <Typography sx={{ fontSize: 12.5, fontWeight: 600, color: '#0f272a' }}>{title}</Typography>
            </Box>
            <Typography sx={{ fontSize: 11.5, color: '#6b8287', lineHeight: 1.5 }}>{desc}</Typography>
          </Box>
        ))}
      </Box>
    </Box>
  );
};

export default MissingTablesAlert;
