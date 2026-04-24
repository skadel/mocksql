import AddIcon from '@mui/icons-material/Add';
import ContentCopyIcon from '@mui/icons-material/ContentCopy';
import {
  Box,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Paper,
  Step,
  StepLabel,
  Stepper,
  TextField,
  Typography
} from '@mui/material';
import React, { useRef, useState } from 'react';
import { GhostButton, PrimaryButton } from '../style/AppButtons';
import { focusedStyles, StyledButton } from '../style/StyledComponents';
import BigQueryUploader, { BigQueryUploaderHandle } from './BigQueryUploader';

interface SchemaFileUploaderProps {
  onUpload: (schemaData: Record<string, any[]>) => void;
  onAddTable: (tableName: string, schemaData: Record<string, any>[]) => void;
}

const preprocessJson = (json: string): string => {
  return json.replace(/"((?:\\.|[^"\\])*)"/g, (match, group) => {
    const fixed = group.replace(/(\r\n|\n|\r)/g, '\\n');
    return `"${fixed}"`;
  });
};

const SCHEMA_SQL = `SELECT *\nFROM \`votre-projet.votre-base-de-données.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS\``;

const SchemaFileUploader: React.FC<SchemaFileUploaderProps> = ({ onUpload, onAddTable }) => {
  const bigQueryRef = useRef<BigQueryUploaderHandle>(null);
  const [schemaFile, setSchemaFile] = useState<File | null>(null);
  const [schemaText, setSchemaText] = useState<string>('');
  const [tableName, setTableName] = useState<string>('');
  const [error, setError] = useState<string | null>(null);
  const [addTableOpen, setAddTableOpen] = useState(false);
  const [activeStep, setActiveStep] = useState(0);
  const [tableError, setTableError] = useState<string | null>(null);

  const parseJsonContent = (json: string): Record<string, any[]> => {
    try {
      const preprocessed = preprocessJson(json);
      const parsed = JSON.parse(preprocessed);
      return { data: Array.isArray(parsed) ? parsed : [parsed] };
    } catch {
      throw new Error('Invalid JSON format');
    }
  };

  const parseCsvContent = (csv: string): { data: Record<string, string | null>[] } => {
    const rows: string[][] = [];
    let currentRow: string[] = [];
    let currentValue = '';
    let insideQuotes = false;

    for (let i = 0; i < csv.length; i++) {
      const char = csv[i];
      if (char === '"') {
        if (insideQuotes && csv[i + 1] === '"') {
          currentValue += '"';
          i++;
        } else {
          insideQuotes = !insideQuotes;
        }
      } else if (char === ',' && !insideQuotes) {
        currentRow.push(currentValue);
        currentValue = '';
      } else if ((char === '\n' || char === '\r') && !insideQuotes) {
        if (char === '\r' && csv[i + 1] === '\n') i++;
        currentRow.push(currentValue);
        rows.push(currentRow);
        currentRow = [];
        currentValue = '';
      } else {
        currentValue += char;
      }
    }
    if (currentValue.length > 0 || currentRow.length > 0) {
      currentRow.push(currentValue);
      rows.push(currentRow);
    }
    const filteredRows = rows.filter(row => row.some(cell => cell.trim() !== ''));
    if (filteredRows.length === 0) return { data: [] };
    const headers = filteredRows[0];
    const data = filteredRows.slice(1).map(row => {
      const rowObj: Record<string, string | null> = {};
      headers.forEach((header, index) => {
        rowObj[header] = row[index] ? row[index].trim() : null;
      });
      return rowObj;
    });
    return { data };
  };

  const tableNameRegex = /^[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+$/;
  const validateTableName = (name: string): boolean => tableNameRegex.test(name);

  const handleClose = () => {
    setAddTableOpen(false);
    setActiveStep(0);
    setTableName('');
    setSchemaText('');
    setTableError(null);
    setError(null);
  };

  const handleNext = () => {
    if (!validateTableName(tableName)) {
      setTableError('Le nom doit suivre le format : projet.dataset.table');
      return;
    }
    setActiveStep(1);
  };

  const handleAddTableSubmit = () => {
    try {
      const parsedData = parseJsonContent(schemaText);
      onAddTable(tableName, parsedData.data);
      handleClose();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const handleSchemaContent = (content: string, fileName: string) => {
    try {
      const fileExtension = fileName.split('.').pop()?.toLowerCase();
      let parsedData: Record<string, any[]>;
      if (fileExtension === 'csv') {
        parsedData = parseCsvContent(content);
      } else if (fileExtension === 'json') {
        parsedData = parseJsonContent(content);
      } else {
        throw new Error('Unsupported file type');
      }
      setSchemaFile(new File([content], fileName));
      setError(null);
      onUpload(parsedData);
    } catch (err) {
      setError((err as Error).message);
    }
  };

  return (
    <Box display="flex" flexDirection="row" alignItems="center" justifyContent="space-between" gap={2}>
      {/* Section Ajouter une Base de Données */}
      <Box mb={6}>
        <Typography
          variant="body1"
          color="#1ca8a4"
          onClick={() => bigQueryRef.current?.triggerUpload()}
          sx={{ cursor: 'pointer' }}
        >
          Ajouter une Base de Données
        </Typography>
        <BigQueryUploader
          ref={bigQueryRef}
          sqlQuery={SCHEMA_SQL}
          onFileContent={handleSchemaContent}
          accept=".csv,.json"
          uploadLabel="Importer"
          instructionsTitle="Instructions pour ajouter une base de données"
          downloadFormat="CSV ou JSON"
        />
        {schemaFile && (
          <Typography variant="body2" sx={{ mt: 1, fontWeight: 'bold', color: '#555' }}>
            Fichier sélectionné : {schemaFile.name}
          </Typography>
        )}
      </Box>

      {/* Section Ajouter une Table */}
      <Box mb={6}>
        <Typography
          variant="body1"
          color="#1ca8a4"
          onClick={() => setAddTableOpen(true)}
          sx={{ cursor: 'pointer' }}
        >
          Ajouter une Table
        </Typography>
        <StyledButton onClick={() => setAddTableOpen(true)}>
          <AddIcon /> Nouvelle Table
        </StyledButton>
      </Box>

      {/* Error Message */}
      {error && (
        <Paper
          elevation={2}
          sx={{ mt: 2, p: 2, backgroundColor: '#ffe6e6', border: '1px solid #f44336', borderRadius: 2 }}
        >
          <Typography variant="body2" color="error">{error}</Typography>
        </Paper>
      )}

      {/* Add Table Dialog — stepper */}
      <Dialog open={addTableOpen} onClose={handleClose} maxWidth="sm" fullWidth>
        <DialogTitle sx={{ color: '#1ca8a4' }}>Ajouter une Table</DialogTitle>
        <DialogContent>
          <Stepper activeStep={activeStep} sx={{ mb: 3 }}>
            <Step><StepLabel>Nom de la table</StepLabel></Step>
            <Step><StepLabel>Schéma JSON</StepLabel></Step>
          </Stepper>

          {activeStep === 0 && (
            <TextField
              autoFocus
              label="Nom de la table"
              value={tableName}
              onChange={(e) => {
                const name = e.target.value;
                setTableName(name);
                setTableError(
                  name && !validateTableName(name)
                    ? 'Le nom doit suivre le format : projet.dataset.table'
                    : null
                );
              }}
              onKeyDown={(e) => { if (e.key === 'Enter' && tableName && !tableError) handleNext(); }}
              fullWidth
              sx={focusedStyles}
              placeholder="projet.dataset.nom_de_la_table"
              error={!!tableError}
              helperText={tableError}
            />
          )}

          {activeStep === 1 && (
            <Box>
              <Box sx={{ mb: 2, p: 1.5, bgcolor: '#f5f5f5', borderRadius: 1 }}>
                <Typography variant="body2" sx={{ mb: 0.5 }}>
                  <strong>Console BigQuery :</strong> sélectionnez la table → onglet <em>Schema</em> → sélectionner toutes les colonnes → <em>Copy as JSON</em>
                </Typography>
                <Typography variant="body2" sx={{ mt: 1, mb: 0.5 }}>
                  <strong>CLI :</strong>
                </Typography>
                <Box sx={{ position: 'relative', bgcolor: '#e8e8e8', borderRadius: 1, p: 1 }}>
                  <Box component="code" sx={{ fontSize: '0.75rem', fontFamily: 'monospace' }}>
                    {`bq show --schema --format=prettyjson ${tableName.replace('.', ':')}`}
                  </Box>
                  <IconButton
                    size="small"
                    onClick={() => navigator.clipboard.writeText(`bq show --schema --format=prettyjson ${tableName.replace('.', ':')}`)}
                    sx={{ position: 'absolute', top: 4, right: 4, color: '#555' }}
                  >
                    <ContentCopyIcon fontSize="small" />
                  </IconButton>
                </Box>
              </Box>
              <TextField
                autoFocus
                placeholder="Collez le schéma JSON ici"
                value={schemaText}
                onChange={(e) => setSchemaText(e.target.value)}
                fullWidth
                multiline
                rows={5}
                sx={focusedStyles}
              />
            </Box>
          )}
        </DialogContent>

        <DialogActions>
          {activeStep === 0 ? (
            <>
              <PrimaryButton
                onClick={handleNext}
                disabled={!tableName || !!tableError}
              >
                Suivant
              </PrimaryButton>
              <GhostButton onClick={handleClose}>Annuler</GhostButton>
            </>
          ) : (
            <>
              <PrimaryButton
                onClick={handleAddTableSubmit}
                disabled={!schemaText.trim()}
              >
                Ajouter
              </PrimaryButton>
              <GhostButton onClick={() => setActiveStep(0)}>Retour</GhostButton>
              <GhostButton onClick={handleClose}>Annuler</GhostButton>
            </>
          )}
        </DialogActions>
      </Dialog>
    </Box>
  );
};

export default React.memo(SchemaFileUploader);
