import CodeIcon from '@mui/icons-material/Code';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import HistoryIcon from '@mui/icons-material/History';
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Box,
  Button,
  Divider,
  List,
  ListItemButton,
  ListItemText,
  Popover,
  Tooltip,
  Typography,
} from '@mui/material';
import { TealIconButton } from '../../../style/AppButtons';
import React, { useEffect, useRef, useState } from 'react';
import SqlEditor from '../../../shared/SqlEditor';
import { SqlHistoryEntry } from '../../../utils/types';

interface SQLQueryBarProps {
  sqlQuery: string;
  onUpdate: (newSql: string) => void;
  disabled?: boolean;
  hasError?: boolean;
  optimizedSql?: string;
  sqlHistory?: SqlHistoryEntry[];
  onHistorySelect?: (entry: SqlHistoryEntry) => void;
  historyRestoreTrigger?: number;
}

const SQLQueryBar: React.FC<SQLQueryBarProps> = ({
  sqlQuery,
  onUpdate,
  disabled,
  hasError,
  optimizedSql,
  sqlHistory,
  onHistorySelect,
  historyRestoreTrigger,
}) => {
  const [expanded, setExpanded] = useState(true);
  const [viewMode, setViewMode] = useState<'raw' | 'optimized'>('raw');
  const [historyAnchor, setHistoryAnchor] = useState<HTMLElement | null>(null);
  const prevDisabled = useRef(disabled);
  const prevTrigger = useRef(historyRestoreTrigger);

  // Collapse only when the request completes (disabled: true → false) without errors
  useEffect(() => {
    if (prevDisabled.current && !disabled && !hasError) {
      setExpanded(false);
    }
    prevDisabled.current = disabled;
  }, [disabled, hasError]);

  // Reset to raw view when a new optimized SQL arrives
  useEffect(() => {
    if (optimizedSql) setViewMode('raw');
  }, [optimizedSql]);

  // When history is restored externally, open the accordion and show the new SQL
  useEffect(() => {
    if (historyRestoreTrigger !== undefined && historyRestoreTrigger !== prevTrigger.current) {
      prevTrigger.current = historyRestoreTrigger;
      setViewMode('raw');
      setExpanded(true);
    }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [historyRestoreTrigger]);

  const firstLine = sqlQuery.split('\n').find((l) => l.trim()) ?? '';
  const preview = firstLine.length > 80 ? firstLine.slice(0, 80) + '…' : firstLine;

  const handleToggle = () => {
    if (!expanded) setViewMode('raw');
    setExpanded((v) => !v);
  };

  const handleHistoryClick = (e: React.MouseEvent<HTMLElement>) => {
    e.stopPropagation();
    setHistoryAnchor(e.currentTarget);
  };

  const handleHistoryEntryClick = (entry: SqlHistoryEntry) => {
    onHistorySelect?.(entry);
    setHistoryAnchor(null);
  };

  const showToggle = !!optimizedSql && optimizedSql.trim() !== sqlQuery.trim();
  const isOptimizedView = viewMode === 'optimized';
  const editorValue = isOptimizedView ? (optimizedSql ?? '') : sqlQuery;
  const hasHistory = sqlHistory && sqlHistory.length > 0;

  return (
    <>
      <Accordion
        expanded={expanded}
        onChange={handleToggle}
        disableGutters
        sx={{
          border: '1px solid #1ca8a4',
          borderRadius: '10px !important',
          boxShadow: 'none',
          mb: 2,
          '&:before': { display: 'none' },
          bgcolor: expanded ? '#fafafa' : '#f0fafa',
        }}
      >
        <AccordionSummary
          expandIcon={<ExpandMoreIcon sx={{ color: '#1ca8a4' }} />}
          sx={{ minHeight: 40, px: 2, py: 0 }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, overflow: 'hidden', flex: 1, mr: 1 }}>
            <CodeIcon sx={{ fontSize: 16, color: '#1ca8a4', flexShrink: 0 }} />
            <Typography
              variant="caption"
              sx={{
                fontFamily: 'monospace',
                fontWeight: 600,
                color: '#1ca8a4',
                mr: 1,
                flexShrink: 0,
              }}
            >
              SQL
            </Typography>
            {!expanded && (
              <Typography
                variant="caption"
                sx={{
                  fontFamily: 'monospace',
                  color: '#555',
                  whiteSpace: 'nowrap',
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                }}
              >
                {preview}
              </Typography>
            )}
          </Box>
          {hasHistory && (
            <Tooltip title={`Historique SQL (${sqlHistory!.length} version${sqlHistory!.length > 1 ? 's' : ''})`}>
              <TealIconButton
                size="small"
                onClick={handleHistoryClick}
                sx={{ flexShrink: 0, mr: 0.5 }}
              >
                <HistoryIcon sx={{ fontSize: 16 }} />
              </TealIconButton>
            </Tooltip>
          )}
        </AccordionSummary>

        <AccordionDetails sx={{ p: 0, borderTop: '1px solid #d0eeec' }}>
          {showToggle && (
            <Box sx={{ display: 'flex', px: 2, py: 0.75, gap: 0.5, bgcolor: '#f5fafa', borderBottom: '1px solid #d0eeec' }}>
              <Button
                size="small"
                variant={!isOptimizedView ? 'contained' : 'text'}
                onClick={() => setViewMode('raw')}
                sx={{
                  fontSize: 11, py: 0.25, px: 1.5, minWidth: 0,
                  textTransform: 'none', fontWeight: 600,
                  borderRadius: '6px 0 0 6px',
                  backgroundColor: !isOptimizedView ? '#1ca8a4' : 'transparent',
                  color: !isOptimizedView ? '#fff' : '#1ca8a4',
                  border: '1px solid #1ca8a4', borderRight: 'none',
                  '&:hover': { backgroundColor: !isOptimizedView ? '#159e9a' : '#e8f7f6' },
                }}
              >
                Original
              </Button>
              <Button
                size="small"
                variant={isOptimizedView ? 'contained' : 'text'}
                onClick={() => setViewMode('optimized')}
                sx={{
                  fontSize: 11, py: 0.25, px: 1.5, minWidth: 0,
                  textTransform: 'none', fontWeight: 600,
                  borderRadius: '0 6px 6px 0',
                  backgroundColor: isOptimizedView ? '#1ca8a4' : 'transparent',
                  color: isOptimizedView ? '#fff' : '#1ca8a4',
                  border: '1px solid #1ca8a4',
                  '&:hover': { backgroundColor: isOptimizedView ? '#159e9a' : '#e8f7f6' },
                }}
              >
                Optimisé
              </Button>
            </Box>
          )}

          <SqlEditor
            value={editorValue}
            onChange={() => {}}
            disabled={true}
            maxHeight={360}
            fontSize={13}
            minHeight={100}
          />
        </AccordionDetails>
      </Accordion>

      {/* History popover */}
      <Popover
        open={Boolean(historyAnchor)}
        anchorEl={historyAnchor}
        onClose={() => setHistoryAnchor(null)}
        anchorOrigin={{ vertical: 'bottom', horizontal: 'left' }}
        transformOrigin={{ vertical: 'top', horizontal: 'left' }}
      >
        <Box sx={{ width: 380, maxHeight: 420, overflow: 'auto' }}>
          <Box sx={{ px: 2, py: 1, bgcolor: '#f0fafa', borderBottom: '1px solid #d0eeec' }}>
            <Typography variant="caption" sx={{ fontWeight: 700, color: '#1ca8a4' }}>
              Historique SQL
            </Typography>
          </Box>
          <List dense disablePadding>
            {[...(sqlHistory ?? [])].reverse().map((entry, i, arr) => {
              const num = arr.length - i;
              const preview = entry.sql.split('\n').find(l => l.trim())?.slice(0, 60) ?? '';
              const hasOptimized = entry.optimizedSql && entry.optimizedSql.trim() !== entry.sql.trim();
              return (
                <React.Fragment key={entry.id}>
                  {i > 0 && <Divider />}
                  <ListItemButton
                    onClick={() => handleHistoryEntryClick(entry)}
                    sx={{ px: 2, py: 0.75, '&:hover': { bgcolor: '#e8f7f6' } }}
                  >
                    <ListItemText
                      primary={
                        <Box sx={{ display: 'flex', alignItems: 'center', gap: 1 }}>
                          <Typography variant="caption" sx={{ fontWeight: 700, color: '#1ca8a4', flexShrink: 0 }}>
                            #{num}
                          </Typography>
                          {hasOptimized && (
                            <Typography variant="caption" sx={{ fontSize: 10, color: '#888', bgcolor: '#f0f0f0', px: 0.5, borderRadius: 0.5 }}>
                              optimisé
                            </Typography>
                          )}
                        </Box>
                      }
                      secondary={
                        <Typography
                          variant="caption"
                          sx={{ fontFamily: 'monospace', fontSize: 11, color: '#555', display: 'block', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' }}
                        >
                          {preview}…
                        </Typography>
                      }
                    />
                  </ListItemButton>
                </React.Fragment>
              );
            })}
          </List>
        </Box>
      </Popover>
    </>
  );
};

export default SQLQueryBar;
