import { ChevronLeft, ChevronRight, FirstPage, LastPage, UnfoldLess, UnfoldMore } from "@mui/icons-material";
import {
  Alert,
  Backdrop,
  Box,
  Chip,
  CircularProgress,
  IconButton,
  LinearProgress,
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TablePagination,
  TableRow,
  Typography
} from "@mui/material";
import React from "react";
import { useTranslation } from "react-i18next";

const PRIMARY = "rgba(28, 168, 164)";
const PRIMARY_DARK = "#157a76";
const PRIMARY_BG = "rgba(28, 168, 164, 0.08)";
const PRIMARY_BORDER = "rgba(28, 168, 164, 0.25)";

/**
 * Métadonnées renvoyées par l'API pour gérer la pagination serveur.
 */
export interface DisplayTableMeta {
  total: number;
  limit: number;
  offset: number;
  sql: string;
}

export interface DisplayTableProps {
  jsonData: Record<string, any>[] | null | undefined;
  tableName?: string;
  project?: string;
  meta?: DisplayTableMeta;
  msgId?: string;
  onPageChange?: (
    page: number,
    project: string,
    sql: string,
    msgId: string,
    limit: number
  ) => void;
}

interface TablePaginationActionsProps {
  count: number;
  page: number;
  rowsPerPage: number;
  onPageChange: (event: React.MouseEvent<HTMLButtonElement>, page: number) => void;
}

function TablePaginationActions(props: TablePaginationActionsProps) {
  const { count, page, rowsPerPage, onPageChange } = props;
  const lastPage = Math.max(0, Math.ceil(count / rowsPerPage) - 1);

  return (
    <Box sx={{ flexShrink: 0, ml: 2, display: "flex", alignItems: "center", gap: 0.5 }}>
      <IconButton
        size="small"
        onClick={(e) => onPageChange(e, 0)}
        disabled={page === 0}
        aria-label="première page"
        sx={paginationBtnSx}
      >
        <FirstPage fontSize="small" />
      </IconButton>
      <IconButton
        size="small"
        onClick={(e) => onPageChange(e, page - 1)}
        disabled={page === 0}
        aria-label="page précédente"
        sx={paginationBtnSx}
      >
        <ChevronLeft fontSize="small" />
      </IconButton>
      <IconButton
        size="small"
        onClick={(e) => onPageChange(e, page + 1)}
        disabled={page >= lastPage}
        aria-label="page suivante"
        sx={paginationBtnSx}
      >
        <ChevronRight fontSize="small" />
      </IconButton>
      <IconButton
        size="small"
        onClick={(e) => onPageChange(e, lastPage)}
        disabled={page >= lastPage}
        aria-label="dernière page"
        sx={paginationBtnSx}
      >
        <LastPage fontSize="small" />
      </IconButton>
    </Box>
  );
}

const paginationBtnSx = {
  color: PRIMARY,
  "&:hover": { backgroundColor: PRIMARY_BG },
  "&.Mui-disabled": { color: "rgba(0,0,0,0.26)" },
};

const MAX_LENGTH = 120;

const DisplayTable: React.FC<DisplayTableProps> = ({
  jsonData,
  tableName,
  project,
  meta,
  msgId,
  onPageChange,
}) => {
  const { t } = useTranslation();
  const [loading, setLoading] = React.useState(false);
  const [expandedCells, setExpandedCells] = React.useState<Set<string>>(new Set());

  React.useEffect(() => {
    if (!loading) return;
    setLoading(false);
  }, [jsonData, meta, loading]);

  const dataArray = React.useMemo<Record<string, any>[]>(
    () => (Array.isArray(jsonData) ? jsonData : []),
    [jsonData]
  );

  const headers = React.useMemo(() => {
    const firstValidRow = dataArray.find((r) => r && typeof r === "object");
    return firstValidRow ? Object.keys(firstValidRow) : [];
  }, [dataArray]);

  const currentPage = React.useMemo(
    () => (meta ? Math.floor(meta.offset / meta.limit) : 0),
    [meta]
  );

  const handlePageChange: React.ComponentProps<typeof TablePagination>["onPageChange"] = (
    _event,
    newPage
  ) => {
    if (meta && onPageChange && msgId && project) {
      setLoading(true);
      onPageChange(newPage, project, meta.sql, msgId, meta.limit);
    }
  };

  const handleRowsPerPageChange: React.ComponentProps<
    typeof TablePagination
  >["onRowsPerPageChange"] = (event) => {
    if (meta && onPageChange && msgId && project) {
      const newLimit = parseInt(event.target.value, 10);
      setLoading(true);
      onPageChange(0, project, meta.sql, msgId, newLimit);
    }
  };

  const toggleCell = (key: string) => {
    setExpandedCells((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  };

  return (
    <>
      {/* Titre de la table */}
      {tableName && (
        <Box sx={{ display: "flex", alignItems: "center", mb: 1, gap: 1 }}>
          <Chip
            label={tableName}
            size="small"
            sx={{
              backgroundColor: PRIMARY_BG,
              color: PRIMARY_DARK,
              border: `1px solid ${PRIMARY_BORDER}`,
              fontWeight: 600,
              fontSize: "0.75rem",
              letterSpacing: "0.03em",
            }}
          />
          {meta && (
            <Typography variant="caption" sx={{ color: "text.secondary" }}>
              {meta.total.toLocaleString()} ligne{meta.total !== 1 ? "s" : ""}
            </Typography>
          )}
        </Box>
      )}

      {/* Loader de changement de page */}
      {loading && (
        <LinearProgress
          sx={{
            mb: 0.5,
            borderRadius: 1,
            "& .MuiLinearProgress-bar": { backgroundColor: PRIMARY },
            backgroundColor: PRIMARY_BG,
          }}
        />
      )}

      {/* Tableau vide */}
      {dataArray.length === 0 && !loading && (
        <Alert
          severity="info"
          sx={{
            borderRadius: "8px",
            border: `1px solid ${PRIMARY_BORDER}`,
            "& .MuiAlert-icon": { color: PRIMARY },
          }}
        >
          {t("empty_table")}
        </Alert>
      )}

      {/* Données */}
      {dataArray.length > 0 && (
        <TableContainer
          component={Paper}
          elevation={0}
          sx={{
            borderRadius: "10px",
            border: `1px solid ${PRIMARY_BORDER}`,
            overflowX: "auto",
            backgroundColor: "#fff",
          }}
        >
          <Table
            aria-label="results-table"
            size="small"
            sx={{ borderCollapse: "separate", borderSpacing: 0 }}
          >
            <TableHead>
              <TableRow>
                {headers.map((h, i) => (
                  <TableCell
                    key={h}
                    sx={{
                      backgroundColor: PRIMARY,
                      color: "#fff",
                      fontWeight: 700,
                      fontSize: "0.68rem",
                      letterSpacing: "0.04em",
                      textTransform: "uppercase",
                      py: 1,
                      px: 1.5,
                      borderRight: i < headers.length - 1 ? "1px solid rgba(255,255,255,0.2)" : "none",
                      whiteSpace: "nowrap",
                    }}
                  >
                    {h.charAt(0).toUpperCase() + h.slice(1)}
                  </TableCell>
                ))}
              </TableRow>
            </TableHead>
            <TableBody>
              {dataArray.map((row, rowIdx) => (
                <TableRow
                  key={rowIdx}
                  sx={{
                    backgroundColor: rowIdx % 2 === 0 ? "#fff" : "rgba(28, 168, 164, 0.03)",
                    "&:hover": { backgroundColor: "rgba(28, 168, 164, 0.07)" },
                    transition: "background-color 0.15s ease",
                  }}
                >
                  {headers.map((h, cellIdx) => {
                    const raw =
                      typeof row[h] === "object"
                        ? JSON.stringify(row[h], null, 2)
                        : String(row[h] ?? "");
                    const isLong = raw.length > MAX_LENGTH;
                    const cellKey = `${rowIdx}-${cellIdx}`;
                    const isExpanded = expandedCells.has(cellKey);

                    return (
                      <TableCell
                        key={cellKey}
                        sx={{
                          borderBottom: `1px solid ${PRIMARY_BORDER}`,
                          borderRight:
                            cellIdx < headers.length - 1
                              ? `1px solid ${PRIMARY_BORDER}`
                              : "none",
                          py: 0.75,
                          px: 1.5,
                          fontSize: "0.72rem",
                          color: "#1a1a2e",
                          verticalAlign: "top",
                          whiteSpace: isExpanded ? "pre-wrap" : "normal",
                          wordBreak: "break-word",
                        }}
                      >
                        <Box display="flex" alignItems="flex-start" gap={0.5}>
                          <Box flexGrow={1} sx={{ lineHeight: 1.5 }}>
                            {isExpanded ? raw : isLong ? raw.slice(0, MAX_LENGTH) + "…" : raw}
                          </Box>
                          {isLong && (
                            <IconButton
                              size="small"
                              onClick={() => toggleCell(cellKey)}
                              sx={{
                                flexShrink: 0,
                                mt: -0.25,
                                color: PRIMARY,
                                padding: "2px",
                                "&:hover": { backgroundColor: PRIMARY_BG },
                              }}
                            >
                              {isExpanded ? (
                                <UnfoldLess sx={{ fontSize: 16 }} />
                              ) : (
                                <UnfoldMore sx={{ fontSize: 16 }} />
                              )}
                            </IconButton>
                          )}
                        </Box>
                      </TableCell>
                    );
                  })}
                </TableRow>
              ))}
            </TableBody>
          </Table>

          {/* Pagination */}
          {meta && (
            <Box
              sx={{
                borderTop: `1px solid ${PRIMARY_BORDER}`,
                backgroundColor: "rgba(28, 168, 164, 0.03)",
              }}
            >
              <TablePagination
                component="div"
                count={meta.total}
                page={currentPage}
                rowsPerPage={meta.limit}
                onPageChange={handlePageChange}
                onRowsPerPageChange={handleRowsPerPageChange}
                rowsPerPageOptions={[10, 20, 50, 100]}
                labelRowsPerPage={t("rows_per_page")}
                ActionsComponent={TablePaginationActions}
                sx={{
                  "& .MuiTablePagination-selectLabel, & .MuiTablePagination-displayedRows": {
                    fontSize: "0.78rem",
                    color: "text.secondary",
                  },
                  "& .MuiSelect-select": { fontSize: "0.78rem" },
                }}
              />
            </Box>
          )}
        </TableContainer>
      )}

      <Backdrop open={loading} sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
        <CircularProgress sx={{ color: PRIMARY }} />
      </Backdrop>
    </>
  );
};

export default React.memo(DisplayTable);
