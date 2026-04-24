import React from 'react';
import { Box, Typography } from '@mui/material';

/**
 * Persistent footer shown in the tests workspace.
 * Communicates that BigQuery queries are transpiled and executed locally via
 * DuckDB — no BigQuery costs are incurred.
 */
const DuckDBFooter: React.FC = () => (
  <Box
    sx={{
      borderTop: '1px solid #e4eaec',
      bgcolor: '#fff',
      px: 2.5,
      py: 1,
      display: 'flex',
      alignItems: 'center',
      gap: 1.5,
      fontSize: 11.5,
      color: '#6b8287',
      flexShrink: 0,
    }}
  >
    {/* DuckDB indicator */}
    <Box sx={{ display: 'inline-flex', alignItems: 'center', gap: '5px', color: '#8a5a00' }}>
      <Box sx={{ width: 7, height: 7, borderRadius: '50%', bgcolor: '#f7c948', flexShrink: 0 }} />
      <Typography sx={{ fontSize: 11.5, fontWeight: 700, color: '#8a5a00' }}>DuckDB local</Typography>
    </Box>
    <Typography sx={{ fontSize: 11.5, color: '#6b8287' }}>
      · Tes requêtes BigQuery sont transpilées et exécutées sur duckdb
    </Typography>
    <Box sx={{ ml: 'auto', display: 'inline-flex', alignItems: 'center', gap: 1 }}>
      <Typography sx={{ fontSize: 11.5, fontWeight: 600, color: '#23a26d' }}>0 € facturé</Typography>
    </Box>
  </Box>
);

export default DuckDBFooter;
