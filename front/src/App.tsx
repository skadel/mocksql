import React from 'react';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { useSelector } from 'react-redux';

import DrawerComponent from './features/appBar/components/DrawerComponent';
import ChatComponent from './features/buildModel/components/QueryChatComponent';
import ModelTestsPage from './features/buildModel/components/ModelTestsPage';
import { RootState } from './app/store';

// v15 design tokens → MUI theme. Hex values mirror style/tokens.css (--mq-*).
// MUI needs literal colours (not CSS vars) for contrast computation.
const base = createTheme();
const theme = createTheme(base, {
  palette: {
    primary: {
      main: '#2bb0a8', // --mq-brand-500
      dark: '#1f948d', // --mq-brand-600 (hover/pressed)
      light: '#7cc8c1', // --mq-brand-300
      contrastText: '#ffffff',
    },
    secondary: {
      main: '#6b8287', // --mq-ink-500
    },
    success: { main: '#23a26d', light: '#e9f7f0', dark: '#1c855a' },
    warning: { main: '#d89323', light: '#fcf3e1', dark: '#a86a00' },
    error: { main: '#d0503f', light: '#fbeceb', dark: '#b23a32' },
    text: {
      primary: '#0f272a', // --mq-ink-900
      secondary: '#6b8287', // --mq-ink-500
    },
    divider: '#c9d3d6', // --mq-line
    background: {
      default: '#dde3e6', // --mq-canvas
      paper: '#ffffff', // --mq-surface-2
    },
  },
  typography: {
    fontFamily:
      "'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif",
    h1: { fontSize: 17, fontWeight: 700, lineHeight: 1.3, letterSpacing: '-0.01em' },
    h2: { fontSize: 15, fontWeight: 600, lineHeight: 1.35 },
    body1: { fontSize: 13.5, lineHeight: 1.55 },
    body2: { fontSize: 12.5, lineHeight: 1.5 },
    caption: { fontSize: 11.5, lineHeight: 1.4 },
    overline: {
      fontSize: 11,
      fontWeight: 600,
      letterSpacing: '0.05em',
      textTransform: 'uppercase',
      lineHeight: 1.2,
    },
  },
  shape: {
    borderRadius: 8, // --mq-r-md
  },
  // Nearly-flat elevation (--mq-shadow-xs/sm/md) on the low indices.
  shadows: base.shadows.map((s, i) => {
    if (i === 1) return '0 1px 2px rgba(15, 39, 42, 0.04)';
    if (i === 2) return '0 1px 3px rgba(15, 39, 42, 0.06), 0 1px 2px rgba(15, 39, 42, 0.04)';
    if (i === 3) return '0 6px 20px rgba(15, 39, 42, 0.10)';
    return s;
  }) as typeof base.shadows,
});


function App() {
  const workspaceMode = useSelector((state: RootState) => state.buildModel.workspaceMode);

  return (
    <ThemeProvider theme={theme}>
      <Router>
        <Box sx={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden' }}>
          <CssBaseline />
          {!workspaceMode && <DrawerComponent />}
          <Box component="main" sx={{ flexGrow: 1, bgcolor: 'background.default', overflow: 'hidden', display: 'flex', flexDirection: 'column' }}>
            <Routes>
              <Route path="/" element={<ChatComponent />} />
              <Route path="/models" element={<ChatComponent />} />
              <Route path="/models/sql/:modelName" element={<ModelTestsPage />} />
              <Route path="/models/:modelID" element={<ChatComponent />} />
              <Route path="/static/*" element={<Navigate to="/" replace />} />
            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;
