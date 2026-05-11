import React from 'react';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { useSelector } from 'react-redux';

import DrawerComponent from './features/appBar/components/DrawerComponent';
import ChatComponent from './features/buildModel/components/QueryChatComponent';
import ModelTestsPage from './features/buildModel/components/ModelTestsPage';
import IntegrationPage from './features/integration/IntegrationPage';
import { RootState } from './app/store';

const theme = createTheme({
  palette: {
    background: {
      default: '#dde3e6',
    },
  },
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
              <Route path="/integration" element={<IntegrationPage />} />
              <Route path="/static/*" element={<Navigate to="/" replace />} />
            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;
