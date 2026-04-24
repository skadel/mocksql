import React from 'react';
import Box from '@mui/material/Box';
import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider, createTheme } from '@mui/material/styles';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';

import DrawerComponent from './features/appBar/components/DrawerComponent';
import ChatComponent from './features/buildModel/components/QueryChatComponent';
const theme = createTheme({
  // personnalisation de votre thème ici…
});


function App() {
  return (
    <ThemeProvider theme={theme}>
      <Router>
        <Box sx={{ display: 'flex', height: '100vh', width: '100vw', overflow: 'hidden' }}>
          <CssBaseline />
          <DrawerComponent />
          <Box component="main" sx={{ flexGrow: 1, bgcolor: 'background.default', p: 2, overflow: 'auto' }}>
            <Routes>
              <Route path="/" element={<ChatComponent />} />
              <Route path="/models" element={<ChatComponent />} />
              <Route path="/models/:projectID" element={<ChatComponent />} />
              <Route path="/models/:projectID/:modelID" element={<ChatComponent />} />

            </Routes>
          </Box>
        </Box>
      </Router>
    </ThemeProvider>
  );
}

export default App;
