import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import SearchIcon from '@mui/icons-material/Search';
import AddIcon from '@mui/icons-material/Add';
import { Box, Drawer, FormControl, IconButton, InputBase, InputLabel, MenuItem, Select, Tooltip, Typography } from '@mui/material';
import { SelectChangeEvent } from '@mui/material/Select';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link as RouterLink } from 'react-router-dom';
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { resetContext } from '../../buildModel/buildModelSlice';
import { setCurrentId, toggleDrawer } from '../appBarSlice';
import { fetchModels } from '../../../api/models';
import SqlFileList from './SqlFileList';

export const drawerWidth = 260;

const TEAL = '#2BB0A8';
const PANEL_BG = '#f3f6f7';
const INK = '#0f272a';
const MUTED = '#6b8287';
const LINE = '#c9d3d6';

/* ── Database icon ─────────────────────────────────────────────────── */
function DbIcon() {
  return (
    <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <ellipse cx="12" cy="5" rx="9" ry="3" />
      <path d="M3 5v14c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
      <path d="M3 12c0 1.66 4.03 3 9 3s9-1.34 9-3" />
    </svg>
  );
}

const DrawerComponent: React.FC = () => {
  const dispatch    = useAppDispatch();
  const { i18n }   = useTranslation();
  const [language, setLanguage] = useState(i18n.language);
  const [search, setSearch]     = useState('');

  const drawerOpen = useAppSelector(s => s.appBarModel.drawerOpen);
  const models     = useAppSelector(s => s.appBarModel.models);

  useEffect(() => {
    dispatch(fetchModels());
  }, [dispatch]);

  const handleChangeLanguage = (e: SelectChangeEvent<string>) => {
    const lang = e.target.value;
    setLanguage(lang);
    i18n.changeLanguage(lang);
  };

  return (
    <>
      {/* Collapse toggle — sits on the right edge of the drawer */}
      <Box
        sx={{
          position: 'fixed',
          top: '50%',
          left: drawerOpen ? drawerWidth - 12 : 0,
          transform: 'translateY(-50%)',
          zIndex: 1300,
          transition: 'left 0.2s ease',
        }}
      >
        <IconButton
          onClick={() => dispatch(toggleDrawer())}
          size="small"
          sx={{
            bgcolor: PANEL_BG,
            border: `1px solid ${LINE}`,
            borderRadius: '0 6px 6px 0',
            width: 20,
            height: 40,
            '&:hover': { bgcolor: '#e4eaec' },
            p: 0,
          }}
        >
          {drawerOpen
            ? <ChevronLeftIcon sx={{ fontSize: 15, color: MUTED }} />
            : <ChevronRightIcon sx={{ fontSize: 15, color: MUTED }} />}
        </IconButton>
      </Box>

      <Drawer
        sx={{
          width: drawerOpen ? drawerWidth : 0,
          flexShrink: 0,
          transition: 'width 0.2s ease',
          '& .MuiDrawer-paper': {
            width: drawerWidth,
            boxSizing: 'border-box',
            bgcolor: PANEL_BG,
            borderRight: `1px solid ${LINE}`,
            display: 'flex',
            flexDirection: 'column',
            transform: drawerOpen ? 'translateX(0)' : `translateX(-${drawerWidth}px)`,
            transition: 'transform 0.2s ease',
            visibility: drawerOpen ? 'visible' : 'hidden',
            overflow: 'hidden',
          },
        }}
        variant="permanent"
        anchor="left"
      >
        {/* ── Header ───────────────────────────────────────────────── */}
        <Box
          sx={{
            display: 'flex',
            alignItems: 'center',
            gap: '10px',
            px: '16px',
            pt: '16px',
            pb: '14px',
          }}
        >
          <Box
            sx={{
              width: 30, height: 30, borderRadius: '8px',
              bgcolor: TEAL, color: '#fff',
              display: 'grid', placeItems: 'center', flexShrink: 0,
            }}
          >
            <DbIcon />
          </Box>

          <Typography sx={{ fontSize: 14, fontWeight: 700, color: INK, flex: 1, letterSpacing: '-0.2px' }}>
            MockSQL
          </Typography>

          <Tooltip title="Nouveau test" placement="right">
            <IconButton
              component={RouterLink}
              to="/"
              onClick={() => {
                dispatch(setCurrentId(''));
                dispatch(resetContext());
              }}
              size="small"
              sx={{
                color: MUTED,
                width: 28, height: 28,
                borderRadius: '7px',
                border: `1px solid ${LINE}`,
                '&:hover': { bgcolor: '#e4eaec', color: INK },
              }}
            >
              <AddIcon sx={{ fontSize: 15 }} />
            </IconButton>
          </Tooltip>
        </Box>

        {/* ── Section label ────────────────────────────────────────── */}
        <Box sx={{ px: '16px', mb: '8px' }}>
          <Typography sx={{ fontSize: 11, fontWeight: 600, color: MUTED, textTransform: 'uppercase', letterSpacing: '0.6px' }}>
            Models{models.length > 0 && ` · ${models.length}`}
          </Typography>
        </Box>

        {/* ── Search ───────────────────────────────────────────────── */}
        <Box
          sx={{
            mx: '10px',
            mb: '8px',
            display: 'flex',
            alignItems: 'center',
            gap: '6px',
            bgcolor: '#fff',
            border: `1px solid ${LINE}`,
            borderRadius: '8px',
            px: '10px',
            py: '6px',
          }}
        >
          <SearchIcon sx={{ fontSize: 14, color: '#a0adb0', flexShrink: 0 }} />
          <InputBase
            placeholder="Rechercher…"
            value={search}
            onChange={e => setSearch(e.target.value)}
            inputProps={{ 'aria-label': 'Rechercher' }}
            sx={{
              fontSize: 12.5,
              color: INK,
              flex: 1,
              '& input::placeholder': { color: '#a0adb0' },
            }}
          />
        </Box>

        {/* ── List content (scrollable) ─────────────────────────────── */}
        <Box sx={{ flex: 1, overflowY: 'auto', pb: '8px' }}>
          <SqlFileList search={search} />
        </Box>

        {/* ── Footer ───────────────────────────────────────────────── */}
        <Box
          sx={{
            borderTop: `1px solid ${LINE}`,
            px: '14px',
            py: '10px',
            display: 'flex',
            flexDirection: 'column',
            gap: '8px',
          }}
        >
          <Box sx={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
            <Box sx={{ width: 6, height: 6, borderRadius: '50%', bgcolor: '#23a26d', flexShrink: 0 }} />
            <Typography sx={{ fontSize: 11.5, color: MUTED }}>Connecté · :8080</Typography>
          </Box>

          <FormControl fullWidth variant="outlined" size="small">
            <InputLabel id="lang-label" sx={{ fontSize: 12 }}>Langue</InputLabel>
            <Select
              labelId="lang-label"
              value={language}
              onChange={handleChangeLanguage}
              label="Langue"
              sx={{ fontSize: 12, borderRadius: '8px', color: INK, bgcolor: '#fff' }}
            >
              <MenuItem value="en" sx={{ fontSize: 12 }}>English</MenuItem>
              <MenuItem value="fr" sx={{ fontSize: 12 }}>Français</MenuItem>
            </Select>
          </FormControl>
        </Box>
      </Drawer>
    </>
  );
};

export default DrawerComponent;
