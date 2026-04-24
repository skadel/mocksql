import BorderColorIcon from '@mui/icons-material/BorderColor';
import ChevronLeftIcon from '@mui/icons-material/ChevronLeft';
import ChevronRightIcon from '@mui/icons-material/ChevronRight';
import { Alert, Avatar, Box, Container, Divider, Drawer, FormControl, IconButton, InputLabel, MenuItem, Select, Tooltip, Typography } from '@mui/material';
import List from '@mui/material/List';
import ListItem from '@mui/material/ListItem';
import ListItemButton from '@mui/material/ListItemButton';
import { SelectChangeEvent } from '@mui/material/Select';
import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link as RouterLink } from "react-router-dom";
import { useAppDispatch, useAppSelector } from '../../../app/hooks';
import { resetContext } from '../../buildModel/buildModelSlice';
import { setCurrentId, toggleDrawer } from '../appBarSlice';
export const drawerWidth = 250;

const DrawerComponent: React.FC = () => {
    const dispatch = useAppDispatch();
    const { t, i18n } = useTranslation();
    const [language, setLanguage] = useState(i18n.language);
    const [error, setError] = useState<string | null>(null); // State for error message
    const currentProjectId = useAppSelector(state => state.appBarModel.currentProjectId);
    const drawerOpen = useAppSelector(state => state.appBarModel.drawerOpen);

    const handleChangeLanguage = (event: SelectChangeEvent<string>) => {
        const selectedLanguage = event.target.value as string;
        setLanguage(selectedLanguage);
        i18n.changeLanguage(selectedLanguage);
    };

    return (
        <>

        {/* Toggle button — visible toujours, positionné sur le bord du drawer */}
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
                    bgcolor: '#f0f0f0',
                    border: '1px solid #ddd',
                    borderRadius: '0 4px 4px 0',
                    width: 20,
                    height: 40,
                    '&:hover': { bgcolor: '#e0e0e0' },
                    p: 0,
                }}
            >
                {drawerOpen ? <ChevronLeftIcon sx={{ fontSize: 16 }} /> : <ChevronRightIcon sx={{ fontSize: 16 }} />}
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
                    backgroundColor: '#f0f0f0',
                    display: 'flex',
                    flexDirection: 'column',
                    justifyContent: 'space-between',
                    transform: drawerOpen ? 'translateX(0)' : `translateX(-${drawerWidth}px)`,
                    transition: 'transform 0.2s ease',
                    visibility: drawerOpen ? 'visible' : 'hidden',
                    overflow: 'hidden',
                }
            }}
            variant="permanent"
            anchor="left"
        >
            <Box>
                <List sx={{ mt: 0, mx: 1, mb: 0, p:0 }}>
                    <ListItem disablePadding>
                        <ListItemButton
                          component={RouterLink}
                          to={currentProjectId ? `/models/${currentProjectId}` : '#'}
                          onClick={() => {
                            if (!currentProjectId) {
                              setError(t('errors.selectProjectError'));
                              return;
                            }
                            dispatch(setCurrentId(''));
                            dispatch(resetContext());
                          }}
                          disabled={!currentProjectId}
                          sx={{ display: 'flex', justifyContent: 'space-between' }}
                        >
                          <Box sx={{ display: 'flex', alignItems: 'center' }}>
                            <Avatar
                              src="/static/logo192.png"
                              alt="Bot Avatar"
                              sx={{ mr: 1, width: 55, height: 55 }}
                            />
                            <Typography variant="h6" noWrap>
                              MockSQL
                            </Typography>
                          </Box>
                          <Tooltip title={t('newModel')} placement="top">
                            <BorderColorIcon />
                          </Tooltip>
                        </ListItemButton>
                    </ListItem>
                </List>
                <Divider />
                {error && ( // Display error if present
                    <Alert severity="error" sx={{ mx: 2, my: 1 }}>
                        {error}
                    </Alert>
                )}
                <Container disableGutters sx={{ margin: 0 , p: 0, paddingLeft: 1, paddingRight: 1}}>
                    {/* SQL file list — TODO: replace ListModels with file autocomplete */}
                </Container>
            </Box>
            <Box>
                <Divider />
                <Container sx={{ py: 2 }}>
                <FormControl fullWidth variant="outlined">
                    <InputLabel 
                        id="language-select-label" 
                        sx={{ fontSize: '0.875rem' }} // Smaller label font size
                    >
                        {t('language')}
                    </InputLabel>
                    <Select
                        labelId="language-select-label"
                        id="language-select"
                        value={language}
                        onChange={handleChangeLanguage}
                        label={t('language')}
                        sx={{
                            fontSize: '0.875rem', // Smaller select text
                        }}
                    >
                        <MenuItem value="en" sx={{ fontSize: '0.875rem' }}>English</MenuItem>
                        <MenuItem value="fr" sx={{ fontSize: '0.875rem' }}>Français</MenuItem>
                    </Select>
                </FormControl>
                </Container>
            </Box>
        </Drawer>

</>
        
    );
};

export default DrawerComponent;
