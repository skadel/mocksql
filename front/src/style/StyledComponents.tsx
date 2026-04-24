import { Accordion, AccordionDetails, AccordionSummary, Box, Button, FormControl, IconButton, List, ListItem, StepConnector, StepIcon, StepLabel, Stepper, TextField } from '@mui/material';
import { createTheme } from '@mui/material/styles';
import { styled } from '@mui/system';

const theme = createTheme({
  palette: {
    action: {
      disabledBackground: '#e0e0e0', // Set your desired disabled background color
      disabled: 'rgba(0, 0, 0, 0.26)', // Set your desired disabled text color
    },
  },
});


export const Container = styled(Box)({
    padding: '20px',
    background: '#ffffff',
});

export const StyledList = styled(List)({
    background: '#ffffff',
    overflow: 'hidden',
});

export const StyledListItem = styled(ListItem)({
    display: 'flex',
    justifyContent: 'space-between',
    alignItems: 'center',
    padding: '8px 16px',
    border: '1px solid #ddd',
    borderRadius: '4px',
});

export const StyledButton = styled(IconButton)(({ theme }) => ({
  marginLeft: theme.spacing(1),
  padding: theme.spacing(1),
  borderRadius: '20px',
  backgroundColor: 'rgba(28, 168, 164, 0.1)',
  color: '#1B3139',
  fontSize: '16px',
  border: '2px solid rgba(28, 168, 164)',
  transition: 'all 0.3s ease',
  boxShadow: '0px 2px 4px rgba(0, 0, 0, 0.1)',
  '&:hover': {
    backgroundColor: 'rgba(28, 168, 164, 0.2)',
    boxShadow: '0px 4px 8px rgba(0, 0, 0, 0.15)',
    transform: 'translateY(-2px)',
  },
  '& .MuiSvgIcon-root': {
    fontSize: '25px',
  },
  '&:disabled': {
    backgroundColor: theme.palette.action.disabledBackground,
    color: theme.palette.action.disabled,
    cursor: 'not-allowed',
    boxShadow: 'none',
    '&:hover': {
      backgroundColor: theme.palette.action.disabledBackground,
    },
  },
}));


export const StyledUploadButton = styled(Button)({
  display: 'inline-flex',
  alignItems: 'center',
  justifyContent: 'center',
  padding: '8px',
  borderRadius: '20px',
  backgroundColor: 'rgba(28, 168, 164, 0.1)',
  color: '#1B3139',
  fontSize: '16px',
  margin: '5px',
  transition: 'background-color 0.3s ease-in-out',
  border: '2px solid rgba(28, 168, 164)',
  minWidth: 'unset',
  width: 'auto',
  '&:hover': {
    backgroundColor: '#b0bec5',
  },
  '& .MuiSvgIcon-root': {
    fontSize: '25px',
    marginRight: '8px',
  },
});

export const LargeStyledButton = styled(IconButton)({
  marginLeft: '8px',
  padding: '12px', // Increased padding for a larger button
  borderRadius: '4px',
  backgroundColor: '#1B3139',
  color: '#FFFFFF',
  fontSize: '18px', // Increased font size for better visibility
  transition: 'background-color 0.3s ease-in-out',
  shape: {
    borderRadius: 30,
  }, 
  '&:hover': {
    backgroundColor: '#627483',
  },
  '& .MuiSvgIcon-root': {
    fontSize: '24px', // Increased icon size for better visibility
  },
  // Styles for the disabled state
  '&:disabled': {
    backgroundColor: theme.palette.action.disabledBackground,
    color: theme.palette.action.disabled,
    cursor: 'not-allowed',
    '&:hover': {
      backgroundColor: theme.palette.action.disabledBackground,
    },
  },
});


export const StyledFormControl = styled(FormControl)`
  width: 100%;
`;

export const StyledAccordion = styled(Accordion)({
  border: '1px solid #ddd',
  boxShadow: 'none',
  '&:not(:last-child)': {
    borderBottom: 0,
  },
});

export const StyledAccordionSummary = styled(AccordionSummary)({
  backgroundColor: '#f5f5f5',
  borderBottom: '1px solid #ddd',
  marginBottom: -1,
  minHeight: 56,
  '&.Mui-expanded': {
    minHeight: 56,
  },
});

export const StyledAccordionDetails = styled(AccordionDetails)({
  padding: '16px',
});

export const StyledStepper = styled(Stepper)({
  backgroundColor: 'transparent',
});

export const StyledStepLabel = styled(StepLabel)({
  '&.MuiStepLabel-active': {
    color: 'green', // Customize active step label color
  },
  '&.MuiStepLabel-completed': {
    color: 'blue', // Customize completed step label color
  },
});

export const StyledStepConnector = styled(StepConnector)({
  '&.MuiStepConnector-line': {
    borderColor: '#ccc', // Customize connector line color
  },
});

export const StyledStepIcon = styled(StepIcon)({
  root: {
    color: '#555', // Customize step icon color
    display: 'flex',
    height: 22,
    alignItems: 'center',
  },
  active: {
    color: 'green', // Customize active step icon color
  },
  completed: {
    color: 'blue', // Customize completed step icon color
  },
});

export const WhiteBorderTextField = styled(TextField)`
  & label.Mui-focused {
    color: black;
  }
  
  & .MuiOutlinedInput-root {
    fieldset {
      border-color: white;
    }
    
    &:hover fieldset {
      border-color: white;
    }
    
    &.Mui-focused fieldset {
      border-color: white;
    }
  }
`;

export const CenteredIconButton = styled(Button)(({ theme }) => ({
  backgroundColor: 'black',
  color: 'white',
  display: 'flex',
  justifyContent: 'center',
  alignItems: 'center',
  padding: '6px', // Reduce padding to make the button smaller
  fontSize: '1rem', // Adjust font size as needed
  minWidth: 'auto', // Remove minimum width to ensure the button size is just right for the icon
  height: 'auto', // Adjust height if needed
  '& .MuiButton-startIcon': {
    margin: 0, // Remove default margin to center the icon
  },
}));


export const primaryColor = 'rgba(28, 168, 164)';
export const primaryColorLight = 'rgba(28, 168, 164, 0.1)';
export const focusedStyles = {
  '& .MuiOutlinedInput-root.Mui-focused .MuiOutlinedInput-notchedOutline': {
    borderColor: primaryColor,
  },
  '& .MuiInputLabel-root.Mui-focused': {
    color: primaryColor,
  },
  '& .MuiInputLabel-outlined.MuiInputLabel-shrink': {
    transform: 'translate(14px, -6px) scale(0.75)', // Position ajustée
    backgroundColor: '#fff', // Fond blanc pour éviter qu'il ne se fonde avec la bordure
    padding: '0 4px', // Ajout d'espace pour un affichage propre
    zIndex: 1, // Assure que le label apparaît au-dessus de la bordure
  },
  '& .MuiOutlinedInput-notchedOutline legend': {
    width: 0, // Empêche la superposition de texte dans l'encadré du champ
  },
};

export const containerStyles = {
        maxWidth: '800px',
        height: '95vh',
        margin: '0 auto',
        padding: 4,
        backgroundColor: '#ffffff',
        borderRadius: '16px',
        boxShadow: '0 8px 20px rgba(0, 0, 0, 0.1)',
        overflow: 'auto',
        display: 'flex',
        flexDirection: 'column',
      }

export const stepperStyles =  {
  marginBottom: 4,
  '& .MuiStepLabel-iconContainer .Mui-completed': {
    color: '#4caf50',
  },
  '& .MuiStepLabel-iconContainer .Mui-active': {
    color: '#1ca8a4',
  },
  '& .MuiStepLabel-root': {
    fontSize: '14px',
    '& .MuiStepLabel-label': {
      fontWeight: 'bold',
      color: '#333',
    },
  },
}

export const layoutStyles = {
  flex: 1,
  overflow: 'auto',
  paddingBottom: 4,
}