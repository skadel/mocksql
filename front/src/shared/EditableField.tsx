import React, { ChangeEvent, useMemo } from "react";
import {
  Box,
  CircularProgress,
  Fade,
  TextField,
  Typography,
  InputAdornment,
  useTheme,
  styled,
} from "@mui/material";

// Couleurs importées
import { primaryColor, primaryColorLight } from "../style/StyledComponents";

interface Props {
  label: string;
  value: string;
  onChange: (v: string) => void;
  /** Affiche un textarea plutôt qu'un input */
  textarea?: boolean;
  /** Affiche un spinner pendant le chargement (l'utilisateur peut continuer à taper) */
  loading?: boolean;
  /** Limite de caractères : affiche un compteur (facultatif) */
  maxLength?: number;
}

/* -------------------------------------------------------------------------- */
/*                               Styled input                                */
/* -------------------------------------------------------------------------- */

const StyledTextField = styled(TextField)(() => ({
  "& .MuiOutlinedInput-root": {
    borderRadius: 10,
    backgroundColor: primaryColorLight,
    transition: "box-shadow 0.2s ease, background-color 0.2s ease",
    "& fieldset": {
      borderColor: primaryColor,
    },
    "&:hover fieldset": {
      borderColor: primaryColor,
    },
    "&.Mui-focused": {
      backgroundColor: "#fff",
      boxShadow: `${primaryColor}40 0 0 0 2px`, // anneau focus semi-transparent
      "& fieldset": {
        borderColor: primaryColor,
      },
    },
    "&.Mui-error fieldset": {
      borderColor: "#f44336",
    },
  },
  "& .MuiInputLabel-root.Mui-focused": {
    color: primaryColor,
  },
  "& .MuiInputLabel-outlined.MuiInputLabel-shrink": {
    transform: "translate(14px, -6px) scale(0.75)",
    backgroundColor: "#fff",
    padding: "0 4px",
    zIndex: 1,
  },
  "& .MuiOutlinedInput-notchedOutline legend": {
    width: 0,
  },
}));

/* -------------------------------------------------------------------------- */

const EditableField: React.FC<Props> = ({
  label,
  value,
  onChange,
  textarea = false,
  loading = false,
  maxLength,
}) => {
  const theme = useTheme();

  const counter = useMemo(() => {
    if (!maxLength) return null;
    return `${value.length}/${maxLength}`;
  }, [value, maxLength]);

  return (
    <Box sx={{ position: "relative", width: "100%" }}>
      <Typography variant="subtitle2" gutterBottom>
        {label}
      </Typography>

      <StyledTextField
        variant="outlined"
        multiline={textarea}
        minRows={textarea ? 3 : undefined}
        maxRows={textarea ? 12 : undefined}
        value={value}
        onChange={(e: ChangeEvent<HTMLInputElement>) => onChange(e.target.value)}
        fullWidth
        size="small"
        inputProps={{ maxLength }}
        InputProps={{
          endAdornment: loading ? (
            <InputAdornment position="end" sx={{ mr: 0.5 }}>
              <CircularProgress size={18} thickness={4} />
            </InputAdornment>
          ) : undefined,
        }}
        helperText={counter}
        FormHelperTextProps={{ sx: { textAlign: "right", mt: 0.5 } }}
      />

      {/* Overlay : un léger fond + pointerEvents none */}
      <Fade in={loading} unmountOnExit>
        <Box
          sx={{
            position: "absolute",
            inset: 0,
            bgcolor: theme.palette.mode === "light" ? "rgba(255,255,255,0.4)" : "rgba(0,0,0,0.3)",
            backdropFilter: "blur(2px)",
            borderRadius: 1,
            pointerEvents: "none",
          }}
        />
      </Fade>
    </Box>
  );
};

export default EditableField;
