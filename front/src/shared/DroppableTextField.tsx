import SendIcon from '@mui/icons-material/Send';
import StopIcon from '@mui/icons-material/Stop';
import { InputAdornment } from '@mui/material';
import React, { useCallback } from 'react';
import { useAppDispatch, useAppSelector } from '../app/hooks';
import { setLoading } from '../features/buildModel/buildModelSlice';
import { CenteredIconButton, WhiteBorderTextField } from '../style/StyledComponents';

type DroppableTextFieldProps = {
  userInput: string;
  setUserInput: React.Dispatch<React.SetStateAction<string>>;
  sendMessage: () => Promise<void> | void;
  stopStream: () => void;
  disabled?: boolean;
  onFocus?: () => void;
  placeholder?: string;
};

const DroppableTextField: React.FC<DroppableTextFieldProps> = ({
  userInput,
  setUserInput,
  sendMessage,
  stopStream,
  disabled = false,
  onFocus,
  placeholder = 'Écrivez votre message…',
}) => {
  const dispatch = useAppDispatch();
  const { loading } = useAppSelector((state) => state.buildModel);
  const handleKeyDown = (event: React.KeyboardEvent) => {
    if (disabled) return;
    if (event.key === 'Enter' && !event.shiftKey) {
      event.preventDefault();
      sendMessage();
    }
  };

  const handleDrop = useCallback((event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
    if (disabled) return;

    if (event.dataTransfer.files && event.dataTransfer.files.length > 0) {
      const file = event.dataTransfer.files[0];
      const reader = new FileReader();
      reader.onload = (readEvent: ProgressEvent<FileReader>) => {
        setUserInput((readEvent.target?.result as string) ?? '');
      };
      reader.readAsText(file);
      event.dataTransfer.clearData();
    }
  }, [setUserInput, disabled]);

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    event.stopPropagation();
  };

  const handleSend = () => {
    if (disabled) return;
    sendMessage();
  };

  const handleStop = () => {
    stopStream();
    dispatch(setLoading(false));
  };

  return (
    <div
      onDrop={handleDrop}
      onDragOver={handleDragOver}
      style={{
        width: '100%',
        boxSizing: 'border-box',
        opacity: disabled ? 0.7 : 1,
        pointerEvents: 'auto',
        position: 'relative',
      }}
    >
      <WhiteBorderTextField
        label="Message MockSQL"
        variant="outlined"
        fullWidth
        multiline
        rows={3}
        margin="normal"
        value={userInput}
        onChange={(e) => setUserInput(e.target.value)}
        onKeyDown={handleKeyDown}
        onFocus={onFocus}
        placeholder={placeholder}
        sx={{
          borderRadius: '20px',
          backgroundColor: "white",
          boxShadow: "0 2px 5px rgba(0, 0, 0, 0.1)",
          transition: "box-shadow 0.3s ease-in-out",
          "&:hover": { boxShadow: "0 4px 10px rgba(0, 0, 0, 0.2)" },
          "& .MuiOutlinedInput-root": {
            borderRadius: "20px",
            "& fieldset": { borderRadius: "20px", borderColor: "gray" },
            "&:hover fieldset": { borderRadius: "20px", borderColor: "darkgray" },
            "&.Mui-focused": {
              boxShadow: "0 4px 10px rgba(0, 0, 0, 0.3)",
              "& fieldset": { borderRadius: "20px", borderColor: "rgba(28, 168, 164, 0.6)" },
            },
          },
          color: "rgba(28, 168, 164)",
          fontFamily: "Arial, sans-serif",
          marginTop: 1,
        }}
        InputProps={{
          readOnly: disabled,
          endAdornment: (
            <InputAdornment position="end">
              {loading ? (
                <CenteredIconButton
                  onClick={handleStop}
                  disabled={false}
                  sx={{
                    backgroundColor: "black",
                    color: "white",
                    marginRight: 1,
                    "&:hover": { backgroundColor: "rgba(0,0,0,0.3)" },
                  }}
                >
                  <StopIcon />
                </CenteredIconButton>
              ) : (
                <CenteredIconButton
                  onClick={handleSend}
                  disabled={disabled}
                  sx={{
                    backgroundColor: "black",
                    color: "white",
                    marginRight: 1,
                    "&:hover": {
                      backgroundColor: "rgba(28,168,164)",
                    },
                  }}
                >
                  <SendIcon />
                </CenteredIconButton>
              )}
            </InputAdornment>
          ),
        }}
        inputProps={{ readOnly: disabled }}
      />

    </div>
  );
};

export default DroppableTextField;
