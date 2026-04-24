import Autocomplete from '@mui/material/Autocomplete';
import CircularProgress from '@mui/material/CircularProgress';
import TextField from '@mui/material/TextField';
import React, { useEffect, useState } from 'react';
import { focusedStyles } from '../../style/StyledComponents';

interface AutocompleteInputProps {
  onSelectSuggestion: (sugggestion: string | null) => void;
  fetchSuggestions: (inputValue: string, project?: string | undefined) => Promise<string[]>;
  resetInputValueOnSelect?: boolean;
  labelName: string;
  setValue: (value: string) => void;
  value?: string;
}


const AutocompleteInput: React.FC<AutocompleteInputProps> = ({ onSelectSuggestion, fetchSuggestions, labelName, setValue, value }) => {
  const [open, setOpen] = useState<boolean>(false);
  const [options, setOptions] = useState<string[]>([]);
  const [loading, setLoading] = useState<boolean>(false);

  useEffect(() => {
    let active = true;
    setLoading(true);
    fetchSuggestions(value || '').then((suggestions) => {
      if (active) {
        setOptions(suggestions);
        setLoading(false);
      }
    });

    return () => {
      active = false;
    };
  }, [value, fetchSuggestions]);

  useEffect(() => {
    setValue(value || '');
  }, [value, setValue]);

  function handleChange(newValue: string | null): void {
    onSelectSuggestion(newValue);
  }

  return (
    <Autocomplete
      id="fetch-autocomplete"
      open={open}
      onOpen={() => setOpen(true)}
      onClose={() => setOpen(false)}
      getOptionLabel={(option: string) => option}
      options={options}
      loading={loading}
      onInputChange={(_, value) => setValue(value)}
      onChange={(_, newValue) => handleChange(newValue)}
      value={value || ''}
      inputValue={value || ''}
      sx={focusedStyles}
      renderInput={(params) => (
        <TextField
          {...params}
          label={labelName}
          variant="outlined"
          InputProps={{
            ...params.InputProps,
            endAdornment: (
              <React.Fragment>
                {loading ? <CircularProgress color="inherit" size={20} /> : null}
                {params.InputProps.endAdornment}
              </React.Fragment>
            ),
          }}
        />
      )}
    />
  );
};

export default AutocompleteInput;
