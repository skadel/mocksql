import { Box } from '@mui/material';
import { highlight, languages } from 'prismjs';
import 'prismjs/components/prism-sql';
import React from 'react';
import Editor from 'react-simple-code-editor';

interface SqlEditorProps {
  value: string;
  onChange?: (v: string) => void;
  readOnly?: boolean;
  disabled?: boolean;
  maxHeight?: number;
  fontSize?: number;
  minHeight?: number;
  background?: string;
  onKeyDown?: React.KeyboardEventHandler;
}

const SqlEditor: React.FC<SqlEditorProps> = ({
  value,
  onChange,
  readOnly,
  disabled,
  maxHeight = 360,
  fontSize = 13,
  minHeight = 100,
  background,
  onKeyDown,
}) => (
  <Box
    onKeyDown={onKeyDown}
    sx={{
      maxHeight,
      overflowY: 'auto',
      '& .npm__react-simple-code-editor__textarea': { outline: 'none !important' },
    }}
  >
    <Editor
      value={value}
      onValueChange={onChange ?? (() => {})}
      highlight={(code) => highlight(code, languages.sql, 'sql')}
      padding={14}
      readOnly={readOnly}
      style={{
        fontFamily: '"Fira Mono", "Fira Code", monospace',
        fontSize,
        minHeight,
        ...(background !== undefined ? { background } : {}),
      }}
      disabled={disabled}
    />
  </Box>
);

export default SqlEditor;
