import UploadIcon from '@mui/icons-material/Upload';
import { Button, Dialog, DialogActions, DialogContent, DialogTitle, Typography } from '@mui/material';
import React, { useState } from 'react';
import { StyledButton } from '../style/StyledComponents';

interface CsvUploaderProps {
    onUpload: (data: Record<string, any[]>) => void;
}

const CsvUploader: React.FC<CsvUploaderProps> = ({ onUpload }) => {
    const [open, setOpen] = useState(false);

    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);

    const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = (e) => {
                const text = e.target?.result as string;
                const rows = text.split('\n').map((row) => row.split(','));

                // Assume the first row is the header
                const [header, ...data] = rows;
                const result = data.map((row) =>
                    Object.fromEntries(header.map((key, i) => [key, row[i] === '' ? null : row[i]]))
                );

                onUpload({ data: result });
                handleClose();
            };
            reader.readAsText(file);
        }
    };

    return (
        <div>
            <StyledButton onClick={handleOpen}>
                <UploadIcon />    Fournir vos exemples
            </StyledButton>
            <Dialog open={open} onClose={handleClose}>
                <DialogTitle>Uploader des Exemples CSV</DialogTitle>
                <DialogContent>
                    <Typography variant="body1">
                        Veuillez choisir le fichier CSV contenant les exemples.
                    </Typography>
                    <input
                        accept=".csv"
                        id="file-upload"
                        type="file"
                        style={{ display: 'none' }}
                        onChange={handleFileUpload}
                    />
                    <label htmlFor="file-upload">
                        <Button component="span">
                            <UploadIcon />    Choisir un fichier
                        </Button>
                    </label>
                </DialogContent>
                <DialogActions>
                    <Button onClick={handleClose}>
                        Annuler
                    </Button>
                </DialogActions>
            </Dialog>
        </div>
    );
};

export default React.memo(CsvUploader);
