import UploadIcon from '@mui/icons-material/Upload';
import { Dialog, DialogActions, DialogContent, DialogTitle, Tooltip, Typography } from '@mui/material';
import React, { useRef, useState } from 'react';
import * as XLSX from 'xlsx';
import { MutedIconButton, PrimaryButton } from '../style/AppButtons';
import { StyledButton } from '../style/StyledComponents';

interface ExcelUploaderProps {
    onUpload: (data: Record<string, any[]>) => void;
}

const ExcelUploader: React.FC<ExcelUploaderProps> = ({ onUpload }) => {
    const [open, setOpen] = useState(false);
    const fileInputRef = useRef<HTMLInputElement>(null);

    const handleOpen = () => setOpen(true);
    const handleClose = () => setOpen(false);

    const handleFileUpload = (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (file) {
            const reader = new FileReader();
            reader.onload = (e) => {
                const data = new Uint8Array(e.target?.result as ArrayBuffer);
                const workbook = XLSX.read(data, { type: 'array' });
                const result: Record<string, any[]> = {};

                workbook.SheetNames.forEach((sheetName) => {
                    const worksheet = workbook.Sheets[sheetName];
                    const rawData = XLSX.utils.sheet_to_json(worksheet, { defval: null });

                    const processedData = rawData.map((row) => {
                        // Assert the type of row to avoid TypeScript errors
                        return Object.fromEntries(
                            Object.entries(row as Record<string, any>).map(([key, value]) => [key, value === '' ? null : value])
                        );
                    });
                    result[sheetName] = processedData;
                });

                console.log(result);
                onUpload(result);
                handleClose();
            };
            reader.readAsArrayBuffer(file);
        }
    };

    return (
        <div>
            <Tooltip title="Modifier les données de test">
                <MutedIconButton size="small" onClick={handleOpen}>
                    <UploadIcon sx={{ fontSize: 14 }} />
                </MutedIconButton>
            </Tooltip>
            <Dialog open={open} onClose={handleClose}>
                <DialogTitle>Uploader des Exemples</DialogTitle>
                <DialogContent>
                    <Typography variant="body1">
                        Veuillez choisir le fichier Excel contenant les exemples.
                    </Typography>
                    <input
                        ref={fileInputRef}
                        accept=".xlsx, .xls"
                        type="file"
                        style={{ display: 'none' }}
                        onChange={handleFileUpload}
                    />
                    <PrimaryButton onClick={() => fileInputRef.current?.click()}>
                        <UploadIcon sx={{ mr: 1 }} /> Choisir un fichier
                    </PrimaryButton>
                </DialogContent>
                <DialogActions>
                    <StyledButton onClick={handleClose}>
                        Annuler
                    </StyledButton>
                </DialogActions>
            </Dialog>
        </div>
    );
};

export default React.memo(ExcelUploader);
