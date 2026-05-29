import UploadIcon from '@mui/icons-material/Upload';
import { Dialog, DialogActions, DialogContent, DialogTitle, Tooltip, Typography } from '@mui/material';
import ExcelJS from 'exceljs';
import React, { useRef, useState } from 'react';
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

    const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
        const file = event.target.files?.[0];
        if (!file) return;

        const buffer = await file.arrayBuffer();
        const workbook = new ExcelJS.Workbook();
        await workbook.xlsx.load(buffer);

        const result: Record<string, any[]> = {};

        workbook.eachSheet((worksheet) => {
            const rows: Record<string, any>[] = [];
            let headers: string[] = [];

            worksheet.eachRow((row, rowNumber) => {
                const values = (row.values as any[]).slice(1); // ExcelJS row values are 1-indexed
                if (rowNumber === 1) {
                    headers = values.map((v) => String(v ?? ''));
                } else {
                    const obj: Record<string, any> = {};
                    headers.forEach((h, i) => {
                        const val = values[i];
                        obj[h] = val === undefined || val === '' ? null : val;
                    });
                    rows.push(obj);
                }
            });

            result[worksheet.name] = rows;
        });

        console.log(result);
        onUpload(result);
        handleClose();
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
