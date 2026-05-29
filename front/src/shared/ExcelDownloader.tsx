import DownloadIcon from '@mui/icons-material/Download';
import { Tooltip } from '@mui/material';
import ExcelJS from 'exceljs';
import React from 'react';
import { MutedIconButton } from '../style/AppButtons';

interface ExcelDownloaderProps {
    data: Record<string, any[]>;
    fileName: string;
}

const ExcelDownloader: React.FC<ExcelDownloaderProps> = ({ data, fileName = 'data.xlsx' }) => {
    const downloadExcel = async () => {
        const workbook = new ExcelJS.Workbook();

        const usedNames = new Set<string>();
        Object.keys(data).forEach((key) => {
            let sheetName = key.slice(0, 31);
            if (usedNames.has(sheetName)) {
                let i = 1;
                while (usedNames.has(`${sheetName.slice(0, 28)}_${i}`)) i++;
                sheetName = `${sheetName.slice(0, 28)}_${i}`;
            }
            usedNames.add(sheetName);

            const worksheet = workbook.addWorksheet(sheetName);
            const rows = data[key];
            if (rows.length > 0) {
                const headers = Object.keys(rows[0]);
                worksheet.addRow(headers);
                rows.forEach((row) => worksheet.addRow(headers.map((h) => row[h])));
            }
        });

        const buffer = await workbook.xlsx.writeBuffer();
        const blob = new Blob([buffer], {
            type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        });
        const url = window.URL.createObjectURL(blob);

        const link = document.createElement('a');
        link.href = url;
        link.setAttribute('download', fileName);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
        window.URL.revokeObjectURL(url);
    };

    return (
        <Tooltip title="Télécharger les données de test">
            <MutedIconButton size="small" onClick={downloadExcel}>
                <DownloadIcon sx={{ fontSize: 14 }} />
            </MutedIconButton>
        </Tooltip>
    );
};

export default React.memo(ExcelDownloader);
