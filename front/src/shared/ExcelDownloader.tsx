import DownloadIcon from '@mui/icons-material/Download';
import { Tooltip } from '@mui/material';
import React from 'react';
import * as XLSX from 'xlsx';
import { MutedIconButton } from '../style/AppButtons';

interface ExcelDownloaderProps {
    data: Record<string, any[]>;
    fileName: string;
}

const ExcelDownloader: React.FC<ExcelDownloaderProps> = ({ data, fileName = 'data.xlsx' }) => {
    const downloadExcel = () => {
        // Create a new workbook
        const workbook = XLSX.utils.book_new();

        // Iterate over each key in the data object
        const usedNames = new Set<string>();
        Object.keys(data).forEach((key) => {
            // Convert each array in the data object to a worksheet
            const worksheet = XLSX.utils.json_to_sheet(data[key]);
            // Append the worksheet to the workbook with the key as the sheet name
            let sheetName = key.slice(0, 31);
            if (usedNames.has(sheetName)) {
                let i = 1;
                while (usedNames.has(`${sheetName.slice(0, 28)}_${i}`)) i++;
                sheetName = `${sheetName.slice(0, 28)}_${i}`;
            }
            usedNames.add(sheetName);
            XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
        });

        // Write the workbook and create a blob
        const excelBuffer = XLSX.write(workbook, { bookType: 'xlsx', type: 'array' });
        const blob = new Blob([excelBuffer], { type: 'application/octet-stream' });
        const url = window.URL.createObjectURL(blob);

        // Create a link and trigger the download
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
