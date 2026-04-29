import { useState, useEffect } from 'react';
import { fetchSqlFiles, SqlFile } from '../../../api/models';

const REFRESH_INTERVAL_MS = 10 * 60 * 1000;

export function useSqlFileLoader(): SqlFile[] {
  const [files, setFiles] = useState<SqlFile[]>([]);

  useEffect(() => {
    const load = () => fetchSqlFiles().then(setFiles);
    load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, []);

  return files;
}
