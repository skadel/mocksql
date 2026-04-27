import { useState, useEffect } from 'react';
import { fetchSqlFiles } from '../../../api/models';

const REFRESH_INTERVAL_MS = 10 * 60 * 1000;

export function useSqlFileLoader(): string[] {
  const [fileNames, setFileNames] = useState<string[]>([]);

  useEffect(() => {
    const load = () => fetchSqlFiles().then(files => setFileNames(files.map(f => f.name)));
    load();
    const id = setInterval(load, REFRESH_INTERVAL_MS);
    return () => clearInterval(id);
  }, []);

  return fileNames;
}
