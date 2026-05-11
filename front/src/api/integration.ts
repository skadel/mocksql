import { IntegrationRunResult, IntegrationStep } from '../utils/types';

const BASE = import.meta.env.VITE_BACKEND_URL;

export interface IntegrationSourceTablesResponse {
  name: string;
  chain: IntegrationStep[];
  source_tables: Record<string, string[]>;
}

export const fetchIntegrationFiles = async (): Promise<string[]> => {
  const res = await fetch(`${BASE}/api/integration`);
  if (!res.ok) return [];
  const data = await res.json();
  return data.files ?? [];
};

export const fetchIntegrationSourceTables = async (
  filename: string,
  dialect = 'bigquery',
): Promise<IntegrationSourceTablesResponse> => {
  const res = await fetch(
    `${BASE}/api/integration/${encodeURIComponent(filename)}/source_tables?dialect=${dialect}`,
  );
  if (!res.ok) throw await res.json();
  return res.json();
};

export const saveIntegrationChain = async (body: {
  filename: string;
  name: string;
  chain: IntegrationStep[];
}): Promise<{ filename: string; saved: boolean }> => {
  const res = await fetch(`${BASE}/api/integration`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await res.json();
  return res.json();
};

export const runIntegrationApi = async (
  file: string,
  project: string,
  dialect = 'bigquery',
): Promise<IntegrationRunResult> => {
  const res = await fetch(`${BASE}/api/integration/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ file, project, dialect }),
  });
  if (!res.ok) throw await res.json();
  return res.json();
};
