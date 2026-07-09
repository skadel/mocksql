/**
 * Nom d'affichage propre d'un dialecte SQL.
 *
 * La capitalisation naïve (`d.charAt(0).toUpperCase() + d.slice(1)`) produit
 * « Bigquery » pour `bigquery` — la marque s'écrit « BigQuery ». Ce helper porte
 * la casse correcte des marques ; fallback = capitalisation simple pour l'inconnu.
 */
const DIALECT_LABELS: Record<string, string> = {
  bigquery: 'BigQuery',
  postgres: 'Postgres',
  postgresql: 'PostgreSQL',
  snowflake: 'Snowflake',
  trino: 'Trino',
  duckdb: 'DuckDB',
};

export function dialectDisplayName(dialect?: string | null): string {
  const key = (dialect ?? '').toLowerCase();
  if (DIALECT_LABELS[key]) return DIALECT_LABELS[key];
  return dialect ? dialect.charAt(0).toUpperCase() + dialect.slice(1) : '';
}
