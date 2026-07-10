// Static HTML table rendering for self-contained exports (no React/MUI at
// runtime). Mirrors the DisplayTable scroll contract: each table is wrapped in
// an overflow-x:auto container and cells use white-space:nowrap so wide tables
// scroll instead of squashing their columns. Markup and its scroll styling live
// together here so the two never drift apart across export surfaces.

export function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

export function renderStaticTable(rows: Record<string, any>[], title: string): string {
  if (!rows || rows.length === 0) return '';
  const cols = Object.keys(rows[0]);
  const header = cols.map((c) => `<th>${escapeHtml(c)}</th>`).join('');
  const body = rows
    .map((r) => `<tr>${cols.map((c) => `<td>${escapeHtml(String(r[c] ?? ''))}</td>`).join('')}</tr>`)
    .join('');
  return `<p class="tbl-title">${escapeHtml(title)}</p><div class="tbl-scroll"><table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table></div>`;
}

// CSS for the markup above — injected into an export's <style> block. Owning the
// scroll contract (.tbl-scroll + nowrap cells) here means a styling fix is made
// once, not re-applied in every exporter.
export const STATIC_TABLE_STYLES = `
  .tbl-title { font-size: 11px; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: .05em; margin: 12px 0 4px; }
  .tbl-scroll { overflow-x: auto; border: 1px solid #e5e8ea; border-radius: 8px; max-width: 100%; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { background: #f4f7f7; font-weight: 600; color: #555; text-align: left; padding: 5px 8px; border: 1px solid #e5e8ea; white-space: nowrap; }
  td { padding: 4px 8px; border: 1px solid #e5e8ea; color: #333; white-space: nowrap; }
  tr:nth-child(even) td { background: #fbfcfc; }`;
