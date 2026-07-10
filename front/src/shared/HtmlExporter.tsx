import IosShareIcon from '@mui/icons-material/IosShare';
import { Tooltip } from '@mui/material';
import React from 'react';
import { MutedIconButton } from '../style/AppButtons';
import { getVerdictInfo } from '../utils/verdict';

interface HtmlExporterProps {
  tests: any[];
  sqlFileName?: string;
}

/* ─── coverage (mirrors TestsPanel logic) ─────────────────────────── */
const BUCKETS = [
  { key: 'happy', label: 'Cas nominal',     weight: 25, re: /logique.m.tier|calcul|pourcentage|croissance|nominal|résultat|attendu|standard/ },
  { key: 'null',  label: 'Valeurs NULL',    weight: 20, re: /null.checks|null|manquant|absent/ },
  { key: 'empty', label: 'Données vides',   weight: 15, re: /vide|aucune|inexistant|0.ligne|zéro|sans.données|ensemble.vide/ },
  { key: 'dup',   label: 'Doublons',        weight: 15, re: /valeurs.dupliqu|doublon|dupliqué|répété/ },
  { key: 'limit', label: 'Valeurs limites', weight: 15, re: /cas.limites|limite|extrême|bord|boundary|borne|plage/ },
  { key: 'tie',   label: 'Tri / Ex æquo',  weight: 10, re: /ex.æquo|ex.aequo|\btie\b|classement|rang\b/ },
];

function axisCompleteness(n: number) {
  if (n === 0) return 0; if (n === 1) return 40; if (n === 2) return 65; if (n === 3) return 85;
  return 100;
}

function computeScore(tests: any[]): number {
  const counts: Record<string, number> = {};
  BUCKETS.forEach(b => { counts[b.key] = 0; });
  tests.forEach(t => {
    const s = ((t.unit_test_description ?? '') + ' ' + (t.tags ?? []).join(' ')).toLowerCase();
    BUCKETS.forEach(b => { if (b.re.test(s)) counts[b.key]++; });
  });
  let score = 0;
  BUCKETS.forEach(b => { score += (b.weight * axisCompleteness(counts[b.key])) / 100; });
  return Math.round(score);
}

/* ─── table renderer ──────────────────────────────────────────────── */
function renderTable(rows: Record<string, any>[], title: string): string {
  if (!rows || rows.length === 0) return '';
  const cols = Object.keys(rows[0]);
  const header = cols.map(c => `<th>${esc(c)}</th>`).join('');
  const body = rows.map(r =>
    `<tr>${cols.map(c => `<td>${esc(String(r[c] ?? ''))}</td>`).join('')}</tr>`
  ).join('');
  return `<p class="tbl-title">${esc(title)}</p><div class="tbl-scroll"><table><thead><tr>${header}</tr></thead><tbody>${body}</tbody></table></div>`;
}

function esc(s: string): string {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

/* ─── HTML generator ──────────────────────────────────────────────── */
function generateHtml(tests: any[], sqlFileName?: string): string {
  const score = computeScore(tests);
  const dateStr = new Date().toLocaleDateString('fr-FR', { day: 'numeric', month: 'long', year: 'numeric' });
  const title = sqlFileName ?? 'Tests SQL';

  const scoreColor = score >= 80 ? '#23a26d' : score >= 50 ? '#d89323' : '#d0503f';

  const testsHtml = tests.map((test, idx) => {
    const vi = getVerdictInfo(test);
    const inputData: Record<string, any[]> = test.data ?? test.test_data ?? {};
    const outputData: any[] = test.results_json
      ? (() => { try { return JSON.parse(test.results_json); } catch { return []; } })()
      : [];
    const tags: string[] = test.tags ?? [];

    const execBadge = vi.execStatus === 'pass'
      ? `<span class="exec pass">✅ PASS</span>`
      : vi.execStatus === 'fail'
        ? `<span class="exec fail">❌ FAIL</span>`
        : `<span class="exec pending">⏳ En cours</span>`;

    const verdictBadge = `<span class="verdict" style="color:${vi.fg};background:${vi.bg};border-color:${vi.border}">${esc(vi.label)}</span>`;
    const tagsHtml = tags.map(t => `<span class="tag">${esc(t)}</span>`).join('');

    const inputTablesHtml = Object.entries(inputData)
      .map(([name, rows]) => renderTable(rows as Record<string, any>[], name))
      .join('');
    const outputTableHtml = renderTable(outputData as Record<string, any>[], 'Résultat attendu');
    const hasData = inputTablesHtml || outputTableHtml;

    return `
<div class="test">
  <div class="test-header">
    ${execBadge}
    <h2>${esc(test.unit_test_description ?? `Test ${idx + 1}`)}</h2>
    ${verdictBadge}
  </div>
  ${tagsHtml ? `<div class="tags">${tagsHtml}</div>` : ''}
  <p class="verdict-text">${esc(vi.text)}</p>
  ${hasData ? `<details class="data-details"><summary>Voir les données</summary><div class="data-body">${inputTablesHtml}${outputTableHtml}</div></details>` : ''}
</div>`;
  }).join('');

  return `<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>${esc(title)}</title>
<style>
  *, *::before, *::after { box-sizing: border-box; }
  body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; font-size: 14px; color: #1a1a1a; background: #f7f9fa; margin: 0; padding: 0; }
  .wrap { max-width: 820px; margin: 0 auto; padding: 32px 20px 60px; }
  header { margin-bottom: 28px; }
  header h1 { font-size: 20px; font-weight: 700; margin: 0 0 4px; color: #1a1a1a; font-family: monospace; }
  .meta { font-size: 12px; color: #666; display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
  .coverage-chip { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; font-weight: 700; padding: 2px 10px; border-radius: 999px; background: #f0faf6; color: ${scoreColor}; border: 1px solid ${scoreColor}; }
  .test { background: #fff; border: 1px solid #e5e8ea; border-radius: 10px; padding: 16px 18px; margin-bottom: 12px; }
  .test-header { display: flex; align-items: flex-start; gap: 10px; flex-wrap: wrap; }
  .test-header h2 { font-size: 14px; font-weight: 600; margin: 0; flex: 1; line-height: 1.45; }
  .exec { font-size: 11px; font-weight: 700; padding: 2px 8px; border-radius: 999px; white-space: nowrap; flex-shrink: 0; margin-top: 1px; }
  .exec.pass { background: #e9f7f0; color: #23a26d; }
  .exec.fail { background: #fbeceb; color: #d0503f; }
  .exec.pending { background: #f4f7f7; color: #888; }
  .verdict { font-size: 11px; font-weight: 700; padding: 2px 9px; border-radius: 999px; border: 1px solid; white-space: nowrap; flex-shrink: 0; margin-top: 1px; }
  .tags { margin-top: 8px; display: flex; flex-wrap: wrap; gap: 5px; }
  .tag { font-size: 11px; padding: 2px 8px; border-radius: 999px; background: #f0f0f0; color: #555; }
  .verdict-text { font-size: 13px; color: #444; margin: 10px 0 0; line-height: 1.55; }
  details.data-details { margin-top: 12px; border-top: 1px solid #eee; padding-top: 10px; }
  summary { font-size: 12px; font-weight: 600; color: #2BB0A8; cursor: pointer; user-select: none; }
  summary:hover { color: #1a8a82; }
  .data-body { margin-top: 10px; }
  .tbl-title { font-size: 11px; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: .05em; margin: 12px 0 4px; }
  .tbl-scroll { overflow-x: auto; border: 1px solid #e5e8ea; border-radius: 8px; max-width: 100%; }
  table { width: 100%; border-collapse: collapse; font-size: 12px; }
  th { background: #f4f7f7; font-weight: 600; color: #555; text-align: left; padding: 5px 8px; border: 1px solid #e5e8ea; white-space: nowrap; }
  td { padding: 4px 8px; border: 1px solid #e5e8ea; color: #333; white-space: nowrap; }
  tr:nth-child(even) td { background: #fbfcfc; }
  footer { margin-top: 40px; font-size: 11px; color: #aaa; text-align: center; border-top: 1px solid #eee; padding-top: 16px; }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>${esc(title)}</h1>
    <div class="meta">
      <span>Exporté le ${dateStr}</span>
      <span>${tests.length} test${tests.length > 1 ? 's' : ''}</span>
      <span class="coverage-chip">Couverture ${score}/100</span>
    </div>
  </header>
  <main>${testsHtml}</main>
  <footer>Généré par <strong>MockSQL</strong></footer>
</div>
</body>
</html>`;
}

/* ─── Component ───────────────────────────────────────────────────── */
const HtmlExporter: React.FC<HtmlExporterProps> = ({ tests, sqlFileName }) => {
  const handleExport = () => {
    const html = generateHtml(tests, sqlFileName);
    const blob = new Blob([html], { type: 'text/html;charset=utf-8' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    const baseName = sqlFileName ? sqlFileName.replace(/\.sql$/i, '') : 'tests';
    a.href = url;
    a.download = `${baseName}-mocksql.html`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  };

  return (
    <Tooltip title="Exporter en HTML">
      <MutedIconButton size="small" onClick={handleExport}>
        <IosShareIcon sx={{ fontSize: 15 }} />
      </MutedIconButton>
    </Tooltip>
  );
};

export default React.memo(HtmlExporter);
