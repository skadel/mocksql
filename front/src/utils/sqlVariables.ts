/**
 * Detects SQL template variables in a SQL query and renders Jinja templates.
 *
 * Detects:
 * 1. SQL dynamic functions: CURRENT_DATE, CURRENT_TIMESTAMP, NOW(), GETDATE(), CURRENT_TIME
 * 2. Jinja-style {{ identifier }} — only simple identifiers, NOT complex expressions
 * 3. dbt var() calls: {{ var('name') }}, {{ var("name") }}
 * 4. dbt ref() calls: {{ ref('model') }} — prompts user to supply the real table name
 * 5. dbt source() calls: {{ source('schema', 'table') }}
 *
 * Also provides renderJinjaTemplate() to expand {% set %} / {% for %} blocks
 * into valid SQL using user-supplied substitution values.
 */

export type SqlVarKind = 'SQL' | 'Jinja' | 'dbt' | 'ref' | 'Custom';

export interface SqlVar {
  /** The placeholder as it appears in the SQL (e.g. "CURRENT_DATE", "{{ ref('stg_orders') }}") */
  key: string;
  kind: SqlVarKind;
  /** Short human-readable note shown in the UI */
  note: string;
  /** User-configured substitution value */
  resolved: string;
}

// ---------------------------------------------------------------------------
// SQL dynamic functions
// ---------------------------------------------------------------------------

interface SqlFunctionDef {
  regex: RegExp;
  key: string;
  note: string;
}

const SQL_DYNAMIC_FUNCTIONS: SqlFunctionDef[] = [
  { regex: /\bCURRENT_DATE\b/i,      key: 'CURRENT_DATE',      note: 'Fonction SQL dynamique' },
  { regex: /\bCURRENT_TIMESTAMP\b/i, key: 'CURRENT_TIMESTAMP', note: 'Fonction SQL dynamique' },
  { regex: /\bCURRENT_TIME\b/i,      key: 'CURRENT_TIME',      note: 'Fonction SQL dynamique' },
  { regex: /\bNOW\s*\(\s*\)/i,       key: 'NOW()',             note: 'Fonction SQL dynamique' },
  { regex: /\bGETDATE\s*\(\s*\)/i,   key: 'GETDATE()',         note: 'Fonction SQL dynamique' },
  { regex: /\bSYSDATE\b/i,           key: 'SYSDATE',           note: 'Fonction SQL dynamique' },
];

// ---------------------------------------------------------------------------
// Jinja / dbt variable detection
// ---------------------------------------------------------------------------

/**
 * Returns metadata if the content inside {{ ... }} looks like a configurable variable.
 * Returns null for complex Jinja expressions we should ignore.
 *
 * Accepted:
 *   {{ ds }}                     → kind: Jinja
 *   {{ run_date }}               → kind: Jinja
 *   {{ var('my_var') }}          → kind: dbt
 *   {{ ref('stg_orders') }}      → kind: ref  (needs table substitution)
 *   {{ source('raw', 'orders') }} → kind: ref  (needs table substitution)
 *
 * Rejected (returns null):
 *   {{ 1 + 1 }}, {{ x | filter }}, {{ this.name }}, {{ config(...) }}
 */
function parseJinjaContent(
  content: string,
): { key: string; kind: 'Jinja' | 'dbt' | 'ref'; note: string } | null {
  const trimmed = content.trim();
  if (!trimmed) return null;

  // Simple identifier: [a-zA-Z_][a-zA-Z0-9_]*
  if (/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(trimmed)) {
    return {
      key: `{{ ${trimmed} }}`,
      kind: 'Jinja',
      note: 'Placeholder Jinja/template',
    };
  }

  // dbt var() call: var('name') or var("name"), optionally with default arg
  const dbtVarMatch = trimmed.match(
    /^var\s*\(\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"](?:\s*,\s*[^)]+)?\s*\)$/,
  );
  if (dbtVarMatch) {
    return {
      key: `{{ ${trimmed} }}`,
      kind: 'dbt',
      note: `Variable dbt (${dbtVarMatch[1]})`,
    };
  }

  // dbt ref() call: ref('model_name')
  const dbtRefMatch = trimmed.match(
    /^ref\s*\(\s*['"]([a-zA-Z_][a-zA-Z0-9_.+-]*)['"](?:\s*,\s*[^)]+)?\s*\)$/,
  );
  if (dbtRefMatch) {
    return {
      key: `{{ ${trimmed} }}`,
      kind: 'ref',
      note: `Modèle dbt → ${dbtRefMatch[1]}`,
    };
  }

  // dbt source() call: source('schema', 'table')
  const dbtSourceMatch = trimmed.match(
    /^source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)$/,
  );
  if (dbtSourceMatch) {
    return {
      key: `{{ ${trimmed} }}`,
      kind: 'ref',
      note: `Source dbt → ${dbtSourceMatch[1]}.${dbtSourceMatch[2]}`,
    };
  }

  return null;
}

// ---------------------------------------------------------------------------
// Public API — detection
// ---------------------------------------------------------------------------

function todayIso(): string {
  return new Date().toISOString().split('T')[0];
}

function stripSqlComments(sql: string): string {
  let s = sql.replace(/\/\*[\s\S]*?\*\//g, ' ');
  s = s.replace(/--[^\n]*/g, ' ');
  return s;
}

/**
 * Returns true if the SQL contains Jinja block tags ({% set %}, {% for %}, {% if %}, …).
 * These require rendering before the SQL can be executed.
 */
export function hasJinjaBlocks(sql: string): boolean {
  return /\{%-?\s*(?:set|for|endfor|if|elif|else|endif|macro|endmacro|call|endcall)\b/.test(sql);
}

/**
 * Detect all template variables in the given SQL string.
 */
export function detectSqlVariables(sql: string): SqlVar[] {
  if (!sql.trim()) return [];

  const today = todayIso();
  const seen = new Set<string>();
  const vars: SqlVar[] = [];

  const cleaned = stripSqlComments(sql);

  // 1. SQL dynamic functions
  for (const def of SQL_DYNAMIC_FUNCTIONS) {
    if (def.regex.test(cleaned) && !seen.has(def.key)) {
      seen.add(def.key);
      vars.push({ key: def.key, kind: 'SQL', note: def.note, resolved: today });
    }
  }

  // 2. Jinja-style {{ ... }} patterns
  const jinjaRe = /\{\{([^{}]*)\}\}/g;
  let m: RegExpExecArray | null;
  while ((m = jinjaRe.exec(cleaned)) !== null) {
    const inner = m[1];
    const parsed = parseJinjaContent(inner);
    if (parsed && !seen.has(parsed.key)) {
      seen.add(parsed.key);
      vars.push({ key: parsed.key, kind: parsed.kind, note: parsed.note, resolved: '' });
    }
  }

  return vars;
}

/**
 * Merge newly detected variables with an existing list, preserving user edits.
 */
export function mergeSqlVariables(
  detected: SqlVar[],
  existing: SqlVar[],
): SqlVar[] {
  const byKey = new Map(existing.map((v) => [v.key, v]));
  const result: SqlVar[] = [];

  for (const d of detected) {
    const prev = byKey.get(d.key);
    result.push(prev ? { ...d, resolved: prev.resolved } : d);
    byKey.delete(d.key);
  }

  for (const [, v] of byKey) {
    if (v.kind === 'Custom' || v.resolved !== '') {
      result.push(v);
    }
  }

  return result;
}

// ---------------------------------------------------------------------------
// Jinja rendering
// ---------------------------------------------------------------------------

/**
 * Parse a simple Jinja/Python value literal.
 * Handles lists, quoted strings, numbers, booleans.
 */
function parseJinjaValue(s: string): unknown {
  // List literal: ['a', 'b', 'c']
  if (s.startsWith('[') && s.endsWith(']')) {
    try {
      return JSON.parse(s.replace(/'/g, '"'));
    } catch {
      const inner = s.slice(1, -1).trim();
      if (!inner) return [];
      return inner.split(',').map((x) => x.trim().replace(/^['"]|['"]$/g, ''));
    }
  }
  // Quoted string
  if (/^['"][\s\S]*['"]$/.test(s)) return s.slice(1, -1);
  // Number
  if (/^-?\d+(\.\d+)?$/.test(s)) return Number(s);
  if (s === 'true' || s === 'True') return true;
  if (s === 'false' || s === 'False') return false;
  return undefined;
}

/**
 * Expand {% for item in list %}...{% endfor %} blocks using the given context.
 * Handles nested loops (up to 10 iterations) and whitespace-control dashes.
 */
function expandJinjaForLoops(
  template: string,
  ctx: Record<string, unknown>,
): string {
  let result = template;

  for (let pass = 0; pass < 20; pass++) {
    const forMatch = result.match(/(\{%-?\s*for\s+(\w+)\s+in\s+(\w+)\s*-?%\})/);
    if (!forMatch || forMatch.index === undefined) break;

    const [forTag, , itemVar, listVar] = forMatch;
    const forStart = forMatch.index;
    const forEnd = forStart + forTag.length;
    const stripAfterForTag = forTag.includes('-%}');

    const rest = result.slice(forEnd);
    let depth = 1;
    let bodyEnd = -1;
    let endForFull = '';

    // Walk through nested for/endfor tags
    const tagRe = /\{%-?\s*(for\s+\w+\s+in\s+\w+|endfor)\s*-?%\}/g;
    let tm: RegExpExecArray | null;
    while ((tm = tagRe.exec(rest)) !== null) {
      if (tm[1].startsWith('for ')) {
        depth++;
      } else {
        depth--;
        if (depth === 0) {
          bodyEnd = tm.index;
          endForFull = tm[0];
          break;
        }
      }
    }

    if (bodyEnd === -1) break; // Unclosed for loop — stop

    let body = rest.slice(0, bodyEnd);
    const endForStrip = endForFull.includes('-%}');
    const afterEndFor = rest.slice(bodyEnd + endForFull.length);

    // Apply whitespace-control: strip the newline right after {%- for ... -%}
    if (stripAfterForTag) body = body.replace(/^[ \t]*\n/, '');

    const list = ctx[listVar];
    if (!Array.isArray(list)) break; // Unknown list variable — stop

    let expanded = '';
    for (const item of list) {
      // Replace {{ itemVar }} occurrences inside the loop body
      const chunk = body.replace(/\{\{-?\s*([\s\S]*?)\s*-?\}\}/g, (m, inner: string) => {
        if (inner.trim() === itemVar) return String(item);
        return m;
      });
      expanded += chunk;
    }

    let after = afterEndFor;
    if (endForStrip) after = after.replace(/^[ \t]*\n/, '');

    result = result.slice(0, forStart) + expanded + after;
  }

  return result;
}

/**
 * Render a Jinja/dbt SQL template to valid SQL by:
 *  1. Stripping {# comments #}
 *  2. Evaluating {% set var = [...] %} to build a rendering context
 *  3. Expanding {% for item in list %}...{% endfor %} loops
 *  4. Substituting {{ ref('x') }}, {{ var('x') }}, {{ identifier }} with user-supplied values
 *
 * @param sql   - Raw Jinja template (may contain {% %} and {{ }} tags)
 * @param vars  - User-configured substitution values from VariablesPanel
 * @returns { rendered, hasUnresolved }
 *   - rendered: the SQL after rendering (may still contain unresolved {{ }} if user left blanks)
 *   - hasUnresolved: true if any {{ }} placeholders remain after rendering
 */
export function renderJinjaTemplate(
  sql: string,
  vars: SqlVar[],
): { rendered: string; hasUnresolved: boolean } {
  const resolvedByKey = new Map(vars.map((v) => [v.key, v.resolved]));

  let s = sql;

  // 1. Strip {# Jinja comments #}
  s = s.replace(/\{#[\s\S]*?#\}/g, '');

  // 2. Parse {% set var = value %} — build a context for loop expansion
  const ctx: Record<string, unknown> = {};
  s = s.replace(
    /\{%-?\s*set\s+(\w+)\s*=\s*([\s\S]*?)\s*-?%\}/g,
    (_match, name: string, val: string) => {
      const parsed = parseJinjaValue(val.trim());
      if (parsed !== undefined) ctx[name] = parsed;
      return ''; // Remove the set tag from output
    },
  );

  // 3. Expand {% for %}...{% endfor %} blocks
  s = expandJinjaForLoops(s, ctx);

  // 4. Substitute remaining {{ expr }} with user-provided values
  let hasUnresolved = false;
  s = s.replace(/\{\{-?\s*([\s\S]*?)\s*-?\}\}/g, (match, inner: string) => {
    const trimmed = inner.trim();
    const fullKey = `{{ ${trimmed} }}`;
    const resolved = resolvedByKey.get(fullKey);
    if (resolved) return resolved;
    if (ctx[trimmed] !== undefined) return String(ctx[trimmed]);
    // Simple identifier with no user value left as-is
    hasUnresolved = true;
    return match;
  });

  return { rendered: s, hasUnresolved };
}

// ---------------------------------------------------------------------------
// Git URL helpers
// ---------------------------------------------------------------------------

export function githubBlobToRaw(url: string): string | null {
  const m = url.match(
    /^https?:\/\/github\.com\/([^/]+)\/([^/]+)\/blob\/([^/]+)\/(.+)$/,
  );
  if (!m) return null;
  const [, owner, repo, branch, path] = m;
  return `https://raw.githubusercontent.com/${owner}/${repo}/${branch}/${path}`;
}

export function gitlabBlobToRaw(url: string): string | null {
  const m = url.match(
    /^(https?:\/\/gitlab\.com\/[^?#]+)\/-\/blob\/([^/]+)\/(.+)$/,
  );
  if (!m) return null;
  const [, base, branch, path] = m;
  return `${base}/-/raw/${branch}/${path}`;
}

export function resolveRawGitUrl(url: string): string {
  return githubBlobToRaw(url) ?? gitlabBlobToRaw(url) ?? url;
}
