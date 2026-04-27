import { formatMessage, getLastMessage } from './messages';
import { MsgType } from './types';

// ---------------------------------------------------------------------------
// formatMessage
// ---------------------------------------------------------------------------

describe('formatMessage', () => {
  describe('message type resolution', () => {
    it('maps human → user', () => {
      const msg = formatMessage({ type: 'human', id: '1', content: 'hello', additional_kwargs: {} });
      expect(msg.type).toBe('user');
    });

    it('maps ai → bot', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: 'response', additional_kwargs: {} });
      expect(msg.type).toBe('bot');
    });

    it('maps contentType query → user', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: 'q', additional_kwargs: { type: 'query' } });
      expect(msg.type).toBe('user');
    });

    it('maps contentType examples_update → user', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: '{}', additional_kwargs: { type: 'examples_update' } });
      expect(msg.type).toBe('user');
    });

    it('maps contentType sql_update → user', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: '', additional_kwargs: { type: 'sql_update' } });
      expect(msg.type).toBe('user');
    });

    it('maps contentType user_examples → user', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: '[]', additional_kwargs: { type: 'user_examples' } });
      expect(msg.type).toBe('user');
    });

    it('maps contentType results → bot', () => {
      const msg = formatMessage({ type: 'ai', id: '1', content: '[]', additional_kwargs: { type: 'results' } });
      expect(msg.type).toBe('bot');
    });
  });

  describe('base fields', () => {
    it('sets id, parent, request, contentType', () => {
      const msg = formatMessage({
        type: 'ai',
        id: 'msg-42',
        content: 'hello',
        additional_kwargs: { type: 'results', parent: 'parent-1', request_id: 'req-99' },
      });
      expect(msg.id).toBe('msg-42');
      expect(msg.parent).toBe('parent-1');
      expect(msg.request).toBe('req-99');
      expect(msg.contentType).toBe('results');
    });

    it('handles missing id gracefully', () => {
      const msg = formatMessage({ type: 'human', content: 'hi', additional_kwargs: {} });
      expect(msg.id).toBe('');
    });

    it('sets contentType to null when absent', () => {
      const msg = formatMessage({ type: 'human', content: 'hi', additional_kwargs: {} });
      expect(msg.contentType).toBeNull();
    });

    it('initialises children as empty array', () => {
      const msg = formatMessage({ type: 'human', content: 'hi', additional_kwargs: {} });
      expect(msg.children).toEqual([]);
    });
  });

  describe('contentType: examples', () => {
    it('parses tables from content (array)', () => {
      const tables = [{ table_a: [{ id: 1 }] }];
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: JSON.stringify(tables),
        additional_kwargs: { type: 'examples' },
      });
      expect(msg.contents.tables).toEqual(tables);
    });

    it('wraps single object tables in array', () => {
      const table = { table_a: [{ id: 1 }] };
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: JSON.stringify(table),
        additional_kwargs: { type: 'examples' },
      });
      expect(Array.isArray(msg.contents.tables)).toBe(true);
      expect((msg.contents.tables as any[])[0]).toEqual(table);
    });

    it('sets sql from additional_kwargs', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '[]',
        additional_kwargs: { type: 'examples', sql: 'SELECT 1' },
      });
      expect(msg.contents.sql).toBe('SELECT 1');
    });

    it('sets optimizedSql from additional_kwargs', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '[]',
        additional_kwargs: { type: 'examples', optimized_sql: 'SELECT a FROM t' },
      });
      expect(msg.contents.optimizedSql).toBe('SELECT a FROM t');
    });
  });

  describe('contentType: results', () => {
    it('parses res from content', () => {
      const rows = [{ id: 1, value: 'x' }];
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: JSON.stringify(rows),
        additional_kwargs: { type: 'results' },
      });
      expect(msg.contents.res).toEqual(rows);
    });

    it('sets sql and optimizedSql', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '[]',
        additional_kwargs: { type: 'results', sql: 'SELECT 1', optimized_sql: 'SELECT 1' },
      });
      expect(msg.contents.sql).toBe('SELECT 1');
      expect(msg.contents.optimizedSql).toBe('SELECT 1');
    });
  });

  describe('contentType: error', () => {
    it('sets contents.error to raw content', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: 'Syntax error near FROM',
        additional_kwargs: { type: 'error' },
      });
      expect(msg.contents.error).toBe('Syntax error near FROM');
    });

    it('maps "unsolvable" to a human-readable message', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: 'unsolvable',
        additional_kwargs: { type: 'error' },
      });
      expect(msg.contents.error).toContain('informations');
      expect(msg.contents.error).not.toBe('unsolvable');
    });
  });

  describe('contentType: sql_update', () => {
    it('sets fixed text label', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '',
        additional_kwargs: { type: 'sql_update' },
      });
      expect(msg.contents.text).toBe('Modification de la requête');
    });
  });

  describe('contentType: user_examples', () => {
    it('sets fixed text label and parses tables', () => {
      const tables = [{ t: [{ x: 1 }] }];
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: JSON.stringify(tables),
        additional_kwargs: { type: 'user_examples' },
      });
      expect(msg.contents.text).toBe('Modification des exemples');
      expect(msg.contents.tables).toEqual(tables);
    });
  });

  describe('contentType: profile_query', () => {
    it('parses profileRequest fields from content', () => {
      const payload = {
        message: 'Profil manquant',
        profile_query: 'SELECT * FROM t',
        missing_columns: [{ table: 't', used_columns: ['id'] }],
      };
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: JSON.stringify(payload),
        additional_kwargs: { type: MsgType.PROFILE_QUERY },
      });
      expect(msg.contents.profileRequest?.message).toBe('Profil manquant');
      expect(msg.contents.profileRequest?.profile_query).toBe('SELECT * FROM t');
      expect(msg.contents.profileRequest?.missing_columns).toEqual(payload.missing_columns);
    });
  });

  describe('contentType: evaluation', () => {
    it('sets text from content', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '**Bon** — Couvre le cas nominal.',
        additional_kwargs: { type: 'evaluation' },
      });
      expect(msg.contents.text).toBe('**Bon** — Couvre le cas nominal.');
    });

    it('sets testIndex from additional_kwargs', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: '**Bon**',
        additional_kwargs: { type: 'evaluation', test_index: 2 },
      });
      expect(msg.testIndex).toBe(2);
    });
  });

  describe('default (unknown contentType)', () => {
    it('sets contents.text to raw content', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: 'Some reasoning text',
        additional_kwargs: { type: 'reasoning' },
      });
      expect(msg.contents.text).toBe('Some reasoning text');
    });
  });

  describe('nested additional_kwargs', () => {
    it('reads type from nested additional_kwargs.additional_kwargs', () => {
      const msg = formatMessage({
        type: 'ai', id: '1',
        content: 'Modification de la requête',
        additional_kwargs: { additional_kwargs: { type: 'sql_update' } },
      });
      expect(msg.contentType).toBe('sql_update');
    });
  });
});

// ---------------------------------------------------------------------------
// getLastMessage
// ---------------------------------------------------------------------------

describe('getLastMessage', () => {
  it('returns null for null input', () => {
    expect(getLastMessage(null, {})).toBeNull();
  });

  it('returns null for empty array', () => {
    expect(getLastMessage([], {})).toBeNull();
  });

  it('returns the last simple message', () => {
    const msgs = [
      { id: 'a', type: 'user', contents: {} },
      { id: 'b', type: 'bot', contents: {} },
    ];
    expect(getLastMessage(msgs, {})).toEqual(msgs[1]);
  });

  it('returns first message when only one exists', () => {
    const msgs = [{ id: 'a', type: 'user', contents: {} }];
    expect(getLastMessage(msgs, {})).toEqual(msgs[0]);
  });

  describe('group handling', () => {
    const branchA = [{ id: 'c', type: 'bot', contents: { text: 'branch A' } }];
    const branchB = [{ id: 'd', type: 'bot', contents: { text: 'branch B' } }];
    const group = {
      type: 'group',
      parentId: 'parent-1',
      branches: [branchA, branchB],
    };

    it('follows the selected branch index', () => {
      const result = getLastMessage([group], { 'parent-1': 1 });
      expect(result).toEqual(branchB[0]);
    });

    it('falls back to the last branch when no selection', () => {
      const result = getLastMessage([group], {});
      expect(result).toEqual(branchB[0]);
    });

    it('clamps out-of-bounds selected index to last branch', () => {
      const result = getLastMessage([group], { 'parent-1': 99 });
      expect(result).toEqual(branchB[0]);
    });

    it('clamps negative index to first branch', () => {
      const result = getLastMessage([group], { 'parent-1': -5 });
      expect(result).toEqual(branchA[0]);
    });
  });
});
