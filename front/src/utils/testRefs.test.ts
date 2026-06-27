import { describe, it, expect } from 'vitest';
import { buildTestUidIndex, linkifyTestRefs, isTestRefHref, testRankFromHref } from './testRefs';

describe('testRefs', () => {
  it('maps test_uid to 1-based screen rank in order', () => {
    const idx = buildTestUidIndex([{ test_uid: 'a3f9' }, { test_uid: '88eb' }]);
    expect(idx).toEqual({ a3f9: 1, '88eb': 2 });
  });

  it('skips tests without a uid', () => {
    const idx = buildTestUidIndex([{ test_uid: 'a3f9' }, {}, { test_uid: '88eb' }]);
    expect(idx).toEqual({ a3f9: 1, '88eb': 3 });
  });

  it('replaces a [[test:UID]] marker with a markdown link to the screen rank', () => {
    const out = linkifyTestRefs('Regarde [[test:88eb]] qui échoue.', { a3f9: 1, '88eb': 2 });
    expect(out).toBe('Regarde [test 2](#mocksql-test-2) qui échoue.');
  });

  it('renders an unknown uid as plain "test", never the raw uid', () => {
    const out = linkifyTestRefs('Le [[test:dead]] a disparu.', { a3f9: 1 });
    expect(out).toBe('Le test a disparu.');
    expect(out).not.toContain('dead');
  });

  it('handles multiple markers in one message', () => {
    const out = linkifyTestRefs('[[test:a3f9]] et [[test:88eb]]', { a3f9: 1, '88eb': 2 });
    expect(out).toBe('[test 1](#mocksql-test-1) et [test 2](#mocksql-test-2)');
  });

  it('recognises and decodes test-ref hrefs', () => {
    expect(isTestRefHref('#mocksql-test-2')).toBe(true);
    expect(isTestRefHref('https://example.com')).toBe(false);
    expect(testRankFromHref('#mocksql-test-2')).toBe(2);
    expect(testRankFromHref('#other')).toBeNull();
  });
});
