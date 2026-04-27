import { relativeDate } from './dates';

const t = (key: string, opts?: { count?: number }): string => {
  const map: Record<string, string> = {
    'relative_date.just_now': 'just now',
    'relative_date.minutes_ago': `${opts?.count ?? '?'} minutes ago`,
    'relative_date.hours_ago': `${opts?.count ?? '?'} hours ago`,
    'relative_date.days_ago': `${opts?.count ?? '?'} days ago`,
  };
  return map[key] ?? key;
};

describe('relativeDate', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('returns empty string for undefined input', () => {
    expect(relativeDate(undefined, t)).toBe('');
  });

  it('returns empty string for empty string input', () => {
    expect(relativeDate('', t)).toBe('');
  });

  it('returns just_now for a timestamp less than 1 minute ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const thirtySecondsAgo = new Date(now.getTime() - 30 * 1000).toISOString();
    expect(relativeDate(thirtySecondsAgo, t)).toBe('just now');
  });

  it('returns minutes_ago for a timestamp 5 minutes ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const fiveMinutesAgo = new Date(now.getTime() - 5 * 60 * 1000).toISOString();
    expect(relativeDate(fiveMinutesAgo, t)).toBe('5 minutes ago');
  });

  it('returns minutes_ago for exactly 59 minutes ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const ago = new Date(now.getTime() - 59 * 60 * 1000).toISOString();
    expect(relativeDate(ago, t)).toBe('59 minutes ago');
  });

  it('returns hours_ago for a timestamp 3 hours ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const threeHoursAgo = new Date(now.getTime() - 3 * 60 * 60 * 1000).toISOString();
    expect(relativeDate(threeHoursAgo, t)).toBe('3 hours ago');
  });

  it('returns hours_ago for exactly 23 hours ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const ago = new Date(now.getTime() - 23 * 60 * 60 * 1000).toISOString();
    expect(relativeDate(ago, t)).toBe('23 hours ago');
  });

  it('returns days_ago for a timestamp 2 days ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const twoDaysAgo = new Date(now.getTime() - 2 * 24 * 60 * 60 * 1000).toISOString();
    expect(relativeDate(twoDaysAgo, t)).toBe('2 days ago');
  });

  it('returns days_ago for exactly 1 day ago', () => {
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const ago = new Date(now.getTime() - 24 * 60 * 60 * 1000).toISOString();
    expect(relativeDate(ago, t)).toBe('1 days ago');
  });

  it('passes count to the translation function', () => {
    const calls: Array<[string, any]> = [];
    const capturingT = (key: string, opts?: any) => {
      calls.push([key, opts]);
      return '';
    };
    const now = new Date('2026-04-27T12:00:00.000Z');
    jest.setSystemTime(now);
    const ago = new Date(now.getTime() - 45 * 60 * 1000).toISOString();
    relativeDate(ago, capturingT);
    expect(calls[0][0]).toBe('relative_date.minutes_ago');
    expect(calls[0][1]).toEqual({ count: 45 });
  });
});
