// Accepts any i18n t() function — return type is widened to unknown since
// i18next TFunction can return TFunctionDetailedResult in some configurations.
type TFunction = (key: string, opts?: any) => unknown;

export function relativeDate(iso: string | undefined, t: TFunction): string {
  if (!iso) return '';
  const diff = Date.now() - new Date(iso).getTime();
  const m = Math.floor(diff / 60000);
  if (m < 1) return String(t('relative_date.just_now'));
  if (m < 60) return String(t('relative_date.minutes_ago', { count: m }));
  const h = Math.floor(m / 60);
  if (h < 24) return String(t('relative_date.hours_ago', { count: h }));
  return String(t('relative_date.days_ago', { count: Math.floor(h / 24) }));
}
