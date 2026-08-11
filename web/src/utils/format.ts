const UNITS = ['B', 'KB', 'MB', 'GB', 'TB'] as const;

export function formatBytes(value: number | null | undefined): string {
  if (value == null) return '—';
  if (value === 0) return '0 B';
  let index = 0;
  let scaled = value;
  while (scaled >= 1024 && index < UNITS.length - 1) {
    scaled /= 1024;
    index += 1;
  }
  return `${scaled >= 100 || index === 0 ? scaled.toFixed(0) : scaled.toFixed(1)} ${UNITS[index]}`;
}

export const formatSpeed = (value: number) => `${formatBytes(value)}/s`;

export const percent = (done: number, total: number) =>
  total > 0 ? Math.min(100, Math.round((done / total) * 100)) : 0;

/** 字节数转回输入框里好读的写法（5242880 → "5M"），整除不了就用原始字节。 */
export function sizeInput(value: number): string {
  if (!value) return '';
  for (const [size, suffix] of [
    [1024 ** 3, 'G'],
    [1024 ** 2, 'M'],
    [1024, 'K'],
  ] as const) {
    if (value % size === 0) return `${value / size}${suffix}`;
  }
  return String(value);
}

/** 解析 "512K" / "5M" / "1G" / 裸字节。非法输入抛错，交给表单校验提示。 */
export function parseSize(value: string): number {
  const match = value.trim().match(/^(\d+(?:\.\d+)?)\s*([KMGT]?B?)?$/i);
  if (!match) throw new Error('请输入 512K、5M、1G 或字节数');
  const unit = (match[2] ?? '').toUpperCase().replace('B', '');
  const power = ['', 'K', 'M', 'G', 'T'].indexOf(unit);
  if (power < 0) throw new Error('大小单位无效');
  return Math.floor(Number(match[1]) * 1024 ** power);
}

const RELATIVE_STEPS = [
  [60, '秒'],
  [60, '分钟'],
  [24, '小时'],
  [Infinity, '天'],
] as const;

/**
 * Unix 秒转「多久以前」。任务列表按编辑时间逆序，卡片上标出这个时间，
 * 排序才是用户能读懂的，而不是一串莫名其妙的顺序。
 *
 * 超过一周就直接显示日期——「23 天前」没有「2026-07-19」有用。
 */
export function timeAgo(unixSeconds: number, now = Date.now()): string {
  if (!unixSeconds) return '—';
  const diff = Math.max(0, Math.floor(now / 1000 - unixSeconds));
  if (diff < 45) return '刚刚';
  if (diff >= 7 * 86_400) {
    return new Date(unixSeconds * 1000).toLocaleDateString('zh-CN');
  }
  let value = diff;
  for (const [factor, unit] of RELATIVE_STEPS) {
    if (value < factor) return `${Math.floor(value)} ${unit}前`;
    value /= factor;
  }
  return `${Math.floor(value)} 天前`;
}
