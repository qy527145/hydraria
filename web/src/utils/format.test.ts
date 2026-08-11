import { describe, expect, it } from 'vitest';
import { formatBytes, formatSpeed, parseSize, percent, sizeInput } from './format';

describe('parseSize', () => {
  it('接受带单位和不带单位的写法', () => {
    expect(parseSize('512K')).toBe(512 * 1024);
    expect(parseSize('5M')).toBe(5 * 1024 * 1024);
    expect(parseSize('1G')).toBe(1024 ** 3);
    expect(parseSize('2MB')).toBe(2 * 1024 * 1024);
    expect(parseSize(' 4m ')).toBe(4 * 1024 * 1024);
    expect(parseSize('1048576')).toBe(1048576);
  });

  it('小数按字节向下取整', () => {
    expect(parseSize('1.5M')).toBe(1572864);
  });

  it('非法输入抛错，而不是悄悄变成 0', () => {
    // 静默失败会把用户的分片大小改成"自动"，属于配置被偷偷改动。
    expect(() => parseSize('abc')).toThrow();
    expect(() => parseSize('5X')).toThrow();
    expect(() => parseSize('')).toThrow();
  });
});

describe('sizeInput / parseSize 往返', () => {
  it('整除的大小回到可读写法后仍解析成同一个值', () => {
    for (const bytes of [512 * 1024, 5 * 1024 * 1024, 1024 ** 3, 2 * 1024 ** 3]) {
      expect(parseSize(sizeInput(bytes))).toBe(bytes);
    }
  });

  it('除不尽时退回原始字节数', () => {
    expect(sizeInput(1234567)).toBe('1234567');
    expect(parseSize(sizeInput(1234567))).toBe(1234567);
  });

  it('0 表示"未设置"，对应空输入框', () => {
    expect(sizeInput(0)).toBe('');
  });
});

describe('formatBytes', () => {
  it('分档并保留一位小数', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(999)).toBe('999 B');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(formatBytes(1024 ** 3)).toBe('1.0 GB');
  });

  it('空值显示占位符而不是 NaN', () => {
    expect(formatBytes(null)).toBe('—');
    expect(formatBytes(undefined)).toBe('—');
  });

  it('速度带 /s 后缀', () => {
    expect(formatSpeed(1024 * 1024)).toBe('1.0 MB/s');
  });
});

describe('percent', () => {
  it('总量未知时返回 0，且永远不超过 100', () => {
    expect(percent(10, 0)).toBe(0);
    expect(percent(5, 10)).toBe(50);
    expect(percent(11, 10)).toBe(100);
  });
});
