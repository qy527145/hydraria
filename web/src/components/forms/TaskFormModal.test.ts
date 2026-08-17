import { describe, expect, it } from 'vitest';
import { deriveThreads, volumeHosts } from './TaskFormModal';

describe('volumeHosts', () => {
  it('把卷 URL 里的域名去重后按出现顺序给出来', () => {
    expect(
      volumeHosts([
        ['https://cdn-a.example.com/movie.part1', 'https://cdn-b.example.com/movie.part1'],
        ['https://cdn-a.example.com/movie.part2'],
      ]),
    ).toEqual(['cdn-a.example.com', 'cdn-b.example.com']);
  });

  it('半截 URL 直接跳过 —— 编辑中的输入框随时是不完整的', () => {
    expect(volumeHosts([['https://', 'not a url', 'https://ok.example.com/a']])).toEqual([
      'ok.example.com',
    ]);
    expect(volumeHosts([[]])).toEqual([]);
  });

  it('IPv6 去掉方括号：映射表按裸地址查，带括号会永远命不中', () => {
    expect(volumeHosts([['http://[2001:db8::1]:8080/a.mp4']])).toEqual(['2001:db8::1']);
  });

  it('裸 IP 也是合法的原地址', () => {
    expect(volumeHosts([['http://10.0.0.1:8000/a', 'http://10.0.0.1:9000/b']])).toEqual(['10.0.0.1']);
  });
});

describe('deriveThreads', () => {
  it('总线程数 = 单卷并发 × 卷数，封顶 128', () => {
    expect(deriveThreads(4, 2)).toBe(8);
    expect(deriveThreads(4, 1)).toBe(4);
    expect(deriveThreads(8, 100)).toBe(128);
    // 卷数还是 0（表单刚打开）时也要给出一个能用的数字。
    expect(deriveThreads(4, 0)).toBe(4);
  });
});
