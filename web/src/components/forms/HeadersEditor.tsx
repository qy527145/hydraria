import { Alert, Input, Segmented, Space, Tooltip } from 'antd';
import { useMemo, useState } from 'react';

/**
 * 自定义请求头编辑器：常用头做成表单，其余走 raw JSON，两种视图随时切换。
 *
 * **JSON 文本是唯一的真相源**，表单只是它的一个视图。两份状态各存一份的话，
 * 「在表单里改了 Cookie、切到 raw、再切回来」这种再普通不过的操作就会开始丢
 * 改动，而且丢得毫无规律。所以表单字段每次编辑都就地重写那段 JSON。
 *
 * 表单模式只认下面三个头 —— 它们占了实际用量的绝大多数，而且都是「填错就
 * 403」的那类。其余的头原样保留（不会因为切到表单模式就被抹掉），只在提示行
 * 里报个数，想编辑就切 raw。
 */
const COMMON: { key: string; label: string; placeholder: string; tip: string }[] = [
  {
    key: 'Referer',
    label: 'Referer',
    placeholder: 'https://example.com/play/123',
    tip: '防盗链最常查的头。通常填播放页地址，或源站所在域名的根路径。',
  },
  {
    key: 'Cookie',
    label: 'Cookie',
    placeholder: 'SESSDATA=…; bili_jct=…',
    tip: '需要登录态的源站填这个。整段从浏览器开发者工具的请求里复制即可。',
  },
  {
    key: 'User-Agent',
    label: 'User-Agent',
    placeholder: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) …',
    tip: '有些源站只放行浏览器 UA。留空则用 Hydraria 自己的默认 UA。',
  },
];

/** 解析成对象；不是合法的 JSON 对象就返回 null（表单模式无法表达它）。 */
function parseHeaders(text: string): Record<string, string> | null {
  const trimmed = text.trim();
  if (!trimmed) return {};
  try {
    const parsed: unknown = JSON.parse(trimmed);
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return null;
    const out: Record<string, string> = {};
    for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
      if (typeof value !== 'string') return null;
      out[key] = value;
    }
    return out;
  } catch {
    return null;
  }
}

/** 头名大小写不敏感：用户可能写 `referer`，不能因此在表单里多出一行。 */
function findKey(headers: Record<string, string>, name: string): string | undefined {
  const lower = name.toLowerCase();
  return Object.keys(headers).find(key => key.toLowerCase() === lower);
}

function serialize(headers: Record<string, string>): string {
  return Object.keys(headers).length ? JSON.stringify(headers, null, 2) : '';
}

interface Props {
  /** JSON 文本。Ant Design Form 通过 value/onChange 注入。 */
  value?: string;
  onChange?: (value: string) => void;
}

export default function HeadersEditor({ value = '', onChange }: Props) {
  const parsed = useMemo(() => parseHeaders(value), [value]);
  // 打开时如果已有内容且能被表单表达，就停在表单视图；raw 里写了表单表达不了的
  // 东西（非法 JSON、嵌套值）时只能留在 raw。
  const [mode, setMode] = useState<'form' | 'raw'>(parsed ? 'form' : 'raw');
  const effective = parsed && mode === 'form' ? 'form' : parsed ? mode : 'raw';

  const set = (name: string, next: string) => {
    const headers = { ...(parsed ?? {}) };
    const existing = findKey(headers, name);
    if (next.trim()) {
      headers[existing ?? name] = next;
    } else if (existing) {
      delete headers[existing];
    }
    onChange?.(serialize(headers));
  };

  const extras = parsed
    ? Object.keys(parsed).filter(key => !COMMON.some(c => c.key.toLowerCase() === key.toLowerCase()))
    : [];

  return (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      <Segmented
        size="small"
        value={effective}
        onChange={next => setMode(next as 'form' | 'raw')}
        options={[
          { label: '常用头', value: 'form', disabled: !parsed },
          { label: 'raw JSON', value: 'raw' },
        ]}
      />

      {effective === 'form' ? (
        <>
          {COMMON.map(item => (
            <Tooltip key={item.key} title={item.tip} placement="topLeft">
              <Input
                addonBefore={item.label}
                placeholder={item.placeholder}
                value={parsed ? (parsed[findKey(parsed, item.key) ?? item.key] ?? '') : ''}
                onChange={event => set(item.key, event.target.value)}
              />
            </Tooltip>
          ))}
          {extras.length > 0 && (
            <Alert
              type="info"
              showIcon
              message={`另有 ${extras.length} 个自定义头会原样保留：${extras.join('、')}`}
              description="切到 raw JSON 可以编辑它们。"
            />
          )}
        </>
      ) : (
        <>
          <Input.TextArea
            rows={5}
            value={value}
            placeholder={'{\n  "Referer": "https://example.com/",\n  "Cookie": "…"\n}'}
            onChange={event => onChange?.(event.target.value)}
          />
          {!parsed && value.trim() && (
            <Alert
              type="warning"
              showIcon
              message="当前内容不是「字符串到字符串」的 JSON 对象，无法切到常用头视图"
            />
          )}
        </>
      )}
    </Space>
  );
}
