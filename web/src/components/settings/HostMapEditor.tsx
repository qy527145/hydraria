import { DeleteOutlined, PlusOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { Alert, Button, Input, Space, Switch, Tooltip, Typography } from 'antd';
import { useState } from 'react';
import { api, type HostMapping, type HostResolution } from '../../api/client';

/**
 * 域名映射编辑器 —— 等价于 `curl --resolve`，或者说一份只对 Hydraria 生效的
 * hosts 文件。
 *
 * 存在的理由：URL 里的域名公共 DNS 解析不出来（私有 CDN、内网回源域名），而
 * 域名本身又改不得 —— 一改签名参数就对不上。所以这里改的只有「TCP 连到哪儿」，
 * URL、Host 头、TLS SNI 全部保持原样，源站看到的仍然是签名时那个域名。
 *
 * 同一个组件既用在全局设置里，也用在任务表单里；两处规则取并集，`from` 撞车时
 * 以任务级为准。
 *
 * 每行的 ⚡ 会直接问后端「这个 host 现在会被连到哪儿」。加这个按钮是因为：配完
 * 映射之后唯一能确认它生效的办法本来是翻日志、或者干脆播一次看会不会 502。
 */
interface Props {
  value?: HostMapping[];
  onChange?: (value: HostMapping[]) => void;
  /** 传了就按这个任务的生效表测（含它自己的任务级规则）。 */
  taskId?: string;
}

const EMPTY: HostMapping = { from: '', to: '', enabled: true };

export default function HostMapEditor({ value, onChange, taskId }: Props) {
  const rows = value ?? [];
  const [probing, setProbing] = useState<number | null>(null);
  const [results, setResults] = useState<Record<number, HostResolution | string>>({});

  const patch = (index: number, next: Partial<HostMapping>) =>
    onChange?.(rows.map((row, i) => (i === index ? { ...row, ...next } : row)));

  const test = async (index: number, host: string) => {
    setProbing(index);
    try {
      const resolution = await api.resolveHost(host, taskId);
      setResults(prev => ({ ...prev, [index]: resolution }));
    } catch (error) {
      setResults(prev => ({
        ...prev,
        [index]: error instanceof Error ? error.message : String(error),
      }));
    } finally {
      setProbing(null);
    }
  };

  return (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      {rows.map((row, index) => (
        <div key={index}>
          <div className="hostmap-row">
            <Input
              placeholder="原域名 / IP，如 cdn.example.com"
              value={row.from}
              onChange={event => patch(index, { from: event.target.value })}
            />
            <span className="hostmap-arrow">→</span>
            <Input
              placeholder="目标 IP / 域名，可带 :端口"
              value={row.to}
              onChange={event => patch(index, { to: event.target.value })}
            />
            <Tooltip title="测试：这个域名现在会被连到哪个地址（用的是已保存的规则）">
              <Button
                type="text"
                icon={<ThunderboltOutlined />}
                loading={probing === index}
                disabled={!row.from.trim()}
                onClick={() => void test(index, row.from.trim())}
              />
            </Tooltip>
            <Tooltip title={row.enabled ? '已启用，点击停用' : '已停用，规则保留但不生效'}>
              <Switch
                size="small"
                checked={row.enabled}
                onChange={checked => patch(index, { enabled: checked })}
              />
            </Tooltip>
            <Button
              danger
              type="text"
              icon={<DeleteOutlined />}
              onClick={() => {
                onChange?.(rows.filter((_, i) => i !== index));
                setResults(prev => ({ ...prev, [index]: '' }));
              }}
            />
          </div>
          <ResultLine result={results[index]} />
        </div>
      ))}
      <Button
        block
        type="dashed"
        icon={<PlusOutlined />}
        onClick={() => onChange?.([...rows, { ...EMPTY }])}
      >
        添加映射
      </Button>
      {rows.length > 0 && (
        <Alert
          type="info"
          showIcon
          message="只改连接目标，不改请求内容"
          description={
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              URL、Host 头、TLS 证书校验用的都还是原域名，签名与防盗链照常有效。
              原地址支持 <code>*.example.com</code> 形式的通配（精确匹配优先）。
              目标端口只在原 URL 没有显式写端口时才生效。命中映射的请求会自动绕开
              系统代理 —— 否则域名由代理去解析，映射不会生效。
            </Typography.Text>
          }
        />
      )}
    </Space>
  );
}

/** ⚡ 的结果。测的是**已保存**的规则，所以刚改完没保存时它报的还是旧值。 */
function ResultLine({ result }: { result?: HostResolution | string }) {
  if (!result) return null;
  if (typeof result === 'string') {
    return result ? <Alert type="error" showIcon banner message={result} /> : null;
  }
  const addrs = result.addresses.join(', ');
  if (result.error) {
    return (
      <Alert
        type="error"
        showIcon
        banner
        message={
          result.mapped_to
            ? `映射命中 ${result.mapped_to}，但目标解析失败：${result.error}`
            : `解析失败：${result.error}`
        }
      />
    );
  }
  return (
    <Alert
      type={result.mapped_to ? 'success' : 'warning'}
      showIcon
      banner
      message={
        result.mapped_to
          ? `已映射到 ${result.mapped_to}${addrs ? ` → ${addrs}` : ''}`
          : `没有规则命中，走正常 DNS${addrs ? `：${addrs}` : ''}（保存后再测）`
      }
    />
  );
}
