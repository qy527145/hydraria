import { DeleteOutlined, GlobalOutlined, PlusOutlined, ThunderboltOutlined } from '@ant-design/icons';
import {
  Alert,
  AutoComplete,
  Button,
  Input,
  Popconfirm,
  Space,
  Switch,
  Tag,
  Tooltip,
  Typography,
  message,
} from 'antd';
import { useState } from 'react';
import { api, type HostMapping, type HostMapScope, type HostResolution } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';

/**
 * 域名映射编辑器 —— 等价于 `curl --resolve`，或者说一份只对 Hydraria 生效的
 * hosts 文件。
 *
 * 存在的理由：URL 里的域名公共 DNS 解析不出来（私有 CDN、内网回源域名），而
 * 域名本身又改不得 —— 一改签名参数就对不上。所以这里改的只有「TCP 连到哪儿」，
 * URL、Host 头、TLS SNI 全部保持原样，源站看到的仍然是签名时那个域名。
 *
 * 同一个组件既用在全局设置里（`scope="global"`），也用在任务表单里
 * （`scope="task"`）；两处规则取并集，`from` 撞车时以任务级为准。
 *
 * 每行的 ⚡ 会直接问后端「这个 host 现在会被连到哪儿」，测的是**屏幕上这份**
 * 规则而不是已保存的那份 —— 按下测试的时机，恰恰是还没保存的时候。
 */
interface Props {
  value?: HostMapping[];
  onChange?: (value: HostMapping[]) => void;
  /** 全局设置 vs 任务表单。决定草稿规则替换哪一层，也决定下面几块要不要出现。 */
  scope?: HostMapScope;
  /** 传了就按这个任务的生效表测（含它自己的任务级规则）。 */
  taskId?: string;
  /**
   * 当前任务卷 URL 里出现过的 host。映射的原地址基本只可能是它们中的一个，
   * 让人回到上面的 textarea 里把域名抄一遍纯属白费手。
   */
  hosts?: string[];
}

const EMPTY: HostMapping = { from: '', to: '', enabled: true };

/** 一次测试的结果，连同「测的是哪一行的哪个值」。 */
interface Probed {
  from: string;
  to: string;
  result: HostResolution | string;
}

const key = (host: string) => host.trim().toLowerCase().replace(/\.$/, '');

export default function HostMapEditor({ value, onChange, scope = 'global', taskId, hosts = [] }: Props) {
  const rows = value ?? [];
  const [probing, setProbing] = useState<number | null>(null);
  const [results, setResults] = useState<Record<number, Probed>>({});
  const [promoting, setPromoting] = useState(false);
  const globalRules = useDashboard(state => state.global?.settings.host_mappings) ?? [];
  const refresh = useDashboard(state => state.refresh);

  const patch = (index: number, next: Partial<HostMapping>) =>
    onChange?.(rows.map((row, i) => (i === index ? { ...row, ...next } : row)));

  // 已经写进规则的 host 不再进下拉：候选的意义是「这个域名你还没配」。
  const covered = new Set(rows.map(row => key(row.from)));
  const options = hosts.filter(host => !covered.has(key(host)));
  // 小标签比下拉更主动，所以门槛也更高：已经被一条**全局**规则管着的域名不该
  // 再被推荐 —— 点下去只会多出一条内容相同的任务级规则。想覆盖全局的人仍然
  // 可以从下拉里选，那是个明确的动作。
  const globalKeys = new Set(
    globalRules.filter(rule => rule.enabled).map(rule => key(rule.from)),
  );
  const suggestions =
    scope === 'task' ? options.filter(host => !globalKeys.has(key(host))) : options;

  const addRow = (from = '') => onChange?.([...rows, { ...EMPTY, from }]);

  const test = async (index: number, row: HostMapping) => {
    setProbing(index);
    const probe = (result: HostResolution | string) =>
      setResults(prev => ({ ...prev, [index]: { from: row.from, to: row.to, result } }));
    try {
      // 关键：把编辑器里当前这份规则一起发过去。不发的话后端只认已保存的那份，
      // 于是「改完 target 再测」报的还是上一次的结果。
      probe(await api.resolveHost(row.from.trim(), { scope, mappings: rows, taskId }));
    } catch (error) {
      probe(error instanceof Error ? error.message : String(error));
    } finally {
      setProbing(null);
    }
  };

  /**
   * 把这几条规则搬到全局设置里去。
   *
   * 同一个域名映射通常对所有任务都成立（内网回源域名不会只对某一个文件成立），
   * 而在任务里配的那份只跟着这一个任务走 —— 下次新建任务还得再敲一遍。
   *
   * 全局那半边是立刻落库的；任务里这几行同时清空，但要等保存任务才生效。
   * 中途放弃编辑也不会有问题：两边规则内容相同，任务级只是盖了一层一模一样的。
   */
  const promote = async () => {
    const usable = rows.filter(row => row.from.trim() && row.to.trim());
    if (!usable.length) return;
    setPromoting(true);
    try {
      // 与后端 `merged_rules` 同一条规则：同名以任务级为准，其余保留。
      const kept = globalRules.filter(g => !usable.some(row => key(row.from) === key(g.from)));
      // 不走 store 的 mutate：它把错误吞掉只弹一条 message，而这里失败与否决定
      // 了要不要把这几行从任务里删掉 —— 存都没存上还删，等于把规则弄丢了。
      await api.saveSettings({ host_mappings: [...kept, ...usable] });
      onChange?.(rows.filter(row => !usable.includes(row)));
      setResults({});
      message.success(`已设为全局映射（${usable.length} 条），对所有任务生效`);
    } catch (error) {
      message.error(error instanceof Error ? error.message : String(error));
    } finally {
      setPromoting(false);
      void refresh();
    }
  };

  return (
    <Space direction="vertical" size={8} style={{ width: '100%' }}>
      {rows.map((row, index) => {
        const probed = results[index];
        // 行里的值一改，上一次的结果就不再是这一行的答案了 —— 与其留着让人
        // 误以为「改了没生效」，不如让它消失，等下一次测试。
        const stale = !probed || probed.from !== row.from || probed.to !== row.to;
        return (
          <div key={index}>
            <div className="hostmap-row">
              <AutoComplete
                style={{ flex: 1, minWidth: 0 }}
                value={row.from}
                options={options.map(host => ({ value: host }))}
                filterOption={(input, option) =>
                  (option?.value ?? '').toLowerCase().includes(input.trim().toLowerCase())
                }
                onChange={next => patch(index, { from: next })}
              >
                <Input placeholder="原域名 / IP，如 cdn.example.com" />
              </AutoComplete>
              <span className="hostmap-arrow">→</span>
              <Input
                placeholder="目标 IP / 域名，可带 :端口"
                value={row.to}
                onChange={event => patch(index, { to: event.target.value })}
              />
              <Tooltip title="测试：按屏幕上这份规则算，这个域名现在会被连到哪个地址（不用先保存）">
                <Button
                  type="text"
                  icon={<ThunderboltOutlined />}
                  loading={probing === index}
                  disabled={!row.from.trim()}
                  onClick={() => void test(index, row)}
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
                  setResults({});
                }}
              />
            </div>
            {!stale && <ResultLine result={probed.result} />}
          </div>
        );
      })}

      {suggestions.length > 0 && (
        <div className="hostmap-suggest">
          <Typography.Text type="secondary">卷 URL 里的域名：</Typography.Text>
          {suggestions.map(host => (
            <Tag key={host} className="hostmap-chip" onClick={() => addRow(host)}>
              + {host}
            </Tag>
          ))}
        </div>
      )}

      <Space.Compact block>
        <Button block type="dashed" icon={<PlusOutlined />} onClick={() => addRow(suggestions[0] ?? '')}>
          添加映射
        </Button>
        {scope === 'task' && rows.some(row => row.from.trim() && row.to.trim()) && (
          <Popconfirm
            title="设为全局映射？"
            description="这几条会立刻写进全局设置，对所有任务生效，并从本任务的列表里移走。"
            onConfirm={() => void promote()}
          >
            <Button type="dashed" icon={<GlobalOutlined />} loading={promoting}>
              设为全局
            </Button>
          </Popconfirm>
        )}
      </Space.Compact>

      {scope === 'task' && <GlobalRules rules={globalRules} overridden={covered} />}

      {rows.length > 0 && (
        <Alert
          type="info"
          showIcon
          message="只改连接目标，不改请求内容"
          description={
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              URL、Host 头、TLS 证书校验用的都还是原域名，签名与防盗链照常有效。
              原地址支持 <code>*.example.com</code> 形式的通配（精确匹配优先）。
              命中映射的请求会自动绕开系统代理 —— 否则域名由代理去解析，映射不会生效。
              <br />
              目标可以带 <code>:端口</code>：原地址是域名时，只在原 URL 没有显式写端口
              时才生效（URL 上的端口优先）；原地址是裸 IP 时映射里的端口总是生效。
              <br />
              ⚡ 测的是屏幕上这份规则，不用先保存；但要让它对真正的请求生效，还是得保存。
            </Typography.Text>
          }
        />
      )}
    </Space>
  );
}

/**
 * 当前生效的全局映射，只读。
 *
 * 任务真正用的是「全局 ∪ 任务级」，只显示任务级那一半，等于让人对着半张表
 * 排查 —— 「我这儿明明没配，怎么连到那个 IP 去了」。
 */
function GlobalRules({ rules, overridden }: { rules: HostMapping[]; overridden: Set<string> }) {
  const active = rules.filter(rule => rule.from.trim());
  if (!active.length) return null;
  return (
    <div className="hostmap-global">
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        全局映射（对所有任务生效，在「设置」里编辑）：
      </Typography.Text>
      {active.map((rule, index) => {
        const shadowed = overridden.has(key(rule.from));
        return (
          <div className="hostmap-global-row" key={index}>
            <code>
              {rule.from} → {rule.to}
            </code>
            {!rule.enabled && <Tag>已停用</Tag>}
            {shadowed && rule.enabled && <Tag color="warning">被本任务覆盖</Tag>}
          </div>
        );
      })}
    </div>
  );
}

/** ⚡ 的结果。测的是屏幕上这份规则，包括还没保存的改动。 */
function ResultLine({ result }: { result: HostResolution | string }) {
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
          ? `已映射到 ${result.mapped_to}${addrs ? ` → ${addrs}` : ''}${
              // 走了自建 DoT 就说一声：TUN 环境下这是「拿到的是真实地址还是
              // fake-ip」的唯一区别，出问题时第一个要看的就是它。
              result.resolver && result.resolver !== 'system' ? `（经 ${result.resolver} 解析）` : ''
            }`
          : `没有规则命中，走正常 DNS${addrs ? `：${addrs}` : ''}`
      }
    />
  );
}
