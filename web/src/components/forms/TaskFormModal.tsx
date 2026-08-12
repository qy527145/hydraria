import {
  Alert,
  Button,
  Card,
  Checkbox,
  Collapse,
  Descriptions,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Tabs,
  Typography,
  message,
} from 'antd';
import { useEffect, useMemo, useState } from 'react';
import type { FormInstance } from 'antd';
import type {
  Disposition,
  PluginEntry,
  ProbeResult,
  RateAlgorithm,
  TaskConfig,
  TaskInfo,
  TaskPluginConfig,
} from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, parseSize, sizeInput } from '../../utils/format';
import HeadersEditor from './HeadersEditor';
import VolumeEditor from './VolumeEditor';

const DRAFT_KEY = 'hydraria.createDraft.v2';

/** 后端 `max_split` 的下限：0 表示自动，否则至少 64K。 */
const MIN_SPLIT = 64 * 1024;

/** 新任务的初始配置。`cache: true` 让播放与缓存默认落在同一份持久文件上。 */
const newTaskConfig: TaskConfig = {
  volumes: [[]],
  // 派生值：保存时按「单卷并发 × 卷数」重算，这里只是让类型完整。
  max_threads: 4,
  max_per_volume: 4,
  max_split: 0,
  cache: true,
  headers: {},
  name: null,
  output_filename: null,
  auto_filename: true,
  rate_limit_bps: 0,
  rate_limit_algorithm: 'token_bucket',
  persist: false,
  plugins: [],
  content_disposition: 'auto',
};

/**
 * 表单内部形态。大小类字段用 "5M" 这样的字符串编辑，请求头用 JSON 文本编辑，
 * 提交时统一转回 TaskConfig，这样草稿存取和校验都只面对一种形状。
 */
interface FormValues {
  max_per_volume: number;
  max_split: string;
  rate_limit_bps: string;
  rate_limit_algorithm: RateAlgorithm;
  headers: string;
  name: string;
  output_filename: string;
  auto_filename: boolean;
  cache: boolean;
  persist: boolean;
  content_disposition: Disposition;
  plugin_enabled: Record<string, boolean>;
  /** 插件字段都渲染成 Input / InputNumber / Switch / Select，值都是标量。 */
  plugin_config: Record<string, Record<string, PluginFieldValue>>;
}

type PluginFieldValue = string | number | boolean | null;

function toFormValues(config: TaskConfig): FormValues {
  return {
    max_per_volume: config.max_per_volume,
    max_split: sizeInput(config.max_split),
    rate_limit_bps: sizeInput(config.rate_limit_bps),
    rate_limit_algorithm: config.rate_limit_algorithm,
    headers: Object.keys(config.headers).length ? JSON.stringify(config.headers, null, 2) : '',
    name: config.name ?? '',
    output_filename: config.output_filename ?? '',
    auto_filename: config.auto_filename,
    cache: config.cache,
    persist: config.persist,
    content_disposition: config.content_disposition,
    // 回填已有插件配置，编辑保存时才不会把它们悄悄重置成默认值。
    plugin_enabled: Object.fromEntries(config.plugins.map(plugin => [plugin.id, plugin.enabled])),
    plugin_config: Object.fromEntries(
      config.plugins.map(plugin => [plugin.id, { ...plugin.config } as Record<string, PluginFieldValue>]),
    ),
  };
}

/**
 * 合并插件配置。注册表里的插件用表单值覆盖，注册表里没有的（比如插件被临时下线）
 * 原样保留，保证一次编辑不会丢掉任务原有的配置内容。
 */
function toPluginConfigs(
  values: FormValues,
  plugins: PluginEntry[],
  existing: TaskPluginConfig[],
): TaskPluginConfig[] {
  const leftovers = new Map(existing.map(plugin => [plugin.id, plugin]));
  const merged = plugins.map(plugin => {
    const previous = leftovers.get(plugin.id);
    leftovers.delete(plugin.id);
    return {
      id: plugin.id,
      // 表单没提供值（插件是后来注册的、面板从未挂载）时沿用任务原有的状态，
      // 而不是默认关掉——一次保存不该悄悄改变任务的行为。
      enabled: values.plugin_enabled?.[plugin.id] ?? previous?.enabled ?? false,
      config: {
        ...(plugin.default_task ?? {}),
        ...(previous?.config ?? {}),
        ...(values.plugin_config?.[plugin.id] ?? {}),
      },
    };
  });
  return [...merged, ...leftovers.values()];
}

/**
 * 线程总数 = 单卷并发上限 × 卷数，和后端 `TaskConfig::normalize` 同一条规则。
 *
 * 前端也算一遍不是重复：保存后界面要立刻显示对的读数，而不是等下一轮轮询。
 * 后端仍会重新派生，这里算错了也不会写进配置。
 */
export function deriveThreads(maxPerVolume: number, volumes: number): number {
  return Math.min(128, Math.max(1, (maxPerVolume || 1) * Math.max(1, volumes)));
}

function toTaskConfig(
  values: FormValues,
  volumes: string[][],
  plugins: PluginEntry[],
  existing: TaskPluginConfig[],
): TaskConfig {
  const filled = volumes.filter(volume => volume.length > 0);
  return {
    volumes: filled,
    max_threads: deriveThreads(values.max_per_volume, filled.length),
    max_per_volume: values.max_per_volume,
    max_split: values.max_split ? parseSize(values.max_split) : 0,
    cache: values.cache,
    headers: values.headers?.trim() ? (JSON.parse(values.headers) as Record<string, string>) : {},
    name: values.name?.trim() || null,
    output_filename: values.output_filename?.trim() || null,
    auto_filename: values.auto_filename,
    rate_limit_bps: values.rate_limit_bps ? parseSize(values.rate_limit_bps) : 0,
    rate_limit_algorithm: values.rate_limit_algorithm,
    persist: values.persist,
    plugins: toPluginConfigs(values, plugins, existing),
    content_disposition: values.content_disposition,
  };
}

/**
 * 分卷的校验结果。URL 在这里就要挑出毛病：留到任务跑起来才报错的话，
 * 用户看到的是「上游 0 字节」，而不是「第 2 卷第 1 行不是合法地址」。
 */
function checkVolumes(volumes: string[][]): string | null {
  const filled = volumes.filter(volume => volume.length > 0);
  if (!filled.length) return '至少填写一个源 URL';
  for (const [index, volume] of volumes.entries()) {
    for (const url of volume) {
      let parsed: URL;
      try {
        parsed = new URL(url);
      } catch {
        return `卷 ${index + 1}：「${url}」不是合法地址`;
      }
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        return `卷 ${index + 1}：「${url}」只支持 http / https`;
      }
    }
  }
  return null;
}

/** 大小输入框的通用校验：留空合法，填了就必须解析得出来，且不小于 `min`。 */
function sizeRule(min = 0, hint?: string) {
  return {
    validator: (_: unknown, value: string) => {
      if (!value?.trim()) return Promise.resolve();
      let bytes: number;
      try {
        bytes = parseSize(value);
      } catch (error) {
        return Promise.reject(error instanceof Error ? error : new Error(String(error)));
      }
      if (bytes < min) {
        return Promise.reject(new Error(hint ?? `不能小于 ${formatBytes(min)}`));
      }
      return Promise.resolve();
    },
  };
}

interface Draft {
  values: FormValues;
  volumes: string[][];
}

function saveDraft(draft: Draft) {
  try {
    localStorage.setItem(DRAFT_KEY, JSON.stringify(draft));
  } catch {
    /* 隐私模式下写不进去，忽略即可 */
  }
}

function loadDraft(): Draft | null {
  try {
    const raw = localStorage.getItem(DRAFT_KEY);
    return raw ? (JSON.parse(raw) as Draft) : null;
  } catch {
    return null;
  }
}

interface Props {
  open: boolean;
  task: TaskInfo | null;
  /** 克隆任务时带进来的种子配置。 */
  seed: TaskConfig | null;
  onClose: () => void;
}

/** 完整任务配置的唯一入口。卡片正面不再散落这些字段，只留播放与缓存两个动作。 */
export default function TaskFormModal({ open, task, seed, onClose }: Props) {
  const [form] = Form.useForm<FormValues>();
  const plugins = useDashboard(state => state.plugins);
  const loadPlugins = useDashboard(state => state.loadPlugins);
  const mutate = useDashboard(state => state.mutate);
  const [volumes, setVolumes] = useState<string[][]>([[]]);
  const [basePlugins, setBasePlugins] = useState<TaskPluginConfig[]>([]);
  const [probing, setProbing] = useState(false);
  const [probe, setProbe] = useState<ProbeResult | null>(null);
  const [tab, setTab] = useState('source');
  const [saving, setSaving] = useState(false);

  // 提交前就能看到，而不是按下保存才弹一条红色 message。
  const volumeIssue = useMemo(() => checkVolumes(volumes), [volumes]);

  // 线程总数是派生值，不再是一个可填的字段 —— 但用户仍然需要看见它，否则
  // 「单卷并发上限」就成了一个不知道会放大多少倍的旋钮。
  const perVolume = Form.useWatch('max_per_volume', form) ?? 1;
  const filledVolumes = volumes.filter(volume => volume.length > 0).length;
  const derivedThreads = deriveThreads(perVolume, filledVolumes);

  useEffect(() => {
    if (!open) return;
    void loadPlugins();
    const config = task?.config ?? seed ?? newTaskConfig;
    const draft = !task && !seed ? loadDraft() : null;
    setVolumes((draft?.volumes ?? config.volumes).map(volume => [...volume]));
    setBasePlugins(config.plugins);
    form.setFieldsValue(draft?.values ?? toFormValues(config));
    setProbe(null);
    setTab('source');
  }, [open, task, seed, form, loadPlugins]);

  const rememberDraft = (values: FormValues, nextVolumes: string[][]) => {
    if (task || seed) return; // 只为「全新任务」留草稿
    saveDraft({ values, volumes: nextVolumes });
  };

  const submit = async () => {
    if (volumeIssue) {
      setTab('source');
      message.error(volumeIssue);
      return;
    }
    try {
      setSaving(true);
      const values = await form.validateFields();
      // 插件面板默认折叠，字段还没挂载，validateFields 不会把它们带出来。
      // getFieldValue 直接读表单 store，未挂载的字段同样取得到，否则一次
      // 「打开编辑、直接保存」就会把插件的启用状态清成关闭。
      const config = toTaskConfig(
        {
          ...values,
          plugin_enabled: (form.getFieldValue('plugin_enabled') ?? {}) as Record<string, boolean>,
          plugin_config: (form.getFieldValue('plugin_config') ?? {}) as FormValues['plugin_config'],
        },
        volumes,
        plugins,
        basePlugins,
      );
      await mutate(() => (task ? api.update(task.task_id, config) : api.create(config)));
      localStorage.removeItem(DRAFT_KEY);
      message.success(task ? '任务配置已保存' : '任务已创建');
      onClose();
    } catch (error) {
      // validateFields 的拒绝里带着出错字段，跳到它所在的分组，
      // 否则错误提示可能藏在一个没打开的标签页里。
      const failed = (error as { errorFields?: { name: (string | number)[] }[] }).errorFields;
      if (failed?.length) {
        setTab(tabOfField(String(failed[0].name[0])));
        return;
      }
      message.error(error instanceof Error ? error.message : String(error));
    } finally {
      setSaving(false);
    }
  };

  const runProbe = async () => {
    try {
      setProbing(true);
      const raw = form.getFieldValue('headers') as string | undefined;
      const headers = raw?.trim() ? (JSON.parse(raw) as Record<string, string>) : {};
      const result = await api.probe(volumes, headers);
      setProbe(result);
      if (result.suggested_filename) form.setFieldValue('output_filename', result.suggested_filename);
    } catch (error) {
      setProbe(null);
      message.error(error instanceof Error ? error.message : String(error));
    } finally {
      setProbing(false);
    }
  };

  const source = (
    <>
      <VolumeEditor
        value={volumes}
        onChange={next => {
          setVolumes(next);
          rememberDraft(form.getFieldsValue(), next);
        }}
      />
      {volumeIssue && <Alert type="warning" showIcon message={volumeIssue} />}

      <Form.Item
        name="headers"
        label="自定义请求头"
        tooltip="每个上游请求都会带上。常用的三个（Referer / Cookie / User-Agent）可以直接填表单，其余的切到 raw JSON 写。"
        rules={[
          {
            validator: (_, value: string) => {
              if (!value?.trim()) return Promise.resolve();
              try {
                const parsed: unknown = JSON.parse(value);
                if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
                  return Promise.reject(new Error('请求头必须是 JSON 对象'));
                }
                return Promise.resolve();
              } catch {
                return Promise.reject(new Error('请求头必须是合法 JSON'));
              }
            },
          },
        ]}
      >
        <HeadersEditor />
      </Form.Item>

      <Space direction="vertical" size={8} style={{ width: '100%' }}>
        <Button loading={probing} disabled={!!volumeIssue} onClick={() => void runProbe()}>
          探测源站
        </Button>
        {probe && (
          <Descriptions size="small" column={2} bordered items={probeItems(probe)} />
        )}
      </Space>
    </>
  );

  const transfer = (
    <>
      <Form.Item
        name="max_per_volume"
        label="单卷并发上限"
        tooltip="单个分卷最多几个并发请求，按源站的单 IP / 单对象连接限制来填（常见是 4）。这是唯一需要你判断的并发数字 —— 它取决于源站，而总线程数只是它乘以卷数。"
        extra={`总线程数 = 单卷并发 × ${filledVolumes} 卷 = ${derivedThreads}`}
      >
        <InputNumber min={1} max={128} style={{ width: '100%' }} />
      </Form.Item>

      <Form.Item
        name="max_split"
        label="分片大小"
        tooltip="留空＝自动：下载按线程均分剩余量（大分片优先，不做试探），播放紧贴读头铺等长小分片（2 MiB），攒出余量后再放大。只有源站要攒齐整段才发第一个字节时才自动降到 8 MiB。手填则是所有分片的硬上限，只在上游对单个 Range 的长度有特殊要求时才需要。"
        extra="留空即可，除非源站对单次 Range 请求的长度有硬性要求"
        rules={[sizeRule(MIN_SPLIT, `手填时不能小于 ${formatBytes(MIN_SPLIT)}（留空表示自动）`)]}
      >
        <Input placeholder="自动" />
      </Form.Item>

      <div className="form-grid">
        <Form.Item
          name="rate_limit_bps"
          label="单任务限速"
          tooltip="留空或 0 表示不限速。写法同分片大小，例如 5M 表示 5 MB/s。"
          rules={[sizeRule()]}
        >
          <Input placeholder="不限" />
        </Form.Item>
        <Form.Item
          name="rate_limit_algorithm"
          label="限速算法"
          tooltip="令牌桶允许攒下来的额度在瞬间放出，播放起播更快；滑动窗口每一秒都严格不超标。"
        >
          <Select
            options={[
              { value: 'token_bucket', label: '令牌桶 — 允许短突发' },
              { value: 'sliding_window', label: '滑动窗口 — 严格一秒窗口' },
            ]}
          />
        </Form.Item>
      </div>

      <Form.Item name="cache" valuePropName="checked" noStyle>
        <Checkbox>播放时写入持久缓存（与缓存场景共享同一份文件）</Checkbox>
      </Form.Item>
    </>
  );

  const output = (
    <>
      <Form.Item
        label="输出文件名"
        tooltip="下载与「强制下载」时用的文件名。留空则用探测到的名字。"
      >
        <Space.Compact block>
          <Form.Item name="output_filename" noStyle>
            <Input placeholder="留空则用自动检测结果" />
          </Form.Item>
          <Button loading={probing} disabled={!!volumeIssue} onClick={() => void runProbe()}>
            探测
          </Button>
        </Space.Compact>
      </Form.Item>

      <div className="form-grid">
        <Form.Item
          name="content_disposition"
          label="浏览器行为"
          tooltip="决定代理短链在浏览器里是直接播放还是弹下载框。播放器和下载工具不受影响。"
        >
          <Select
            options={[
              { value: 'auto', label: '默认 — 跟随源站' },
              { value: 'inline', label: '强制预览' },
              { value: 'attachment', label: '强制下载' },
            ]}
          />
        </Form.Item>
      </div>

      <Space size="large" wrap>
        <Form.Item name="auto_filename" valuePropName="checked" noStyle>
          <Checkbox>自动检测文件名</Checkbox>
        </Form.Item>
        <Form.Item name="persist" valuePropName="checked" noStyle>
          <Checkbox>重启后保留任务</Checkbox>
        </Form.Item>
      </Space>
    </>
  );

  return (
    <Modal
      title={task ? '编辑任务配置' : '新建代理任务'}
      width={880}
      open={open}
      onCancel={onClose}
      onOk={() => void submit()}
      okText={task ? '保存配置' : '生成代理短链'}
      okButtonProps={{ loading: saving }}
      destroyOnHidden
    >
      <Typography.Paragraph type="secondary">
        这里是完整的任务配置。创建后卡片上只保留两个动作：把代理地址交给播放器，或按下缓存补齐整个文件。
      </Typography.Paragraph>
      <Form
        form={form}
        layout="vertical"
        initialValues={toFormValues(newTaskConfig)}
        onValuesChange={(_, all) => rememberDraft(all, volumes)}
      >
        {/* 任务名放在标签页外面：它是列表里认任务的唯一抓手，藏进某一页等于
            让人每次新建都去翻。留在这里也就不会随标签切换消失。 */}
        <Form.Item name="name" label="任务名">
          <Input placeholder="便于在列表里辨认，例如「4K 蓝光原盘」" allowClear />
        </Form.Item>

        {/* forceRender：未选中的分组也要挂载，否则 validateFields 看不见它们的字段，
            「打开就保存」会把没露过面的配置写成空值。 */}
        <Tabs
          activeKey={tab}
          onChange={setTab}
          items={[
            { key: 'source', label: '源与分卷', children: source, forceRender: true },
            { key: 'transfer', label: '传输与并发', children: transfer, forceRender: true },
            { key: 'output', label: '输出与行为', children: output, forceRender: true },
            {
              key: 'plugins',
              label: `插件（${plugins.length}）`,
              children: <PluginFields plugins={plugins} form={form} />,
            },
          ]}
        />
      </Form>
    </Modal>
  );
}

/** 表单字段 → 它所在的分组，用于校验失败时自动切过去。 */
function tabOfField(name: string): string {
  if (name === 'headers') return 'source';
  if (['max_per_volume', 'max_split', 'rate_limit_bps', 'rate_limit_algorithm', 'cache'].includes(name)) {
    return 'transfer';
  }
  if (name.startsWith('plugin_')) return 'plugins';
  return 'output';
}

function probeItems(probe: ProbeResult) {
  return [
    {
      key: 'ranges',
      label: '多线程',
      children: probe.accepts_ranges ? '支持 Range，可并行拉取' : '不支持 Range，只能单线程顺序读',
    },
    { key: 'size', label: '总大小', children: probe.total_size ? formatBytes(probe.total_size) : '未知' },
    { key: 'type', label: '内容类型', children: probe.content_type ?? '未知' },
    { key: 'name', label: '检测到的文件名', children: probe.detected_filename ?? '未提供' },
  ];
}

function PluginFields({ plugins, form }: { plugins: PluginEntry[]; form: FormInstance<FormValues> }) {
  if (!plugins.length) return <Typography.Text type="secondary">当前没有可用插件。</Typography.Text>;
  return (
    <Collapse
      className="plugin-collapse"
      items={plugins.map(plugin => ({
        key: plugin.id,
        label: plugin.name,
        extra: <Typography.Text type="secondary">{plugin.description}</Typography.Text>,
        // 折叠面板懒渲染，未展开的插件字段不会挂载；forceRender 让它们照样进
        // 表单 store，一次「展开 A、保存」才不会清掉 B 的配置。
        forceRender: true,
        children: <PluginCard plugin={plugin} form={form} />,
      }))}
    />
  );
}

/**
 * 一个插件的配置块。
 *
 * 必填只在插件**启用时**才成立。插件停用的话它的配置根本不会被读到，把它标成
 * 必填等于「想创建任务？先给一个你根本不打算用的加密插件编一个密钥」。
 * （`forceRender` 之后这些字段总是挂载的，所以校验规则必须自己认这件事，
 * 不能再指望没挂载就不校验。）
 *
 * 停用时字段仍然可编辑，不置灰：先粘密钥再打开开关是很自然的顺序。
 */
function PluginCard({ plugin, form }: { plugin: PluginEntry; form: FormInstance<FormValues> }) {
  const enabled = Form.useWatch(['plugin_enabled', plugin.id], form) ?? false;
  return (
    <Card size="small" variant="borderless">
      <Form.Item name={['plugin_enabled', plugin.id]} valuePropName="checked">
        <Switch checkedChildren="启用" unCheckedChildren="停用" />
      </Form.Item>
      {!enabled && plugin.task_fields.some(field => field.required) && (
        <Typography.Paragraph type="secondary">
          插件已停用，下面的配置不会生效，也不做必填校验。
        </Typography.Paragraph>
      )}
      {plugin.task_fields.map(field => (
        <Form.Item
          key={field.key}
          name={['plugin_config', plugin.id, field.key]}
          label={field.label}
          tooltip={field.hint}
          valuePropName={field.kind === 'boolean' ? 'checked' : 'value'}
          rules={
            field.required && enabled
              ? [{ required: true, message: `启用 ${plugin.name} 后此项必填` }]
              : undefined
          }
        >
          {field.kind === 'boolean' ? (
            <Switch />
          ) : field.kind === 'number' ? (
            <InputNumber />
          ) : field.kind === 'text_area' ? (
            <Input.TextArea rows={3} />
          ) : field.kind === 'select' ? (
            <Select options={field.options ?? []} />
          ) : (
            // hex / path / dir_path / size 都是文本输入：size 收 "64K" 这类写法，
            // 路径与十六进制串也都按原样提交给后端校验。
            <Input />
          )}
        </Form.Item>
      ))}
    </Card>
  );
}
