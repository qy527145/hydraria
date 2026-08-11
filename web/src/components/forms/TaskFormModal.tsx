import {
  Button,
  Card,
  Checkbox,
  Collapse,
  Form,
  Input,
  InputNumber,
  Modal,
  Select,
  Space,
  Switch,
  Typography,
  message,
} from 'antd';
import { useEffect, useState } from 'react';
import type {
  Disposition,
  PluginEntry,
  RateAlgorithm,
  TaskConfig,
  TaskInfo,
  TaskPluginConfig,
} from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { parseSize, sizeInput } from '../../utils/format';
import VolumeEditor from './VolumeEditor';

const DRAFT_KEY = 'hydraria.createDraft.v2';

/** 新任务的初始配置。`cache: true` 让播放与缓存默认落在同一份持久文件上。 */
const newTaskConfig: TaskConfig = {
  volumes: [[]],
  max_threads: 16,
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
  max_threads: number;
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
    max_threads: config.max_threads,
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

function toTaskConfig(
  values: FormValues,
  volumes: string[][],
  plugins: PluginEntry[],
  existing: TaskPluginConfig[],
): TaskConfig {
  return {
    volumes: volumes.filter(volume => volume.length > 0),
    max_threads: values.max_threads,
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

  useEffect(() => {
    if (!open) return;
    void loadPlugins();
    const config = task?.config ?? seed ?? newTaskConfig;
    const draft = !task && !seed ? loadDraft() : null;
    setVolumes((draft?.volumes ?? config.volumes).map(volume => [...volume]));
    setBasePlugins(config.plugins);
    form.setFieldsValue(draft?.values ?? toFormValues(config));
  }, [open, task, seed, form, loadPlugins]);

  const rememberDraft = (values: FormValues, nextVolumes: string[][]) => {
    if (task || seed) return; // 只为「全新任务」留草稿
    saveDraft({ values, volumes: nextVolumes });
  };

  const submit = async () => {
    try {
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
      if (!config.volumes.length) throw new Error('至少填写一个源 URL');
      await mutate(() => (task ? api.update(task.task_id, config) : api.create(config)));
      localStorage.removeItem(DRAFT_KEY);
      message.success(task ? '任务配置已保存' : '任务已创建');
      onClose();
    } catch (error) {
      message.error(error instanceof Error ? error.message : String(error));
    }
  };

  const probe = async () => {
    try {
      setProbing(true);
      const raw = form.getFieldValue('headers') as string | undefined;
      const headers = raw?.trim() ? (JSON.parse(raw) as Record<string, string>) : {};
      const result = await api.probe(volumes, headers);
      if (result.suggested_filename) form.setFieldValue('output_filename', result.suggested_filename);
      message.success(
        result.accepts_ranges
          ? '探测成功：源支持 Range，可多线程并行'
          : '探测成功：源不支持 Range，只能单线程顺序读',
      );
    } catch (error) {
      message.error(error instanceof Error ? error.message : String(error));
    } finally {
      setProbing(false);
    }
  };

  return (
    <Modal
      title={task ? '编辑任务配置' : '新建代理任务'}
      width={820}
      open={open}
      onCancel={onClose}
      onOk={() => void submit()}
      okText={task ? '保存配置' : '生成代理短链'}
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
        <VolumeEditor
          value={volumes}
          onChange={next => {
            setVolumes(next);
            rememberDraft(form.getFieldsValue(), next);
          }}
        />

        <div className="form-grid three">
          <Form.Item name="max_threads" label="最大并发线程" rules={[{ required: true }]}>
            <InputNumber min={1} max={128} />
          </Form.Item>
          <Form.Item name="max_per_volume" label="单卷并发上限">
            <InputNumber min={1} max={128} />
          </Form.Item>
          <Form.Item name="max_split" label="分片大小（空=自动）">
            <Input placeholder="5M" />
          </Form.Item>
        </div>

        <Form.Item
          name="headers"
          label="自定义请求头 JSON"
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
          <Input.TextArea rows={4} placeholder={'{\n  "Cookie": "…"\n}'} />
        </Form.Item>

        <div className="form-grid">
          <Form.Item name="name" label="任务名">
            <Input placeholder="便于在列表里辨认" />
          </Form.Item>
          <Form.Item name="rate_limit_bps" label="单任务限速">
            <Input placeholder="不限" />
          </Form.Item>
        </div>

        <Form.Item label="输出文件名">
          <Space.Compact block>
            <Form.Item name="output_filename" noStyle>
              <Input placeholder="留空则用自动检测结果" />
            </Form.Item>
            <Button loading={probing} onClick={() => void probe()}>
              探测
            </Button>
          </Space.Compact>
        </Form.Item>

        <div className="form-grid">
          <Form.Item name="content_disposition" label="浏览器行为">
            <Select
              options={[
                { value: 'auto', label: '默认' },
                { value: 'inline', label: '强制预览' },
                { value: 'attachment', label: '强制下载' },
              ]}
            />
          </Form.Item>
          <Form.Item name="rate_limit_algorithm" label="限速算法">
            <Select
              options={[
                { value: 'token_bucket', label: '令牌桶 — 允许短突发' },
                { value: 'sliding_window', label: '滑动窗口 — 严格一秒窗口' },
              ]}
            />
          </Form.Item>
        </div>

        <Space size="large" wrap>
          <Form.Item name="auto_filename" valuePropName="checked" noStyle>
            <Checkbox>自动检测文件名</Checkbox>
          </Form.Item>
          <Form.Item name="cache" valuePropName="checked" noStyle>
            <Checkbox>播放时写入持久缓存（与缓存场景共享同一份文件）</Checkbox>
          </Form.Item>
          <Form.Item name="persist" valuePropName="checked" noStyle>
            <Checkbox>重启后保留任务</Checkbox>
          </Form.Item>
        </Space>

        <Collapse
          className="plugin-collapse"
          items={[
            {
              key: 'plugins',
              label: `插件（${plugins.length} 个可用）`,
              children: <PluginFields plugins={plugins} />,
            },
          ]}
        />
      </Form>
    </Modal>
  );
}

function PluginFields({ plugins }: { plugins: PluginEntry[] }) {
  if (!plugins.length) return <Typography.Text type="secondary">当前没有可用插件。</Typography.Text>;
  return (
    <>
      {plugins.map(plugin => (
        <Card size="small" key={plugin.id} title={plugin.name}>
          <Form.Item name={['plugin_enabled', plugin.id]} valuePropName="checked">
            <Switch checkedChildren="启用" unCheckedChildren="停用" />
          </Form.Item>
          {plugin.task_fields.map(field => (
            <Form.Item
              key={field.key}
              name={['plugin_config', plugin.id, field.key]}
              label={field.label}
              tooltip={field.hint}
              valuePropName={field.kind === 'boolean' ? 'checked' : 'value'}
              rules={field.required ? [{ required: true }] : undefined}
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
      ))}
    </>
  );
}
