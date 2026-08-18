import { DeleteOutlined } from '@ant-design/icons';
import { Button, Form, Input, Modal, Popconfirm, Select, Statistic, message } from 'antd';
import { useEffect } from 'react';
import { api, type GlobalState, type HostMapping, type RateAlgorithm } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, parseSize, sizeInput } from '../../utils/format';
import HostMapEditor from './HostMapEditor';

interface SettingsForm {
  global_rate_limit_bps: string;
  global_rate_limit_algorithm: RateAlgorithm;
  host_mappings: HostMapping[];
  dns: string;
}

interface Props {
  open: boolean;
  onClose: () => void;
  global: GlobalState | null;
}

export default function SettingsModal({ open, onClose, global }: Props) {
  const [form] = Form.useForm<SettingsForm>();
  const mutate = useDashboard(state => state.mutate);

  useEffect(() => {
    if (!open) return;
    void api.settings().then(settings =>
      form.setFieldsValue({
        global_rate_limit_bps: sizeInput(settings.global_rate_limit_bps),
        global_rate_limit_algorithm: settings.global_rate_limit_algorithm,
        host_mappings: settings.host_mappings ?? [],
        dns: settings.dns ?? '',
      }),
    );
  }, [open, form]);

  const save = async () => {
    try {
      const values = await form.validateFields();
      await mutate(() =>
        api.saveSettings({
          global_rate_limit_bps: values.global_rate_limit_bps
            ? parseSize(values.global_rate_limit_bps)
            : 0,
          global_rate_limit_algorithm: values.global_rate_limit_algorithm,
          // 两头都空的行是加了又没填的，后端也会丢掉，这里先滤一遍免得白跑一趟。
          host_mappings: (values.host_mappings ?? []).filter(m => m.from.trim() || m.to.trim()),
          dns: (values.dns ?? '').trim(),
        }),
      );
      message.success('全局设置已保存');
      onClose();
    } catch (error) {
      message.error(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <Modal title="全局设置" open={open} onCancel={onClose} onOk={() => void save()} okText="保存">
      <Form form={form} layout="vertical">
        <Form.Item name="global_rate_limit_bps" label="全局限速（空=不限）">
          <Input placeholder="10M" />
        </Form.Item>
        <Form.Item name="global_rate_limit_algorithm" label="限速算法">
          <Select
            options={[
              { value: 'token_bucket', label: '令牌桶 — 允许短突发' },
              { value: 'sliding_window', label: '滑动窗口 — 严格一秒窗口' },
            ]}
          />
        </Form.Item>
        <Form.Item
          name="host_mappings"
          label="域名映射（域名解析不了、又不能改 URL 时用）"
          tooltip="等价于 curl --resolve：只换 TCP 连接的目标地址，URL 与 Host 头保持原样，签名参数不受影响。对所有任务生效。"
        >
          <HostMapEditor scope="global" />
        </Form.Item>
        <Form.Item
          name="dns"
          label="映射目标的解析 DNS（空=系统解析）"
          tooltip="只用来解析上面那些映射目标的域名，其余请求一律照常走系统解析。开着 TUN 模式的代理时系统解析会返回 fake-ip（198.18.x.x），映射就会静默失效——表现为连上了、状态码也对、但一个字节都不来。这时填 tls://1.1.1.1 让 Hydraria 自己走 DNS over TLS 查真实地址即可。只收 IP 地址；查不通会自动退回系统解析。"
          extra="开了 TUN 代理而下载不动时填它；国内可用 tls://223.5.5.5，通常更快"
        >
          <Input placeholder="留空=系统解析；tls://1.1.1.1" />
        </Form.Item>
      </Form>
      <div className="settings-cache">
        <Statistic title="持久缓存占用" value={formatBytes(global?.cache_total_bytes ?? 0)} />
        <Popconfirm
          title="清理所有任务的持久缓存？"
          description="正在播放的任务会重新从源站拉取。"
          onConfirm={() =>
            void mutate(() => api.clearAllCache()).then(() => message.success('全部缓存已清理'))
          }
        >
          <Button danger icon={<DeleteOutlined />}>
            清空全部缓存
          </Button>
        </Popconfirm>
      </div>
    </Modal>
  );
}
