import { DeleteOutlined } from '@ant-design/icons';
import { Button, Form, Input, Modal, Popconfirm, Select, Statistic, message } from 'antd';
import { useEffect } from 'react';
import { api, type GlobalState, type RateAlgorithm } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, parseSize, sizeInput } from '../../utils/format';

interface SettingsForm {
  global_rate_limit_bps: string;
  global_rate_limit_algorithm: RateAlgorithm;
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
