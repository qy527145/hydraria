import { Button, Card, Empty, Form, Input, InputNumber, Modal, Select, Switch, message } from 'antd';
import type { PluginEntry } from '../../api/client';
import { api } from '../../api/client';

interface Props {
  open: boolean;
  onClose: () => void;
  plugins: PluginEntry[];
}

/** 插件的全局配置。任务级插件配置在「编辑任务配置」里维护。 */
export default function PluginsModal({ open, onClose, plugins }: Props) {
  return (
    <Modal
      title="插件与工具"
      width={760}
      open={open}
      onCancel={onClose}
      footer={<Button onClick={onClose}>关闭</Button>}
    >
      {plugins.length ? (
        plugins.map(plugin => <PluginCard key={plugin.id} plugin={plugin} />)
      ) : (
        <Empty description="没有已注册的插件" />
      )}
    </Modal>
  );
}

function PluginCard({ plugin }: { plugin: PluginEntry }) {
  const [form] = Form.useForm();

  const save = async () => {
    try {
      const values = await form.validateFields();
      await api.savePluginGlobal(plugin.id, values);
      message.success(`${plugin.name} 已保存`);
    } catch (error) {
      message.error(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <Card className="plugin-card" title={plugin.name} extra={plugin.id}>
      <p>{plugin.description}</p>
      <Form
        form={form}
        layout="vertical"
        initialValues={(plugin.global ?? plugin.default_global ?? {}) as Record<string, unknown>}
      >
        {plugin.global_fields.map(field => (
          <Form.Item
            key={field.key}
            name={field.key}
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
              <Input />
            )}
          </Form.Item>
        ))}
        <Button onClick={() => void save()}>保存全局配置</Button>
      </Form>
      {plugin.has_forward && (
        <p className="plugin-note">
          正向工具仍通过插件 API 提供；任务里的后处理配置在「编辑任务配置」的插件一栏维护。
        </p>
      )}
    </Card>
  );
}
