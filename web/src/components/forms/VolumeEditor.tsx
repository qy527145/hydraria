import { DeleteOutlined, HolderOutlined, PlusOutlined } from '@ant-design/icons';
import { Button, Card, Input, Space, Typography } from 'antd';

interface Props {
  /** 外层数组是分卷（按顺序拼接），内层是同一卷的镜像 URL。 */
  value: string[][];
  onChange: (value: string[][]) => void;
}

/** 分卷编辑器：一卷一个文本框，每行一个镜像地址。 */
export default function VolumeEditor({ value, onChange }: Props) {
  const volumes = value.length ? value : [[]];

  const setUrls = (index: number, text: string) => {
    const next = volumes.map(volume => [...volume]);
    next[index] = text
      .split(/\r?\n/)
      .map(line => line.trim())
      .filter(Boolean);
    onChange(next);
  };

  const move = (from: number, to: number) => {
    const next = volumes.map(volume => [...volume]);
    const [item] = next.splice(from, 1);
    next.splice(to, 0, item);
    onChange(next);
  };

  return (
    <div className="volumes">
      <div className="field-heading">
        <span>分卷 / 源 URL</span>
        <Typography.Text type="secondary">
          {volumes.length} 卷 · {volumes.flat().length} URL
        </Typography.Text>
      </div>
      {volumes.map((volume, index) => (
        <Card
          size="small"
          key={index}
          title={
            <Space>
              <HolderOutlined />
              <span>卷 {index + 1}</span>
            </Space>
          }
          extra={
            <Space size={4}>
              <Button size="small" disabled={index === 0} onClick={() => move(index, index - 1)}>
                ↑
              </Button>
              <Button
                size="small"
                disabled={index === volumes.length - 1}
                onClick={() => move(index, index + 1)}
              >
                ↓
              </Button>
              {volumes.length > 1 && (
                <Button
                  size="small"
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => onChange(volumes.filter((_, i) => i !== index))}
                />
              )}
            </Space>
          }
        >
          <Input.TextArea
            value={volume.join('\n')}
            rows={3}
            placeholder={'每行一个镜像 URL\nhttps://cdn-a.example/file.mp4'}
            onChange={event => setUrls(index, event.target.value)}
          />
        </Card>
      ))}
      <Button block type="dashed" icon={<PlusOutlined />} onClick={() => onChange([...volumes, []])}>
        添加分卷
      </Button>
    </div>
  );
}
