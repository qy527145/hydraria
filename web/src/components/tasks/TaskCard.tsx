import {
  DeleteOutlined,
  EditOutlined,
  EllipsisOutlined,
  ExportOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import { Button, Card, Dropdown, Popconfirm, Space, Tag, Typography, message, type MenuProps } from 'antd';
import { useState } from 'react';
import type { TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import CacheHeatmap from './CacheHeatmap';
import CacheSection from './CacheSection';
import PlaybackSection from './PlaybackSection';
import TaskDetails from './TaskDetails';

interface Props {
  task: TaskInfo;
  onEdit: (task: TaskInfo) => void;
  onClone: (task: TaskInfo) => void;
}

/**
 * 一张卡片 = 一个任务的两个场景。完整的任务配置（分卷、线程、请求头、插件…）
 * 收进「编辑完整配置」弹窗，卡片正面只留下播放与缓存两列可操作内容。
 */
export default function TaskCard({ task, onEdit, onClone }: Props) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const mutate = useDashboard(state => state.mutate);

  const copyJson = async () => {
    await navigator.clipboard.writeText(JSON.stringify(task.config, null, 2));
    message.success('配置 JSON 已复制');
  };

  const menu: MenuProps['items'] = [
    { key: 'details', icon: <InfoCircleOutlined />, label: '任务详情', onClick: () => setDetailsOpen(true) },
    { key: 'edit', icon: <EditOutlined />, label: '编辑完整配置', onClick: () => onEdit(task) },
    { key: 'clone', label: '克隆配置', onClick: () => onClone(task) },
    { key: 'copy', label: '复制 JSON', onClick: () => void copyJson() },
    {
      key: 'export',
      icon: <ExportOutlined />,
      label: (
        <a href={`/api/tasks/${task.task_id}/export`} download>
          导出 JSON
        </a>
      ),
    },
  ];

  const title = (
    <div className="task-heading">
      <div>
        <Typography.Title level={4}>{task.config.name || '未命名任务'}</Typography.Title>
        <Typography.Text code>{task.task_id}</Typography.Text>
      </div>
      <Space size={4}>
        <Tag>{task.config.volumes.length} 卷</Tag>
        <Tag>{task.config.max_threads} 线程</Tag>
      </Space>
    </div>
  );

  const actions = (
    <Space>
      <Dropdown menu={{ items: menu }} trigger={['click']}>
        <Button type="text" icon={<EllipsisOutlined />} />
      </Dropdown>
      <Popconfirm
        title="删除任务？"
        description="已缓存的数据会保留在磁盘，可在缓存一栏单独清理。"
        onConfirm={() => void mutate(() => api.remove(task.task_id))}
      >
        <Button type="text" danger icon={<DeleteOutlined />} />
      </Popconfirm>
    </Space>
  );

  // 热力条放在两列下方通栏：它描述的是两个场景共同写出的同一份本地文件。
  const total = task.cache_job?.total_bytes ?? task.cache?.total_size ?? 0;
  const heat = task.cache_job?.bitmap_summary ?? task.cache?.bitmap_summary ?? [];

  return (
    <Card className="task-card" title={title} extra={actions}>
      <div className="scenario-grid">
        <PlaybackSection task={task} />
        <CacheSection task={task} />
      </div>
      {total > 0 && (
        <div className="shared-map">
          <div className="field-heading">
            <span>本地分片分布</span>
            <Typography.Text type="secondary">播放与缓存共享，已落盘的区间不会重复下载</Typography.Text>
          </div>
          <CacheHeatmap values={heat} total={total} />
        </div>
      )}
      <TaskDetails task={task} open={detailsOpen} onClose={() => setDetailsOpen(false)} />
    </Card>
  );
}
