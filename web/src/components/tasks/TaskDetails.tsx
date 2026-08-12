import { Badge, Descriptions, Drawer, Table, Typography } from 'antd';
import type { TaskInfo, UrlHealth } from '../../api/client';
import { formatBytes, formatSpeed } from '../../utils/format';
import CacheHeatmap from './CacheHeatmap';

interface Props {
  task: TaskInfo;
  open: boolean;
  onClose: () => void;
}

/** 只读诊断面板。卡片正面只放两个场景的操作，排障信息全部收到这里。 */
export default function TaskDetails({ task, open, onClose }: Props) {
  const { config } = task;
  // 卡片上的分布条是压扁的，容不下逐段的字节区间；这里放全尺寸的带 tooltip 版本。
  const heat = task.cache_job?.bitmap_summary ?? task.cache?.bitmap_summary ?? [];
  const total = task.cache_job?.total_bytes ?? task.cache?.total_size ?? 0;
  return (
    <Drawer title={`${config.name || task.task_id} · 任务详情`} width={760} open={open} onClose={onClose}>
      <Descriptions
        column={2}
        size="small"
        bordered
        items={[
          { key: 'id', label: '任务 ID', children: task.task_id },
          {
            key: 'threads',
            label: '线程',
            children: `${config.max_threads}（单卷 ${config.max_per_volume} × ${config.volumes.length} 卷）`,
          },
          { key: 'split', label: '分片', children: config.max_split ? formatBytes(config.max_split) : '自动' },
          {
            key: 'sources',
            label: '分卷 / URL',
            children: `${config.volumes.length} / ${config.volumes.flat().length}`,
          },
          { key: 'persist', label: '持久化', children: config.persist ? '是' : '否' },
          { key: 'write', label: '播放写透', children: config.cache ? '开启' : '关闭' },
        ]}
      />

      {total > 0 && (
        <>
          <Typography.Title level={5}>本地分片分布</Typography.Title>
          <Typography.Paragraph type="secondary">
            播放与缓存共享同一份文件，已落盘的区间不会重复下载。悬停看每段的字节范围。
          </Typography.Paragraph>
          <CacheHeatmap values={heat} total={total} />
        </>
      )}

      <Typography.Title level={5}>源状态</Typography.Title>
      <Table<UrlHealth>
        rowKey="url"
        size="small"
        pagination={false}
        dataSource={task.url_health}
        columns={[
          {
            title: '状态',
            width: 90,
            render: (_, health) => (
              <Badge
                status={health.last_error ? 'error' : health.last_status ? 'success' : 'default'}
                text={health.last_status ?? '—'}
              />
            ),
          },
          { title: '源', dataIndex: 'url', ellipsis: true },
          { title: '连接', dataIndex: 'in_flight_requests', width: 70 },
          { title: '速度', width: 100, render: (_, health) => formatSpeed(health.current_speed_bps) },
          { title: '贡献', width: 100, render: (_, health) => formatBytes(health.bytes_contributed) },
        ]}
      />

      <Typography.Title level={5}>原始配置</Typography.Title>
      <pre className="config-json">{JSON.stringify(config, null, 2)}</pre>
    </Drawer>
  );
}
