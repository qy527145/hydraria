import {
  CheckCircleOutlined,
  CloudDownloadOutlined,
  DeleteOutlined,
  PauseOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import { Button, Popconfirm } from 'antd';
import type { CacheJobInfo, TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, formatSpeed, percent } from '../../utils/format';

/** 缓存按钮的三种形态。播放中途按下不影响播放，反之亦然。 */
function primaryAction(state: CacheJobInfo['state'] | undefined, complete: boolean) {
  if (complete) return { label: '已完整', icon: <CheckCircleOutlined />, kind: 'done' as const };
  if (state === 'running') return { label: '暂停', icon: <PauseOutlined />, kind: 'pause' as const };
  if (state === 'paused') return { label: '继续', icon: <PlayCircleOutlined />, kind: 'start' as const };
  return { label: '缓存整个文件', icon: <CloudDownloadOutlined />, kind: 'start' as const };
}

/**
 * 场景 02 的操作条：把整个文件补齐到本地。
 *
 * 与播放共用同一份稀疏文件和同一组线程，已落盘的区间直接跳过，所以边看边缓存
 * 不会把同一段下载两遍。状态行区分「播放顺手落盘的」和「没开始」——两者本地
 * 都有数据，但意思完全不同。
 */
export default function CacheSection({ task }: { task: TaskInfo }) {
  const mutate = useDashboard(state => state.mutate);
  const job = task.cache_job;
  const cache = task.cache;

  const total = job?.total_bytes ?? cache?.total_size ?? 0;
  const done = Math.min(job?.done_bytes ?? cache?.bytes_cached ?? 0, total || Number.MAX_SAFE_INTEGER);
  const complete = job?.state === 'done' || (total > 0 && done >= total);
  const action = primaryAction(job?.state, complete);
  const hasLocalData = done > 0;

  const status = complete
    ? '本地已是完整文件'
    : job?.state === 'failed'
      ? `失败：${job.error ?? '未知错误'}`
      : job?.state === 'running'
        ? `${percent(done, total)}% · ${formatSpeed(job.current_speed_bps)} · ${job.threads} 线程`
        : hasLocalData
          ? `${formatBytes(done)}${total ? ` / ${formatBytes(total)}` : ''} · ${job?.state === 'paused' ? '已暂停' : '播放顺手落盘'}`
          : '本地暂无数据';

  return (
    <div className={`scenario-bar cache${job?.state === 'failed' ? ' failed' : ''}`}>
      <div className="scenario-label">
        <CloudDownloadOutlined />
        <span>完整缓存</span>
        <em title={status}>{status}</em>
      </div>
      <div className="scenario-buttons">
        <Button
          size="small"
          type={action.kind === 'start' ? 'primary' : 'default'}
          disabled={action.kind === 'done'}
          icon={action.icon}
          onClick={() =>
            void mutate(() =>
              action.kind === 'pause' ? api.cachePause(task.task_id) : api.cacheStart(task.task_id),
            )
          }
        >
          {action.label}
        </Button>
        {hasLocalData && job?.state !== 'running' && (
          <Popconfirm
            title="清理该任务已缓存的数据？"
            description="播放仍可继续，但需要重新从源站拉取。"
            onConfirm={() => void mutate(() => api.cacheClear(task.task_id))}
          >
            <Button size="small" danger icon={<DeleteOutlined />} title="清理缓存" />
          </Popconfirm>
        )}
      </div>
    </div>
  );
}
