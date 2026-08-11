import {
  CheckCircleOutlined,
  CloudDownloadOutlined,
  DeleteOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
} from '@ant-design/icons';
import { Button, Popconfirm, Progress, Tag } from 'antd';
import type { CacheJobInfo, TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, formatSpeed, percent } from '../../utils/format';

/** 缓存按钮的四种形态。播放中途按下不影响播放，反之亦然。 */
function primaryAction(state: CacheJobInfo['state'] | undefined, complete: boolean) {
  if (complete) return { label: '已缓存完整文件', icon: <CheckCircleOutlined />, kind: 'done' as const };
  if (state === 'running') return { label: '暂停缓存', icon: <PauseCircleOutlined />, kind: 'pause' as const };
  if (state === 'paused') return { label: '继续缓存', icon: <PlayCircleOutlined />, kind: 'start' as const };
  return { label: '缓存整个文件', icon: <CloudDownloadOutlined />, kind: 'start' as const };
}

/**
 * 缓存一栏的状态标签。没点过缓存但本地已经有数据，说明那是播放顺手落盘的，
 * 标成「未开始」会让人以为白下载了一遍。
 */
function statusTag(state: CacheJobInfo['state'] | undefined, complete: boolean, hasLocalData: boolean) {
  if (complete) return { color: 'success', text: '已完成' };
  if (state === 'running') return { color: 'processing', text: '缓存中' };
  if (state === 'failed') return { color: 'error', text: '失败' };
  if (state === 'paused') return { color: 'warning', text: '已暂停' };
  return hasLocalData
    ? { color: 'cyan', text: '播放已落盘部分' }
    : { color: 'default', text: '未开始' };
}

/**
 * 场景 02：把整个文件补齐到本地。
 * 与播放共用同一份稀疏文件和同一组线程，已落盘的区间直接跳过，
 * 所以边看边缓存不会把同一段下载两遍。
 */
export default function CacheSection({ task }: { task: TaskInfo }) {
  const mutate = useDashboard(state => state.mutate);
  const job = task.cache_job;
  const cache = task.cache;

  const total = job?.total_bytes ?? cache?.total_size ?? 0;
  const done = Math.min(job?.done_bytes ?? cache?.bytes_cached ?? 0, total || Number.MAX_SAFE_INTEGER);
  const pct = percent(done, total);
  const complete = job?.state === 'done' || (total > 0 && done >= total);
  const failed = job?.state === 'failed';
  const action = primaryAction(job?.state, complete);
  const hasLocalData = done > 0;
  const tag = statusTag(job?.state, complete, hasLocalData);

  return (
    <section className="scenario cache">
      <div className="scenario-title">
        <div>
          <small>场景 02</small>
          <h3>完整缓存</h3>
          <p>按整体吞吐排布线程，尽快补齐全文件；已被播放拉下来的分片不会重复下载。</p>
        </div>
        <Tag color={tag.color}>{tag.text}</Tag>
      </div>

      <Progress percent={pct} strokeColor={{ '0%': '#38bdf8', '100%': '#9b7bff' }} />

      <div className="scenario-metrics">
        <span>
          <b>
            {formatBytes(done)} / {total ? formatBytes(total) : '未知'}
          </b>
          <small>已缓存</small>
        </span>
        <span>
          <b>{formatSpeed(job?.current_speed_bps ?? 0)}</b>
          <small>{job?.threads ?? task.config.max_threads} 线程并行</small>
        </span>
      </div>

      {failed && <div className="error-line">{job?.error}</div>}

      <div className="scenario-actions">
        <Button
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
            <Button danger icon={<DeleteOutlined />} title="清理缓存" />
          </Popconfirm>
        )}
      </div>
    </section>
  );
}
