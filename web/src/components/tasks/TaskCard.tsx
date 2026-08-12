import {
  DeleteOutlined,
  EditOutlined,
  EllipsisOutlined,
  ExportOutlined,
  InfoCircleOutlined,
} from '@ant-design/icons';
import { Button, Dropdown, Popconfirm, Tag, Tooltip, message, type MenuProps } from 'antd';
import { useState } from 'react';
import type { TaskConfig, TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, formatSpeed, percent, timeAgo } from '../../utils/format';
import CacheSection from './CacheSection';
import PlaybackSection from './PlaybackSection';
import Sparkline from './Sparkline';
import TaskDetails from './TaskDetails';

interface Props {
  task: TaskInfo;
  onEdit: (task: TaskInfo) => void;
  onClone: (task: TaskInfo) => void;
}

/** 去重后的源 URL 数。后端有同名的 `TaskConfig::urls()`，只是不上线传。 */
function sourceCount(config: TaskConfig): number {
  return new Set(config.volumes.flat()).size;
}

/** 太长的 URL 从中间截断：结尾往往是文件名和签名，比中段有用得多。 */
function elide(url: string, max = 72): string {
  if (url.length <= max) return url;
  const head = Math.ceil((max - 1) / 2);
  return `${url.slice(0, head)}…${url.slice(url.length - (max - 1 - head))}`;
}

/**
 * 悬停任务名时看到的源信息。
 *
 * 卡片正面只有代理短链，而短链长得都一样——光看卡片认不出「这个任务到底指向
 * 哪个文件」。首卷第一个镜像就够回答这个问题，剩下的镜像和分卷只报个数。
 */
function sourceHint(config: TaskConfig) {
  const volumes = config.volumes.filter(volume => volume.length > 0);
  const first = volumes[0]?.[0];
  if (!first) return '没有配置源 URL';
  const mirrors = volumes[0].length;
  return (
    <div className="tc-hint">
      <div className="tc-hint-cap">
        卷 1{mirrors > 1 && ` · ${mirrors} 个镜像`}
        {volumes.length > 1 && ` · 共 ${volumes.length} 卷`}
      </div>
      <div className="tc-hint-url">{elide(first)}</div>
    </div>
  );
}

/**
 * 一张卡片 = 一个任务。上半是密集的读数区（吞吐、落盘分布、四格指标），下半是
 * 按两个场景分组的操作条。
 *
 * 代理地址**只**出现在「代理播放」那条操作条上。它既是读数又是动作入口，放两份
 * 的结果是同一个 URL 在一张卡上出现两次、两个复制按钮，用户还得猜它们有没有区别。
 *
 * 读数在上、动作在下，而不是按场景切成左右两栏：`served`、`conns`、`split`
 * 这些数字对播放和缓存都成立，硬塞进某一栏只会被复制两遍或被丢掉一份。真正
 * 分场景的只有按钮——那正是下面两条操作条的职责。
 */
export default function TaskCard({ task, onEdit, onClone }: Props) {
  const [detailsOpen, setDetailsOpen] = useState(false);
  const mutate = useDashboard(state => state.mutate);

  const copyJson = async () => {
    await navigator.clipboard.writeText(JSON.stringify(task.config, null, 2));
    message.success('配置 JSON 已复制');
  };
  const menu: MenuProps['items'] = [
    { key: 'details', icon: <InfoCircleOutlined />, label: '源看板 / 任务详情', onClick: () => setDetailsOpen(true) },
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

  const job = task.cache_job;
  const cache = task.cache;
  // 缓存填充进行中：曲线该跟着它走，而不是跟着一个没人连的 bytes_served。
  const filling = job?.state === 'running';
  const total = job?.total_bytes ?? cache?.total_size ?? 0;
  const done = Math.min(job?.done_bytes ?? cache?.bytes_cached ?? 0, total || Number.MAX_SAFE_INTEGER);
  const pct = percent(done, total);
  const heat = job?.bitmap_summary ?? cache?.bitmap_summary ?? [];
  const probes = (cache?.hits ?? 0) + (cache?.misses ?? 0);

  // 四格读数，和旧版一致。六格在 460px 宽的卡片上每格只剩 ~60px，`1 · 16 thr`
  // 这种最常见的值都会被截断——格子多了反而更难读。`conns` 挪进播放操作条的
  // 状态行（本来就写着「N 个连接」），`split` 进详情抽屉：它是配置，不是读数。
  //
  // 空值留 `—` 而不是让格子消失：卡片之间的格子对不齐的话，扫一眼比较多个任务
  // 就没法做了。
  const metrics: { label: string; value: string; sub?: string; title: string }[] = [
    {
      label: 'sources',
      value: `${sourceCount(task.config)}`,
      sub: `· ${task.config.max_threads} thr`,
      title: `${sourceCount(task.config)} 个去重后的源 URL · ${task.config.max_threads} 个并发线程（单卷 ${task.config.max_per_volume} × ${task.config.volumes.length} 卷）`,
    },
    { label: 'served', value: formatBytes(task.bytes_served), title: '累计发给客户端的字节' },
    {
      label: 'cached',
      value: done > 0 ? formatBytes(done) : '—',
      sub: total > 0 && done > 0 ? `${pct}%` : undefined,
      title: total > 0 ? `本地 ${formatBytes(done)} / ${formatBytes(total)}` : '总大小未知',
    },
    {
      label: 'hit-rate',
      value: probes > 0 ? `${percent(cache?.hits ?? 0, probes)}%` : '—',
      sub: probes > 0 ? `${cache?.hits}H/${cache?.misses}M` : undefined,
      title: probes > 0 ? `命中 ${cache?.hits} 次 / 未命中 ${cache?.misses} 次` : '还没有读取记录',
    },
  ];

  return (
    <article className={`task-card${task.paused ? ' paused' : ''}`}>
      <header className="tc-head">
        <span className="tc-id">{task.task_id}</span>
        <Tooltip title={sourceHint(task.config)} placement="topLeft">
          <span className="tc-name">{task.config.name || '(未命名)'}</span>
        </Tooltip>
        <Tooltip title={`${task.updated_at > task.created_at ? '最后编辑' : '创建'}于 ${new Date((task.updated_at || task.created_at) * 1000).toLocaleString('zh-CN')}`}>
          <span className="tc-age">{timeAgo(task.updated_at || task.created_at)}</span>
        </Tooltip>
        <span className="tc-badges">
          {task.paused ? (
            <Tag color="warning">paused</Tag>
          ) : (
            <Tag color="success">running</Tag>
          )}
          {task.config.cache && <Tag color="processing">cache</Tag>}
          {task.config.persist && <Tag color="purple">persist</Tag>}
          {task.config.rate_limit_bps > 0 && (
            <Tag color="red">
              ≤{formatSpeed(task.config.rate_limit_bps)}
            </Tag>
          )}
        </span>
        <Dropdown menu={{ items: menu }} trigger={['click']}>
          <Button type="text" size="small" icon={<EllipsisOutlined />} />
        </Dropdown>
        <Popconfirm
          title="删除任务？"
          description="已缓存的数据会保留在磁盘，可在缓存一栏单独清理。"
          onConfirm={() => void mutate(() => api.remove(task.task_id))}
        >
          <Button type="text" size="small" danger icon={<DeleteOutlined />} />
        </Popconfirm>
      </header>

      {/* 一张卡片上其实有两条方向相反的流：发给客户端的（bytes_served）和从
          源站拉进磁盘的（缓存填充）。曲线只有一条，所以显示**正在发生的那条**
          并如实标注 —— 早先固定显示前者，于是按下「缓存整个文件」后，250 MB/s
          的下载在卡片上写着 0 B/s。两者相加不是办法：边播边缓存时同一批字节会
          被数两遍。 */}
      <Tooltip
        title={
          <>
            <div>发给客户端：{formatSpeed(task.current_speed_bps)}</div>
            <div>从源站拉取：{formatSpeed(job?.current_speed_bps ?? 0)}</div>
          </>
        }
      >
        <div className="tc-spark">
          <span className="tc-cap">{filling ? '缓存拉取' : '实时吞吐'}</span>
          <Sparkline samples={filling ? (job?.speed_samples ?? []) : task.speed_samples} />
          <b>{formatSpeed(filling ? (job?.current_speed_bps ?? 0) : task.current_speed_bps)}</b>
        </div>
      </Tooltip>

      {/* 进度条和分布条即使没有本地数据也占位。同一行里有的卡片有、有的没有的话，
          卡片高度就参差不齐；空态本身也是信息（"这个任务还没落过盘"）。 */}
      <Tooltip
        title={
          total > 0
            ? `本地 ${formatBytes(done)} / ${formatBytes(total)}（播放与缓存共享，已落盘的区间不会重复下载）`
            : '本地还没有数据'
        }
      >
        <div className="tc-local">
          <div className="tc-progress">
            <div className="tc-progress-bar" style={{ width: `${pct}%` }} />
          </div>
          {heat.length > 0 ? (
            <div className="tc-heat" aria-label="本地分片分布">
              {heat.map((value, index) => (
                <span
                  key={index}
                  style={{ background: `rgba(110,168,255,${(0.08 + (value / 100) * 0.92).toFixed(2)})` }}
                />
              ))}
            </div>
          ) : (
            <div className="tc-heat empty" aria-label="本地暂无分片" />
          )}
        </div>
      </Tooltip>

      <div className="tc-metrics">
        {metrics.map(metric => (
          <div className="tc-cell" key={metric.label} title={metric.title}>
            <div className="tc-cap">{metric.label}</div>
            <div className="tc-val">
              {metric.value}
              {metric.sub && <small>{metric.sub}</small>}
            </div>
          </div>
        ))}
      </div>

      <footer className="tc-scenarios">
        <PlaybackSection task={task} />
        <CacheSection task={task} />
      </footer>

      <TaskDetails task={task} open={detailsOpen} onClose={() => setDetailsOpen(false)} />
    </article>
  );
}
