import {
  CopyOutlined,
  LinkOutlined,
  PauseCircleOutlined,
  PlayCircleOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Button, Tag, Typography, message } from 'antd';
import type { TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';
import { formatBytes, formatSpeed } from '../../utils/format';

/**
 * 场景 01：把 /stream/<id> 交给播放器。
 * 只要有连接在读，调度器就切到低延迟策略，把线程压到 seek 位置周围。
 */
export default function PlaybackSection({ task }: { task: TaskInfo }) {
  const mutate = useDashboard(state => state.mutate);
  const streaming = task.active_connections > 0;
  const readers = task.cache_job?.active_readers ?? 0;

  const copy = async () => {
    await navigator.clipboard.writeText(task.proxy_url);
    message.success('代理地址已复制');
  };

  return (
    <section className="scenario playback">
      <div className="scenario-title">
        <div>
          <small>场景 01</small>
          <h3>代理播放</h3>
          <p>把地址喂给播放器。seek 之后线程立刻改抓新位置周围的分片，优先让画面动起来。</p>
        </div>
        <Tag
          color={task.paused ? 'warning' : streaming ? 'processing' : 'default'}
          icon={task.paused ? <PauseCircleOutlined /> : <PlayCircleOutlined />}
        >
          {task.paused ? '已暂停' : streaming ? `${task.active_connections} 个连接` : '待连接'}
        </Tag>
      </div>

      <div className="proxy-link">
        <Typography.Text ellipsis title={task.proxy_url}>
          {task.proxy_url}
        </Typography.Text>
        <Button type="text" icon={<CopyOutlined />} title="复制地址" onClick={() => void copy()} />
        <Button
          type="text"
          icon={<LinkOutlined />}
          title="在新标签打开"
          onClick={() => window.open(task.proxy_url, '_blank')}
        />
      </div>

      <div className="scenario-metrics">
        <span>
          <b>{formatSpeed(task.current_speed_bps)}</b>
          <small>代理速度</small>
        </span>
        <span>
          <b>{formatBytes(task.bytes_served)}</b>
          <small>累计服务</small>
        </span>
      </div>

      <Button
        block
        icon={task.paused ? <PlayCircleOutlined /> : <PauseCircleOutlined />}
        onClick={() =>
          void mutate(() => (task.paused ? api.resume(task.task_id) : api.pause(task.task_id)))
        }
      >
        {task.paused ? '恢复代理' : '暂停代理'}
      </Button>

      {readers > 0 && (
        <div className="activity">
          <ThunderboltOutlined /> seek 优先调度生效中 · {readers} 路读取
        </div>
      )}
    </section>
  );
}
