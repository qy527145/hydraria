import { CopyOutlined, LinkOutlined, PauseOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { Button, message } from 'antd';
import type { TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';

/**
 * 场景 01 的操作条：把 /stream/<id> 交给播放器。
 * 只要有连接在读，调度器就切到低延迟策略，把线程压到 seek 位置周围。
 *
 * 这里只放按钮和一行活体状态——解释性的文字属于手册，不属于每张卡片重复 N 遍。
 */
export default function PlaybackSection({ task }: { task: TaskInfo }) {
  const mutate = useDashboard(state => state.mutate);
  const readers = task.cache_job?.active_readers ?? 0;

  const copy = async () => {
    await navigator.clipboard.writeText(task.proxy_url);
    message.success('代理地址已复制');
  };

  const status = task.paused
    ? '已暂停'
    : readers > 0
      ? `${readers} 路读取 · seek 优先调度`
      : task.active_connections > 0
        ? `${task.active_connections} 个连接`
        : '待播放器连接';

  return (
    <div className="scenario-bar playback">
      <div className="scenario-label">
        <PlayCircleOutlined />
        <span>代理播放</span>
        <em>{status}</em>
      </div>
      <div className="scenario-buttons">
        <Button
          size="small"
          type={task.paused ? 'primary' : 'default'}
          icon={task.paused ? <PlayCircleOutlined /> : <PauseOutlined />}
          onClick={() =>
            void mutate(() => (task.paused ? api.resume(task.task_id) : api.pause(task.task_id)))
          }
        >
          {task.paused ? '恢复' : '暂停'}
        </Button>
        <Button size="small" icon={<CopyOutlined />} onClick={() => void copy()}>
          复制地址
        </Button>
        <Button
          size="small"
          icon={<LinkOutlined />}
          title="在新标签打开"
          onClick={() => window.open(task.proxy_url, '_blank')}
        />
      </div>
    </div>
  );
}
