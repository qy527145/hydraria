import { CopyOutlined, LinkOutlined, PauseOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { Button, message } from 'antd';
import type { TaskInfo } from '../../api/client';
import { api } from '../../api/client';
import { useDashboard } from '../../stores/dashboard';

/**
 * 场景 01 的操作条：把 /stream/<id> 交给播放器。
 * 只要有连接在读，调度器就切到低延迟策略，把线程压到 seek 位置周围。
 *
 * 这里是代理地址在卡片上的**唯一**入口 —— 三个动作（暂停/恢复、复制、打开）
 * 都围着它。地址本身不再单独占一行：那一行既不能点也没有别的信息量，只会让
 * 同一个 URL 在一张卡上出现两次。
 *
 * 三个按钮都带文字。只有图标的那个「打开」总要靠 tooltip 才认得出来，而它和
 * 「复制」是一对最容易混淆的动作，省下的那点宽度不值得。
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
        <em title={task.proxy_url}>{status}</em>
      </div>
      <div className="scenario-buttons">
        <Button
          size="small"
          type={task.paused ? 'primary' : 'default'}
          icon={task.paused ? <PlayCircleOutlined /> : <PauseOutlined />}
          title={task.paused ? '恢复代理，/stream 重新对外服务' : '暂停代理，/stream 返回 503（配置与缓存保留）'}
          onClick={() =>
            void mutate(() => (task.paused ? api.resume(task.task_id) : api.pause(task.task_id)))
          }
        >
          {task.paused ? '恢复' : '暂停'}
        </Button>
        <Button size="small" icon={<CopyOutlined />} title={`复制 ${task.proxy_url}`} onClick={() => void copy()}>
          复制
        </Button>
        <Button
          size="small"
          icon={<LinkOutlined />}
          title="在新标签打开代理地址"
          onClick={() => window.open(task.proxy_url, '_blank')}
        >
          打开
        </Button>
      </div>
    </div>
  );
}
