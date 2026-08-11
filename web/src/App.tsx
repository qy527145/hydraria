import {
  CloudServerOutlined,
  PlusOutlined,
  SearchOutlined,
  SettingOutlined,
  ThunderboltOutlined,
} from '@ant-design/icons';
import { Alert, Button, Empty, Input, Layout, Space, Spin, Statistic } from 'antd';
import { useEffect, useMemo, useState } from 'react';
import type { TaskConfig, TaskInfo } from './api/client';
import TaskFormModal from './components/forms/TaskFormModal';
import PluginsModal from './components/plugins/PluginsModal';
import SettingsModal from './components/settings/SettingsModal';
import TaskCard from './components/tasks/TaskCard';
import { startPolling, useDashboard } from './stores/dashboard';
import { formatBytes, formatSpeed } from './utils/format';

export default function App() {
  const { tasks, global, plugins, loading, error, loadPlugins } = useDashboard();
  const [query, setQuery] = useState('');
  const [formOpen, setFormOpen] = useState(false);
  const [editing, setEditing] = useState<TaskInfo | null>(null);
  const [seed, setSeed] = useState<TaskConfig | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [pluginsOpen, setPluginsOpen] = useState(false);

  useEffect(() => startPolling(), []);

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return tasks;
    return tasks.filter(
      task =>
        (task.config.name ?? '').toLowerCase().includes(q) ||
        task.task_id.includes(q) ||
        task.config.volumes.flat().some(url => url.toLowerCase().includes(q)),
    );
  }, [tasks, query]);

  // 新建既用于空白表单，也用于「克隆配置」——后者带一份种子配置进来。
  const openCreate = (config: TaskConfig | null = null) => {
    setEditing(null);
    setSeed(config);
    setFormOpen(true);
  };
  const openEdit = (task: TaskInfo) => {
    setEditing(task);
    setSeed(null);
    setFormOpen(true);
  };

  return (
    <Layout className="app-shell">
      <Layout.Header className="app-header">
        <div className="brand">
          <span className="brand-logo">
            <CloudServerOutlined />
          </span>
          <span>
            <b>HYDRARIA</b>
            <small>SCENARIO-AWARE STREAMING</small>
          </span>
        </div>
        <div className="header-stats">
          <Statistic title="实时吞吐" value={formatSpeed(global?.current_speed_bps ?? 0)} />
          <Statistic title="活跃连接" value={global?.active_connections ?? 0} />
          <Statistic title="持久缓存" value={formatBytes(global?.cache_total_bytes ?? 0)} />
        </div>
        <Space>
          <Button
            icon={<ThunderboltOutlined />}
            onClick={() => {
              void loadPlugins();
              setPluginsOpen(true);
            }}
          >
            插件
          </Button>
          <Button icon={<SettingOutlined />} onClick={() => setSettingsOpen(true)}>
            设置
          </Button>
          <Button type="primary" icon={<PlusOutlined />} onClick={() => openCreate()}>
            新建任务
          </Button>
        </Space>
      </Layout.Header>

      <Layout.Content>
        <main className="page">
          {/* 仪表盘是天天盯的东西，不是落地页：标题和介绍文字换成一条工具栏，
              省下的整屏高度直接给任务卡片。 */}
          <div className="toolbar">
            <Input
              allowClear
              prefix={<SearchOutlined />}
              placeholder="搜索任务名称、ID 或 URL"
              value={query}
              onChange={event => setQuery(event.target.value)}
            />
            <span className="toolbar-count">
              {query.trim() && filtered.length !== tasks.length
                ? `匹配 ${filtered.length} / ${tasks.length}`
                : `${tasks.length} 个任务 · 按最近编辑排序`}
            </span>
          </div>

          {error && <Alert type="error" showIcon message={error} />}

          {loading ? (
            <Spin className="loading" size="large" />
          ) : filtered.length ? (
            <div className="task-list">
              {filtered.map(task => (
                <TaskCard
                  key={task.task_id}
                  task={task}
                  onEdit={openEdit}
                  onClone={source =>
                    openCreate({
                      ...source.config,
                      name: `${source.config.name || source.task_id} 副本`,
                    })
                  }
                />
              ))}
            </div>
          ) : (
            <Empty
              description={tasks.length ? '没有匹配任务' : '尚无任务，创建代理短链后即可分别控制播放与缓存'}
            >
              <Button type="primary" onClick={() => openCreate()}>
                新建任务
              </Button>
            </Empty>
          )}
        </main>
      </Layout.Content>

      <TaskFormModal open={formOpen} task={editing} seed={seed} onClose={() => setFormOpen(false)} />
      <SettingsModal open={settingsOpen} onClose={() => setSettingsOpen(false)} global={global} />
      <PluginsModal open={pluginsOpen} onClose={() => setPluginsOpen(false)} plugins={plugins} />
    </Layout>
  );
}
