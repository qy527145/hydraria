import { message } from 'antd';
import { create } from 'zustand';
import { api, type GlobalState, type PluginEntry, type TaskInfo } from '../api/client';

const POLL_INTERVAL_MS = 1_000;

interface DashboardState {
  tasks: TaskInfo[];
  global: GlobalState | null;
  plugins: PluginEntry[];
  loading: boolean;
  error: string | null;
  refresh: () => Promise<void>;
  loadPlugins: () => Promise<void>;
  /**
   * 执行一个写操作，成功后立刻刷新，让卡片上的状态马上跟上。
   *
   * 失败必须**说出来**。调用点几乎都是 `void mutate(...)`，异常抛出去就变成
   * 一个没人处理的 rejected promise —— 用户按下「缓存整个文件」、源站连不上，
   * 界面却毫无反应，看起来像按钮坏了。这里统一弹一条错误并记进 store。
   */
  mutate: (action: () => Promise<unknown>) => Promise<void>;
}

const describe = (error: unknown) => (error instanceof Error ? error.message : String(error));

export const useDashboard = create<DashboardState>((set, get) => ({
  tasks: [],
  global: null,
  plugins: [],
  loading: true,
  error: null,
  refresh: async () => {
    try {
      const [tasks, global] = await Promise.all([api.tasks(), api.global()]);
      set({ tasks, global, loading: false, error: null });
    } catch (error) {
      set({ loading: false, error: describe(error) });
    }
  },
  loadPlugins: async () => {
    try {
      set({ plugins: await api.plugins() });
    } catch (error) {
      set({ error: describe(error) });
    }
  },
  mutate: async action => {
    try {
      await action();
      set({ error: null });
    } catch (error) {
      const text = describe(error);
      message.error(text);
      set({ error: text });
    } finally {
      await get().refresh();
    }
  },
}));

/**
 * 秒级轮询。用「上一轮结束后再排下一轮」而不是固定 setInterval，
 * 后端卡顿时不会堆积请求；标签页隐藏时暂停，回到前台立即补一次。
 */
export function startPolling() {
  let stopped = false;
  let timer: number | undefined;

  const tick = async () => {
    if (stopped) return;
    if (!document.hidden) await useDashboard.getState().refresh();
    if (!stopped) timer = window.setTimeout(() => void tick(), POLL_INTERVAL_MS);
  };

  const onVisible = () => {
    if (!document.hidden) void useDashboard.getState().refresh();
  };

  void tick();
  document.addEventListener('visibilitychange', onVisible);

  return () => {
    stopped = true;
    if (timer !== undefined) window.clearTimeout(timer);
    document.removeEventListener('visibilitychange', onVisible);
  };
}
