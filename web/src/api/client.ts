/**
 * 后端 API 的类型定义与调用封装。
 * 字段名与 Rust 侧的 serde 结构一一对应，改后端记得同步这里。
 */
export type RateAlgorithm = 'token_bucket' | 'sliding_window';
export type Disposition = 'auto' | 'inline' | 'attachment';
export type CacheJobState = 'idle' | 'running' | 'paused' | 'done' | 'failed';

export interface TaskPluginConfig {
  id: string;
  enabled: boolean;
  config: Record<string, unknown>;
}

export interface TaskConfig {
  /** 外层是分卷（顺序拼接），内层是同一卷的镜像 URL。 */
  volumes: string[][];
  max_threads: number;
  max_per_volume: number;
  /** 分片大小，0 表示自动。 */
  max_split: number;
  /** 代理播放时是否写入持久缓存（决定播放能否与缓存场景共享分片）。 */
  cache: boolean;
  headers: Record<string, string>;
  name: string | null;
  output_filename: string | null;
  auto_filename: boolean;
  rate_limit_bps: number;
  rate_limit_algorithm: RateAlgorithm;
  persist: boolean;
  plugins: TaskPluginConfig[];
  content_disposition: Disposition;
  /** 任务级域名映射，与全局设置里的那份取并集，同名以任务级为准。 */
  host_mappings: HostMapping[];
}

/** 磁盘上这份缓存的统计，由 CacheStore 汇总。 */
export interface CacheStats {
  key: string;
  total_size: number;
  bytes_cached: number;
  blocks_cached: number;
  blocks_total: number;
  hits: number;
  misses: number;
  etag: string | null;
  /** 位图降采样，每项是该区间已落盘的百分比（0-100）。 */
  bitmap_summary: number[];
}

/** 任务级缓存协调器的状态：整文件补齐进度 + 当前播放读取者数量。 */
export interface CacheJobInfo {
  state: CacheJobState;
  error: string | null;
  total_bytes: number;
  done_bytes: number;
  current_speed_bps: number;
  speed_samples: number[];
  threads: number;
  active_readers: number;
  bitmap_summary: number[];
}

export interface UrlHealth {
  url: string;
  last_status: number | null;
  last_error: string | null;
  last_latency_ms: number | null;
  bytes_contributed: number;
  successful_requests: number;
  failed_requests: number;
  current_speed_bps: number;
  last_used_at: number | null;
  in_flight_requests: number;
  volume_size: number | null;
}

export interface TaskInfo {
  task_id: string;
  proxy_url: string;
  config: TaskConfig;
  created_at: number;
  /** 最后一次配置编辑的 Unix 秒；从未编辑过时等于 created_at。列表按它逆序。 */
  updated_at: number;
  bytes_served: number;
  active_connections: number;
  paused: boolean;
  cache: CacheStats | null;
  cache_job: CacheJobInfo | null;
  url_health: UrlHealth[];
  current_speed_bps: number;
  speed_samples: number[];
}

/**
 * 一条域名映射：只改 TCP 连到哪儿，URL / Host 头 / TLS SNI 都保持原样，
 * 所以带签名的地址不会因为换了出口而失效。
 */
export interface HostMapping {
  /** 原域名、原 IP，或 `*.example.com` 形式的通配后缀。 */
  from: string;
  /** 目标域名或 IP，可带 `:端口`。 */
  to: string;
  enabled: boolean;
}

/** `POST /api/hostmap/resolve` 的响应：这个 host 到底会被连到哪儿。 */
export interface HostResolution {
  host: string;
  /** 命中的映射目标；null = 没有规则命中，走的是正常 DNS。 */
  mapped_to: string | null;
  addresses: string[];
  error: string | null;
  /** 检测到的代理环境变量名。命中映射的请求会自动绕开代理，这里只是告知。 */
  proxy_env: string | null;
}

/**
 * 测试一份规则时，这份规则是「全部规则」还是「盖在全局之上的一层」。
 *
 * 差别在删掉一条规则之后才显出来：全局面板里删掉一条再测，必须报「没有规则
 * 命中」，按任务级的合并语义则会把刚删掉的那条从已保存的全局表里又捞回来。
 */
export type HostMapScope = 'global' | 'task';

export interface GlobalSettings {
  global_rate_limit_bps: number;
  global_rate_limit_algorithm: RateAlgorithm;
  plugin_globals: Record<string, unknown>;
  download_dir?: string | null;
  host_mappings: HostMapping[];
}

export interface GlobalState {
  settings: GlobalSettings;
  /** 所有缓存填充任务的合计拉取速率。与 current_speed_bps 是方向相反的两条流。 */
  cache_fill_speed_bps: number;
  current_speed_bps: number;
  speed_samples: number[];
  cache_total_bytes: number;
  task_count: number;
  active_connections: number;
  bytes_served_total: number;
}

export interface ConfigField {
  key: string;
  label: string;
  kind: 'text' | 'text_area' | 'hex' | 'path' | 'dir_path' | 'number' | 'size' | 'boolean' | 'select';
  hint?: string;
  required: boolean;
  generate_random_bytes?: number;
  default_filename?: string;
  path_mode?: string;
  options?: { value: string; label: string }[];
}

/**
 * `GET /api/plugins` 的一项。Rust 侧用 `#[serde(flatten)]` 把插件元数据和当前
 * 生效的全局配置拍平成一个对象，所以这里没有 `info` 这层包装。
 */
export interface PluginEntry {
  id: string;
  name: string;
  description: string;
  global_fields: ConfigField[];
  task_fields: ConfigField[];
  /** 正向工具（如加密打包）的参数表单描述。 */
  forward_fields: ConfigField[];
  has_forward: boolean;
  default_global: unknown;
  default_task: Record<string, unknown> | null;
  /** 当前全局配置；用户没改过时等于 default_global。 */
  global: unknown;
}

export interface ProbeResult {
  detected_filename: string | null;
  suggested_filename: string | null;
  total_size: number | null;
  content_type: string | null;
  accepts_ranges: boolean;
}

export interface CreatedTask {
  task_id: string;
  proxy_url: string;
  /** 只在请求里带了 start_cache 时出现。 */
  cache_started?: boolean;
  cache_error?: string;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const headers = new Headers(init?.headers);
  if (init?.body) headers.set('Content-Type', 'application/json');
  const response = await fetch(path, { ...init, headers });
  if (!response.ok) {
    // 后端错误统一是 { error: "…" }；解析失败就退回状态码。
    let text = `请求失败 (${response.status})`;
    try {
      const body = (await response.json()) as { error?: string };
      if (body.error) text = body.error;
    } catch {
      /* 保留状态码文案 */
    }
    throw new Error(text);
  }
  if (response.status === 204) return undefined as T;
  const type = response.headers.get('content-type') ?? '';
  return type.includes('json') ? (response.json() as Promise<T>) : (response.text() as Promise<T>);
}

export const api = {
  tasks: () => request<TaskInfo[]>('/api/tasks'),
  global: () => request<GlobalState>('/api/global'),
  settings: () => request<GlobalSettings>('/api/settings'),
  plugins: () => request<PluginEntry[]>('/api/plugins'),

  create: (config: TaskConfig) =>
    request<CreatedTask>('/api/tasks', { method: 'POST', body: JSON.stringify(config) }),
  update: (id: string, config: TaskConfig) =>
    request<TaskInfo>(`/api/tasks/${id}`, { method: 'PATCH', body: JSON.stringify(config) }),
  remove: (id: string) => request<void>(`/api/tasks/${id}`, { method: 'DELETE' }),

  // 场景 01：代理播放的开关
  pause: (id: string) => request<TaskInfo>(`/api/tasks/${id}/pause`, { method: 'POST' }),
  resume: (id: string) => request<TaskInfo>(`/api/tasks/${id}/resume`, { method: 'POST' }),

  // 场景 02：整文件缓存。start 幂等，pause 只停缓存、不影响正在播放的连接。
  cacheStart: (id: string) => request<CacheJobInfo>(`/api/tasks/${id}/cache`, { method: 'POST' }),
  cachePause: (id: string) => request<CacheJobInfo>(`/api/tasks/${id}/cache/pause`, { method: 'POST' }),
  cacheClear: (id: string) => request<void>(`/api/tasks/${id}/cache`, { method: 'DELETE' }),
  clearAllCache: () => request<{ bytes_freed: number }>('/api/cache', { method: 'DELETE' }),

  saveSettings: (settings: Partial<GlobalSettings>) =>
    request<GlobalSettings>('/api/settings', { method: 'PUT', body: JSON.stringify(settings) }),
  probe: (volumes: string[][], headers: Record<string, string>, host_mappings: HostMapping[] = []) =>
    request<ProbeResult>('/api/probe', {
      method: 'POST',
      body: JSON.stringify({ volumes, headers, host_mappings }),
    }),
  /**
   * 诊断：这个域名最终会被连到哪儿。
   *
   * 走 POST 而不是 GET，是为了能把**编辑中**（还没保存）的规则一起发过去。
   * 只认已保存规则的话，「改完 target 再测一次」报的永远是上一次的结果 ——
   * 而按下测试的时机，恰恰是规则还没保存的时候。
   */
  resolveHost: (host: string, opts: { scope: HostMapScope; mappings?: HostMapping[]; taskId?: string } ) =>
    request<HostResolution>('/api/hostmap/resolve', {
      method: 'POST',
      body: JSON.stringify({
        host,
        scope: opts.scope,
        mappings: opts.mappings,
        task_id: opts.taskId,
      }),
    }),
  savePluginGlobal: (id: string, config: unknown) =>
    request<void>(`/api/plugins/${id}/global`, { method: 'PUT', body: JSON.stringify(config) }),
};
