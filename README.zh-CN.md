# Hydraria

[English](README.md) · **简体中文**

> 用 Rust 写的高性能、低延迟、多线程 HTTP 流式代理 —— 内置 Web 面板。

Hydraria 把一个慢的、单一来源的 HTTP 下载，重写成多源并行抓取，再以稳定的短链把拼接好的字节流回任意标准 HTTP 客户端（浏览器、VLC、IINA、Aria2、`wget` 等）。整体只有一个静态链接的 Rust 二进制，前端面板通过 `rust-embed` 嵌进二进制，无需另外部署前端。

```
[ 用户 / Web UI ] --(1. 配置任务)--> [ 任务管理器 ] --(2. 生成短链)--> /stream/a1b2c3
                                                                    |
[ 播放器 / 下载器 ] <--(4. 单流输出)-- [ 代理引擎 ] <--(3. GET 短链)
       |                                                         |
       v                                            (内部：多线程多源拉取)
 [ 收到一条流 ]                                  [ origin 1, origin 2, origin 3 ... ]
```

## 亮点

- **多线程分块抓取** —— 每个请求被切成 `max_split` 大小的字节段，按 `max_threads` 并发拉取。调度器对在飞集合用滑动窗口策略，当前正在被消费的 chunk 始终在运行中（不饥饿）。
- **多源容错** —— 列出多个源 URL，chunks 在源之间轮询；某段失败会透明地在另一个源上重试。
- **Range / Seek 支持** —— 播放器发 `Range: bytes=…`（例如视频前向快进）时，Hydraria 从该偏移重新规划 chunks，不重下已下载部分。任何带 Range 的请求都返回 `206`（这是 Chrome `<video>` 判断是否可 seek 的依据）。
- **磁盘缓存** —— 任务级开关。字节落到一个稀疏文件里，文件以 URL 列表的 SHA-256 为键，1 MB 块完成度用位图跟踪。同一任务的第二次请求完全从盘上服务，0 上游流量。每次探测会用 ETag 校验；上游变更自动整盘清空重抓。
- **直通回退** —— 源不支持 Range 时自动落到单流直通模式，保证不可分段的源也能播。
- **背压感知流** —— chunk planner 走有界 `tokio::sync::mpsc` 管线，客户端读慢就会自然反压到上游抓取，不会爆内存。
- **任务级自定义请求头** —— 任务创建时一次设好 `Cookie`、`User-Agent`、`Referer` 等，所有上游 chunk 请求都会带上。
- **暂停 / 恢复 / 编辑** —— 任务可暂停（流返回 503，配置和缓存仍在），可通过 `PATCH /api/tasks/:id` 在线编辑，无需删了重建。
- **限速** —— 任务级和全局级令牌桶（任务上 `rate_limit_bps`，设置里 `global_rate_limit_bps`）。允许小幅突发，长程均值压在上限。
- **持久化** —— 任务上 `persist: true` 会落盘到 `~/.hydraria/tasks.json`（状态变化时每 ~5s 原子写一次），设置也一并持久化，下次启动自动恢复。
- **每源健康度** —— 任务跟踪每条 URL 的最新状态码、TTFB 延迟、当前吞吐、累计贡献字节、最近错误，全部在面板的"源状态看板"里展示。
- **实时迷你图** —— 全局吞吐和每任务吞吐按 ~1 Hz 采样，面板画 60 点的实时 SVG 曲线。
- **嵌入式面板** —— Web UI 编译进二进制（`rust-embed`），不需要额外部署静态目录。

## 架构

| 层 | 模块 | 职责 |
| --- | --- | --- |
| 核心引擎（数据面） | [src/engine.rs](src/engine.rs) | 探测上游、规划 chunk、滑动窗口并行抓取、按序拼回单条字节流。记录每源抓取结果。 |
| 缓存 | [src/cache.rs](src/cache.rs) | 按 URL 集合分桶的稀疏文件 + 1 MB 块位图，ETag 不匹配自动失效。 |
| 限速 | [src/ratelimit.rs](src/ratelimit.rs) | 令牌桶（任务级 + 全局）。 |
| 控制面 | [src/routes.rs](src/routes.rs), [src/models.rs](src/models.rs) | 任务管理器、REST API、短链生成、内存任务表、每任务健康度跟踪、吞吐采样、持久化。 |
| 应用层 | [src/main.rs](src/main.rs), [src/assets.rs](src/assets.rs) | CLI（`--bind`、`--cache-dir`、`--state-file`），axum 服务，根路径暴露面板。 |
| Web UI | [web/index.html](web/index.html) | 单文件面板（原生 JS，零构建步骤）：弹窗创建表单、网格/列表切换、搜索、源健康度面板、实时迷你图、表单实时校验、复制带✓ 反馈。 |

## 技术栈

| 关注点 | crate |
| --- | --- |
| 异步运行时 | `tokio` |
| HTTP 服务 / 路由 | `axum` 0.8 |
| HTTP 客户端 | `reqwest`（streaming + rustls） |
| 静态资源嵌入 | `rust-embed` |
| 并发原语 | `tokio::sync::mpsc`、`tokio::sync::Semaphore`、`parking_lot::RwLock` |
| 日志 | `tracing` + `tracing-subscriber` |
| CLI | `clap` |

## 构建与运行

### 从 crates.io 安装（推荐）

```bash
cargo install hydraria
```

会把 `hydraria` 装到 `~/.cargo/bin/`（请确认 `PATH` 包含它）。需要 Rust 1.85+。

### 从源码构建

```bash
git clone https://github.com/qy527145/hydraria.git
cd hydraria
cargo build --release
```

二进制位于 `./target/release/hydraria`。

### 运行

```bash
hydraria \
  --bind 127.0.0.1:9527 \
  --cache-dir ~/.hydraria/cache \
  --state-file ~/.hydraria/tasks.json
```

默认值：`--bind 127.0.0.1:9527`、`--cache-dir ~/.hydraria/cache`、
`--state-file ~/.hydraria/tasks.json`。

打开 `http://127.0.0.1:9527/` 访问面板。日志默认输出到 stdout，控制级别用 `RUST_LOG`，例如 `RUST_LOG=hydraria=debug,info`。

## API

### 控制面

#### `POST /api/tasks`

新建一个代理任务，返回短链。

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "urls": ["https://server1.com/file.mp4", "https://server2.com/file.mp4"],
    "max_threads": 16,
    "max_split": "5M",
    "cache": false,
    "headers": {
      "User-Agent": "Mozilla/5.0",
      "Cookie": "session=xxxx"
    }
  }'
```

响应：

```json
{ "task_id": "a1b2c3", "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3" }
```

`max_split` 可以是字节数，也可以是人类可读字符串：`"5M"`、`"512K"`、`"1G"` 等。

#### `GET /api/tasks`

列出所有活动任务及统计（已服务字节、活动连接、配置）。

#### `GET /api/tasks/:task_id`

获取单个任务的状态。

#### `DELETE /api/tasks/:task_id`

停止并移除任务，返回 `204`。

#### `PATCH /api/tasks/:task_id`

原地局部更新任务 —— `urls`、`max_threads`、`max_split`、`cache`、`headers`、`name` 的任意子集。返回更新后的 `TaskInfo`。

```bash
curl -X PATCH http://127.0.0.1:9527/api/tasks/a1b2c3 \
  -H 'content-type: application/json' \
  -d '{"max_threads": 32, "cache": true}'
```

#### `POST /api/tasks/:task_id/pause` 与 `…/resume`

暂停后 `GET /stream/:task_id` 返回 `503 Service Unavailable`，任务配置 + 缓存保持不变。恢复后翻回正常。两者都返回当前 `TaskInfo`。

#### `DELETE /api/tasks/:task_id/cache`

清空该任务在盘上的缓存（稀疏文件 + 位图 + meta），任务本身保留。返回 `204`。

#### `GET /api/settings` · `PUT /api/settings`

全局设置（目前是 `global_rate_limit_bps`，单位 B/s 或形如 `"10M"` 的人类字符串；`0`/null 表示不限）。PUT body 是局部更新。

#### `GET /api/global`

面板用的快照：总览统计、当前全局吞吐、近 60 点吞吐迷你图、缓存目录总占用。

### 数据面

#### `GET /stream/:task_id`

客户端消费的端点，行为像普通 HTTP 文件服务：

- 支持 `Range: bytes=start-end`，也支持后缀范围 `bytes=-N`。
- 部分读返回 `206 Partial Content`，全量读返回 `200 OK`。
- 转发上游探测得到的 `Content-Type`、`ETag`、`Last-Modified`、`Accept-Ranges`。
- 加上 `X-Hydraria-Task: <task_id>` 方便追踪。

也支持 `HEAD`，给下载前先探测的客户端用。

## 示例：丢给 VLC / IINA / Aria2

```bash
# 1. 创建任务
TASK=$(curl -s -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{"urls":["https://your-source/file.mp4"],"max_threads":16,"max_split":"5M"}' \
  | sed 's/.*"task_id":"\([^"]*\)".*/\1/')

# 2. 在任意客户端打开短链
vlc        "http://127.0.0.1:9527/stream/$TASK"
aria2c -x16 "http://127.0.0.1:9527/stream/$TASK"
wget        "http://127.0.0.1:9527/stream/$TASK"
```

客户端看到的是一条纯 HTTP/1.1 流。Hydraria 在内部把实际抓取扇出成多个并发 range 请求，分摊到所有配置的源。

## 分块流的工作原理

1. **探测** —— 客户端连上来时，Hydraria 先发一个 `HEAD` 拿便宜的元数据（Content-Type、Content-Length、ETag、Last-Modified），再发一个 `Range: bytes=0-0` `GET`。GET 的 206 是唯一可信的"源支持 Range"信号（很多 CDN 提供 Range 但 HEAD 里不带 `Accept-Ranges`）。
2. **缓存查找（若开启）** —— Hydraria 按 URL 列表 SHA-256 打开（或建立）缓存条目，meta 中 ETag/总大小与上游不一致就视为过期、原地清空。
3. **规划** —— 给定客户端的有效 range `[start, end]`，引擎切成 `max_split` 大小的子段。
4. **拉取** —— 每个子段在滑动窗口调度器下并发抓取。同一个 task 既按序消费 chunk、又在前一个完成时立刻 spawn 下一个，保证 `max_threads` 个 chunk 在飞、当前消费 chunk 始终在运行集合中（普通信号量会和有界 per-chunk channel 形成死锁）。
5. **拼接** —— 序列化 task 按规划顺序消费每个 chunk 的 channel，转发给客户端。有界 channel 在客户端读慢时反压回 fetcher。
6. **回写缓存** —— 开启缓存的任务，每段从上游收到的字节都按绝对偏移 `pwrite` 到稀疏文件。块完成度按"每块字节计数器"跟踪，整块覆盖完后翻位图（位图随后 fsync 落盘）。
7. **重试** —— 某段中途失败，引擎换轮询表里的下一个 URL 重试该段。

## 配置参考

| 字段 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `urls` | `string[]` | 必填 | 源 URL。每个源都要能提供同一份内容。 |
| `max_threads` | `int` | `8` | 单条客户端连接上的最大并发 chunk fetcher 数。 |
| `max_split` | `int` 或人类字符串 | `5M` | 每段大小。更小 = 容错和 seek 更快，但开销更高。 |
| `cache` | `bool` | `false` | 任务级磁盘缓存开关。 |
| `headers` | `object<string,string>` | `{}` | 附加到每个上游请求的请求头。 |
| `name` | `string?` | `null` | 面板里展示的可选别名。 |

## 项目结构

```
.
├── Cargo.toml
├── src
│   ├── main.rs        # CLI + axum 启动
│   ├── lib.rs         # 模块入口
│   ├── models.rs      # TaskConfig、TaskEntry、AppState、GlobalSettings、UrlHealth
│   ├── engine.rs      # 多线程分块抓取 + range 解析 + 健康度钩子
│   ├── cache.rs       # 稀疏文件 + 位图缓存，按 ETag 鉴别
│   ├── ratelimit.rs   # 令牌桶（任务 + 全局）
│   ├── routes.rs      # axum 路由（控制面 + 数据面）
│   ├── assets.rs      # rust-embed 静态资源 handler
│   └── error.rs       # ProxyError + IntoResponse
└── web
    └── index.html     # 面板（编译时嵌入二进制）
```

## 更新日志

### v0.1.6

- **修复调度器饥饿问题** —— 慢客户端（如浏览器单连接下载）场景下，主循环 `biased select!` 优先 drain `rx` 导致 `release_rx` 长时间不被 poll，所有上游 URL 集体显示 0 B/s 直到已缓冲数据耗尽。修复后在每送完一项数据后非阻塞 drain release 事件并立即补 spawn，保证上游始终热起来。
- **减少线程偷懒** —— chunk channel buffer 从固定 4 调整为按 split 大小动态计算（~split/16 KiB，封顶 512），避免 fetcher 在 channel 满后被反压、表现为"0 B/s on volumes 2+"；同时调整调度器的 strict pass 改为按 plan 序而非跨卷打散，把并行集中在 serializer 即将消费的前几卷，避免预取远期卷浪费带宽。
- **插件分卷交互优化** —— 创建/编辑表单中的插件 + 分卷面板交互打磨。
- **缓存识别优化** —— 进一步收敛 cache 命中判断的边界条件。

### v0.1.5

- **双层并发调度** —— 引入 `max_per_volume` 软上限，按"每卷连接数 + 任务总线程数"两级调度，单 IP/单 URL 不再被堆满；与卷间公平分配并存。配套增加 strict/overflow 两遍 spawn 策略，确保 `max_threads` 始终用满。
- **源看板增强** —— URL health 新增 `in_flight_requests`、`volume_size`，dashboard 实时显示每路在飞请求数和对应卷大小。
- **HEAD 探测跳过表** —— 任务级共享 `head_unsupported` 集合，已知拒绝 HEAD 的 URL 直接走 1-byte Range GET，省一轮往返。
- **全局缓存清理** —— `DELETE /api/cache` 一键清空所有任务的本地缓存。

### v0.1.4

- **粘性弹窗操作栏** —— 长表单底部按钮 sticky，随时可关闭。
- **JSON 导入还原插件配置** —— 配置导入流程修复，插件段不再丢失。

### v0.1.3

- **插件系统 + ChaCha20 解密插件** —— 字节出站管线可挂多个 transform，按反向应用；自带 ChaCha20-Poly1305 解密插件，支持加密回源 + 客户端明文播放。
- **修复 cache 并发写竞态** —— 多 fetcher 同时写同一 block 时的位图竞态修复。

### v0.1.2 / v0.1.1

- **7 项核心优化** —— cache 跨卷、断点续传、按权重选源、克隆/导入导出、cache 热力图、`download` CLI。
- **拖拽重排分卷 + 自动文件名 LCP 合并** —— UI 支持拖拽调整卷顺序；多卷任务的输出文件名取各卷文件名的最长公共前缀。
- **跨卷预热调度** —— 卷边界前预连下一卷，避免切换时 TCP 建连延迟。

## Roadmap

- 缓存目录的 LRU 淘汰（目前无界增长）。
- 非 localhost 绑定时给控制面 API 加鉴权 / token。
- `download` CLI 子命令：直接驱动引擎落到本地文件。
- 探测结果缓存：缓存命中的请求跳过上游 HEAD。

## 许可证

MIT（或自行调整后发布）。
