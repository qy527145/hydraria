# Hydraria REST API 参考

> 面板能配的每一样东西，API 都能配。这份文档是那句话的完整版：任务的每个字段、每个
> 端点、每种错误。面板自己也只用这些接口，没有私有通道。
>
> 配套文档：[使用手册](MANUAL.md) · [README](../README.zh-CN.md)

## 目录

1. [约定](#1-约定)
2. [任务配置字段全表](#2-任务配置字段全表)
3. [完整请求示例](#3-完整请求示例)
4. [任务端点](#4-任务端点)
5. [任务响应（TaskInfo）字段](#5-任务响应taskinfo字段)
6. [辅助端点](#6-辅助端点)
7. [数据平面](#7-数据平面)
8. [脚本配方](#8-脚本配方)

---

## 1. 约定

| 项 | 值 |
|---|---|
| 基地址 | `http://127.0.0.1:9527`（`--bind` 决定） |
| 请求体 | JSON，`Content-Type: application/json` |
| 错误体 | `{"error": "..."}` |
| 成功 | `200`（带响应体）/ `204`（无内容） |
| 认证 | **没有**。控制面裸奔，所以默认只绑 `127.0.0.1`；要对外提供服务，请套一层反向代理并自己加鉴权 |

出错时的状态码：

| 状态 | 含义 |
|---|---|
| `404` | 任务 ID 不存在 |
| `416` | `Range` 头无法满足（数据平面） |
| `500` | 参数不合法（字段名打错、URL 形状不对、映射规则或插件配置有问题…）；错误文字里会写清是哪一项 |
| `502` | 上游不可用 / 探测失败 |
| `503` | 任务被暂停，或上游在限流 |

**大小类字段**（`max_split`、`rate_limit_bps`）既收字节数，也收 `"5M"` / `"512K"` / `"1G"`
这类字符串。

**未知字段直接报错，不会被静默忽略。** `max_treads` 打错一个字母会得到

```json
{"error":"internal: unknown field(s): max_treads — accepted fields are: volumes, max_threads, …"}
```

而不是一个「看起来建成了、其实在用默认并发跑」的任务。

---

## 2. 任务配置字段全表

一个任务的全部可配项。**只有 URL 是必填的**，其余不写就用下面这些默认值。

| 字段 | 类型 | 默认 | 说明 |
|---|---|---|---|
| `volumes` | `string[][]` | — | **必填**。二维：外层是分卷（按顺序拼接成一个文件），内层是该卷的镜像地址（内容相同、可互换）。既分卷又镜像时只能用这个字段，见 [URL 怎么写](#url-怎么写)；只有一层时可以用 `url` / `urls` / `uri` / `uris` 简写 |
| `max_per_volume` | int | `4` | 单个分卷上最多几个并发请求。按源站的单 IP / 单对象连接限制填，这是**唯一**需要判断的并发数字 |
| `max_threads` | int | 派生 | 总线程数 = `max_per_volume` × 卷数，上限 128。**传了会被忽略并重算** —— 两个数字各配一份时总有一个会输 |
| `max_split` | int \| string | `0` | 单次上游 Range 请求的字节上限。`0` = 自动（下载按线程均分剩余量，播放贴着读头铺 2 MiB 小分片）。手填 ≥ `64K`，只在源站对单次 Range 长度有硬要求时才需要 |
| `cache` | bool | `false` | 代理播放时是否写入持久缓存。开启后播放与「整文件缓存」共享同一份磁盘文件，已落盘的区间不会重复下载。（面板的新建表单默认勾着它；API 不写则是 `false`） |
| `persist` | bool | `true` | 重启后自动恢复该任务。默认开 —— 短链一旦贴进播放器、播放列表或脚本，就不该因为重启变成死链。临时任务传 `false` |
| `headers` | `{string: string}` | `{}` | 每个上游请求都会带上。常用 `Cookie` / `Referer` / `User-Agent` |
| `name` | string \| null | `null` | 任务名，仅用于面板显示与搜索。全空白等同于 `null` |
| `output_filename` | string \| null | `null` | 代理响应 `Content-Disposition` 里的文件名 |
| `auto_filename` | bool | `true` | `true` = 用运行时探测到的文件名（探不到时回落 `output_filename` → `name`）；`false` = 只用 `output_filename`（为空则不发 `Content-Disposition`） |
| `rate_limit_bps` | int \| string | `0` | 单任务限速，`0` = 不限 |
| `rate_limit_algorithm` | `"token_bucket"` \| `"sliding_window"` | `"token_bucket"` | 令牌桶允许短突发（起播更快）；滑动窗口每一秒都严格不超标 |
| `content_disposition` | `"auto"` \| `"inline"` \| `"attachment"` | `"auto"` | 浏览器打开短链时的行为。`auto` 跟随源站；`inline` 强制预览（并把源站笼统的 `application/octet-stream` 按文件名猜成更具体的 MIME）；`attachment` 强制下载。播放器和下载工具不受影响 |
| `host_mappings` | `HostMapping[]` | `[]` | 任务级域名映射，与全局那份取并集，`from` 撞车时以任务级为准 |
| `plugins` | `TaskPlugin[]` | `[]` | 字节变换插件（如 ChaCha20 解密），按**正向顺序**存放，代理方向反向应用 |

### `HostMapping`

等价于 `curl --resolve`：**只改 TCP 连到哪儿**，URL、`Host` 头、TLS SNI 全部保持原样，
所以签名 URL 不会因此失效。

| 字段 | 类型 | 说明 |
|---|---|---|
| `from` | string | URL 里写的那个域名 / IP，或 `*.example.com` 形式的通配后缀（精确匹配优先，多个通配取最长）。**不能带端口** —— 映射是按主机来的，端口来自 URL |
| `to` | string | 目标 IP 或域名，**可带 `:端口`**（`1.2.3.4:8443`、`backup.example.com:8443`、IPv6 写 `[::1]:8443`）。端口什么时候说了算见下 |
| `enabled` | bool | 默认 `true`。停用的规则保留但不参与解析 |

命中映射的请求会**自动绕开代理** —— 否则域名由代理去解析，映射静默失效。

**`to` 里的端口什么时候生效**，取决于原地址是域名还是裸 IP（两条实现路径不同）：

| 原地址 | 原 URL | 映射 `to` | 实际连到 | 为什么 |
|---|---|---|---|---|
| 域名 | `http://cdn.example.com/f` | `1.2.3.4:8443` | `1.2.3.4:8443` | 域名走 DNS 解析器，URL 没写端口时用映射里的 |
| 域名 | `http://cdn.example.com:8000/f` | `1.2.3.4:8443` | `1.2.3.4:`**`8000`** | **原 URL 的显式端口优先**，映射里的被忽略（只有主机被换掉） |
| 裸 IP | `http://10.0.0.2:8000/f` | `192.168.1.9:8080` | `192.168.1.9:`**`8080`** | 裸 IP 走 URL 改写那条路，映射里的端口**总是**赢 |

所以：**想靠映射换端口，原地址是域名时就别在 URL 上写端口**；原地址是裸 IP 则无所谓。
不确定的话直接问 [`POST /api/hostmap/resolve`](#post-apihostmapresolve--诊断编辑中的映射)，或者看
日志里那行 `host map: …`。

（这三行是 `tests/hostmap_port_e2e.rs` 里的断言 —— 真起了两个监听端口，看请求最后落在
谁身上。）

### `TaskPlugin`

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 插件 id，`GET /api/plugins` 可列出（内置：`chacha20`） |
| `enabled` | bool | 停用的槽位配置保留但不生效，也不做校验 |
| `config` | object | 插件自己的字段，形状见 `GET /api/plugins` 的 `task_fields` |

`chacha20` 的 `config`：`{"secret": "<88 个十六进制字符>"}`（32 字节 key + 12 字节
nonce 拼接；旧格式的 `{"key": "<64>", "nonce": "<24>"}` 也认）。

**启用的插件在建任务时就会被校验**：密钥写错当场报错，而不是等到有客户端来播时才
变成 500。

---

## 3. 完整请求示例

把每个字段都写满的一份请求 —— 可以直接删掉不需要的行当模板用：

```bash
curl -X POST 'http://127.0.0.1:9527/api/tasks?start_cache=1' \
  -H 'content-type: application/json' \
  -d '{
    "volumes": [
      ["https://cdn1.example.com/movie.part01", "https://cdn2.example.com/movie.part01"],
      ["https://cdn1.example.com/movie.part02"]
    ],

    "max_per_volume": 6,
    "max_split": "8M",

    "cache": true,
    "persist": true,

    "headers": {
      "User-Agent": "Mozilla/5.0",
      "Referer": "https://example.com/play/123",
      "Cookie": "session=xxxx"
    },

    "name": "4K 蓝光原盘",
    "output_filename": "movie.mkv",
    "auto_filename": false,

    "rate_limit_bps": "2M",
    "rate_limit_algorithm": "sliding_window",
    "content_disposition": "attachment",

    "host_mappings": [
      { "from": "cdn1.example.com", "to": "10.0.0.1:8443", "enabled": true },
      { "from": "*.cdn2.example.com", "to": "backup.example.com", "enabled": false }
    ],

    "plugins": [
      {
        "id": "chacha20",
        "enabled": true,
        "config": {
          "secret": "00112233445566778899aabbccddeeff00112233445566778899aabbccddeeff000102030405060708090a0b"
        }
      }
    ]
  }'
```

```json
{
  "task_id": "a1b2c3",
  "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3",
  "cache_started": true
}
```

> 这份示例的字段集合与 `tests/api_e2e.rs` 里
> `every_task_config_field_is_reachable_through_the_api` 用的那份**逐字对应**（只把域名
> 换成了永远解析不出来的 `.invalid`）：那个用例把它发进去、读回来、逐字段比对。所以
> 这里的字段名和形状不会和实现走散 —— 少接一个字段，测试就红了。

绝大多数场景其实只要一行：

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' -d '{"url": "https://cdn/movie.mp4"}'
```

---

## 4. 任务端点

### `POST /api/tasks` — 创建任务

请求体就是一份[任务配置](#2-任务配置字段全表)：**只有 URL 必填**，其余字段（自定义
请求头、并发、限速、域名映射、插件……）都写在**同一个 JSON 对象**里，不写就走默认值。
字段全表见 [§2](#2-任务配置字段全表)，把每个字段都写满的示例见 [§3](#3-完整请求示例)。

#### URL 怎么写

Hydraria 的源是**二维**的：外层是**分卷**（按顺序拼接成一个文件），内层是同一卷的
**镜像**（可互换的副本，内容相同）。两层里有一层只有一个时可以用简写；`uri` / `uris`
是 aria2 的叫法，一并接受：

| 请求体 | 卷 × 镜像 | 含义 |
|---|---|---|
| `{"url": "https://a/f.mp4"}` | 1 × 1 | 一个文件、一个源 |
| `{"urls": ["https://a/f.mp4", "https://b/f.mp4"]}` | 1 × 2 | 一个文件、两个**镜像** |
| `{"volumes": ["https://a/f", "https://b/f"]}` | 1 × 2 | 扁平写法，同上 |
| `{"volumes": [["https://a/p1"], ["https://a/p2"]]}` | 2 × 1 | 两个**分卷**，顺序拼接 |
| `{"volumes": [["https://a/p1", "https://b/p1"], ["https://a/p2", "https://b/p2"]]}` | 2 × 2 | 两个分卷，**每卷各两个镜像** |

**既有分卷又有镜像就用最后那种写法** —— 二维的 `volumes` 是唯一能同时表达两层的形式，
简写表达不了。一部电影切成两段、每段都放在两个 CDN 上：

```jsonc
{
  "volumes": [
    // 卷 1 = 文件的前半段，两个地址任选其一（内容相同）
    ["https://cdn-a.example.com/movie.part01", "https://cdn-b.example.com/movie.part01"],
    // 卷 2 = 后半段，同样两个镜像
    ["https://cdn-a.example.com/movie.part02", "https://cdn-b.example.com/movie.part02"]
  ],
  "max_per_volume": 4
}
```

这个任务对客户端表现为**一个**完整文件（两卷按顺序拼接），并发是 4 × 2 卷 = 8 条
线程，每卷内部在自己的两个镜像之间轮转，某个镜像挂了自动换另一个。

两层的顺序含义完全不同，别弄反：

- **卷的顺序 = 文件的字节顺序。** 写颠倒了，拼出来的文件是坏的（而且不会报错 ——
  服务端没法知道哪一段该在前面）。
- **镜像的顺序只是偏好。** 任选一个都得到同样的字节：探测时按你写的顺序依次试，真正
  抓数据时按每个源的健康度（成功率、延迟）加权挑，坏掉的会被自动绕开。

一个列表里混着字符串和数组会被拒绝，而不是去猜 —— 猜错的代价是一个看起来建成了、
播出来却是错的任务。`volumes` 和简写都给了时以 `volumes` 为准。

#### 其余参数：同一个 JSON 里一起给

```bash
curl -X POST http://127.0.0.1:9527/api/tasks -H 'content-type: application/json' -d '{
  "url": "https://cdn.example.com/movie.mp4",
  "name": "示例任务",
  "headers": { "Referer": "https://example.com/", "Cookie": "session=xxxx" },
  "max_per_volume": 8,
  "rate_limit_bps": "5M",
  "host_mappings": [{ "from": "cdn.example.com", "to": "10.0.0.1" }],
  "cache": true
}'
```

按「想做什么」找字段（每个字段的完整说明见 [§2](#2-任务配置字段全表)）：

| 想做的事 | 字段 |
|---|---|
| 带 `Referer` / `Cookie` / `User-Agent` 等请求头 | `headers` |
| 调并发 | `max_per_volume`。**总线程数 = 它 × 卷数**，`max_threads` 是派生值，传了会被忽略 |
| 限速 | `rate_limit_bps`（`"5M"` 这类写法也认）+ `rate_limit_algorithm` |
| 域名解析不出来 / 要指到别的 IP | `host_mappings` |
| 限制单次 Range 请求的长度 | `max_split`（留 `0` 让它自动，绝大多数情况不用管） |
| 播放时顺手把数据落盘 | `cache` |
| 临时任务，重启后不要保留 | `persist: false` |
| 浏览器打开短链时强制下载 / 强制预览 | `content_disposition` |
| 内容是加密的，要边播边解 | `plugins` |
| 指定下载文件名 | `output_filename` + `auto_filename: false` |

**查询参数**

| 参数 | 说明 |
|---|---|
| `start_cache` | 建完立刻开始把整个文件拉进缓存（aria2 那种「加进来就开始下」）。`1` / `true` / `yes` / `on` / 空值都算真；也可以写在请求体里：`"start_cache": true` |

**响应**

| 字段 | 说明 |
|---|---|
| `task_id` | 短 ID |
| `proxy_url` | 代理短链，直接交给播放器 / 下载器 |
| `cache_started` | 仅当请求了 `start_cache` 时出现 |
| `cache_error` | 缓存那一步失败的原因 |

**缓存起不来时任务照样建成**，原因单独报 —— 脚本才能区分「任务没建成」和「任务建
好了，只是源站现在不通」：

```json
{ "task_id": "a1b2c3", "proxy_url": "…", "cache_started": false,
  "cache_error": "internal: cannot reach the upstream: upstream returned non-success status: 404" }
```

### `GET /api/tasks` — 列表

返回 [`TaskInfo`](#5-任务响应taskinfo字段) 数组，按**最近编辑**倒序（同一秒创建的按
`task_id` 稳定排序），所以刚改过的任务总在最前面。

### `GET /api/tasks/:task_id` — 单个任务

返回一个 `TaskInfo`；任务不存在时 `404`。

### `PATCH /api/tasks/:task_id` — 局部更新

只动请求里**出现过**的字段，返回更新后的 `TaskInfo`。任务在线改，不用删了重建；改
URL 时磁盘缓存会跟着迁移（缓存键由 URL 集合算出）。

清空语义：

| 想做的事 | 写法 |
|---|---|
| 别动某个字段 | 不写它 |
| 清空任务名 / 输出文件名 | `{"name": null}`（`""` 和全空白等价） |
| 清掉所有请求头 | `{"headers": {}}` |
| 清掉所有域名映射 / 插件 | `{"host_mappings": []}` / `{"plugins": []}` |
| 换掉过期的签名链接 | `{"url": "https://cdn/movie.mp4?sign=fresh"}` |

URL 认与创建相同的那批简写；**没有提到 URL 的 PATCH 不会动源列表**。`start_cache`
在这里不作数（它只对创建有意义），写了会报未知字段 —— 要开缓存请调下面的
`POST …/cache`。

改动会重置一些内部状态：改 URL 或请求头会作废探测缓存，改 URL 还会重建每源健康度
（存活下来的 URL 保留统计）。

### `DELETE /api/tasks/:task_id` — 删除

`204`。**已缓存的数据保留在磁盘**（缓存按 URL 集合寻址，重建同样的任务即可继续用）；
要一起清掉，先调 `DELETE /api/tasks/:id/cache`。

### `GET /api/tasks/:task_id/export` — 导出配置

以附件形式返回该任务的 `TaskConfig` JSON（文件名 `hydraria-task-<id>.json`）。这份
JSON 原样 POST 回 `/api/tasks` 就能重建一个一样的任务 —— 迁移、备份、模板都靠这个
闭环。

### `POST /api/tasks/:task_id/pause` · `…/resume` — 暂停 / 恢复

暂停期间 `GET /stream/:task_id` 返回 `503`，配置和缓存都不动。两者都返回当前
`TaskInfo`。

### `POST /api/tasks/:task_id/cache` — 开始 / 继续整文件缓存

把整个文件补齐到本地缓存。与代理播放共享同一份稀疏文件和同一个工作线程池：播放已
经拉过的区间不会重下，暂停缓存也不会影响正在播放的连接。

幂等：已经在跑时是空操作，文件已完整时立即回 `done`。返回
[`cache_job`](#cache_job)。

失败会返回 `500` 并说明原因：源站连不上（带真实状态码 / 网络错误）、不支持 Range、
或没报出可缓存的大小。

### `POST /api/tasks/:task_id/cache/pause` — 暂停缓存

只停整文件填充，播放照常。停止是**立即**的：已经从网络收到的数据落盘，网络下载当场
中断，下次启动自动重新分配空洞。返回 `cache_job`。

### `DELETE /api/tasks/:task_id/cache` — 清掉该任务的缓存

删掉稀疏文件 + 位图 + 元数据，任务本身保留，`204`。缓存键由 URL 集合算出，所以指向
同一份内容的其他任务也会一起停止写入。

### `DELETE /api/cache` — 清掉所有缓存

返回 `{"bytes_freed": 123456}`。正在播放的任务会重新从源站拉。

---

## 5. 任务响应（TaskInfo）字段

```json
{
  "task_id": "a1b2c3",
  "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3",
  "config": { "...": "上面那份任务配置，含派生后的 max_threads" },
  "created_at": 1755400000,
  "updated_at": 1755400123,
  "bytes_served": 52428800,
  "active_connections": 1,
  "paused": false,
  "current_speed_bps": 8388608,
  "speed_samples": [0, 1048576, 8388608],
  "cache": { "...": "见下" },
  "cache_job": { "...": "见下" },
  "url_health": [ { "...": "见下" } ]
}
```

| 字段 | 说明 |
|---|---|
| `created_at` / `updated_at` | Unix 秒。`updated_at` 是最后一次成功编辑的时间；从未编辑过时等于 `created_at`（被拒的编辑不会动它） |
| `bytes_served` | 累计发给客户端的字节 |
| `active_connections` | 当前活跃的客户端连接数 |
| `paused` | 见 pause / resume |
| `current_speed_bps` / `speed_samples` | **发给客户端**的速率，约 1 Hz 采样，最近 60 点。注意它和缓存拉取是方向相反的两条流：缓存填充跑满带宽时这个数可能是 0（一个客户端都没连） |
| `cache` | 磁盘上这份缓存的统计，没有缓存时为 `null` |
| `cache_job` | 缓存协调器状态，没起过就是 `null` |
| `url_health` | 每个去重后的源 URL 一项 |

### `cache`

| 字段 | 说明 |
|---|---|
| `key` | 缓存键（URL 集合的摘要），同 URL 的任务共享 |
| `total_size` / `bytes_cached` | 文件总大小 / 已落盘字节 |
| `blocks_cached` / `blocks_total` | 已完成块数 / 总块数（1 MiB 一块） |
| `hits` / `misses` | 读取时的命中 / 未命中次数 |
| `etag` | 记录下来的上游校验值；变了会自动清空重取 |
| `bitmap_summary` | 位图降采样，每项是该区间已落盘的百分比（0-100），最多 ~128 项。面板画的热力条就是它 |

### `cache_job`

| 字段 | 说明 |
|---|---|
| `state` | `idle` / `running` / `paused` / `done` / `failed` |
| `error` | `failed` 时的原因 |
| `total_bytes` / `done_bytes` | 进度就用这两个算 |
| `current_speed_bps` / `speed_samples` | **从源站拉取**的速率 |
| `threads` | 当前工作线程数 |
| `active_readers` | 正在从这份缓存读的播放连接数 |
| `bitmap_summary` | 同上 |

### `url_health`

| 字段 | 说明 |
|---|---|
| `url` | 源地址 |
| `last_status` / `last_error` | 最近一次的 HTTP 状态码 / 错误文字 |
| `last_latency_ms` | 最近一次的首字节延迟 |
| `bytes_contributed` | 这个源累计贡献的字节 |
| `successful_requests` / `failed_requests` | 成功 / 失败请求数 |
| `current_speed_bps` | 这个源当前的速率 |
| `last_used_at` | Unix 秒 |
| `in_flight_requests` | 当前在飞的请求数 |
| `volume_size` | 它所属分卷的大小，探测到之后才有值 |

排查用法：某个镜像的 `failed_requests` 一直涨而 `bytes_contributed` 不动，就是它
坏了；`last_status` 是 403 通常意味着签名过期，`PATCH` 换个新地址即可。

---

## 6. 辅助端点

### `POST /api/probe` — 一次性探测

不建任务，先看看源站是什么情况。URL 认与建任务相同的那批简写。

```bash
curl -X POST http://127.0.0.1:9527/api/probe -H 'content-type: application/json' \
  -d '{"url": "https://cdn/movie.mp4", "headers": {"Referer": "https://example.com/"}}'
```

```json
{ "detected_filename": "movie.mp4", "suggested_filename": "movie.mp4",
  "total_size": 4294967296, "content_type": "video/mp4", "accepts_ranges": true }
```

请求体还可以带 `host_mappings` —— 探测必须和播放走同一条路，否则「探测通过一播就
502」会非常费解。`accepts_ranges: false` 意味着这个源只能单线程顺序读，也不能做整
文件缓存。

### `GET /api/hostmap/resolve` — 诊断已保存的映射

`?host=` 接受域名、IP 或整条 URL；`&task_id=` 按该任务的生效表算（全局 ∪ 任务级）。

```json
{ "host": "cdn.example.com", "mapped_to": "1.2.3.4:8443",
  "addresses": ["1.2.3.4"], "error": null, "proxy_env": "HTTPS_PROXY",
  "resolver": "system" }
```

`mapped_to: null` = 没有规则命中，走的是正常 DNS。`proxy_env` 只是告知检测到了代理
环境变量（命中映射的请求本来就会绕开代理）。`resolver` 是这次解析的执行者：
`system`，或具体的 DoT 服务器（`tls://1.1.1.1`）—— 只有「命中映射、且目标还是
域名」时才可能不是 `system`，TUN 环境下排查「拿到的是真实地址还是 fake-ip」先看它。

### `POST /api/hostmap/resolve` — 诊断**编辑中**的映射

面板上那个 ⚡ 用的就是它：按下测试的时机，恰恰是规则还没保存的时候。

```bash
curl -X POST http://127.0.0.1:9527/api/hostmap/resolve \
  -H 'content-type: application/json' \
  -d '{"host": "cdn.example.com", "scope": "task",
       "mappings": [{"from": "cdn.example.com", "to": "1.2.3.4", "enabled": true}]}'
```

| 字段 | 说明 |
|---|---|
| `host` | 域名、IP 或整条 URL |
| `mappings` | 要测的规则。不传就按已保存的算（等价于 GET）。只填了一半的行会被忽略；规则本身写错会报错 —— 那正是按下测试想知道的答案 |
| `scope` | `task`（默认）把 `mappings` 盖在当前生效的全局规则之上，和任务真跑起来时一致；`global` 表示 `mappings` 就是全部规则 —— 在设置里删掉一条再测，才能正确地报「没有规则命中」 |
| `task_id` | 只在不传 `mappings` 时用到 |

### `GET /api/settings` · `PUT /api/settings` — 全局设置

PUT 是局部更新，只动出现过的键。

| 键 | 说明 |
|---|---|
| `global_rate_limit_bps` | 全局限速，`0` = 不限，收 `"10M"` 这类写法 |
| `global_rate_limit_algorithm` | `token_bucket` / `sliding_window` |
| `host_mappings` | 全局域名映射，对所有任务生效。**任何一条不合法都会让整个 PUT 失败** —— 半张表比没有表更难排查 |
| `dns` | 解析**映射目标**用的 DNS。空 / 省略 = 系统解析器；`tls://1.1.1.1` = 自己走 DoT 查（只收 IP 地址，非 IP 会让整个 PUT 失败）。开着 TUN 模式的代理时系统解析会给 fake-ip，域名映射会静默失效 —— 见 [MANUAL §6.5](MANUAL.md#65-域名映射等价于-curl---resolve)。**只作用于映射目标那一次解析**，没命中映射的请求照常走系统解析；DoT 查不通自动退回系统解析 |
| `plugin_globals` | 按插件 id 索引的全局配置 |
| `download_dir` | 下载按钮的默认目录 |

### `GET /api/global` — 全局快照

```json
{ "settings": { "...": "同上" }, "cache_fill_speed_bps": 0, "current_speed_bps": 0,
  "speed_samples": [], "cache_total_bytes": 0, "task_count": 3,
  "active_connections": 0, "bytes_served_total": 0 }
```

出与入分开报：`current_speed_bps` 是发给客户端的，`cache_fill_speed_bps` 是从源站拉
进磁盘的。相加会在「边播边缓存」时把同一批字节数两遍。

### 插件

| 端点 | 作用 |
|---|---|
| `GET /api/plugins` | 插件目录：`id` / `name` / `description` / `global_fields` / `task_fields` / `forward_fields` / `has_forward` / `default_global` / `default_task` / 当前 `global`。想知道某个插件的 `config` 该写什么，看它的 `task_fields` |
| `GET` · `PUT /api/plugins/:id/global` | 读 / 写单个插件的全局配置（写入前按插件自己的 schema 校验） |
| `POST /api/plugins/:id/forward` | 执行插件的正向工具（如加密打包）。响应是 **NDJSON** 流，每行一个对象：`{"type":"progress",…}` / `{"type":"result",…}` / `{"type":"error",…}` |

### 其他

| 端点 | 作用 |
|---|---|
| `GET /api/fs/info` | 本机是否支持原生文件选择器 |
| `POST /api/fs/pick` | 弹出原生文件 / 目录选择框（面板用） |
| `GET /healthz` | 存活探针，返回 `ok` |

---

## 7. 数据平面

### `GET /stream/:task_id`

客户端真正消费的端点，行为就是一台普通 HTTP 文件服务器：

- 支持 `Range: bytes=start-end` 和后缀范围 `bytes=-N`；开放端 `bytes=X-` 走 seek
  优化路径（起播和拖动都靠它）。
- 客户端带了 `Range` 就返回 `206`，否则 `200`。
- 透传源站探测到的 `Content-Type`、`ETag`、`Last-Modified`、`Accept-Ranges`。
- 附加 `X-Hydraria-Task: <task_id>` 便于排查。
- 支持 `HEAD`。
- 任务被暂停时 `503`；源站不可用时 `502`。

内部会把这一条流扇出成多个并发 Range 请求分摊到所有源，客户端只看到一条普通的
HTTP/1.1 流。

---

## 8. 脚本配方

### 建任务并等它缓存完

```bash
#!/usr/bin/env bash
set -euo pipefail
HYDRARIA=${HYDRARIA:-http://127.0.0.1:9527}

id=$(curl -sS -X POST "$HYDRARIA/api/tasks?start_cache=1" \
       -H 'content-type: application/json' \
       -d "{\"url\": \"$1\", \"cache\": true}" | jq -r .task_id)

while :; do
  read -r state done total < <(
    curl -sS "$HYDRARIA/api/tasks/$id" |
      jq -r '.cache_job | "\(.state) \(.done_bytes) \(.total_bytes)"')
  printf '\r%s %s/%s' "$state" "$done" "$total"
  case $state in done) echo " ✓"; break ;; failed) echo " ✗"; exit 1 ;; esac
  sleep 2
done
```

### 批量下发

```bash
jq -rn '$ARGS.positional[]' --args "$@" | while read -r url; do
  curl -sS -X POST "$HYDRARIA/api/tasks" -H 'content-type: application/json' \
    -d "$(jq -n --arg u "$url" '{url: $u, cache: true, max_per_volume: 8}')" |
    jq -r '.proxy_url'
done
```

### 签名过期后换地址（保留缓存与统计）

```bash
curl -sS -X PATCH "$HYDRARIA/api/tasks/$id" -H 'content-type: application/json' \
  -d "$(jq -n --arg u "$fresh_url" '{url: $u}')" | jq -r '.config.volumes'
```

### 备份 / 迁移所有任务

```bash
# 导出
curl -sS "$HYDRARIA/api/tasks" | jq '[.[].config]' > tasks.json
# 在另一台机器上导入
jq -c '.[]' tasks.json | while read -r cfg; do
  curl -sS -X POST "$HYDRARIA/api/tasks" -H 'content-type: application/json' -d "$cfg"
done
```
