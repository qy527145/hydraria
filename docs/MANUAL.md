# Hydraria 使用手册

> 一个基于 Rust 的高性能、低延迟、多线程 HTTP 流代理。
> 单二进制 + 内嵌 Web 仪表盘，无前端部署、无依赖、即开即用。

---

## 目录

1. [Hydraria 是什么](#1-hydraria-是什么)
2. [核心亮点](#2-核心亮点)
3. [与 aria2 / IDM / 普通下载器的差异](#3-与-aria2--idm--普通下载器的差异)
4. [典型使用场景](#4-典型使用场景)
5. [快速上手](#5-快速上手)
6. [仪表盘使用指南](#6-仪表盘使用指南)
7. [任务字段全量参考](#7-任务字段全量参考)
8. [分卷 + 加密完整流程](#8-分卷--加密完整流程)
9. [CLI 用法](#9-cli-用法)
10. [REST API 参考](#10-rest-api-参考)
11. [常见问题 FAQ](#11-常见问题-faq)
12. [故障排查](#12-故障排查)
13. [合规与隐私声明](#13-合规与隐私声明)

---

## 1. Hydraria 是什么

Hydraria 把一个**慢的、单源的 HTTP 下载**变成**并行的多源拉取**，同时对外仍然暴露一个普通的 HTTP 短链。任何标准 HTTP 客户端 —— 浏览器 `<video>`、VLC、IINA、PotPlayer、aria2、wget、curl —— 都可以直接消费这个短链，无需感知背后发生了什么。

```
[ 浏览器 / 播放器 / 下载器 ]
            │ GET http://127.0.0.1:9527/stream/a1b2c3
            ▼
   ┌────────────────────┐
   │   Hydraria 代理     │ ← 单二进制 + 内嵌仪表盘
   └─────────┬──────────┘
             │ 并行多线程多源拉取
   ┌─────────┴──────────┐
   ▼         ▼          ▼
 源1.mp4   源2.mp4    源3.mp4   (镜像)
   ▼         ▼          ▼
 卷01      卷02       卷03     (分卷,有序拼接)
```

短链可以分享给同局域网设备、丢进 VLC、嵌入网页 `<video>`，**无需先下载到本地**。

---

## 2. 核心亮点

### 🚀 多线程分片拉取
单次客户端请求被切成若干 `max_split` 字节的子区间，由 `max_threads` 个 fetcher 并行拉取，再由 serializer 按顺序拼回原始字节流交给客户端。滑动窗口调度避免饿死正在被消费的 chunk，保证带宽利用率。

### 🌐 多源镜像与健康看板
一个任务可以配置多个镜像 URL。chunk 在镜像间按加权策略分配 —— 最近吞吐高的镜像优先，连续失败的镜像降权。仪表盘的"源状态看板"实时显示每个镜像的：当前速度、累计字节、TTFB 延迟、最后状态码、错误信息。失败 chunk 自动在下一个镜像重试，对客户端透明。

### 📚 分卷（Volumes）—— Hydraria 独有概念
跟"镜像"正交：分卷是**按顺序拼接**的多个文件 part（part01、part02、part03…），每个卷自己可以有多个镜像。
- 单卷多镜像 = 传统多源场景。
- 多卷各自多镜像 = 整文件被切片分发后再拼回。

Hydraria 在 probe 阶段就探明每个卷的大小、ETag、accept-ranges，按合并坐标系（merged offset）规划 chunk，确保任意一个 chunk 只命中一个卷的一个 URL，简单且不会拼接错位。

### 🔐 ChaCha20 端到端加密插件
内置插件：发送端用 `forward` 工具加密源文件（可选同时分卷），分发到任意 HTTP 服务器；接收端在 Hydraria 任务里填入合并密钥（key+nonce 88 hex），代理在流式分发时实时解密。

> 关键性质：ChaCha20 是按字节寻址的流密码，**加密-分卷-下载-解密**全过程基于**合并后的字节偏移**，所以分卷可以任意切分/合并，只要顺序保持。

### 💾 磁盘缓存
按任务可选。命中后零外发流量。
- 稀疏文件 + 1 MB block 位图存储，sha256(URL列表) 作为 key。
- ETag/Last-Modified 自动校验，源变化时自动失效并重新拉。
- 缓存的是**上游原始字节**（密文），插件在读取路径才解密 —— 改密钥不会作废已下载数据。
- 跨任务共享 —— 同一组 URL 不同 header 的任务共用同一份缓存。
- 仪表盘渲染 block 热力图，看得到缓存命中分布。

### 🐢 双算法限速
- 任务级 `rate_limit_bps` + 全局 `global_rate_limit_bps`，可独立配置。
- 算法可选：
  - **令牌桶（默认）** —— 允许短突发（≤0.5s 配额），适合视频起播。
  - **滑动窗口** —— 任意 1 秒内绝对不超，适合严格配额场景。

### 🎚️ 最大分片大小自由配置
`max_split` 支持 `5M` / `512K` / `1G` 等可读字符串，下到 64K。
- **小分片** = 失败重试代价低、seek 启动快，但 HTTP overhead 高。
- **大分片** = 长尾吞吐稳定，但 seek 时被取消的字节更多。
- 默认 5M，对绝大多数 CDN 是好的平衡点。
- 对 `Range: X-`（无上限的范围请求，例如 PotPlayer 起播）自动启用**首块自适应小分片**：前 4 个 chunk 削到 512K，让 seek 响应更快。

### ⚡ 强制取消
客户端断开（用户 seek 或关播放器）时，driver 任务立刻 `JoinHandle::abort()` 掉所有还在跑的 fetcher，立即释放上游带宽，新连接不再被遗留请求抢占。

### 🎯 浏览器行为可控
任务级 `content_disposition` 三选一：
- `auto`（默认）—— `inline` + 上游 Content-Type，浏览器自决（视频通常预览，二进制通常下载）。
- `inline`（强制预览）—— 修正通用 MIME（`octet-stream` 按文件名猜成 `video/mp4` 等），让 `<video>` 真的能渲染。
- `attachment`（强制下载）—— 永远触发"另存为"，跨设备分发文件时常用。

### 🖥️ 内嵌 Web 仪表盘
单文件 vanilla JS，编译进二进制（rust-embed），打开 `http://127.0.0.1:9527/` 即用。功能涵盖：
- 任务创建/编辑/暂停/恢复/删除/克隆
- 拖拽重排分卷、即插即用插件配置
- JSON 或 URL 列表粘贴导入（创建和编辑都支持）
- 单任务实时吞吐 sparkline、源看板、缓存热力图
- 全局速率/缓存占用/活跃连接概览
- 弹窗操作栏 sticky 在底部，长表单也能随时关闭

### 💽 任务持久化
开启 `persist: true` 的任务自动写入 `~/.hydraria/tasks.json`，重启自动恢复。每 5 秒检查脏状态，原子写盘。

---

## 3. 与 aria2 / IDM / 普通下载器的差异

| 维度 | Hydraria | aria2c | IDM / 浏览器多线程 |
|---|---|---|---|
| **形态** | HTTP 流代理（客户端拿到一条 URL） | 下载器（写到本地文件） | 浏览器内部 |
| **可被 VLC/IINA/浏览器直接消费** | ✅ 标准 HTTP/206 | ❌ 需要先下完 | N/A |
| **多线程分片** | ✅ `max_threads` × `max_split` | ✅ `-x` × `-k` | ✅ |
| **多源镜像** | ✅ 加权挑选 + 健康看板 | ✅ 简单 round-robin | ❌ |
| **多卷有序拼接** | ✅ 独有 —— 直接按顺序流式拼回 | ❌ 需自己拼好再播 | ❌ |
| **分卷加密 (ChaCha20)** | ✅ 内置插件 + 一键加密分卷工具 | ❌ | ❌ |
| **范围请求 / seek** | ✅ 206 + 取消旧 fetcher + 首块小分片 | N/A（不是流） | N/A |
| **磁盘缓存** | ✅ 稀疏文件 + bitmap + ETag 校验 | ✅ 自己管理文件 | 浏览器缓存 |
| **限速 + 算法选择** | ✅ 令牌桶/滑动窗口 + 任务/全局两级 | ✅ 仅速率 | ❌ |
| **任务持久化 + 仪表盘** | ✅ 内嵌 | 需 AriaNg 等外挂 | ❌ |
| **浏览器行为控制** | ✅ inline/attachment/auto 三态 | N/A | N/A |

> **小结**：aria2 是优秀的下载器；Hydraria 是把"高速并行拉取"封装成 HTTP 代理服务，重点解决**实时播放**和**多卷分发**两个场景。分卷加密、对 Range 请求的优化、可调的最大分片，是它跟所有"下载器系"工具最直接的差异。

---

## 4. 典型使用场景

### 场景 A：网盘单线程限速
你拿到了某个网盘的直链（蓝奏云/123/天翼/OSS 私链等），但服务端把单连接限到 2 MB/s。

- 把同一个直链填多份（或者镜像到不同节点）作为多镜像，
- `max_threads = 16` + `max_split = 2M`，
- 用 VLC 打开短链 —— 实际带宽可以叠到 30+ MB/s。

### 场景 B：跨设备播放 NAS / 朋友的私链
朋友的私有 OSS 上挂了一部蓝光片，但 NAS 客户端速度感人。
- Hydraria 部署在你的内网一台机器（或 VPS）上，
- 创建任务（多镜像 + 缓存 + ChaCha20 加密插件可选），
- 把 `http://your-host/stream/xxxx` 给朋友的 IINA 或电视盒子打开。
- 第二次播放走缓存，零外发流量。

### 场景 C：分卷加密分发，规避内容审查
你有一个大文件想跨多个免费图床/网盘分发，又不希望被中间环节扫描或撤档：
1. 仪表盘 → 插件 → ChaCha20 → 正向工具：
   - 输入文件、输出目录、`max_volume_size = 50M`、`split_mode = random`、勾选"启用加密"。
   - 点执行 —— 拿到一组随机大小的密文卷（默认 `.part01.enc / .part02.enc / …`）和一个 88 位 hex 的合并密钥。
2. 把卷文件上传到不同图床/网盘/对象存储 —— 每个卷都是无意义的密文，CDN 看不到内容。
3. 接收端在 Hydraria 新建任务：
   - 按顺序填入每个卷的下载 URL（每卷一行；同一卷的多个镜像写连续多行；卷之间空一行隔开）。
   - 启用 chacha20 插件，粘贴密钥。
   - 短链交给播放器 —— 边解密边播。

### 场景 D：在浏览器里直接预览二进制
某些 CDN 给所有文件回 `Content-Type: application/octet-stream`，浏览器一律下载。把任务的"浏览器行为"设为 **强制预览**，Hydraria 会按文件名猜 MIME 重写响应头，`<video>` / `<audio>` / 图片就能正常渲染。

### 场景 E：调试/迁移 / 把 HTTP 接口适配成流
你在做一个跑数据/媒体管线的脚本，需要把多个分片接口拼成一个连续流喂给 ffmpeg？把它们当多卷创建一个任务，`ffmpeg -i http://localhost:9527/stream/xxxx` 即可。

---

## 5. 快速上手

### 5.1 安装

需要 Rust 1.85+（edition 2024）。

#### 方式一：从 crates.io 安装（推荐）

```bash
cargo install hydraria
```

二进制会被放到 `~/.cargo/bin/hydraria`，确保该目录在 `PATH` 里就能直接 `hydraria` 启动。

#### 方式二：源码构建

```bash
git clone https://github.com/qy527145/hydraria.git
cd hydraria
cargo build --release
```

产物：`./target/release/hydraria`（约 10 MB，静态链接）。适合需要本地改代码或交叉编译的场景。

### 5.2 启动

```bash
hydraria \
  --bind 127.0.0.1:9527 \
  --cache-dir ~/.hydraria/cache \
  --state-file ~/.hydraria/tasks.json
```

默认值就是上面这些；裸跑 `hydraria` 即可（源码构建出来的二进制用 `./target/release/hydraria`）。

环境变量 `RUST_LOG=hydraria=debug,info` 控制日志详细程度。

浏览器打开 `http://127.0.0.1:9527/`，看到仪表盘即成功。

### 5.3 第一个任务

仪表盘右上角 **+ 新建任务** → 在分卷编辑器里粘贴一两个 URL → **生成短链** → 仪表盘列表里点复制按钮，把 `http://127.0.0.1:9527/stream/xxxx` 拖进 VLC。

---

## 6. 仪表盘使用指南

### 6.1 头部

- **统计 pills**：任务数 / 活跃连接 / 累计下行 / 缓存占用。
- **全局速率 + sparkline**：60 个采样点滚动的实时吞吐曲线。
- **搜索框**：按任务名 / task_id / URL 子串过滤；按 `/` 聚焦。
- **视图切换**：网格 / 列表。
- **设置**：全局限速 + 算法 + 缓存占用一览。
- **插件**：插件列表、全局配置、正向工具。

### 6.2 任务卡片

- **proxy URL**：点击复制；旁边状态徽章（running / paused / cache / persist / 速率限制等）。
- **实时吞吐 sparkline + 数字**。
- **源状态看板（展开）**：每个 URL 一行，看速度、字节、状态码、错误。
- **操作按钮**：暂停/恢复 / 编辑 / 复制配置 / 清空缓存 / 删除 / 导出 JSON / 克隆。
- **缓存热力图**：128 段灰阶 → 蓝阶，hover 看每段字节范围和命中率。

### 6.3 新建 / 编辑任务

#### 分卷编辑器
- 默认一卷一格 textarea，每行一个镜像 URL。
- **+ 添加分卷** 在末尾新加；**+ 在此处插入** 在任意两卷之间插入。
- 拖拽卷头部的 `⋮⋮` 把卷重新排序（用于校正分卷顺序）。
- 多卷的下载文件名：仪表盘默认对每卷探测到的文件名取 LCP，结果填到"下载文件名"。

#### 主要字段
| 字段 | 说明 |
|---|---|
| 最大并发线程 | `max_threads`，默认 16，1-128。 |
| 分片大小 | `max_split`，默认 5M，最小 64K。 |
| 自定义请求头 | JSON 对象，常用 `Cookie` / `Referer` / `User-Agent`。 |
| 任务名 | 用于仪表盘搜索和列表显示。 |
| 单任务限速 | `2M` / `512K`；空 = 不限。 |
| 下载文件名 + 自动检测 | 不勾自动检测时用此字段；勾选后运行时探测覆盖。 |
| 限速算法 | 令牌桶 / 滑动窗口。 |
| 启用本地磁盘缓存 | 开启 = 命中后零外发。 |
| 持久化保存 | 重启后自动恢复。 |
| 浏览器行为 | auto / 强制预览 / 强制下载（详见 §2）。 |
| 插件 | 勾选要启用的插件并填写每任务密钥等。 |

#### 导入 / 探测
- **↥ 导入**：粘贴标准 JSON（任务导出文件原样）或一段 URL 列表（空行隔开多卷）；编辑模式只覆盖导入里实际给出的字段。
- **探测**：根据当前 URL 和 headers 做一次 HEAD + 1 byte GET，把检测到的文件名填回输入框。

### 6.4 插件 & 工具

打开顶栏 **插件** —— 每个插件一张卡片：
- 描述
- 全局配置（如 ChaCha20 的 I/O buffer）—— "保存全局配置"
- 正向工具（如 ChaCha20 加密 + 分卷）—— 填参数后"执行"
- 结果区显示输出文件、生成的合并密钥（一键复制）等

#### ChaCha20 工具字段
| 字段 | 说明 |
|---|---|
| 输入文件（明文）绝对路径 | 选择/粘贴本地路径 |
| 输出目录 | 不存在会自动创建 |
| 文件名前缀 | 默认 = 输入文件基名（无扩展名） |
| 分卷后缀模板 | `{N}` 会被替换为零填充卷号；默认 `.part{N}.enc` |
| 分卷最大大小 | `5M`/`512K`/`1G`；空/0 = 单文件输出 |
| 分卷策略 | 随机大小（伪装分发） / 固定大小（末卷为余数） |
| 启用 ChaCha20 加密 | 关 = 纯切分，密钥/Nonce 字段被忽略 |
| 自动生成缺失的密钥/Nonce | 执行前 UI 客户端随机生成并回填 |
| 解密密钥（key+nonce 合并） | 88 hex；正向时若启用了"自动生成"，会自动填入 |

执行后结果区的 `secret` 字段就是接收方需要粘贴的那一串。

---

## 7. 任务字段全量参考

```jsonc
{
  // 分卷布局：外层 = 卷顺序；内层 = 该卷的镜像列表
  "volumes": [
    ["https://cdn1.com/movie.part01.mp4", "https://cdn2.com/movie.part01.mp4"],
    ["https://cdn1.com/movie.part02.mp4"]
  ],

  "max_threads": 16,                   // int, 默认 8
  "max_split": "5M",                   // 字符串(可读)或字节数, 默认 5M
  "cache": false,                      // bool, 默认 false
  "persist": false,                    // bool, 默认 false

  "headers": {                         // object<string,string>
    "User-Agent": "Mozilla/5.0",
    "Cookie": "session=xxxx"
  },

  "name": "my-movie",                  // string?, 默认 null
  "output_filename": "movie.mp4",      // string?, 默认 null
  "auto_filename": true,               // bool, 默认 true

  "rate_limit_bps": "2M",              // 字符串/数字, 0 或空 = 不限
  "rate_limit_algorithm": "token_bucket",  // 或 "sliding_window"

  "content_disposition": "auto",       // "auto" / "inline" / "attachment"

  "plugins": [
    {
      "id": "chacha20",
      "enabled": true,
      "config": {
        "secret": "abcd...88 hex chars..."
        // 旧导出文件里也可能是 { "key": "...64...", "nonce": "...24..." } —— 兼容
      }
    }
  ]
}
```

字段细节：
- `max_split` ≥ 64K；分片越小 seek 越快，但 HTTP overhead 越大。
- `rate_limit_bps`：0 不限；非零时算法决定突发性。
- 单卷多镜像 ≡ 历史的"多源镜像同一文件"模式。
- `output_filename` 和 `auto_filename`：勾选自动检测时优先用上游探测；否则用 `output_filename` 字面值（空 = 不发 Content-Disposition）。

---

## 8. 分卷 + 加密完整流程

> 目标：把 `movie.mkv`（4 GB）拆成 80 个左右随机大小的密文卷，分别传到不同图床，接收端通过 Hydraria 边解密边播。

### 8.1 发送端

```
仪表盘 → 插件 → ChaCha20 加解密 → 正向工具
├── 输入文件: /Users/me/movie.mkv
├── 输出目录: /tmp/movie-encrypted
├── 文件名前缀: movie
├── 分卷后缀模板: .part{N}.enc  (默认即可)
├── 分卷最大大小: 50M
├── 分卷策略: 随机大小 (伪装)
├── 启用 ChaCha20 加密: ✓
├── 自动生成缺失的密钥/Nonce: ✓
└── [▶ 执行]
```

结果面板会列出每个输出文件的路径和大小，以及 `secret` 字段。**立刻复制保存这 88 hex —— 关闭后无法找回。**

把 `/tmp/movie-encrypted/movie.partXX.enc` 上传到任意 HTTP 可访问的服务器。

### 8.2 接收端

```
仪表盘 → + 新建任务 → ↥ 导入 (URL 列表):

https://imghost-a.com/movie.part01.enc
https://imghost-a-mirror.com/movie.part01.enc

https://imghost-b.com/movie.part02.enc
https://imghost-c.com/movie.part02.enc

... (每卷可以多镜像；卷之间空行)

→ 启用 chacha20 插件 → 粘贴 secret
→ 生成短链
→ VLC/IINA 打开
```

注意点：
- **卷顺序必须严格按发送端的顺序**。如果传错顺序，密文长度对了但解密出来是乱码。
- 仪表盘"探测"会给你建议的合并文件名（多卷取 LCP，自动去掉 `.partNN.enc` 等共同后缀）。
- 如果某卷有多个镜像，连续多行写在同一段里；卷之间空行隔开。

### 8.3 进阶：纯分卷（不加密）
ChaCha20 工具的"启用加密"取消勾选 = 退化成纯文件分割器，输出 `.part01 / .part02 …` 系列，方便分发到限制单文件大小的服务器。接收端不需要插件，只需按顺序填卷的 URL 即可。

---

## 9. CLI 用法

### 9.1 服务模式（默认）

```bash
hydraria                                  # 等价于 hydraria server
hydraria --bind 0.0.0.0:9527              # 局域网可访问
hydraria --cache-dir /data/cache \
         --state-file /data/state.json
```

环境变量 `RUST_LOG=hydraria=debug,info` 控制日志。

### 9.2 download 子命令（直接下载）

不启动服务，直接用引擎把 URL 写到本地文件：

```bash
hydraria download https://cdn.example.com/file.iso \
  -o ./file.iso \
  --threads 16 \
  --split 5M \
  -H "Cookie: session=xxxx" \
  --cache
```

参数：
| 选项 | 说明 |
|---|---|
| `urls...` | 一个或多个源 URL |
| `-o, --output` | 输出文件路径，`-` 表示 stdout |
| `--threads` | 并发数（默认 16） |
| `--split` | 分片大小（默认 5M） |
| `-H, --header` | 重复传 `-H "k: v"` 加请求头 |
| `--volumes` | 把 URL 视为有序分卷（默认是镜像） |
| `--cache` | 共用主服务的缓存目录，断点续传 |

实时进度条 + 速率显示，写到 stderr。

---

## 10. REST API 参考

### 控制平面

| 方法 路径 | 作用 |
|---|---|
| `POST /api/tasks` | 创建任务，返回 `{task_id, proxy_url}` |
| `GET /api/tasks` | 列表 + 实时统计 |
| `GET /api/tasks/:id` | 单任务详情 |
| `PATCH /api/tasks/:id` | 部分更新（任意 TaskConfig 字段） |
| `DELETE /api/tasks/:id` | 删除任务（缓存保留） |
| `POST /api/tasks/:id/pause` · `…/resume` | 暂停/恢复 |
| `DELETE /api/tasks/:id/cache` | 清空该任务的磁盘缓存 |
| `GET /api/tasks/:id/export` | 下载任务配置 JSON |
| `POST /api/probe` | 一次性探测，返回 `{filename, total_size, content_type, accepts_ranges}` |
| `GET /api/settings` · `PUT /api/settings` | 全局设置（限速 + 插件全局配置） |
| `GET /api/global` | 仪表盘全局快照 |
| `GET /api/plugins` | 插件目录 + 全局配置 |
| `GET/PUT /api/plugins/:id/global` | 单插件全局配置 |
| `POST /api/plugins/:id/forward` | 执行插件的正向工具 |
| `GET /api/fs/info` · `POST /api/fs/pick` | 原生文件选择器支持探测 + 调用 |

### 数据平面

`GET /stream/:task_id`
- 支持 `Range: bytes=start-end` 和后缀范围 `bytes=-N`，开放端范围 `bytes=X-` 走 seek 优化路径。
- 部分读返回 `206 Partial Content`；全量返回 `200 OK`。
- 透传 `Content-Type`、`ETag`、`Last-Modified`、`Accept-Ranges`。
- 附加 `X-Hydraria-Task: <task_id>` 便于排查。
- `HEAD` 支持。

### 创建任务示例

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "volumes": [
      ["https://cdn1/movie.mp4", "https://cdn2/movie.mp4"]
    ],
    "max_threads": 16,
    "max_split": "5M",
    "cache": true,
    "headers": {"User-Agent": "Mozilla/5.0"},
    "content_disposition": "inline"
  }'
```

---

## 11. 常见问题 FAQ

**Q：为什么我的 PotPlayer/VLC 起播很慢？**
A：默认 `max_split = 5M`，前几个 chunk 拉满才能播。对开放端 Range 请求（`Range: 0-`），Hydraria 自动把前 4 个 chunk 切到 512K 加速首帧。如果还慢，可以把 `max_split` 调到 1M-2M 试试。

**Q：seek 时画面卡住一会再播是什么原因？**
A：旧的 in-flight chunk 占着上游带宽。Hydraria 已经在 driver 上做了 `abort` 取消所有运行中的 fetcher，但 TCP 关闭也有几十到几百毫秒。如果延迟仍不可接受，缩小 `max_split` 进一步降低被取消的字节量。

**Q：开了缓存，但是改了密钥后旧密文还有效吗？**
A：有效。缓存存的是上游原始字节（密文），解密在读取路径才发生 —— 改密钥不影响已缓存的字节，下次播放用新密钥从缓存取出再解密即可。

**Q：分卷传错了顺序怎么办？**
A：编辑任务，用每张卡片头部的 `⋮⋮` 拖拽手柄重排，保存即可。已经在跑的连接不受影响（按旧配置跑完），新连接立刻按新顺序拉。

**Q：缓存占用越来越大怎么清理？**
A：单任务可以从卡片菜单"清空缓存"。全局可以删 `~/.hydraria/cache` 整个目录（先停服务）。LRU 自动清理在 Roadmap。

**Q：能跑在 ARM Linux / NAS / 树莓派吗？**
A：可以。`cargo build --release --target aarch64-unknown-linux-gnu` 之类的交叉编译都 OK。运行内存几十 MB 级。

**Q：仪表盘怎么暴露给公网？**
A：当前**没有做认证**，请勿直接绑 `0.0.0.0` 暴露公网。建议局域网或通过 nginx + Basic Auth / OAuth 反代。鉴权在 Roadmap。

**Q：上游 URL 失效后任务会怎样？**
A：fetcher 在所有镜像都尝试后报错，客户端拿到 502。源看板会显示每个 URL 的错误信息和状态码，方便排查是哪一个失效了。

---

## 12. 故障排查

### 启动相关
- `Address already in use`：端口冲突，换 `--bind`。
- `Permission denied` on cache dir：换 `--cache-dir` 到当前用户可写的位置。

### 任务相关
- **502 Bad Gateway**：上游探测全失败。开 `RUST_LOG=hydraria=debug` 看具体哪个 URL、什么错误。
- **416 Range Not Satisfiable**：客户端发了非法 Range（例如 start > total）。一般是播放器 bug。
- **403 / 401**：上游需要鉴权 header，确认 `headers` 字段填全。Cookie 有时效。
- **任务正常拉但播放器一直缓冲**：很可能上游不支持 Range；Hydraria 会自动降级成 passthrough 模式（单源单流），多线程加速失效。看任务源看板的 `accepts_ranges` 是否 `true`。

### 加密相关
- **解密后是乱码**：检查 `secret` 是否完全正确（88 hex，不能有空格/换行），分卷顺序是否对，所有卷文件是不是用同一组 key+nonce 加密的。
- **`secret hex decode` 错误**：粘贴时混进了非 hex 字符；重新点 🎲 或纯净粘贴。

### 缓存相关
- **缓存命中率低**：每次源 URL 变化（ETag 变）都会触发缓存失效。如果上游 ETag 不稳定，关掉缓存或者把 URL 列表里只保留稳定的源。
- **磁盘满**：清理 `~/.hydraria/cache`，或者把 `--cache-dir` 指向大盘。

### 日志
```
RUST_LOG=hydraria=debug,reqwest=warn,hyper=warn  ./hydraria
```
关键事件：`stream start`、`probe ok`、`stream_range`、`cache HIT/MISS`、`client gone`、`warmup spawned`。

---

## 13. 合规与隐私声明

- Hydraria 是**网络管线工具**，本身不内置任何破解、绕过 DRM 或抓取功能。源 URL 需要用户自行提供且必须是用户本人有权访问的资源。
- 适用场景：跨设备播放自己/朋友的私有资源、绕过单连接限速以提升用户合理使用体验、研究教学、个人备份的分卷分发等。
- **请勿**用于版权侵权、数据窃取、规避平台合理风控、传播违法内容等任何不合法用途。使用者需自行承担因此产生的全部责任。
- 项目本身不收集任何上行数据；所有任务、统计、日志均存储在本机，全部代码开源可审计。

---

## 附：项目仓库与版本

仓库：<https://github.com/qy527145/hydraria>
当前版本：v0.1.3+（详见 `Cargo.toml`）
License：MIT
反馈：GitHub Issues / Discussions 欢迎 PR。
