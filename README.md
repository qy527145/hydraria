# Hydraria

**English** · [简体中文](README.zh-CN.md)

> A high-performance, low-latency, multi-threaded HTTP streaming proxy written in Rust — with a built-in web dashboard.

Hydraria turns a slow, single-source HTTP download into a parallelized, multi-source pull, then streams the assembled bytes back to any standard HTTP client (browser, VLC, IINA, Aria2, `wget`, …) over a stable short link. The whole thing is one statically linked Rust binary; the dashboard is embedded inside it via `rust-embed`, so there is nothing to deploy on the frontend.

```
[ user / Web UI ] --(1. configure task)--> [ Task Manager ] --(2. mint short link)--> /stream/a1b2c3
                                                                    |
[ player / downloader ] <--(4. single-threaded stream)-- [ Proxy Engine ] <--(3. GET short link)
       |                                                         |
       v                                            (internally: multi-threaded multi-source pull)
 [ receives one stream ]                                  [ origin 1, origin 2, origin 3 ... ]
```

## Highlights

- **Claim-based multi-threaded fetching** — `max_threads` workers each claim a contiguous range, pull it in one request, and write it at its absolute offset. An idle worker escalates through three tiers: carve from the **largest gap** (T1) → steal the tail of someone else's in-flight request (T2) → hedge it on a budget once only the tail is left (T3). Steal victims are picked by slowness, not size, and a request that hasn't moved for 30 s is declared dead and re-cut.
- **Multi-source failover** — list multiple origin URLs; chunks are round-robined across them and a failed chunk transparently retries on a different origin.
- **Range / Seek support** — when a player issues `Range: bytes=…` (e.g. seeking forward in a video), Hydraria re-plans the chunks from that offset. No re-download of earlier bytes. Always responds with `206` when the client sent a Range header (which is what Chrome's `<video>` element relies on to know seeking is supported).
- **Disk cache** — opt-in per task. Bytes are stored in a sparse file keyed by the SHA-256 of the URL list, with a bitmap tracking 1-MB block completion. A second request to the same task is served entirely from disk; no upstream traffic. ETag-validated on every probe; an upstream change auto-wipes and re-fetches.
- **Passthrough fallback** — if the origin doesn't advertise byte-range support, Hydraria automatically falls back to a single-stream passthrough so unrangeable sources still work.
- **Backpressure-aware streaming** — the chunk planner uses a bounded-channel pipeline (`tokio::sync::mpsc`), so a slow client throttles upstream fetches instead of blowing memory.
- **Custom headers per task** — set `Cookie`, `User-Agent`, `Referer`, etc. once at task creation; every upstream chunk request carries them.
- **Host mapping** — the equivalent of `curl --resolve`, settable globally *and* per task (`host_mappings`; the two are unioned, task wins on a conflicting source). `--map from=to` on the CLI. Point a hostname at an IP or a backup host when public DNS can't resolve it. Only the TCP target moves: the URL, the `Host` header and the TLS SNI stay exactly as written, so signed URLs keep verifying. Supports `*.example.com` suffixes, a port on the target, and bare-IP sources. A mapped request automatically **bypasses the proxy** — otherwise the proxy resolves the hostname itself and the mapping silently does nothing. `POST /api/hostmap/resolve` (and the ⚡ button in the dashboard) reports where a host actually ends up — including for rules you are still typing, since it accepts the draft rule set rather than only what's saved.
- **DoT for mapping targets** — with a **TUN-mode** proxy running, the system resolver hands back a fake-ip (`198.18.0.0/15`) for the mapping target, so host mapping silently stops working: the connection succeeds, the status code is right, and not one byte arrives. Set `dns` to `tls://1.1.1.1` in the settings (`--dns` on the CLI) and Hydraria resolves the target itself over DNS-over-TLS — TLS rather than plain UDP:53 because TUN setups usually hijack port 53 as well. **Its scope is that one lookup:** requests that hit no mapping resolve exactly as before, and a DoT failure falls back to the system resolver rather than breaking downloads that used to work. Takes an IP address only (a resolver address that itself needs resolving would be back at square one).
- **Pause / resume / edit** — tasks can be paused (stream returns 503 while config + cache stay intact) and live-edited via `PATCH /api/tasks/:id`. No need to delete and recreate.
- **Rate limiting** — per-task and global token-bucket limiters (`rate_limit_bps` on the task config, `global_rate_limit_bps` on settings). Small bursts allowed; long-run average held to the cap.
- **Persistence** — `persist` is **on by default**; tasks are written to `~/.hydraria/tasks.json` (atomic write every ~5s when state changes) and restored at next startup, so a short link pasted into a playlist or script keeps working across restarts. Settings persist alongside. Set `persist: false` for throwaway tasks.
- **Scriptable** — `POST /api/tasks` takes `{"url": "…"}` (or `urls` / `uris` / `volumes`), everything else defaulted, and `?start_cache=1` makes it "add and start downloading" the way `aria2c`/Motrix/Gopeed do. See [API](#api).
- **Per-source health** — each task tracks per-URL last status, TTFB latency, current throughput, total bytes contributed and last error — surfaced in the dashboard's "源状态看板".
- **Real-time sparkline** — both global throughput and per-task throughput are sampled at ~1 Hz; the dashboard renders a live 60-sample SVG curve.
- **Embedded dashboard** — the web UI is compiled into the binary (`rust-embed`); no external static-file directory needed.

## Architecture

| Layer | Module | Responsibility |
| --- | --- | --- |
| Core engine (data plane) | [src/engine.rs](src/engine.rs) | Probe upstream, plan chunks, run the parallel fetcher with a sliding-window scheduler, serialize chunks back into a single ordered byte stream. Records per-URL fetch outcomes. |
| Cache | [src/cache.rs](src/cache.rs) | Per-URL-set sparse-file cache with a 1 MB block bitmap. Auto-clears on ETag mismatch. |
| Rate limiter | [src/ratelimit.rs](src/ratelimit.rs) | Token-bucket limiter (per-task and global). |
| Host mapping | [src/hostmap.rs](src/hostmap.rs) | `curl --resolve` equivalent: a hot-swappable table behind the client's DNS resolver, so only the TCP target moves. |
| DoT resolver | [src/dns.rs](src/dns.rs) | Resolves mapping targets itself, sidestepping a TUN proxy's fake-ip hijack. Used only on a mapping hit; falls back to the system resolver on failure. |
| Control plane | [src/routes.rs](src/routes.rs), [src/models.rs](src/models.rs) | Task manager, REST API, short-link generation, in-memory task store, per-task health tracker, throughput sampler, persistence. |
| Application layer | [src/main.rs](src/main.rs), [src/assets.rs](src/assets.rs) | CLI (`--bind`, `--cache-dir`, `--state-file`), axum server, embedded dashboard at `/`. |
| Web UI | [web/index.html](web/index.html) | Single-file dashboard (vanilla JS, no build step) with: modal create form, grid/list toggle, search, source health panel, live sparkline, live input validation, copy-with-✓ feedback. |

## Tech stack

| Concern | Crate |
| --- | --- |
| Async runtime | `tokio` |
| HTTP server / routing | `axum` 0.8 |
| HTTP client | `reqwest` (with streaming + rustls) |
| Embedded static assets | `rust-embed` |
| Concurrency primitives | `tokio::sync::mpsc`, `tokio::sync::Semaphore`, `parking_lot::RwLock` |
| Logging | `tracing` + `tracing-subscriber` |
| CLI | `clap` |

## Build & Run

### Install from crates.io (recommended)

```bash
cargo install hydraria
```

This drops a `hydraria` binary into `~/.cargo/bin/` (make sure it's on your `PATH`). Requires Rust 1.85+.

### Build from source

```bash
git clone https://github.com/qy527145/hydraria.git
cd hydraria
cargo build --release
```

The binary lands at `./target/release/hydraria`.

### Run

```bash
hydraria \
  --bind 127.0.0.1:9527 \
  --cache-dir ~/.hydraria/cache \
  --state-file ~/.hydraria/tasks.json
```

Defaults: `--bind 127.0.0.1:9527`, `--cache-dir ~/.hydraria/cache`,
`--state-file ~/.hydraria/tasks.json`.

Then open the dashboard at `http://127.0.0.1:9527/`. Logs go to stdout;
control verbosity with `RUST_LOG`, e.g. `RUST_LOG=hydraria=debug,info`.

## API

Everything the dashboard can configure, the API can configure — the dashboard
uses these same endpoints and has no private channel. Below is the tour;
**[docs/API.md](docs/API.md) is the complete reference**: every task field with
its type and default, every endpoint, every error, plus one request example with
all fields filled in.

### Control plane

#### `POST /api/tasks`

Create a new proxy task. Returns the short link.

The only required input is the URL(s). Everything else falls back to the same
defaults the dashboard's create form uses, so a script never has to spell out a
full config:

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{"url": "https://server1.com/file.mp4"}'
```

```json
{ "task_id": "a1b2c3", "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3" }
```

URLs may be written any of these ways — `uri`/`uris` are accepted as aliases of
`url`/`urls`, matching aria2's naming. The source list is **two-dimensional**:
the outer level is volumes (concatenated in order into one file), the inner
level is mirrors of that volume (interchangeable copies):

| Body | Vol × mirror | Meaning |
| --- | --- | --- |
| `{"url": "https://a/f.mp4"}` | 1 × 1 | one file, one source |
| `{"urls": ["https://a/f.mp4", "https://b/f.mp4"]}` | 1 × 2 | one file, two **mirrors** |
| `{"volumes": [["https://a/p1"], ["https://a/p2"]]}` | 2 × 1 | two **volumes**, concatenated in order |
| `{"volumes": [["https://a/p1", "https://b/p1"], ["https://a/p2", "https://b/p2"]]}` | 2 × 2 | two volumes, **two mirrors each** |

For volumes *and* mirrors together, use the last form — the 2-D `volumes` field
is the only one that can express both levels. Volume order **is** the file's byte
order; mirror order is only a preference.

Mixing strings and arrays in one list is rejected rather than guessed — the cost
of guessing wrong is a task that looks fine and plays garbage.

Every other field (`headers`, `max_per_volume`, `host_mappings`, `plugins`, …)
goes in the same JSON object; see [docs/API.md](docs/API.md) for the full table.

`?start_cache=1` (or `"start_cache": true` in the body) also kicks off the
whole-file cache fill immediately, i.e. "add and start downloading":

```bash
curl -X POST 'http://127.0.0.1:9527/api/tasks?start_cache=1' \
  -H 'content-type: application/json' \
  -d '{"url": "https://server1.com/file.mp4", "name": "movie"}'
```

```json
{ "task_id": "a1b2c3", "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3",
  "cache_started": true }
```

The task is created even when the fill can't start (origin unreachable, no Range
support); the reason comes back separately so a script can tell "the task wasn't
created" from "the task exists but the origin is down right now":

```json
{ "task_id": "a1b2c3", "proxy_url": "…", "cache_started": false,
  "cache_error": "internal: cannot reach the upstream: upstream returned non-success status: 404" }
```

Any other `TaskConfig` field can be supplied alongside:

```bash
curl -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{
    "urls": ["https://server1.com/file.mp4", "https://server2.com/file.mp4"],
    "max_per_volume": 8,
    "max_split": "5M",
    "cache": true,
    "persist": true,
    "headers": {
      "User-Agent": "Mozilla/5.0",
      "Cookie": "session=xxxx"
    }
  }'
```

`max_split` accepts either a number of bytes or a human-readable string: `"5M"`, `"512K"`, `"1G"`, etc.
`max_threads` is derived (`max_per_volume` × volume count) and ignored if sent.

#### `GET /api/tasks`

List all active tasks with stats (bytes served, active connections, config).

#### `GET /api/tasks/:task_id`

Fetch a single task's status.

#### `DELETE /api/tasks/:task_id`

Stop & remove a task. Returns `204`.

#### `PATCH /api/tasks/:task_id`

Partially update a task in place — any subset of `TaskConfig`. Returns the
updated `TaskInfo`. URLs accept the same aliases as create, so rotating an
expired signed link is one line; a PATCH that never mentions URLs leaves the
source list untouched.

```bash
curl -X PATCH http://127.0.0.1:9527/api/tasks/a1b2c3 \
  -H 'content-type: application/json' \
  -d '{"url": "https://server1.com/file.mp4?sign=fresh"}'

curl -X PATCH http://127.0.0.1:9527/api/tasks/a1b2c3 \
  -H 'content-type: application/json' \
  -d '{"max_per_volume": 8, "cache": true}'
```

#### `POST /api/tasks/:task_id/pause` and `…/resume`

Pause makes `GET /stream/:task_id` return `503 Service Unavailable` while the
task config + cache remain intact. Resume flips it back. Both return the
current `TaskInfo`.

#### `POST /api/tasks/:task_id/cache` and `…/cache/pause`

Fill the whole file into the local cache, and pause that whenever you like.
Both share one sparse file and one worker pool with proxied playback: ranges
playback already pulled are never fetched twice, and pausing the fill leaves
live playback connections untouched. Both return the current `cache_job`
(state, progress, speed, worker count, active readers).

`POST …/cache` is idempotent: a no-op while a fill is running, and an immediate
`done` once the file is complete.

#### `DELETE /api/tasks/:task_id/cache`

Wipe this task's on-disk cache (sparse file + bitmap + meta). The task itself
is kept. Returns `204`. The cache key is derived from the URL list, so any other
task pointing at the same content stops writing to it too.

#### `GET /api/settings` · `PUT /api/settings`

Global settings. The PUT body is a partial update; only the keys present are
touched.

| Key | Meaning |
| --- | --- |
| `global_rate_limit_bps` | B/s, or a human size string like `"10M"`. `0`/null = unlimited. |
| `global_rate_limit_algorithm` | `token_bucket` \| `sliding_window`. |
| `plugin_globals` | Per-plugin global config blob, keyed by plugin id. |
| `download_dir` | Default directory for the download button. |
| `host_mappings` | `[{from, to, enabled}]`. `from` is the host as written in the URL (or `*.example.com`); `to` is an IP or host, optionally `:port`. A bad rule fails the whole PUT — the table is never left half-applied. Tasks can carry their own list, unioned over this one. |

#### `GET /api/hostmap/resolve` · `POST /api/hostmap/resolve`

Diagnostic: where does a host actually end up? `?host=` takes a hostname, an IP,
or a whole URL; `&task_id=` evaluates it against that task's effective table
(global ∪ task-level) instead of the global one.

```json
{ "host": "cdn.example.com", "mapped_to": "1.2.3.4:8443",
  "addresses": ["1.2.3.4"], "error": null, "proxy_env": "HTTPS_PROXY" }
```

The POST form additionally evaluates rules that **aren't saved yet** — which is
what the dashboard's ⚡ button uses, because you press it right after editing a
rule and before saving it:

```bash
curl -X POST http://127.0.0.1:9527/api/hostmap/resolve \
  -H 'content-type: application/json' \
  -d '{"host": "cdn.example.com", "scope": "task",
       "mappings": [{"from": "cdn.example.com", "to": "1.2.3.4", "enabled": true}]}'
```

| Field | Meaning |
| --- | --- |
| `host` | hostname, IP, or a whole URL |
| `mappings` | the rules to evaluate. Omit to use what's saved (identical to GET). Half-filled rows are ignored; an invalid rule is reported as an error, since that's the answer you were looking for. |
| `scope` | `task` (default) layers `mappings` over the live global rules, exactly as a running task would. `global` treats `mappings` as the complete set — so deleting a rule in the settings panel correctly reports "no rule matched". |
| `task_id` | only used when `mappings` is omitted. |

#### `GET /api/global`

Snapshot used by the dashboard: aggregate stats, current global throughput, a
60-sample sparkline of recent throughput, total cache footprint on disk.

### Data plane

#### `GET /stream/:task_id`

The endpoint clients consume. Behaves like a regular HTTP file server:

- Honors `Range: bytes=start-end` (and suffix ranges `bytes=-N`).
- Returns `206 Partial Content` for partial reads, `200 OK` for full reads.
- Forwards `Content-Type`, `ETag`, `Last-Modified`, `Accept-Ranges` from the origin probe.
- Adds `X-Hydraria-Task: <task_id>` for traceability.

`HEAD` is also supported for clients that probe before downloading.

## Example: drop into VLC / IINA / Aria2

```bash
# 1. Create task
TASK=$(curl -s -X POST http://127.0.0.1:9527/api/tasks \
  -H 'content-type: application/json' \
  -d '{"url":"https://your-source/file.mp4"}' \
  | sed 's/.*"task_id":"\([^"]*\)".*/\1/')

# 2. Open the short link in any client
vlc        "http://127.0.0.1:9527/stream/$TASK"
aria2c -x16 "http://127.0.0.1:9527/stream/$TASK"
wget        "http://127.0.0.1:9527/stream/$TASK"
```

The client sees a single, plain HTTP/1.1 stream. Hydraria fans the actual fetching out to many parallel range requests across all configured origins.

## Example: a `hydra-add` script

The create API is deliberately shaped like `aria2c <url>` so a one-liner is
enough to wire it into a download-manager habit, a browser extension, or a
`*arr`-style automation:

```bash
#!/usr/bin/env bash
# hydra-add <url> [name] — create a task and start caching it right away.
set -euo pipefail
HYDRARIA=${HYDRARIA:-http://127.0.0.1:9527}

curl -sS -X POST "$HYDRARIA/api/tasks?start_cache=1" \
  -H 'content-type: application/json' \
  -d "$(jq -n --arg url "$1" --arg name "${2:-}" \
        '{url: $url} + (if $name == "" then {} else {name: $name} end)')" \
  | jq -r 'if .cache_error then "created \(.task_id) but caching failed: \(.cache_error)"
           else "\(.proxy_url)" end'
```

Poll `GET /api/tasks` for `cache_job.done_bytes` / `.total_bytes` to track
progress, or just hand `proxy_url` to a player and let playback pull what it
needs.

## How chunked streaming works

1. **Probe** — when a client connects, Hydraria first issues a `HEAD` for cheap metadata (Content-Type, Content-Length, ETag, Last-Modified) and then a `Range: bytes=0-0` `GET`. The 206 response from the GET is the only reliable signal that an origin actually supports byte ranges (many CDNs serve ranges but don't advertise `Accept-Ranges` on HEAD).
2. **Cache check (if enabled)** — Hydraria opens (or rewires) the cache entry keyed by the SHA-256 of the URL list. A stored meta with a non-matching ETag/size is treated as stale and the on-disk state is wiped.
3. **Claim** — the effective range `[start, end]` goes to the scheduler whole; there is no pre-computed plan, and a worker carves a range only when it asks for one. How big depends on the scenario: a download takes an even share of the work left (remaining ÷ free workers, largest-first, no probing), while playback packs the pool into short, equal claims right behind the read head (2 MiB by default), lengthening them to `buffer / threads` only once the reader has runway — an ordered reader can only emit its contiguous prefix, so equal claims are what make that prefix advance at the pool's aggregate rate instead of one connection's. Sizing only shrinks on evidence: a claim that times out having delivered nothing (the "materialize the whole range before sending a byte" relay shape) drops it to 8 MiB and records the ceiling. A non-zero `max_split` is a hard ceiling on every claim.
4. **Pull** — one request per worker per range, written to the local file at its absolute offset. A shared end-watermark is the only preemption mechanism: when someone moves it inward, the worker retires cleanly at its next stream item, so no byte is ever fetched twice.
5. **Stitch** — the ordered reader reads from that local file and drags the critical window along as it goes. Write order is irrelevant; only the reader cares about order.
6. **Cache writeback** — for cache-enabled tasks, every byte received from upstream is `pwrite`-ed to the sparse cache file at its absolute offset. Block-completion is tracked via a per-block byte-counter; when a block is fully covered, its bit is flipped in the bitmap (which is fsync-rotated to disk).
7. **Retry** — if a chunk's origin fails mid-stream, the engine retries that chunk on the next URL in the round-robin list.

## Configuration reference

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `urls` | `string[]` | required | Origin URLs. The same content must be available on each. |
| `max_threads` | `int` | `8` | Maximum concurrent chunk fetchers per client connection. |
| `max_split` | `int` or human string | `0` (auto) | Hard ceiling on one range request. `0` lets the scheduler size claims itself; a non-zero value caps every claim. |
| `cache` | `bool` | `false` | Reserved for future on-disk caching. |
| `headers` | `object<string,string>` | `{}` | Headers to attach to every upstream request. |
| `host_mappings` | `[{from, to, enabled}]` | `[]` | Task-level host mappings, unioned over the global ones (task wins on a conflicting `from`). |
| `name` | `string?` | `null` | Optional friendly name shown in the dashboard. |

## Project layout

```
.
├── Cargo.toml
├── src
│   ├── main.rs        # CLI + axum server bootstrap
│   ├── lib.rs         # module roots
│   ├── models.rs      # TaskConfig, TaskEntry, AppState, GlobalSettings, UrlHealth
│   ├── engine.rs      # multi-threaded chunked fetcher + range parser + health hooks
│   ├── cache.rs       # sparse-file + bitmap cache, ETag-keyed
│   ├── ratelimit.rs   # token-bucket limiter (per-task + global)
│   ├── routes.rs      # axum router for control + data plane
│   ├── assets.rs      # rust-embed-backed static asset handler
│   └── error.rs       # ProxyError + IntoResponse
└── web
    └── index.html     # dashboard (embedded into the binary at compile time)
```

## Changelog

### v0.1.6

- **Scheduler starvation fix** — under slow clients (single-connection browser downloads), the main loop's `biased select!` preferred draining `rx` and left `release_rx` un-polled for long stretches, so every upstream URL dropped to 0 B/s until the already-buffered data drained. After each forwarded item we now non-blocking-drain pending release events and immediately re-spawn, keeping upstreams hot end to end.
- **Less thread laziness** — per-chunk channel buffer changed from a fixed 4 to a split-derived size (~split / 16 KiB, capped at 512), so fetchers no longer backpressure their channel and stall as "0 B/s on volumes 2+". The scheduler's strict pass now picks by plan order rather than spreading across volumes, concentrating parallelism on what the serializer is about to consume instead of pre-fetching far-future volumes.
- **Plugin / volume UX polish** — improvements to the volume + plugin sections of the create / edit modal.
- **Cache identity tighter** — additional edge cases tightened around cache-hit recognition.

### v0.1.5

- **Two-level concurrent scheduling** — introduced `max_per_volume` as a soft cap, so per-URL/per-IP concurrency stays bounded while task-wide `max_threads` still fills up. Strict + overflow two-pass spawn keeps the budget at its limit even when work is unevenly distributed.
- **Source dashboard upgrades** — URL health now reports `in_flight_requests` and `volume_size`; the dashboard renders live in-flight counts and per-volume sizes per URL.
- **HEAD-unsupported skip list** — per-task shared set of URLs that have rejected HEAD; subsequent probes skip straight to the 1-byte Range GET.
- **Global cache wipe** — `DELETE /api/cache` clears the local cache for every task in one shot.

### v0.1.4

- **Sticky modal action bar** — long forms keep their close / save buttons in reach at the bottom.
- **Plugin config preserved on JSON import** — fixed an import path that dropped the plugin section.

### v0.1.3

- **Plugin system + ChaCha20 decrypt plugin** — outgoing byte pipeline can stack multiple transforms, applied in reverse. Bundled ChaCha20-Poly1305 decrypt plugin enables encrypted origin → plaintext client playback.
- **Cache concurrent-write race fixed** — bitmap race when multiple fetchers write the same block in parallel.

### v0.1.2 / v0.1.1

- **Seven core optimizations** — cross-volume cache, resume-from-offset, weighted source selection, clone / import / export, cache heatmap, `download` CLI subcommand.
- **Drag-to-reorder volumes + auto filename LCP merge** — UI supports drag-reordering; multi-volume tasks pick the longest common prefix across per-volume filenames as the default output name.
- **Cross-volume warm-up scheduling** — pre-opens connections to the next volume before the boundary, avoiding TCP-setup latency at the transition.

## Roadmap

- LRU eviction for the cache directory (currently grows unbounded).
- Auth / token gating on the control-plane API for non-localhost binds.
- A `download` CLI subcommand that drives the engine directly to a local file.
- Probe-result caching to skip the upstream HEAD on warm cache hits.

## License

MIT (or whatever you choose — adjust before publishing).
