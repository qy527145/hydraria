# Hydraria

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

- **Multi-threaded chunked fetching** — each request is split into `max_split`-sized byte ranges and pulled concurrently up to `max_threads`. The scheduler uses a sliding window over the in-flight set, so the chunk being drained is always among those running (no starvation).
- **Multi-source failover** — list multiple origin URLs; chunks are round-robined across them and a failed chunk transparently retries on a different origin.
- **Range / Seek support** — when a player issues `Range: bytes=…` (e.g. seeking forward in a video), Hydraria re-plans the chunks from that offset. No re-download of earlier bytes. Always responds with `206` when the client sent a Range header (which is what Chrome's `<video>` element relies on to know seeking is supported).
- **Disk cache** — opt-in per task. Bytes are stored in a sparse file keyed by the SHA-256 of the URL list, with a bitmap tracking 1-MB block completion. A second request to the same task is served entirely from disk; no upstream traffic. ETag-validated on every probe; an upstream change auto-wipes and re-fetches.
- **Passthrough fallback** — if the origin doesn't advertise byte-range support, Hydraria automatically falls back to a single-stream passthrough so unrangeable sources still work.
- **Backpressure-aware streaming** — the chunk planner uses a bounded-channel pipeline (`tokio::sync::mpsc`), so a slow client throttles upstream fetches instead of blowing memory.
- **Custom headers per task** — set `Cookie`, `User-Agent`, `Referer`, etc. once at task creation; every upstream chunk request carries them.
- **Pause / resume / edit** — tasks can be paused (stream returns 503 while config + cache stay intact) and live-edited via `PATCH /api/tasks/:id`. No need to delete and recreate.
- **Embedded dashboard** — the web UI is compiled into the binary (`rust-embed`); no external static-file directory needed.

## Architecture

| Layer | Module | Responsibility |
| --- | --- | --- |
| Core engine (data plane) | [src/engine.rs](src/engine.rs) | Probe upstream, plan chunks, run the parallel fetcher with a sliding-window scheduler, serialize chunks back into a single ordered byte stream. |
| Cache | [src/cache.rs](src/cache.rs) | Per-URL-set sparse-file cache with a 1 MB block bitmap. Auto-clears on ETag mismatch. |
| Control plane | [src/routes.rs](src/routes.rs), [src/models.rs](src/models.rs) | Task manager, REST API, short-link generation, in-memory task store. |
| Application layer | [src/main.rs](src/main.rs), [src/assets.rs](src/assets.rs) | CLI (`--bind`, `--cache-dir`), axum server, embedded dashboard at `/`. |
| Web UI | [web/index.html](web/index.html) | Single-file dashboard (vanilla JS, no build step) with edit modal, pause / resume, cache stats and progress bars. |

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

Requires Rust 1.85+ (edition 2024).

```bash
cargo build --release
./target/release/hydraria --bind 127.0.0.1:9527
# or specify a cache directory:
./target/release/hydraria --bind 127.0.0.1:9527 --cache-dir ~/.hydraria/cache
```

Then open the dashboard:

```
http://127.0.0.1:9527/
```

`--cache-dir` defaults to `~/.hydraria/cache`. Logs go to stdout; control
verbosity with `RUST_LOG`, e.g. `RUST_LOG=hydraria=debug,info`.

## API

### Control plane

#### `POST /api/tasks`

Create a new proxy task. Returns the short link.

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

Response:

```json
{ "task_id": "a1b2c3", "proxy_url": "http://127.0.0.1:9527/stream/a1b2c3" }
```

`max_split` accepts either a number of bytes or a human-readable string: `"5M"`, `"512K"`, `"1G"`, etc.

#### `GET /api/tasks`

List all active tasks with stats (bytes served, active connections, config).

#### `GET /api/tasks/:task_id`

Fetch a single task's status.

#### `DELETE /api/tasks/:task_id`

Stop & remove a task. Returns `204`.

#### `PATCH /api/tasks/:task_id`

Partially update a task in place — any subset of `urls`, `max_threads`,
`max_split`, `cache`, `headers`, `name`. Returns the updated `TaskInfo`.

```bash
curl -X PATCH http://127.0.0.1:9527/api/tasks/a1b2c3 \
  -H 'content-type: application/json' \
  -d '{"max_threads": 32, "cache": true}'
```

#### `POST /api/tasks/:task_id/pause` and `…/resume`

Pause makes `GET /stream/:task_id` return `503 Service Unavailable` while the
task config + cache remain intact. Resume flips it back. Both return the
current `TaskInfo`.

#### `DELETE /api/tasks/:task_id/cache`

Wipe this task's on-disk cache (sparse file + bitmap + meta). The task itself
is kept. Returns `204`.

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
  -d '{"urls":["https://your-source/file.mp4"],"max_threads":16,"max_split":"5M"}' \
  | sed 's/.*"task_id":"\([^"]*\)".*/\1/')

# 2. Open the short link in any client
vlc        "http://127.0.0.1:9527/stream/$TASK"
aria2c -x16 "http://127.0.0.1:9527/stream/$TASK"
wget        "http://127.0.0.1:9527/stream/$TASK"
```

The client sees a single, plain HTTP/1.1 stream. Hydraria fans the actual fetching out to many parallel range requests across all configured origins.

## How chunked streaming works

1. **Probe** — when a client connects, Hydraria first issues a `HEAD` for cheap metadata (Content-Type, Content-Length, ETag, Last-Modified) and then a `Range: bytes=0-0` `GET`. The 206 response from the GET is the only reliable signal that an origin actually supports byte ranges (many CDNs serve ranges but don't advertise `Accept-Ranges` on HEAD).
2. **Cache check (if enabled)** — Hydraria opens (or rewires) the cache entry keyed by the SHA-256 of the URL list. A stored meta with a non-matching ETag/size is treated as stale and the on-disk state is wiped.
3. **Plan** — given the client's effective range `[start, end]`, the engine slices it into `max_split`-sized sub-ranges.
4. **Pull** — each sub-range fetches in parallel under a sliding-window scheduler. The same task that drains chunks in order also spawns the next chunk when the previous one finishes; this keeps `max_threads` chunks in-flight while guaranteeing the chunk being drained is always among the running set (a plain semaphore would deadlock against the bounded per-chunk channels).
5. **Stitch** — a serializer task drains the per-chunk channels in plan order and forwards bytes to the client. Bounded channels backpressure the fetchers when the client reads slowly.
6. **Cache writeback** — for cache-enabled tasks, every byte received from upstream is `pwrite`-ed to the sparse cache file at its absolute offset. Block-completion is tracked via a per-block byte-counter; when a block is fully covered, its bit is flipped in the bitmap (which is fsync-rotated to disk).
7. **Retry** — if a chunk's origin fails mid-stream, the engine retries that chunk on the next URL in the round-robin list.

## Configuration reference

| Field | Type | Default | Meaning |
| --- | --- | --- | --- |
| `urls` | `string[]` | required | Origin URLs. The same content must be available on each. |
| `max_threads` | `int` | `8` | Maximum concurrent chunk fetchers per client connection. |
| `max_split` | `int` or human string | `5M` | Size of each chunk. Smaller = faster failover & seek, more overhead. |
| `cache` | `bool` | `false` | Reserved for future on-disk caching. |
| `headers` | `object<string,string>` | `{}` | Headers to attach to every upstream request. |
| `name` | `string?` | `null` | Optional friendly name shown in the dashboard. |

## Project layout

```
.
├── Cargo.toml
├── src
│   ├── main.rs        # CLI + axum server bootstrap
│   ├── lib.rs         # module roots
│   ├── models.rs      # TaskConfig, TaskStore, AppState, short_id()
│   ├── engine.rs      # multi-threaded chunked fetcher + range parser
│   ├── cache.rs       # sparse-file + bitmap cache, ETag-keyed
│   ├── routes.rs      # axum router for control + data plane
│   ├── assets.rs      # rust-embed-backed static asset handler
│   └── error.rs       # ProxyError + IntoResponse
└── web
    └── index.html     # dashboard (embedded into the binary at compile time)
```

## Roadmap

- Persistent task store (SQLite) so tasks survive a restart (the cache already does).
- Per-task bandwidth limits.
- A `download` CLI subcommand that drives the engine directly to a local file.
- Auth / token gating on the control-plane API for non-localhost binds.
- Probe-result caching to skip the upstream HEAD on warm cache hits.

## License

MIT (or whatever you choose — adjust before publishing).
